//! Unit tests for the Plugin Call Guard (DESIGN §3.2 "Plugin Call Guard", §3.3, §4.5,
//! §4.7, §4.8 "Guard tests"). Unless a test says otherwise it runs on a paused tokio
//! clock, so elapsed time is exact and asserted instead of wall time. The mapping of a
//! guarded failure to an outcome or reason is tested in `domain/ports_tests.rs`.
//!
//! Planned API in `crate::infra::call_guard`:
//!
//! ```text
//! pub enum PluginKind { Section, Audit }
//! pub enum CallKind { Key, Schema, Apply, Remove, RequestHandoff, FinalResultHandoff }
//! impl CallKind { pub fn plugin_kind(self) -> PluginKind }  // Key/Schema/Apply/Remove ->
//!                                                           // Section; hand-offs -> Audit
//! pub struct CallMeta {            // log metadata only; never manifest content
//!     pub request_id: Option<Uuid>,   // None for `key` (called at startup, no request)
//!     pub call_kind: CallKind,
//!     pub section_key: Option<String>,
//! }
//!
//! #[derive(Clone)]                 // clones share the same in-flight counters
//! pub struct PluginCallGuard;
//! impl PluginCallGuard {
//!     /// Panics, with a message naming `plugin_max_in_flight`, when the cap is 0
//!     /// (config validation already guarantees 1..=1_000_000).
//!     pub fn new(plugin_call_timeout: Duration, plugin_max_in_flight: usize) -> Self;
//!
//!     /// Reserves a slot for `plugin_id` on the FIRST POLL (before any await): cap full ->
//!     /// `Ready(Err(NotSent))` at once, `fut` dropped unpolled. Otherwise spawns `fut`
//!     /// detached and waits up to the timeout (no margin). The call keeps running when
//!     /// the wait ends (timeout) or the returned future is dropped; no cancel signal.
//!     /// The slot is released BEFORE the result is delivered to the caller, on every
//!     /// outcome, including panic, so `in_flight` is already updated when `call` returns.
//!     /// A panic maps to `Internal(text)` where the text is FIXED or bounded and never
//!     /// contains the panic message. A call that ends after the caller stopped waiting
//!     /// logs exactly one event `plugin_call_late_result` with fields `request_id`
//!     /// (when known), `plugin_kind` (`section` | `audit`), `call_kind` (snake_case),
//!     /// `section_key` (when known), `elapsed_ms` (since the call started) and
//!     /// `result_kind` (`ok` | `error` | `panic`): never the plugin's error detail, panic
//!     /// text or section value. A call that ends within the limit logs no such event.
//!     pub async fn call<T, E, F>(&self, plugin_id: &str, meta: CallMeta, fut: F)
//!         -> Result<T, PluginCallError<E>>
//!     where F: Future<Output = Result<T, E>> + Send + 'static,
//!           T: Send + 'static, E: Send + 'static;
//!
//!     /// Calls running for `plugin_id`, detached ones included.
//!     pub fn in_flight(&self, plugin_id: &str) -> usize;
//!
//!     /// Waits until no call runs or `deadline` is cancelled (the `stop` deadline token;
//!     /// the guard never hands it to a plugin). Returns the number of calls still
//!     /// running, and logs one event `plugin_calls_abandoned` per plugin with fields
//!     /// `plugin_id` and `count`. Cancels nothing; idle -> 0 at once.
//!     pub async fn drain(&self, deadline: &CancellationToken) -> usize;
//! }
//! ```
//!
//! Tests:
//! - `call_within_timeout_returns_the_result`, `plugin_error_passes_through_unchanged`
//! - `call_exceeding_timeout_returns_timed_out_at_exactly_the_timeout` (§3.2 no margin)
//! - `timed_out_call_is_not_cancelled_and_completes_detached` (§4.5, §4.8)
//! - `caller_dropping_the_call_does_not_cancel_it` (§3.2, §4.5)
//! - `in_flight_count_includes_detached_calls` (§3.2)
//! - `first_poll_reserves_the_slot`, `refused_call_is_ready_not_sent_on_first_poll` (§3.2)
//! - `full_cap_refuses_without_sending_and_ends_at_once_as_not_sent` (§3.2, §4.5)
//! - `concurrent_calls_over_the_cap_get_not_sent` (§3.2, order independent)
//! - `cap_holds_under_multi_thread_contention` (64 calls, cap 3: exactly 61 refused)
//! - `slot_is_released_before_the_result_is_delivered` (multi-thread, sequential)
//! - `refused_call_does_not_cancel_running_calls` (§3.2)
//! - `slot_is_freed_when_a_detached_call_completes`, `slot_is_freed_after_a_normal_call`
//! - `panicking_call_maps_to_internal_and_frees_its_slot` (§3.2, §4.8)
//! - `panic_message_never_reaches_the_internal_error` (§2.2 no manifest content)
//! - `late_panic_after_timeout_frees_the_slot` (§4.5)
//! - `caps_are_per_plugin_instance` (§3.2), `guard_clones_share_counters`
//! - `every_call_kind_is_guarded`, `call_kinds_belong_to_their_plugin_kind` (§3.3)
//! - `meta_without_request_id_is_accepted` (key call at startup, §3.3)
//! - `audit_handoff_over_cap_is_not_sent` (§3.3)
//! - `new_rejects_a_zero_cap`
//! - `late_result_is_logged_once_by_metadata_only`, `late_result_logs_error_and_panic_kinds`,
//!   `result_within_the_limit_logs_no_late_event` (§3.2, §4.8)
//! - `drain_returns_zero_at_once_when_idle`,
//!   `drain_returns_zero_when_detached_calls_finish_in_time`,
//!   `drain_counts_hung_calls_of_one_plugin`, `drain_sums_across_plugins`,
//!   `drain_mixed_finished_and_hung_calls`, `drain_with_a_cancelled_token_returns_at_once`,
//!   `drain_logs_abandoned_count_per_plugin`, `drain_cancels_nothing` (§3.2)

use std::future::Future;
use std::io::Write;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use application_manager_sdk::models::HandoffOperation;
use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginClientV1, AuditPluginError, CallerIdentity,
    RequestHandoffContent, SectionPluginClientV1, SectionPluginError,
};
use serde_json::{Value, json};
use tokio::sync::Barrier;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

use crate::domain::ports::PluginCallError;
use crate::infra::call_guard::{CallKind, CallMeta, PluginCallGuard, PluginKind};
use crate::test_support::{
    AuditCall, CallBehavior, FakeAuditPlugin, FakeSectionPlugin, SectionCall,
};

const TIMEOUT: Duration = Duration::from_millis(500);
const YEAR: Duration = Duration::from_hours(365 * 24);
const ALL_KINDS: [CallKind; 6] = [
    CallKind::Key,
    CallKind::Schema,
    CallKind::Apply,
    CallKind::Remove,
    CallKind::RequestHandoff,
    CallKind::FinalResultHandoff,
];

type SectionResult = Result<(), PluginCallError<SectionPluginError>>;
type AuditResult = Result<String, PluginCallError<AuditPluginError>>;

fn caller() -> CallerIdentity {
    CallerIdentity {
        subject_id: Uuid::from_u128(1),
        subject_type: None,
        subject_tenant_id: Uuid::from_u128(2),
        token_scopes: vec![],
        request_id: Uuid::from_u128(3),
    }
}

fn envelope() -> ApplicationEnvelope {
    ApplicationEnvelope {
        id: "app".to_owned(),
        version: "1".to_owned(),
    }
}

fn meta(call_kind: CallKind) -> CallMeta {
    CallMeta {
        request_id: Some(Uuid::from_u128(3)),
        call_kind,
        section_key: Some("netw".to_owned()),
    }
}

fn guard(cap: usize) -> PluginCallGuard {
    PluginCallGuard::new(TIMEOUT, cap)
}

fn section(key: &str) -> Arc<FakeSectionPlugin> {
    Arc::new(FakeSectionPlugin::new(key))
}

// The caller and envelope are bound inside the async block, before the call, so the
// plugin future never borrows a temporary (a tail-expression borrow would not compile).
async fn guarded_apply(
    guard: &PluginCallGuard,
    plugin_id: &str,
    fake: &Arc<FakeSectionPlugin>,
) -> SectionResult {
    let fake = Arc::clone(fake);
    guard
        .call(plugin_id, meta(CallKind::Apply), async move {
            let caller = caller();
            let envelope = envelope();
            fake.apply(&caller, &envelope, Value::Null).await
        })
        .await
}

async fn guarded_request_handoff(
    guard: &PluginCallGuard,
    plugin_id: &str,
    fake: &Arc<FakeAuditPlugin>,
) -> AuditResult {
    let fake = Arc::clone(fake);
    guard
        .call(plugin_id, meta(CallKind::RequestHandoff), async move {
            let caller = caller();
            let content = RequestHandoffContent {
                operation: HandoffOperation::Seed,
                request_id: Uuid::from_u128(3),
                received_at: "2026-10-12T00:00:00Z".to_owned(),
                application: None,
                manifest: None,
                decided_category: None,
                id_length_bytes: None,
                id_sha256: None,
            };
            fake.request_handoff(&caller, &content).await
        })
        .await
}

// A cancellation token that fires `after` from now, from a spawned task.
fn cancel_after(after: Duration) -> CancellationToken {
    let token = CancellationToken::new();
    let trigger = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(after).await;
        trigger.cancel();
    });
    token
}

// Log capture. One global subscriber (installed once) writes to a per-thread buffer, so
// callsite interest is never cached as "disabled" by a parallel test that has no
// capture. Current-thread runtime tests only: the buffer belongs to the test thread.
thread_local! {
    static CURRENT_LOG: std::cell::RefCell<Option<LogBuffer>> = const { std::cell::RefCell::new(None) };
}

#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

struct RoutedWriter;

impl Write for RoutedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        CURRENT_LOG.with(|current| {
            if let Some(buffer) = current.borrow().as_ref() {
                buffer.0.lock().expect("log lock").extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for RoutedWriter {
    type Writer = RoutedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        RoutedWriter
    }
}

impl LogBuffer {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("log lock").clone()).expect("utf8 log")
    }

    fn lines_with(&self, message: &str) -> Vec<String> {
        self.text()
            .lines()
            .filter(|line| line.contains(message))
            .map(str::to_owned)
            .collect()
    }
}

// Stops routing to the buffer when dropped.
struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        CURRENT_LOG.with(|current| *current.borrow_mut() = None);
    }
}

fn capture_logs() -> (LogBuffer, CaptureGuard) {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(RoutedWriter)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other global tracing subscriber in this test binary");
    });
    let buffer = LogBuffer::default();
    CURRENT_LOG.with(|current| *current.borrow_mut() = Some(buffer.clone()));
    (buffer, CaptureGuard)
}

fn has_field(line: &str, field: &str, value: &str) -> bool {
    let wanted = format!("{field}={value}");
    line.split_whitespace().any(|token| token == wanted)
}

const LATE: &str = "plugin_call_late_result";
const ABANDONED: &str = "plugin_calls_abandoned";

// DESIGN §3.2: a call that ends within the limit returns its own result.
#[tokio::test(start_paused = true)]
async fn call_within_timeout_returns_the_result() {
    let guard = guard(2);
    let fake = section("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_millis(100)),
    );
    let started = Instant::now();
    assert_eq!(guarded_apply(&guard, "p", &fake).await, Ok(()));
    assert_eq!(started.elapsed(), Duration::from_millis(100));
    assert_eq!(guard.in_flight("p"), 0);
}

// DESIGN §3.3: error kinds travel unchanged.
#[tokio::test(start_paused = true)]
async fn plugin_error_passes_through_unchanged() {
    let errors = [
        SectionPluginError::rejected("bad", Some("/a")),
        SectionPluginError::not_permitted("perm"),
        SectionPluginError::internal("boom"),
    ];
    for err in errors {
        let guard = guard(2);
        let fake = section("k");
        fake.on(SectionCall::Apply, CallBehavior::err(err.clone()));
        assert_eq!(
            guarded_apply(&guard, "p", &fake).await,
            Err(PluginCallError::Plugin(err))
        );
    }
}

// DESIGN §3.2 "adds no margin to the limit"; §4.8 asserts elapsed limits, not wall time.
#[tokio::test(start_paused = true)]
async fn call_exceeding_timeout_returns_timed_out_at_exactly_the_timeout() {
    let guard = guard(2);
    let fake = section("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(10)),
    );
    let started = Instant::now();
    assert_eq!(
        guarded_apply(&guard, "p", &fake).await,
        Err(PluginCallError::TimedOut)
    );
    assert_eq!(started.elapsed(), TIMEOUT);
}

// DESIGN §4.5 / §4.8: "A call that completes after the limit has its side effect
// observed after the response." No cancel signal reaches the plugin.
#[tokio::test(start_paused = true)]
async fn timed_out_call_is_not_cancelled_and_completes_detached() {
    let guard = guard(2);
    let fake = section("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    assert_eq!(
        guarded_apply(&guard, "p", &fake).await,
        Err(PluginCallError::TimedOut)
    );
    assert_eq!(fake.call_count(), 1);
    assert_eq!(fake.completed(), 0, "still running right after the timeout");
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert_eq!(fake.completed(), 1, "the detached call ran to completion");
}

// DESIGN §4.5: the request task is detached from the HTTP request, but the guard must
// also survive a caller that drops the `call` future (for example an outer timeout).
#[tokio::test(start_paused = true)]
async fn caller_dropping_the_call_does_not_cancel_it() {
    let guard = guard(2);
    let fake = section("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(1)),
    );
    let outcome = tokio::time::timeout(
        Duration::from_millis(100),
        guarded_apply(&guard, "p", &fake),
    )
    .await;
    assert!(outcome.is_err(), "the outer timeout drops the call future");
    assert_eq!(guard.in_flight("p"), 1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(fake.completed(), 1, "the plugin call was not cancelled");
    assert_eq!(guard.in_flight("p"), 0);
}

// DESIGN §3.2: the count includes detached calls still running after their timeout.
#[tokio::test(start_paused = true)]
async fn in_flight_count_includes_detached_calls() {
    let guard = guard(3);
    let fake = section("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    assert_eq!(guard.in_flight("p"), 0);
    drop(guarded_apply(&guard, "p", &fake).await);
    assert_eq!(guard.in_flight("p"), 1, "detached call still counts");
    drop(guarded_apply(&guard, "p", &fake).await);
    assert_eq!(guard.in_flight("p"), 2);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(guard.in_flight("p"), 0, "late completions free their slots");
}

// F-010: the slot is reserved on the first poll, before any await point.
#[tokio::test(start_paused = true)]
async fn first_poll_reserves_the_slot() {
    let guard = guard(2);
    let fake = section("k");
    fake.on(SectionCall::Apply, CallBehavior::hang());
    let mut call = pin!(guarded_apply(&guard, "p", &fake));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(call.as_mut().poll(&mut cx).is_pending());
    assert_eq!(guard.in_flight("p"), 1, "reserved by the first poll");
}

#[tokio::test(start_paused = true)]
async fn refused_call_is_ready_not_sent_on_first_poll() {
    let guard = guard(1);
    let fake = section("k");
    fake.on(SectionCall::Apply, CallBehavior::hang());
    drop(guarded_apply(&guard, "p", &fake).await); // times out, stays in flight
    let mut call = pin!(guarded_apply(&guard, "p", &fake));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        call.as_mut().poll(&mut cx),
        Poll::Ready(Err(PluginCallError::NotSent))
    );
    assert_eq!(fake.call_count(), 1, "the refused future was never polled");
}

// DESIGN §3.2 / §4.5: cap full -> not sent, ends at once, distinct from a timeout.
#[tokio::test(start_paused = true)]
async fn full_cap_refuses_without_sending_and_ends_at_once_as_not_sent() {
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(10)),
    );
    drop(guarded_apply(&guard, "p", &fake).await); // times out, stays in flight
    assert_eq!(fake.call_count(), 1);

    let before = Instant::now();
    let result = guarded_apply(&guard, "p", &fake).await;
    assert_eq!(result, Err(PluginCallError::NotSent));
    assert_eq!(before.elapsed(), Duration::ZERO, "refusal is immediate");
    assert_eq!(
        fake.call_count(),
        1,
        "the refused call never reached the plugin"
    );
}

// Order independent: only the counts are asserted.
#[tokio::test(start_paused = true)]
async fn concurrent_calls_over_the_cap_get_not_sent() {
    let guard = guard(2);
    let fake = section("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_millis(100)),
    );
    let results = tokio::join!(
        guarded_apply(&guard, "p", &fake),
        guarded_apply(&guard, "p", &fake),
        guarded_apply(&guard, "p", &fake),
    );
    let results = [results.0, results.1, results.2];
    let ok = results.iter().filter(|r| **r == Ok(())).count();
    let refused = results
        .iter()
        .filter(|r| **r == Err(PluginCallError::NotSent))
        .count();
    assert_eq!((ok, refused), (2, 1));
    assert_eq!(fake.call_count(), 2);
}

// F-010: real threads, real clock. Exactly `cap` calls are admitted, whatever the
// interleaving.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cap_holds_under_multi_thread_contention() {
    let guard = PluginCallGuard::new(Duration::from_millis(100), 3);
    let fake = section("k");
    fake.on(SectionCall::Apply, CallBehavior::hang());
    let barrier = Arc::new(Barrier::new(64));
    let tasks: Vec<_> = (0..64)
        .map(|_| {
            let guard = guard.clone();
            let fake = Arc::clone(&fake);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                guarded_apply(&guard, "p", &fake).await
            })
        })
        .collect();
    let mut refused = 0;
    let mut timed_out = 0;
    for task in tasks {
        match task.await.expect("no panic") {
            Err(PluginCallError::NotSent) => refused += 1,
            Err(PluginCallError::TimedOut) => timed_out += 1,
            other => panic!("unexpected result {other:?}"),
        }
    }
    assert_eq!(refused, 61);
    assert_eq!(timed_out, 3);
    assert_eq!(guard.in_flight("p"), 3);
    assert_eq!(fake.call_count(), 3);
    // The three hung detached calls end with the runtime at test exit.
}

// F-026: the slot is released before the caller sees the result, so a sequential
// caller on another thread never meets a stale slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slot_is_released_before_the_result_is_delivered() {
    let guard = guard(1);
    let fake = section("k");
    for round in 0..200 {
        assert_eq!(
            guarded_apply(&guard, "p", &fake).await,
            Ok(()),
            "round {round}"
        );
        assert_eq!(guard.in_flight("p"), 0, "round {round}");
    }
}

// DESIGN §3.2: "No running call is cancelled."
#[tokio::test(start_paused = true)]
async fn refused_call_does_not_cancel_running_calls() {
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    drop(guarded_apply(&guard, "p", &fake).await); // timed out, detached
    drop(guarded_apply(&guard, "p", &fake).await); // refused
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(fake.completed(), 1);
}

#[tokio::test(start_paused = true)]
async fn slot_is_freed_when_a_detached_call_completes() {
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(1)),
    );
    assert_eq!(
        guarded_apply(&guard, "p", &fake).await,
        Err(PluginCallError::TimedOut)
    );
    assert_eq!(
        guarded_apply(&guard, "p", &fake).await,
        Err(PluginCallError::NotSent)
    );
    tokio::time::sleep(Duration::from_secs(1)).await; // detached call completes
    assert_eq!(guarded_apply(&guard, "p", &fake).await, Ok(()));
    assert_eq!(fake.call_count(), 2, "the refused call is not in the log");
}

#[tokio::test(start_paused = true)]
async fn slot_is_freed_after_a_normal_call() {
    let guard = guard(1);
    let fake = section("k");
    for _ in 0..5 {
        assert_eq!(guarded_apply(&guard, "p", &fake).await, Ok(()));
        assert_eq!(guard.in_flight("p"), 0);
    }
}

// DESIGN §3.2 "Turns a panic ... into an internal error"; §4.8 "does not affect later calls".
#[tokio::test(start_paused = true)]
async fn panicking_call_maps_to_internal_and_frees_its_slot() {
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(SectionCall::Apply, CallBehavior::panic());
    let result = guarded_apply(&guard, "p", &fake).await;
    assert!(
        matches!(result, Err(PluginCallError::Internal(_))),
        "got {result:?}"
    );
    assert_eq!(guard.in_flight("p"), 0, "a panic frees the slot");
    assert_eq!(guarded_apply(&guard, "p", &fake).await, Ok(()));
}

// DESIGN §2.2: a panic message can hold manifest values; it must not leave the guard.
#[tokio::test(start_paused = true)]
async fn panic_message_never_reaches_the_internal_error() {
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::panic_with("SENTINEL-PANIC-TEXT"),
    );
    let error = guarded_apply(&guard, "p", &fake)
        .await
        .expect_err("a panic is an error");
    let PluginCallError::Internal(text) = &error else {
        panic!("expected Internal, got {error:?}");
    };
    assert!(!text.contains("SENTINEL"), "text leaks the panic: {text}");
    assert!(
        !format!("{error:?}").contains("SENTINEL"),
        "Debug leaks the panic"
    );
}

// DESIGN §4.5: a panic after the caller stopped waiting still frees the slot.
#[tokio::test(start_paused = true)]
async fn late_panic_after_timeout_frees_the_slot() {
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::panic().with_delay(Duration::from_secs(2)),
    );
    assert_eq!(
        guarded_apply(&guard, "p", &fake).await,
        Err(PluginCallError::TimedOut)
    );
    assert_eq!(guard.in_flight("p"), 1);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(guard.in_flight("p"), 0, "the late panic released the slot");
    assert_eq!(guarded_apply(&guard, "p", &fake).await, Ok(()));
}

// DESIGN §3.2: "counts the calls in flight to each plugin instance".
#[tokio::test(start_paused = true)]
async fn caps_are_per_plugin_instance() {
    let guard = guard(1);
    let hung = section("a");
    hung.on(SectionCall::Apply, CallBehavior::hang());
    let healthy = section("b");
    assert_eq!(
        guarded_apply(&guard, "a", &hung).await,
        Err(PluginCallError::TimedOut)
    );
    assert_eq!(
        guarded_apply(&guard, "a", &hung).await,
        Err(PluginCallError::NotSent)
    );
    assert_eq!(guarded_apply(&guard, "b", &healthy).await, Ok(()));
    assert_eq!(guard.in_flight("a"), 1);
    assert_eq!(guard.in_flight("b"), 0);
    assert_eq!(healthy.call_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn guard_clones_share_counters() {
    let guard = guard(1);
    let other = guard.clone();
    let fake = section("k");
    fake.enqueue(SectionCall::Apply, CallBehavior::hang());
    drop(guarded_apply(&guard, "p", &fake).await);
    assert_eq!(other.in_flight("p"), 1);
    assert_eq!(
        guarded_apply(&other, "p", &fake).await,
        Err(PluginCallError::NotSent)
    );
}

// DESIGN §3.2: the time-limit rule applies to every call kind (key, schema, apply,
// remove, both hand-offs); the guard result is the same for each.
#[tokio::test(start_paused = true)]
async fn every_call_kind_is_guarded() {
    for kind in ALL_KINDS {
        let guard = guard(1);
        let plugin_id = format!("plugin-{kind:?}");

        let ok = guard
            .call(&plugin_id, meta(kind), async { Ok::<u8, String>(7) })
            .await;
        assert_eq!(ok, Ok(7), "{kind:?}");

        let started = Instant::now();
        let timed_out = guard
            .call(
                &plugin_id,
                meta(kind),
                std::future::pending::<Result<u8, String>>(),
            )
            .await;
        assert_eq!(timed_out, Err(PluginCallError::TimedOut), "{kind:?}");
        assert_eq!(started.elapsed(), TIMEOUT, "{kind:?}");

        let refused = guard
            .call(&plugin_id, meta(kind), async { Ok::<u8, String>(7) })
            .await;
        assert_eq!(refused, Err(PluginCallError::NotSent), "{kind:?}");
    }
}

#[test]
fn call_kinds_belong_to_their_plugin_kind() {
    let table = [
        (CallKind::Key, PluginKind::Section),
        (CallKind::Schema, PluginKind::Section),
        (CallKind::Apply, PluginKind::Section),
        (CallKind::Remove, PluginKind::Section),
        (CallKind::RequestHandoff, PluginKind::Audit),
        (CallKind::FinalResultHandoff, PluginKind::Audit),
    ];
    for (kind, plugin_kind) in table {
        assert_eq!(kind.plugin_kind(), plugin_kind, "{kind:?}");
    }
}

// DESIGN §3.3: `key` runs at startup, so there is no request id.
#[tokio::test(start_paused = true)]
async fn meta_without_request_id_is_accepted() {
    let guard = guard(1);
    let meta = CallMeta {
        request_id: None,
        call_kind: CallKind::Key,
        section_key: None,
    };
    let result = guard
        .call("p", meta, async { Ok::<_, String>("netw") })
        .await;
    assert_eq!(result, Ok("netw"));
}

// DESIGN §3.3: a cap refusal of a hand-off is not accepted.
#[tokio::test(start_paused = true)]
async fn audit_handoff_over_cap_is_not_sent() {
    let guard = guard(1);
    let audit = Arc::new(FakeAuditPlugin::new());
    audit.enqueue(AuditCall::RequestHandoff, CallBehavior::hang());
    drop(guarded_request_handoff(&guard, "audit", &audit).await);
    let refused = guarded_request_handoff(&guard, "audit", &audit).await;
    assert_eq!(refused, Err(PluginCallError::NotSent));
    assert_eq!(audit.call_count(), 1);
}

// DESIGN §4.7: `plugin_max_in_flight` is validated to 1..=1_000_000 at init, so a zero
// cap here is a programming error.
#[test]
#[should_panic(expected = "plugin_max_in_flight")]
fn new_rejects_a_zero_cap() {
    drop(PluginCallGuard::new(TIMEOUT, 0));
}

// DESIGN §3.2: "The late result is logged by metadata only ... and discarded." Exactly
// once, with the metadata fields and none of the plugin's detail or section value.
#[tokio::test(start_paused = true)]
async fn late_result_is_logged_once_by_metadata_only() {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::internal("SENTINEL-DETAIL"))
            .with_delay(Duration::from_secs(2)),
    );
    let fake_for_call = Arc::clone(&fake);
    let result = guard
        .call(
            "p",
            CallMeta {
                request_id: Some(Uuid::from_u128(3)),
                call_kind: CallKind::Apply,
                section_key: Some("netw".to_owned()),
            },
            async move {
                let caller = caller();
                let envelope = envelope();
                fake_for_call
                    .apply(&caller, &envelope, json!({"secret": "SENTINEL-VALUE"}))
                    .await
            },
        )
        .await;
    assert_eq!(result, Err(PluginCallError::TimedOut));
    assert!(
        logs.lines_with(LATE).is_empty(),
        "nothing logged before the late result"
    );

    tokio::time::sleep(Duration::from_secs(3)).await;
    let lines = logs.lines_with(LATE);
    assert_eq!(lines.len(), 1, "exactly one late-result event: {lines:?}");
    let line = &lines[0];
    assert!(
        has_field(line, "request_id", &Uuid::from_u128(3).to_string()),
        "{line}"
    );
    assert!(has_field(line, "plugin_kind", "section"), "{line}");
    assert!(has_field(line, "call_kind", "apply"), "{line}");
    assert!(has_field(line, "section_key", "netw"), "{line}");
    assert!(has_field(line, "elapsed_ms", "2000"), "{line}");
    assert!(has_field(line, "result_kind", "error"), "{line}");
    assert!(
        !logs.text().contains("SENTINEL"),
        "content leaked: {}",
        logs.text()
    );
}

#[tokio::test(start_paused = true)]
async fn late_result_logs_error_and_panic_kinds() {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(3);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::panic_with("SENTINEL-PANIC").with_delay(Duration::from_secs(2)),
    );
    drop(guarded_apply(&guard, "p", &fake).await);
    drop(guarded_apply(&guard, "p", &fake).await);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let lines = logs.lines_with(LATE);
    assert_eq!(lines.len(), 2, "{lines:?}");
    assert!(
        lines.iter().any(|l| has_field(l, "result_kind", "ok")),
        "{lines:?}"
    );
    assert!(
        lines.iter().any(|l| has_field(l, "result_kind", "panic")),
        "{lines:?}"
    );
    assert!(!logs.text().contains("SENTINEL"));
}

#[tokio::test(start_paused = true)]
async fn result_within_the_limit_logs_no_late_event() {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(1);
    let fake = section("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_millis(100)),
    );
    assert_eq!(guarded_apply(&guard, "p", &fake).await, Ok(()));
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(logs.lines_with(LATE).is_empty(), "{}", logs.text());
}

// DESIGN §3.2 `drain(deadline)`; the deadline is the stop token (user decision F-009).
#[tokio::test(start_paused = true)]
async fn drain_returns_zero_at_once_when_idle() {
    let guard = guard(2);
    let started = Instant::now();
    assert_eq!(guard.drain(&CancellationToken::new()).await, 0);
    assert_eq!(started.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn drain_returns_zero_when_detached_calls_finish_in_time() {
    let guard = guard(2);
    let fake = section("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    drop(guarded_apply(&guard, "p", &fake).await); // detached after 500ms
    let started = Instant::now();
    let deadline = cancel_after(Duration::from_secs(10));
    assert_eq!(guard.drain(&deadline).await, 0);
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(1500),
        "waits only until the last detached call ends"
    );
    assert_eq!(fake.completed(), 1);
}

#[tokio::test(start_paused = true)]
async fn drain_counts_hung_calls_of_one_plugin() {
    let guard = guard(2);
    let fake = section("k");
    fake.on(SectionCall::Apply, CallBehavior::hang());
    drop(guarded_apply(&guard, "a", &fake).await);
    drop(guarded_apply(&guard, "a", &fake).await);
    let started = Instant::now();
    let deadline = cancel_after(Duration::from_secs(1));
    assert_eq!(guard.drain(&deadline).await, 2);
    assert_eq!(started.elapsed(), Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn drain_sums_across_plugins() {
    let guard = guard(2);
    let hung_a = section("a");
    hung_a.on(SectionCall::Apply, CallBehavior::hang());
    let hung_b = section("b");
    hung_b.on(SectionCall::Apply, CallBehavior::hang());
    drop(guarded_apply(&guard, "a", &hung_a).await);
    drop(guarded_apply(&guard, "a", &hung_a).await);
    drop(guarded_apply(&guard, "b", &hung_b).await);
    let deadline = cancel_after(Duration::from_secs(1));
    assert_eq!(guard.drain(&deadline).await, 3);
}

// One call finishes at 1337ms, one never does; the deadline is 5s.
#[tokio::test(start_paused = true)]
async fn drain_mixed_finished_and_hung_calls() {
    let guard = guard(2);
    let finishing = section("a");
    finishing.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_millis(1337)),
    );
    let hung = section("b");
    hung.on(SectionCall::Apply, CallBehavior::hang());
    let (first, second) = tokio::join!(
        guarded_apply(&guard, "a", &finishing),
        guarded_apply(&guard, "b", &hung),
    );
    assert_eq!(first, Err(PluginCallError::TimedOut));
    assert_eq!(second, Err(PluginCallError::TimedOut));
    let started = Instant::now();
    let deadline = cancel_after(Duration::from_secs(5));
    assert_eq!(guard.drain(&deadline).await, 1);
    assert_eq!(started.elapsed(), Duration::from_secs(5));
    assert_eq!(guard.in_flight("a"), 0, "the finished call freed its slot");
    assert_eq!(guard.in_flight("b"), 1);
}

#[tokio::test(start_paused = true)]
async fn drain_with_a_cancelled_token_returns_at_once() {
    let guard = guard(1);
    let fake = section("k");
    fake.on(SectionCall::Apply, CallBehavior::hang());
    drop(guarded_apply(&guard, "p", &fake).await);
    let deadline = CancellationToken::new();
    deadline.cancel();
    let started = Instant::now();
    assert_eq!(guard.drain(&deadline).await, 1);
    assert_eq!(started.elapsed(), Duration::ZERO);
}

// DESIGN §3.2: "logs their count per plugin".
#[tokio::test(start_paused = true)]
async fn drain_logs_abandoned_count_per_plugin() {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(2);
    let hung_a = section("a");
    hung_a.on(SectionCall::Apply, CallBehavior::hang());
    let hung_b = section("b");
    hung_b.on(SectionCall::Apply, CallBehavior::hang());
    drop(guarded_apply(&guard, "plugin-a", &hung_a).await);
    drop(guarded_apply(&guard, "plugin-a", &hung_a).await);
    drop(guarded_apply(&guard, "plugin-b", &hung_b).await);
    let deadline = cancel_after(Duration::from_secs(1));
    assert_eq!(guard.drain(&deadline).await, 3);
    let lines = logs.lines_with(ABANDONED);
    assert_eq!(lines.len(), 2, "one event per plugin: {lines:?}");
    assert!(
        lines
            .iter()
            .any(|l| has_field(l, "plugin_id", "plugin-a") && has_field(l, "count", "2")),
        "{lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|l| has_field(l, "plugin_id", "plugin-b") && has_field(l, "count", "1")),
        "{lines:?}"
    );
}

// DESIGN §3.2: detached calls never take the shutdown token; the guard cancels nothing.
#[tokio::test(start_paused = true)]
async fn drain_cancels_nothing() {
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(5)),
    );
    drop(guarded_apply(&guard, "p", &fake).await);
    let deadline = cancel_after(Duration::from_secs(1));
    assert_eq!(guard.drain(&deadline).await, 1);
    assert_eq!(guard.in_flight("p"), 1, "abandoned, not cancelled");
    tokio::time::sleep(YEAR).await;
    assert_eq!(
        fake.completed(),
        1,
        "the abandoned call still ran to completion"
    );
}

// F-001: the result was delivered to the receiver, but the caller future is dropped
// before it polled it. The receiver guard logs the late result, exactly once.
#[tokio::test(start_paused = true)]
async fn result_delivered_but_never_polled_is_logged_once_on_drop() {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_millis(100)),
    );
    {
        let mut call = pin!(guarded_apply(&guard, "p", &fake));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(call.as_mut().poll(&mut cx).is_pending());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(guard.in_flight("p"), 0, "the plugin has finished");
        assert!(
            logs.lines_with(LATE).is_empty(),
            "the supervisor's send succeeded, nothing logged yet"
        );
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let lines = logs.lines_with(LATE);
    assert_eq!(lines.len(), 1, "exactly one late-result event: {lines:?}");
    assert!(has_field(&lines[0], "result_kind", "ok"), "{}", lines[0]);
    assert!(has_field(&lines[0], "call_kind", "apply"), "{}", lines[0]);
}

// F-011: when the result and the timeout are ready at the same poll, the result wins.
#[tokio::test(start_paused = true)]
async fn plugin_finishing_exactly_at_the_timeout_returns_the_result() {
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(SectionCall::Apply, CallBehavior::ok().with_delay(TIMEOUT));
    assert_eq!(guarded_apply(&guard, "p", &fake).await, Ok(()));
}

// F-004: events emitted inside the guarded future keep the caller's span as parent.
async fn event_inside_guarded_call(delay: Duration, wait_after: Duration) -> String {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(1);
    let span = tracing::info_span!("caller_span");
    drop(
        tracing::Instrument::instrument(
            async move {
                guard
                    .call("p", meta(CallKind::Apply), async move {
                        tokio::time::sleep(delay).await;
                        tracing::info!("INSIDE_EVENT");
                        Ok::<(), ()>(())
                    })
                    .await
            },
            span,
        )
        .await,
    );
    tokio::time::sleep(wait_after).await;
    let lines = logs.lines_with("INSIDE_EVENT");
    assert_eq!(lines.len(), 1, "{lines:?}");
    lines[0].clone()
}

#[tokio::test(start_paused = true)]
async fn guarded_future_events_have_the_caller_span_on_the_normal_path() {
    let line = event_inside_guarded_call(Duration::from_millis(10), Duration::ZERO).await;
    assert!(line.contains("caller_span"), "{line}");
}

#[tokio::test(start_paused = true)]
async fn guarded_future_events_have_the_caller_span_after_a_timeout() {
    let line = event_inside_guarded_call(Duration::from_secs(2), Duration::from_secs(3)).await;
    assert!(line.contains("caller_span"), "{line}");
}

// F-005: a cancelled inner task (runtime shutdown) is not reported as a panic. Dropping
// a runtime with a hung call cannot show it deterministically: the supervisor is torn
// down together with the inner task and never logs. The classification is tested directly.
#[tokio::test]
async fn join_error_classification_separates_panic_from_cancellation() {
    use crate::infra::call_guard::Finished;
    let panicked = tokio::spawn(async { panic!("boom") }).await;
    let panicked: Finished<(), ()> = Finished::from_join(panicked);
    assert_eq!(panicked.result_kind(), "panic");
    let task = tokio::spawn(std::future::pending::<Result<(), ()>>());
    task.abort();
    let cancelled = Finished::from_join(task.await);
    assert_eq!(cancelled.result_kind(), "cancelled");
    let ok: Finished<(), ()> = Finished::from_join(tokio::spawn(async { Ok(()) }).await);
    assert_eq!(ok.result_kind(), "ok");
}

// F-007: `cause` says why the result was late.
#[tokio::test(start_paused = true)]
async fn late_result_after_timeout_has_cause_timeout() {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    drop(guarded_apply(&guard, "p", &fake).await);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let lines = logs.lines_with(LATE);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "cause", "timeout"), "{}", lines[0]);
}

#[tokio::test(start_paused = true)]
async fn late_result_after_caller_drop_has_cause_caller_dropped() {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_millis(100)),
    );
    {
        let mut call = pin!(guarded_apply(&guard, "p", &fake));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(call.as_mut().poll(&mut cx).is_pending());
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let lines = logs.lines_with(LATE);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "cause", "caller_dropped"),
        "{}",
        lines[0]
    );
}

#[tokio::test(start_paused = true)]
async fn delivered_but_unpolled_result_has_cause_caller_dropped() {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(1);
    let fake = section("k");
    {
        let mut call = pin!(guarded_apply(&guard, "p", &fake));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(call.as_mut().poll(&mut cx).is_pending());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let lines = logs.lines_with(LATE);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "cause", "caller_dropped"),
        "{}",
        lines[0]
    );
}

// F-010: control characters in the section key are escaped in the log.
#[tokio::test(start_paused = true)]
async fn section_key_with_control_characters_is_logged_escaped() {
    let (logs, _subscriber) = capture_logs();
    let guard = guard(1);
    let fake = section("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    let fake_for_call = Arc::clone(&fake);
    drop(
        guard
            .call(
                "p",
                CallMeta {
                    request_id: None,
                    call_kind: CallKind::Apply,
                    section_key: Some("a\nb".to_owned()),
                },
                async move {
                    let caller = caller();
                    let envelope = envelope();
                    fake_for_call.apply(&caller, &envelope, Value::Null).await
                },
            )
            .await,
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    let text = logs.text();
    assert_eq!(text.lines().count(), 1, "raw newline in log: {text:?}");
    assert!(text.contains(r"section_key=a\nb"), "{text}");
}
