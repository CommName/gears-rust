//! Unit tests for the fake section and audit plugins (DESIGN §4.8, §3.3).
//!
//! Planned API in `crate::test_support` (cfg(test) only):
//!
//! ```text
//! pub struct CallBehavior<E>;   // fields private
//! impl<E> CallBehavior<E> {
//!     pub fn ok() -> Self;  pub fn err(e: E) -> Self;  pub fn hang() -> Self; // never returns
//!     pub fn panic() -> Self;  pub fn panic_with(message: &str) -> Self;
//!     pub fn with_delay(self, delay: Duration) -> Self;  // tokio sleep before acting
//! }
//!
//! pub enum SectionCall { Key, Schema, Apply, Remove }  // Debug, Clone, Copy, PartialEq, Eq
//! #[derive(Debug, Clone, PartialEq)]
//! pub struct SectionCallRecord {                       // full clones of what was received
//!     pub kind: SectionCall,
//!     pub caller: Option<CallerIdentity>,              // None for `key`
//!     pub envelope: Option<ApplicationEnvelope>,       // apply / remove
//!     pub section: Option<Value>,                      // apply
//! }
//! pub struct FakeSectionPlugin;  // impl SectionPluginClientV1; Send + Sync
//! impl FakeSectionPlugin {
//!     pub fn new(key: &str) -> Self;                   // every call Ok at once;
//!                                                      // schema {"type":"object"}
//!     pub fn with_schema(self, schema: Value) -> Self;
//!     pub fn on(&self, call: SectionCall, b: CallBehavior<SectionPluginError>);       // persists
//!     pub fn enqueue(&self, call: SectionCall, b: CallBehavior<SectionPluginError>);  // one call
//!     pub fn calls(&self) -> Vec<SectionCallRecord>;   // appended on entry, in order
//!     pub fn call_count(&self) -> usize;
//!     pub fn completed(&self) -> usize;                // returned Ok or Err; not panic / hang
//! }
//!
//! pub enum AuditCall { RequestHandoff, FinalResultHandoff }
//! #[derive(Debug, Clone, PartialEq)]
//! pub struct AuditCallRecord {
//!     pub kind: AuditCall,
//!     pub caller: CallerIdentity,
//!     pub request: Option<RequestHandoffContent>,     // request hand-off
//!     pub audit_id: Option<String>,                   // final-result hand-off argument
//!     pub result: Option<FinalResultContent>,         // final-result hand-off
//! }
//! pub struct FakeAuditPlugin;    // impl AuditPluginClientV1; Default
//! impl FakeAuditPlugin {         // new(), with_audit_id(&str) (default "audit-1"),
//!     // on(AuditCall, CallBehavior<AuditPluginError>), enqueue(..), calls(), call_count(),
//!     // completed()  -- same semantics as the section fake
//! }
//! ```
//!
//! `enqueue` behaviours of a call kind run before `on` / default behaviour of that
//! kind, one per call, in order; other kinds are unaffected.
//!
//! Section fake:
//! - `section_defaults_succeed_at_once`: key, schema (default and configured), apply, remove.
//! - `section_each_error_kind_is_returned`: all three error kinds, every call kind.
//! - `section_on_persists_across_calls`: `on` is not one-shot.
//! - `section_enqueued_behaviors_are_consumed_in_order_then_default`.
//! - `section_enqueue_overrides_on_then_on_resumes`: precedence.
//! - `section_enqueue_is_per_call_kind`.
//! - `section_behaviors_are_per_call_kind`.
//! - `section_completed_counts_ok_and_err_calls`.
//! - `section_delay_holds_the_call_for_the_configured_time`.
//! - `section_hang_never_returns_for_every_call_kind`: pending after 365 days.
//! - `section_panic_unwinds_and_is_logged`, `section_panic_with_carries_the_message`.
//! - `section_log_records_full_arguments_in_order`.
//! - `section_fake_is_arc_dyn_client`.
//!
//! Audit fake:
//! - `audit_defaults_accept_with_audit_id`, `audit_each_error_kind_is_returned`,
//!   `audit_on_persists_across_calls`, `audit_enqueued_behaviors_are_consumed_in_order_then_default`,
//!   `audit_enqueue_overrides_on_then_on_resumes`, `audit_behaviors_are_per_call_kind`,
//!   `audit_completed_counts_ok_and_err_calls`, `audit_delay_holds_the_call_for_the_configured_time`,
//!   `audit_hang_never_returns_for_both_hand_offs`, `audit_panic_unwinds_and_is_logged`,
//!   `audit_log_records_distinct_ids_per_source`, `audit_fake_is_arc_dyn_client`.

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::models::{HandoffOperation, OutcomeCategory, RegistryEffect};
use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginClientV1, AuditPluginError, CallerIdentity, FinalResultContent,
    RequestHandoffContent, SectionPluginClientV1, SectionPluginError,
};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uuid::Uuid;

use crate::test_support::{
    AuditCall, AuditCallRecord, CallBehavior, FakeAuditPlugin, FakeSectionPlugin, SectionCall,
    SectionCallRecord,
};

const YEAR: Duration = Duration::from_hours(365 * 24);

fn caller(request_id: u128) -> CallerIdentity {
    CallerIdentity {
        subject_id: Uuid::from_u128(100),
        subject_type: Some("user".to_owned()),
        subject_tenant_id: Uuid::from_u128(200),
        token_scopes: vec!["scope-a".to_owned()],
        request_id: Uuid::from_u128(request_id),
    }
}

fn envelope(id: &str) -> ApplicationEnvelope {
    ApplicationEnvelope {
        id: id.to_owned(),
        version: "1.0.0".to_owned(),
    }
}

fn request_content(request_id: u128) -> RequestHandoffContent {
    RequestHandoffContent {
        operation: HandoffOperation::Seed,
        request_id: Uuid::from_u128(request_id),
        received_at: "2026-10-12T00:00:00Z".to_owned(),
        application: None,
        manifest: Some(json!({"sections": {}})),
        decided_category: None,
        id_length_bytes: None,
        id_sha256: None,
    }
}

fn final_content(request_id: u128, audit_id: &str) -> FinalResultContent {
    FinalResultContent {
        audit_id: audit_id.to_owned(),
        request_id: Uuid::from_u128(request_id),
        validation: None,
        sections: vec![],
        category: OutcomeCategory::Succeeded,
        registry_effect: RegistryEffect::Unchanged,
        completed_at: "2026-10-12T00:00:01Z".to_owned(),
    }
}

fn section_errors() -> Vec<SectionPluginError> {
    vec![
        SectionPluginError::rejected("bad", Some("/a")),
        SectionPluginError::not_permitted("missing"),
        SectionPluginError::internal("boom"),
    ]
}

const SECTION_CALLS: [SectionCall; 4] = [
    SectionCall::Key,
    SectionCall::Schema,
    SectionCall::Apply,
    SectionCall::Remove,
];

// Runs one call of `kind` and reduces the result to success or failure.
async fn run_section(
    fake: &FakeSectionPlugin,
    kind: SectionCall,
) -> Result<(), SectionPluginError> {
    let caller = caller(1);
    let envelope = envelope("app");
    match kind {
        SectionCall::Key => fake.key().await.map(drop),
        SectionCall::Schema => fake.schema(&caller).await.map(drop),
        SectionCall::Apply => fake.apply(&caller, &envelope, Value::Null).await,
        SectionCall::Remove => fake.remove(&caller, &envelope).await,
    }
}

fn spawn_section(fake: &Arc<FakeSectionPlugin>, kind: SectionCall) -> JoinHandle<()> {
    let fake = Arc::clone(fake);
    tokio::spawn(async move {
        drop(run_section(&fake, kind).await);
    })
}

async fn run_audit(fake: &FakeAuditPlugin, kind: AuditCall) -> Result<(), AuditPluginError> {
    let caller = caller(1);
    match kind {
        AuditCall::RequestHandoff => {
            let content = request_content(1);
            fake.request_handoff(&caller, &content).await.map(drop)
        }
        AuditCall::FinalResultHandoff => {
            let content = final_content(1, "a");
            fake.final_result_handoff(&caller, "a", &content).await
        }
    }
}

const AUDIT_CALLS: [AuditCall; 2] = [AuditCall::RequestHandoff, AuditCall::FinalResultHandoff];

fn spawn_audit(fake: &Arc<FakeAuditPlugin>, kind: AuditCall) -> JoinHandle<()> {
    let fake = Arc::clone(fake);
    tokio::spawn(async move {
        drop(run_audit(&fake, kind).await);
    })
}

// ---------------------------------------------------------------------------------
// FakeSectionPlugin
// ---------------------------------------------------------------------------------

// DESIGN §3.3: the four section operations; default fake behaviour is success.
#[tokio::test]
async fn section_defaults_succeed_at_once() {
    let fake = FakeSectionPlugin::new("netw");
    let caller = caller(1);
    let envelope = envelope("app");
    assert_eq!(fake.key().await, Ok("netw".to_owned()));
    assert_eq!(
        fake.schema(&caller).await,
        Ok(json!({"type": "object"})),
        "default schema"
    );
    assert_eq!(
        fake.apply(&caller, &envelope, json!({"x": 1})).await,
        Ok(())
    );
    assert_eq!(fake.remove(&caller, &envelope).await, Ok(()));
    assert_eq!(fake.completed(), 4);

    let custom = FakeSectionPlugin::new("k").with_schema(json!({"type": "string"}));
    assert_eq!(custom.schema(&caller).await, Ok(json!({"type": "string"})));
}

#[tokio::test]
async fn section_each_error_kind_is_returned() {
    for err in section_errors() {
        let fake = FakeSectionPlugin::new("k");
        for call in SECTION_CALLS {
            fake.on(call, CallBehavior::err(err.clone()));
            assert_eq!(run_section(&fake, call).await, Err(err.clone()), "{call:?}");
        }
        assert_eq!(fake.call_count(), 4);
    }
}

// F-014: `on` is not one-shot.
#[tokio::test]
async fn section_on_persists_across_calls() {
    let fake = FakeSectionPlugin::new("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::internal("x")),
    );
    for _ in 0..3 {
        assert!(run_section(&fake, SectionCall::Apply).await.is_err());
    }
}

// DESIGN §4.8: per-call configuration.
#[tokio::test]
async fn section_enqueued_behaviors_are_consumed_in_order_then_default() {
    let fake = FakeSectionPlugin::new("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::internal("first")),
    );
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::not_permitted("second")),
    );
    assert_eq!(
        run_section(&fake, SectionCall::Apply).await,
        Err(SectionPluginError::internal("first"))
    );
    assert_eq!(
        run_section(&fake, SectionCall::Apply).await,
        Err(SectionPluginError::not_permitted("second"))
    );
    assert_eq!(
        run_section(&fake, SectionCall::Apply).await,
        Ok(()),
        "queue empty: falls back to the default"
    );
}

#[tokio::test]
async fn section_enqueue_overrides_on_then_on_resumes() {
    let fake = FakeSectionPlugin::new("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::internal("on")),
    );
    fake.enqueue(SectionCall::Apply, CallBehavior::ok());
    assert_eq!(run_section(&fake, SectionCall::Apply).await, Ok(()));
    assert_eq!(
        run_section(&fake, SectionCall::Apply).await,
        Err(SectionPluginError::internal("on")),
        "the persistent behaviour resumes after the queue"
    );
}

#[tokio::test]
async fn section_enqueue_is_per_call_kind() {
    let fake = FakeSectionPlugin::new("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::internal("x")),
    );
    assert_eq!(run_section(&fake, SectionCall::Remove).await, Ok(()));
    assert_eq!(run_section(&fake, SectionCall::Schema).await, Ok(()));
    assert!(run_section(&fake, SectionCall::Apply).await.is_err());
}

#[tokio::test]
async fn section_behaviors_are_per_call_kind() {
    let fake = FakeSectionPlugin::new("k");
    fake.on(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::internal("x")),
    );
    assert!(run_section(&fake, SectionCall::Apply).await.is_err());
    assert_eq!(run_section(&fake, SectionCall::Remove).await, Ok(()));
    assert_eq!(run_section(&fake, SectionCall::Key).await, Ok(()));
}

// F-015: `completed` counts a call that returned, whether Ok or Err.
#[tokio::test]
async fn section_completed_counts_ok_and_err_calls() {
    let fake = FakeSectionPlugin::new("k");
    fake.enqueue(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::internal("x")),
    );
    assert!(run_section(&fake, SectionCall::Apply).await.is_err());
    assert_eq!(fake.completed(), 1, "an Err return completes");
    assert_eq!(run_section(&fake, SectionCall::Apply).await, Ok(()));
    assert_eq!(fake.completed(), 2);
    assert_eq!(fake.call_count(), 2);
}

// DESIGN §4.8 "Clock": delays use the tokio clock, so a paused runtime auto-advances.
#[tokio::test(start_paused = true)]
async fn section_delay_holds_the_call_for_the_configured_time() {
    let fake = Arc::new(FakeSectionPlugin::new("k"));
    fake.on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_millis(300)),
    );
    let started = Instant::now();
    let task = spawn_section(&fake, SectionCall::Apply);
    tokio::time::sleep(Duration::from_millis(299)).await;
    assert_eq!(fake.call_count(), 1, "the call has started");
    assert_eq!(fake.completed(), 0, "but not finished before the delay");
    task.await.expect("no panic");
    assert_eq!(started.elapsed(), Duration::from_millis(300));
    assert_eq!(fake.completed(), 1);
}

// F-016: far beyond any plausible timeout, for every call kind.
#[tokio::test(start_paused = true)]
async fn section_hang_never_returns_for_every_call_kind() {
    let fake = Arc::new(FakeSectionPlugin::new("k"));
    for call in SECTION_CALLS {
        fake.on(call, CallBehavior::hang());
    }
    let tasks: Vec<_> = SECTION_CALLS
        .iter()
        .map(|call| spawn_section(&fake, *call))
        .collect();
    tokio::time::sleep(YEAR).await;
    for task in &tasks {
        assert!(!task.is_finished());
    }
    assert_eq!(fake.call_count(), 4);
    assert_eq!(fake.completed(), 0);
    for task in tasks {
        task.abort();
    }
}

#[tokio::test]
async fn section_panic_unwinds_and_is_logged() {
    let fake = Arc::new(FakeSectionPlugin::new("k"));
    fake.enqueue(SectionCall::Apply, CallBehavior::panic());
    let joined = spawn_section(&fake, SectionCall::Apply).await;
    assert!(joined.expect_err("task must panic").is_panic());
    assert_eq!(fake.call_count(), 1, "the call is in the log");
    assert_eq!(fake.completed(), 0, "a panicked call is not completed");
    assert_eq!(run_section(&fake, SectionCall::Remove).await, Ok(()));
}

#[tokio::test]
async fn section_panic_with_carries_the_message() {
    let fake = Arc::new(FakeSectionPlugin::new("k"));
    fake.enqueue(
        SectionCall::Remove,
        CallBehavior::panic_with("SENTINEL-PANIC"),
    );
    let joined = spawn_section(&fake, SectionCall::Remove).await;
    let payload = joined.expect_err("must panic").into_panic();
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .expect("string panic payload");
    assert!(message.contains("SENTINEL-PANIC"), "{message}");
}

// DESIGN §3.3 pass-through: everything the plugin received is observable.
#[tokio::test]
async fn section_log_records_full_arguments_in_order() {
    let fake = FakeSectionPlugin::new("k");
    let apply_envelope = ApplicationEnvelope {
        id: "app-1".to_owned(),
        version: "2.3.4".to_owned(),
    };
    drop(fake.key().await);
    drop(fake.schema(&caller(7)).await);
    drop(
        fake.apply(&caller(8), &apply_envelope, json!({"a": 1}))
            .await,
    );
    drop(fake.remove(&caller(9), &envelope("app-2")).await);
    assert_eq!(
        fake.calls(),
        vec![
            SectionCallRecord {
                kind: SectionCall::Key,
                caller: None,
                envelope: None,
                section: None
            },
            SectionCallRecord {
                kind: SectionCall::Schema,
                caller: Some(caller(7)),
                envelope: None,
                section: None
            },
            SectionCallRecord {
                kind: SectionCall::Apply,
                caller: Some(caller(8)),
                envelope: Some(apply_envelope),
                section: Some(json!({"a": 1}))
            },
            SectionCallRecord {
                kind: SectionCall::Remove,
                caller: Some(caller(9)),
                envelope: Some(envelope("app-2")),
                section: None
            },
        ]
    );
}

#[tokio::test]
async fn section_fake_is_arc_dyn_client() {
    let fake: Arc<dyn SectionPluginClientV1> = Arc::new(FakeSectionPlugin::new("dyn"));
    assert_eq!(fake.key().await, Ok("dyn".to_owned()));
}

// ---------------------------------------------------------------------------------
// FakeAuditPlugin
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn audit_defaults_accept_with_audit_id() {
    let fake = FakeAuditPlugin::new().with_audit_id("evt-9");
    let caller = caller(1);
    assert_eq!(
        fake.request_handoff(&caller, &request_content(1)).await,
        Ok("evt-9".to_owned())
    );
    assert_eq!(
        fake.final_result_handoff(&caller, "evt-9", &final_content(1, "evt-9"))
            .await,
        Ok(())
    );
    assert_eq!(
        FakeAuditPlugin::new()
            .request_handoff(&caller, &request_content(1))
            .await,
        Ok("audit-1".to_owned())
    );
    assert_eq!(fake.completed(), 2);
}

#[tokio::test]
async fn audit_each_error_kind_is_returned() {
    for err in [
        AuditPluginError::refused("no"),
        AuditPluginError::unavailable("down"),
    ] {
        let fake = FakeAuditPlugin::new();
        for call in AUDIT_CALLS {
            fake.on(call, CallBehavior::err(err.clone()));
            assert_eq!(run_audit(&fake, call).await, Err(err.clone()), "{call:?}");
        }
    }
}

#[tokio::test]
async fn audit_on_persists_across_calls() {
    let fake = FakeAuditPlugin::new();
    fake.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("x")),
    );
    for _ in 0..3 {
        assert!(run_audit(&fake, AuditCall::RequestHandoff).await.is_err());
    }
}

#[tokio::test]
async fn audit_enqueued_behaviors_are_consumed_in_order_then_default() {
    let fake = FakeAuditPlugin::new();
    fake.enqueue(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("once")),
    );
    fake.enqueue(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::unavailable("twice")),
    );
    assert_eq!(
        run_audit(&fake, AuditCall::RequestHandoff).await,
        Err(AuditPluginError::refused("once"))
    );
    assert_eq!(
        run_audit(&fake, AuditCall::RequestHandoff).await,
        Err(AuditPluginError::unavailable("twice"))
    );
    assert_eq!(run_audit(&fake, AuditCall::RequestHandoff).await, Ok(()));
}

#[tokio::test]
async fn audit_enqueue_overrides_on_then_on_resumes() {
    let fake = FakeAuditPlugin::new();
    fake.on(
        AuditCall::FinalResultHandoff,
        CallBehavior::err(AuditPluginError::refused("on")),
    );
    fake.enqueue(AuditCall::FinalResultHandoff, CallBehavior::ok());
    assert_eq!(
        run_audit(&fake, AuditCall::FinalResultHandoff).await,
        Ok(())
    );
    assert_eq!(
        run_audit(&fake, AuditCall::FinalResultHandoff).await,
        Err(AuditPluginError::refused("on"))
    );
}

#[tokio::test]
async fn audit_behaviors_are_per_call_kind() {
    let fake = FakeAuditPlugin::new();
    fake.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("x")),
    );
    fake.enqueue(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::unavailable("y")),
    );
    assert_eq!(
        run_audit(&fake, AuditCall::FinalResultHandoff).await,
        Ok(())
    );
    assert!(run_audit(&fake, AuditCall::RequestHandoff).await.is_err());
}

#[tokio::test]
async fn audit_completed_counts_ok_and_err_calls() {
    let fake = FakeAuditPlugin::new();
    fake.enqueue(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("x")),
    );
    assert!(run_audit(&fake, AuditCall::RequestHandoff).await.is_err());
    assert_eq!(fake.completed(), 1);
    assert_eq!(run_audit(&fake, AuditCall::RequestHandoff).await, Ok(()));
    assert_eq!(fake.completed(), 2);
    assert_eq!(fake.call_count(), 2);
}

#[tokio::test(start_paused = true)]
async fn audit_delay_holds_the_call_for_the_configured_time() {
    let fake = FakeAuditPlugin::new();
    fake.on(
        AuditCall::RequestHandoff,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    let started = Instant::now();
    drop(run_audit(&fake, AuditCall::RequestHandoff).await);
    assert_eq!(started.elapsed(), Duration::from_secs(2));
}

#[tokio::test(start_paused = true)]
async fn audit_hang_never_returns_for_both_hand_offs() {
    let fake = Arc::new(FakeAuditPlugin::new());
    for call in AUDIT_CALLS {
        fake.on(call, CallBehavior::hang());
    }
    let tasks: Vec<_> = AUDIT_CALLS
        .iter()
        .map(|call| spawn_audit(&fake, *call))
        .collect();
    tokio::time::sleep(YEAR).await;
    for task in &tasks {
        assert!(!task.is_finished());
    }
    assert_eq!(fake.call_count(), 2);
    assert_eq!(fake.completed(), 0);
    for task in tasks {
        task.abort();
    }
}

#[tokio::test]
async fn audit_panic_unwinds_and_is_logged() {
    let fake = Arc::new(FakeAuditPlugin::new());
    fake.on(AuditCall::FinalResultHandoff, CallBehavior::panic());
    let joined = spawn_audit(&fake, AuditCall::FinalResultHandoff).await;
    assert!(joined.expect_err("must panic").is_panic());
    assert_eq!(fake.call_count(), 1);
    assert_eq!(fake.completed(), 0);
}

// F-017: every source differs, so a record built from the wrong source fails.
#[tokio::test]
async fn audit_log_records_distinct_ids_per_source() {
    let fake = FakeAuditPlugin::new();
    let request = request_content(5);
    let result = final_content(6, "audit-from-result");
    drop(fake.request_handoff(&caller(4), &request).await);
    drop(
        fake.final_result_handoff(&caller(7), "audit-from-argument", &result)
            .await,
    );
    assert_eq!(
        fake.calls(),
        vec![
            AuditCallRecord {
                kind: AuditCall::RequestHandoff,
                caller: caller(4),
                request: Some(request),
                audit_id: None,
                result: None,
            },
            AuditCallRecord {
                kind: AuditCall::FinalResultHandoff,
                caller: caller(7),
                request: None,
                audit_id: Some("audit-from-argument".to_owned()),
                result: Some(result),
            },
        ]
    );
}

#[tokio::test]
async fn audit_fake_is_arc_dyn_client() {
    let fake: Arc<dyn AuditPluginClientV1> = Arc::new(FakeAuditPlugin::new());
    let caller = caller(1);
    assert!(
        fake.request_handoff(&caller, &request_content(1))
            .await
            .is_ok()
    );
}
