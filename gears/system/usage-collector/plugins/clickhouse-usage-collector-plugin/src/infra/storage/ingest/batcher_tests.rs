// Test modules using bare `panic!` opt in explicitly.
#![allow(clippy::panic)]

//! Batcher behaviour over the scripted [`FakeSink`], on the paused clock:
//! every timer (`max_wait`, backoff, the drain deadline) advances only when
//! every task is idle, so the tests are deterministic and instant.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use usage_collector_sdk::UsageCollectorPluginError;

use crate::infra::metrics::{DrainOutcome, Metrics};
use crate::infra::storage::ingest::ingest_tests::limits;
use crate::infra::storage::ingest::sink::test_support::{FakeSink, Step};
use crate::infra::storage::ingest::sink::{FailureKind, FlushSink};
use crate::infra::storage::ingest::test_rows::{row, row_with_value, rows};
use crate::infra::storage::ingest::{
    CallOutcome, DrainReport, FLUSH_BACKOFF_BASE, IngestLimits, IngestQueue, PendingCall, Submitted,
};
use crate::infra::storage::record_store::{InsertKind, insert_dedup_token};

/// A running queue over a fake sink.
struct Harness {
    queue: Arc<IngestQueue>,
    sink: Arc<FakeSink>,
    cancel: CancellationToken,
    batcher: JoinHandle<DrainReport>,
}

impl Harness {
    fn start(limits: IngestLimits, metrics: Arc<Metrics>) -> Self {
        let queue = IngestQueue::new(limits, metrics);
        let sink = FakeSink::new();
        let cancel = CancellationToken::new();
        let dyn_sink: Arc<dyn FlushSink> = Arc::clone(&sink) as Arc<dyn FlushSink>;
        let batcher = tokio::spawn(Arc::clone(&queue).run(dyn_sink, cancel.clone()));
        Self {
            queue,
            sink,
            cancel,
            batcher,
        }
    }

    fn with_defaults() -> Self {
        Self::start(limits(), Arc::new(Metrics::new()))
    }

    fn submit(&self, rows: Vec<crate::infra::storage::entity::UsageRecordRow>) -> Submitted {
        self.queue
            .submit(PendingCall { rows })
            .expect("within bounds")
    }

    /// Cancel and await the batcher's report.
    async fn stop(self) -> DrainReport {
        self.cancel.cancel();
        self.batcher.await.expect("the batcher task completes")
    }
}

/// Let every runnable task run and the paused clock pass one `max_wait`, so
/// whatever was queued has been cut (and parked in the sink when gated).
async fn settle() {
    tokio::time::sleep(limits().max_wait + Duration::from_millis(10)).await;
}

/// A generous wait: the batcher is driven by the paused clock, so this only
/// bounds a genuinely stuck test.
const BUDGET: Duration = Duration::from_secs(60);

fn all_ok(outcome: &CallOutcome) -> bool {
    outcome.rows.iter().all(Result::is_ok)
}

fn all_transient(outcome: &CallOutcome) -> bool {
    outcome
        .rows
        .iter()
        .all(|r| matches!(r, Err(UsageCollectorPluginError::Transient { .. })))
}

// ── sharing a flush ──────────────────────────────────────────────────────────

/// Three concurrent calls under the row limit share one flush `INSERT`, in
/// FIFO order, and each receives its own per-row outcomes.
#[tokio::test(start_paused = true)]
async fn concurrent_calls_share_one_flush_and_get_per_row_outcomes() {
    let h = Harness::with_defaults();
    let a = h.submit(rows(0, 1));
    let b = h.submit(rows(10, 2));
    let c = h.submit(rows(20, 1));
    let (a, b, c) = tokio::join!(a.wait(BUDGET), b.wait(BUDGET), c.wait(BUDGET));
    let (a, b, c) = (a.unwrap(), b.unwrap(), c.unwrap());
    assert_eq!((a.rows.len(), b.rows.len(), c.rows.len()), (1, 2, 1));
    assert!(all_ok(&a) && all_ok(&b) && all_ok(&c));

    let calls = h.sink.calls();
    assert_eq!(calls.len(), 1, "one INSERT for the three calls");
    let ids: Vec<_> = calls[0].rows.iter().map(|r| r.id).collect();
    assert_eq!(ids, vec![row(0).id, row(10).id, row(11).id, row(20).id]);
    assert_eq!(
        calls[0].token,
        insert_dedup_token(&calls[0].rows, InsertKind::Record),
        "the token is computed over exactly the flushed rows"
    );
    let report = h.stop().await;
    assert_eq!(report.outcome, DrainOutcome::Drained);
    assert_eq!(report.flushed_calls, 3);
}

/// No caller is answered before the sink's INSERT returns.
#[tokio::test(start_paused = true)]
async fn no_outcome_before_the_sink_completes() {
    let h = Harness::with_defaults();
    let gate = Arc::new(Notify::new());
    h.sink.script([Step::Gate(Arc::clone(&gate))]);
    let waiter = tokio::spawn(h.submit(rows(0, 1)).wait(BUDGET));
    // Past `max_wait` the flush has been cut and is parked in the sink; the
    // waiter must still be pending.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(h.sink.call_count(), 1, "the flush reached the sink");
    assert!(
        !waiter.is_finished(),
        "no outcome before the INSERT completes"
    );
    assert_eq!(h.queue.snapshot().in_flight_rows, 1);
    gate.notify_one();
    let outcome = waiter.await.unwrap().unwrap();
    assert!(all_ok(&outcome));
    assert_eq!(h.queue.snapshot().in_flight_rows, 0);
    h.stop().await;
}

// ── triggers ─────────────────────────────────────────────────────────────────

/// Under low traffic the timer flushes at exactly `max_wait` after the oldest
/// call; continuous arrivals every 10 ms do not postpone it.
#[tokio::test(start_paused = true)]
async fn timer_flush_fires_at_max_wait_from_the_oldest_call_under_continuous_arrivals() {
    let h = Harness::with_defaults(); // max_wait 50 ms, max_rows 4
    let t0 = tokio::time::Instant::now();
    let mut waiters = vec![tokio::spawn(h.submit(rows(0, 1)).wait(BUDGET))];
    for i in 1..3_u128 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        waiters.push(tokio::spawn(h.submit(rows(i * 10, 1)).wait(BUDGET)));
    }
    for w in waiters {
        assert!(all_ok(&w.await.unwrap().unwrap()));
    }
    let calls = h.sink.calls();
    assert_eq!(calls.len(), 1, "every arrival rode the oldest call's timer");
    assert_eq!(
        calls[0].at,
        t0 + Duration::from_millis(50),
        "the cut happens at max_wait from the oldest call, not the newest"
    );
    assert_eq!(calls[0].rows.len(), 3);

    // Low traffic: one lone call is flushed at max_wait as well.
    let t1 = tokio::time::Instant::now();
    let lone = h.submit(rows(100, 1)).wait(BUDGET).await.unwrap();
    assert!(all_ok(&lone));
    assert_eq!(h.sink.calls()[1].at, t1 + Duration::from_millis(50));
    h.stop().await;
}

/// Reaching `max_rows` cuts a flush at once, without waiting for the timer.
#[tokio::test(start_paused = true)]
async fn size_trigger_cuts_at_max_rows_without_waiting() {
    let h = Harness::with_defaults(); // max_rows 4
    let t0 = tokio::time::Instant::now();
    let big = h.submit(rows(0, 4)).wait(BUDGET).await.unwrap();
    assert!(all_ok(&big));
    assert_eq!(
        h.sink.calls()[0].at,
        t0,
        "no timer wait once max_rows is reached"
    );
    h.stop().await;
}

/// Reaching `max_bytes` cuts a flush at once too.
#[tokio::test(start_paused = true)]
async fn size_trigger_cuts_at_max_bytes_without_waiting() {
    let mut l = limits();
    l.max_bytes = 1; // any row exceeds it
    l.max_outstanding_bytes = usize::MAX;
    let h = Harness::start(l, Arc::new(Metrics::new()));
    let t0 = tokio::time::Instant::now();
    let outcome = h.submit(rows(0, 1)).wait(BUDGET).await.unwrap();
    assert!(all_ok(&outcome));
    assert_eq!(h.sink.calls()[0].at, t0);
    h.stop().await;
}

// ── bounds and budgets ───────────────────────────────────────────────────────

/// A call whose budget runs out while its rows are queued gets `Transient`;
/// its rows are dropped at the next cut and never reach the sink.
#[tokio::test(start_paused = true)]
async fn queue_wait_beyond_the_budget_is_transient_and_queued_rows_are_dropped() {
    let mut l = limits();
    l.max_concurrent_flushes = 1;
    let h = Harness::start(l, Arc::new(Metrics::new()));
    // Occupy the only flush permit with a gated flush.
    let gate = Arc::new(Notify::new());
    h.sink.script([Step::Gate(Arc::clone(&gate))]);
    let blocked = tokio::spawn(h.submit(rows(0, 4)).wait(BUDGET));
    settle().await;
    assert_eq!(h.sink.call_count(), 1);

    // The next call cannot be cut (no permit) and times out while queued.
    let err = h
        .submit(rows(10, 1))
        .wait(Duration::from_millis(5))
        .await
        .unwrap_err();
    assert!(matches!(err, UsageCollectorPluginError::Transient { .. }));
    assert_eq!(
        h.queue.snapshot().queued_rows,
        1,
        "still queued until the next cut"
    );

    gate.notify_one();
    assert!(all_ok(&blocked.await.unwrap().unwrap()));
    settle().await;
    assert_eq!(
        h.sink.call_count(),
        1,
        "the abandoned row was dropped, not flushed"
    );
    assert_eq!(h.queue.snapshot().queued_rows, 0);
    assert_eq!(h.queue.snapshot().coalesce_entries, 0);
    h.stop().await;
}

// ── retries ──────────────────────────────────────────────────────────────────

/// A transient failure is retried with the identical rows, order and token,
/// after a jittered backoff, and the waiters succeed on the retry.
#[tokio::test(start_paused = true)]
async fn one_token_per_flush_and_retries_reuse_rows_order_and_token() {
    let h = Harness::with_defaults();
    h.sink
        .script([Step::Fail(FailureKind::Transient), Step::Ok]);
    let outcome = h.submit(rows(0, 3)).wait(BUDGET).await.unwrap();
    assert!(all_ok(&outcome));
    let calls = h.sink.calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].token, calls[1].token);
    let ids = |c: &super::super::sink::test_support::Recorded| {
        c.rows.iter().map(|r| (r.id, r.version)).collect::<Vec<_>>()
    };
    assert_eq!(
        ids(&calls[0]),
        ids(&calls[1]),
        "same rows, order and versions"
    );
    let delay = calls[1].at.saturating_duration_since(calls[0].at);
    assert!(
        delay >= FLUSH_BACKOFF_BASE.mul_f64(0.75) && delay <= FLUSH_BACKOFF_BASE.mul_f64(1.25),
        "first backoff is base ±25 %, got {delay:?}"
    );
    h.stop().await;
}

/// After the last attempt fails, every waiter of the flush gets `Transient`.
#[tokio::test(start_paused = true)]
async fn exhausted_transient_retries_reach_every_waiter() {
    let h = Harness::with_defaults();
    h.sink.script([
        Step::Fail(FailureKind::Transient),
        Step::Fail(FailureKind::Transient),
        Step::Fail(FailureKind::Transient),
    ]);
    let a = h.submit(rows(0, 1));
    let b = h.submit(rows(10, 1));
    let (a, b) = tokio::join!(a.wait(BUDGET), b.wait(BUDGET));
    assert!(all_transient(&a.unwrap()));
    assert!(all_transient(&b.unwrap()));
    assert_eq!(h.sink.call_count(), 3, "bounded attempts");
    h.stop().await;
}

/// No retry while draining: a transient failure after cancellation is
/// reported at once so the drain fits its budget.
#[tokio::test(start_paused = true)]
async fn retry_is_skipped_in_drain_mode() {
    let h = Harness::with_defaults();
    h.sink.script([Step::Fail(FailureKind::Transient)]);
    let waiter = tokio::spawn(h.submit(rows(0, 1)).wait(BUDGET));
    h.cancel.cancel();
    let outcome = waiter.await.unwrap().unwrap();
    assert!(all_transient(&outcome));
    assert_eq!(h.sink.call_count(), 1, "no retry in drain mode");
    let report = h.batcher.await.unwrap();
    assert_eq!(report.outcome, DrainOutcome::Drained);
}

/// No retry once every waiter is gone: a transient failure of a flush whose
/// callers all left is abandoned, not retried.
#[tokio::test(start_paused = true)]
async fn retry_is_abandoned_when_no_live_waiter_remains() {
    let h = Harness::with_defaults();
    let gate = Arc::new(Notify::new());
    h.sink.script([Step::GateThenFail(
        Arc::clone(&gate),
        FailureKind::Transient,
    )]);
    let submitted = h.submit(rows(0, 1));
    settle().await;
    assert_eq!(h.sink.call_count(), 1, "the flush is in flight");
    drop(submitted);
    gate.notify_one();
    settle().await;
    assert_eq!(h.sink.call_count(), 1, "no retry for an abandoned flush");
    let snap = h.queue.snapshot();
    assert_eq!((snap.in_flight_rows, snap.coalesce_entries), (0, 0));
    h.stop().await;
}

// ── isolation ────────────────────────────────────────────────────────────────

/// A row-attributable failure of a combined flush retries each call once on
/// its own, under the call's own token; the bad call fails, the rest succeed,
/// and nothing is retried further.
#[tokio::test(start_paused = true)]
async fn row_attributable_failure_isolates_each_call_once_without_recursion() {
    let h = Harness::with_defaults();
    h.sink.script([
        Step::Fail(FailureKind::RowAttributable),
        Step::Ok,
        Step::Fail(FailureKind::RowAttributable),
        Step::Ok,
    ]);
    let a = h.submit(rows(0, 1));
    let b = h.submit(rows(10, 2));
    let c = h.submit(rows(20, 1));
    let (a, b, c) = tokio::join!(a.wait(BUDGET), b.wait(BUDGET), c.wait(BUDGET));
    let (a, b, c) = (a.unwrap(), b.unwrap(), c.unwrap());
    assert!(all_ok(&a));
    assert!(
        b.rows
            .iter()
            .all(|r| matches!(r, Err(UsageCollectorPluginError::Internal(_)))),
        "the isolated call that failed again carries the error"
    );
    assert!(all_ok(&c));
    let calls = h.sink.calls();
    assert_eq!(
        calls.len(),
        4,
        "one combined attempt + one per call, no more"
    );
    assert_eq!(calls[1].rows.len(), 1);
    assert_eq!(calls[2].rows.len(), 2);
    assert_eq!(calls[3].rows.len(), 1);
    assert_eq!(
        calls[2].token,
        insert_dedup_token(&rows(10, 2), InsertKind::Record),
        "an isolated call carries the token a non-batched write would"
    );
    h.stop().await;
}

/// A row-attributable failure of a flush carrying a single call fans out:
/// there is nothing to isolate.
#[tokio::test(start_paused = true)]
async fn row_attributable_failure_on_a_single_call_flush_fans_out() {
    let h = Harness::with_defaults();
    h.sink.script([Step::Fail(FailureKind::RowAttributable)]);
    let outcome = h.submit(rows(0, 2)).wait(BUDGET).await.unwrap();
    assert!(
        outcome
            .rows
            .iter()
            .all(|r| matches!(r, Err(UsageCollectorPluginError::Internal(_))))
    );
    assert_eq!(h.sink.call_count(), 1);
    h.stop().await;
}

/// A schema / permission / unknown failure fans out to every waiter without
/// an extra INSERT per call.
#[tokio::test(start_paused = true)]
async fn internal_failure_fans_out_without_isolation() {
    let h = Harness::with_defaults();
    h.sink.script([Step::Fail(FailureKind::Internal)]);
    let a = h.submit(rows(0, 1));
    let b = h.submit(rows(10, 1));
    let (a, b) = tokio::join!(a.wait(BUDGET), b.wait(BUDGET));
    for outcome in [a.unwrap(), b.unwrap()] {
        assert!(
            outcome
                .rows
                .iter()
                .all(|r| matches!(r, Err(UsageCollectorPluginError::Internal(_)))),
        );
    }
    assert_eq!(
        h.sink.call_count(),
        1,
        "no per-call retry for a server-wide error"
    );
    h.stop().await;
}

// ── coalescing through the queue ─────────────────────────────────────────────

/// Two identical creates in flight share one row and one INSERT; a differing
/// payload for the same key is a conflict without touching the sink.
#[tokio::test(start_paused = true)]
async fn identical_in_flight_creates_coalesce_and_conflicting_ones_do_not() {
    let h = Harness::with_defaults();
    let gate = Arc::new(Notify::new());
    h.sink.script([Step::Gate(Arc::clone(&gate))]);
    let first = tokio::spawn(h.submit(vec![row(1)]).wait(BUDGET));
    settle().await;
    assert_eq!(h.sink.call_count(), 1, "the first row is in flight");
    let twin = tokio::spawn(h.submit(vec![row(1)]).wait(BUDGET));
    let conflict = h
        .submit(vec![row_with_value(1, 7)])
        .wait(BUDGET)
        .await
        .unwrap();
    match &conflict.rows[0] {
        Err(UsageCollectorPluginError::IdempotencyConflict { existing_id, .. }) => {
            assert_eq!(*existing_id, row(1).id);
        }
        other => panic!("expected a conflict, got {other:?}"),
    }
    gate.notify_one();
    assert!(all_ok(&first.await.unwrap().unwrap()));
    assert!(all_ok(&twin.await.unwrap().unwrap()));
    assert_eq!(
        h.sink.call_count(),
        1,
        "the twin never produced a second INSERT"
    );
    h.stop().await;
}

// ── cancellation ─────────────────────────────────────────────────────────────

/// A caller leaving while its flush is in flight does not cancel the shared
/// INSERT: the other caller still succeeds and nothing leaks.
#[tokio::test(start_paused = true)]
async fn cancelled_waiter_during_a_flush_does_not_cancel_the_shared_insert() {
    let h = Harness::with_defaults();
    let gate = Arc::new(Notify::new());
    h.sink.script([Step::Gate(Arc::clone(&gate))]);
    let leaving = h.submit(rows(0, 1));
    let staying = tokio::spawn(h.submit(rows(10, 1)).wait(BUDGET));
    settle().await;
    assert_eq!(h.sink.call_count(), 1);
    drop(leaving);
    gate.notify_one();
    assert!(all_ok(&staying.await.unwrap().unwrap()));
    assert_eq!(h.sink.calls()[0].rows.len(), 2, "both rows were written");
    let snap = h.queue.snapshot();
    assert_eq!(
        (snap.in_flight_rows, snap.queued_rows, snap.coalesce_entries),
        (0, 0, 0)
    );
    h.stop().await;
}

// ── concurrency ──────────────────────────────────────────────────────────────

/// Up to `max_concurrent_flushes` INSERTs overlap; the next waits for a
/// permit rather than running unbounded or one at a time.
#[tokio::test(start_paused = true)]
async fn flush_concurrency_is_bounded_but_not_serialised() {
    let mut lim = limits();
    lim.max_rows = 1;
    lim.max_concurrent_flushes = 2;
    let h = Harness::start(lim, Arc::new(Metrics::new()));
    let gate_a = Arc::new(Notify::new());
    let gate_b = Arc::new(Notify::new());
    h.sink.script([
        Step::Gate(Arc::clone(&gate_a)),
        Step::Gate(Arc::clone(&gate_b)),
        Step::Ok,
    ]);
    let first = tokio::spawn(h.submit(rows(0, 1)).wait(BUDGET));
    let second = tokio::spawn(h.submit(rows(10, 1)).wait(BUDGET));
    let third = tokio::spawn(h.submit(rows(20, 1)).wait(BUDGET));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        h.sink.call_count(),
        2,
        "two flushes in flight, the third waits"
    );
    assert_eq!(h.sink.max_in_flight(), 2);
    assert!(!third.is_finished());
    gate_a.notify_one();
    assert!(all_ok(&first.await.unwrap().unwrap()));
    assert!(all_ok(&third.await.unwrap().unwrap()));
    assert_eq!(h.sink.call_count(), 3);
    gate_b.notify_one();
    assert!(all_ok(&second.await.unwrap().unwrap()));
    assert_eq!(h.sink.max_in_flight(), 2, "never more than the bound");
    h.stop().await;
}

// ── shutdown ─────────────────────────────────────────────────────────────────

/// Cancellation closes admission before draining: a late submit is refused,
/// the queued work is flushed without waiting for `max_wait`, and in-flight
/// flushes are awaited.
#[tokio::test(start_paused = true)]
async fn drain_closes_admission_cuts_queued_work_and_awaits_in_flight_flushes() {
    let h = Harness::with_defaults();
    let gate = Arc::new(Notify::new());
    h.sink.script([Step::Gate(Arc::clone(&gate)), Step::Ok]);
    let in_flight = tokio::spawn(h.submit(rows(0, 1)).wait(BUDGET));
    settle().await;
    let queued = tokio::spawn(h.submit(rows(10, 1)).wait(BUDGET));
    let t_cancel = tokio::time::Instant::now();
    h.cancel.cancel();
    tokio::task::yield_now().await;
    let err = h
        .queue
        .submit(PendingCall { rows: rows(20, 1) })
        .unwrap_err();
    assert!(matches!(err, UsageCollectorPluginError::Transient { .. }));

    assert!(all_ok(&queued.await.unwrap().unwrap()));
    assert_eq!(
        h.sink.calls()[1].at,
        t_cancel,
        "queued work is cut at once on cancellation, not after max_wait"
    );
    assert!(!h.batcher.is_finished(), "the gated flush is still awaited");
    gate.notify_one();
    assert!(all_ok(&in_flight.await.unwrap().unwrap()));
    let report = h.batcher.await.unwrap();
    assert_eq!(report.outcome, DrainOutcome::Drained);
    assert_eq!(report.flushed_calls, 2);
}

/// At the drain deadline in-flight flushes are aborted and their waiters are
/// completed with `Transient`; nothing is left outstanding.
#[tokio::test(start_paused = true)]
async fn drain_deadline_aborts_in_flight_flushes_and_completes_waiters_with_transient() {
    let h = Harness::with_defaults(); // drain_deadline 5 s
    let gate = Arc::new(Notify::new()); // never released
    h.sink.script([Step::Gate(gate)]);
    let stuck = tokio::spawn(h.submit(rows(0, 1)).wait(BUDGET));
    settle().await;
    let t_cancel = tokio::time::Instant::now();
    let queue = Arc::clone(&h.queue);
    let report = h.stop().await;
    assert_eq!(report.outcome, DrainOutcome::Deadline);
    assert!(
        tokio::time::Instant::now() >= t_cancel + Duration::from_secs(5),
        "the deadline was waited out"
    );
    assert!(all_transient(&stuck.await.unwrap().unwrap()));
    let snap = queue.snapshot();
    assert_eq!(
        (snap.in_flight_rows, snap.queued_rows, snap.coalesce_entries),
        (0, 0, 0)
    );
}

/// A panic inside a flush task completes that flush's waiters with
/// `Transient`, releases its permit, and leaves the batcher running.
#[tokio::test(start_paused = true)]
async fn flush_task_panic_completes_its_waiters_and_releases_the_permit() {
    let mut l = limits();
    l.max_rows = 1;
    l.max_concurrent_flushes = 1;
    let h = Harness::start(l, Arc::new(Metrics::new()));
    h.sink.script([Step::Panic, Step::Ok]);
    let doomed = h.submit(rows(0, 1)).wait(BUDGET).await.unwrap();
    assert!(all_transient(&doomed));
    // The only permit was released: the next flush runs.
    let fine = h.submit(rows(10, 1)).wait(BUDGET).await.unwrap();
    assert!(all_ok(&fine));
    assert_eq!(h.queue.snapshot().in_flight_rows, 0);
    let report = h.stop().await;
    assert_eq!(report.outcome, DrainOutcome::Drained);
}

/// A hard abort of the batcher (the lifecycle's deadline) fails every
/// unresolved waiter and closes admission; nothing hangs.
#[tokio::test(start_paused = true)]
async fn aborting_the_batcher_fails_queued_waiters_and_closes_admission() {
    let mut l = limits();
    l.max_concurrent_flushes = 1;
    let h = Harness::start(l, Arc::new(Metrics::new()));
    let gate = Arc::new(Notify::new()); // never released
    h.sink.script([Step::Gate(gate)]);
    let in_flight = tokio::spawn(h.submit(rows(0, 1)).wait(BUDGET));
    settle().await;
    let queued = tokio::spawn(h.submit(rows(10, 1)).wait(BUDGET));
    settle().await;

    h.batcher.abort();
    assert!(h.batcher.await.unwrap_err().is_cancelled());
    assert!(all_transient(&queued.await.unwrap().unwrap()));
    assert!(all_transient(&in_flight.await.unwrap().unwrap()));
    let err = h
        .queue
        .submit(PendingCall { rows: rows(20, 1) })
        .unwrap_err();
    assert!(matches!(err, UsageCollectorPluginError::Transient { .. }));
    let snap = h.queue.snapshot();
    assert_eq!(
        (snap.in_flight_rows, snap.queued_rows, snap.coalesce_entries),
        (0, 0, 0)
    );
}

/// The outstanding gauges and flush series are recorded through a run.
#[tokio::test(start_paused = true)]
async fn batcher_records_flush_and_queue_metrics() {
    use opentelemetry::metrics::MeterProvider as _;

    use crate::infra::metrics::label;
    use crate::infra::metrics::metrics_tests::{counter_sum_with_label, local_provider};

    let (provider, exporter) = local_provider();
    let metrics = Arc::new(Metrics::with_meter(
        &provider.meter(crate::infra::metrics::SCOPE_NAME),
    ));
    let h = Harness::start(limits(), metrics);
    h.sink
        .script([Step::Fail(FailureKind::Transient), Step::Ok]);
    assert!(all_ok(&h.submit(rows(0, 2)).wait(BUDGET).await.unwrap()));
    let report = h.stop().await;
    assert_eq!(report.outcome, DrainOutcome::Drained);
    provider.force_flush().unwrap();
    assert_eq!(
        counter_sum_with_label(
            &exporter,
            "uc_clickhouse_flushes_total",
            label::OUTCOME,
            label::OUTCOME_OK
        ),
        1
    );
    assert_eq!(
        counter_sum_with_label(
            &exporter,
            "uc_clickhouse_shutdown_drain_total",
            label::OUTCOME,
            label::OUTCOME_DRAINED
        ),
        1
    );
    assert_eq!(
        counter_sum_with_label(
            &exporter,
            "uc_clickhouse_admission_rejected_total",
            label::REASON,
            label::REASON_CLOSED
        ),
        0
    );
}
