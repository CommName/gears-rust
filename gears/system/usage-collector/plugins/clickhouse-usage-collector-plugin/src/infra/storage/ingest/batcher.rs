//! The batcher loop and the flush tasks it spawns.
//!
//! One task runs [`run`]: it plans against the queue state, waits for work or
//! the oldest call's deadline, takes a flush permit, cuts a flush and spawns a
//! flush task for it. Flush tasks are bounded by a semaphore sized
//! `max_concurrent_flushes`, so several `INSERT`s overlap without the loop
//! ever blocking on one of them. On cancellation the loop closes admission,
//! cuts everything queued, awaits the flush tasks up to the drain deadline,
//! and completes whatever is left.
//!
//! Two guards make every exit path complete its waiters: [`BatcherGuard`]
//! (owned by the loop) fails every unresolved row when the loop ends for any
//! reason, and [`FlushGuard`] (owned by each flush task) fails the task's rows
//! if the task is aborted or panics before it resolved them.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinSet};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use usage_collector_sdk::UsageCollectorPluginError;

use super::sink::{FailureKind, FlushError, FlushSink};
use super::state::{Flush, Plan, RowRef};
use super::{DrainReport, FLUSH_BACKOFF_BASE, FLUSH_MAX_ATTEMPTS, Shared, deliver};
use crate::infra::metrics::{DrainOutcome, FlushOutcome};
use crate::infra::storage::record_store::{InsertKind, err_for_slot, insert_dedup_token_for};

/// Drive the queue until `cancel` fires and the drain completes.
pub(super) async fn run(
    shared: Arc<Shared>,
    sink: Arc<dyn FlushSink>,
    cancel: CancellationToken,
) -> DrainReport {
    let mut guard = BatcherGuard::new(Arc::clone(&shared));
    let semaphore = Arc::new(Semaphore::new(shared.limits.max_concurrent_flushes.max(1)));
    let mut flushes: JoinSet<()> = JoinSet::new();
    let mut drain_started: Option<Instant> = None;
    let mut flushed_calls = 0_usize;

    loop {
        let plan = shared.state.lock().plan(Instant::now(), &shared.limits);
        match plan {
            Plan::Finished => break,
            Plan::WaitUntil(deadline) => {
                wait_for_work(&shared, &cancel, &mut flushes, deadline, &mut drain_started).await;
            }
            Plan::CutNow => {
                let Some(permit) =
                    acquire_permit(&shared, &semaphore, &cancel, &mut drain_started).await
                else {
                    continue;
                };
                flushed_calls += cut_and_spawn(&shared, &sink, &mut flushes, permit);
            }
        }
    }

    let outcome = drain_tail(&shared, &mut flushes, drain_started).await;
    guard.finish(outcome);
    DrainReport {
        outcome,
        flushed_calls,
    }
}

/// Enter the draining phase: admission closes, queued work is cut without
/// waiting for `max_wait`, and the drain deadline starts.
fn begin_drain(shared: &Shared, drain_started: &mut Option<Instant>) {
    if drain_started.is_none() {
        let snapshot = {
            let mut state = shared.state.lock();
            state.close_admission();
            state.snapshot()
        };
        shared.publish(&snapshot);
        *drain_started = Some(Instant::now());
        tracing::info!(
            drain_deadline_secs = shared.limits.drain_deadline.as_secs_f64(),
            "ingest queue draining: admission closed"
        );
    }
}

/// Block until something changes the plan: an enqueue, the oldest call's
/// deadline, a flush finishing, or cancellation.
async fn wait_for_work(
    shared: &Shared,
    cancel: &CancellationToken,
    flushes: &mut JoinSet<()>,
    deadline: Option<Instant>,
    drain_started: &mut Option<Instant>,
) {
    let draining = drain_started.is_some();
    tokio::select! {
        biased;
        () = cancel.cancelled(), if !draining => begin_drain(shared, drain_started),
        Some(result) = flushes.join_next(), if !flushes.is_empty() => observe_join(result),
        () = shared.wake.notified() => {}
        () = tokio::time::sleep_until(deadline.unwrap_or_else(Instant::now)), if deadline.is_some() => {}
    }
}

/// Take a flush permit, unless cancellation arrives first (then the caller
/// re-plans in the draining phase, where the permit wait is unconditional).
async fn acquire_permit(
    shared: &Shared,
    semaphore: &Arc<Semaphore>,
    cancel: &CancellationToken,
    drain_started: &mut Option<Instant>,
) -> Option<OwnedSemaphorePermit> {
    let draining = drain_started.is_some();
    tokio::select! {
        biased;
        () = cancel.cancelled(), if !draining => {
            begin_drain(shared, drain_started);
            None
        }
        permit = Arc::clone(semaphore).acquire_owned() => permit.ok(),
    }
}

/// Cut a flush and spawn its task; returns how many calls it carries.
fn cut_and_spawn(
    shared: &Arc<Shared>,
    sink: &Arc<dyn FlushSink>,
    flushes: &mut JoinSet<()>,
    permit: OwnedSemaphorePermit,
) -> usize {
    // Reap finished tasks so the set cannot grow under sustained load.
    while let Some(result) = flushes.try_join_next() {
        observe_join(result);
    }
    let cut = shared.state.lock().cut(&shared.limits, Instant::now());
    deliver(cut.deliveries);
    for wait in cut.waits {
        shared.metrics.record_queue_wait(wait.as_secs_f64());
    }
    shared.publish(&cut.snapshot);
    let Some(flush) = cut.flush else {
        return 0;
    };
    shared
        .metrics
        .record_queue_age(flush.oldest_age.as_secs_f64());
    let calls = flush.spans.len();
    flushes.spawn(flush_task(
        Arc::clone(shared),
        Arc::clone(sink),
        flush,
        permit,
    ));
    calls
}

/// Log a flush task that ended abnormally; its [`FlushGuard`] has already
/// completed the rows.
fn observe_join(result: Result<(), JoinError>) {
    if let Err(err) = result {
        if err.is_panic() {
            tracing::error!("ingest flush task panicked; its waiters were failed");
        } else {
            tracing::debug!("ingest flush task was aborted");
        }
    }
}

/// Await the in-flight flushes once nothing is queued; abort them at the
/// drain deadline.
async fn drain_tail(
    shared: &Shared,
    flushes: &mut JoinSet<()>,
    drain_started: Option<Instant>,
) -> DrainOutcome {
    let deadline = drain_started.unwrap_or_else(Instant::now) + shared.limits.drain_deadline;
    while !flushes.is_empty() {
        tokio::select! {
            biased;
            Some(result) = flushes.join_next() => observe_join(result),
            () = tokio::time::sleep_until(deadline) => {
                tracing::warn!(
                    in_flight = flushes.len(),
                    "ingest drain deadline reached; aborting in-flight flushes"
                );
                flushes.abort_all();
                while let Some(result) = flushes.join_next().await {
                    observe_join(result);
                }
                return DrainOutcome::Deadline;
            }
        }
    }
    DrainOutcome::Drained
}

/// Completes every unresolved row when the loop ends, however it ends.
struct BatcherGuard {
    shared: Arc<Shared>,
    finished: Option<DrainOutcome>,
}

impl BatcherGuard {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            finished: None,
        }
    }

    fn finish(&mut self, outcome: DrainOutcome) {
        self.finished = Some(outcome);
    }
}

impl Drop for BatcherGuard {
    fn drop(&mut self) {
        let (deliveries, snapshot) = {
            let mut state = self.shared.state.lock();
            let deliveries = state.fail_all_unresolved("ingest queue stopped");
            state.set_closed();
            (deliveries, state.snapshot())
        };
        let stranded = deliveries.len();
        deliver(deliveries);
        self.shared.publish(&snapshot);
        let outcome = self.finished.unwrap_or(DrainOutcome::Aborted);
        self.shared.metrics.inc_shutdown_drain(outcome);
        if stranded > 0 || self.finished.is_none() {
            tracing::warn!(
                ?outcome,
                completed_with_transient = stranded,
                "ingest batcher stopped with unresolved waiters"
            );
        }
    }
}

/// Completes a flush task's rows if the task ends before it resolved them.
struct FlushGuard {
    shared: Arc<Shared>,
    refs: Vec<RowRef>,
    rows: f64,
    bytes: f64,
    started: Instant,
    armed: bool,
}

impl FlushGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for FlushGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let outcome = || {
            Err(UsageCollectorPluginError::transient(
                "ingest flush aborted before its outcome was known",
            ))
        };
        let (deliveries, snapshot) = {
            let mut state = self.shared.state.lock();
            (state.resolve_rows(&self.refs, &outcome), state.snapshot())
        };
        deliver(deliveries);
        self.shared.publish(&snapshot);
        self.shared.metrics.record_flush(
            FlushOutcome::Aborted,
            self.started.elapsed().as_secs_f64(),
            self.rows,
            self.bytes,
        );
    }
}

/// How the attempt loop ended.
enum Attempt {
    Ok,
    /// Transient, and every waiter is gone: nothing to retry for.
    Abandoned(FlushError),
    Failed(FlushError),
}

/// Write one flush: attempt (with bounded retry), then resolve its rows.
async fn flush_task(
    shared: Arc<Shared>,
    sink: Arc<dyn FlushSink>,
    flush: Flush,
    _permit: OwnedSemaphorePermit,
) {
    let started = Instant::now();
    let rows_count = as_f64(flush.rows.len());
    let bytes_count = as_f64(flush.bytes);
    let mut guard = FlushGuard {
        shared: Arc::clone(&shared),
        refs: flush.refs.clone(),
        rows: rows_count,
        bytes: bytes_count,
        started,
        armed: true,
    };

    let attempt = attempt_flush(&shared, sink.as_ref(), &flush).await;
    let (outcome, deliveries) = match attempt {
        Attempt::Ok => (
            FlushOutcome::Ok,
            shared.state.lock().resolve_rows(&flush.refs, &|| Ok(())),
        ),
        Attempt::Abandoned(err) => (
            FlushOutcome::Abandoned,
            shared
                .state
                .lock()
                .resolve_rows(&flush.refs, &|| Err(err_for_slot(&err.error))),
        ),
        Attempt::Failed(err)
            if err.kind == FailureKind::RowAttributable && flush.spans.len() > 1 =>
        {
            (
                FlushOutcome::Isolated,
                isolate(&shared, sink.as_ref(), &flush, &err).await,
            )
        }
        Attempt::Failed(err) => {
            let outcome = match err.kind {
                FailureKind::Transient => FlushOutcome::Transient,
                FailureKind::RowAttributable | FailureKind::Internal => FlushOutcome::Internal,
            };
            tracing::warn!(error = %err.error, ?outcome, calls = flush.spans.len(), rows = flush.rows.len(), "ingest flush failed; reporting to every waiter");
            (
                outcome,
                shared
                    .state
                    .lock()
                    .resolve_rows(&flush.refs, &|| Err(err_for_slot(&err.error))),
            )
        }
    };
    guard.disarm();
    deliver(deliveries);
    let snapshot = shared.state.lock().snapshot();
    shared.publish(&snapshot);
    shared.metrics.record_flush(
        outcome,
        started.elapsed().as_secs_f64(),
        rows_count,
        bytes_count,
    );
}

/// Send the flush, retrying a `Transient` failure a bounded number of times
/// with the identical payload and token — never while draining, never once
/// every waiter is gone.
async fn attempt_flush(shared: &Shared, sink: &dyn FlushSink, flush: &Flush) -> Attempt {
    let mut attempt = 1_u32;
    loop {
        match sink.insert(&flush.rows, &flush.token).await {
            Ok(()) => return Attempt::Ok,
            Err(err) if err.kind == FailureKind::Transient => {
                let (open, live) = {
                    let state = shared.state.lock();
                    (
                        state.phase() == super::state::Phase::Open,
                        state.has_live_subscriber(&flush.refs),
                    )
                };
                if !live {
                    return Attempt::Abandoned(err);
                }
                if attempt >= FLUSH_MAX_ATTEMPTS || !open {
                    return Attempt::Failed(err);
                }
                shared.metrics.inc_flush_retry();
                let delay = backoff(attempt);
                tracing::warn!(error = %err.error, attempt, delay_ms = delay.as_millis(), "ingest flush failed transiently; retrying the same flush");
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(err) => return Attempt::Failed(err),
        }
    }
}

/// Backoff before attempt `attempt + 1`: base × 2^(attempt−1), ±25 % jitter.
fn backoff(attempt: u32) -> Duration {
    let factor = 2_u32.saturating_pow(attempt.saturating_sub(1));
    let base = FLUSH_BACKOFF_BASE.saturating_mul(factor);
    let jitter: f64 = rand::random_range(0.75_f64..=1.25_f64);
    Duration::from_secs_f64(base.as_secs_f64() * jitter)
}

/// Call-level isolation after a row-attributable failure of a combined flush:
/// each call is written once more on its own, directly through the sink,
/// under the token a non-batched write of it would carry. One attempt per
/// call, no retry, no re-enqueue, no recursion.
async fn isolate(
    shared: &Shared,
    sink: &dyn FlushSink,
    flush: &Flush,
    cause: &FlushError,
) -> Vec<super::state::Delivery> {
    tracing::warn!(
        error = %cause.error,
        calls = flush.spans.len(),
        "ingest flush failed on row content; isolating each call once"
    );
    let mut deliveries = Vec::new();
    for span in &flush.spans {
        shared.metrics.inc_isolation_attempt();
        tracing::debug!(
            call = span.call,
            rows = span.range.len(),
            "isolating one call of a failed flush"
        );
        let rows = flush.rows.get(span.range.clone()).unwrap_or(&[]);
        let refs = flush.refs.get(span.range.clone()).unwrap_or(&[]);
        let token = insert_dedup_token_for(rows.iter().map(Arc::as_ref), InsertKind::Record);
        let result = sink.insert(rows, &token).await;
        let mut state = shared.state.lock();
        match result {
            Ok(()) => deliveries.extend(state.resolve_rows(refs, &|| Ok(()))),
            Err(err) => {
                deliveries.extend(state.resolve_rows(refs, &|| Err(err_for_slot(&err.error))));
            }
        }
    }
    deliveries
}

#[allow(
    clippy::cast_precision_loss,
    reason = "row and byte counts are bounded by the configured flush limits"
)]
fn as_f64(n: usize) -> f64 {
    n as f64
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "batcher_tests.rs"]
mod batcher_tests;
