//! Bounded in-process group commit for `usage_records` ingestion.
//!
//! Every `create_usage_record` / `create_usage_records` call runs its own
//! validation, catalog check and dedup pre-read, composes the rows it has to
//! write, and then hands those rows to the one [`IngestQueue`] of the plugin
//! instead of issuing its own `INSERT`. A single background batcher
//! ([`batcher`]) cuts the queued calls into flushes — on a row limit, a byte
//! limit, or the age of the *oldest* queued call — and writes each flush as one
//! synchronous `INSERT` through a [`sink::FlushSink`]. A call's rows always
//! travel in one flush, in input order; a flush combines as many whole calls
//! as fit. Every caller awaits the outcome of the flush that carried its rows,
//! so no call is acknowledged before its `INSERT` succeeded.
//!
//! ## Bounds
//!
//! Admission ([`admission`]) is decided synchronously against the outstanding
//! work — queued *and* in flight — on three axes: calls, rows and estimated
//! bytes. A call over any of them is refused with `Transient` (retry hint 1 s)
//! and nothing is recorded for it. A call that alone exceeds a flush target
//! (`max_rows` / `max_bytes`) but fits the outstanding bound is admitted and
//! flushed on its own, never split; a call that cannot ever fit the outstanding
//! bound is refused as `Internal`, because retrying it would never help.
//!
//! ## Coalescing inside one instance
//!
//! A row whose canonical dedup tuple (`record_store::DedupKey`) is already queued or in
//! flight is not enqueued twice. If the pending row is canonically equal
//! (`mapper::canonical_equal_rows`), the new call's slot *follows* that row and
//! receives its outcome; otherwise the slot is decided as
//! `IdempotencyConflict` at once, naming the pending row's `id`. The coalescing
//! entry lives exactly as long as the row is unresolved. This is an
//! instance-local optimisation — other collector instances write their own
//! flushes under their own tokens, and the read paths resolve the resulting
//! twins (`record_store` module docs).
//!
//! ## Failure handling
//!
//! A flush is immutable: the same rows, order, versions and
//! `insert_deduplication_token` on every attempt. A `Transient` failure is
//! retried a bounded number of times with jittered backoff (never while
//! draining, never once every waiter is gone), then reported to every waiter.
//! A failure the server attributes to row content, on a flush that combined
//! several calls, is isolated: each call is written once more on its own,
//! directly through the sink, under the token a non-batched write of that call
//! would have carried. That fallback never re-enters the queue and never
//! recurses. Schema, permission and other server-wide errors fan out as they
//! are. A `Distributed` `INSERT` can commit on some shards before failing, so a
//! `Transient` outcome means "unknown"; the caller's retry pre-read absorbs the
//! rows that did land.
//!
//! ## Lifecycle
//!
//! [`IngestQueue::run`] is driven by the gear's lifecycle entry. When its
//! cancellation token fires, admission closes (late callers get `Transient`),
//! everything queued is flushed without waiting for `max_wait`, in-flight
//! flushes are awaited up to the drain deadline, and every waiter is completed.
//! Guards complete the remaining waiters with `Transient` on every other exit
//! path — a panic in the batcher, a hard abort by the lifecycle, a flush task
//! aborted at the deadline — so no caller is ever left hanging.
//!
//! ## Cancellation
//!
//! A caller that drops its future before enqueue leaves nothing behind. One
//! that drops it while queued is detected at the next cut (its receiver is
//! closed) and its rows are dropped unless another call follows them. One that
//! drops it during a flush does not affect the `INSERT`, which carries other
//! callers' rows; its outcome is simply discarded.

pub mod admission;
pub mod batcher;
pub mod sink;
mod state;

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use usage_collector_sdk::UsageCollectorPluginError;

use crate::infra::metrics::{DrainOutcome, Metrics, OutstandingState};
use crate::infra::storage::entity::UsageRecordRow;

use self::admission::estimated_row_bytes;
use self::sink::FlushSink;
use self::state::{Delivery, QueueState, Snapshot};

/// Upper bound on the drain deadline, so the lifecycle stop budget derived
/// from it (`drain_deadline + 1 s`) stays below the host's 35 s per-gear
/// shutdown deadline whatever `request_timeout_secs` is.
pub(crate) const DRAIN_DEADLINE_CAP: Duration = Duration::from_secs(29);

/// Attempts a flush gets on a `Transient` failure (the first one included).
pub(crate) const FLUSH_MAX_ATTEMPTS: u32 = 3;

/// Backoff before the second attempt; doubles per attempt, ±25 % jitter.
pub(crate) const FLUSH_BACKOFF_BASE: Duration = Duration::from_millis(50);

/// The queue's limits, derived from `InsertBatchConfig` and the client
/// deadline (`config::InsertBatchConfig::limits`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestLimits {
    /// Rows per flush; a flush is cut once it holds at least this many.
    pub max_rows: usize,
    /// Estimated row bytes per flush; same trigger semantics as `max_rows`.
    pub max_bytes: usize,
    /// Maximum age of the oldest queued call before a flush is cut.
    pub max_wait: Duration,
    /// Outstanding calls (queued + in flight) admitted at once.
    pub queue_capacity: usize,
    /// Flush `INSERT`s in flight at once.
    pub max_concurrent_flushes: usize,
    /// Outstanding rows admitted at once (`max_rows × (flushes + 1)`).
    pub max_outstanding_rows: usize,
    /// Outstanding estimated bytes admitted at once.
    pub max_outstanding_bytes: usize,
    /// Budget for the shutdown drain, measured from the cancellation.
    pub drain_deadline: Duration,
}

/// The rows one SPI call has to write, in input order: what a store hands to
/// [`IngestQueue::submit`] after its own pre-checks decided everything else.
#[derive(Debug)]
pub struct PendingCall {
    /// Composed rows; distinct dedup tuples within one call (the store's batch
    /// composition already folds in-call twins onto one row).
    pub rows: Vec<UsageRecordRow>,
}

/// One outcome per [`PendingCall::rows`] entry, in the same order. `Ok(())`
/// means the row is committed.
#[derive(Debug)]
pub struct CallOutcome {
    /// Per-row outcomes, parallel to the submitted rows.
    pub rows: Vec<Result<(), UsageCollectorPluginError>>,
}

/// How the batcher ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainReport {
    /// Whether the drain completed, hit its deadline, or was aborted.
    pub outcome: DrainOutcome,
    /// Calls whose rows were cut into a flush over the batcher's lifetime.
    pub flushed_calls: usize,
}

/// An admitted call. Dropping it before [`Submitted::wait`] resolves abandons
/// the call: its rows are dropped at the next cut unless another call follows
/// them, and an in-flight `INSERT` carrying them completes regardless.
#[derive(Debug)]
pub struct Submitted {
    rx: oneshot::Receiver<CallOutcome>,
    metrics: Arc<Metrics>,
}

impl Submitted {
    /// Await the flush outcome for at most `budget`.
    ///
    /// # Errors
    ///
    /// `Transient` when `budget` elapses first (the rows stay in their flush;
    /// the caller's retry pre-read absorbs them if they land) or when the queue
    /// stopped before the write was resolved. Either error applies to every
    /// row of the call.
    pub async fn wait(self, budget: Duration) -> Result<CallOutcome, UsageCollectorPluginError> {
        match tokio::time::timeout(budget, self.rx).await {
            Ok(Ok(outcome)) => Ok(outcome),
            Ok(Err(_closed)) => Err(UsageCollectorPluginError::transient(
                "ingest queue stopped before the write was resolved",
            )),
            Err(_elapsed) => {
                self.metrics.inc_wait_timeout();
                Err(UsageCollectorPluginError::transient(
                    "ingest queue wait exceeded the request budget",
                ))
            }
        }
    }
}

/// State shared by the submit side, the batcher and its flush tasks.
pub(crate) struct Shared {
    limits: IngestLimits,
    state: Mutex<QueueState>,
    /// Signalled on every enqueue that adds queued rows; `notify_one` keeps a
    /// permit when the batcher is busy, so no enqueue is lost.
    wake: Notify,
    metrics: Arc<Metrics>,
}

impl Shared {
    /// Publish the outstanding-work gauges from a state snapshot.
    fn publish(&self, snapshot: &Snapshot) {
        self.metrics.set_ingest_outstanding(
            OutstandingState::Queued,
            snapshot.queued_calls,
            snapshot.queued_rows,
            snapshot.queued_bytes,
        );
        self.metrics.set_ingest_outstanding(
            OutstandingState::InFlight,
            snapshot.in_flight_calls,
            snapshot.in_flight_rows,
            snapshot.in_flight_bytes,
        );
    }
}

/// Send every completed call its outcome. A closed receiver (the caller gave
/// up) is not an error: the outcome is simply discarded.
fn deliver(deliveries: Vec<Delivery>) {
    for delivery in deliveries {
        if delivery.tx.send(delivery.outcome).is_err() {
            tracing::debug!("ingest waiter left before its outcome was delivered");
        }
    }
}

/// The plugin's group-commit queue: one per plugin instance.
pub struct IngestQueue {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for IngestQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestQueue")
            .field("limits", &self.shared.limits)
            .finish_non_exhaustive()
    }
}

impl IngestQueue {
    /// Build a queue. Nothing runs until [`IngestQueue::run`] is driven;
    /// calls submitted before that simply wait (within their budget).
    #[must_use]
    pub fn new(limits: IngestLimits, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            shared: Arc::new(Shared {
                limits,
                state: Mutex::new(QueueState::new()),
                wake: Notify::new(),
                metrics,
            }),
        })
    }

    /// The limits this queue enforces.
    #[must_use]
    pub fn limits(&self) -> IngestLimits {
        self.shared.limits
    }

    /// Admit and enqueue one call. Synchronous: admission, coalescing and
    /// enqueue happen in one critical section with no await.
    ///
    /// # Errors
    ///
    /// `Transient` (retry hint 1 s) when the call exceeds an outstanding bound
    /// or admission is closed; `Internal` when the call alone exceeds an
    /// outstanding bound and so could never be admitted. Nothing is recorded
    /// for a refused call.
    pub fn submit(&self, call: PendingCall) -> Result<Submitted, UsageCollectorPluginError> {
        let row_bytes: Vec<usize> = call.rows.iter().map(estimated_row_bytes).collect();
        let bytes: usize = row_bytes.iter().sum();
        let now = Instant::now();
        let enqueued = {
            let mut state = self.shared.state.lock();
            if let Err(reason) = admission::check(
                &self.shared.limits,
                state.outstanding(),
                call.rows.len(),
                bytes,
                state.phase(),
            ) {
                drop(state);
                self.shared.metrics.inc_admission_rejected(reason);
                return Err(admission::rejection(reason));
            }
            state.enqueue(call, &row_bytes, now)
        };
        deliver(enqueued.deliveries);
        if enqueued.coalesced > 0 {
            self.shared.metrics.inc_coalesced_rows(enqueued.coalesced);
        }
        if enqueued.conflicts > 0 {
            self.shared
                .metrics
                .inc_coalesce_conflicts(enqueued.conflicts);
        }
        self.shared.publish(&enqueued.snapshot);
        if enqueued.wake {
            self.shared.wake.notify_one();
        }
        Ok(Submitted {
            rx: enqueued.rx,
            metrics: Arc::clone(&self.shared.metrics),
        })
    }

    /// Drive the batcher until `cancel` fires and the drain completes.
    ///
    /// The lifecycle entry awaits this; see the module docs for the drain and
    /// cancellation contract.
    pub async fn run(
        self: Arc<Self>,
        sink: Arc<dyn FlushSink>,
        cancel: CancellationToken,
    ) -> DrainReport {
        batcher::run(Arc::clone(&self.shared), sink, cancel).await
    }

    /// Stop admitting calls (idempotent). `run` does this itself when its token
    /// fires; exposed for callers that must refuse work before that.
    pub fn close_admission(&self) {
        let snapshot = {
            let mut state = self.shared.state.lock();
            state.close_admission();
            state.snapshot()
        };
        self.shared.publish(&snapshot);
    }

    /// Current outstanding-work snapshot, for assertions.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Snapshot {
        self.shared.state.lock().snapshot()
    }
}

/// What is left of `client_deadline` for a call that entered the store at
/// `entered`: the queue wait and the flush must both fit in it.
#[must_use]
pub(crate) fn remaining_budget(entered: Instant, client_deadline: Duration) -> Duration {
    client_deadline.saturating_sub(entered.elapsed())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
pub(crate) mod test_rows;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "ingest/ingest_tests.rs"]
pub(crate) mod ingest_tests;
