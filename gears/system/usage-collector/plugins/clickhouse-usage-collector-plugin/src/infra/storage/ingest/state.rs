//! The queue's bookkeeping: pure, synchronous transitions over one mutex.
//!
//! Every method here runs inside the queue's critical section and returns the
//! [`Delivery`]s it completed, so the caller sends them after unlocking. No
//! method awaits, blocks, or touches a metric.
//!
//! ## Model
//!
//! A *call* owns the rows it enqueued ([`OwnedRow`]). A row carries a list of
//! *subscribers*: the owner's slot first, then the slots of later calls that
//! coalesced onto it. A call's `remaining` counts its input slots not yet
//! decided — by a flush outcome reaching one of its rows (owned or followed),
//! by an immediate conflict at enqueue, or by a drop. When it reaches zero the
//! call is completed and removed. A row is *live* while any subscriber's
//! caller still holds its receiver; dead rows are dropped at the next cut.
//!
//! Accounting (`outstanding_*`, `queued_*`) is adjusted only here, at the two
//! row transitions — enqueue and resolve — so a waiter that disappears cannot
//! double-release anything.

use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};
use usage_collector_sdk::UsageCollectorPluginError;

use super::admission::Outstanding;
use super::{CallOutcome, IngestLimits, PendingCall};
use crate::infra::storage::entity::UsageRecordRow;
use crate::infra::storage::mapper::canonical_equal_rows;
use crate::infra::storage::record_store::{
    DedupKey, InsertKind, insert_dedup_token_for, row_dedup_key,
};

pub(super) type CallId = u64;

/// One owned row: its call and its index in that call's `owned`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowRef {
    pub call: CallId,
    pub idx: usize,
}

/// A slot that receives a row's outcome.
#[derive(Debug, Clone, Copy)]
struct Subscriber {
    call: CallId,
    row_idx: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowStage {
    Queued,
    InFlight,
    Resolved,
}

#[derive(Debug)]
struct OwnedRow {
    row: Arc<UsageRecordRow>,
    key: DedupKey,
    bytes: usize,
    stage: RowStage,
    subscribers: Vec<Subscriber>,
}

#[derive(Debug)]
struct CallState {
    owned: Vec<OwnedRow>,
    /// Parallel to the call's input rows.
    outcomes: Vec<Option<Result<(), UsageCollectorPluginError>>>,
    /// Input slots not yet decided.
    remaining: usize,
    tx: Option<oneshot::Sender<CallOutcome>>,
    enqueued_at: Instant,
}

/// Whether the queue admits, drains, or is done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Open,
    Draining,
    Closed,
}

/// What the batcher should do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Plan {
    /// Cut a flush now.
    CutNow,
    /// Wait for an enqueue, or until the instant (the oldest call's deadline).
    WaitUntil(Option<Instant>),
    /// Draining and nothing is queued (or closed).
    Finished,
}

/// A completed call's outcome, to be sent after the lock is released.
#[derive(Debug)]
pub struct Delivery {
    pub tx: oneshot::Sender<CallOutcome>,
    pub outcome: CallOutcome,
}

/// The rows of one call inside a flush, as a contiguous range.
#[derive(Debug, Clone)]
pub(super) struct CallSpan {
    pub call: CallId,
    pub range: Range<usize>,
}

/// An immutable flush: the rows, their references, which call each span
/// belongs to, and the token computed once over exactly these rows.
#[derive(Debug)]
pub(super) struct Flush {
    pub rows: Vec<Arc<UsageRecordRow>>,
    pub refs: Vec<RowRef>,
    pub spans: Vec<CallSpan>,
    pub token: String,
    pub bytes: usize,
    /// Age of the oldest call in the flush at the cut.
    pub oldest_age: Duration,
}

/// Result of [`QueueState::enqueue`].
pub(super) struct Enqueued {
    pub rx: oneshot::Receiver<CallOutcome>,
    pub deliveries: Vec<Delivery>,
    pub coalesced: u64,
    pub conflicts: u64,
    /// Whether the call added queued rows (the batcher must be woken).
    pub wake: bool,
    pub snapshot: Snapshot,
}

/// Result of [`QueueState::cut`].
pub(super) struct Cut {
    pub flush: Option<Flush>,
    pub deliveries: Vec<Delivery>,
    /// Queue wait of every call cut into the flush.
    pub waits: Vec<Duration>,
    pub snapshot: Snapshot,
}

/// Gauge values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Snapshot {
    pub queued_calls: u64,
    pub queued_rows: u64,
    pub queued_bytes: u64,
    pub in_flight_calls: u64,
    pub in_flight_rows: u64,
    pub in_flight_bytes: u64,
    /// Live coalescing entries (rows queued or in flight).
    pub coalesce_entries: u64,
}

fn as_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

#[derive(Debug)]
pub struct QueueState {
    phase: Phase,
    calls: HashMap<CallId, CallState>,
    /// Calls with at least one queued (not yet cut) owned row, FIFO.
    queued: VecDeque<CallId>,
    /// Dedup tuple → the unresolved row that owns it.
    coalesce: HashMap<DedupKey, RowRef>,
    outstanding_calls: usize,
    outstanding_rows: usize,
    outstanding_bytes: usize,
    queued_rows: usize,
    queued_bytes: usize,
    next_call: CallId,
}

impl QueueState {
    pub fn new() -> Self {
        Self {
            phase: Phase::Open,
            calls: HashMap::new(),
            queued: VecDeque::new(),
            coalesce: HashMap::new(),
            outstanding_calls: 0,
            outstanding_rows: 0,
            outstanding_bytes: 0,
            queued_rows: 0,
            queued_bytes: 0,
            next_call: 0,
        }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn outstanding(&self) -> Outstanding {
        Outstanding {
            calls: self.outstanding_calls,
            rows: self.outstanding_rows,
            bytes: self.outstanding_bytes,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let queued_calls = self.queued.len();
        Snapshot {
            queued_calls: as_u64(queued_calls),
            queued_rows: as_u64(self.queued_rows),
            queued_bytes: as_u64(self.queued_bytes),
            in_flight_calls: as_u64(self.outstanding_calls.saturating_sub(queued_calls)),
            in_flight_rows: as_u64(self.outstanding_rows.saturating_sub(self.queued_rows)),
            in_flight_bytes: as_u64(self.outstanding_bytes.saturating_sub(self.queued_bytes)),
            coalesce_entries: as_u64(self.coalesce.len()),
        }
    }

    /// Stop admitting (Open → Draining); a no-op in any other phase.
    pub fn close_admission(&mut self) {
        if self.phase == Phase::Open {
            self.phase = Phase::Draining;
        }
    }

    pub(super) fn set_closed(&mut self) {
        self.phase = Phase::Closed;
    }

    /// Enqueue an admitted call (see the module docs for the join model).
    ///
    /// `row_bytes` is parallel to `call.rows`.
    pub(super) fn enqueue(
        &mut self,
        call: PendingCall,
        row_bytes: &[usize],
        now: Instant,
    ) -> Enqueued {
        let id = self.next_call;
        self.next_call = self.next_call.wrapping_add(1);
        let (tx, rx) = oneshot::channel();
        let slots = call.rows.len();
        let mut state = CallState {
            owned: Vec::with_capacity(slots),
            outcomes: (0..slots).map(|_| None).collect(),
            remaining: slots,
            tx: Some(tx),
            enqueued_at: now,
        };
        let mut coalesced = 0_u64;
        let mut conflicts = 0_u64;
        let mut owned_bytes = 0_usize;

        for (row_idx, row) in call.rows.into_iter().enumerate() {
            let key = row_dedup_key(&row);
            if let Some(&leader) = self.coalesce.get(&key) {
                // A leader from this very call cannot exist: the store folds
                // in-call twins before submitting. Guard anyway so a repeated
                // key never dereferences a call that is not registered yet.
                let pending = if leader.call == id {
                    state.owned.get_mut(leader.idx)
                } else {
                    self.calls
                        .get_mut(&leader.call)
                        .and_then(|c| c.owned.get_mut(leader.idx))
                };
                if let Some(pending) = pending {
                    let existing_id = pending.row.id;
                    if matches!(canonical_equal_rows(&pending.row, &row), Ok(true)) {
                        pending.subscribers.push(Subscriber { call: id, row_idx });
                        coalesced += 1;
                    } else {
                        Self::decide_in_place(
                            &mut state,
                            row_idx,
                            Err(UsageCollectorPluginError::IdempotencyConflict {
                                idempotency_key: row.idempotency_key.clone(),
                                existing_id,
                            }),
                        );
                        conflicts += 1;
                    }
                    continue;
                }
            }
            let bytes = row_bytes.get(row_idx).copied().unwrap_or(0);
            let idx = state.owned.len();
            state.owned.push(OwnedRow {
                row: Arc::new(row),
                key: key.clone(),
                bytes,
                stage: RowStage::Queued,
                subscribers: vec![Subscriber { call: id, row_idx }],
            });
            self.coalesce.insert(key, RowRef { call: id, idx });
            owned_bytes += bytes;
        }

        let owned_rows = state.owned.len();
        let wake = owned_rows > 0;
        self.outstanding_rows += owned_rows;
        self.outstanding_bytes += owned_bytes;
        self.queued_rows += owned_rows;
        self.queued_bytes += owned_bytes;

        let mut deliveries = Vec::new();
        if state.remaining == 0 {
            // Every slot was decided at enqueue (all conflicts): complete now,
            // the call never counts as outstanding.
            if let Some(delivery) = Self::complete(state) {
                deliveries.push(delivery);
            }
        } else {
            self.outstanding_calls += 1;
            self.calls.insert(id, state);
            if wake {
                self.queued.push_back(id);
            }
        }
        Enqueued {
            rx,
            deliveries,
            coalesced,
            conflicts,
            wake,
            snapshot: self.snapshot(),
        }
    }

    /// What the batcher should do at `now`.
    pub(super) fn plan(&self, now: Instant, limits: &IngestLimits) -> Plan {
        match self.phase {
            Phase::Closed => Plan::Finished,
            Phase::Draining => {
                if self.queued.is_empty() {
                    Plan::Finished
                } else {
                    Plan::CutNow
                }
            }
            Phase::Open => {
                if self.queued_rows >= limits.max_rows || self.queued_bytes >= limits.max_bytes {
                    return Plan::CutNow;
                }
                let Some(oldest) = self.queued.front().and_then(|id| self.calls.get(id)) else {
                    return Plan::WaitUntil(None);
                };
                let deadline = oldest.enqueued_at + limits.max_wait;
                if deadline <= now {
                    Plan::CutNow
                } else {
                    Plan::WaitUntil(Some(deadline))
                }
            }
        }
    }

    /// Cut the next flush from the FIFO: whole calls only, never split, dead
    /// rows dropped on the way. `None` when nothing live was queued.
    pub(super) fn cut(&mut self, limits: &IngestLimits, now: Instant) -> Cut {
        let mut rows: Vec<Arc<UsageRecordRow>> = Vec::new();
        let mut refs: Vec<RowRef> = Vec::new();
        let mut spans: Vec<CallSpan> = Vec::new();
        let mut bytes = 0_usize;
        let mut waits = Vec::new();
        let mut deliveries = Vec::new();
        let mut oldest_age = Duration::ZERO;

        while let Some(&id) = self.queued.front() {
            let Some(call) = self.calls.get(&id) else {
                self.queued.pop_front();
                continue;
            };
            let (live, dead) = self.partition_queued_rows(call);
            let live_bytes: usize = live
                .iter()
                .filter_map(|&i| call.owned.get(i))
                .map(|r| r.bytes)
                .sum();
            if !rows.is_empty()
                && (rows.len() + live.len() > limits.max_rows
                    || bytes + live_bytes > limits.max_bytes)
            {
                break;
            }
            let age = now.saturating_duration_since(call.enqueued_at);
            self.queued.pop_front();
            for idx in dead {
                deliveries.extend(self.resolve_row(RowRef { call: id, idx }, &dropped_outcome));
            }
            if live.is_empty() {
                continue;
            }
            if rows.is_empty() {
                oldest_age = age;
            }
            waits.push(age);
            let start = rows.len();
            if let Some(call) = self.calls.get_mut(&id) {
                for idx in live {
                    if let Some(owned) = call.owned.get_mut(idx) {
                        owned.stage = RowStage::InFlight;
                        rows.push(Arc::clone(&owned.row));
                        refs.push(RowRef { call: id, idx });
                        bytes += owned.bytes;
                        self.queued_rows = self.queued_rows.saturating_sub(1);
                        self.queued_bytes = self.queued_bytes.saturating_sub(owned.bytes);
                    }
                }
            }
            spans.push(CallSpan {
                call: id,
                range: start..rows.len(),
            });
            if rows.len() >= limits.max_rows || bytes >= limits.max_bytes {
                break;
            }
        }

        let flush = (!rows.is_empty()).then(|| {
            let token = insert_dedup_token_for(rows.iter().map(Arc::as_ref), InsertKind::Record);
            Flush {
                rows,
                refs,
                spans,
                token,
                bytes,
                oldest_age,
            }
        });
        Cut {
            flush,
            deliveries,
            waits,
            snapshot: self.snapshot(),
        }
    }

    /// Resolve the given rows with `outcome` (called once per subscriber, so
    /// every slot gets its own error value). Already-resolved rows are skipped.
    pub(super) fn resolve_rows(
        &mut self,
        refs: &[RowRef],
        outcome: &dyn Fn() -> Result<(), UsageCollectorPluginError>,
    ) -> Vec<Delivery> {
        let mut deliveries = Vec::new();
        for &row_ref in refs {
            deliveries.extend(self.resolve_row(row_ref, outcome));
        }
        deliveries
    }

    /// Whether any of `refs` still has a subscriber whose caller is waiting.
    pub(super) fn has_live_subscriber(&self, refs: &[RowRef]) -> bool {
        refs.iter().any(|row_ref| {
            self.calls
                .get(&row_ref.call)
                .and_then(|c| c.owned.get(row_ref.idx))
                .is_some_and(|row| self.is_row_live(row))
        })
    }

    /// Fail every unresolved row with `Transient(detail)` and complete every
    /// call; the queue is left empty and closed. Idempotent.
    pub(super) fn fail_all_unresolved(&mut self, detail: &'static str) -> Vec<Delivery> {
        self.phase = Phase::Closed;
        let refs: Vec<RowRef> = self
            .calls
            .iter()
            .flat_map(|(&call, state)| {
                state
                    .owned
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| row.stage != RowStage::Resolved)
                    .map(move |(idx, _)| RowRef { call, idx })
            })
            .collect();
        let outcome = move || Err(UsageCollectorPluginError::transient(detail));
        let mut deliveries = self.resolve_rows(&refs, &outcome);
        // Calls with no unresolved row left (followers of rows resolved above
        // are completed through those rows; anything else is an invariant
        // break and is completed rather than leaked).
        let leftover: Vec<CallId> = self.calls.keys().copied().collect();
        for id in leftover {
            if let Some(state) = self.calls.remove(&id) {
                self.outstanding_calls = self.outstanding_calls.saturating_sub(1);
                deliveries.extend(Self::complete(state));
            }
        }
        self.queued.clear();
        self.coalesce.clear();
        self.outstanding_rows = 0;
        self.outstanding_bytes = 0;
        self.queued_rows = 0;
        self.queued_bytes = 0;
        deliveries
    }

    // ── internals ───────────────────────────────────────────────────────────

    fn is_row_live(&self, row: &OwnedRow) -> bool {
        row.subscribers.iter().any(|sub| {
            self.calls
                .get(&sub.call)
                .and_then(|c| c.tx.as_ref())
                .is_some_and(|tx| !tx.is_closed())
        })
    }

    /// Split a call's queued rows into (live, dead) indices.
    fn partition_queued_rows(&self, call: &CallState) -> (Vec<usize>, Vec<usize>) {
        let mut live = Vec::new();
        let mut dead = Vec::new();
        for (idx, row) in call.owned.iter().enumerate() {
            if row.stage != RowStage::Queued {
                continue;
            }
            if self.is_row_live(row) {
                live.push(idx);
            } else {
                dead.push(idx);
            }
        }
        (live, dead)
    }

    /// Resolve one row: release its accounting and coalescing entry, decide
    /// every subscriber's slot, complete the calls that reach zero.
    fn resolve_row(
        &mut self,
        row_ref: RowRef,
        outcome: &dyn Fn() -> Result<(), UsageCollectorPluginError>,
    ) -> Vec<Delivery> {
        let Some(call) = self.calls.get_mut(&row_ref.call) else {
            return Vec::new();
        };
        let Some(row) = call.owned.get_mut(row_ref.idx) else {
            return Vec::new();
        };
        if row.stage == RowStage::Resolved {
            return Vec::new();
        }
        let was_queued = row.stage == RowStage::Queued;
        row.stage = RowStage::Resolved;
        let bytes = row.bytes;
        let key = row.key.clone();
        let subscribers = std::mem::take(&mut row.subscribers);

        if self.coalesce.get(&key) == Some(&row_ref) {
            self.coalesce.remove(&key);
        }
        self.outstanding_rows = self.outstanding_rows.saturating_sub(1);
        self.outstanding_bytes = self.outstanding_bytes.saturating_sub(bytes);
        if was_queued {
            self.queued_rows = self.queued_rows.saturating_sub(1);
            self.queued_bytes = self.queued_bytes.saturating_sub(bytes);
        }

        let mut deliveries = Vec::new();
        for sub in subscribers {
            deliveries.extend(self.decide_slot(sub, outcome()));
        }
        deliveries
    }

    /// Decide one slot of one call; complete the call when nothing remains.
    fn decide_slot(
        &mut self,
        sub: Subscriber,
        outcome: Result<(), UsageCollectorPluginError>,
    ) -> Option<Delivery> {
        let state = self.calls.get_mut(&sub.call)?;
        Self::decide_in_place(state, sub.row_idx, outcome);
        if state.remaining > 0 {
            return None;
        }
        let state = self.calls.remove(&sub.call)?;
        self.outstanding_calls = self.outstanding_calls.saturating_sub(1);
        Self::complete(state)
    }

    fn decide_in_place(
        state: &mut CallState,
        row_idx: usize,
        outcome: Result<(), UsageCollectorPluginError>,
    ) {
        if let Some(slot @ None) = state.outcomes.get_mut(row_idx) {
            *slot = Some(outcome);
            state.remaining = state.remaining.saturating_sub(1);
        }
    }

    /// Turn a finished call into its delivery (none if the sender is gone).
    fn complete(state: CallState) -> Option<Delivery> {
        let tx = state.tx?;
        let rows = state
            .outcomes
            .into_iter()
            .map(|slot| {
                slot.unwrap_or_else(|| {
                    Err(UsageCollectorPluginError::internal(
                        "ingest slot left undecided (invariant break)",
                    ))
                })
            })
            .collect();
        Some(Delivery {
            tx,
            outcome: CallOutcome { rows },
        })
    }
}

/// Outcome of a row dropped at a cut because no caller was waiting for it.
fn dropped_outcome() -> Result<(), UsageCollectorPluginError> {
    Err(UsageCollectorPluginError::transient(
        "ingest call abandoned before its flush",
    ))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "state_tests.rs"]
mod state_tests;
