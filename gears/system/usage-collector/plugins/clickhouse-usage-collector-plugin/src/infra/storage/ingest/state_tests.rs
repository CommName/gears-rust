// Test modules using bare `panic!` opt in explicitly.
#![allow(clippy::panic)]

use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::Instant;
use usage_collector_sdk::UsageCollectorPluginError;

use super::{Delivery, Phase, Plan, QueueState, RowRef};
use crate::infra::storage::ingest::admission::estimated_row_bytes;
use crate::infra::storage::ingest::ingest_tests::limits;
use crate::infra::storage::ingest::test_rows::{row, row_with_value, rows};
use crate::infra::storage::ingest::{CallOutcome, PendingCall};

/// Enqueue `rows` at `now`; returns the receiver and the rest of the result.
fn enqueue(
    state: &mut QueueState,
    rows: Vec<crate::infra::storage::entity::UsageRecordRow>,
    now: Instant,
) -> (oneshot::Receiver<CallOutcome>, Enqueued) {
    let bytes: Vec<usize> = rows.iter().map(estimated_row_bytes).collect();
    let enq = state.enqueue(PendingCall { rows }, &bytes, now);
    (
        enq.rx,
        Enqueued {
            deliveries: enq.deliveries,
            coalesced: enq.coalesced,
            conflicts: enq.conflicts,
            wake: enq.wake,
        },
    )
}

/// [`super::Enqueued`] without its receiver.
struct Enqueued {
    deliveries: Vec<Delivery>,
    coalesced: u64,
    conflicts: u64,
    wake: bool,
}

fn send(deliveries: Vec<Delivery>) {
    for d in deliveries {
        drop(d.tx.send(d.outcome));
    }
}

fn transient() -> Result<(), UsageCollectorPluginError> {
    Err(UsageCollectorPluginError::transient("t"))
}

#[test]
fn enqueue_assigns_owned_rows_and_coalesce_entries() {
    let mut state = QueueState::new();
    let now = Instant::now();
    let (_rx, enq) = enqueue(&mut state, rows(0, 3), now);
    assert!(enq.wake);
    assert_eq!(enq.coalesced, 0);
    assert_eq!(enq.conflicts, 0);
    assert!(enq.deliveries.is_empty());
    let snap = state.snapshot();
    assert_eq!(snap.queued_calls, 1);
    assert_eq!(snap.queued_rows, 3);
    assert_eq!(snap.coalesce_entries, 3);
    assert_eq!(state.outstanding().calls, 1);
    assert_eq!(state.outstanding().rows, 3);
}

/// An identical pending row gains a follower; a differing one yields an
/// immediate `IdempotencyConflict` naming the pending row's id.
#[test]
fn identical_key_becomes_a_follower_and_conflicting_key_is_a_conflict() {
    let mut state = QueueState::new();
    let now = Instant::now();
    let (_leader_rx, _) = enqueue(&mut state, vec![row(1)], now);

    let (mut rx, enq) = enqueue(
        &mut state,
        vec![row(1), row_with_value(1, 999), row(2)],
        now,
    );
    assert_eq!(enq.coalesced, 1);
    assert_eq!(enq.conflicts, 1);
    assert!(enq.wake, "row(2) is a new owned row");
    assert_eq!(state.snapshot().coalesce_entries, 2, "row(1) and row(2)");
    assert_eq!(
        state.outstanding().rows,
        2,
        "the follower and the conflict own nothing"
    );
    assert!(rx.try_recv().is_err(), "undecided slots remain");

    // Resolving the leader's row completes the leader call and decides the
    // follower slot; the second call still owns row(2).
    let leader = RowRef { call: 0, idx: 0 };
    let deliveries = state.resolve_rows(&[leader], &|| Ok(()));
    assert_eq!(deliveries.len(), 1, "only the leader call completes");
    send(deliveries);
    assert!(rx.try_recv().is_err(), "row(2) is still undecided");
    let follower_row = RowRef { call: 1, idx: 0 };
    let deliveries = state.resolve_rows(&[follower_row], &|| Ok(()));
    assert_eq!(deliveries.len(), 1, "now the second call completes");
    send(deliveries);
    let outcome = rx.try_recv().unwrap();
    assert!(outcome.rows[0].is_ok());
    match &outcome.rows[1] {
        Err(UsageCollectorPluginError::IdempotencyConflict { existing_id, .. }) => {
            assert_eq!(*existing_id, row(1).id);
        }
        other => panic!("expected a conflict, got {other:?}"),
    }
    assert!(outcome.rows[2].is_ok());
    assert_eq!(state.outstanding().calls, 0);
    assert_eq!(state.snapshot().coalesce_entries, 0);
}

/// A call whose every row conflicts is decided at enqueue and never counts
/// as outstanding.
#[test]
fn all_conflict_call_is_delivered_immediately() {
    let mut state = QueueState::new();
    let now = Instant::now();
    let (_leader_rx, _) = enqueue(&mut state, vec![row(1)], now);
    let (mut rx, enq) = enqueue(&mut state, vec![row_with_value(1, 5)], now);
    assert_eq!(enq.conflicts, 1);
    assert!(!enq.wake);
    assert_eq!(enq.deliveries.len(), 1);
    send(enq.deliveries);
    let outcome = rx.try_recv().unwrap();
    assert!(matches!(
        outcome.rows[0],
        Err(UsageCollectorPluginError::IdempotencyConflict { .. })
    ));
    assert_eq!(
        state.outstanding().calls,
        1,
        "only the leader is outstanding"
    );
}

/// Calls are cut whole, FIFO; a call that would overflow `max_rows` starts
/// the next flush.
#[test]
fn cut_takes_whole_calls_in_fifo_order_and_never_splits_one() {
    let l = limits(); // max_rows 4
    let mut state = QueueState::new();
    let now = Instant::now();
    let (_a, _) = enqueue(&mut state, rows(0, 2), now);
    let (_b, _) = enqueue(&mut state, rows(10, 2), now);
    let (_c, _) = enqueue(&mut state, rows(20, 1), now);

    let cut = state.cut(&l, now);
    let flush = cut.flush.unwrap();
    assert_eq!(flush.rows.len(), 4, "a and b fill the flush exactly");
    assert_eq!(flush.spans.len(), 2);
    assert_eq!(flush.spans[0].range, 0..2);
    assert_eq!(flush.spans[1].range, 2..4);
    assert_eq!(cut.waits.len(), 2);
    assert_eq!(state.snapshot().queued_calls, 1);
    assert_eq!(state.snapshot().in_flight_rows, 4);

    let cut = state.cut(&l, now);
    let flush = cut.flush.unwrap();
    assert_eq!(flush.rows.len(), 1);
    assert_eq!(flush.spans[0].call, 2);
    assert!(state.cut(&l, now).flush.is_none(), "nothing left");
}

/// A call over `max_rows` is admitted and flushed alone, never split.
#[test]
fn cut_flushes_an_oversize_call_alone() {
    let l = limits();
    let mut state = QueueState::new();
    let now = Instant::now();
    let (_a, _) = enqueue(&mut state, rows(0, 1), now);
    let (_b, _) = enqueue(&mut state, rows(10, 6), now);
    let (_c, _) = enqueue(&mut state, rows(20, 1), now);

    let first = state.cut(&l, now).flush.unwrap();
    assert_eq!(first.rows.len(), 1, "a alone: b would overflow");
    let second = state.cut(&l, now).flush.unwrap();
    assert_eq!(second.rows.len(), 6, "b alone, whole");
    let third = state.cut(&l, now).flush.unwrap();
    assert_eq!(third.rows.len(), 1);
}

/// Dropping the receiver kills a call's rows: the next cut drops them and
/// releases every piece of accounting.
#[test]
fn cut_drops_rows_whose_subscribers_are_all_gone_and_releases_accounting() {
    let l = limits();
    let mut state = QueueState::new();
    let now = Instant::now();
    let (rx, _) = enqueue(&mut state, rows(0, 2), now);
    drop(rx);
    let cut = state.cut(&l, now);
    assert!(cut.flush.is_none());
    assert!(cut.waits.is_empty());
    assert_eq!(state.outstanding().calls, 0);
    assert_eq!(state.outstanding().rows, 0);
    assert_eq!(state.outstanding().bytes, 0);
    assert_eq!(state.snapshot().coalesce_entries, 0);
    assert_eq!(state.snapshot().queued_calls, 0);
}

/// A cancelled leader's row stays live while a follower waits on it.
#[test]
fn cut_keeps_a_cancelled_leaders_row_for_a_live_follower() {
    let l = limits();
    let mut state = QueueState::new();
    let now = Instant::now();
    let (leader_rx, _) = enqueue(&mut state, vec![row(1)], now);
    let (mut follower_rx, _) = enqueue(&mut state, vec![row(1)], now);
    drop(leader_rx);

    let flush = state
        .cut(&l, now)
        .flush
        .expect("the row is live for the follower");
    assert_eq!(flush.rows.len(), 1);
    let deliveries = state.resolve_rows(&flush.refs, &|| Ok(()));
    send(deliveries);
    assert!(follower_rx.try_recv().unwrap().rows[0].is_ok());
    assert_eq!(state.outstanding().calls, 0);
}

/// Each subscriber's slot gets its own outcome value, and a call completes
/// when its last slot is decided — across owned and followed rows.
#[test]
fn resolve_rows_fans_each_subscribers_outcome_and_completes_calls() {
    let l = limits();
    let mut state = QueueState::new();
    let now = Instant::now();
    let (mut a_rx, _) = enqueue(&mut state, rows(0, 2), now);
    let (mut b_rx, _) = enqueue(&mut state, vec![row(0), row(5)], now);
    let flush = state.cut(&l, now).flush.unwrap();
    assert_eq!(flush.rows.len(), 3, "row(0) shared, row(1), row(5)");

    let deliveries = state.resolve_rows(&flush.refs, &transient);
    assert_eq!(deliveries.len(), 2);
    send(deliveries);
    let a = a_rx.try_recv().unwrap();
    let b = b_rx.try_recv().unwrap();
    assert!(a.rows.iter().all(Result::is_err));
    assert!(b.rows.iter().all(Result::is_err));
    assert_eq!(state.outstanding().calls, 0);
    assert_eq!(state.outstanding().rows, 0);
    // Idempotent: resolving again changes nothing and delivers nothing.
    assert!(state.resolve_rows(&flush.refs, &|| Ok(())).is_empty());
}

/// The coalescing entry outlives the cut: a row in flight still absorbs an
/// identical enqueue; once resolved, the same key is a new row.
#[test]
fn coalesce_entry_lives_until_the_row_resolves() {
    let l = limits();
    let mut state = QueueState::new();
    let now = Instant::now();
    let (_a, _) = enqueue(&mut state, vec![row(1)], now);
    let flush = state.cut(&l, now).flush.unwrap();
    let (_b, enq) = enqueue(&mut state, vec![row(1)], now);
    assert_eq!(enq.coalesced, 1, "in-flight rows still coalesce");
    send(state.resolve_rows(&flush.refs, &|| Ok(())));
    let (_c, enq) = enqueue(&mut state, vec![row(1)], now);
    assert_eq!(enq.coalesced, 0, "a resolved row no longer coalesces");
    assert!(enq.wake);
}

/// The timer deadline is the oldest call's; later arrivals do not move it,
/// and the size triggers cut regardless of age.
#[test]
fn plan_uses_the_oldest_calls_enqueue_time() {
    let l = limits();
    let mut state = QueueState::new();
    let t0 = Instant::now();
    assert_eq!(state.plan(t0, &l), Plan::WaitUntil(None));
    let (_a, _) = enqueue(&mut state, rows(0, 1), t0);
    assert_eq!(state.plan(t0, &l), Plan::WaitUntil(Some(t0 + l.max_wait)));
    let (_b, _) = enqueue(&mut state, rows(10, 1), t0 + Duration::from_millis(40));
    assert_eq!(
        state.plan(t0 + Duration::from_millis(45), &l),
        Plan::WaitUntil(Some(t0 + l.max_wait)),
        "the newer call does not postpone the oldest call's deadline"
    );
    assert_eq!(state.plan(t0 + l.max_wait, &l), Plan::CutNow);
    let (_c, _) = enqueue(&mut state, rows(20, 2), t0);
    assert_eq!(state.plan(t0, &l), Plan::CutNow, "max_rows reached");
    state.close_admission();
    assert_eq!(state.phase(), Phase::Draining);
    assert_eq!(state.plan(t0, &l), Plan::CutNow);
    state.set_closed();
    assert_eq!(state.plan(t0, &l), Plan::Finished);
}

/// Failing everything completes every waiter — queued and in flight — with
/// `Transient`, and leaves nothing outstanding.
#[test]
fn fail_all_unresolved_completes_every_waiter_and_zeroes_accounting() {
    let l = limits();
    let mut state = QueueState::new();
    let now = Instant::now();
    let (mut a_rx, _) = enqueue(&mut state, rows(0, 2), now);
    let _flush = state.cut(&l, now).flush.unwrap();
    let (mut b_rx, _) = enqueue(&mut state, rows(10, 1), now);
    let (mut c_rx, _) = enqueue(&mut state, vec![row(0)], now);

    let deliveries = state.fail_all_unresolved("stopped");
    assert_eq!(deliveries.len(), 3);
    send(deliveries);
    for rx in [&mut a_rx, &mut b_rx, &mut c_rx] {
        let outcome = rx.try_recv().unwrap();
        assert!(
            outcome
                .rows
                .iter()
                .all(|r| matches!(r, Err(UsageCollectorPluginError::Transient { .. })))
        );
    }
    assert_eq!(state.phase(), Phase::Closed);
    assert_eq!(state.outstanding().calls, 0);
    assert_eq!(state.outstanding().rows, 0);
    assert_eq!(state.snapshot().coalesce_entries, 0);
    assert_eq!(state.plan(now, &l), Plan::Finished);
}
