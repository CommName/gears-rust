//! Unit tests for the clock, request-id and startup doubles (DESIGN §3.2 "Ports and
//! Composition", §4.8 "Clock").
//!
//! Planned API in `crate::test_support` (cfg(test) only; the traits are in
//! `crate::domain::ports`):
//!
//! ```text
//! pub struct FixedClock;   // new(SystemTime); impl Clock; never changes
//! pub struct ManualClock;  // new(SystemTime); advance(&self, Duration); impl Clock
//! pub struct SequentialIdSource; // impl RequestIdSource; new() starts at
//!                                // Uuid::from_u128(1); starting_at(u128); thread-safe
//! pub struct StartupHolder; // impl StartupPort; Default; flag-only; inherent publish()
//! ```
//!
//! `SequentialIdSource` ids are plain counters, ordered by byte order; they are NOT
//! UUID v7. The production source (a later phase, DESIGN open item on request-id
//! generation) must emit time-ordered v7 ids.
//!
//! - `fixed_clock_never_changes`: same time on every call (§4.8).
//! - `manual_clock_starts_at_given_time` / `manual_clock_advances_only_when_told`:
//!   reads never move it; `advance` accumulates (§4.8).
//! - `manual_clock_advance_visible_through_arc_dyn`: shared handles see `advance`.
//! - `clocks_coerce_to_arc_dyn_clock`: both are `Arc<dyn Clock>`.
//! - `sequential_ids_are_deterministic`: two fresh sources, same sequence (§3.2).
//! - `sequential_ids_are_strictly_time_ordered`: byte order equals generation order.
//! - `sequential_ids_start_at_requested_value`: `starting_at` seeds the sequence.
//! - `sequential_ids_are_unique_across_threads`: 4 threads behind a barrier produce
//!   exactly the set 1..=1000.
//! - `id_source_coerces_to_arc_dyn`.
//! - `startup_holder_fresh_is_not_ready_and_not_stopping`.
//! - `startup_holder_publish_makes_it_ready`.
//! - `startup_holder_stopping_is_not_ready_but_stopping`.
//! - `startup_holder_stopping_before_publish_stays_not_ready`.
//! - `startup_holder_double_publish_is_harmless`.
//! - `startup_holder_is_arc_dyn_port`.

use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::domain::ports::{Clock, RequestIdSource, StartupPort};
use crate::test_support::{FixedClock, ManualClock, SequentialIdSource, StartupHolder};

fn t0() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_700_000_000)
}

#[test]
fn fixed_clock_never_changes() {
    let clock = FixedClock::new(t0());
    assert_eq!(clock.now(), t0());
    assert_eq!(clock.now(), t0());
}

#[test]
fn manual_clock_starts_at_given_time() {
    assert_eq!(ManualClock::new(t0()).now(), t0());
}

#[test]
fn manual_clock_advances_only_when_told() {
    let clock = ManualClock::new(t0());
    assert_eq!(clock.now(), t0());
    assert_eq!(clock.now(), t0(), "reading the clock must not move it");
    clock.advance(Duration::from_secs(5));
    assert_eq!(clock.now(), t0() + Duration::from_secs(5));
    clock.advance(Duration::from_millis(250));
    assert_eq!(
        clock.now(),
        t0() + Duration::from_millis(5250),
        "advances accumulate"
    );
}

#[test]
fn manual_clock_advance_visible_through_arc_dyn() {
    let manual = Arc::new(ManualClock::new(t0()));
    let shared: Arc<dyn Clock> = manual.clone();
    manual.advance(Duration::from_secs(1));
    assert_eq!(shared.now(), t0() + Duration::from_secs(1));
}

#[test]
fn clocks_coerce_to_arc_dyn_clock() {
    let clocks: Vec<Arc<dyn Clock>> = vec![
        Arc::new(FixedClock::new(t0())),
        Arc::new(ManualClock::new(t0())),
    ];
    for clock in clocks {
        assert_eq!(clock.now(), t0());
    }
}

#[test]
fn sequential_ids_are_deterministic() {
    let a = SequentialIdSource::new();
    let b = SequentialIdSource::new();
    let first: Vec<Uuid> = (0..5).map(|_| a.next_id()).collect();
    let second: Vec<Uuid> = (0..5).map(|_| b.next_id()).collect();
    assert_eq!(first, second);
    assert_eq!(first[0], Uuid::from_u128(1));
}

#[test]
fn sequential_ids_are_strictly_time_ordered() {
    let source = SequentialIdSource::new();
    let ids: Vec<Uuid> = (0..1000).map(|_| source.next_id()).collect();
    for pair in ids.windows(2) {
        assert!(
            pair[0].as_bytes() < pair[1].as_bytes(),
            "ids must increase in byte order"
        );
    }
}

#[test]
fn sequential_ids_start_at_requested_value() {
    let source = SequentialIdSource::starting_at(41);
    assert_eq!(source.next_id(), Uuid::from_u128(41));
    assert_eq!(source.next_id(), Uuid::from_u128(42));
}

#[test]
fn sequential_ids_are_unique_across_threads() {
    let source = Arc::new(SequentialIdSource::new());
    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let source = Arc::clone(&source);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                (0..250).map(|_| source.next_id()).collect::<Vec<_>>()
            })
        })
        .collect();
    let mut seen = Vec::new();
    for handle in handles {
        seen.extend(handle.join().expect("thread must not panic"));
    }
    assert_eq!(seen.len(), 1000);
    let seen: HashSet<Uuid> = seen.into_iter().collect();
    let expected: HashSet<Uuid> = (1..=1000_u128).map(Uuid::from_u128).collect();
    assert_eq!(seen, expected, "no gap and no repeat");
}

#[test]
fn id_source_coerces_to_arc_dyn() {
    let source: Arc<dyn RequestIdSource> = Arc::new(SequentialIdSource::new());
    assert_eq!(source.next_id(), Uuid::from_u128(1));
}

// DESIGN §3.2 "Startup" port: publish once, set the stopping flag, readers observe
// ready = published and not stopping (§4.8 startup readiness tests).
#[test]
fn startup_holder_fresh_is_not_ready_and_not_stopping() {
    let holder = StartupHolder::default();
    assert!(!holder.is_ready());
    assert!(!holder.is_stopping());
}

#[test]
fn startup_holder_publish_makes_it_ready() {
    let holder = StartupHolder::default();
    holder.publish();
    assert!(holder.is_ready());
    assert!(!holder.is_stopping());
}

#[test]
fn startup_holder_stopping_is_not_ready_but_stopping() {
    let holder = StartupHolder::default();
    holder.publish();
    holder.set_stopping();
    assert!(!holder.is_ready());
    assert!(holder.is_stopping());
}

#[test]
fn startup_holder_stopping_before_publish_stays_not_ready() {
    let holder = StartupHolder::default();
    holder.set_stopping();
    holder.publish();
    assert!(!holder.is_ready(), "stopping wins over a later publish");
    assert!(holder.is_stopping());
}

#[test]
fn startup_holder_double_publish_is_harmless() {
    let holder = StartupHolder::default();
    holder.publish();
    holder.publish();
    assert!(holder.is_ready());
    assert!(!holder.is_stopping());
}

#[test]
fn startup_holder_is_arc_dyn_port() {
    let holder = Arc::new(StartupHolder::default());
    let port: Arc<dyn StartupPort> = holder.clone();
    assert!(!port.is_ready());
    holder.publish();
    assert!(port.is_ready());
    port.set_stopping();
    assert!(!port.is_ready());
}
