//! Unit tests for the production clock and request-id source (DESIGN §3.2 "Ports and
//! Composition": the clock and the request-id source are ports; production uses the system
//! clock and `UUIDv7` ids).
//!
//! - `the_system_clock_reads_the_wall_clock`
//! - `request_ids_are_unique_version_7_ids`
//! - `request_ids_carry_the_current_time`

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::domain::ports::{Clock, RequestIdSource};
use crate::infra::system_ports::{SystemClock, UuidV7RequestIds};

#[test]
fn the_system_clock_reads_the_wall_clock() {
    let before = SystemTime::now();
    let read = SystemClock.now();
    let after = SystemTime::now();

    assert!(
        before <= read && read <= after,
        "{before:?} {read:?} {after:?}"
    );
}

#[test]
fn request_ids_are_unique_version_7_ids() {
    let ids = UuidV7RequestIds;
    let seen: HashSet<Uuid> = (0..1000).map(|_| ids.next_id()).collect();

    assert_eq!(seen.len(), 1000, "no repeat");
    for id in seen {
        assert_eq!(id.get_version_num(), 7, "{id}");
    }
}

#[test]
fn request_ids_carry_the_current_time() {
    let id = UuidV7RequestIds.next_id();

    let stamp = id.get_timestamp().expect("a v7 id carries a timestamp");
    let (secs, _) = stamp.to_unix();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs();
    assert!(
        now.abs_diff(secs) <= Duration::from_secs(5).as_secs(),
        "{secs} vs {now}"
    );
}
