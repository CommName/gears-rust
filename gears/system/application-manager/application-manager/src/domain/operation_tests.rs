#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use time::macros::datetime;

use super::*;

const COMMIT: OffsetDateTime = datetime!(2026-10-22 10:00:00 UTC);
const SECS: fn(u64) -> Duration = Duration::from_secs;

#[test]
fn seed_window_is_n_plus_one_timeouts_plus_margin() {
    let deadline = run_deadline(COMMIT, OperationAction::Seed, 3, SECS(10), SECS(30)).unwrap();
    // (3 + 1) x 10s + 30s
    assert_eq!(deadline, COMMIT + SECS(70));
}

#[test]
fn uninstall_window_is_n_timeouts_plus_margin() {
    let deadline = run_deadline(COMMIT, OperationAction::Uninstall, 3, SECS(10), SECS(30)).unwrap();
    // 3 x 10s + 30s
    assert_eq!(deadline, COMMIT + SECS(60));
}

#[test]
fn seed_with_no_plugins_still_owns_one_timeout() {
    let deadline = run_deadline(COMMIT, OperationAction::Seed, 0, SECS(10), SECS(30)).unwrap();
    assert_eq!(deadline, COMMIT + SECS(40));
}

#[test]
fn uninstall_with_no_plugins_is_the_margin_alone() {
    let deadline = run_deadline(COMMIT, OperationAction::Uninstall, 0, SECS(10), SECS(30)).unwrap();
    assert_eq!(deadline, COMMIT + SECS(30));
}

#[test]
fn window_grows_with_the_plugin_count() {
    let at = |n| run_deadline(COMMIT, OperationAction::Seed, n, SECS(5), SECS(1)).unwrap();
    assert_eq!(at(1) - at(0), time::Duration::seconds(5));
    assert_eq!(at(14) - at(13), time::Duration::seconds(5));
}

#[test]
fn sub_second_parts_are_kept() {
    let deadline = run_deadline(
        COMMIT,
        OperationAction::Seed,
        1,
        Duration::from_millis(1500),
        Duration::from_millis(250),
    )
    .unwrap();
    // 2 x 1.5s + 0.25s
    assert_eq!(deadline, COMMIT + Duration::from_millis(3250));
}

#[test]
fn plugin_count_overflow_is_an_error() {
    for action in [OperationAction::Seed, OperationAction::Uninstall] {
        assert_eq!(
            run_deadline(COMMIT, action, usize::MAX, SECS(10), SECS(30)),
            Err(RunDeadlineOverflow)
        );
    }
}

#[test]
fn timeout_product_overflow_is_an_error() {
    assert_eq!(
        run_deadline(COMMIT, OperationAction::Seed, 2, Duration::MAX, SECS(1)),
        Err(RunDeadlineOverflow)
    );
}

#[test]
fn margin_overflow_is_an_error() {
    assert_eq!(
        run_deadline(
            COMMIT,
            OperationAction::Uninstall,
            0,
            SECS(1),
            Duration::MAX
        ),
        Err(RunDeadlineOverflow)
    );
}

#[test]
fn window_past_the_time_range_is_an_error() {
    let near_end = datetime!(9999-12-31 23:59:00 UTC);
    assert_eq!(
        run_deadline(near_end, OperationAction::Seed, 1, SECS(3600), SECS(3600)),
        Err(RunDeadlineOverflow)
    );
}
