//! Tests for the shared log capture (used by the call guard, startup and adapter log
//! tests).
//!
//! Planned API in `crate::test_support`: `capture_logs() -> (LogBuffer, CaptureGuard)`,
//! `LogBuffer::{text, lines_with, assert_live}`, `has_field(line, field, value)`,
//! `field_value(line, field) -> Option<String>`.
//!
//! - `capture_sees_events_of_its_own_thread_only`
//! - `assert_live_passes_while_capturing`
//! - `fields_are_read_with_or_without_quotes`
//! - `capture_is_stable_across_parallel_threads`
//!
//! Process-wide capture by sentinel (P6): `capture_logs_containing(sentinel) ->
//! (SentinelCapture, SentinelGuard)`, `SentinelCapture::{text,
//! assert_live_on_a_blocking_thread}`.
//!
//! - `the_sentinel_capture_sees_the_events_of_other_threads_that_carry_the_sentinel`
//! - `the_sentinel_capture_ignores_events_without_the_sentinel`
//! - `the_sentinel_capture_stops_when_its_guard_drops`

use crate::test_support::{capture_logs, capture_logs_containing, field_value, has_field};

#[test]
fn capture_sees_events_of_its_own_thread_only() {
    let (logs, _guard) = capture_logs();
    let other = std::thread::spawn(|| {
        let (other_logs, _g) = capture_logs();
        tracing::warn!("only_in_other");
        other_logs.lines_with("only_in_other").len()
    });

    tracing::warn!("only_in_mine");

    assert_eq!(other.join().expect("thread joins"), 1);
    assert_eq!(logs.lines_with("only_in_mine").len(), 1);
    assert!(logs.lines_with("only_in_other").is_empty());
}

#[test]
fn assert_live_passes_while_capturing() {
    let (logs, _guard) = capture_logs();

    logs.assert_live();
}

#[test]
fn fields_are_read_with_or_without_quotes() {
    let line = "WARN x: msg a=1 b=\"two\" c=x,y";

    assert!(has_field(line, "a", "1"));
    assert!(has_field(line, "b", "two"));
    assert!(!has_field(line, "a", "2"));
    assert_eq!(field_value(line, "c").as_deref(), Some("x,y"));
    assert_eq!(field_value(line, "d"), None);
}

#[test]
fn capture_is_stable_across_parallel_threads() {
    let handles: Vec<_> = (0..16)
        .map(|i| {
            std::thread::spawn(move || {
                let (logs, _g) = capture_logs();
                for _ in 0..50 {
                    tracing::warn!(n = i, "parallel_event");
                }
                logs.lines_with("parallel_event").len()
            })
        })
        .collect();

    for h in handles {
        assert_eq!(h.join().expect("thread joins"), 50);
    }
}

#[tokio::test]
async fn the_sentinel_capture_sees_the_events_of_other_threads_that_carry_the_sentinel() {
    let (logs, _guard) = capture_logs_containing("SENTINEL-capture-a-5c1");

    logs.assert_live_on_a_blocking_thread().await;
    std::thread::spawn(|| tracing::warn!("from_a_plain_thread SENTINEL-capture-a-5c1"))
        .join()
        .expect("thread joins");

    assert_eq!(logs.text().lines().count(), 2, "{}", logs.text());
}

#[tokio::test]
async fn the_sentinel_capture_ignores_events_without_the_sentinel() {
    let (logs, _guard) = capture_logs_containing("SENTINEL-capture-b-5c1");

    tokio::task::spawn_blocking(|| tracing::warn!("unrelated_event"))
        .await
        .expect("task");
    tracing::warn!("unrelated_event_on_the_test_thread");

    assert_eq!(logs.text(), "");
}

#[test]
fn the_sentinel_capture_stops_when_its_guard_drops() {
    let (logs, guard) = capture_logs_containing("SENTINEL-capture-c-5c1");
    tracing::warn!("first SENTINEL-capture-c-5c1");
    drop(guard);

    tracing::warn!("second SENTINEL-capture-c-5c1");

    assert_eq!(logs.text().lines().count(), 1, "{}", logs.text());
}
