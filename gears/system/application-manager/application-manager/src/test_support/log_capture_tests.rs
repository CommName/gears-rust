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

use crate::test_support::{capture_logs, field_value, has_field};

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
