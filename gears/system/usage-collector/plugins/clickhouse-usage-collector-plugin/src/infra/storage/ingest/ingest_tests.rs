// Test modules using bare `panic!` opt in explicitly.
#![allow(clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use usage_collector_sdk::UsageCollectorPluginError;

use super::admission::{Outstanding, check, estimated_row_bytes, rejection};
use super::state::Phase;
use super::{IngestLimits, IngestQueue, PendingCall, remaining_budget};
use crate::infra::metrics::{AdmissionReason, Metrics};
use crate::infra::storage::ingest::test_rows::{row, rows};

/// Small limits so every bound is reachable in a test.
pub fn limits() -> IngestLimits {
    IngestLimits {
        max_rows: 4,
        max_bytes: 1 << 20,
        max_wait: Duration::from_millis(50),
        queue_capacity: 3,
        max_concurrent_flushes: 2,
        max_outstanding_rows: 12,
        max_outstanding_bytes: 3 << 20,
        drain_deadline: Duration::from_secs(5),
    }
}

// ── admission ────────────────────────────────────────────────────────────────

/// The estimate grows with every string and map entry, and is the same for
/// two rows that differ only in fixed-width columns.
#[test]
fn estimated_row_bytes_counts_strings_and_metadata() {
    let base = row(1);
    let mut longer = row(2);
    longer.resource_id.push_str("-suffix");
    assert_eq!(
        estimated_row_bytes(&longer),
        estimated_row_bytes(&base) + "-suffix".len()
    );
    let mut with_metadata = row(3);
    with_metadata
        .metadata
        .insert("region".to_owned(), "eu".to_owned());
    assert_eq!(
        estimated_row_bytes(&with_metadata),
        estimated_row_bytes(&base) + "region".len() + "eu".len() + 2
    );
    let mut other_value = row(1);
    other_value.value = rust_decimal::Decimal::new(7, 0);
    assert_eq!(
        estimated_row_bytes(&other_value),
        estimated_row_bytes(&base)
    );
}

/// Checks run closed → oversize → calls → rows → bytes, each independently.
#[test]
fn admission_check_orders_closed_oversize_calls_rows_bytes() {
    let l = limits();
    let empty = Outstanding::default();
    assert_eq!(
        check(&l, empty, 1, 1, Phase::Draining),
        Err(AdmissionReason::Closed)
    );
    assert_eq!(
        check(&l, empty, 1, 1, Phase::Closed),
        Err(AdmissionReason::Closed)
    );
    // Oversize beats every other bound, even on an empty queue.
    assert_eq!(
        check(&l, empty, 13, 1, Phase::Open),
        Err(AdmissionReason::Oversize)
    );
    assert_eq!(
        check(&l, empty, 1, (3 << 20) + 1, Phase::Open),
        Err(AdmissionReason::Oversize)
    );
    let full_calls = Outstanding {
        calls: 3,
        rows: 0,
        bytes: 0,
    };
    assert_eq!(
        check(&l, full_calls, 1, 1, Phase::Open),
        Err(AdmissionReason::Calls)
    );
    let full_rows = Outstanding {
        calls: 1,
        rows: 10,
        bytes: 0,
    };
    assert_eq!(
        check(&l, full_rows, 3, 1, Phase::Open),
        Err(AdmissionReason::Rows)
    );
    assert_eq!(check(&l, full_rows, 2, 1, Phase::Open), Ok(()));
    let full_bytes = Outstanding {
        calls: 1,
        rows: 0,
        bytes: 3 << 20,
    };
    assert_eq!(
        check(&l, full_bytes, 1, 1, Phase::Open),
        Err(AdmissionReason::Bytes)
    );
    // A call larger than a flush target but inside the outstanding bound is
    // admitted (it will be flushed alone).
    assert_eq!(check(&l, empty, 5, 1, Phase::Open), Ok(()));
}

/// Every bound and the closed queue are retryable with a hint; an oversize
/// call is permanent.
#[test]
fn rejections_are_transient_with_a_retry_hint_except_oversize() {
    for reason in [
        AdmissionReason::Closed,
        AdmissionReason::Calls,
        AdmissionReason::Rows,
        AdmissionReason::Bytes,
    ] {
        match rejection(reason) {
            UsageCollectorPluginError::Transient {
                retry_after_seconds,
                ..
            } => assert_eq!(retry_after_seconds, Some(1), "{reason:?}"),
            other => panic!("{reason:?} must be transient, got {other:?}"),
        }
    }
    assert!(matches!(
        rejection(AdmissionReason::Oversize),
        UsageCollectorPluginError::Internal(_)
    ));
}

// ── submit / wait ────────────────────────────────────────────────────────────

/// A queue nothing drains still admits within its bounds and refuses beyond
/// them, counting each refusal under its reason.
#[tokio::test(start_paused = true)]
async fn submit_enforces_the_bounds_and_counts_rejections() {
    use opentelemetry::metrics::MeterProvider as _;

    use crate::infra::metrics::label;
    use crate::infra::metrics::metrics_tests::{counter_sum_with_label, local_provider};

    let (provider, exporter) = local_provider();
    let metrics = Arc::new(Metrics::with_meter(
        &provider.meter(crate::infra::metrics::SCOPE_NAME),
    ));
    let queue = IngestQueue::new(limits(), metrics);

    // Three calls fill `queue_capacity`; the fourth is refused on calls.
    let kept: Vec<_> = (0..3)
        .map(|i| {
            queue
                .submit(PendingCall {
                    rows: rows(i * 10, 1),
                })
                .unwrap()
        })
        .collect();
    let err = queue
        .submit(PendingCall { rows: rows(100, 1) })
        .unwrap_err();
    assert!(matches!(err, UsageCollectorPluginError::Transient { .. }));

    // An oversize call is refused as internal, whatever else is queued.
    let err = queue
        .submit(PendingCall {
            rows: rows(200, 13),
        })
        .unwrap_err();
    assert!(matches!(err, UsageCollectorPluginError::Internal(_)));

    provider.force_flush().unwrap();
    assert_eq!(
        counter_sum_with_label(
            &exporter,
            "uc_clickhouse_admission_rejected_total",
            label::REASON,
            label::REASON_CALLS
        ),
        1
    );
    assert_eq!(
        counter_sum_with_label(
            &exporter,
            "uc_clickhouse_admission_rejected_total",
            label::REASON,
            label::REASON_OVERSIZE
        ),
        1
    );
    let snapshot = queue.snapshot();
    assert_eq!(snapshot.queued_calls, 3);
    assert_eq!(snapshot.queued_rows, 3);
    drop(kept);
}

/// With no batcher running a call waits out its budget and gets `Transient`.
#[tokio::test(start_paused = true)]
async fn wait_times_out_with_a_transient_error_when_nothing_flushes() {
    let queue = IngestQueue::new(limits(), Arc::new(Metrics::new()));
    let submitted = queue.submit(PendingCall { rows: rows(0, 1) }).unwrap();
    let err = submitted.wait(Duration::from_millis(10)).await.unwrap_err();
    assert!(matches!(err, UsageCollectorPluginError::Transient { .. }));
}

/// Once admission is closed every submit is refused as `Transient`.
#[tokio::test(start_paused = true)]
async fn submit_after_close_admission_is_transient() {
    let queue = IngestQueue::new(limits(), Arc::new(Metrics::new()));
    queue.close_admission();
    let err = queue.submit(PendingCall { rows: rows(0, 1) }).unwrap_err();
    assert!(matches!(err, UsageCollectorPluginError::Transient { .. }));
}

/// The queue budget is what is left of the client deadline after the
/// pre-checks, and never goes negative.
#[tokio::test(start_paused = true)]
async fn remaining_budget_is_measured_from_entry_and_saturates() {
    let entered = tokio::time::Instant::now();
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(
        remaining_budget(entered, Duration::from_secs(5)),
        Duration::from_secs(3)
    );
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        remaining_budget(entered, Duration::from_secs(5)),
        Duration::ZERO
    );
}
