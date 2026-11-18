#![allow(clippy::unwrap_used, clippy::expect_used)]

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

use super::*;

fn provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    (provider, exporter)
}

/// `read` applied to the last export of the metric `name`.
fn last<R>(
    exporter: &InMemoryMetricExporter,
    name: &str,
    read: impl FnOnce(&AggregatedMetrics) -> R,
) -> R {
    let finished = exporter.get_finished_metrics().unwrap();
    let metric = finished
        .iter()
        .rev()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .find(|metric| metric.name() == name)
        .unwrap_or_else(|| panic!("{name} was not exported"));
    read(metric.data())
}

/// Each data point of the u64 sum `name`, as sorted `key=value` labels.
fn sum(exporter: &InMemoryMetricExporter, name: &str) -> Vec<(String, u64)> {
    last(exporter, name, |metric| {
        let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric else {
            panic!("{name} is not a u64 sum");
        };
        let mut points: Vec<(String, u64)> = sum
            .data_points()
            .map(|point| {
                let mut labels: Vec<String> = point
                    .attributes()
                    .map(|kv| format!("{}={}", kv.key, kv.value))
                    .collect();
                labels.sort();
                (labels.join(","), point.value())
            })
            .collect();
        points.sort();
        points
    })
}

#[test]
fn every_signal_reaches_its_instrument_with_its_labels() {
    let (provider, exporter) = provider();
    let signals = MeterSignals::new(&provider.meter(METER_NAME));

    signals.request("seed", "accepted");
    signals.request("seed", "accepted");
    signals.operation_completed("seed", "succeeded", "run");
    signals.operation_duration("seed", Duration::from_millis(250));
    signals.section_call("noop", "timed_out");
    signals.caller_refused("read_history");
    signals.request_tasks(1);
    signals.request_tasks(1);
    signals.request_tasks(-1);
    signals.history_event(true);
    signals.history_event(false);
    signals.dependency_unavailable("pdp");
    signals.sweep("expire", false);
    signals.operations_expired(3);
    signals.startup_failed("plugin_conflict");
    provider.force_flush().unwrap();

    assert_eq!(
        sum(&exporter, REQUESTS),
        vec![("action=seed,outcome=accepted".to_owned(), 2)]
    );
    assert_eq!(
        sum(&exporter, OPERATIONS_COMPLETED),
        vec![("action=seed,category=succeeded,reason=run".to_owned(), 1)]
    );
    assert_eq!(
        sum(&exporter, SECTION_CALLS),
        vec![("outcome=timed_out,section_key=noop".to_owned(), 1)]
    );
    assert_eq!(
        sum(&exporter, CALLER_REFUSALS),
        vec![("action=read_history".to_owned(), 1)]
    );
    assert_eq!(
        sum(&exporter, HISTORY_EVENTS),
        vec![
            ("result=not_recorded".to_owned(), 1),
            ("result=recorded".to_owned(), 1)
        ]
    );
    assert_eq!(
        sum(&exporter, DEPENDENCY_UNAVAILABLE),
        vec![("dependency=pdp".to_owned(), 1)]
    );
    assert_eq!(
        sum(&exporter, SWEEPS),
        vec![("result=failed,step=expire".to_owned(), 1)]
    );
    assert_eq!(sum(&exporter, OPERATIONS_EXPIRED), vec![(String::new(), 3)]);
    assert_eq!(
        sum(&exporter, STARTUP_FAILURES),
        vec![("reason=plugin_conflict".to_owned(), 1)]
    );
    let in_flight = last(&exporter, REQUEST_TASKS, |metric| {
        let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric else {
            panic!("{REQUEST_TASKS} is not an i64 sum");
        };
        sum.data_points()
            .map(opentelemetry_sdk::metrics::data::SumDataPoint::value)
            .sum::<i64>()
    });
    assert_eq!(in_flight, 1);
    let (count, total) = last(&exporter, OPERATION_DURATION, |metric| {
        let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric else {
            panic!("{OPERATION_DURATION} is not an f64 histogram");
        };
        histogram
            .data_points()
            .fold((0, 0.0), |(c, s), p| (c + p.count(), s + p.sum()))
    });
    assert_eq!(count, 1);
    assert!((total - 0.25).abs() < 1e-9);
}

#[test]
fn the_global_meter_accepts_every_signal() {
    let signals = MeterSignals::from_global();
    assert!(!format!("{signals:?}").is_empty());
    signals.request("uninstall", "conflict");
    signals.sweep("purge", true);
}
