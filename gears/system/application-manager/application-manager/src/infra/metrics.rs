//! OpenTelemetry adapter implementing [`Signals`] (DESIGN 4.1).
//!
//! Instruments come from the process-global meter provider the host installs;
//! they are no-ops until an exporter is wired. Names are full literal
//! Prometheus names: counters end in `_total`, the duration histogram in
//! `_seconds`. Labels are fixed tokens and section keys; application ids and
//! operation ids never are.

use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

use crate::domain::signals::Signals;

/// Meter / instrumentation scope name.
pub const METER_NAME: &str = "application_manager";

/// Synchronous answers to seed and uninstall, by `action` and `outcome`.
pub const REQUESTS: &str = "application_manager_requests_total";
/// Completed operations, by `action`, `category` and `reason`.
pub const OPERATIONS_COMPLETED: &str = "application_manager_operations_completed_total";
/// Operation run time, from acceptance to the final transaction, by `action`.
pub const OPERATION_DURATION: &str = "application_manager_operation_duration_seconds";
/// Section outcomes, by `section_key` and `outcome`.
pub const SECTION_CALLS: &str = "application_manager_section_calls_total";
/// Requests the allowed callers or the PDP refused, by `action`.
pub const CALLER_REFUSALS: &str = "application_manager_caller_refusals_total";
/// Request tasks in flight.
pub const REQUEST_TASKS: &str = "application_manager_request_tasks";
/// History events, by `result` (`recorded` or `not_recorded`).
pub const HISTORY_EVENTS: &str = "application_manager_history_events_total";
/// Calls a dependency could not answer, by `dependency`.
pub const DEPENDENCY_UNAVAILABLE: &str = "application_manager_dependency_unavailable_total";
/// Sweeper steps, by `step` and `result`.
pub const SWEEPS: &str = "application_manager_sweeps_total";
/// Open operations completed by the expiry completion.
pub const OPERATIONS_EXPIRED: &str = "application_manager_operations_expired_total";
/// Failed startups, by `reason`.
pub const STARTUP_FAILURES: &str = "application_manager_startup_failures_total";

/// OpenTelemetry-backed [`Signals`].
pub struct MeterSignals {
    requests: Counter<u64>,
    operations_completed: Counter<u64>,
    operation_duration: Histogram<f64>,
    section_calls: Counter<u64>,
    caller_refusals: Counter<u64>,
    request_tasks: UpDownCounter<i64>,
    history_events: Counter<u64>,
    dependency_unavailable: Counter<u64>,
    sweeps: Counter<u64>,
    operations_expired: Counter<u64>,
    startup_failures: Counter<u64>,
}

impl std::fmt::Debug for MeterSignals {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeterSignals").finish_non_exhaustive()
    }
}

impl MeterSignals {
    /// The instrument set of `meter`.
    #[must_use]
    pub fn new(meter: &Meter) -> Self {
        let counter = |name: &'static str, description: &'static str| {
            meter
                .u64_counter(name)
                .with_description(description)
                .build()
        };
        Self {
            requests: counter(REQUESTS, "Synchronous answers to seed and uninstall"),
            operations_completed: counter(OPERATIONS_COMPLETED, "Completed operations"),
            operation_duration: meter
                .f64_histogram(OPERATION_DURATION)
                .with_description("Operation run time, from acceptance to completion")
                .build(),
            section_calls: counter(SECTION_CALLS, "Section outcomes by section key"),
            caller_refusals: counter(CALLER_REFUSALS, "Requests refused for the caller"),
            request_tasks: meter
                .i64_up_down_counter(REQUEST_TASKS)
                .with_description("Request tasks in flight")
                .build(),
            history_events: counter(HISTORY_EVENTS, "History events written or not written"),
            dependency_unavailable: counter(
                DEPENDENCY_UNAVAILABLE,
                "Calls a dependency could not answer",
            ),
            sweeps: counter(SWEEPS, "Operation sweeper steps"),
            operations_expired: counter(
                OPERATIONS_EXPIRED,
                "Open operations completed by the expiry completion",
            ),
            startup_failures: counter(STARTUP_FAILURES, "Failed startups"),
        }
    }

    /// The instrument set of the process-global meter provider.
    #[must_use]
    pub fn from_global() -> Self {
        Self::new(&opentelemetry::global::meter(METER_NAME))
    }
}

impl Signals for MeterSignals {
    fn request(&self, action: &'static str, outcome: &'static str) {
        self.requests.add(
            1,
            &[
                KeyValue::new("action", action),
                KeyValue::new("outcome", outcome),
            ],
        );
    }

    fn operation_completed(
        &self,
        action: &'static str,
        category: &'static str,
        reason: &'static str,
    ) {
        self.operations_completed.add(
            1,
            &[
                KeyValue::new("action", action),
                KeyValue::new("category", category),
                KeyValue::new("reason", reason),
            ],
        );
    }

    fn operation_duration(&self, action: &'static str, took: Duration) {
        self.operation_duration
            .record(took.as_secs_f64(), &[KeyValue::new("action", action)]);
    }

    fn section_call(&self, key: &str, outcome: &'static str) {
        self.section_calls.add(
            1,
            &[
                KeyValue::new("section_key", key.to_owned()),
                KeyValue::new("outcome", outcome),
            ],
        );
    }

    fn caller_refused(&self, action: &'static str) {
        self.caller_refusals
            .add(1, &[KeyValue::new("action", action)]);
    }

    fn request_tasks(&self, delta: i64) {
        self.request_tasks.add(delta, &[]);
    }

    fn history_event(&self, recorded: bool) {
        let result = if recorded { "recorded" } else { "not_recorded" };
        self.history_events
            .add(1, &[KeyValue::new("result", result)]);
    }

    fn dependency_unavailable(&self, dependency: &'static str) {
        self.dependency_unavailable
            .add(1, &[KeyValue::new("dependency", dependency)]);
    }

    fn sweep(&self, step: &'static str, succeeded: bool) {
        let result = if succeeded { "succeeded" } else { "failed" };
        self.sweeps.add(
            1,
            &[KeyValue::new("step", step), KeyValue::new("result", result)],
        );
    }

    fn operations_expired(&self, count: u64) {
        self.operations_expired.add(count, &[]);
    }

    fn startup_failed(&self, reason: &'static str) {
        self.startup_failures
            .add(1, &[KeyValue::new("reason", reason)]);
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "metrics_tests.rs"]
mod tests;
