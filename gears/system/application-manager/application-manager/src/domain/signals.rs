//! Operational signals (DESIGN 4.1): the counts the gear raises, as a port so
//! the domain records them without a metrics library. Labels are fixed tokens
//! and section keys; application ids and operation ids never are.

use std::time::Duration;

use application_manager_sdk::{OperationAction, RequestCategory, SectionOutcome};

/// What the gear counts.
pub trait Signals: Send + Sync {
    /// One synchronous answer to a seed or uninstall: `outcome` is
    /// `accepted` or the refusal's category.
    fn request(&self, action: &'static str, outcome: &'static str);

    /// One operation completed with `category`; `reason` is `run`,
    /// `supervisor` or `expired`.
    fn operation_completed(
        &self,
        action: &'static str,
        category: &'static str,
        reason: &'static str,
    );

    /// How long one operation ran, from acceptance to its final transaction.
    fn operation_duration(&self, action: &'static str, took: Duration);

    /// One section outcome of section key `key`, including `timed_out` and
    /// `not_sent`.
    fn section_call(&self, key: &str, outcome: &'static str);

    /// One request the allowed callers or the PDP refused for `action`.
    fn caller_refused(&self, action: &'static str);

    /// A request task started (`1`) or ended (`-1`).
    fn request_tasks(&self, delta: i64);

    /// One history event written (`true`) or not written (`false`).
    fn history_event(&self, recorded: bool);

    /// One call a dependency could not answer: `storage` or `pdp`.
    fn dependency_unavailable(&self, dependency: &'static str);

    /// One sweeper step (`expire` or `purge`) and whether it succeeded.
    fn sweep(&self, step: &'static str, succeeded: bool);

    /// `count` open operations completed by the expiry completion.
    fn operations_expired(&self, count: u64);

    /// A startup that failed with `reason`.
    fn startup_failed(&self, reason: &'static str);
}

/// Signals that go nowhere.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSignals;

impl Signals for NoSignals {
    fn request(&self, _action: &'static str, _outcome: &'static str) {}
    fn operation_completed(
        &self,
        _action: &'static str,
        _category: &'static str,
        _reason: &'static str,
    ) {
    }
    fn operation_duration(&self, _action: &'static str, _took: Duration) {}
    fn section_call(&self, _key: &str, _outcome: &'static str) {}
    fn caller_refused(&self, _action: &'static str) {}
    fn request_tasks(&self, _delta: i64) {}
    fn history_event(&self, _recorded: bool) {}
    fn dependency_unavailable(&self, _dependency: &'static str) {}
    fn sweep(&self, _step: &'static str, _succeeded: bool) {}
    fn operations_expired(&self, _count: u64) {}
    fn startup_failed(&self, _reason: &'static str) {}
}

/// The label of `action`.
#[must_use]
pub const fn action_label(action: OperationAction) -> &'static str {
    match action {
        OperationAction::Uninstall => "uninstall",
        _ => "seed",
    }
}

/// The label of `category`.
#[must_use]
pub const fn category_label(category: RequestCategory) -> &'static str {
    match category {
        RequestCategory::Succeeded => "succeeded",
        RequestCategory::InvalidManifest => "invalid_manifest",
        RequestCategory::PluginRejected => "plugin_rejected",
        RequestCategory::NotPermitted => "not_permitted",
        RequestCategory::Conflict => "conflict",
        RequestCategory::TemporarilyUnavailable => "temporarily_unavailable",
        _ => "internal_failure",
    }
}

/// The label of `outcome`.
#[must_use]
pub const fn outcome_label(outcome: SectionOutcome) -> &'static str {
    match outcome {
        SectionOutcome::Succeeded => "succeeded",
        SectionOutcome::Rejected => "rejected",
        SectionOutcome::NotPermitted => "not_permitted",
        SectionOutcome::TimedOut => "timed_out",
        SectionOutcome::NotSent => "not_sent",
        SectionOutcome::NotDispatched => "not_dispatched",
        SectionOutcome::Lost => "lost",
        _ => "internal_failure",
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "signals_tests.rs"]
mod tests;
