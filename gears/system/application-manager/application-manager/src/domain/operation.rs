//! The operation record, the application record, the run window, and the
//! completion rules (DESIGN 3.1, 3.7, 4.2).

use std::time::Duration;

use application_manager_sdk::{
    OperationAction, OutcomeReport, RequestCategory, SectionOutcome, SectionOutcomeEntry,
};
use time::OffsetDateTime;
use toolkit_macros::domain_model;
use uuid::Uuid;

/// State of an operation record.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    /// Committed with the accepted event; not yet started.
    Accepted,
    /// Marked running.
    Running,
    /// Final, with a category.
    Completed,
}

/// One operation record (`operations` table, DESIGN 3.7). It holds no manifest
/// content and no caller identity.
#[domain_model]
#[derive(Debug, Clone, PartialEq)]
#[allow(
    clippy::struct_field_names,
    reason = "field names are the DESIGN 3.7 column names"
)]
pub struct Operation {
    /// Time-ordered operation id.
    pub operation_id: Uuid,
    /// Exact application id.
    pub application_id: String,
    /// `seed` or `uninstall`.
    pub action: OperationAction,
    /// Current status.
    pub status: OperationStatus,
    /// Set exactly when `completed`.
    pub category: Option<RequestCategory>,
    /// The outcome report so far.
    pub report: OutcomeReport,
    /// Database time of the acceptance.
    pub accepted_at: OffsetDateTime,
    /// Set by mark running.
    pub started_at: Option<OffsetDateTime>,
    /// Set exactly when final.
    pub finished_at: Option<OffsetDateTime>,
    /// End of the run window.
    pub run_deadline: OffsetDateTime,
    /// Id of the finished event.
    pub finished_event_id: Uuid,
}

/// What the acceptance transaction inserts.
#[domain_model]
#[derive(Debug, Clone, PartialEq)]
pub struct NewOperation {
    /// Operation id, assigned at receive.
    pub operation_id: Uuid,
    /// Exact application id.
    pub application_id: String,
    /// `seed` or `uninstall`.
    pub action: OperationAction,
    /// The run window ([`run_window`]); the run deadline is the database time
    /// of the acceptance plus this window.
    pub run_window: Duration,
    /// Id of the finished event, assigned at receive.
    pub finished_event_id: Uuid,
    /// The initial report.
    pub report: OutcomeReport,
}

/// The open operation of an application.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOperation {
    /// Its id.
    pub operation_id: Uuid,
    /// `run_deadline <= now`.
    pub expired: bool,
}

/// What the acceptance transaction wrote, by database time.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accepted {
    /// Database time of the acceptance.
    pub accepted_at: OffsetDateTime,
    /// End of the run window.
    pub run_deadline: OffsetDateTime,
}

/// Why an operation completed outside its final transaction.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionReason {
    /// The expiry completion: the run deadline passed with the operation open.
    Expired,
}

/// The last finished seed of one application (`applications` table, DESIGN
/// 3.7).
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationRecord {
    /// Exact application id.
    pub application_id: String,
    /// Version of the last finished seed, as sent.
    pub version: String,
    /// That seed's operation.
    pub operation_id: Uuid,
    /// That seed's category.
    pub category: RequestCategory,
    /// When that seed finished.
    pub finished_at: OffsetDateTime,
}

/// The run window does not fit the duration range.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("run window overflows the duration range")]
pub struct RunWindowOverflow;

/// The run window of an operation over `plugins` section plugins: N x
/// (`plugin_call_timeout` + `storage_call_timeout`) for the plugin calls and
/// their outcome writes, plus 2 x `storage_call_timeout` for mark running and
/// the final transaction, plus `margin`, for a seed or an uninstall (DESIGN
/// 4.2). The run deadline is the database time read in the acceptance
/// transaction plus this window.
///
/// # Errors
/// [`RunWindowOverflow`] when any step of the arithmetic overflows.
pub fn run_window(
    plugins: usize,
    plugin_call_timeout: Duration,
    storage_call_timeout: Duration,
    margin: Duration,
) -> Result<Duration, RunWindowOverflow> {
    let plugins = u32::try_from(plugins).map_err(|_| RunWindowOverflow)?;
    plugin_call_timeout
        .checked_add(storage_call_timeout)
        .and_then(|per_plugin| per_plugin.checked_mul(plugins))
        .and_then(|calls| calls.checked_add(storage_call_timeout.checked_mul(2)?))
        .and_then(|window| window.checked_add(margin))
        .ok_or(RunWindowOverflow)
}

/// The category of an operation completed after dispatch, from its section
/// outcomes, by the outcome table's precedence (DESIGN 3.3): a plugin's
/// `not_permitted`, then `rejected`, then any internal failure (`internal_failure`,
/// `timed_out`, `lost`, `not_sent`, `not_dispatched`, or a plugin with no
/// outcome); succeeded only when every plugin succeeded.
#[must_use]
pub fn completed_category(sections: &[SectionOutcomeEntry]) -> RequestCategory {
    let outcomes: Vec<Option<SectionOutcome>> = sections.iter().map(|s| s.outcome).collect();
    if outcomes.contains(&Some(SectionOutcome::NotPermitted)) {
        RequestCategory::NotPermitted
    } else if outcomes.contains(&Some(SectionOutcome::Rejected)) {
        RequestCategory::PluginRejected
    } else if outcomes
        .iter()
        .all(|outcome| *outcome == Some(SectionOutcome::Succeeded))
    {
        RequestCategory::Succeeded
    } else {
        RequestCategory::InternalFailure
    }
}

/// The category of an expiry completion: as [`completed_category`], but never
/// succeeded, since the expiry completion is itself an internal failure (DESIGN
/// 3.3, after dispatch 3). An operation that expired while `accepted` has no
/// section entries and is an internal failure.
#[must_use]
pub fn expiry_category(sections: &[SectionOutcomeEntry]) -> RequestCategory {
    match completed_category(sections) {
        RequestCategory::Succeeded => RequestCategory::InternalFailure,
        category => category,
    }
}

/// Give every section without an outcome `not_dispatched`, or `lost` for the
/// first such section when the operation was `running`: that plugin may have
/// been called when the request task stopped (DESIGN 3.1 "Expiry
/// completion").
pub fn complete_open_sections(report: &mut OutcomeReport, running: bool) {
    let mut first = running;
    for entry in report.sections.iter_mut().filter(|e| e.outcome.is_none()) {
        entry.outcome = Some(if first {
            SectionOutcome::Lost
        } else {
            SectionOutcome::NotDispatched
        });
        first = false;
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "operation_tests.rs"]
mod tests;
