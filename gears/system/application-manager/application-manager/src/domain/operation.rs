//! The operation record, the run window, and the open-operation view
//! (DESIGN 3.1, 4.2).

use std::time::Duration;

use application_manager_sdk::{OperationAction, OutcomeReport, RequestCategory};
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
    /// Final, ended at the run deadline.
    Interrupted,
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
    /// Receive time.
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
    /// Receive time.
    pub accepted_at: OffsetDateTime,
    /// End of the run window.
    pub run_deadline: OffsetDateTime,
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

/// The run deadline does not fit the time range.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("run deadline overflows the time range")]
pub struct RunDeadlineOverflow;

/// The run deadline: `commit_time` plus (N + 1) x `plugin_call_timeout` for a
/// seed, or N x `plugin_call_timeout` for an uninstall, plus `margin`, with N
/// the number of plugins (DESIGN 4.2).
///
/// # Errors
/// [`RunDeadlineOverflow`] when any step of the arithmetic overflows.
pub fn run_deadline(
    commit_time: OffsetDateTime,
    action: OperationAction,
    plugin_count: usize,
    plugin_call_timeout: Duration,
    margin: Duration,
) -> Result<OffsetDateTime, RunDeadlineOverflow> {
    let timeouts = match action {
        OperationAction::Uninstall => plugin_count,
        // A seed also owns one call for the key set.
        _ => plugin_count.checked_add(1).ok_or(RunDeadlineOverflow)?,
    };
    let timeouts = u32::try_from(timeouts).map_err(|_| RunDeadlineOverflow)?;
    let window = plugin_call_timeout
        .checked_mul(timeouts)
        .and_then(|calls| calls.checked_add(margin))
        .ok_or(RunDeadlineOverflow)?;
    let window = time::Duration::try_from(window).map_err(|_| RunDeadlineOverflow)?;
    commit_time.checked_add(window).ok_or(RunDeadlineOverflow)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "operation_tests.rs"]
mod tests;
