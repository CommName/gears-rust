//! The outcome table of DESIGN 3.3 as pure functions, one per decision point.
//!
//! The first matching rank decides. Nothing here does I/O.

use application_manager_sdk::{RequestCategory, SectionOutcomeEntry, UnavailableItem};
use toolkit_macros::domain_model;

use crate::domain::actions::Action;
use crate::domain::error::DomainError;
use crate::domain::operation::OpenOperation;
use crate::domain::ports::{AccessError, StorageError};
use crate::domain::router::UnavailableReason;
use crate::domain::schema_validator::Problem;

/// What the gear's own gate found.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// Ready, not stopping, with a free request-task permit.
    Open,
    /// Startup has not finished, or `stop` has begun (rank 1).
    NotReady,
    /// No free request-task permit (rank 1).
    AtCapacity,
}

/// Everything the synchronous ranks 1 to 4 look at.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncFacts {
    /// The action asked for, named by a rank 2 refusal.
    pub action: Action,
    /// The gate.
    pub gate: Gate,
    /// The access control answer.
    pub access: Result<(), AccessError>,
    /// The storage step of a permitted caller: the open operation of the
    /// application. `None` when it was not consulted.
    pub open: Option<Result<Option<OpenOperation>, StorageError>>,
    /// Envelope problems, or an uninstall id outside the limits.
    pub envelope_problems: Vec<Problem>,
}

/// The refusal of ranks 1 to 4, in DESIGN order, or `None` to go on to
/// acceptance. A storage answer counts only for a permitted caller; an open
/// operation past its run deadline is not a conflict.
#[must_use]
pub fn sync_refusal(_facts: &SyncFacts) -> Option<DomainError> {
    todo!("PR 5: ranks 1 to 4")
}

/// The category a refused event records for `err`; a gear defect records
/// internal failure.
#[must_use]
pub fn category_of(_err: &DomainError) -> RequestCategory {
    todo!("PR 5: category of a refusal")
}

/// What one section plugin's `schema` gave, after checking.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaState {
    /// A usable schema.
    Usable,
    /// No schema.
    Unavailable(UnavailableReason),
    /// The plugin refused the caller.
    NotPermitted,
}

/// The decision of ranks 5 and 6.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaStage {
    /// `temporarily_unavailable` (rank 5) or `not_permitted` (rank 6).
    pub category: RequestCategory,
    /// The unavailable schemas of rank 5, in plugin order; empty for rank 6.
    pub unavailable: Vec<UnavailableItem>,
}

/// Ranks 5 and 6 over the `(key, state)` of every plugin in plugin order:
/// unavailable wins over not permitted; `None` when every schema is usable.
#[must_use]
pub fn schema_category(_states: &[(&str, SchemaState)]) -> Option<SchemaStage> {
    todo!("PR 5: ranks 5 and 6")
}

/// The category after dispatch: not permitted, then rejected, then internal
/// failure (internal failure, timed out, not sent, not dispatched), else
/// succeeded.
#[must_use]
pub fn dispatch_category(_sections: &[SectionOutcomeEntry]) -> RequestCategory {
    todo!("PR 5: after-dispatch ranks")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "outcome_tests.rs"]
mod tests;
