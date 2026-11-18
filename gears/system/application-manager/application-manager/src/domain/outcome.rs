//! Outcome ranking (DESIGN 3.3 "Outcome table"): the synchronous ranks of a
//! seed or uninstall, as a pure function of the collected facts, and the
//! category each refusal records. The categories after dispatch are
//! [`crate::domain::operation::completed_category`].

use application_manager_sdk::RequestCategory;
use toolkit_macros::domain_model;

use crate::domain::actions::CallerAction;
use crate::domain::error::DomainError;
use crate::domain::operation::OpenOperation;
use crate::domain::ports::StorageError;
use crate::domain::schema_validator::Problem;

/// What the request gate decided.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    /// A request-task permit was taken.
    Open,
    /// Startup has not finished, or `stop` has begun.
    NotReady,
    /// No request-task permit is free.
    AtCapacity,
}

/// The facts the synchronous phase collects, in the order it collects them.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncFacts {
    /// What the request asks for.
    pub action: CallerAction,
    /// The gate's answer.
    pub gate: Gate,
    /// Whether the allowed callers grant `action`.
    pub allowed: bool,
    /// The open-operation step, run only for an allowed caller.
    pub open: Option<Result<Option<OpenOperation>, StorageError>>,
    /// Problems of the envelope or the uninstall id.
    pub envelope_problems: Vec<Problem>,
}

/// The synchronous refusal of `facts`, or `None` when the request goes on to
/// acceptance. The first matching rank decides (DESIGN 3.3):
///
/// 1. not ready or at capacity, or storage cannot answer for an allowed
///    caller;
/// 2. the caller is not allowed;
/// 3. an open operation before its run deadline (an expired one is completed
///    by the acceptance, not a conflict);
/// 4. an envelope problem or an uninstall id outside the limits.
#[must_use]
pub fn sync_refusal(facts: &SyncFacts) -> Option<DomainError> {
    match facts.gate {
        Gate::NotReady => return Some(DomainError::NotReady),
        Gate::AtCapacity => return Some(DomainError::AtCapacity),
        Gate::Open => {}
    }
    if facts.allowed && matches!(facts.open, Some(Err(_))) {
        return Some(DomainError::StorageUnavailable);
    }
    if !facts.allowed {
        return Some(DomainError::NotPermitted {
            action: facts.action,
        });
    }
    if let Some(Ok(Some(open))) = facts.open
        && !open.expired
    {
        return Some(DomainError::Conflict {
            operation_id: open.operation_id,
        });
    }
    if !facts.envelope_problems.is_empty() {
        return Some(DomainError::InvalidManifest {
            problems: facts.envelope_problems.clone(),
        });
    }
    None
}

/// The category a refused event records for `refusal`. A gear defect is not
/// ranked and records internal failure.
#[must_use]
pub fn category_of(refusal: &DomainError) -> RequestCategory {
    match refusal {
        DomainError::NotReady | DomainError::AtCapacity | DomainError::StorageUnavailable => {
            RequestCategory::TemporarilyUnavailable
        }
        DomainError::NotPermitted { .. } => RequestCategory::NotPermitted,
        DomainError::Conflict { .. } => RequestCategory::Conflict,
        DomainError::InvalidManifest { .. } => RequestCategory::InvalidManifest,
        DomainError::UnknownSectionKey | DomainError::OperationNotFound | DomainError::Defect => {
            RequestCategory::InternalFailure
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "outcome_tests.rs"]
mod tests;
