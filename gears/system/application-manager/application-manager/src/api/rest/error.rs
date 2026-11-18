//! Map domain refusals to canonical errors (DESIGN 3.3 "Refusal bodies").
//!
//! Every refusal is a canonical Problem with no extension member, carrying
//! the request id. Temporarily unavailable carries
//! [`REFUSAL_RETRY_AFTER_SECONDS`]; not permitted names the action. The
//! permission-denied builder fixes the Problem `detail`, so the request id and
//! action go in its `reason`.

use toolkit_canonical_errors::{CanonicalError, resource_error};
use uuid::Uuid;

use crate::constants::REFUSAL_RETRY_AFTER_SECONDS;
use crate::domain::error::DomainError;

/// Errors attributable to the application resource. The literal mirrors
/// [`application_manager_sdk::APPLICATION_RESOURCE_TYPE`] (proc-macros cannot
/// resolve a const); a test pins the two equal.
#[resource_error(gts_id!("cf.core.application_manager.application.v1~"))]
pub struct ApplicationError;

/// The canonical error for `err`, tagged with `request_id`.
#[must_use]
pub fn to_canonical(err: &DomainError, request_id: Uuid) -> CanonicalError {
    match err {
        DomainError::NotReady => unavailable(request_id, "the application manager is not ready"),
        DomainError::NotPermitted { action } => ApplicationError::permission_denied()
            .with_reason(format!(
                "request {request_id}: action {action} is not permitted"
            ))
            .create(),
    }
}

fn unavailable(request_id: Uuid, what: &str) -> CanonicalError {
    CanonicalError::service_unavailable()
        .with_detail(format!("request {request_id}: {what}"))
        .with_retry_after_seconds(REFUSAL_RETRY_AFTER_SECONDS)
        .create()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "error_tests.rs"]
mod tests;
