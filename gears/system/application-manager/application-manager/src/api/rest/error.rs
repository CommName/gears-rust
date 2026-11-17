//! Map domain refusals to canonical errors (DESIGN 3.3 "Refusal bodies").
//!
//! Every refusal is a canonical Problem with no extension member, carrying
//! the request id. Temporarily unavailable carries
//! [`REFUSAL_RETRY_AFTER_SECONDS`]; not permitted names the action; a
//! conflict has reason `application_busy` and names the open operation; an
//! invalid manifest lists its field violations. The permission-denied builder
//! fixes the Problem `detail`, so the request id and action go in its
//! `reason`; the invalid-argument builder fixes it too, so the request id is
//! its first field violation.

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
        DomainError::AtCapacity => {
            unavailable(request_id, "the application manager is at capacity")
        }
        DomainError::StorageUnavailable => {
            unavailable(request_id, "operation storage cannot answer")
        }
        DomainError::NotPermitted { action } => ApplicationError::permission_denied()
            .with_reason(format!(
                "request {request_id}: action {action} is not permitted"
            ))
            .create(),
        DomainError::Conflict { operation_id } => ApplicationError::aborted(format!(
            "request {request_id}: operation {operation_id} of the application is open"
        ))
        .with_reason("application_busy")
        .create(),
        DomainError::InvalidManifest { problems } => {
            // The invalid-argument builder fixes `detail`, so the request id
            // is the first field violation.
            let mut error = ApplicationError::invalid_argument().with_field_violation(
                "request_id",
                request_id.to_string(),
                "request_id",
            );
            for problem in problems {
                error = error.with_field_violation(
                    problem.location.clone(),
                    problem.message.clone(),
                    problem.rule.clone(),
                );
            }
            error.create()
        }
        DomainError::UnknownSectionKey => {
            ApplicationError::not_found(format!("request {request_id}: unknown section key"))
                .with_resource("section_key")
                .create()
        }
        DomainError::OperationNotFound => {
            ApplicationError::not_found(format!("request {request_id}: operation not found"))
                .with_resource("operation")
                .create()
        }
        DomainError::Defect => {
            CanonicalError::internal(format!("request {request_id}: internal failure")).create()
        }
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
