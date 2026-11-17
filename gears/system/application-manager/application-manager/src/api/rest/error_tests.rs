#![allow(clippy::unwrap_used, clippy::expect_used)]

use application_manager_sdk::APPLICATION_RESOURCE_TYPE;
use toolkit_canonical_errors::Problem;

use super::*;
use crate::domain::actions::CallerAction;

fn problem(err: &DomainError) -> Problem {
    Problem::from(to_canonical(err, Uuid::from_u128(42)))
}

#[test]
fn not_ready_carries_request_id_and_retry_after() {
    let p = problem(&DomainError::NotReady);
    assert_eq!(p.status, Some(503));
    assert!(
        p.problem_type
            .ends_with("cf.core.err.service_unavailable.v1~")
    );
    assert!(
        p.detail.contains(&Uuid::from_u128(42).to_string()),
        "{}",
        p.detail
    );
    assert_eq!(
        p.context["retry_after_seconds"],
        REFUSAL_RETRY_AFTER_SECONDS
    );
}

#[test]
fn not_permitted_names_the_action_and_request_id() {
    let p = problem(&DomainError::NotPermitted {
        action: CallerAction::ReadSchema,
    });
    assert_eq!(p.status, Some(403));
    assert!(
        p.problem_type
            .ends_with("cf.core.err.permission_denied.v1~")
    );
    let reason = p.context["reason"].as_str().unwrap();
    assert!(
        reason.contains(CallerAction::ReadSchema.as_str()),
        "{reason}"
    );
    assert!(
        reason.contains(&Uuid::from_u128(42).to_string()),
        "{reason}"
    );
}

#[test]
fn resource_marker_matches_the_sdk_resource_type() {
    let p = problem(&DomainError::NotPermitted {
        action: CallerAction::Seed,
    });
    assert_eq!(p.context["resource_type"], APPLICATION_RESOURCE_TYPE);
}
