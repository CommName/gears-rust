//! `PolicyEnforcer` adapter for [`AccessControl`].
//!
//! Every gear-level action is a platform-level permission on
//! `APPLICATION_RESOURCE_TYPE`, decided with no context tenant and no
//! constraints (ADR 0005).

use std::time::Duration;

use application_manager_sdk::APPLICATION_RESOURCE_TYPE;
use async_trait::async_trait;
use authz_resolver_sdk::{AccessRequest, EnforcerError, PolicyEnforcer, ResourceType};
use toolkit_security::{AccessScope, SecurityContext};

use crate::domain::actions::Action;
use crate::domain::ports::{AccessControl, AccessError};

/// The application resource advertises no properties: the gear filters no
/// rows, so any constraint in a permit is refused.
const APPLICATION_RESOURCE: ResourceType =
    ResourceType::from_static(APPLICATION_RESOURCE_TYPE, &[]);

/// [`AccessControl`] backed by the platform `PolicyEnforcer`.
#[derive(Debug)]
pub struct PolicyAccessControl {
    enforcer: PolicyEnforcer,
}

impl PolicyAccessControl {
    /// An adapter over `enforcer` whose PDP calls end at `pdp_call_timeout`.
    ///
    /// The gear passes [`PolicyEnforcer::from_hub`], so the authz client is
    /// resolved per call; a missing client is an evaluation failure.
    #[must_use]
    pub fn new(enforcer: PolicyEnforcer, pdp_call_timeout: Duration) -> Self {
        Self {
            enforcer: enforcer.with_deadline(pdp_call_timeout),
        }
    }
}

#[async_trait]
impl AccessControl for PolicyAccessControl {
    async fn authorize(&self, ctx: &SecurityContext, action: Action) -> Result<(), AccessError> {
        let request = AccessRequest::new().require_constraints(false);
        let answer = self
            .enforcer
            .access_scope_with(ctx, &APPLICATION_RESOURCE, action.as_str(), None, &request)
            .await;
        outcome(answer, ctx, action)
    }
}

/// Map an enforcer answer to the access outcome. Only an unconstrained
/// permit permits; the PDP internals are logged, never returned.
fn outcome(
    answer: Result<AccessScope, EnforcerError>,
    ctx: &SecurityContext,
    action: Action,
) -> Result<(), AccessError> {
    let (refusal, cause) = match answer {
        Ok(scope) if scope.is_unconstrained() => return Ok(()),
        // Fail-closed guard: with no advertised properties the enforcer
        // reports any PDP constraint as `CompileFailed`, so this arm stays
        // unreachable unless the resource gains properties.
        Ok(_) => (
            AccessError::NotPermitted,
            "permit with constraints".to_owned(),
        ),
        Err(EnforcerError::Denied { .. }) => (AccessError::NotPermitted, "denied".to_owned()),
        Err(EnforcerError::CompileFailed(e)) => (AccessError::NotPermitted, e.to_string()),
        Err(EnforcerError::EvaluationFailed(e)) => (AccessError::Unavailable, e.to_string()),
    };
    log_refusal(refusal, ctx, action, &cause);
    Err(refusal)
}

fn log_refusal(refusal: AccessError, ctx: &SecurityContext, action: Action, cause: &str) {
    let subject_id = ctx.subject_id();
    let action = action.as_str();
    match refusal {
        AccessError::NotPermitted => {
            tracing::info!(action, %subject_id, cause, "application-manager: action not permitted");
        }
        AccessError::Unavailable => {
            tracing::warn!(action, %subject_id, cause, "application-manager: access control cannot answer");
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "policy_tests.rs"]
mod tests;
