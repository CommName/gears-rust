//! Policy adapter over `PolicyEnforcer` (DESIGN §3.3 "Authorization", ADR 0007).
//!
//! One PDP call per decision, with no context tenant and no tenant-mode override (the
//! platform decides). The application resource has no row scoping, so only an unconstrained
//! permit is a permit: a permit that carries any constraint is `refused` (fail closed), however
//! it came about (`require_constraints = false` lets the compiler pass constraints on a supported
//! property through, and the application resource supports none). The PDP gets the caller's
//! `SecurityContext` unchanged, bearer token included. The adapter owns the PDP deadline: its
//! expiry is `timed_out`, any other failure to answer is `unreachable`, and a denial or an
//! uncompilable decision is `refused`.

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::gts::APPLICATION_RESOURCE_TYPE;
use async_trait::async_trait;
use authz_resolver_sdk::{
    AccessRequest, AuthZResolverApi, EnforcerError, PolicyEnforcer, ResourceType,
};
use toolkit::client_hub::ClientHub;
use toolkit_security::{AccessScope, SecurityContext};

use crate::domain::ports::{PdpUnavailable, PolicyAction, PolicyDecision, PolicyPort};

/// The resource property that carries the application id.
pub const APPLICATION_ID_PROPERTY: &str = "application_id";

static RESOURCE: ResourceType = ResourceType::from_static(APPLICATION_RESOURCE_TYPE, &[]);

/// `PolicyPort` over the platform PDP, bounded by the adapter's own deadline.
pub struct PolicyAdapter {
    enforcer: PolicyEnforcer,
    /// The hub the authz client is resolved from, when there is one (names a missing client in
    /// the failure log).
    hub: Option<Arc<ClientHub>>,
    deadline: Duration,
}

impl PolicyAdapter {
    /// Builds the adapter over `authz`; `deadline` is `pdp_call_timeout`.
    ///
    /// The enforcer's own deadline is set to the maximum so that it never fires first: its
    /// expiry would come back as an `EvaluationFailed`, indistinguishable from any other
    /// failure to answer, while the adapter's own timeout below tells the expiry apart
    /// (`timed_out`).
    #[must_use]
    pub fn new(authz: Arc<dyn AuthZResolverApi>, deadline: Duration) -> Self {
        Self {
            enforcer: PolicyEnforcer::new(authz).with_deadline(Duration::MAX),
            hub: None,
            deadline,
        }
    }

    /// Builds the adapter over the authz client resolved from `hub` at each decision, so a
    /// missing client is an unavailable decision, never an `init` failure. The enforcer's own
    /// deadline is out of the way as in [`Self::new`].
    #[must_use]
    pub fn from_hub(hub: Arc<ClientHub>, deadline: Duration) -> Self {
        Self {
            enforcer: PolicyEnforcer::from_hub(Arc::clone(&hub)).with_deadline(Duration::MAX),
            hub: Some(hub),
            deadline,
        }
    }

    fn client_missing(&self) -> bool {
        self.hub
            .as_ref()
            .is_some_and(|hub| hub.get::<dyn AuthZResolverApi>().is_err())
    }
}

/// Maps the enforcer's answer to a decision. `client_missing` is consulted only for a failed
/// evaluation. A failure logs `pdp_evaluation_failed` with a content-free cause kind.
pub(crate) fn decision_of(
    answer: Result<AccessScope, EnforcerError>,
    client_missing: impl FnOnce() -> bool,
) -> PolicyDecision {
    match answer {
        Ok(scope) if scope.is_unconstrained() => PolicyDecision::Permit,
        // Fail closed: the application resource has no row scoping to apply a constraint to.
        Ok(_) | Err(EnforcerError::Denied { .. }) => PolicyDecision::Refused,
        Err(EnforcerError::CompileFailed(_)) => {
            tracing::warn!(cause = "compile_failed", "pdp_evaluation_failed");
            PolicyDecision::Refused
        }
        Err(EnforcerError::EvaluationFailed(_)) => {
            let cause = if client_missing() {
                "client_missing"
            } else {
                "evaluation_error"
            };
            tracing::warn!(cause, "pdp_evaluation_failed");
            PolicyDecision::Unavailable(PdpUnavailable::Unreachable)
        }
    }
}

#[async_trait]
impl PolicyPort for PolicyAdapter {
    async fn decide(
        &self,
        ctx: &SecurityContext,
        action: PolicyAction,
        application_id: Option<&str>,
    ) -> PolicyDecision {
        let mut request = AccessRequest::new().require_constraints(false);
        if let Some(id) = application_id {
            request = request.resource_property(APPLICATION_ID_PROPERTY, id);
        }
        let answer = tokio::time::timeout(
            self.deadline,
            self.enforcer
                .access_scope_with(ctx, &RESOURCE, action.name(), None, &request),
        )
        .await;
        match answer {
            Err(_elapsed) => PolicyDecision::Unavailable(PdpUnavailable::TimedOut),
            Ok(answer) => decision_of(answer, || self.client_missing()),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "policy_tests.rs"]
mod policy_tests;
