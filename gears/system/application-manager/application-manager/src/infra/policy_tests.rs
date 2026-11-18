#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use authz_resolver_sdk::PolicyEnforcer;
use toolkit_security::access_scope::{ScopeConstraint, ScopeFilter};
use toolkit_security::pep_properties;
use uuid::Uuid;

use super::*;
use crate::test_support::{FakeAuthZ, Pdp, tenant_ctx};

const PDP_TIMEOUT: Duration = Duration::from_secs(2);

/// ADR 0004: one row per PDP answer, with its expected outcome.
#[tokio::test(start_paused = true)]
async fn each_pdp_answer_maps_to_its_outcome() {
    let cases = [
        (Pdp::Permit, Ok(())),
        (Pdp::Deny, Err(AccessError::NotPermitted)),
        // No advertised properties: the enforcer reports any constraint as
        // `CompileFailed`; the constrained-scope arm is tested below.
        (Pdp::ConstrainedPermit, Err(AccessError::NotPermitted)),
        (Pdp::Fail, Err(AccessError::Unavailable)),
        (Pdp::Hang, Err(AccessError::Unavailable)),
    ];
    for (answer, expected) in cases {
        let pdp = FakeAuthZ::new(answer);
        let access = PolicyAccessControl::new(PolicyEnforcer::new(pdp.clone()), PDP_TIMEOUT);
        let started = tokio::time::Instant::now();
        let got = access
            .authorize(&tenant_ctx(), TenantAction::ReadHistory)
            .await;
        assert_eq!(got, expected, "{answer:?}");
        assert!(
            started.elapsed() <= PDP_TIMEOUT,
            "{answer:?}: bounded by pdp_call_timeout"
        );

        let requests = pdp.requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "{answer:?}");
        let request = &requests[0];
        assert!(
            request.context.tenant_context.is_none(),
            "{answer:?}: no context tenant (ADR 0004)"
        );
        assert!(
            request.resource.properties.is_empty(),
            "{answer:?}: no resource properties"
        );
        assert_eq!(request.resource.resource_type, APPLICATION_RESOURCE_TYPE);
    }
}

#[tokio::test]
async fn sends_the_requested_action() {
    for action in TenantAction::ALL {
        let pdp = FakeAuthZ::new(Pdp::Permit);
        let access = PolicyAccessControl::new(PolicyEnforcer::new(pdp.clone()), PDP_TIMEOUT);
        access.authorize(&tenant_ctx(), action).await.unwrap();
        let sent = pdp.requests.lock().unwrap()[0].action.name.clone();
        assert_eq!(sent, action.as_str());
    }
}

#[tokio::test]
async fn missing_authz_client_is_unavailable() {
    let hub = Arc::new(toolkit::client_hub::ClientHub::new());
    let access = PolicyAccessControl::new(PolicyEnforcer::from_hub(hub), PDP_TIMEOUT);
    assert_eq!(
        access
            .authorize(&tenant_ctx(), TenantAction::ReadApplications)
            .await,
        Err(AccessError::Unavailable)
    );
}

#[test]
fn only_an_unconstrained_scope_permits() {
    let constrained = AccessScope::from_constraints(vec![
        ScopeConstraint::try_new(vec![ScopeFilter::eq(
            pep_properties::OWNER_TENANT_ID,
            Uuid::from_u128(0xB),
        )])
        .unwrap(),
    ]);
    assert!(!constrained.is_unconstrained());
    assert_eq!(
        outcome(Ok(constrained), &tenant_ctx(), TenantAction::ReadHistory),
        Err(AccessError::NotPermitted)
    );
    assert_eq!(
        outcome(
            Ok(AccessScope::allow_all()),
            &tenant_ctx(),
            TenantAction::ReadHistory
        ),
        Ok(())
    );
}
