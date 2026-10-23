#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use authz_resolver_sdk::PolicyEnforcer;
use toolkit_security::access_scope::{ScopeConstraint, ScopeFilter};
use toolkit_security::pep_properties;
use uuid::Uuid;

use super::*;
use crate::domain::actions::Action;
use crate::test_support::{FakeAuthZ, Pdp, ctx};

const PDP_TIMEOUT: Duration = Duration::from_secs(2);

/// ADR 0005: one row per PDP answer, with its expected outcome.
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
        let got = access.authorize(&ctx(), Action::ReadSchema, None).await;
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
            "{answer:?}: no context tenant (ADR 0005)"
        );
        assert_eq!(request.action.name, Action::ReadSchema.as_str());
        assert_eq!(request.resource.resource_type, APPLICATION_RESOURCE_TYPE);
    }
}

#[tokio::test]
async fn sends_the_requested_action() {
    for action in Action::ALL {
        let pdp = FakeAuthZ::new(Pdp::Permit);
        let access = PolicyAccessControl::new(PolicyEnforcer::new(pdp.clone()), PDP_TIMEOUT);
        access.authorize(&ctx(), action, None).await.unwrap();
        let sent = pdp.requests.lock().unwrap()[0].action.name.clone();
        assert_eq!(sent, action.as_str());
    }
}

#[tokio::test]
async fn missing_authz_client_is_unavailable() {
    let hub = Arc::new(toolkit::client_hub::ClientHub::new());
    let access = PolicyAccessControl::new(PolicyEnforcer::from_hub(hub), PDP_TIMEOUT);
    assert_eq!(
        access.authorize(&ctx(), Action::ReadSchema, None).await,
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
        outcome(Ok(constrained), &ctx(), Action::ReadSchema),
        Err(AccessError::NotPermitted)
    );
    assert_eq!(
        outcome(Ok(AccessScope::allow_all()), &ctx(), Action::ReadSchema),
        Ok(())
    );
}

#[tokio::test]
async fn sends_the_application_id_as_a_resource_property_only_when_given() {
    let pdp = FakeAuthZ::new(Pdp::Permit);
    let access = PolicyAccessControl::new(PolicyEnforcer::new(pdp.clone()), PDP_TIMEOUT);
    access
        .authorize(&ctx(), Action::Seed, Some("acme.app"))
        .await
        .unwrap();
    access
        .authorize(&ctx(), Action::ReadSchema, None)
        .await
        .unwrap();
    let requests = pdp.requests.lock().unwrap();
    assert_eq!(
        requests[0].resource.properties.get("application_id"),
        Some(&serde_json::json!("acme.app"))
    );
    assert!(
        !requests[1]
            .resource
            .properties
            .contains_key("application_id"),
        "a schema read sends no application id"
    );
}

#[tokio::test]
async fn an_application_id_does_not_change_the_answer_mapping() {
    for (answer, expected) in [
        (Pdp::Permit, Ok(())),
        (Pdp::Deny, Err(AccessError::NotPermitted)),
        (Pdp::ConstrainedPermit, Err(AccessError::NotPermitted)),
        (Pdp::Fail, Err(AccessError::Unavailable)),
    ] {
        let pdp = FakeAuthZ::new(answer);
        let access = PolicyAccessControl::new(PolicyEnforcer::new(pdp), PDP_TIMEOUT);
        assert_eq!(
            access
                .authorize(&ctx(), Action::Uninstall, Some("acme"))
                .await,
            expected,
            "{answer:?}"
        );
    }
}
