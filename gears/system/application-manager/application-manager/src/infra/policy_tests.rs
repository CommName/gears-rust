//! Unit tests for the policy adapter (DESIGN §3.2 "Ports and Composition" `PolicyPort` row,
//! §3.3 "Authorization", §4.5 "Timeouts outside the plugin calls", §4.8 "Unit" row: "the
//! decision request with no context tenant, no tenant-mode override, and `require_constraints
//! = false`; a constraint-free permit compiling to the unscoped scope, and any constraint
//! failing to compile", ADR 0007).
//!
//! Planned API in `crate::infra::policy`:
//!
//! ```text
//! /// `PolicyPort` over `PolicyEnforcer`: resource type `APPLICATION_RESOURCE_TYPE` with NO
//! /// supported constraint property, `AccessRequest` with `require_constraints(false)`, no
//! /// context tenant, no tenant mode; the caller's SecurityContext is passed through
//! /// unchanged, bearer token included (F-008: it was rebuilt from a `CallerIdentity` and so
//! /// had no token). The ADAPTER owns the PDP
//! /// deadline: its own `tokio::time::timeout(deadline, ..)` around the enforcer call (the
//! /// enforcer's own bound is set out of the way), so the expiry is told apart from every
//! /// other failure.
//! pub struct PolicyAdapter;
//! impl PolicyAdapter {
//!     pub fn new(authz: Arc<dyn AuthZResolverApi>, deadline: Duration) -> Self;
//!     pub fn from_hub(hub: Arc<ClientHub>, deadline: Duration) -> Self; // client resolved per decision
//! }
//! impl PolicyPort for PolicyAdapter {
//!     // Ok(scope)                  -> Permit   (an unconstrained permit compiles to allow-all)
//!     // Denied, CompileFailed      -> Refused  (a permit with any constraint cannot compile)
//!     // deadline expired          -> Unavailable(PdpUnavailable::TimedOut)
//!     // EvaluationFailed (the PDP call failed, or no authz client)
//!                                 -> Unavailable(PdpUnavailable::Unreachable)
//!     async fn decide(&self, ctx: &SecurityContext, action, application_id: Option<&str>)
//!         -> PolicyDecision;
//! }
//! pub const APPLICATION_ID_PROPERTY: &str = "application_id";
//! // PolicyDecision gains the reason: Unavailable(PdpUnavailable) (domain::ports, F-010).
//! ```
//!
//! Decisions (DESIGN is silent): the resource id of the evaluation request is never set (the
//! application id is a resource PROPERTY, §3.3); no capability is advertised; one call to the
//! PDP per decision, no retry (ADR 0001 style: the gear never retries a dependency).
#![allow(clippy::unchecked_time_subtraction)]

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::gts::{
    ACTION_LIST, ACTION_READ_SCHEMA, ACTION_SEED, ACTION_UNINSTALL, APPLICATION_RESOURCE_TYPE,
};
use authz_resolver_sdk::AuthZResolverApi;
use serde_json::json;
use tokio::time::Instant;
use toolkit::client_hub::ClientHub;
use toolkit_security::{AccessScope, SecurityContext};
use uuid::Uuid;

use crate::domain::ports::{PdpUnavailable, PolicyAction, PolicyDecision, PolicyPort};
use crate::infra::policy::{APPLICATION_ID_PROPERTY, PolicyAdapter, decision_of};
use crate::test_support::{
    AuthzScript, FakeAuthz, bounded, capture_logs, constraint_on, empty_constraint, eq_constraint,
    has_field,
};
use authz_resolver_sdk::models::{EvaluationResponse, EvaluationResponseContext};

const DEADLINE: Duration = Duration::from_secs(2);

const BEARER: &str = "caller-bearer-token";

fn caller() -> SecurityContext {
    SecurityContext::builder()
        .subject_id(Uuid::from_u128(0x51))
        .subject_type("service")
        .subject_tenant_id(Uuid::from_u128(0x7E))
        .token_scopes(vec!["scope-a".to_owned(), "scope-b".to_owned()])
        .bearer_token(BEARER.to_owned())
        .build()
        .expect("a security context")
}

fn adapter(script: AuthzScript) -> (PolicyAdapter, Arc<FakeAuthz>) {
    let authz = Arc::new(FakeAuthz::new(script));
    let adapter = PolicyAdapter::new(Arc::clone(&authz) as Arc<dyn AuthZResolverApi>, DEADLINE);
    (adapter, authz)
}

#[tokio::test]
async fn an_unconstrained_permit_is_a_permit() {
    let (adapter, authz) = adapter(AuthzScript::Permit(vec![]));

    let decision = adapter
        .decide(&caller(), PolicyAction::Seed, Some("app"))
        .await;

    assert_eq!(decision, PolicyDecision::Permit);
    assert_eq!(authz.requests().len(), 1, "one PDP call per decision");
}

#[tokio::test]
async fn a_denial_is_refused() {
    let (adapter, _) = adapter(AuthzScript::Deny);

    assert_eq!(
        adapter
            .decide(&caller(), PolicyAction::Uninstall, Some("app"))
            .await,
        PolicyDecision::Refused
    );
}

#[tokio::test]
async fn an_evaluation_failure_is_unavailable_and_is_not_retried() {
    let (adapter, authz) = adapter(AuthzScript::Fail);

    let decision = adapter.decide(&caller(), PolicyAction::Seed, None).await;

    assert_eq!(
        decision,
        PolicyDecision::Unavailable(PdpUnavailable::Unreachable)
    );
    assert_eq!(authz.requests().len(), 1, "no retry");
}

// A transport-style failure (the service is unavailable) is a failure to answer, not a
// timeout: only the adapter's own deadline is `timed_out`.
#[tokio::test]
async fn a_transport_style_failure_is_unreachable_and_not_timed_out() {
    let (adapter, authz) = adapter(AuthzScript::Unavailable);

    let decision = adapter.decide(&caller(), PolicyAction::Seed, None).await;

    assert_eq!(
        decision,
        PolicyDecision::Unavailable(PdpUnavailable::Unreachable)
    );
    assert_eq!(authz.requests().len(), 1, "no retry");
}

// DESIGN §3.3: a permit that carries any constraint cannot compile (the application resource
// declares no supported property) and fails closed as not permitted.
#[tokio::test]
async fn a_permit_with_a_tenant_constraint_is_refused() {
    let tenant = Uuid::from_u128(0x7E);
    let (adapter, _) = adapter(AuthzScript::Permit(vec![constraint_on(
        "owner_tenant_id",
        tenant,
    )]));

    for action in [
        PolicyAction::Seed,
        PolicyAction::Uninstall,
        PolicyAction::List,
        PolicyAction::ReadSchema,
    ] {
        assert_eq!(
            adapter.decide(&caller(), action, None).await,
            PolicyDecision::Refused,
            "{action:?}"
        );
    }
}

#[tokio::test]
async fn a_permit_with_any_other_constraint_is_refused() {
    for property in ["application_id", "id", "anything_else"] {
        let (adapter, _) = adapter(AuthzScript::Permit(vec![constraint_on(
            property,
            Uuid::from_u128(1),
        )]));

        assert_eq!(
            adapter
                .decide(&caller(), PolicyAction::Seed, Some("app"))
                .await,
            PolicyDecision::Refused,
            "a constraint on {property}"
        );
    }
}

#[tokio::test]
async fn a_permit_with_several_constraints_is_refused() {
    let (adapter, _) = adapter(AuthzScript::Permit(vec![
        constraint_on("owner_tenant_id", Uuid::from_u128(1)),
        constraint_on("owner_tenant_id", Uuid::from_u128(2)),
    ]));

    assert_eq!(
        adapter.decide(&caller(), PolicyAction::Seed, None).await,
        PolicyDecision::Refused
    );
}

// DESIGN §3.3, §4.5: the adapter bounds the decision with `pdp_call_timeout` (its own deadline)
// and reports the expiry as `timed_out`; the bound has no margin.
#[tokio::test(start_paused = true)]
async fn a_pdp_that_never_answers_is_timed_out_after_exactly_the_deadline() {
    let (adapter, authz) = adapter(AuthzScript::Hang);
    let started = Instant::now();

    let decision = bounded(adapter.decide(&caller(), PolicyAction::Seed, None)).await;

    assert_eq!(
        decision,
        PolicyDecision::Unavailable(PdpUnavailable::TimedOut)
    );
    assert_eq!(
        started.elapsed(),
        DEADLINE,
        "one deadline, no more, no less"
    );
    assert_eq!(authz.requests().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn an_answer_just_inside_the_deadline_is_used_and_one_just_past_it_is_not() {
    let (inside, _) = adapter(AuthzScript::PermitAfter(
        DEADLINE - Duration::from_millis(1),
    ));
    let (past, _) = adapter(AuthzScript::PermitAfter(
        DEADLINE + Duration::from_millis(1),
    ));

    assert_eq!(
        inside.decide(&caller(), PolicyAction::Seed, None).await,
        PolicyDecision::Permit
    );
    assert_eq!(
        bounded(past.decide(&caller(), PolicyAction::Seed, None)).await,
        PolicyDecision::Unavailable(PdpUnavailable::TimedOut)
    );
}

// ADR 0007, §3.3: no context tenant, no tenant-mode override, constraints not required, for
// every action (the key list and both schema reads included).
#[tokio::test]
async fn every_request_names_no_context_tenant_and_does_not_require_constraints() {
    let (adapter, authz) = adapter(AuthzScript::Permit(vec![]));
    let actions = [
        (PolicyAction::Seed, ACTION_SEED),
        (PolicyAction::Uninstall, ACTION_UNINSTALL),
        (PolicyAction::List, ACTION_LIST),
        (PolicyAction::ReadSchema, ACTION_READ_SCHEMA),
    ];

    for (action, _) in actions {
        adapter.decide(&caller(), action, None).await;
    }

    let requests = authz.requests();
    assert_eq!(requests.len(), actions.len());
    for (request, (_, name)) in requests.iter().zip(actions) {
        assert!(
            request.context.tenant_context.is_none(),
            "{name}: no context tenant and no tenant mode: {:?}",
            request.context.tenant_context
        );
        assert!(!request.context.require_constraints, "{name}");
        assert_eq!(request.action.name, name);
        assert_eq!(request.resource.resource_type, APPLICATION_RESOURCE_TYPE);
        assert_eq!(
            request.resource.id, None,
            "{name}: the id is a property, not the resource id"
        );
        assert!(
            request.context.supported_properties.is_empty(),
            "{name}: the application resource declares no supported constraint property"
        );
        assert!(request.context.capabilities.is_empty(), "{name}");
    }
}

#[tokio::test]
async fn seed_and_uninstall_send_the_application_id_as_a_resource_property() {
    let (adapter, authz) = adapter(AuthzScript::Permit(vec![]));

    adapter
        .decide(&caller(), PolicyAction::Seed, Some("app-1"))
        .await;
    adapter
        .decide(&caller(), PolicyAction::Uninstall, Some("app-2"))
        .await;

    let requests = authz.requests();
    assert_eq!(APPLICATION_ID_PROPERTY, "application_id");
    assert_eq!(
        requests[0].resource.properties.get("application_id"),
        Some(&json!("app-1"))
    );
    assert_eq!(requests[0].resource.properties.len(), 1);
    assert_eq!(
        requests[1].resource.properties.get("application_id"),
        Some(&json!("app-2"))
    );
}

#[tokio::test]
async fn the_application_id_is_sent_exactly_as_given() {
    let (adapter, authz) = adapter(AuthzScript::Permit(vec![]));
    let ids = ["App", " app ", "caf\u{e9}", "a/b", ".."];

    for id in ids {
        adapter
            .decide(&caller(), PolicyAction::Uninstall, Some(id))
            .await;
    }

    for (request, id) in authz.requests().iter().zip(ids) {
        assert_eq!(
            request.resource.properties.get("application_id"),
            Some(&json!(id)),
            "no trimming, folding or normalization"
        );
    }
}

#[tokio::test]
async fn without_an_application_id_no_property_is_sent() {
    let (adapter, authz) = adapter(AuthzScript::Permit(vec![]));

    adapter.decide(&caller(), PolicyAction::Seed, None).await;
    adapter.decide(&caller(), PolicyAction::List, None).await;

    for request in authz.requests() {
        assert!(
            request.resource.properties.is_empty(),
            "{:?}",
            request.resource.properties
        );
    }
}

// F-008 (decision): the PDP gets the caller's context unchanged, bearer token included, as in
// the other gears. This pin said the opposite (`bearer_token.is_none()`, "a CallerIdentity has
// no token to forward") while the port took a `CallerIdentity`.
#[tokio::test]
async fn the_request_carries_the_caller_identity_and_the_callers_bearer_token() {
    let (adapter, authz) = adapter(AuthzScript::Permit(vec![]));
    let caller = caller();

    adapter
        .decide(&caller, PolicyAction::Seed, Some("app"))
        .await;

    let request = &authz.requests()[0];
    assert_eq!(request.subject.id, caller.subject_id());
    assert_eq!(request.subject.subject_type.as_deref(), Some("service"));
    assert_eq!(request.context.token_scopes, caller.token_scopes());
    assert_eq!(
        request
            .context
            .bearer_token
            .as_ref()
            .map(secrecy::ExposeSecret::expose_secret),
        Some(BEARER),
        "the caller's token reaches the PDP"
    );
    // The subject's OWN tenant travels as a subject property (the enforcer's contract);
    // it is not a context tenant.
    assert_eq!(
        request.subject.properties.get("tenant_id"),
        Some(&json!(caller.subject_tenant_id().to_string()))
    );
    assert!(request.context.tenant_context.is_none());
}

#[tokio::test]
async fn from_hub_resolves_the_authz_client_at_decision_time() {
    let hub = Arc::new(ClientHub::new());
    let adapter = PolicyAdapter::from_hub(Arc::clone(&hub), DEADLINE);

    // No client registered yet: the PDP cannot answer, and the adapter was built fine.
    assert_eq!(
        adapter.decide(&caller(), PolicyAction::Seed, None).await,
        PolicyDecision::Unavailable(PdpUnavailable::Unreachable)
    );

    let authz = Arc::new(FakeAuthz::new(AuthzScript::Permit(vec![])));
    hub.register::<dyn AuthZResolverApi>(Arc::clone(&authz) as Arc<dyn AuthZResolverApi>);
    assert_eq!(
        adapter
            .decide(&caller(), PolicyAction::Seed, Some("app"))
            .await,
        PolicyDecision::Permit
    );
    assert_eq!(authz.requests().len(), 1);
}

// The adapter built over the hub has the same deadline: a hung PDP behind the hub is
// `timed_out` after exactly `deadline`.
#[tokio::test(start_paused = true)]
async fn from_hub_bounds_a_hung_pdp_with_the_given_deadline() {
    let hub = Arc::new(ClientHub::new());
    let authz = Arc::new(FakeAuthz::new(AuthzScript::Hang));
    hub.register::<dyn AuthZResolverApi>(Arc::clone(&authz) as Arc<dyn AuthZResolverApi>);
    let adapter = PolicyAdapter::from_hub(hub, DEADLINE);
    let started = Instant::now();

    let decision = bounded(adapter.decide(&caller(), PolicyAction::Seed, None)).await;

    assert_eq!(
        decision,
        PolicyDecision::Unavailable(PdpUnavailable::TimedOut)
    );
    assert_eq!(started.elapsed(), DEADLINE, "one deadline, no margin");
    assert_eq!(authz.requests().len(), 1);
}

fn response(
    decision: bool,
    constraints: Vec<authz_resolver_sdk::constraints::Constraint>,
) -> AuthzScript {
    AuthzScript::Raw(EvaluationResponse {
        decision,
        context: EvaluationResponseContext {
            constraints,
            ..Default::default()
        },
    })
}

// Fail-closed edges (DESIGN §3.3): a permit may be unconstrained only; any constraint in the
// answer, however degenerate, is not permitted, and a deny stays a deny whatever it carries.
#[tokio::test]
async fn a_permit_with_a_degenerate_or_equality_constraint_is_refused() {
    let tenant = Uuid::from_u128(0x7E);
    let cases = [
        (
            "empty predicate list",
            response(true, vec![empty_constraint()]),
        ),
        (
            "equality on the tenant",
            response(
                true,
                vec![eq_constraint("owner_tenant_id", &tenant.to_string())],
            ),
        ),
        (
            "equality on the application id",
            response(true, vec![eq_constraint("application_id", "app")]),
        ),
        (
            "mixed: an empty constraint and a tenant predicate",
            response(
                true,
                vec![empty_constraint(), constraint_on("owner_tenant_id", tenant)],
            ),
        ),
        (
            "mixed: an equality and an empty constraint",
            response(
                true,
                vec![eq_constraint("application_id", "app"), empty_constraint()],
            ),
        ),
    ];
    for (name, script) in cases {
        let (adapter, authz) = adapter(script);

        let decision = adapter
            .decide(&caller(), PolicyAction::Seed, Some("app"))
            .await;

        assert_eq!(decision, PolicyDecision::Refused, "{name}");
        assert_eq!(authz.requests().len(), 1, "{name}: one PDP call");
    }
}

#[tokio::test]
async fn a_deny_that_carries_constraints_is_refused() {
    for constraints in [
        vec![],
        vec![empty_constraint()],
        vec![constraint_on("owner_tenant_id", Uuid::from_u128(1))],
    ] {
        let (adapter, _) = adapter(response(false, constraints));

        assert_eq!(
            adapter
                .decide(&caller(), PolicyAction::Uninstall, None)
                .await,
            PolicyDecision::Refused
        );
    }
}

// F-005: only an unconstrained scope is a permit. A constrained scope (what a supported
// property would compile to; the application resource declares none, so the adapter cannot get
// one from the platform compiler today) is refused, not permitted by accident.
#[test]
fn a_constrained_scope_is_refused_and_an_unconstrained_one_is_permitted() {
    assert_eq!(
        decision_of(Ok(AccessScope::allow_all()), || false),
        PolicyDecision::Permit
    );
    assert_eq!(
        decision_of(Ok(AccessScope::for_tenant(Uuid::from_u128(7))), || false),
        PolicyDecision::Refused
    );
    assert_eq!(
        decision_of(Ok(AccessScope::for_resource(Uuid::from_u128(7))), || false),
        PolicyDecision::Refused
    );
}

// F-009: the failure to answer is logged with a content-free cause kind.
#[tokio::test]
async fn an_evaluation_failure_logs_pdp_evaluation_failed_with_its_cause() {
    let (logs, _guard) = capture_logs();
    let (failing, _) = adapter(AuthzScript::Fail);
    failing
        .decide(&caller(), PolicyAction::Seed, Some("SENTINEL-APP-ID"))
        .await;
    let hub = Arc::new(ClientHub::new());
    let hubbed = PolicyAdapter::from_hub(hub, DEADLINE);
    hubbed.decide(&caller(), PolicyAction::Seed, None).await;
    let (denying, _) = adapter(AuthzScript::Permit(vec![constraint_on(
        "owner_tenant_id",
        Uuid::from_u128(1),
    )]));
    denying.decide(&caller(), PolicyAction::Seed, None).await;
    let (plain_deny, _) = adapter(AuthzScript::Deny);
    plain_deny.decide(&caller(), PolicyAction::Seed, None).await;

    logs.assert_live();
    let lines = logs.lines_with("pdp_evaluation_failed");
    assert_eq!(lines.len(), 3, "a denial is not a failure: {lines:?}");
    for (line, cause) in lines
        .iter()
        .zip(["evaluation_error", "client_missing", "compile_failed"])
    {
        assert!(line.contains("WARN"), "{line}");
        assert!(has_field(line, "cause", cause), "{cause}: {line}");
        assert!(!line.contains("SENTINEL"), "no request content: {line}");
    }
}

// F-013: the enforcer's own deadline is out of the way (`Duration::MAX`), so only the
// adapter's deadline bounds a decision: a PDP slower than the enforcer's default (5s) still
// answers inside a longer `pdp_call_timeout`, from either constructor.
#[tokio::test(start_paused = true)]
async fn the_enforcers_own_deadline_never_cuts_a_decision_short() {
    let long = Duration::from_hours(1);
    let authz = Arc::new(FakeAuthz::new(AuthzScript::PermitAfter(
        Duration::from_mins(30),
    )));
    let direct = PolicyAdapter::new(Arc::clone(&authz) as Arc<dyn AuthZResolverApi>, long);
    let hub = Arc::new(ClientHub::new());
    hub.register::<dyn AuthZResolverApi>(Arc::clone(&authz) as Arc<dyn AuthZResolverApi>);
    let hubbed = PolicyAdapter::from_hub(hub, long);

    assert_eq!(
        direct.decide(&caller(), PolicyAction::Seed, None).await,
        PolicyDecision::Permit
    );
    assert_eq!(
        hubbed.decide(&caller(), PolicyAction::Seed, None).await,
        PolicyDecision::Permit
    );
}
