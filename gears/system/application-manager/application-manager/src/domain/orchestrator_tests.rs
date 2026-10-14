//! Unit tests for the request orchestrator's pre-dispatch gate (DESIGN §3.2 "Request
//! Orchestrator" steps 1-5 and its precedence table, §3.6 "Pre-Dispatch Gate" paths 1-9,
//! §3.3 "Authorization", §4.5, §4.6 logs, §4.8 "Unit" and "Clock" rows, ADR 0001 no retry,
//! ADR 0007 no tenant). Dispatch, the registry write and the final-result hand-off (steps
//! 6-8), the detached request task and the supervisor are P7; the not-ready and request-task
//! cap gate is P8 (REST).
//!
//! Planned API in `crate::domain::orchestrator`:
//!
//! ```text
//! pub struct OrchestratorSettings { max_envelope_text_length: usize,
//!                                   registry_operation_timeout: Duration, step_timeout: Duration }
//! impl OrchestratorSettings { pub fn from_config(&ApplicationManagerConfig) -> Self }
//!                                  // step_timeout = plugin_call_timeout
//! pub enum RequestInput { Seed { body: Vec<u8> }, Uninstall { application_id: String } }
//! pub struct Proceed { envelope: ApplicationEnvelope, manifest: Option<Value>,
//!                      schemas: Vec<SectionSchema> }
//! pub enum GateOutcome { Decided(OutcomeReport), Proceed(Proceed) }
//! pub struct GateResult { request_id: Uuid, caller: CallerIdentity,
//!                         audit_id: Option<String>, outcome: GateOutcome }
//! pub struct Orchestrator;
//! impl Orchestrator {
//!     pub fn new(policy: Arc<dyn PolicyPort>, registry: Arc<dyn RegistryStore>,
//!                startup: Arc<StartupResultHolder>, clock: Arc<dyn Clock>,
//!                ids: Arc<dyn RequestIdSource>, settings: OrchestratorSettings) -> Self;
//!     /// Steps 1-5. Never fails: every outcome is a decided category or a proceed. Nothing is
//!     /// dispatched, written or handed off as a final result.
//!     pub async fn pre_dispatch(&self, ctx: &SecurityContext, input: RequestInput) -> GateResult;
//!     pub fn abandoned_validations(&self) -> u64;           // see orchestrator_validation_tests
//! }
//! pub fn join_problems(limit: Vec<Problem>, validation: Vec<Problem>) -> Vec<Problem>;
//! ```
//!
//! The decided report is the DESIGN §3.1 outcome report: `category`, `application`, `partial`
//! false, no `sections`, `problems`, `unavailable`, `registry_effect` `unchanged`, `audit_id`
//! (only when the request hand-off was accepted).
//!
//! Flow pinned here (each call recorded in one shared ordered event log):
//! 1. receive: request id from the `RequestIdSource`, receive time from the `Clock` (RFC 3339),
//!    one parse of the body, envelope limit problems recorded;
//! 2. `PolicyPort::decide`, whose deadline belongs to the policy adapter (`pdp_call_timeout`):
//!    the gate adds no second PDP bound. A port answer `Unavailable(reason)` is the
//!    `access_control` item with that reason (`unreachable`, or `timed_out` for the adapter's
//!    own deadline); a hung PDP is exercised through the real `PolicyAdapter`;
//! 3. only after a permit: `RegistryStore::get` bounded by `registry_operation_timeout`
//!    (`registry_storage` with `RegistryError::reason()`); uninstall of an absent id is not
//!    found; a never-storable uninstall id is not found with NO read;
//! 4. one request hand-off through the audit client (always attempted once, even when a
//!    category is already decided; `decided_category` carries it);
//! 5. seed only, undecided and accepted: live schema fetch, then validation.
//!
//! DESIGN statements these tests rely on that §3.2 and §3.3 must carry: the not-ready branch
//! below is defensive (the REST not-ready gate answers first); a constrained permit still
//! attempts the request hand-off (§3.6 path 4); the schema fetch also runs for a body that did
//! not parse (rank 4 precedes rank 5).
#![allow(
    clippy::duration_suboptimal_units,
    clippy::non_ascii_literal,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::unnecessary_wraps
)]

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use application_manager_sdk::models::{
    ApplicationRef, Dependency, HandoffOperation, OutcomeCategory, OutcomeReport, Problem,
    RegistryEffect, UnavailableEntry, UnavailableReason,
};
use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginError, CallerIdentity, RequestHandoffContent,
    SectionPluginError,
};
use aws_lc_rs::digest::{SHA256, digest};
use secrecy::ExposeSecret;
use serde_json::{Value, json};
use tokio::time::Instant;
use uuid::Uuid;

use crate::config::ApplicationManagerConfig;
use crate::domain::limits::REGISTRY_TEXT_MAX_BYTES;
use crate::domain::orchestrator::{GateOutcome, OrchestratorSettings, Proceed};
use crate::domain::ports::{PdpUnavailable, PolicyAction, RegistryError};
use crate::domain::registry::{LastResult, RegistryEntry};
use crate::domain::schema_validator::SectionSchema;
use crate::infra::policy::PolicyAdapter;
use crate::test_support::{
    AuditCall, AuthzScript, CallBehavior, FakeAuditPlugin, FakeAuthz, Gate, GateBuilder,
    MAX_ENVELOPE_LEN, PDP_TIMEOUT, PLUGIN_TIMEOUT, PolicyScript, RECEIVED_AT, REGISTRY_TIMEOUT,
    RegistryCall, RegistryOp, SectionCall, bounded, capture_logs, capture_logs_containing,
    constraint_on, decided, field_value, has_field, policy_adapter, security_context, seed_body,
};

// ---- helpers ---------------------------------------------------------------------------------

fn files_gate() -> GateBuilder {
    GateBuilder::new().section("files", json!({"type": "object"}))
}

fn valid_body() -> Vec<u8> {
    seed_body("app-1", "1.0", &json!({"files": {}}))
}

fn entry(id: &str, version: &str) -> RegistryEntry {
    RegistryEntry::new(
        id,
        version,
        LastResult::Succeeded,
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        "audit-old",
    )
    .expect("a storable entry")
}

fn app(id: Option<&str>, version: Option<&str>) -> Option<ApplicationRef> {
    Some(ApplicationRef {
        id: id.map(str::to_owned),
        version: version.map(str::to_owned),
    })
}

fn item(dependency: Dependency, key: Option<&str>, reason: UnavailableReason) -> UnavailableEntry {
    UnavailableEntry {
        dependency,
        key: key.map(str::to_owned),
        reason,
    }
}

fn access(reason: UnavailableReason) -> UnavailableEntry {
    item(Dependency::AccessControl, None, reason)
}

fn storage(reason: UnavailableReason) -> UnavailableEntry {
    item(Dependency::RegistryStorage, None, reason)
}

fn audit_item(reason: UnavailableReason) -> UnavailableEntry {
    item(Dependency::AuditPlugin, None, reason)
}

fn schema_item(key: &str, reason: UnavailableReason) -> UnavailableEntry {
    item(Dependency::SectionSchema, Some(key), reason)
}

// The report of a request that ended before dispatch: nothing took part, nothing changed.
fn report(
    category: OutcomeCategory,
    application: Option<ApplicationRef>,
    problems: Vec<Problem>,
    unavailable: Vec<UnavailableEntry>,
    audit_id: Option<&str>,
) -> OutcomeReport {
    OutcomeReport {
        category,
        application,
        partial: false,
        sections: Vec::new(),
        problems,
        unavailable,
        registry_effect: RegistryEffect::Unchanged,
        audit_id: audit_id.map(str::to_owned),
    }
}

fn handoff(gate: &Gate) -> RequestHandoffContent {
    let calls = gate.audit.calls();
    assert_eq!(calls.len(), 1, "exactly one request hand-off: {calls:?}");
    calls[0].request.clone().expect("a request hand-off")
}

fn sha256_text(text: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::from("sha256:");
    for byte in digest(&SHA256, text.as_bytes()).as_ref() {
        write!(out, "{byte:02x}").expect("writing to a string");
    }
    out
}

fn reg(op: RegistryOp, id: Option<&str>) -> RegistryCall {
    RegistryCall {
        op,
        id: id.map(str::to_owned),
    }
}

fn never_storable_ids() -> Vec<String> {
    vec![
        String::new(),
        "a\nb".to_owned(),
        "a\0b".to_owned(),
        "\u{7f}".to_owned(),
        "tab\there".to_owned(),
        "\u{9f}".to_owned(),
        "x".repeat(REGISTRY_TEXT_MAX_BYTES + 1),
    ]
}

fn internal_schema_error() -> CallBehavior<SectionPluginError> {
    CallBehavior::err(SectionPluginError::internal("boom"))
}

// ---- the happy paths -----------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_valid_seed_proceeds_with_the_envelope_the_manifest_and_the_live_schemas() {
    let gate = files_gate().build();

    let result = gate.seed(valid_body()).await;

    assert_eq!(result.audit_id.as_deref(), Some("audit-1"));
    assert_eq!(
        result.outcome,
        GateOutcome::Proceed(Proceed {
            envelope: ApplicationEnvelope {
                id: "app-1".to_owned(),
                version: "1.0".to_owned(),
            },
            manifest: Some(json!({
                "application": {"id": "app-1", "version": "1.0"},
                "sections": {"files": {}},
            })),
            schemas: vec![SectionSchema {
                key: "files".to_owned(),
                schema: json!({"type": "object"}),
            }],
        })
    );
}

// DESIGN §3.2 steps 2-5 in order: the permission first, the registry read after a permit, the
// hand-off, then the schema fetch; no dispatch and no write.
#[tokio::test(start_paused = true)]
async fn a_valid_seed_runs_the_steps_in_order_and_dispatches_nothing() {
    let gate = files_gate()
        .section("db", json!({"type": "object"}))
        .build();

    gate.seed(seed_body("app-1", "1.0", &json!({"files": {}, "db": {}})))
        .await;

    let events = gate.events.events();
    assert_eq!(
        &events[..3],
        ["pdp:seed", "registry:get:app-1", "audit:request_handoff"]
    );
    let mut schema_events: Vec<&str> = events[3..].iter().map(String::as_str).collect();
    schema_events.sort_unstable();
    assert_eq!(
        schema_events,
        ["section:db:schema", "section:files:schema"],
        "after the hand-off only the live schema calls, nothing is applied"
    );
}

#[tokio::test(start_paused = true)]
async fn a_valid_uninstall_proceeds_with_the_id_and_the_last_seeded_version() {
    let gate = files_gate().entry(entry("app-1", "3.1")).build();

    let result = gate.uninstall("app-1").await;

    assert_eq!(result.audit_id.as_deref(), Some("audit-1"));
    assert_eq!(
        result.outcome,
        GateOutcome::Proceed(Proceed {
            envelope: ApplicationEnvelope {
                id: "app-1".to_owned(),
                version: "3.1".to_owned(),
            },
            manifest: None,
            schemas: Vec::new(),
        })
    );
    assert_eq!(
        gate.events.events(),
        [
            "pdp:uninstall",
            "registry:get:app-1",
            "audit:request_handoff"
        ],
        "an uninstall fetches no schema and calls no section plugin"
    );
}

#[tokio::test(start_paused = true)]
async fn a_valid_seed_with_no_section_plugin_proceeds() {
    let gate = GateBuilder::new().build();

    let result = gate.seed(seed_body("a", "1", &json!({}))).await;

    assert!(
        matches!(&result.outcome, GateOutcome::Proceed(p) if p.schemas.is_empty()),
        "{result:?}"
    );
    assert_eq!(
        gate.events.events(),
        ["pdp:seed", "registry:get:a", "audit:request_handoff"]
    );
}

#[tokio::test(start_paused = true)]
async fn the_request_id_the_caller_identity_and_the_receive_time_come_from_the_ports() {
    let gate = files_gate().build();

    let result = gate.seed(valid_body()).await;

    assert_eq!(
        result.request_id,
        Uuid::from_u128(1),
        "first id of the sequential source"
    );
    assert_eq!(result.caller.request_id, result.request_id);
    let ctx = security_context();
    assert_eq!(result.caller.subject_id, ctx.subject_id());
    assert_eq!(result.caller.subject_tenant_id, ctx.subject_tenant_id());
    assert_eq!(result.caller.subject_type.as_deref(), Some("service"));
    assert_eq!(result.caller.token_scopes, ctx.token_scopes());
    // Every port call received the same identity; the hand-off content agrees with it.
    let seen_by_pdp = &gate.policy.calls()[0].ctx;
    assert_eq!(
        CallerIdentity::from_security_context(seen_by_pdp, result.request_id),
        result.caller
    );
    // F-008 (decision: the PDP gets the caller's context unchanged, token included; it was
    // pinned absent before): the token reaches the PDP and no plugin.
    assert_eq!(
        seen_by_pdp.bearer_token().map(ExposeSecret::expose_secret),
        Some("pdp-bearer-token")
    );
    assert_eq!(gate.audit.calls()[0].caller, result.caller);
    assert_eq!(
        gate.sections[0].calls()[0].caller.as_ref(),
        Some(&result.caller)
    );
    let content = handoff(&gate);
    assert_eq!(content.request_id, result.request_id);
    assert_eq!(content.received_at, RECEIVED_AT);
}

#[tokio::test(start_paused = true)]
async fn each_request_takes_the_next_request_id() {
    let gate = files_gate().entry(entry("a", "1")).build();

    let first = gate.uninstall("a").await;
    let second = gate.uninstall("a").await;

    assert_eq!(first.request_id, Uuid::from_u128(1));
    assert_eq!(second.request_id, Uuid::from_u128(2));
}

// ---- pre-dispatch rank 1: platform access control or registry storage cannot answer ----------

#[tokio::test(start_paused = true)]
async fn rank_1_pdp_unavailable_is_temporarily_unavailable_with_no_registry_access() {
    let gate = files_gate()
        .policy(PolicyScript::Unavailable(PdpUnavailable::Unreachable))
        .build();

    let result = gate.seed(valid_body()).await;

    assert_eq!(
        decided(&result),
        &report(
            OutcomeCategory::TemporarilyUnavailable,
            app(Some("app-1"), Some("1.0")),
            vec![],
            vec![access(UnavailableReason::Unreachable)],
            Some("audit-1"),
        )
    );
    // DESIGN §3.6 path 3: the request hand-off is attempted; nothing else is.
    assert_eq!(gate.events.events(), ["pdp:seed", "audit:request_handoff"]);
    assert_eq!(
        handoff(&gate).decided_category,
        Some(OutcomeCategory::TemporarilyUnavailable)
    );
}

// The reason is the policy port's: the gate maps `Unavailable(reason)` to the item and adds no
// reason of its own.
#[tokio::test(start_paused = true)]
async fn rank_1_the_reason_the_policy_port_gives_is_the_reason_of_the_item() {
    for reason in [PdpUnavailable::Unreachable, PdpUnavailable::TimedOut] {
        let gate = files_gate()
            .policy(PolicyScript::Unavailable(reason))
            .build();

        let result = gate.seed(valid_body()).await;

        assert_eq!(decided(&result).unavailable, vec![access(reason.into())]);
        assert_eq!(
            decided(&result).category,
            OutcomeCategory::TemporarilyUnavailable
        );
    }
}

// DESIGN §4.5, §4.7: the PDP deadline is the policy adapter's (`pdp_call_timeout`); the gate adds
// no second bound. The real adapter over a PDP that never answers: `timed_out` after exactly
// one deadline, from the adapter's own expiry.
#[tokio::test(start_paused = true)]
async fn rank_1_a_pdp_that_never_answers_ends_after_exactly_one_deadline_as_timed_out() {
    let (port, authz) = policy_adapter(AuthzScript::Hang);
    let gate = files_gate().policy_port(port).build();
    let started = Instant::now();

    let result = bounded(gate.seed(valid_body())).await;

    assert_eq!(
        decided(&result).unavailable,
        vec![access(UnavailableReason::TimedOut)]
    );
    assert_eq!(
        decided(&result).category,
        OutcomeCategory::TemporarilyUnavailable
    );
    assert_eq!(
        started.elapsed(),
        PDP_TIMEOUT,
        "one pdp_call_timeout, no margin"
    );
    assert_eq!(authz.requests().len(), 1, "one PDP call, no retry");
    assert_eq!(
        gate.events.events(),
        ["audit:request_handoff"],
        "the PDP is behind the adapter here; then the hand-off, nothing else"
    );
    assert!(gate.registry_calls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn rank_1_registry_storage_down_for_a_permitted_caller_is_temporarily_unavailable() {
    for (error, reason) in [
        (RegistryError::Unavailable, UnavailableReason::Unreachable),
        (RegistryError::TimedOut, UnavailableReason::TimedOut),
    ] {
        let gate = files_gate().build();
        gate.memory.fail(RegistryOp::Get, error);

        let result = gate.seed(valid_body()).await;

        assert_eq!(
            decided(&result),
            &report(
                OutcomeCategory::TemporarilyUnavailable,
                app(Some("app-1"), Some("1.0")),
                vec![],
                vec![storage(reason)],
                Some("audit-1"),
            )
        );
        // §3.6 path 5: no schema fetch, no section call, registry unchanged.
        assert_eq!(
            gate.events.events(),
            ["pdp:seed", "registry:get:app-1", "audit:request_handoff"]
        );
        assert!(gate.memory.entries().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn rank_1_a_registry_that_never_answers_ends_after_exactly_one_registry_limit() {
    let gate = files_gate().registry_never_answers().build();
    let started = Instant::now();

    let result = bounded(gate.seed(valid_body())).await;

    assert_eq!(
        decided(&result).unavailable,
        vec![storage(UnavailableReason::TimedOut)]
    );
    assert_eq!(
        started.elapsed(),
        REGISTRY_TIMEOUT,
        "one registry_operation_timeout"
    );
    assert_eq!(
        gate.registry_calls(),
        vec![reg(RegistryOp::Get, Some("app-1"))]
    );
    assert_eq!(
        gate.audit.call_count(),
        1,
        "the hand-off is still attempted"
    );
}

#[tokio::test(start_paused = true)]
async fn rank_1_uninstall_with_storage_down_is_temporarily_unavailable_not_not_found() {
    let gate = files_gate().build();
    gate.memory
        .fail(RegistryOp::Get, RegistryError::Unavailable);

    let result = gate.uninstall("app-1").await;

    assert_eq!(
        decided(&result).category,
        OutcomeCategory::TemporarilyUnavailable
    );
    assert_eq!(
        decided(&result).unavailable,
        vec![storage(UnavailableReason::Unreachable)]
    );
}

// ---- rank 2: refused ----------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn rank_2_a_refused_seed_is_not_permitted_with_no_registry_access() {
    let gate = files_gate().policy(PolicyScript::Refuse).build();

    let result = gate.seed(valid_body()).await;

    assert_eq!(
        decided(&result),
        &report(
            OutcomeCategory::NotPermitted,
            app(Some("app-1"), Some("1.0")),
            vec![],
            vec![],
            Some("audit-1"),
        )
    );
    // §3.6 path 4: the hand-off is attempted, carrying the decided category.
    assert_eq!(gate.events.events(), ["pdp:seed", "audit:request_handoff"]);
    assert_eq!(
        handoff(&gate).decided_category,
        Some(OutcomeCategory::NotPermitted)
    );
    assert!(gate.registry_calls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn rank_2_a_refused_uninstall_is_not_permitted_and_never_looks_the_id_up() {
    let gate = files_gate()
        .policy(PolicyScript::Refuse)
        .entry(entry("app-1", "1"))
        .build();

    let result = gate.uninstall("app-1").await;

    assert_eq!(decided(&result).category, OutcomeCategory::NotPermitted);
    assert_eq!(
        decided(&result).application,
        app(Some("app-1"), None),
        "the id as submitted, no version: the entry was never read"
    );
    assert!(
        gate.registry_calls().is_empty(),
        "no registry access without a permit"
    );
}

// DESIGN §4.8: "a refused caller with storage down (not permitted)".
#[tokio::test(start_paused = true)]
async fn rank_2_a_refused_caller_with_storage_down_is_not_permitted() {
    let gate = files_gate().policy(PolicyScript::Refuse).build();
    gate.memory
        .fail(RegistryOp::Get, RegistryError::Unavailable);

    let result = gate.seed(valid_body()).await;

    assert_eq!(decided(&result).category, OutcomeCategory::NotPermitted);
    assert!(decided(&result).unavailable.is_empty());
    assert!(gate.registry_calls().is_empty());
}

// ADR 0007, §3.3: a permit that carries a constraint (a tenant predicate or any other) cannot
// compile and is not permitted (gate path 4): no registry access, no schema fetch, nothing
// dispatched. The real policy adapter over a fake PDP, through the orchestrator.
#[tokio::test(start_paused = true)]
async fn a_constrained_permit_ends_as_not_permitted_with_no_registry_access_or_dispatch() {
    for constraint in [
        constraint_on("owner_tenant_id", Uuid::from_u128(0x7E)),
        constraint_on("application_id", Uuid::from_u128(5)),
    ] {
        let authz = Arc::new(FakeAuthz::new(AuthzScript::Permit(vec![constraint])));
        let adapter = PolicyAdapter::new(authz.clone(), PDP_TIMEOUT);
        let gate = files_gate().policy_port(Arc::new(adapter)).build();

        let result = gate.seed(valid_body()).await;

        assert_eq!(decided(&result).category, OutcomeCategory::NotPermitted);
        assert_eq!(authz.requests().len(), 1);
        assert!(gate.registry_calls().is_empty(), "no registry access");
        assert!(
            gate.events_with("section:").is_empty(),
            "no schema fetch and no dispatch: {:?}",
            gate.events.events()
        );
        // DESIGN §3.6 path 4: the request hand-off is attempted exactly once and carries the
        // decided category.
        assert_eq!(
            handoff(&gate).decided_category,
            Some(OutcomeCategory::NotPermitted)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn through_the_real_adapter_no_request_names_a_context_tenant_for_seed_or_uninstall() {
    let authz = Arc::new(FakeAuthz::new(AuthzScript::Permit(vec![])));
    let adapter = PolicyAdapter::new(authz.clone(), PDP_TIMEOUT);
    let gate = files_gate()
        .policy_port(Arc::new(adapter))
        .entry(entry("app-2", "1"))
        .build();

    let seed = gate.seed(valid_body()).await;
    let uninstall = gate.uninstall("app-2").await;

    assert!(matches!(seed.outcome, GateOutcome::Proceed(_)));
    assert!(matches!(uninstall.outcome, GateOutcome::Proceed(_)));
    let requests = authz.requests();
    assert_eq!(requests.len(), 2);
    for r in &requests {
        assert!(
            r.context.tenant_context.is_none(),
            "no context tenant, no tenant mode"
        );
        assert!(!r.context.require_constraints);
    }
    assert_eq!(requests[0].action.name, "seed");
    assert_eq!(
        requests[0].resource.properties.get("application_id"),
        Some(&json!("app-1"))
    );
    assert_eq!(requests[1].action.name, "uninstall");
    assert_eq!(
        requests[1].resource.properties.get("application_id"),
        Some(&json!("app-2"))
    );
}

// ---- rank 3: uninstall of an id with no entry --------------------------------------------------

#[tokio::test(start_paused = true)]
async fn rank_3_uninstall_of_an_absent_id_is_not_found_after_one_registry_read() {
    let gate = files_gate().build();

    let result = gate.uninstall("ghost").await;

    assert_eq!(
        decided(&result),
        &report(
            OutcomeCategory::NotFound,
            app(Some("ghost"), None),
            vec![],
            vec![],
            Some("audit-1"),
        )
    );
    assert_eq!(
        gate.registry_calls(),
        vec![reg(RegistryOp::Get, Some("ghost"))]
    );
    assert_eq!(
        gate.events.events(),
        [
            "pdp:uninstall",
            "registry:get:ghost",
            "audit:request_handoff"
        ],
        "§3.6 path 6: the hand-off is attempted"
    );
    let content = handoff(&gate);
    assert_eq!(content.decided_category, Some(OutcomeCategory::NotFound));
    assert_eq!(content.application, app(Some("ghost"), None));
}

// §3.2 step 3, §3.6 path 7: an id that can never be stored is decided as not found with NO
// registry read, even with storage down; the hand-off carries the id's length and digest.
#[tokio::test(start_paused = true)]
async fn rank_3_a_never_storable_uninstall_id_is_not_found_with_no_registry_read() {
    for id in never_storable_ids() {
        let gate = files_gate().build();
        gate.memory
            .fail(RegistryOp::Get, RegistryError::Unavailable);

        let result = gate.uninstall(&id).await;

        assert_eq!(
            decided(&result),
            &report(
                OutcomeCategory::NotFound,
                None,
                vec![],
                vec![],
                Some("audit-1")
            ),
            "id of {} bytes",
            id.len()
        );
        assert!(
            gate.registry_calls().is_empty(),
            "no registry read for {id:?}"
        );
        assert_eq!(
            gate.events.events(),
            ["pdp:uninstall", "audit:request_handoff"]
        );
        let content = handoff(&gate);
        assert_eq!(
            content.application, None,
            "a never-storable id is never carried as a field"
        );
        assert_eq!(content.id_length_bytes, Some(id.len() as u64));
        assert_eq!(
            content.id_sha256.as_deref(),
            Some(sha256_text(&id).as_str())
        );
        assert_eq!(content.decided_category, Some(OutcomeCategory::NotFound));
        // And the PDP got no application_id property for it.
        assert_eq!(gate.policy.calls()[0].application_id, None);
    }
}

#[tokio::test(start_paused = true)]
async fn the_digest_of_the_empty_id_is_the_known_sha256_of_empty_input() {
    let gate = files_gate().build();

    gate.uninstall("").await;

    let content = handoff(&gate);
    assert_eq!(
        content.id_sha256.as_deref(),
        Some("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    );
    assert_eq!(content.id_length_bytes, Some(0));
}

// §3.2 step 3: any other uninstall id is looked up, including one longer than
// `max_envelope_text_length` (that bound is for seed only); 255 bytes is the last storable.
#[tokio::test(start_paused = true)]
async fn an_uninstall_id_that_can_be_stored_is_looked_up_whatever_the_envelope_bound() {
    let long = "l".repeat(MAX_ENVELOPE_LEN + 20);
    let max = "m".repeat(REGISTRY_TEXT_MAX_BYTES);
    let ids = [
        long,
        max,
        "caf\u{e9}".to_owned(),
        " padded ".to_owned(),
        "..".to_owned(),
        "a/b".to_owned(),
    ];
    for id in ids {
        let gate = files_gate().entry(entry(&id, "9")).build();

        let result = gate.uninstall(&id).await;

        assert!(
            matches!(result.outcome, GateOutcome::Proceed(_)),
            "{}: {result:?}",
            id.len()
        );
        assert_eq!(gate.registry_calls(), vec![reg(RegistryOp::Get, Some(&id))]);
        assert_eq!(
            gate.policy.calls()[0].application_id.as_deref(),
            Some(id.as_str())
        );
        let content = handoff(&gate);
        assert_eq!(content.application, app(Some(&id), Some("9")));
        assert_eq!(
            content.id_sha256, None,
            "digest only for a never-storable id"
        );
        assert_eq!(content.id_length_bytes, None);
    }
}

#[tokio::test(start_paused = true)]
async fn uninstall_ids_that_differ_by_case_or_space_do_not_match_each_other() {
    let gate = files_gate().entry(entry("App", "1")).build();

    let wrong_case = gate.uninstall("app").await;
    let wrong_space = gate.uninstall("App ").await;

    assert_eq!(decided(&wrong_case).category, OutcomeCategory::NotFound);
    assert_eq!(decided(&wrong_space).category, OutcomeCategory::NotFound);
}

// ---- rank 4: the request hand-off is not accepted --------------------------------------------

#[tokio::test(start_paused = true)]
async fn rank_4_a_hand_off_that_is_not_accepted_dispatches_nothing_and_fetches_no_schema() {
    let scripts: [(CallBehavior<AuditPluginError>, UnavailableReason); 4] = [
        (
            CallBehavior::err(AuditPluginError::refused("no")),
            UnavailableReason::Refused,
        ),
        (
            CallBehavior::err(AuditPluginError::unavailable("down")),
            UnavailableReason::Unreachable,
        ),
        (CallBehavior::hang(), UnavailableReason::TimedOut),
        (CallBehavior::panic(), UnavailableReason::Unreachable),
    ];
    for (behavior, reason) in scripts {
        let audit = FakeAuditPlugin::new();
        audit.on(AuditCall::RequestHandoff, behavior);
        let gate = files_gate().audit(audit).build();

        let result = bounded(gate.seed(valid_body())).await;

        assert_eq!(
            decided(&result),
            &report(
                OutcomeCategory::TemporarilyUnavailable,
                app(Some("app-1"), Some("1.0")),
                vec![],
                vec![audit_item(reason)],
                None,
            ),
            "{reason:?}"
        );
        assert_eq!(result.audit_id, None, "no audit id");
        // The point of the gate: nothing reaches a section plugin, schema or dispatch.
        assert_eq!(
            gate.events.events(),
            ["pdp:seed", "registry:get:app-1", "audit:request_handoff"],
            "{reason:?}"
        );
        assert_eq!(gate.audit.call_count(), 1, "exactly one attempt, no retry");
        assert!(
            gate.sections[0].calls().is_empty(),
            "no section plugin call of any kind"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn rank_4_a_hung_hand_off_takes_one_plugin_limit() {
    let audit = FakeAuditPlugin::new();
    audit.on(AuditCall::RequestHandoff, CallBehavior::hang());
    let gate = files_gate().audit(audit).build();
    let started = Instant::now();

    bounded(gate.seed(valid_body())).await;

    assert_eq!(started.elapsed(), PLUGIN_TIMEOUT);
}

// §3.2: "If that hand-off is not accepted, a not permitted or not found category is kept, and
// any other request ends as temporarily unavailable."
#[tokio::test(start_paused = true)]
async fn a_hand_off_that_is_not_accepted_keeps_not_permitted() {
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("no")),
    );
    let gate = files_gate()
        .policy(PolicyScript::Refuse)
        .audit(audit)
        .build();

    let result = gate.seed(valid_body()).await;

    assert_eq!(
        decided(&result),
        &report(
            OutcomeCategory::NotPermitted,
            app(Some("app-1"), Some("1.0")),
            vec![],
            vec![audit_item(UnavailableReason::Refused)],
            None,
        )
    );
}

#[tokio::test(start_paused = true)]
async fn a_hand_off_that_is_not_accepted_keeps_not_found() {
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::unavailable("x")),
    );
    let gate = files_gate().audit(audit).build();

    let result = gate.uninstall("ghost").await;

    assert_eq!(decided(&result).category, OutcomeCategory::NotFound);
    assert_eq!(
        decided(&result).unavailable,
        vec![audit_item(UnavailableReason::Unreachable)]
    );
    assert_eq!(result.audit_id, None);
}

#[tokio::test(start_paused = true)]
async fn a_hand_off_that_is_not_accepted_after_a_rank_1_cause_lists_both_dependencies() {
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("no")),
    );
    let gate = files_gate()
        .policy(PolicyScript::Unavailable(PdpUnavailable::Unreachable))
        .audit(audit)
        .build();

    let result = gate.seed(valid_body()).await;

    assert_eq!(
        decided(&result).category,
        OutcomeCategory::TemporarilyUnavailable
    );
    assert_eq!(
        decided(&result).unavailable,
        vec![
            access(UnavailableReason::Unreachable),
            audit_item(UnavailableReason::Refused),
        ],
        "every dependency that could not answer, in the order the gate met them"
    );
}

#[tokio::test(start_paused = true)]
async fn rank_4_a_schema_that_is_not_served_is_temporarily_unavailable_per_key() {
    let gate = files_gate()
        .section("db", json!({"type": "object"}))
        .section("ok", json!({}))
        .build();
    gate.sections[0].on(SectionCall::Schema, CallBehavior::hang());
    gate.sections[1].on(SectionCall::Schema, internal_schema_error());

    let body = seed_body("app-1", "1.0", &json!({"files": {}, "db": {}, "ok": {}}));
    let result = bounded(gate.seed(body)).await;

    assert_eq!(
        decided(&result),
        &report(
            OutcomeCategory::TemporarilyUnavailable,
            app(Some("app-1"), Some("1.0")),
            vec![],
            vec![
                schema_item("files", UnavailableReason::TimedOut),
                schema_item("db", UnavailableReason::Unreachable),
            ],
            Some("audit-1"),
        ),
        "one item per failing key in priority order; the hand-off was accepted"
    );
    assert!(
        gate.events_with("section:")
            .iter()
            .all(|e| e.ends_with(":schema")),
        "nothing dispatched"
    );
}

#[tokio::test(start_paused = true)]
async fn rank_4_an_unusable_schema_is_temporarily_unavailable_with_reason_unusable() {
    let gate = GateBuilder::new()
        .section("good", json!({"type": "object"}))
        .section("bad", json!({"$ref": "https://example.com/other.json"}))
        .build();

    let result = gate
        .seed(seed_body("app-1", "1.0", &json!({"good": {}, "bad": {}})))
        .await;

    assert_eq!(
        decided(&result),
        &report(
            OutcomeCategory::TemporarilyUnavailable,
            app(Some("app-1"), Some("1.0")),
            vec![],
            vec![schema_item("bad", UnavailableReason::Unusable)],
            Some("audit-1"),
        )
    );
}

// §3.2 table: rank 4 (schema) precedes rank 5 (invalid manifest): the manifest is not judged
// against schemas that cannot be used.
#[tokio::test(start_paused = true)]
async fn rank_4_schema_unavailable_beats_an_invalid_manifest() {
    let gate = files_gate().build();
    gate.sections[0].on(SectionCall::Schema, internal_schema_error());

    let invalid_envelope = gate.seed(seed_body("", "1.0", &json!({"files": {}}))).await;
    let malformed = gate.seed(b"{ not json".to_vec()).await;

    for result in [invalid_envelope, malformed] {
        assert_eq!(
            decided(&result).category,
            OutcomeCategory::TemporarilyUnavailable
        );
        assert!(decided(&result).problems.is_empty());
        assert_eq!(
            decided(&result).unavailable,
            vec![schema_item("files", UnavailableReason::Unreachable)]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn rank_4_unusable_schema_beats_an_invalid_manifest() {
    let gate = GateBuilder::new()
        .section("bad", json!({"$ref": "https://example.com/x"}))
        .build();

    let result = gate
        .seed(seed_body("app", "1", &json!({"unknown": 1})))
        .await;

    assert_eq!(
        decided(&result).category,
        OutcomeCategory::TemporarilyUnavailable
    );
    assert_eq!(
        decided(&result).unavailable,
        vec![schema_item("bad", UnavailableReason::Unusable)]
    );
}

// ---- rank 5: invalid manifest ------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn rank_5_an_invalid_manifest_is_invalid_manifest_with_every_problem() {
    let gate = files_gate()
        .section("db", json!({"type": "object", "required": ["host"]}))
        .build();

    let body = seed_body("app-1", "1.0", &json!({"files": 5, "db": {}, "extra": {}}));
    let result = gate.seed(body).await;

    let r = decided(&result);
    assert_eq!(r.category, OutcomeCategory::InvalidManifest);
    let got: Vec<(&str, &str)> = r
        .problems
        .iter()
        .map(|p| (p.location.as_str(), p.rule.as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("/sections/db", "required"),
            ("/sections/extra", "unknown_section"),
            ("/sections/files", "type"),
        ],
        "every problem, sorted by location"
    );
    assert_eq!(r.audit_id.as_deref(), Some("audit-1"));
    assert!(r.unavailable.is_empty());
    assert_eq!(r.registry_effect, RegistryEffect::Unchanged);
    assert_eq!(r.application, app(Some("app-1"), Some("1.0")));
}

#[tokio::test(start_paused = true)]
async fn rank_5_an_invalid_manifest_is_handed_off_before_the_schema_fetch() {
    let gate = files_gate().build();

    gate.seed(seed_body("app-1", "1.0", &json!({"files": 5})))
        .await;

    assert_eq!(
        gate.events.events(),
        [
            "pdp:seed",
            "registry:get:app-1",
            "audit:request_handoff",
            "section:files:schema"
        ],
        "an undecided request is handed off with no decided category, then validated"
    );
    assert_eq!(handoff(&gate).decided_category, None);
}

// ---- precedence: the first matching rank decides ---------------------------------------------

#[tokio::test(start_paused = true)]
async fn rank_1_pdp_down_beats_a_malformed_body() {
    let gate = files_gate()
        .policy(PolicyScript::Unavailable(PdpUnavailable::Unreachable))
        .build();

    let result = gate.seed(b"][".to_vec()).await;

    assert_eq!(
        decided(&result).category,
        OutcomeCategory::TemporarilyUnavailable
    );
    assert!(
        decided(&result).problems.is_empty(),
        "no validation problem: rank 5 never ran"
    );
    assert!(gate.events_with("section:").is_empty());
}

#[tokio::test(start_paused = true)]
async fn rank_2_refused_beats_a_malformed_body() {
    let gate = files_gate().policy(PolicyScript::Refuse).build();

    let result = gate.seed(b"][".to_vec()).await;

    assert_eq!(decided(&result).category, OutcomeCategory::NotPermitted);
    assert!(decided(&result).problems.is_empty());
}

#[tokio::test(start_paused = true)]
async fn rank_1_registry_down_beats_an_invalid_manifest_and_an_unusable_schema() {
    let gate = GateBuilder::new()
        .section("bad", json!({"$ref": "https://example.com/x"}))
        .build();
    gate.memory
        .fail(RegistryOp::Get, RegistryError::Unavailable);

    let result = gate.seed(seed_body("", "", &json!({}))).await;

    assert_eq!(
        decided(&result).unavailable,
        vec![storage(UnavailableReason::Unreachable)]
    );
    assert!(decided(&result).problems.is_empty());
}

#[tokio::test(start_paused = true)]
async fn rank_2_refused_beats_rank_3_not_found() {
    let gate = files_gate().policy(PolicyScript::Refuse).build();

    let result = gate.uninstall("ghost").await;

    assert_eq!(decided(&result).category, OutcomeCategory::NotPermitted);
    assert!(gate.registry_calls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn rank_1_pdp_down_and_rank_2_refused_beat_a_never_storable_uninstall_id() {
    let down = files_gate()
        .policy(PolicyScript::Unavailable(PdpUnavailable::Unreachable))
        .build();
    let refused = files_gate().policy(PolicyScript::Refuse).build();

    let first = down.uninstall("bad\nid").await;
    let second = refused.uninstall("").await;

    assert_eq!(
        decided(&first).category,
        OutcomeCategory::TemporarilyUnavailable
    );
    assert_eq!(decided(&second).category, OutcomeCategory::NotPermitted);
}

// ---- not ready (no startup result) ------------------------------------------------------------

// DEFENSIVE branch: the REST not-ready gate (P8) answers a request before the gear has a startup
// result or while it stops, so a request never reaches `pre_dispatch` without one. The gate still
// answers safely if one does: not ready, nothing called.

#[tokio::test(start_paused = true)]
async fn without_a_published_startup_result_nothing_is_called_and_the_request_is_not_ready() {
    let gate = files_gate().unpublished().build();

    let seed = gate.seed(valid_body()).await;
    let uninstall = gate.uninstall("app-1").await;

    for result in [seed, uninstall] {
        assert_eq!(
            decided(&result),
            &report(
                OutcomeCategory::TemporarilyUnavailable,
                None,
                vec![],
                vec![audit_item(UnavailableReason::NotReady)],
                None,
            )
        );
    }
    assert!(
        gate.events.events().is_empty(),
        "no PDP, registry, hand-off or plugin call"
    );
}

// ---- the application id: one parse, one id (DESIGN §3.2 invariant) -------------------------

#[tokio::test(start_paused = true)]
async fn the_seed_id_is_the_same_value_at_every_use() {
    let gate = files_gate().build();

    let result = gate.seed(valid_body()).await;

    let GateOutcome::Proceed(p) = result.outcome else {
        panic!("proceed")
    };
    assert_eq!(
        gate.policy.calls()[0].application_id.as_deref(),
        Some("app-1")
    );
    assert_eq!(
        gate.registry_calls(),
        vec![reg(RegistryOp::Get, Some("app-1"))]
    );
    assert_eq!(handoff(&gate).application, app(Some("app-1"), Some("1.0")));
    assert_eq!(p.envelope.id, "app-1");
}

// Seed with no extracted id, or an id that can never be stored, reads the empty id (so the step
// still proves storage can answer) and sends no PDP property.
#[tokio::test(start_paused = true)]
async fn a_seed_without_a_storable_id_reads_the_empty_id_and_sends_no_pdp_property() {
    let json_bytes = |v: Value| serde_json::to_vec(&v).expect("serializes");
    let bodies: Vec<(&str, Vec<u8>)> = vec![
        (
            "no id",
            json_bytes(json!({"application": {"version": "1"}, "sections": {"files": {}}})),
        ),
        (
            "id not a string",
            json_bytes(
                json!({"application": {"id": 7, "version": "1"}, "sections": {"files": {}}}),
            ),
        ),
        (
            "no application",
            json_bytes(json!({"sections": {"files": {}}})),
        ),
        ("empty id", seed_body("", "1", &json!({"files": {}}))),
        (
            "control character",
            seed_body("a\u{1}b", "1", &json!({"files": {}})),
        ),
        ("nul", seed_body("a\u{0}b", "1", &json!({"files": {}}))),
        (
            "over 255 bytes",
            seed_body(&"x".repeat(256), "1", &json!({"files": {}})),
        ),
        ("malformed", b"{\"application\": ".to_vec()),
        ("not utf-8", vec![0xff, 0xfe, 0x7b]),
        ("empty body", Vec::new()),
        ("null", b"null".to_vec()),
        ("array", b"[1,2]".to_vec()),
    ];
    for (name, body) in bodies {
        let gate = files_gate().build();

        let result = gate.seed(body).await;

        assert_eq!(gate.policy.calls().len(), 1, "{name}");
        assert_eq!(
            gate.policy.calls()[0].application_id,
            None,
            "{name}: no PDP property"
        );
        assert_eq!(
            gate.registry_calls(),
            vec![reg(RegistryOp::Get, Some(""))],
            "{name}: reads the empty id"
        );
        assert_eq!(
            decided(&result).category,
            OutcomeCategory::InvalidManifest,
            "{name}"
        );
        assert_eq!(result.audit_id.as_deref(), Some("audit-1"), "{name}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_seed_id_beyond_the_envelope_bound_but_storable_is_still_sent_and_read() {
    let id = "i".repeat(MAX_ENVELOPE_LEN + 1);
    let gate = files_gate().build();

    let result = gate.seed(seed_body(&id, "1", &json!({"files": {}}))).await;

    assert_eq!(
        gate.policy.calls()[0].application_id.as_deref(),
        Some(id.as_str())
    );
    assert_eq!(gate.registry_calls(), vec![reg(RegistryOp::Get, Some(&id))]);
    assert_eq!(decided(&result).category, OutcomeCategory::InvalidManifest);
    assert_eq!(
        decided(&result).application,
        app(None, Some("1")),
        "the id is over the envelope limit, so the outcome omits it; the version is within"
    );
    assert_eq!(handoff(&gate).application, app(None, Some("1")));
    assert!(
        handoff(&gate).manifest.is_some(),
        "the parsed body is still the manifest"
    );
}

#[tokio::test(start_paused = true)]
async fn a_member_name_written_with_an_escape_repeats_the_plain_name_and_no_id_is_extracted() {
    // The escaped spelling of `id` (backslash, u, 0069, d) and `id` are one member name:
    // the repeat ends the parse (DESIGN §3.2 step 1), so nothing reaches the PDP property,
    // the registry reads the empty id, and the hand-off has no manifest.
    let escaped_id = ["\\", "u0069d"].concat();
    let body = format!(
        r#"{{"application": {{"id": "a", "{escaped_id}": "b", "version": "1"}}, "sections": {{"files": {{}}}}}}"#
    );
    assert!(body.contains("\\u0069d"), "the test body holds the escape");
    let gate = files_gate().build();

    let result = gate.seed(body.into_bytes()).await;

    assert_eq!(gate.policy.calls()[0].application_id, None);
    assert_eq!(gate.registry_calls(), vec![reg(RegistryOp::Get, Some(""))]);
    let content = handoff(&gate);
    assert_eq!(content.manifest, None);
    assert_eq!(content.application, None);
    let r = decided(&result);
    assert_eq!(r.category, OutcomeCategory::InvalidManifest);
    assert_eq!(r.problems.len(), 1);
    assert_eq!(r.problems[0].rule, "duplicate_member");
    assert_eq!(r.problems[0].location, "/application");
}

// ---- the request hand-off content (DESIGN §3.1 "Audit event content") --------------------------

#[tokio::test(start_paused = true)]
async fn the_seed_hand_off_carries_the_operation_ids_manifest_and_no_decided_category() {
    let gate = files_gate().build();

    gate.seed(valid_body()).await;

    assert_eq!(
        handoff(&gate),
        RequestHandoffContent {
            operation: HandoffOperation::Seed,
            request_id: Uuid::from_u128(1),
            received_at: RECEIVED_AT.to_owned(),
            application: app(Some("app-1"), Some("1.0")),
            manifest: Some(json!({
                "application": {"id": "app-1", "version": "1.0"},
                "sections": {"files": {}},
            })),
            decided_category: None,
            id_length_bytes: None,
            id_sha256: None,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn the_uninstall_hand_off_carries_the_id_and_last_version_and_no_manifest() {
    let gate = files_gate().entry(entry("app-1", "4.2")).build();

    gate.uninstall("app-1").await;

    assert_eq!(
        handoff(&gate),
        RequestHandoffContent {
            operation: HandoffOperation::Uninstall,
            request_id: Uuid::from_u128(1),
            received_at: RECEIVED_AT.to_owned(),
            application: app(Some("app-1"), Some("4.2")),
            manifest: None,
            decided_category: None,
            id_length_bytes: None,
            id_sha256: None,
        }
    );
}

#[tokio::test(start_paused = true)]
async fn an_invalid_body_is_handed_off_without_a_manifest_and_a_null_body_with_a_null_manifest() {
    let gate = files_gate().build();
    gate.seed(b"{ nope".to_vec()).await;
    assert_eq!(
        handoff(&gate).manifest,
        None,
        "an invalid body is not a manifest"
    );
    assert_eq!(handoff(&gate).application, None);

    let gate = files_gate().build();
    gate.seed(b"null".to_vec()).await;
    assert_eq!(
        handoff(&gate).manifest,
        Some(Value::Null),
        "a JSON null body is carried as null, distinct from an omitted member"
    );
}

#[tokio::test(start_paused = true)]
async fn the_hand_off_application_omits_a_member_that_is_over_the_envelope_limit() {
    let gate = files_gate().build();

    gate.seed(seed_body(
        "fine",
        &"v".repeat(MAX_ENVELOPE_LEN + 1),
        &json!({"files": {}}),
    ))
    .await;

    assert_eq!(handoff(&gate).application, app(Some("fine"), None));
}

#[tokio::test(start_paused = true)]
async fn every_decided_rank_before_the_hand_off_puts_its_category_in_the_hand_off() {
    let cases: Vec<(Gate, bool, OutcomeCategory)> = vec![
        (
            files_gate()
                .policy(PolicyScript::Unavailable(PdpUnavailable::Unreachable))
                .build(),
            true,
            OutcomeCategory::TemporarilyUnavailable,
        ),
        (
            files_gate().policy(PolicyScript::Refuse).build(),
            true,
            OutcomeCategory::NotPermitted,
        ),
        (files_gate().build(), false, OutcomeCategory::NotFound),
    ];
    for (gate, seed, category) in cases {
        if seed {
            gate.seed(valid_body()).await;
        } else {
            gate.uninstall("ghost").await;
        }

        assert_eq!(handoff(&gate).decided_category, Some(category));
    }
}

// ---- invariants over every pre-dispatch scenario ----------------------------------------------

struct Scenario {
    name: &'static str,
    gate: Gate,
    seed: bool,
    permitted: bool,
}

fn scenarios() -> Vec<Scenario> {
    let refused_audit = || {
        let audit = FakeAuditPlugin::new();
        audit.on(
            AuditCall::RequestHandoff,
            CallBehavior::err(AuditPluginError::refused("no")),
        );
        audit
    };
    let schema_down = || {
        let gate = files_gate().build();
        gate.sections[0].on(SectionCall::Schema, CallBehavior::hang());
        gate
    };
    let s = |name, gate, seed, permitted| Scenario {
        name,
        gate,
        seed,
        permitted,
    };
    vec![
        s("valid seed", files_gate().build(), true, true),
        s(
            "valid uninstall",
            files_gate().entry(entry("u", "1")).build(),
            false,
            true,
        ),
        s(
            "pdp down",
            files_gate()
                .policy(PolicyScript::Unavailable(PdpUnavailable::Unreachable))
                .build(),
            true,
            false,
        ),
        s(
            "pdp hangs",
            files_gate()
                .policy_port(policy_adapter(AuthzScript::Hang).0)
                .build(),
            true,
            false,
        ),
        s(
            "refused seed",
            files_gate().policy(PolicyScript::Refuse).build(),
            true,
            false,
        ),
        s(
            "refused uninstall",
            files_gate().policy(PolicyScript::Refuse).build(),
            false,
            false,
        ),
        s(
            "registry hangs",
            files_gate().registry_never_answers().build(),
            true,
            true,
        ),
        s("uninstall absent", files_gate().build(), false, true),
        s(
            "hand-off refused",
            files_gate().audit(refused_audit()).build(),
            true,
            true,
        ),
        s("schema down", schema_down(), true, true),
        s(
            "unusable schema",
            GateBuilder::new()
                .section("files", json!({"$ref": "https://e.x/y"}))
                .build(),
            true,
            true,
        ),
        s("invalid manifest", files_gate().build(), true, true),
        s("not ready", files_gate().unpublished().build(), true, false),
    ]
}

fn body_for(name: &str) -> Vec<u8> {
    if name == "invalid manifest" {
        seed_body("", "", &json!({"files": 1}))
    } else {
        valid_body()
    }
}

// Runs the request of a scenario and returns its result.
async fn run_scenario(sc: &Scenario) -> crate::domain::orchestrator::GateResult {
    if sc.seed {
        bounded(sc.gate.seed(body_for(sc.name))).await
    } else if sc.name == "valid uninstall" {
        bounded(sc.gate.uninstall("u")).await
    } else {
        bounded(sc.gate.uninstall("ghost")).await
    }
}

// DESIGN §3.2 invariants, judged on the fakes' own call records (not on event labels): `pre_dispatch`
// makes no `apply` or `remove` call (a section plugin is only ever asked for its `schema`), no
// registry change or listing, no registry access without a permit, exactly one attempt per
// hand-off, and no schema call before an accepted hand-off.
#[tokio::test(start_paused = true)]
async fn pre_dispatch_never_dispatches_or_writes_on_any_path() {
    for sc in scenarios() {
        let registry_before = sc.gate.memory.entries();

        let result = run_scenario(&sc).await;

        for (i, plugin) in sc.gate.sections.iter().enumerate() {
            for call in plugin.calls() {
                assert_eq!(
                    call.kind,
                    SectionCall::Schema,
                    "{}: section {i} was asked for {:?}",
                    sc.name,
                    call.kind
                );
            }
        }
        for call in sc.gate.registry_calls() {
            assert_eq!(
                call.op,
                RegistryOp::Get,
                "{}: a registry change or listing: {call:?}",
                sc.name
            );
        }
        assert_eq!(
            sc.gate.memory.entries(),
            registry_before,
            "{}: registry unchanged",
            sc.name
        );
        if !sc.permitted {
            assert!(
                sc.gate.registry_calls().is_empty(),
                "{}: registry access without a permit",
                sc.name
            );
        }
        let audit_calls = sc.gate.audit.calls();
        assert!(
            audit_calls.len() <= 1,
            "{}: one attempt per hand-off",
            sc.name
        );
        assert!(
            audit_calls
                .iter()
                .all(|c| c.kind == AuditCall::RequestHandoff),
            "{}: only the request hand-off is made",
            sc.name
        );
        // No section call before the request hand-off was attempted and accepted.
        let events = sc.gate.events.events();
        let schema_calls: usize = sc.gate.sections.iter().map(|p| p.call_count()).sum();
        if schema_calls > 0 {
            let first_section = events
                .iter()
                .position(|e| e.starts_with("section:"))
                .expect("a section call is in the event log");
            let handoff_at = events.iter().position(|e| e == "audit:request_handoff");
            assert!(
                handoff_at.is_some_and(|h| h < first_section),
                "{}: a section call before the hand-off in {events:?}",
                sc.name
            );
            assert!(
                result.audit_id.is_some(),
                "{}: a section call without an accepted hand-off",
                sc.name
            );
        }
        // The outcome of a decided request never claims a registry effect or a section outcome.
        if let GateOutcome::Decided(r) = &result.outcome {
            assert_eq!(r.registry_effect, RegistryEffect::Unchanged, "{}", sc.name);
            assert!(r.sections.is_empty() && !r.partial, "{}", sc.name);
            assert_eq!(
                r.audit_id, result.audit_id,
                "{}: the outcome and the result agree on the audit id",
                sc.name
            );
        }
    }
}

// Scoped to `pre_dispatch` (steps 1-5): the final-result hand-off is step 8 and belongs to the
// full request, which adds it; here no path of the pre-dispatch gate makes one.
#[tokio::test(start_paused = true)]
async fn pre_dispatch_makes_no_final_result_hand_off_on_any_path() {
    for sc in scenarios() {
        run_scenario(&sc).await;

        assert!(
            sc.gate
                .audit
                .calls()
                .iter()
                .all(|c| c.audit_id.is_none() && c.result.is_none()),
            "{}: a final-result hand-off",
            sc.name
        );
    }
}

// "Assert no dispatch before an accepted request hand-off (fake call log)": a slow hand-off
// still completes before the first schema call starts.
#[tokio::test(start_paused = true)]
async fn the_first_schema_call_starts_only_after_a_slow_hand_off_was_accepted() {
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::ok().with_delay(Duration::from_millis(300)),
    );
    let gate = files_gate().audit(audit).build();
    let started = Instant::now();

    gate.seed(valid_body()).await;

    assert_eq!(
        gate.events.events(),
        [
            "pdp:seed",
            "registry:get:app-1",
            "audit:request_handoff",
            "section:files:schema"
        ]
    );
    assert_eq!(gate.audit.completed(), 1, "the hand-off had returned");
    assert_eq!(started.elapsed(), Duration::from_millis(300));
}

// ---- timing: the bounds have no margin -----------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_hung_pdp_then_a_hung_hand_off_cost_one_limit_each_and_nothing_else() {
    let audit = FakeAuditPlugin::new();
    audit.on(AuditCall::RequestHandoff, CallBehavior::hang());
    let gate = files_gate()
        .policy_port(policy_adapter(AuthzScript::Hang).0)
        .audit(audit)
        .build();
    let started = Instant::now();

    bounded(gate.seed(valid_body())).await;

    assert_eq!(started.elapsed(), PDP_TIMEOUT + PLUGIN_TIMEOUT);
}

// ---- configuration --------------------------------------------------------------------------------

#[test]
fn the_settings_come_from_the_config_with_the_plugin_limit_as_the_step_timeout() {
    let config: ApplicationManagerConfig = serde_json::from_value(json!({
        "plugin_call_timeout": "700ms",
        "plugin_max_in_flight": 8,
        "max_request_tasks": 16,
        "max_envelope_text_length": 77,
        "pdp_call_timeout": "2s",
        "registry_operation_timeout": "3s"
    }))
    .expect("a valid config");

    let settings = OrchestratorSettings::from_config(&config);

    assert_eq!(settings.max_envelope_text_length, 77);
    assert_eq!(settings.registry_operation_timeout, Duration::from_secs(3));
    assert_eq!(settings.step_timeout, Duration::from_millis(700));
}

// ---- logs -------------------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn an_unavailable_dependency_is_logged_with_its_name_and_reason() {
    let (logs, _guard) = capture_logs();
    let gate = files_gate()
        .policy(PolicyScript::Unavailable(PdpUnavailable::Unreachable))
        .build();

    gate.seed(valid_body()).await;

    logs.assert_live();
    let lines = logs.lines_with("dependency_unavailable");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "dependency", "access_control"),
        "{}",
        lines[0]
    );
    assert!(
        has_field(&lines[0], "reason", "unreachable"),
        "{}",
        lines[0]
    );
    assert_eq!(
        field_value(&lines[0], "request_id").as_deref(),
        Some(request_id_text(1).as_str()),
        "{}",
        lines[0]
    );
}

#[tokio::test(start_paused = true)]
async fn an_unusable_schema_is_logged_with_the_key_and_the_cause_only() {
    let (logs, _guard) = capture_logs();
    let gate = GateBuilder::new()
        .section(
            "bad",
            json!({"$ref": "https://example.com/SECRET-SCHEMA-TEXT"}),
        )
        .build();

    gate.seed(seed_body("app", "1", &json!({"bad": {}}))).await;

    logs.assert_live();
    let lines = logs.lines_with("unusable_schema");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "key", "bad"), "{}", lines[0]);
    assert!(
        has_field(&lines[0], "cause", "external_reference"),
        "{}",
        lines[0]
    );
    assert!(
        !logs.text().contains("SECRET-SCHEMA-TEXT"),
        "schema content in the log"
    );
}

// §4.8 security (c): a sentinel manifest value sent through the paths is absent from the logs of
// EVERY thread: the capture is process-wide (filtered by the sentinel), so an event from the
// blocking validation thread is seen, and a probe from a blocking thread proves it.
#[tokio::test(start_paused = true)]
async fn a_sentinel_manifest_value_never_reaches_the_logs_on_any_pre_dispatch_path() {
    let sentinel = "SENTINEL-9d41-manifest-value";
    let (logs, _guard) = capture_logs_containing(sentinel);
    logs.assert_live_on_a_blocking_thread().await;
    let body = seed_body("app-1", "1.0", &json!({"files": {"secret": sentinel}}));
    for script in [
        PolicyScript::Permit,
        PolicyScript::Refuse,
        PolicyScript::Unavailable(PdpUnavailable::Unreachable),
    ] {
        files_gate().policy(script).build().seed(body.clone()).await;
    }
    let schema_down = files_gate().build();
    schema_down.sections[0].on(SectionCall::Schema, CallBehavior::hang());
    bounded(schema_down.seed(body)).await;
    files_gate()
        .build()
        .seed(seed_body(
            "app-1",
            "1.0",
            &json!({"files": 1, "secret": sentinel}),
        ))
        .await;
    GateBuilder::new()
        .section("files", json!({"$ref": "https://example.com/x"}))
        .build()
        .seed(seed_body(
            "app-1",
            "1.0",
            &json!({"files": {"secret": sentinel}}),
        ))
        .await;

    let seen = logs.text();
    let own_lines: Vec<&str> = seen
        .lines()
        .filter(|l| !l.contains("blocking_probe"))
        .collect();
    assert!(
        own_lines.is_empty(),
        "a manifest value reached a log:\n{}",
        own_lines.join("\n")
    );
}

// ---- the policy action and the id the orchestrator sends ------------------------------------------------

#[tokio::test(start_paused = true)]
async fn seed_asks_for_seed_and_uninstall_for_uninstall() {
    let gate = files_gate().build();

    gate.seed(valid_body()).await;
    gate.uninstall("x").await;

    let calls = gate.policy.calls();
    assert_eq!(calls[0].action, PolicyAction::Seed);
    assert_eq!(calls[1].action, PolicyAction::Uninstall);
    assert_eq!(calls[1].application_id.as_deref(), Some("x"));
}

#[tokio::test(start_paused = true)]
async fn a_decided_report_for_a_seed_names_the_application_only_with_members_within_the_limits() {
    let gate = files_gate().policy(PolicyScript::Refuse).build();

    let both = gate.seed(seed_body("id", "v", &json!({}))).await;
    let none = gate.seed(b"{}".to_vec()).await;

    assert_eq!(decided(&both).application, app(Some("id"), Some("v")));
    assert_eq!(
        decided(&none).application,
        None,
        "nothing extracted: the member is omitted"
    );
}

// ---- a seed over an existing entry ---------------------------------------------------------------

// DESIGN §3.2: a seed replaces the entry; the registry read of step 3 never blocks it and the
// stored version never replaces the manifest's own.
#[tokio::test(start_paused = true)]
async fn a_re_seed_of_an_existing_entry_proceeds_with_the_manifests_own_envelope() {
    let gate = files_gate().entry(entry("app-1", "0.9")).build();

    let result = gate.seed(valid_body()).await;

    assert_eq!(result.audit_id.as_deref(), Some("audit-1"));
    assert_eq!(
        result.outcome,
        GateOutcome::Proceed(Proceed {
            envelope: ApplicationEnvelope {
                id: "app-1".to_owned(),
                version: "1.0".to_owned(),
            },
            manifest: Some(json!({
                "application": {"id": "app-1", "version": "1.0"},
                "sections": {"files": {}},
            })),
            schemas: vec![SectionSchema {
                key: "files".to_owned(),
                schema: json!({"type": "object"}),
            }],
        })
    );
    let content = handoff(&gate);
    assert_eq!(
        content.application,
        app(Some("app-1"), Some("1.0")),
        "the hand-off names the manifest's version, not the stored 0.9"
    );
    assert_eq!(
        content.decided_category, None,
        "an existing entry decides nothing"
    );
    assert_eq!(
        gate.registry_calls(),
        vec![reg(RegistryOp::Get, Some("app-1"))]
    );
    assert_eq!(
        gate.events.events(),
        [
            "pdp:seed",
            "registry:get:app-1",
            "audit:request_handoff",
            "section:files:schema"
        ]
    );
}

// ---- the schema fetch at the gate -----------------------------------------------------------------

// DESIGN §3.3 "so the key phase takes one time limit": the schema phase too. Three plugins that
// never answer cost one limit in all, and each is asked exactly once.
#[tokio::test(start_paused = true)]
async fn rank_4_hung_schemas_cost_one_plugin_limit_in_all_and_one_call_per_key() {
    let gate = files_gate()
        .section("db", json!({"type": "object"}))
        .section("cache", json!({"type": "object"}))
        .build();
    for plugin in &gate.sections {
        plugin.on(SectionCall::Schema, CallBehavior::hang());
    }
    let body = seed_body("app-1", "1.0", &json!({"files": {}, "db": {}, "cache": {}}));
    let started = Instant::now();

    let result = bounded(gate.seed(body)).await;

    assert_eq!(
        started.elapsed(),
        PLUGIN_TIMEOUT,
        "one plugin_call_timeout for all three, not three"
    );
    assert_eq!(
        decided(&result).unavailable,
        vec![
            schema_item("files", UnavailableReason::TimedOut),
            schema_item("db", UnavailableReason::TimedOut),
            schema_item("cache", UnavailableReason::TimedOut),
        ]
    );
    let mut schema_events = gate.events_with("section:");
    schema_events.sort_unstable();
    assert_eq!(
        schema_events,
        [
            "section:cache:schema",
            "section:db:schema",
            "section:files:schema"
        ],
        "one schema call per key and nothing else"
    );
}

// DESIGN §3.2 "no retry": a schema that cannot be served is asked for once per request.
#[tokio::test(start_paused = true)]
async fn rank_4_a_schema_that_fails_is_asked_for_once_whatever_the_failure() {
    let gate = files_gate()
        .section("db", json!({}))
        .section("cache", json!({}))
        .section("ok", json!({}))
        .build();
    gate.sections[0].on(SectionCall::Schema, CallBehavior::hang());
    gate.sections[1].on(SectionCall::Schema, internal_schema_error());
    gate.sections[2].on(
        SectionCall::Schema,
        CallBehavior::err(SectionPluginError::rejected("no", None)),
    );

    bounded(gate.seed(seed_body(
        "app-1",
        "1.0",
        &json!({"files": {}, "db": {}, "cache": {}, "ok": {}}),
    )))
    .await;

    for (i, plugin) in gate.sections.iter().enumerate() {
        assert_eq!(plugin.call_count(), 1, "section {i}: one call, no retry");
    }
}

#[tokio::test(start_paused = true)]
async fn rank_4_a_schema_plugin_at_its_in_flight_cap_is_reported_not_sent() {
    let gate = files_gate().guard_cap(1).build();
    gate.sections[0].on(SectionCall::Schema, CallBehavior::hang());
    let first = bounded(gate.seed(valid_body())).await;
    assert_eq!(
        decided(&first).unavailable,
        vec![schema_item("files", UnavailableReason::TimedOut)]
    );

    let second = bounded(gate.seed(valid_body())).await;

    assert_eq!(
        decided(&second),
        &report(
            OutcomeCategory::TemporarilyUnavailable,
            app(Some("app-1"), Some("1.0")),
            vec![],
            vec![schema_item("files", UnavailableReason::NotSent)],
            Some("audit-1"),
        )
    );
    assert_eq!(
        gate.sections[0].call_count(),
        1,
        "the refused call never reached the plugin"
    );
}

#[tokio::test(start_paused = true)]
async fn rank_4_a_hand_off_at_the_audit_plugins_in_flight_cap_is_not_accepted_as_not_sent() {
    let audit = FakeAuditPlugin::new();
    audit.on(AuditCall::RequestHandoff, CallBehavior::hang());
    let gate = files_gate().audit(audit).guard_cap(1).build();
    let first = bounded(gate.seed(valid_body())).await;
    assert_eq!(
        decided(&first).unavailable,
        vec![audit_item(UnavailableReason::TimedOut)]
    );

    let second = bounded(gate.seed(valid_body())).await;

    assert_eq!(
        decided(&second),
        &report(
            OutcomeCategory::TemporarilyUnavailable,
            app(Some("app-1"), Some("1.0")),
            vec![],
            vec![audit_item(UnavailableReason::NotSent)],
            None,
        )
    );
    assert_eq!(
        gate.audit.call_count(),
        1,
        "the refused call never reached the plugin"
    );
    assert!(
        gate.sections[0].calls().is_empty(),
        "no schema call after a hand-off that was not accepted"
    );
}

// §3.3 "Schema errors": the items follow the priority order whichever plugin fails how.
#[tokio::test(start_paused = true)]
async fn rank_4_unreachable_and_unusable_schemas_are_both_listed_in_priority_order() {
    let rejected = || CallBehavior::err(SectionPluginError::rejected("no", None));
    for (first, second, expected) in [
        (
            internal_schema_error(),
            rejected(),
            [UnavailableReason::Unreachable, UnavailableReason::Unusable],
        ),
        (
            rejected(),
            internal_schema_error(),
            [UnavailableReason::Unusable, UnavailableReason::Unreachable],
        ),
    ] {
        let gate = GateBuilder::new()
            .section("zz", json!({}))
            .section("aa", json!({}))
            .section("mm", json!({}))
            .build();
        gate.sections[0].on(SectionCall::Schema, first);
        gate.sections[1].on(SectionCall::Schema, second);

        let result = gate
            .seed(seed_body(
                "app",
                "1",
                &json!({"zz": {}, "aa": {}, "mm": {}}),
            ))
            .await;

        assert_eq!(
            decided(&result).unavailable,
            vec![
                schema_item("zz", expected[0]),
                schema_item("aa", expected[1]),
            ],
            "priority order (zz first), not key order"
        );
        assert_eq!(
            decided(&result).category,
            OutcomeCategory::TemporarilyUnavailable
        );
    }
}

#[tokio::test(start_paused = true)]
async fn rank_4_unusable_schemas_are_listed_in_priority_order() {
    let gate = GateBuilder::new()
        .section("zz", json!({"$ref": "https://example.com/z"}))
        .section("mm", json!({"type": "object"}))
        .section("aa", json!({"$ref": "https://example.com/a"}))
        .build();

    let result = gate
        .seed(seed_body(
            "app",
            "1",
            &json!({"zz": {}, "mm": {}, "aa": {}}),
        ))
        .await;

    assert_eq!(
        decided(&result).unavailable,
        vec![
            schema_item("zz", UnavailableReason::Unusable),
            schema_item("aa", UnavailableReason::Unusable),
        ],
        "both unusable keys, in priority order"
    );
    assert!(decided(&result).problems.is_empty());
}

// §3.2 table: rank 4 (the schemas) precedes rank 5 for a body that did not parse too: the
// schema check still runs, and an unusable schema ends the request.
#[tokio::test(start_paused = true)]
async fn rank_4_an_unusable_schema_beats_a_malformed_body() {
    let gate = GateBuilder::new()
        .section("bad", json!({"$ref": "https://example.com/x"}))
        .build();

    let result = gate.seed(b"{ not json".to_vec()).await;

    assert_eq!(
        decided(&result),
        &report(
            OutcomeCategory::TemporarilyUnavailable,
            None,
            vec![],
            vec![schema_item("bad", UnavailableReason::Unusable)],
            Some("audit-1"),
        ),
        "no parse problem: the unusable schema decided first"
    );
}

#[tokio::test(start_paused = true)]
async fn a_proceeding_seed_lists_its_schemas_in_priority_order() {
    let gate = GateBuilder::new()
        .section("zeta", json!({"type": "object", "title": "z"}))
        .section("alpha", json!({"type": "object", "title": "a"}))
        .section("mid", json!({"type": "object", "title": "m"}))
        .build();

    let result = gate
        .seed(seed_body(
            "app",
            "1",
            &json!({"zeta": {}, "alpha": {}, "mid": {}}),
        ))
        .await;

    let GateOutcome::Proceed(p) = result.outcome else {
        panic!("proceed")
    };
    let keys: Vec<&str> = p.schemas.iter().map(|s| s.key.as_str()).collect();
    assert_eq!(
        keys,
        ["zeta", "alpha", "mid"],
        "priority order, not key order"
    );
    assert_eq!(
        p.schemas[1].schema["title"], "a",
        "each key keeps its own schema"
    );
}

// ---- the hand-off of a request that ended on a receive problem ------------------------------------

// DESIGN §3.1 "Audit event content": `decided_category` is present only when the category was
// decided before the hand-off. A body that did not parse, or an envelope over the limits, is
// found by the validation after it (step 5), so the hand-off carries no category.
#[tokio::test(start_paused = true)]
async fn a_seed_with_receive_problems_is_handed_off_with_no_decided_category() {
    let bodies = [
        ("malformed", b"{ nope".to_vec()),
        ("empty id", seed_body("", "1", &json!({"files": {}}))),
        (
            "id over the limit",
            seed_body(
                &"i".repeat(MAX_ENVELOPE_LEN + 1),
                "1",
                &json!({"files": {}}),
            ),
        ),
        (
            "duplicate member",
            br#"{"application": {"id": "a", "id": "b"}}"#.to_vec(),
        ),
    ];
    for (name, body) in bodies {
        let gate = files_gate().build();

        let result = gate.seed(body).await;

        assert_eq!(handoff(&gate).decided_category, None, "{name}");
        assert_eq!(
            decided(&result).category,
            OutcomeCategory::InvalidManifest,
            "{name}"
        );
    }
}

// ---- a hand-off that is not accepted, for an uninstall and with other causes ---------------------

#[tokio::test(start_paused = true)]
async fn an_uninstall_whose_hand_off_is_not_accepted_keeps_the_last_version_in_the_outcome() {
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("no")),
    );
    let gate = files_gate()
        .audit(audit)
        .entry(entry("app-1", "4.2"))
        .build();

    let result = gate.uninstall("app-1").await;

    assert_eq!(
        decided(&result),
        &report(
            OutcomeCategory::TemporarilyUnavailable,
            app(Some("app-1"), Some("4.2")),
            vec![],
            vec![audit_item(UnavailableReason::Refused)],
            None,
        ),
        "found and undecided: temporarily unavailable, naming the entry it found"
    );
    assert_eq!(result.audit_id, None);
}

#[tokio::test(start_paused = true)]
async fn a_hand_off_that_is_not_accepted_after_a_registry_failure_lists_registry_then_audit() {
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::unavailable("down")),
    );
    let gate = files_gate().audit(audit).build();
    gate.memory.fail(RegistryOp::Get, RegistryError::TimedOut);

    let result = gate.seed(valid_body()).await;

    assert_eq!(
        decided(&result),
        &report(
            OutcomeCategory::TemporarilyUnavailable,
            app(Some("app-1"), Some("1.0")),
            vec![],
            vec![
                storage(UnavailableReason::TimedOut),
                audit_item(UnavailableReason::Unreachable),
            ],
            None,
        ),
        "the registry cause first, then the audit plugin: the order the gate met them"
    );
}

// ---- a never-storable uninstall id on every outcome ------------------------------------------------

// DESIGN §3.1: an id that can never be stored is never carried as a field; its length in BYTES
// and its SHA-256 describe it in the request hand-off on every outcome that reaches the
// hand-off, with the category decided at that point.
#[tokio::test(start_paused = true)]
async fn a_never_storable_uninstall_id_is_described_by_its_byte_length_and_digest_on_every_outcome()
{
    let ids = [
        "a\nb".to_owned(),
        "caf\u{e9}\n".to_owned(),
        "\u{1f600}\u{0}".to_owned(),
        "\u{e9}".repeat(130), // 260 bytes: over the 255-byte bound, 130 characters
    ];
    let outcomes = [
        (
            PolicyScript::Unavailable(PdpUnavailable::Unreachable),
            OutcomeCategory::TemporarilyUnavailable,
        ),
        (PolicyScript::Refuse, OutcomeCategory::NotPermitted),
        (PolicyScript::Permit, OutcomeCategory::NotFound),
    ];
    assert!(
        ids.iter()
            .filter(|id| id.len() > id.chars().count())
            .count()
            >= 3,
        "the multi-byte ids tell a byte length from a character count"
    );
    for id in &ids {
        for (script, category) in outcomes {
            let gate = files_gate().policy(script).build();

            let result = gate.uninstall(id).await;

            let content = handoff(&gate);
            assert_eq!(content.application, None, "{category:?}: no id field");
            assert_eq!(
                content.id_length_bytes,
                Some(id.len() as u64),
                "{category:?}"
            );
            assert_eq!(
                content.id_sha256.as_deref(),
                Some(sha256_text(id).as_str()),
                "{category:?}"
            );
            assert_eq!(content.decided_category, Some(category));
            assert_eq!(decided(&result).application, None, "{category:?}");
            assert_eq!(decided(&result).category, category);
            assert!(gate.registry_calls().is_empty(), "{category:?}: no read");
        }
    }
}

// ---- logs: every dependency, the request id, not ready, the contract break ------------------------

fn request_id_text(n: u128) -> String {
    Uuid::from_u128(n).to_string()
}

#[tokio::test(start_paused = true)]
async fn each_unavailable_dependency_is_logged_with_its_name_reason_key_and_request_id() {
    let refused_audit = FakeAuditPlugin::new();
    refused_audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("no")),
    );
    let registry_down = files_gate().build();
    registry_down
        .memory
        .fail(RegistryOp::Get, RegistryError::Unavailable);
    let schema_down = files_gate().build();
    schema_down.sections[0].on(SectionCall::Schema, CallBehavior::hang());
    // (gate, dependency token, key, reason token)
    let cases: Vec<(Gate, &str, Option<&str>, &str)> = vec![
        (registry_down, "registry_storage", None, "unreachable"),
        (
            files_gate().audit(refused_audit).build(),
            "audit_plugin",
            None,
            "refused",
        ),
        (schema_down, "section_schema", Some("files"), "timed_out"),
    ];
    for (gate, dependency, key, reason) in cases {
        let (logs, _guard) = capture_logs();

        bounded(gate.seed(valid_body())).await;

        logs.assert_live();
        let lines = logs.lines_with("dependency_unavailable");
        assert_eq!(lines.len(), 1, "{dependency}: {lines:?}");
        assert!(
            has_field(&lines[0], "dependency", dependency),
            "{}",
            lines[0]
        );
        assert!(has_field(&lines[0], "reason", reason), "{}", lines[0]);
        assert_eq!(
            field_value(&lines[0], "key").as_deref(),
            key,
            "{dependency}: {}",
            lines[0]
        );
        assert_eq!(
            field_value(&lines[0], "request_id").as_deref(),
            Some(request_id_text(1).as_str()),
            "{}",
            lines[0]
        );
    }
}

// §4.6: not ready is a request the gate cannot hand off; it writes the one
// `handoff_not_accepted` event with its reason, and no content.
#[tokio::test(start_paused = true)]
async fn a_request_with_no_startup_result_logs_one_handoff_not_accepted_with_reason_not_ready() {
    let (logs, _guard) = capture_logs();
    let gate = files_gate().unpublished().build();

    gate.seed(valid_body()).await;

    logs.assert_live();
    let lines = logs.lines_with("handoff_not_accepted");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "reason", "not_ready"), "{}", lines[0]);
    assert!(has_field(&lines[0], "kind", "request"), "{}", lines[0]);
    assert_eq!(
        field_value(&lines[0], "request_id").as_deref(),
        Some(request_id_text(1).as_str())
    );
}

// Through the orchestrator, a refused hand-off is logged once, with the reason the outcome names
// (the client logs it; the orchestrator does not log a second `handoff_not_accepted`).
#[tokio::test(start_paused = true)]
async fn a_refused_hand_off_through_the_orchestrator_logs_exactly_one_handoff_not_accepted() {
    let scripts: [(CallBehavior<AuditPluginError>, &str); 4] = [
        (
            CallBehavior::err(AuditPluginError::refused("no")),
            "refused",
        ),
        (
            CallBehavior::err(AuditPluginError::unavailable("down")),
            "unreachable",
        ),
        (CallBehavior::hang(), "timed_out"),
        (CallBehavior::panic(), "unreachable"),
    ];
    for (behavior, token) in scripts {
        let (logs, _guard) = capture_logs();
        let audit = FakeAuditPlugin::new();
        audit.on(AuditCall::RequestHandoff, behavior);
        let gate = files_gate().audit(audit).build();

        bounded(gate.seed(valid_body())).await;

        logs.assert_live();
        let lines = logs.lines_with("handoff_not_accepted");
        assert_eq!(lines.len(), 1, "{token}: {lines:?}");
        assert!(has_field(&lines[0], "reason", token), "{}", lines[0]);
    }
}

// §3.3 "Schema errors": a plugin that returns `Rejected` or `NotPermitted` from `schema` broke
// the contract; the log says so (cause `contract`) with the key and the request id.
#[tokio::test(start_paused = true)]
async fn a_schema_a_plugin_rejects_is_logged_as_unusable_with_the_cause_contract() {
    let (logs, _guard) = capture_logs();
    let gate = GateBuilder::new()
        .section("one", json!({}))
        .section("two", json!({}))
        .build();
    gate.sections[0].on(
        SectionCall::Schema,
        CallBehavior::err(SectionPluginError::rejected("no", None)),
    );
    gate.sections[1].on(
        SectionCall::Schema,
        CallBehavior::err(SectionPluginError::not_permitted("no")),
    );

    gate.seed(seed_body("app", "1", &json!({"one": {}, "two": {}})))
        .await;

    logs.assert_live();
    let lines = logs.lines_with("unusable_schema");
    assert_eq!(lines.len(), 2, "{lines:?}");
    for (line, key) in lines.iter().zip(["one", "two"]) {
        assert!(has_field(line, "key", key), "{line}");
        assert!(has_field(line, "cause", "contract"), "{line}");
        assert_eq!(
            field_value(line, "request_id").as_deref(),
            Some(request_id_text(1).as_str())
        );
    }
}

#[tokio::test(start_paused = true)]
async fn an_unusable_schema_log_carries_the_request_id() {
    let (logs, _guard) = capture_logs();
    let gate = GateBuilder::new()
        .section("bad", json!({"$ref": "https://example.com/x"}))
        .build();

    gate.seed(seed_body("app", "1", &json!({"bad": {}}))).await;

    logs.assert_live();
    let lines = logs.lines_with("unusable_schema");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(
        field_value(&lines[0], "request_id").as_deref(),
        Some(request_id_text(1).as_str())
    );
}
