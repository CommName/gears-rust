//! Unit tests for step 8, the final-result hand-off, through the full request (P7, DESIGN §3.2
//! step 8 and "A decided request ... gets the final-result hand-off once its request hand-off is
//! accepted", §3.1 "Audit event content" (`FinalResultContent`), §3.3 audit plugin API "Not
//! accepted: ... never gets a final-result hand-off" / "Final-result hand-off ... exactly one",
//! §4.5 row "Final-result hand-off not accepted", ADR 0001 no retry, PRD
//! `cpt-cf-application-manager-fr-audit-ordering`).
//!
//! Planned API in `crate::domain::audit_handoff`:
//!
//! ```text
//! impl AuditHandoffClient {
//!     /// One attempt, no retry. A failure is logged `handoff_not_accepted` (warn) with
//!     /// `kind="final_result"`, `reason` and `request_id`, never the content, and returned.
//!     pub async fn final_handoff(&self, caller: &CallerIdentity, audit_id: &str,
//!                                content: &FinalResultContent) -> Result<(), UnavailableReason>;
//! }
//! ```
//!
//! Pinned here, for the full request (`Gate::run`):
//! - every request whose request hand-off was accepted gets exactly one final-result hand-off,
//!   after everything else the request does, on every path: dispatched, decided before the
//!   hand-off (not permitted, not found, PDP or registry unavailable), invalid manifest, schema
//!   unavailable;
//! - a request whose request hand-off was not accepted gets none (and nothing is dispatched);
//! - its content: the audit id (equal to the argument), the request id (equal to the request
//!   hand-off's), the category and the registry effect of the report, `validation` only when the
//!   validation ran (`valid` true with no problems for a dispatched seed; `valid` false with
//!   the report's problems for an invalid manifest; omitted otherwise, and always for an
//!   uninstall), `sections` per plugin in priority order for a dispatched request (empty for a
//!   request decided before the schema phase: PDP, registry, not found, unaccepted hand-off; what
//!   an invalid manifest or an unavailable schema lists is left open), each with the fingerprint of the schema served for that
//!   request on a seed and none on an uninstall, and `completed_at` read when the request
//!   completes;
//! - a final-result hand-off that is not accepted changes nothing: the report is the one the
//!   accepted run gives, the failure is logged and not retried.
#![allow(
    clippy::duration_suboptimal_units,
    clippy::too_many_lines,
    clippy::type_complexity
)]

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use crate::domain::orchestrator::ValidationFn;
use application_manager_sdk::models::{
    FinalSection, OutcomeCategory, RegistryEffect, SectionOutcome, UnavailableReason,
    ValidationResult,
};
use application_manager_sdk::{AuditPluginError, SectionPluginError};
use serde_json::json;
use tokio::time::Instant;
use uuid::Uuid;

use crate::domain::fingerprint::schema_fingerprint;
use crate::domain::orchestrator::RequestInput;
use crate::domain::ports::{PdpUnavailable, PluginCallError, RegistryError};
use crate::test_support::{
    AuditCall, AuthzScript, CallBehavior, Gate, GateBuilder, ManualClock, PLUGIN_TIMEOUT,
    PolicyScript, RegistryOp, ScriptedAuditSink, SectionCall, ValidationLatch, audit_kinds,
    bounded, capture_logs, constraint_on, db_schema, field_value, files_schema, final_content,
    has_field, has_final, net_schema, policy_adapter, request_content, seed_body, stored,
    three_section_body, three_section_gate,
};

use SectionOutcome as S;

fn seed(body: Vec<u8>) -> RequestInput {
    RequestInput::Seed { body }
}

fn uninstall(id: &str) -> RequestInput {
    RequestInput::Uninstall {
        application_id: id.to_owned(),
    }
}

struct Scenario {
    name: &'static str,
    gate: Gate,
    input: RequestInput,
    category: OutcomeCategory,
    /// Whether the request dispatches (calls `apply` or `remove`, writes or deletes).
    dispatches: bool,
    /// `Some(valid)` when the validation ran.
    validation: Option<bool>,
    /// Whether the request got as far as the schema phase (the schemas fetched or the
    /// validation started): what the final content then lists is not pinned (DESIGN gap).
    schema_phase: bool,
}

fn scenarios() -> Vec<Scenario> {
    let deny_apply = || CallBehavior::err(SectionPluginError::not_permitted("needs more"));
    let (constrained, _authz) = policy_adapter(AuthzScript::Permit(vec![constraint_on(
        "owner_tenant_id",
        Uuid::from_u128(0x7E),
    )]));
    let schema_down = || {
        let gate = three_section_gate().build();
        gate.sections[1].on(
            SectionCall::Schema,
            CallBehavior::err(SectionPluginError::internal("down")),
        );
        gate
    };
    let schema_unusable = || {
        let gate = three_section_gate().build();
        gate.sections[2].on(
            SectionCall::Schema,
            CallBehavior::err(SectionPluginError::rejected("what", None)),
        );
        gate
    };
    let upsert_down = || {
        let gate = three_section_gate().build();
        gate.memory
            .fail(RegistryOp::Upsert, RegistryError::Unavailable);
        gate
    };
    let delete_down = || {
        let gate = three_section_gate().entry(stored("app-1", "3.1")).build();
        gate.memory
            .fail(RegistryOp::Delete, RegistryError::TimedOut);
        gate
    };
    let one_rejects = || {
        let gate = three_section_gate().build();
        gate.sections[1].on(
            SectionCall::Apply,
            CallBehavior::err(SectionPluginError::rejected("no", None)),
        );
        gate
    };
    let one_denies = || {
        let gate = three_section_gate().build();
        gate.sections[0].on(SectionCall::Apply, deny_apply());
        gate
    };
    let one_fails = || {
        let gate = three_section_gate().build();
        gate.sections[2].on(
            SectionCall::Apply,
            CallBehavior::err(SectionPluginError::internal("boom")),
        );
        gate
    };
    let registry_down = || {
        let gate = three_section_gate().build();
        gate.memory
            .fail(RegistryOp::Get, RegistryError::Unavailable);
        gate
    };
    vec![
        Scenario {
            name: "seed succeeded",
            gate: three_section_gate().build(),
            input: seed(three_section_body()),
            category: OutcomeCategory::Succeeded,
            dispatches: true,
            validation: Some(true),
            schema_phase: false,
        },
        Scenario {
            name: "seed with a rejecting section",
            gate: one_rejects(),
            input: seed(three_section_body()),
            category: OutcomeCategory::PluginRejected,
            dispatches: true,
            validation: Some(true),
            schema_phase: false,
        },
        Scenario {
            name: "seed with a section refusing the caller",
            gate: one_denies(),
            input: seed(three_section_body()),
            category: OutcomeCategory::NotPermitted,
            dispatches: true,
            validation: Some(true),
            schema_phase: false,
        },
        Scenario {
            name: "seed with a failing section",
            gate: one_fails(),
            input: seed(three_section_body()),
            category: OutcomeCategory::InternalFailure,
            dispatches: true,
            validation: Some(true),
            schema_phase: false,
        },
        Scenario {
            name: "seed whose upsert fails",
            gate: upsert_down(),
            input: seed(three_section_body()),
            category: OutcomeCategory::InternalFailure,
            dispatches: true,
            validation: Some(true),
            schema_phase: false,
        },
        Scenario {
            name: "seed refused by the PDP",
            gate: three_section_gate().policy(PolicyScript::Refuse).build(),
            input: seed(three_section_body()),
            category: OutcomeCategory::NotPermitted,
            dispatches: false,
            validation: None,
            schema_phase: false,
        },
        Scenario {
            name: "seed with a PDP that cannot answer",
            gate: three_section_gate()
                .policy(PolicyScript::Unavailable(PdpUnavailable::Unreachable))
                .build(),
            input: seed(three_section_body()),
            category: OutcomeCategory::TemporarilyUnavailable,
            dispatches: false,
            validation: None,
            schema_phase: false,
        },
        Scenario {
            name: "seed permitted with a constraint",
            gate: three_section_gate().policy_port(constrained).build(),
            input: seed(three_section_body()),
            category: OutcomeCategory::NotPermitted,
            dispatches: false,
            validation: None,
            schema_phase: false,
        },
        Scenario {
            name: "seed with the registry down",
            gate: registry_down(),
            input: seed(three_section_body()),
            category: OutcomeCategory::TemporarilyUnavailable,
            dispatches: false,
            validation: None,
            schema_phase: false,
        },
        Scenario {
            name: "seed with a section schema down",
            gate: schema_down(),
            input: seed(three_section_body()),
            category: OutcomeCategory::TemporarilyUnavailable,
            dispatches: false,
            validation: None,
            schema_phase: true,
        },
        Scenario {
            name: "seed with an unusable section schema",
            gate: schema_unusable(),
            input: seed(three_section_body()),
            category: OutcomeCategory::TemporarilyUnavailable,
            dispatches: false,
            validation: None,
            schema_phase: true,
        },
        Scenario {
            name: "seed with an invalid manifest",
            gate: three_section_gate().build(),
            input: seed(br#"{"not": "a manifest"}"#.to_vec()),
            category: OutcomeCategory::InvalidManifest,
            dispatches: false,
            validation: Some(false),
            schema_phase: true,
        },
        Scenario {
            name: "seed with a body that does not parse",
            gate: three_section_gate().build(),
            input: seed(b"{".to_vec()),
            category: OutcomeCategory::InvalidManifest,
            dispatches: false,
            validation: Some(false),
            schema_phase: true,
        },
        Scenario {
            name: "uninstall succeeded",
            gate: three_section_gate().entry(stored("app-1", "3.1")).build(),
            input: uninstall("app-1"),
            category: OutcomeCategory::Succeeded,
            dispatches: true,
            validation: None,
            schema_phase: false,
        },
        Scenario {
            name: "uninstall whose delete fails",
            gate: delete_down(),
            input: uninstall("app-1"),
            category: OutcomeCategory::InternalFailure,
            dispatches: true,
            validation: None,
            schema_phase: false,
        },
        Scenario {
            name: "uninstall of an id with no entry",
            gate: three_section_gate().build(),
            input: uninstall("app-1"),
            category: OutcomeCategory::NotFound,
            dispatches: false,
            validation: None,
            schema_phase: false,
        },
        Scenario {
            name: "uninstall of an id that can never be stored",
            gate: three_section_gate().build(),
            input: uninstall("bad\nid"),
            category: OutcomeCategory::NotFound,
            dispatches: false,
            validation: None,
            schema_phase: false,
        },
        Scenario {
            name: "uninstall refused by the PDP",
            gate: three_section_gate()
                .entry(stored("app-1", "3.1"))
                .policy(PolicyScript::Refuse)
                .build(),
            input: uninstall("app-1"),
            category: OutcomeCategory::NotPermitted,
            dispatches: false,
            validation: None,
            schema_phase: false,
        },
    ]
}

// PRD audit ordering: exactly one final-result hand-off for every request whose request
// hand-off was accepted, after everything else, with the audit id and the request id of the
// request and the category and registry effect of the report.
#[tokio::test(start_paused = true)]
async fn every_accepted_request_gets_exactly_one_final_result_hand_off_after_all_else() {
    for sc in scenarios() {
        let report = bounded(sc.gate.run(sc.input.clone())).await;

        assert_eq!(report.category, sc.category, "{}", sc.name);
        assert_eq!(
            audit_kinds(&sc.gate),
            [AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
            "{}: one request hand-off, then one final-result hand-off",
            sc.name
        );
        let events = sc.gate.events.events();
        assert_eq!(
            events.last().map(String::as_str),
            Some("audit:final_result_handoff"),
            "{}: the final result is the last thing: {events:?}",
            sc.name
        );
        let request = request_content(&sc.gate);
        let (audit_id, content) = final_content(&sc.gate);
        assert_eq!(audit_id, "audit-1", "{}", sc.name);
        assert_eq!(report.audit_id.as_deref(), Some("audit-1"), "{}", sc.name);
        assert_eq!(content.audit_id, audit_id, "{}", sc.name);
        assert_eq!(content.request_id, request.request_id, "{}", sc.name);
        assert_eq!(content.category, report.category, "{}", sc.name);
        assert_eq!(
            content.registry_effect, report.registry_effect,
            "{}",
            sc.name
        );
        let calls = sc.gate.audit.calls();
        assert_eq!(
            calls[1].caller.request_id, request.request_id,
            "{}",
            sc.name
        );
        assert_eq!(calls[1].caller, calls[0].caller, "{}: one caller", sc.name);
        let dispatched = events.iter().any(|e| {
            e.ends_with(":apply")
                || e.ends_with(":remove")
                || e.starts_with("registry:upsert")
                || e.starts_with("registry:delete")
        });
        assert_eq!(dispatched, sc.dispatches, "{}: {events:?}", sc.name);
        assert_eq!(
            content.validation.as_ref().map(|v| v.valid),
            sc.validation,
            "{}",
            sc.name
        );
        if !sc.dispatches {
            if !sc.schema_phase {
                assert!(content.sections.is_empty(), "{}", sc.name);
            }
            assert_eq!(
                report.registry_effect,
                RegistryEffect::Unchanged,
                "{}",
                sc.name
            );
        }
    }
}

// DESIGN §3.1: a dispatched seed's final content lists each plugin in priority order with its
// outcome and the fingerprint of the schema served for THIS request; the validation is valid.
#[tokio::test(start_paused = true)]
async fn the_final_result_of_a_dispatched_seed_carries_outcomes_and_schema_fingerprints() {
    let gate = three_section_gate().build();
    gate.sections[1].on(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::rejected("no", None)),
    );

    let report = gate.run_seed(three_section_body()).await;

    let (_, content) = final_content(&gate);
    assert_eq!(
        content.sections,
        vec![
            FinalSection {
                key: "files".to_owned(),
                outcome: S::Succeeded,
                schema_fingerprint: Some(schema_fingerprint(&files_schema())),
            },
            FinalSection {
                key: "db".to_owned(),
                outcome: S::Rejected,
                schema_fingerprint: Some(schema_fingerprint(&db_schema())),
            },
            FinalSection {
                key: "net".to_owned(),
                outcome: S::Succeeded,
                schema_fingerprint: Some(schema_fingerprint(&net_schema())),
            },
        ]
    );
    assert_eq!(
        content.validation,
        Some(ValidationResult {
            valid: true,
            problems: Vec::new(),
        })
    );
    assert_eq!(content.category, OutcomeCategory::PluginRejected);
    assert_eq!(report.category, OutcomeCategory::PluginRejected);
}

// The fingerprint is of the schema "exactly as served for that request": a plugin that changes
// its schema gives a new fingerprint on the next request.
#[tokio::test(start_paused = true)]
async fn the_fingerprint_follows_the_schema_served_for_that_request() {
    let gate = three_section_gate().build();
    gate.run_seed(three_section_body()).await;
    let (_, first) = final_content(&gate);
    let changed = json!({
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "description": "second edition",
    });
    gate.sections[0].set_schema(changed.clone());

    gate.run_seed(three_section_body()).await;

    let seconds: Vec<_> = gate
        .audit
        .calls()
        .into_iter()
        .filter_map(|c| c.result)
        .collect();
    assert_eq!(seconds.len(), 2);
    assert_eq!(
        first.sections[0].schema_fingerprint,
        Some(schema_fingerprint(&files_schema()))
    );
    assert_eq!(
        seconds[1].sections[0].schema_fingerprint,
        Some(schema_fingerprint(&changed))
    );
    assert_ne!(
        seconds[1].sections[0].schema_fingerprint,
        first.sections[0].schema_fingerprint
    );
    assert_eq!(
        seconds[1].sections[1].schema_fingerprint, first.sections[1].schema_fingerprint,
        "an unchanged schema keeps its fingerprint"
    );
}

// DESIGN §3.1: `schema_fingerprint` is omitted when no schema was fetched for the section, as on
// uninstall; `validation` is omitted when the validation did not run.
#[tokio::test(start_paused = true)]
async fn the_final_result_of_an_uninstall_has_no_fingerprint_and_no_validation() {
    let gate = three_section_gate().entry(stored("app-1", "3.1")).build();
    gate.sections[1].on(
        SectionCall::Remove,
        CallBehavior::err(SectionPluginError::internal("boom")),
    );

    gate.run_uninstall("app-1").await;

    let (_, content) = final_content(&gate);
    assert_eq!(
        content.sections,
        vec![
            FinalSection {
                key: "files".to_owned(),
                outcome: S::Succeeded,
                schema_fingerprint: None,
            },
            FinalSection {
                key: "db".to_owned(),
                outcome: S::InternalFailure,
                schema_fingerprint: None,
            },
            FinalSection {
                key: "net".to_owned(),
                outcome: S::Succeeded,
                schema_fingerprint: None,
            },
        ]
    );
    assert_eq!(content.validation, None);
    assert_eq!(content.registry_effect, RegistryEffect::Deleted);
    let wire = serde_json::to_value(&content).expect("serializes");
    assert!(
        wire.get("validation").is_none(),
        "omitted, never null: {wire}"
    );
}

// DESIGN §3.1: an invalid manifest's final result carries the validation with the report's
// problems.
#[tokio::test(start_paused = true)]
async fn the_final_result_of_an_invalid_manifest_carries_every_problem() {
    let gate = three_section_gate().build();
    let body = seed_body(
        "app-1",
        "1.0",
        &json!({"files": {"path": 1}, "db": {}, "net": {}}),
    );

    let report = gate.run_seed(body).await;

    assert_eq!(report.category, OutcomeCategory::InvalidManifest);
    assert!(!report.problems.is_empty());
    let (_, content) = final_content(&gate);
    assert_eq!(
        content.validation,
        Some(ValidationResult {
            valid: false,
            problems: report.problems,
        })
    );
    // The sections of an invalid manifest's final content are not pinned (DESIGN gap).
}

// DESIGN §3.1: `completed_at` is read when the request completes (RFC 3339 text), the receive
// time is read when it is received.
#[tokio::test(start_paused = true)]
async fn completed_at_is_read_at_completion_and_received_at_at_receipt() {
    let clock = Arc::new(ManualClock::new(
        UNIX_EPOCH + Duration::from_secs(1_800_000_000),
    ));
    let gate = three_section_gate().clock(Arc::clone(&clock) as _).build();
    gate.sections[0].on(
        SectionCall::Apply,
        CallBehavior::ok().with_delay(Duration::from_millis(400)),
    );
    let mover = {
        let clock = Arc::clone(&clock);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            clock.advance(Duration::from_secs(3600));
        })
    };

    bounded(gate.run_seed(three_section_body())).await;
    mover.await.expect("the mover ends");

    assert_eq!(request_content(&gate).received_at, "2027-01-15T08:00:00Z");
    assert_eq!(final_content(&gate).1.completed_at, "2027-01-15T09:00:00Z");
}

// DESIGN §3.1 / §3.3: one request id on every call of the request, and none of them carries the
// bearer token.
#[tokio::test(start_paused = true)]
async fn one_request_id_and_no_bearer_token_on_every_call_of_the_request() {
    let gate = three_section_gate().build();

    gate.run_seed(three_section_body()).await;
    gate.run_seed(three_section_body()).await;

    let audit = gate.audit.calls();
    assert_eq!(audit.len(), 4);
    let first = audit[0].caller.request_id;
    let second = audit[2].caller.request_id;
    assert_ne!(first, second, "each request has its own id");
    assert_eq!(audit[1].caller.request_id, first);
    assert_eq!(audit[3].caller.request_id, second);
    for plugin in &gate.sections {
        let ids: Vec<Uuid> = plugin
            .calls()
            .iter()
            .filter_map(|c| c.caller.as_ref().map(|c| c.request_id))
            .collect();
        assert_eq!(ids.len(), 4, "a schema and an apply per request");
        assert_eq!(ids.iter().filter(|id| **id == first).count(), 2);
        assert_eq!(ids.iter().filter(|id| **id == second).count(), 2);
    }
    assert!(!format!("{audit:?}").contains("pdp-bearer-token"));
}

// ---- a request hand-off that was not accepted gets no final-result hand-off ----------------------

// DESIGN §3.3 "Not accepted: A request hand-off that is not accepted never gets a final-result
// hand-off", PRD audit ordering: nothing is dispatched, no audit id.
#[tokio::test(start_paused = true)]
async fn a_request_hand_off_that_is_not_accepted_gets_no_final_result_hand_off() {
    type B = CallBehavior<AuditPluginError>;
    let cases: [(&str, B, UnavailableReason); 4] = [
        (
            "refused",
            B::err(AuditPluginError::refused("no")),
            UnavailableReason::Refused,
        ),
        (
            "unavailable",
            B::err(AuditPluginError::unavailable("down")),
            UnavailableReason::Unreachable,
        ),
        ("never answers", B::hang(), UnavailableReason::TimedOut),
        ("panics", B::panic(), UnavailableReason::Unreachable),
    ];
    for (name, behavior, reason) in cases {
        for input in [seed(three_section_body()), uninstall("app-1")] {
            let audit = crate::test_support::FakeAuditPlugin::new();
            audit.on(AuditCall::RequestHandoff, behavior.clone());
            let gate = three_section_gate()
                .entry(stored("app-1", "3.1"))
                .audit(audit)
                .build();

            let report = bounded(gate.run(input.clone())).await;

            assert_eq!(
                report.category,
                OutcomeCategory::TemporarilyUnavailable,
                "{name}"
            );
            assert_eq!(report.audit_id, None, "{name}");
            assert_eq!(
                report.unavailable.len(),
                1,
                "{name}: {:?}",
                report.unavailable
            );
            assert_eq!(report.unavailable[0].reason, reason, "{name}");
            assert_eq!(
                audit_kinds(&gate),
                [AuditCall::RequestHandoff],
                "{name}: one attempt and no final-result hand-off"
            );
            assert_eq!(report.registry_effect, RegistryEffect::Unchanged, "{name}");
            let entries = gate.memory.entries();
            assert_eq!(entries.len(), 1, "{name}: the stored entry is untouched");
            assert_eq!(entries[0].version(), "3.1", "{name}: and keeps its version");
            let events = gate.events.events();
            assert!(
                events.iter().all(|e| !e.ends_with(":apply")
                    && !e.ends_with(":remove")
                    && !e.ends_with(":schema")
                    && !e.starts_with("registry:upsert")
                    && !e.starts_with("registry:delete")),
                "{name}: nothing is dispatched: {events:?}"
            );
        }
    }
}

// The category already decided survives a hand-off that is not accepted (PRD audit ordering),
// and there is still no final-result hand-off.
#[tokio::test(start_paused = true)]
async fn a_decided_category_without_an_accepted_hand_off_gets_no_final_result() {
    let audit = crate::test_support::FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("no")),
    );
    let gate = three_section_gate()
        .policy(PolicyScript::Refuse)
        .audit(audit)
        .build();

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(report.category, OutcomeCategory::NotPermitted);
    assert!(!has_final(&gate));
}

// ---- a final-result hand-off that is not accepted changes nothing ---------------------------------

// DESIGN §3.3 "Final-result hand-off ... Not accepted: the response is unchanged, and the failure
// is logged and counted", §4.5: one attempt; the entry is written anyway.
#[tokio::test(start_paused = true)]
async fn a_final_result_hand_off_that_is_not_accepted_leaves_the_result_unchanged() {
    type B = CallBehavior<AuditPluginError>;
    let reference = {
        let gate = three_section_gate().build();
        gate.run_seed(three_section_body()).await
    };
    let cases: [(&str, B, &str); 4] = [
        (
            "refused",
            B::err(AuditPluginError::refused("no")),
            "refused",
        ),
        (
            "unavailable",
            B::err(AuditPluginError::unavailable("down")),
            "unreachable",
        ),
        ("never answers", B::hang(), "timed_out"),
        ("panics", B::panic(), "unreachable"),
    ];
    for (name, behavior, reason) in cases {
        let (log, _guard) = capture_logs();
        let audit = crate::test_support::FakeAuditPlugin::new();
        audit.on(AuditCall::FinalResultHandoff, behavior);
        let gate = three_section_gate().audit(audit).build();
        let started = Instant::now();

        let report = bounded(gate.run_seed(three_section_body())).await;

        assert_eq!(report, reference, "{name}: the response is unchanged");
        assert_eq!(
            audit_kinds(&gate),
            [AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
            "{name}: one attempt, no retry"
        );
        assert_eq!(
            gate.memory.entries().len(),
            1,
            "{name}: the entry is written"
        );
        let lines = log.lines_with("handoff_not_accepted");
        assert_eq!(lines.len(), 1, "{name}: {lines:?}");
        assert!(
            has_field(&lines[0], "kind", "final_result"),
            "{name}: {}",
            lines[0]
        );
        assert!(
            has_field(&lines[0], "reason", reason),
            "{name}: {}",
            lines[0]
        );
        assert_eq!(
            field_value(&lines[0], "request_id"),
            Some(request_content(&gate).request_id.to_string()),
            "{name}: {}",
            lines[0]
        );
        if name == "never answers" {
            assert_eq!(started.elapsed(), PLUGIN_TIMEOUT, "{name}");
        } else {
            assert_eq!(started.elapsed(), Duration::ZERO, "{name}");
        }
    }
}

// The same for an uninstall and for a request that ended before dispatch.
#[tokio::test(start_paused = true)]
async fn a_failed_final_result_hand_off_changes_no_other_kind_of_result_either() {
    let inputs = [
        ("uninstall", uninstall("app-1"), OutcomeCategory::Succeeded),
        ("not found", uninstall("other"), OutcomeCategory::NotFound),
        (
            "invalid",
            seed(b"{".to_vec()),
            OutcomeCategory::InvalidManifest,
        ),
    ];
    for (name, input, category) in inputs {
        let reference = three_section_gate()
            .entry(stored("app-1", "3.1"))
            .build()
            .run(input.clone())
            .await;
        let audit = crate::test_support::FakeAuditPlugin::new();
        audit.on(
            AuditCall::FinalResultHandoff,
            CallBehavior::err(AuditPluginError::unavailable("down")),
        );
        let gate = three_section_gate()
            .entry(stored("app-1", "3.1"))
            .audit(audit)
            .build();

        let report = gate.run(input).await;

        assert_eq!(report.category, category, "{name}");
        assert_eq!(report, reference, "{name}");
        assert_eq!(audit_kinds(&gate).len(), 2, "{name}: one attempt");
    }
}

// DESIGN §4.5 and §3.3: a hung final-result hand-off costs one plugin limit at the end of the
// request and nothing else; a seed whose plugins all hang costs their limits one after another.
#[tokio::test(start_paused = true)]
async fn the_time_of_a_request_is_the_sum_of_the_limits_it_hits() {
    let gate = three_section_gate().build();
    gate.sections[0].on(SectionCall::Apply, CallBehavior::hang());
    gate.sections[1].on(SectionCall::Apply, CallBehavior::hang());
    gate.audit
        .on(AuditCall::FinalResultHandoff, CallBehavior::hang());
    let started = Instant::now();

    let report = bounded(gate.run_seed(three_section_body())).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(started.elapsed(), PLUGIN_TIMEOUT * 3);
}

// DESIGN §3.3: the audit plugin's in-flight cap counts the final-result hand-off that timed out
// and is still running; the next request's request hand-off is then `not_sent`: not accepted, no
// audit id, nothing dispatched, no final-result hand-off, and the plugin never saw the call.
#[tokio::test(start_paused = true)]
async fn a_full_audit_cap_gives_not_sent_for_the_next_request_hand_off() {
    let gate = three_section_gate().guard_cap(1).build();
    gate.audit
        .on(AuditCall::FinalResultHandoff, CallBehavior::hang());

    let first = bounded(gate.run_seed(three_section_body())).await;
    let second = bounded(gate.run_seed(three_section_body())).await;

    assert_eq!(
        first.category,
        OutcomeCategory::Succeeded,
        "the first result is unchanged"
    );
    assert_eq!(second.category, OutcomeCategory::TemporarilyUnavailable);
    assert_eq!(second.audit_id, None);
    assert_eq!(second.unavailable.len(), 1);
    assert_eq!(second.unavailable[0].reason, UnavailableReason::NotSent);
    assert_eq!(
        gate.audit.call_count(),
        2,
        "the second request never reached the plugin"
    );
    assert_eq!(
        gate.sections[0]
            .calls()
            .iter()
            .filter(|c| c.kind == SectionCall::Apply)
            .count(),
        1,
        "and dispatched nothing"
    );
}

// DESIGN §3.3 "Final-result hand-off ... Not accepted": a final-result hand-off the guard did not
// send (the audit plugin's in-flight cap) is `not_sent`: logged with that reason and the request
// id, the response unchanged, nothing retried. (The guard gives `not_sent` only for a full cap,
// so the answer is scripted outside the guard.)
#[tokio::test(start_paused = true)]
async fn a_final_result_hand_off_that_was_not_sent_is_logged_as_not_sent_and_changes_nothing() {
    let reference = three_section_gate()
        .build()
        .run_seed(three_section_body())
        .await;
    let (log, _guard) = capture_logs();
    let gate = three_section_gate()
        .wrap_audit_sink(|sink| {
            Arc::new(ScriptedAuditSink::new(
                sink,
                AuditCall::FinalResultHandoff,
                PluginCallError::NotSent,
            ))
        })
        .build();

    let report = bounded(gate.run_seed(three_section_body())).await;

    assert_eq!(report, reference);
    let lines = log.lines_with("handoff_not_accepted");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "kind", "final_result"), "{}", lines[0]);
    assert!(has_field(&lines[0], "reason", "not_sent"), "{}", lines[0]);
    assert_eq!(
        field_value(&lines[0], "request_id"),
        Some(request_content(&gate).request_id.to_string()),
        "{}",
        lines[0]
    );
}

// ---- a validation that does not finish -------------------------------------------------------------

// DESIGN §3.2 step 8: a request whose hand-off was accepted gets its final result also when the
// validation does not finish (step timeout, at capacity, a panic): the category is temporarily
// unavailable with nothing to name, the audit id is kept, the validation did not run so the
// content carries none, and nothing was dispatched.
#[tokio::test]
async fn a_validation_that_does_not_finish_still_gets_the_final_result_hand_off() {
    let latch = ValidationLatch::new();
    let one_section = || GateBuilder::new().section("files", json!({"type": "object"}));
    let panicking: Arc<ValidationFn> = Arc::new(|_, _| panic!("validation boom"));
    let gates = [
        (
            "step timeout",
            one_section()
                .validation(latch.validation())
                .settings(|s| s.step_timeout = Duration::from_millis(50))
                .build(),
        ),
        (
            "at capacity",
            one_section().settings(|s| s.max_validations = 0).build(),
        ),
        ("panic", one_section().validation(panicking).build()),
    ];
    let body = seed_body("app-1", "1.0", &json!({"files": {}}));

    for (name, gate) in gates {
        let report = gate.run(seed(body.clone())).await;

        assert_eq!(
            report.category,
            OutcomeCategory::TemporarilyUnavailable,
            "{name}"
        );
        assert!(
            report.unavailable.is_empty(),
            "{name}: {:?}",
            report.unavailable
        );
        assert_eq!(report.audit_id.as_deref(), Some("audit-1"), "{name}");
        assert_eq!(
            audit_kinds(&gate),
            [AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
            "{name}"
        );
        let (audit_id, content) = final_content(&gate);
        assert_eq!(audit_id, "audit-1", "{name}");
        assert_eq!(
            content.validation, None,
            "{name}: the validation did not finish"
        );
        assert_eq!(
            content.category,
            OutcomeCategory::TemporarilyUnavailable,
            "{name}"
        );
        assert_eq!(content.registry_effect, RegistryEffect::Unchanged, "{name}");
        let events = gate.events.events();
        assert!(
            events
                .iter()
                .all(|e| !e.ends_with(":apply") && !e.starts_with("registry:upsert")),
            "{name}: nothing is dispatched: {events:?}"
        );
    }
    latch.release();
    latch.wait_finished(1).await;
}

// DESIGN §3.3 "Not accepted": a decision of `not_found` or of a temporarily unavailable
// dependency (the PDP, the registry) with a refusing audit plugin keeps its category, gets no
// audit id and no final-result hand-off.
#[tokio::test(start_paused = true)]
async fn not_found_and_unavailable_decisions_without_an_accepted_hand_off_get_no_final_result() {
    let refusing = || {
        let audit = crate::test_support::FakeAuditPlugin::new();
        audit.on(
            AuditCall::RequestHandoff,
            CallBehavior::err(AuditPluginError::refused("no")),
        );
        audit
    };
    let registry_down = {
        let gate = three_section_gate().audit(refusing()).build();
        gate.memory
            .fail(RegistryOp::Get, RegistryError::Unavailable);
        gate
    };
    let cases = [
        (
            "uninstall of an id with no entry",
            three_section_gate().audit(refusing()).build(),
            uninstall("app-1"),
            OutcomeCategory::NotFound,
        ),
        (
            "the PDP cannot answer",
            three_section_gate()
                .policy(PolicyScript::Unavailable(PdpUnavailable::Unreachable))
                .audit(refusing())
                .build(),
            seed(three_section_body()),
            OutcomeCategory::TemporarilyUnavailable,
        ),
        (
            "the registry is down",
            registry_down,
            seed(three_section_body()),
            OutcomeCategory::TemporarilyUnavailable,
        ),
    ];

    for (name, gate, input, category) in cases {
        let report = bounded(gate.run(input)).await;

        assert_eq!(report.category, category, "{name}");
        assert_eq!(report.audit_id, None, "{name}");
        assert!(!has_final(&gate), "{name}");
        assert_eq!(audit_kinds(&gate), [AuditCall::RequestHandoff], "{name}");
    }
}
