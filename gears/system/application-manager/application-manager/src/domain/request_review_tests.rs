//! Unit tests for the fixes of the P7 implementation review: the permit that tracks admission
//! until the supervisor ends (and the drain that waits for it), the contained recovery that never
//! parses the input again, the schema fingerprints kept for a recovered final result, the
//! operation the input decides, the audit id that cannot be stored, the bounded plugin detail,
//! `dispatch_started`, and the cause and the span of `request_task_failed` (DESIGN §3.2 "Request
//! task ownership", "Request progress record" and "On a panic or join error", §4.5, §4.6).
#![allow(clippy::duration_suboptimal_units, clippy::too_many_lines)]

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use application_manager_sdk::models::{
    Dependency, HandoffOperation, OutcomeCategory, RegistryEffect, SectionOutcome,
    UnavailableEntry, UnavailableReason,
};
use application_manager_sdk::{AuditPluginError, SectionPluginError};
use tokio_util::sync::CancellationToken;

use crate::domain::fingerprint::schema_fingerprint;
use crate::domain::orchestrator::{PARSE_PANIC_MARKER, RequestInput};
use crate::test_support::{
    AuditCall, CallBehavior, FakeAuditPlugin, GateBuilder, ManualClock, PanickingClock,
    PanickingSectionPort, PolicyScript, RegistryOp, SectionCall, audit_kinds, bounded,
    capture_logs, db_schema, field_value, files_schema, final_content, has_field, keys, net_schema,
    outcomes, security_context, stored, three_section_body, three_section_gate,
};

fn seed() -> RequestInput {
    RequestInput::Seed {
        body: three_section_body(),
    }
}

fn uninstall() -> RequestInput {
    RequestInput::Uninstall {
        application_id: "app-1".to_owned(),
    }
}

// One request slot, a stored entry for the uninstalls, and a slow permission decision that keeps
// a request in flight for 100ms.
fn gate(cap: usize) -> GateBuilder {
    three_section_gate()
        .entry(stored("app-1", "3.1"))
        .settings(move |s| s.max_request_tasks = cap)
        .policy_delay(Duration::from_millis(100))
}

fn never_cancelled() -> CancellationToken {
    CancellationToken::new()
}

// ---- F-001: the permit tracks admission until the supervisor ends ----------------------------

// A permit that was taken and not yet used counts for the drain: the REST handler may be between
// `try_admit` and `start_request` when the stop begins.
#[tokio::test(start_paused = true)]
async fn drain_waits_for_a_permit_that_was_taken_and_not_yet_used() {
    let gate = gate(2).build();
    let permit = gate
        .orchestrator
        .try_admit(HandoffOperation::Seed)
        .expect("a free permit");
    let token = never_cancelled();

    let waiting =
        tokio::time::timeout(Duration::from_secs(5), gate.orchestrator.drain(&token)).await;
    assert!(waiting.is_err(), "drain still waits for the permit");

    drop(permit);
    assert_eq!(bounded(gate.orchestrator.drain(&token)).await, 0);
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_deadline_reports_a_permit_still_held_as_abandoned() {
    let (log, _guard) = capture_logs();
    let gate = gate(2).build();
    let permit = gate
        .orchestrator
        .try_admit(HandoffOperation::Seed)
        .expect("a free permit");
    let token = CancellationToken::new();
    let canceller = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        canceller.cancel();
    });

    let abandoned = bounded(gate.orchestrator.drain(&token)).await;

    assert_eq!(abandoned, 1, "the held permit is abandoned");
    assert_eq!(log.lines_with("request_tasks_abandoned").len(), 0);
    let lines = log.lines_with("request_permits_abandoned");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "count", "1"), "{}", lines[0]);
    drop(permit);
}

// The token of the permit moves into the supervisor: a request admitted before the drain began
// runs to its end, and the drain returns only after it.
#[tokio::test(start_paused = true)]
async fn a_request_admitted_before_the_drain_runs_to_its_end_and_the_drain_waits_for_it() {
    let gate = gate(2).build();
    let permit = gate
        .orchestrator
        .try_admit(HandoffOperation::Seed)
        .expect("a free permit");
    let token = never_cancelled();
    let drain = gate.orchestrator.drain(&token);
    tokio::pin!(drain);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut drain)
            .await
            .is_err(),
        "drain waits for the permit"
    );

    let handle = gate
        .orchestrator
        .start_request(permit, security_context(), seed());
    let (abandoned, report) = bounded(async { tokio::join!(&mut drain, handle) }).await;

    assert_eq!(abandoned, 0);
    assert_eq!(
        report.expect("joins").category,
        OutcomeCategory::Succeeded,
        "the request admitted before the drain ran completely"
    );
    assert_eq!(audit_kinds(&gate).len(), 2);
    assert!(gate.orchestrator.try_admit(HandoffOperation::Seed).is_err());
}

// ---- F-006: dropping the handle cuts nothing short and frees nothing -------------------------

#[tokio::test(start_paused = true)]
async fn a_dropped_handle_keeps_the_drain_waiting_until_the_request_ends() {
    let gate = gate(1).build();
    drop(gate.start(seed()));
    tokio::time::sleep(Duration::from_millis(10)).await;
    let token = never_cancelled();

    let waiting =
        tokio::time::timeout(Duration::from_millis(20), gate.orchestrator.drain(&token)).await;
    assert!(waiting.is_err(), "the request still runs");
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 1);

    assert_eq!(bounded(gate.orchestrator.drain(&token)).await, 0);
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
    assert_eq!(audit_kinds(&gate).len(), 2, "the request completed");
}

// ---- F-007: the input decides the operation --------------------------------------------------

// Replaces the debug_assert `should_panic` test: a permit of the other operation is no longer a
// debug-only defect with a release behaviour that follows the permit; the input decides the
// operation everywhere (gauge, abandoned log, recovered effect), and the permit's operation is
// only what the refusal log used.
#[tokio::test(start_paused = true)]
async fn the_input_decides_the_operation_not_the_permit() {
    let (log, _guard) = capture_logs();
    let gate = gate(2).build();
    let permit = gate
        .orchestrator
        .try_admit(HandoffOperation::Seed)
        .expect("a free permit");

    let handle = gate
        .orchestrator
        .start_request(permit, security_context(), uninstall());
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 1);
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(gate.orchestrator.drain(&token).await, 1);

    let lines = log.lines_with("request_tasks_abandoned");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "operation", "uninstall"),
        "{}",
        lines[0]
    );
    let report = bounded(handle).await.expect("joins");
    assert_eq!(report.category, OutcomeCategory::Succeeded);
    assert_eq!(report.registry_effect, RegistryEffect::Deleted);
}

#[tokio::test(start_paused = true)]
async fn a_recovered_effect_follows_the_input_not_the_permit() {
    let gate = three_section_gate().entry(stored("app-1", "3.1")).build();
    gate.memory.panic_on(RegistryOp::Delete);
    let permit = gate
        .orchestrator
        .try_admit(HandoffOperation::Seed)
        .expect("a free permit");

    let report = bounded(
        gate.orchestrator
            .start_request(permit, security_context(), uninstall()),
    )
    .await
    .expect("joins");

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(
        report.registry_effect,
        RegistryEffect::DeleteFailed,
        "an uninstall that panicked in the delete, whatever the permit says"
    );
}

// ---- F-002: contained recovery that never parses ---------------------------------------------

// A panic in the parse of step 1: the supervisor must not parse the same input again (it would
// panic again, outside any guard). The request ends as internal failure and nothing could be
// built for a hand-off.
#[tokio::test(start_paused = true)]
async fn recovery_does_not_parse_the_input_again() {
    let gate = three_section_gate()
        .settings(|s| s.max_request_tasks = 1)
        .build();

    let report = bounded(gate.run(RequestInput::Seed {
        body: PARSE_PANIC_MARKER.to_vec(),
    }))
    .await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert!(audit_kinds(&gate).is_empty(), "no hand-off could be built");
    assert_eq!(keys(&report), ["files", "db", "net"]);
    tokio::task::yield_now().await;
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
    assert!(gate.orchestrator.try_admit(HandoffOperation::Seed).is_ok());
}

// A panic inside the recovery itself (the clock fails on every reading after the first): the
// supervisor still answers, with what the record holds, and releases the permit.
#[tokio::test(start_paused = true)]
async fn a_panic_inside_the_recovery_is_contained() {
    let (log, _guard) = capture_logs();
    let inner = Arc::new(ManualClock::new(
        UNIX_EPOCH + Duration::from_secs(1_800_000_000),
    ));
    let gate = three_section_gate()
        .settings(|s| s.max_request_tasks = 1)
        .policy(PolicyScript::Panic)
        .clock(Arc::new(PanickingClock::from_reading(inner, 2)))
        .build();

    let report = bounded(gate.run(seed())).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(
        report.audit_id.as_deref(),
        Some("audit-1"),
        "the supervisor's accepted request hand-off is kept"
    );
    assert_eq!(report.registry_effect, RegistryEffect::Unchanged);
    assert_eq!(
        audit_kinds(&gate),
        [AuditCall::RequestHandoff],
        "the final-result content could not be built"
    );
    let lines = log.lines_with("request_recovery_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "operation", "seed"), "{}", lines[0]);
    tokio::task::yield_now().await;
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
    assert!(gate.orchestrator.try_admit(HandoffOperation::Seed).is_ok());
}

// ---- F-003: fingerprints in the progress record ----------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_recovered_final_result_keeps_the_fingerprints_of_the_fetched_schemas() {
    let gate = three_section_gate()
        .wrap_section_port(0, |port| {
            Arc::new(PanickingSectionPort::new(port, SectionCall::Apply, 1))
        })
        .build();

    let report = bounded(gate.run(seed())).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    let (_, content) = final_content(&gate);
    let fingerprints: Vec<Option<String>> = content
        .sections
        .iter()
        .map(|s| s.schema_fingerprint.clone())
        .collect();
    assert_eq!(
        fingerprints,
        [
            Some(schema_fingerprint(&files_schema())),
            Some(schema_fingerprint(&db_schema())),
            Some(schema_fingerprint(&net_schema())),
        ]
    );
}

// ---- F-013: dispatch_started -----------------------------------------------------------------

// The panic is inside the first apply call, before any section was recorded: the dispatch had
// begun all the same, so the registry effect is `not_updated`, not `unchanged`.
#[tokio::test(start_paused = true)]
async fn a_panic_inside_the_first_apply_counts_the_dispatch_as_begun() {
    let gate = three_section_gate()
        .wrap_section_port(0, |port| {
            Arc::new(PanickingSectionPort::new(port, SectionCall::Apply, 1))
        })
        .build();

    let report = bounded(gate.run(seed())).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(outcomes(&report), [SectionOutcome::NotDispatched; 3]);
    assert_eq!(report.registry_effect, RegistryEffect::NotUpdated);
}

// A seed that panicked before its dispatch began (the permission decision) is `unchanged`.
#[tokio::test(start_paused = true)]
async fn a_panic_before_the_dispatch_leaves_the_registry_unchanged() {
    let gate = three_section_gate().policy(PolicyScript::Panic).build();

    let report = bounded(gate.run(seed())).await;

    assert_eq!(report.registry_effect, RegistryEffect::Unchanged);
}

// ---- F-008: an audit id that cannot be stored ------------------------------------------------

#[tokio::test(start_paused = true)]
async fn an_audit_id_with_a_nul_is_not_accepted_and_nothing_is_dispatched() {
    let (log, _guard) = capture_logs();
    let gate = three_section_gate()
        .audit(FakeAuditPlugin::new().with_audit_id("a\0b"))
        .build();

    let report = bounded(gate.run(seed())).await;

    assert_eq!(report.category, OutcomeCategory::TemporarilyUnavailable);
    assert_eq!(
        report.unavailable,
        [UnavailableEntry {
            dependency: Dependency::AuditPlugin,
            key: None,
            reason: UnavailableReason::Unreachable,
        }]
    );
    assert_eq!(report.audit_id, None);
    assert!(
        gate.sections.iter().all(|p| p
            .calls()
            .iter()
            .all(|c| c.kind != SectionCall::Apply && c.kind != SectionCall::Remove)),
        "nothing was applied"
    );
    assert!(
        gate.registry_calls()
            .iter()
            .all(|c| c.op == RegistryOp::Get),
        "the registry was only read"
    );
    assert_eq!(audit_kinds(&gate), [AuditCall::RequestHandoff], "no final");
    let lines = log.lines_with("handoff_not_accepted");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "reason", "unreachable"),
        "{}",
        lines[0]
    );
    assert!(!lines[0].contains("a\\u{0}b"), "{}", lines[0]);
}

// ---- F-009: the plugin detail is bounded -----------------------------------------------------

// `SectionPluginError::Rejected` is `#[non_exhaustive]`, so a test outside the SDK cannot build
// one with an over-long detail by a struct literal; the constructors already cut. The report
// builder cuts again (defence in depth), and this pins the bound on what a report carries.
#[tokio::test(start_paused = true)]
async fn a_rejection_detail_in_the_report_is_bounded() {
    let long = "x".repeat(1 << 20);
    let gate = three_section_gate().build();
    gate.sections[0].on(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::rejected(long.clone(), Some(&long))),
    );

    let report = bounded(gate.run(seed())).await;

    let first = &report.sections[0];
    assert_eq!(first.outcome, SectionOutcome::Rejected);
    let limit = application_manager_sdk::errors::PLUGIN_DETAIL_MAX_BYTES;
    assert!(first.detail.as_ref().is_some_and(|d| d.len() <= limit));
    assert!(first.location.as_ref().is_some_and(|l| l.len() <= limit));
}

// ---- F-011: the cause and the span of a failed request task ----------------------------------

#[tokio::test(start_paused = true)]
async fn request_task_failed_names_the_cause_and_never_the_payload() {
    let (log, _guard) = capture_logs();
    let gate = three_section_gate().policy(PolicyScript::Panic).build();

    let _ = bounded(gate.run(seed())).await;

    let lines = log.lines_with("request_task_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(
        field_value(&lines[0], "cause").as_deref(),
        Some("panic"),
        "{}",
        lines[0]
    );
    assert!(!lines[0].contains("fake"), "no payload: {}", lines[0]);
}

#[tokio::test]
async fn a_join_error_of_a_cancelled_task_is_named_cancelled() {
    let task = tokio::spawn(std::future::pending::<()>());
    task.abort();
    let failure = task.await.expect_err("cancelled");

    assert_eq!(super::failure_cause(&failure), "cancelled");
}

#[tokio::test(start_paused = true)]
async fn the_request_task_logs_inside_the_span_of_the_caller() {
    let (log, _guard) = capture_logs();
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("no")),
    );
    let gate = three_section_gate().audit(audit).build();
    let span = tracing::info_span!("rest_call", marker = "m1");

    let handle = span.in_scope(|| gate.start(seed()));
    drop(bounded(handle).await);

    let lines = log.lines_with("handoff_not_accepted");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("marker=\"m1\""), "{}", lines[0]);
}

#[tokio::test(start_paused = true)]
async fn the_supervisor_logs_inside_the_span_of_the_caller() {
    let (log, _guard) = capture_logs();
    let gate = three_section_gate().policy(PolicyScript::Panic).build();
    let span = tracing::info_span!("rest_call", marker = "m1");

    let handle = span.in_scope(|| gate.start(seed()));
    drop(bounded(handle).await);

    let lines = log.lines_with("request_task_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("marker=\"m1\""), "{}", lines[0]);
}
