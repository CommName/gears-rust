//! Unit tests for the supervisor (P7, DESIGN §3.2 "If the request task panics or fails to join,
//! the supervisor ends the request as internal failure, which is outside the precedence table,
//! and completes the audit steps from the request progress record", the record's fields and the
//! five rules of "On a panic or join error", §3.6 paths, §4.5 row "The request task panics or
//! fails to join", §4.6 "a request task that panicked or failed to join, with the operation and
//! the request id", §4.8 "Request task tests": "A panic is injected at each progress point of the
//! request task: before and after the request hand-off attempted flag is set, during the schema
//! phase, between sections, after 'write started' and before the write result, and before and
//! after the final-result attempted flag is set. Each time, the request ends as internal failure,
//! and the counting test audit plugin sees at most one request hand-off and at most one
//! final-result hand-off, the final one only after an accepted request hand-off. A panic after
//! 'write started' reports the registry effect as not confirmed. The in-flight request task gauge
//! returns to zero after each test.").
//!
//! How the points are reached (no production seam is needed): the call guard turns a plugin panic
//! into an ordinary failure, so a panic of the TASK is injected where no guard stands: the policy
//! port (`PolicyScript::Panic`), the registry store (`panic_on`), the clock (`PanickingClock`),
//! an unguarded section port (`PanickingSectionPort`, panics at the entry of a call, before the
//! plugin) and an unguarded audit sink (`PanickingAuditSink`, panics after the plugin counted the
//! hand-off, so the task never learns the answer).
//!
//! Pinned per point (the rules of §3.2):
//! - category `internal_failure`, whatever the progress, and the supervisor's own result is
//!   returned to the caller (the `RequestHandle` gives `Ok`);
//! - the request hand-off is attempted by the supervisor only when the task had not attempted it
//!   (with the retained request content: operation, request id, receive time, and for a seed the
//!   parsed manifest); the final-result hand-off only when the request hand-off was accepted and
//!   the task had not attempted it; never a second attempt of either;
//! - sections: the recorded outcomes in priority order, then `not_dispatched` for every plugin
//!   not yet called;
//! - registry effect: the recorded result when known; `write started` as not confirmed
//!   (`not_updated` on seed, `delete_failed` on uninstall); none yet as `unchanged` when no
//!   plugin was called and as `not_updated` once a seed's dispatch had begun;
//! - the supervisor calls no section plugin and runs no registry operation of its own;
//! - the permit is released (the gauge is zero and the next request is admitted); a log event
//!   `request_task_failed` carries the operation and the request id and no panic text.
//!
//! Not pinned (decision 4 of the P7 test report): the report's `partial`, `application` and
//! `unavailable` after a panic, and a panic inside the first `apply` call (whether that call
//! counts as begun).
#![allow(clippy::duration_suboptimal_units, clippy::too_many_lines)]

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::AuditPluginError;
use application_manager_sdk::models::{
    ApplicationRef, HandoffOperation, OutcomeCategory, OutcomeReport, RegistryEffect,
    SectionOutcome,
};
use aws_lc_rs::digest::{SHA256, digest};
use serde_json::Value;
use tokio::time::Instant;
use uuid::Uuid;

use crate::domain::orchestrator::RequestInput;
use crate::test_support::{
    AuditCall, CallBehavior, EventLog, FakeAuditPlugin, Gate, GateBuilder, ManualClock,
    PLUGIN_TIMEOUT, PanickingAuditSink, PanickingClock, PanickingSectionPort, PolicyScript,
    RECEIVED_AT, RegistryOp, SectionCall, audit_kinds, bounded, capture_logs, field_value,
    final_content, has_field, keys, outcomes, request_content, stored, three_section_body,
    three_section_gate,
};

use SectionOutcome as S;

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

// One request slot, so a leaked permit shows as a refusal of the next request.
fn gate() -> GateBuilder {
    three_section_gate().settings(|s| s.max_request_tasks = 1)
}

fn gate_with_entry() -> GateBuilder {
    gate().entry(stored("app-1", "3.1"))
}

struct Expect {
    kinds: Vec<AuditCall>,
    audit_id: Option<&'static str>,
    sections: Vec<SectionOutcome>,
    effect: RegistryEffect,
    /// The category of the final-result content: the supervisor's `internal_failure`, unless the
    /// task had already handed its own result off before it panicked.
    final_category: OutcomeCategory,
}

const NONE_CALLED: [SectionOutcome; 3] = [S::NotDispatched; 3];

fn applies(gate: &Gate) -> usize {
    gate.sections
        .iter()
        .flat_map(|p| p.calls())
        .filter(|c| c.kind == SectionCall::Apply || c.kind == SectionCall::Remove)
        .count()
}

fn registry_ops(gate: &Gate) -> Vec<RegistryOp> {
    gate.registry_calls().into_iter().map(|c| c.op).collect()
}

/// Runs `input` and checks everything every panic point must satisfy.
#[allow(clippy::cognitive_complexity)]
async fn crash(name: &str, gate: &Gate, input: RequestInput, expect: &Expect) -> OutcomeReport {
    let (log, _guard) = capture_logs();
    let operation = match input {
        RequestInput::Seed { .. } => "seed",
        RequestInput::Uninstall { .. } => "uninstall",
    };

    let report = bounded(gate.run(input)).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure, "{name}");
    assert_eq!(
        audit_kinds(gate),
        expect.kinds,
        "{name}: hand-offs, at most one of each"
    );
    assert_eq!(report.audit_id.as_deref(), expect.audit_id, "{name}");
    assert_eq!(outcomes(&report), expect.sections, "{name}");
    assert_eq!(
        keys(&report),
        ["files", "db", "net"],
        "{name}: every plugin is listed"
    );
    assert_eq!(report.registry_effect, expect.effect, "{name}");
    assert!(report.problems.is_empty(), "{name}");
    // The final-result hand-off only after an accepted request hand-off, and then with the same
    // category, effect and sections as the report.
    if expect.kinds.contains(&AuditCall::FinalResultHandoff) {
        let (audit_id, content) = final_content(gate);
        assert_eq!(Some(audit_id.as_str()), expect.audit_id, "{name}");
        assert_eq!(content.category, expect.final_category, "{name}");
        assert_eq!(content.registry_effect, expect.effect, "{name}");
        let final_outcomes: Vec<SectionOutcome> =
            content.sections.iter().map(|s| s.outcome).collect();
        assert_eq!(final_outcomes, expect.sections, "{name}");
        assert_eq!(
            content.request_id,
            request_content(gate).request_id,
            "{name}"
        );
    }
    // The failure is logged with the operation and the request id (the first the gate's id source
    // gave), and no panic text. The log is read before anything else is started.
    let lines = log.lines_with("request_task_failed");
    assert_eq!(lines.len(), 1, "{name}: {lines:?}");
    assert!(
        has_field(&lines[0], "operation", operation),
        "{name}: {}",
        lines[0]
    );
    assert_eq!(
        field_value(&lines[0], "request_id"),
        Some(Uuid::from_u128(1).to_string()),
        "{name}: {}",
        lines[0]
    );
    // The permit is back and the gauge is zero; admission is checked without running a request.
    tokio::task::yield_now().await;
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0, "{name}");
    assert!(
        gate.orchestrator
            .try_admit(HandoffOperation::Uninstall)
            .is_ok(),
        "{name}: the next request is admitted"
    );
    assert!(
        !log.text().contains("fake panic"),
        "{name}: a panic's text is never logged: {}",
        log.text()
    );
    report
}

// ---- seed ---------------------------------------------------------------------------------------

// Before the request hand-off attempted flag: the permission decision panics. The supervisor
// attempts the request hand-off with the retained content, then, accepted, the final result.
#[tokio::test(start_paused = true)]
async fn a_panic_in_the_permission_decision_is_completed_by_the_supervisor() {
    // The clock moves by an hour while the permission decision runs, so a supervisor that read
    // the clock again for the retained receive time would give another one.
    let clock = Arc::new(ManualClock::new(
        std::time::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
    ));
    let gate = gate()
        .policy(PolicyScript::Panic)
        .policy_delay(Duration::from_millis(100))
        .clock(Arc::clone(&clock) as _)
        .build();
    let mover = {
        let clock = Arc::clone(&clock);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            clock.advance(Duration::from_secs(3600));
        })
    };
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("policy panic", &gate, seed(), &expect).await;
    mover.await.expect("the clock mover ends");

    // The supervisor's request hand-off carries the retained request content.
    let content = request_content(&gate);
    assert_eq!(
        content.application,
        Some(ApplicationRef {
            id: Some("app-1".to_owned()),
            version: Some("1.0".to_owned()),
        }),
        "a seed names the application of its manifest"
    );
    assert_eq!(
        content.operation,
        application_manager_sdk::models::HandoffOperation::Seed
    );
    assert_eq!(content.received_at, RECEIVED_AT);
    assert_eq!(
        content.manifest,
        Some(serde_json::from_slice::<Value>(&three_section_body()).expect("valid JSON"))
    );
    assert_eq!(content.decided_category, None);
    // The supervisor calls no plugin and runs no registry operation.
    assert!(gate.registry_calls().is_empty());
    assert_eq!(
        gate.events.events(),
        [
            "pdp:seed",
            "audit:request_handoff",
            "audit:final_result_handoff"
        ]
    );
}

// The retained content of a request that never produced a storable id: no application, the length
// and the digest instead of the id, and the id itself nowhere in the content.
#[tokio::test(start_paused = true)]
async fn the_supervisor_hands_off_a_never_storable_uninstall_id_by_length_and_digest() {
    const ID: &str = "bad\nid-SENTINEL";
    let gate = gate().policy(PolicyScript::Panic).build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };
    let input = RequestInput::Uninstall {
        application_id: ID.to_owned(),
    };

    let report = crash("never-storable id", &gate, input, &expect).await;

    assert_eq!(report.application, None, "{report:?}");
    let content = request_content(&gate);
    assert_eq!(content.operation, HandoffOperation::Uninstall);
    assert_eq!(content.application, None);
    assert_eq!(content.manifest, None);
    assert_eq!(content.id_length_bytes, Some(ID.len() as u64));
    let mut expected = String::from("sha256:");
    for byte in digest(&SHA256, ID.as_bytes()).as_ref() {
        write!(expected, "{byte:02x}").expect("writing to a string");
    }
    assert_eq!(content.id_sha256, Some(expected));
    let wire = serde_json::to_string(&content).expect("serializes");
    assert!(!wire.contains("SENTINEL"), "the id is nowhere in {wire}");
}

// A seed body that does not parse leaves nothing to retain but the operation, the request id and
// the receive time: no manifest, no application. The request still ends and gives its permit back.
#[tokio::test(start_paused = true)]
async fn the_supervisor_hands_off_an_unparsable_seed_without_manifest_or_application() {
    let gate = gate().policy(PolicyScript::Panic).build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };
    let input = RequestInput::Seed {
        body: b"{not json".to_vec(),
    };

    let report = crash("unparsable body", &gate, input, &expect).await;

    assert_eq!(report.application, None);
    let content = request_content(&gate);
    assert_eq!(content.operation, HandoffOperation::Seed);
    assert_eq!(content.manifest, None);
    assert_eq!(content.application, None);
    assert_eq!(content.received_at, RECEIVED_AT);
    assert_eq!(content.id_length_bytes, None);
    assert_eq!(content.id_sha256, None);
}

// A request already decided (here: refused by the PDP) whose task panics after the request
// hand-off was accepted and before its final-result hand-off: the caller gets `internal_failure`
// (the supervisor's category, whatever was decided), the supervisor makes the one final-result
// attempt and the request hand-off is not repeated.
#[tokio::test(start_paused = true)]
async fn a_decided_request_that_panics_before_its_final_result_ends_as_internal_failure() {
    let events = EventLog::new();
    let clock = Arc::new(PanickingClock::after_event(
        Arc::new(ManualClock::new(
            std::time::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        )),
        Arc::clone(&events),
        "audit:request_handoff",
    ));
    let gate = gate()
        .events(events)
        .policy(PolicyScript::Refuse)
        .clock(clock as _)
        .build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("decided then panic", &gate, seed(), &expect).await;

    assert_eq!(
        request_content(&gate).decided_category,
        Some(OutcomeCategory::NotPermitted),
        "the task's own hand-off carried the decision"
    );
    assert_eq!(applies(&gate), 0);
}

// DESIGN §3.2: the permit and the gauge belong to the supervisor, not to the request task: they
// are held through the supervisor's recovery (here a request hand-off that never answers, one
// plugin limit) and given back only when it ends.
#[tokio::test(start_paused = true)]
async fn the_permit_and_the_gauge_are_held_until_the_supervisors_recovery_ends() {
    let gate = gate().policy(PolicyScript::Panic).build();
    gate.audit
        .on(AuditCall::RequestHandoff, CallBehavior::hang());
    let started = Instant::now();
    let handle = gate.start(seed());

    tokio::time::sleep(PLUGIN_TIMEOUT / 2).await;

    assert!(
        gate.orchestrator.try_admit(HandoffOperation::Seed).is_err(),
        "the permit is held while the supervisor recovers"
    );
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 1);
    let report = bounded(handle).await.expect("the supervisor reports");
    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(started.elapsed(), PLUGIN_TIMEOUT);
    tokio::task::yield_now().await;
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
    assert!(gate.orchestrator.try_admit(HandoffOperation::Seed).is_ok());
    assert_eq!(audit_kinds(&gate), [AuditCall::RequestHandoff]);
}

#[tokio::test(start_paused = true)]
async fn a_panic_in_the_registry_step_is_completed_by_the_supervisor() {
    let gate = gate().build();
    gate.memory.panic_on(RegistryOp::Get);
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("registry read panic", &gate, seed(), &expect).await;

    assert_eq!(
        registry_ops(&gate),
        [RegistryOp::Get],
        "the supervisor runs no registry operation"
    );
    assert_eq!(applies(&gate), 0);
}

// After the attempted flag (the plugin took the hand-off, the task never saw the answer): no
// second attempt, and no final-result hand-off because nobody knows it was accepted.
#[tokio::test(start_paused = true)]
async fn a_panic_after_the_request_hand_off_was_attempted_gets_no_second_attempt_and_no_final() {
    let gate = gate()
        .wrap_audit_sink(|sink| Arc::new(PanickingAuditSink::new(sink, AuditCall::RequestHandoff)))
        .build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff],
        audit_id: None,
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("request hand-off panic", &gate, seed(), &expect).await;

    assert_eq!(applies(&gate), 0, "nothing is dispatched");
    assert_eq!(
        gate.events.events(),
        ["pdp:seed", "registry:get:app-1", "audit:request_handoff"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_panic_in_the_schema_phase_still_gets_its_final_result() {
    let gate = gate()
        .wrap_section_port(1, |inner| {
            Arc::new(PanickingSectionPort::new(inner, SectionCall::Schema, 1))
        })
        .build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("schema phase panic", &gate, seed(), &expect).await;

    assert_eq!(applies(&gate), 0);
    assert!(gate.memory.entries().is_empty());
    assert_eq!(registry_ops(&gate), [RegistryOp::Get]);
}

// Between sections: the first plugin was applied and its outcome recorded; the second call
// panics before reaching its plugin. A seed whose dispatch had begun is reported `not_updated`.
#[tokio::test(start_paused = true)]
async fn a_panic_between_sections_reports_the_recorded_outcomes_and_not_dispatched() {
    let gate = gate()
        .wrap_section_port(1, |inner| {
            Arc::new(PanickingSectionPort::new(inner, SectionCall::Apply, 1))
        })
        .build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: vec![S::Succeeded, S::NotDispatched, S::NotDispatched],
        effect: RegistryEffect::NotUpdated,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("between sections", &gate, seed(), &expect).await;

    assert_eq!(
        applies(&gate),
        1,
        "only the first plugin was applied, and nobody retried"
    );
    assert!(
        gate.memory.entries().is_empty(),
        "the supervisor writes nothing"
    );
    assert_eq!(registry_ops(&gate), [RegistryOp::Get]);
}

// After "write started", before the result: the upsert panics. Not confirmed: `not_updated`.
#[tokio::test(start_paused = true)]
async fn a_panic_after_write_started_reports_the_write_as_not_confirmed() {
    let gate = gate().build();
    gate.memory.panic_on(RegistryOp::Upsert);
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: vec![S::Succeeded; 3],
        effect: RegistryEffect::NotUpdated,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("write started", &gate, seed(), &expect).await;

    assert_eq!(
        registry_ops(&gate),
        [RegistryOp::Get, RegistryOp::Upsert],
        "the supervisor does not write again"
    );
    assert_eq!(applies(&gate), 3, "and calls no plugin again");
}

// After the result of the write is known and before the final-result attempted flag: the clock
// panics when the task builds the final content. The supervisor makes the one final attempt, and
// reports the known result, `updated`.
#[tokio::test(start_paused = true)]
async fn a_panic_before_the_final_result_flag_is_completed_by_the_supervisor() {
    let events = EventLog::new();
    let clock = Arc::new(PanickingClock::after_event(
        Arc::new(ManualClock::new(
            std::time::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        )),
        Arc::clone(&events),
        "registry:upsert:app-1",
    ));
    let gate = gate().events(events).clock(Arc::clone(&clock) as _).build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: vec![S::Succeeded; 3],
        effect: RegistryEffect::Updated,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("before the final flag", &gate, seed(), &expect).await;

    assert_eq!(gate.memory.entries().len(), 1, "the write had completed");
    assert!(!final_content(&gate).1.completed_at.is_empty());
}

// After the final-result attempted flag: the plugin counted the hand-off and the task panicked
// before it learned the answer. The supervisor makes no second attempt.
#[tokio::test(start_paused = true)]
async fn a_panic_after_the_final_result_was_attempted_gets_no_second_attempt() {
    let gate = gate()
        .wrap_audit_sink(|sink| {
            Arc::new(PanickingAuditSink::new(sink, AuditCall::FinalResultHandoff))
        })
        .build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: vec![S::Succeeded; 3],
        effect: RegistryEffect::Updated,
        // The task's own final-result hand-off went out before it panicked: the audit trail keeps
        // the result the task decided, while the caller is told of the failure.
        final_category: OutcomeCategory::Succeeded,
    };

    crash("after the final flag", &gate, seed(), &expect).await;

    assert_eq!(gate.memory.entries().len(), 1);
    assert_eq!(applies(&gate), 3);
}

// ---- uninstall ------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn an_uninstall_that_panics_before_anything_is_completed_by_the_supervisor() {
    let gate = gate_with_entry().policy(PolicyScript::Panic).build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("uninstall policy panic", &gate, uninstall(), &expect).await;

    let content = request_content(&gate);
    assert_eq!(
        content.operation,
        application_manager_sdk::models::HandoffOperation::Uninstall
    );
    assert_eq!(
        content.application.and_then(|a| a.id).as_deref(),
        Some("app-1")
    );
    assert_eq!(gate.memory.entries().len(), 1, "the entry is untouched");
    assert!(gate.registry_calls().is_empty());
}

// "Write started" for an uninstall is the delete: not confirmed, `delete_failed`; the removes
// never ran, so every plugin is `not_dispatched`.
#[tokio::test(start_paused = true)]
async fn an_uninstall_that_panics_in_the_delete_reports_delete_failed() {
    let gate = gate_with_entry().build();
    gate.memory.panic_on(RegistryOp::Delete);
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::DeleteFailed,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("delete started", &gate, uninstall(), &expect).await;

    assert_eq!(registry_ops(&gate), [RegistryOp::Get, RegistryOp::Delete]);
    assert_eq!(applies(&gate), 0);
}

// The delete had finished (its result is known: `deleted`) when the second remove panics.
#[tokio::test(start_paused = true)]
async fn an_uninstall_that_panics_between_removes_keeps_the_known_delete_result() {
    let gate = gate_with_entry()
        .wrap_section_port(1, |inner| {
            Arc::new(PanickingSectionPort::new(inner, SectionCall::Remove, 1))
        })
        .build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: vec![S::Succeeded, S::NotDispatched, S::NotDispatched],
        effect: RegistryEffect::Deleted,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("between removes", &gate, uninstall(), &expect).await;

    assert!(gate.memory.entries().is_empty());
    assert_eq!(applies(&gate), 1);
    assert_eq!(registry_ops(&gate), [RegistryOp::Get, RegistryOp::Delete]);
}

#[tokio::test(start_paused = true)]
async fn an_uninstall_that_panics_before_the_final_result_flag_is_completed_by_the_supervisor() {
    let events = EventLog::new();
    let clock = Arc::new(PanickingClock::after_event(
        Arc::new(ManualClock::new(
            std::time::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        )),
        Arc::clone(&events),
        "registry:delete:app-1",
    ));
    let gate = gate_with_entry().events(events).clock(clock as _).build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: vec![S::Succeeded; 3],
        effect: RegistryEffect::Deleted,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash(
        "uninstall before the final flag",
        &gate,
        uninstall(),
        &expect,
    )
    .await;

    assert!(gate.memory.entries().is_empty());
}

// ---- the supervisor's own hand-offs are guarded and single -------------------------------------

// The supervisor's request hand-off is the one attempt: refused, there is no final result and no
// retry.
#[tokio::test(start_paused = true)]
async fn a_supervisor_hand_off_that_is_refused_is_not_retried_and_gets_no_final_result() {
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::refused("no")),
    );
    let gate = gate().policy(PolicyScript::Panic).audit(audit).build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff],
        audit_id: None,
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };

    crash("refused", &gate, seed(), &expect).await;
}

// Nothing hangs: the supervisor's hand-offs go through the guard and its one time limit.
#[tokio::test(start_paused = true)]
async fn a_supervisor_hand_off_that_never_answers_ends_at_the_plugin_limit() {
    let audit = FakeAuditPlugin::new();
    audit.on(AuditCall::RequestHandoff, CallBehavior::hang());
    let gate = gate().policy(PolicyScript::Panic).audit(audit).build();
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff],
        audit_id: None,
        sections: NONE_CALLED.to_vec(),
        effect: RegistryEffect::Unchanged,
        final_category: OutcomeCategory::InternalFailure,
    };
    let started = Instant::now();

    crash("hung request hand-off", &gate, seed(), &expect).await;

    assert_eq!(started.elapsed(), PLUGIN_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn a_supervisor_final_result_that_never_answers_ends_at_the_plugin_limit() {
    let gate = gate().build();
    gate.memory.panic_on(RegistryOp::Upsert);
    gate.audit
        .on(AuditCall::FinalResultHandoff, CallBehavior::hang());
    let expect = Expect {
        kinds: vec![AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        audit_id: Some("audit-1"),
        sections: vec![S::Succeeded; 3],
        effect: RegistryEffect::NotUpdated,
        final_category: OutcomeCategory::InternalFailure,
    };
    let started = Instant::now();

    crash("hung final result", &gate, seed(), &expect).await;

    assert_eq!(started.elapsed(), PLUGIN_TIMEOUT);
}

// The permit goes back even when the supervisor had to complete a request whose handle was
// dropped by the caller.
#[tokio::test(start_paused = true)]
async fn a_crashed_request_whose_caller_disconnected_still_releases_its_slot() {
    let gate = gate().build();
    gate.memory.panic_on(RegistryOp::Upsert);

    drop(gate.start(seed()));
    assert!(
        gate.orchestrator.try_admit(HandoffOperation::Seed).is_err(),
        "the slot is held while it runs"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
    assert_eq!(audit_kinds(&gate).len(), 2);
    assert!(gate.orchestrator.try_admit(HandoffOperation::Seed).is_ok());
    bounded(
        gate.orchestrator
            .drain(&tokio_util::sync::CancellationToken::new()),
    )
    .await;
}
