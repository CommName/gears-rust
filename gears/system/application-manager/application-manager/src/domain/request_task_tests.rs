//! Unit tests for the detached request task, the request-task cap and `drain` (P7, DESIGN §3.2
//! "Steps 1 to 8 run as one request task, detached from the HTTP request" and "Request task
//! ownership", §4.5 rows "The request-task cap is reached" and "The caller disconnects, or the
//! gateway deadline passes, at any step", §4.6 logs ("a request refused at the request-task cap,
//! with the operation", "request tasks abandoned at the stop deadline, with the count per
//! operation"), §4.8 "Request task tests": "The HTTP request is dropped at each step: during the
//! permission decision, the registry step, the request hand-off, the schema phase, dispatch, the
//! registry write, and the final-result hand-off. Each time, the request task completes, and the
//! hand-off counts match. The in-flight request task gauge returns to zero after each test, and
//! a stop test shows `drain` abandoning a never-ending request task at the stop deadline with
//! a log.").
//!
//! Planned API: see `request_dispatch_tests` (`try_admit`, `start_request`,
//! `in_flight_request_tasks`, `drain`, `AtCapacity::report`). The harness drives them through
//! `Gate::start` / `Gate::try_start`; the join handle returned is what the REST handler awaits,
//! and dropping it is a caller disconnect.
//!
//! Pinned here:
//! - the cap is `max_request_tasks`: a request that finds no free permit is refused at once with
//!   category temporarily unavailable and an `audit_plugin` item with reason `at_capacity`, and
//!   nothing is called for it (no PDP, no registry, no hand-off, no plugin). The refusal is
//!   logged as `handoff_not_accepted` with `kind="request"`, `reason="at_capacity"` and the
//!   `operation`. A permit is held until the supervisor ends, also when its handle was dropped;
//! - dropping the handle at any step cuts nothing short;
//! - `drain(token)` waits for the running supervisors; once the token is cancelled it returns the
//!   number still running and logs `request_tasks_abandoned` per operation with `count`, and
//!   cancels nothing.
#![allow(clippy::duration_suboptimal_units, clippy::too_many_lines)]

use std::time::Duration;

use application_manager_sdk::models::{
    ApplicationRef, Dependency, HandoffOperation, OutcomeCategory, OutcomeReport, RegistryEffect,
    UnavailableEntry, UnavailableReason,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::domain::orchestrator::RequestInput;
use crate::test_support::{
    AuditCall, CallBehavior, FakeAuditPlugin, Gate, GateBuilder, RegistryOp, SectionCall,
    audit_kinds, bounded, capture_logs, field_value, final_content, has_field, security_context,
    stored, three_section_body, three_section_gate,
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

fn base(cap: usize) -> GateBuilder {
    three_section_gate()
        .entry(stored("app-1", "3.1"))
        .settings(move |s| s.max_request_tasks = cap)
}

// A slow permission decision keeps a request in flight for 100ms.
fn capped(cap: usize) -> GateBuilder {
    base(cap).policy_delay(Duration::from_millis(100))
}

fn never_cancelled() -> CancellationToken {
    CancellationToken::new()
}

fn applies(gate: &Gate) -> usize {
    gate.sections
        .iter()
        .flat_map(|p| p.calls())
        .filter(|c| c.kind == SectionCall::Apply)
        .count()
}

/// The request is still running: the one permit is taken (nothing else is admitted) and the
/// gauge shows one task. Checked with `try_admit`, which starts nothing.
fn assert_holds_its_permit(gate: &Gate, name: &str) {
    assert!(
        gate.orchestrator.try_admit(HandoffOperation::Seed).is_err(),
        "{name}: the permit is held while the request runs"
    );
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 1, "{name}");
}

/// The request ended: the gauge is zero (after one yield) and the permit is free again.
async fn assert_permit_released(gate: &Gate, name: &str) {
    tokio::task::yield_now().await;
    assert_eq!(
        gate.orchestrator.in_flight_request_tasks(),
        0,
        "{name}: the gauge is zero"
    );
    assert!(
        gate.orchestrator.try_admit(HandoffOperation::Seed).is_ok(),
        "{name}: the permit is free"
    );
}

// ---- the cap ---------------------------------------------------------------------------------

// DESIGN §4.5: a new seed or uninstall past `max_request_tasks` finds no free permit and is
// refused before any task starts; nothing is called for it.
#[tokio::test(start_paused = true)]
async fn a_request_past_max_request_tasks_is_refused_at_capacity_and_calls_nothing() {
    let gate = capped(2).build();
    let first = gate.start(seed());
    let second = gate.start(uninstall());
    // Both supervisors are running (inside the slow permission step) when the third arrives.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let Err(refused) = gate.try_start(seed()) else {
        panic!("the third request must be refused");
    };

    assert_eq!(
        refused.report(),
        OutcomeReport {
            category: OutcomeCategory::TemporarilyUnavailable,
            application: None,
            partial: false,
            sections: Vec::new(),
            problems: Vec::new(),
            unavailable: vec![UnavailableEntry {
                dependency: Dependency::AuditPlugin,
                key: None,
                reason: UnavailableReason::AtCapacity,
            }],
            registry_effect: RegistryEffect::Unchanged,
            audit_id: None,
        }
    );
    // Let the two admitted requests run: only they made calls.
    let (first, second) = bounded(async { (first.await, second.await) }).await;
    assert!(first.is_ok() && second.is_ok());
    assert_eq!(
        gate.policy.calls().len(),
        2,
        "the refused request asked no PDP"
    );
    assert_eq!(
        gate.audit.call_count(),
        4,
        "two requests, a hand-off pair each"
    );
    let reads = gate
        .registry_calls()
        .into_iter()
        .filter(|c| c.op == RegistryOp::Get)
        .count();
    assert_eq!(reads, 2, "the refused request read nothing");
}

// DESIGN §4.6 / PRD audit ordering: logged, with the operation, as a request hand-off that was
// not accepted.
#[tokio::test(start_paused = true)]
async fn the_refusal_at_the_cap_is_logged_with_its_operation() {
    let (log, _guard) = capture_logs();
    let gate = capped(1).build();
    let running = gate.start(seed());
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert!(gate.try_start(seed()).is_err());
    assert!(gate.try_start(uninstall()).is_err());

    let lines = log.lines_with("handoff_not_accepted");
    assert_eq!(lines.len(), 2, "{lines:?}");
    for (line, operation) in lines.iter().zip(["seed", "uninstall"]) {
        assert!(has_field(line, "kind", "request"), "{line}");
        assert!(has_field(line, "reason", "at_capacity"), "{line}");
        assert!(has_field(line, "operation", operation), "{line}");
        assert!(!line.contains("app-1"), "no request content: {line}");
    }
    bounded(running).await.expect("the admitted request ends");
}

// DESIGN §3.2 "Request task ownership": the permit is released when the supervisor ends, so the
// next request is admitted; the gauge shows the tasks in flight.
#[tokio::test(start_paused = true)]
async fn a_permit_is_released_when_the_request_ends() {
    let gate = capped(1).build();
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);

    let handle = gate.start(seed());
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 1);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 1);
    assert!(gate.try_start(seed()).is_err());
    let report = bounded(handle).await.expect("joins");

    assert_eq!(report.category, OutcomeCategory::Succeeded);
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
    let next = gate.start(uninstall());
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 1);
    bounded(next).await.expect("joins");
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
}

// DESIGN §4.5: "counting tasks whose HTTP request was dropped": a dropped handle does not free the
// permit; the end of the work does.
#[tokio::test(start_paused = true)]
async fn a_request_whose_handle_was_dropped_still_holds_its_permit_until_it_ends() {
    let gate = capped(2).build();
    drop(gate.start(seed()));
    drop(gate.start(uninstall()));
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert!(
        gate.try_start(seed()).is_err(),
        "both tasks are still running"
    );
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 2);

    // The work ends (the PDP delay is 100ms) and gives the permits back; drain, which closes
    // admission, comes last.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
    let permits = [
        gate.orchestrator.try_admit(HandoffOperation::Seed),
        gate.orchestrator.try_admit(HandoffOperation::Uninstall),
    ];
    assert!(permits.iter().all(Result::is_ok), "free permits again");
    drop(permits);
    assert_eq!(
        bounded(gate.orchestrator.drain(&never_cancelled())).await,
        0
    );
}

// DESIGN §3.2: `drain` closes the task tracker, so after it no request is admitted (the gear is
// stopping; the REST handler answers its refusal like any other). A second drain is harmless.
#[tokio::test(start_paused = true)]
async fn after_drain_no_request_is_admitted() {
    let gate = capped(2).build();
    assert_eq!(
        bounded(gate.orchestrator.drain(&never_cancelled())).await,
        0
    );

    assert!(gate.orchestrator.try_admit(HandoffOperation::Seed).is_err());
    assert!(gate.try_start(uninstall()).is_err());
    assert_eq!(
        bounded(gate.orchestrator.drain(&never_cancelled())).await,
        0,
        "a second drain returns at once"
    );
    assert!(gate.policy.calls().is_empty(), "nothing was started");
}

// DESIGN §3.2: the permit names the operation it was admitted for (what the refusal log of the
// admission used). The request that is started takes its operation from its input; that is
// pinned in `request_review_tests` (`the_input_decides_the_operation_not_the_permit`), which
// replaces the `debug_assert` `should_panic` test that stood here: a debug-only refusal left
// release builds following the permit, and the operation was passed twice.
#[tokio::test(start_paused = true)]
async fn a_permit_carries_the_operation_it_was_admitted_for() {
    let gate = capped(2).build();

    let seed_permit = gate
        .orchestrator
        .try_admit(HandoffOperation::Seed)
        .expect("a free permit");
    let uninstall_permit = gate
        .orchestrator
        .try_admit(HandoffOperation::Uninstall)
        .expect("a free permit");

    assert_eq!(seed_permit.operation(), HandoffOperation::Seed);
    assert_eq!(uninstall_permit.operation(), HandoffOperation::Uninstall);
}

// DESIGN §3.2: "The REST handler takes a permit before the orchestrator starts the supervisor":
// a permit that was taken and not used counts, and is given back when dropped.
#[tokio::test(start_paused = true)]
async fn an_admitted_permit_counts_until_it_is_used_up_or_dropped() {
    let gate = capped(1).build();

    let permit = gate
        .orchestrator
        .try_admit(HandoffOperation::Seed)
        .expect("a free permit");
    assert!(
        gate.orchestrator
            .try_admit(HandoffOperation::Uninstall)
            .is_err()
    );
    assert_eq!(
        gate.orchestrator.in_flight_request_tasks(),
        0,
        "no task yet"
    );
    drop(permit);

    let again = gate.orchestrator.try_admit(HandoffOperation::Uninstall);
    assert!(again.is_ok());
}

// The cap is one pool for both operations, and it is exactly `max_request_tasks`.
#[tokio::test(start_paused = true)]
async fn the_cap_is_one_pool_of_exactly_max_request_tasks_permits() {
    let gate = capped(3).build();

    let handles = [
        gate.start(seed()),
        gate.start(uninstall()),
        gate.start(seed()),
    ];
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(gate.try_start(uninstall()).is_err());
    assert!(gate.try_start(seed()).is_err());

    for handle in handles {
        bounded(handle).await.expect("joins");
    }
    assert!(gate.try_start(seed()).is_ok());
}

// ---- the caller disconnects ---------------------------------------------------------------------

/// How one disconnect test slows one step down, and what the event log shows while the request is
/// inside that step.
struct Step {
    name: &'static str,
    /// The delay of the permission decision (zero except in its own step).
    policy: Duration,
    slow: fn(&Gate),
    /// The events when the handle is dropped.
    events_then: &'static [&'static str],
}

const DELAY: Duration = Duration::from_millis(100);
const DROP_AT: Duration = Duration::from_millis(50);

fn seed_steps() -> Vec<Step> {
    vec![
        Step {
            name: "the permission decision",
            policy: DELAY,
            slow: |_| {},
            events_then: &["pdp:seed"],
        },
        Step {
            name: "the registry step",
            policy: Duration::ZERO,
            slow: |g| g.memory.delay(RegistryOp::Get, DELAY),
            events_then: &["pdp:seed", "registry:get:app-1"],
        },
        Step {
            name: "the request hand-off",
            policy: Duration::ZERO,
            slow: |g| {
                g.audit.on(
                    AuditCall::RequestHandoff,
                    CallBehavior::ok().with_delay(DELAY),
                );
            },
            events_then: &["pdp:seed", "registry:get:app-1", "audit:request_handoff"],
        },
        Step {
            name: "the schema phase",
            policy: Duration::ZERO,
            slow: |g| {
                for p in &g.sections {
                    p.on(SectionCall::Schema, CallBehavior::ok().with_delay(DELAY));
                }
            },
            events_then: &[
                "pdp:seed",
                "registry:get:app-1",
                "audit:request_handoff",
                "section:files:schema",
                "section:db:schema",
                "section:net:schema",
            ],
        },
        Step {
            name: "dispatch",
            policy: Duration::ZERO,
            slow: |g| g.sections[1].on(SectionCall::Apply, CallBehavior::ok().with_delay(DELAY)),
            events_then: &[
                "pdp:seed",
                "registry:get:app-1",
                "audit:request_handoff",
                "section:files:schema",
                "section:db:schema",
                "section:net:schema",
                "section:files:apply",
                "section:db:apply",
            ],
        },
        Step {
            name: "the registry write",
            policy: Duration::ZERO,
            slow: |g| g.memory.delay(RegistryOp::Upsert, DELAY),
            events_then: &[
                "pdp:seed",
                "registry:get:app-1",
                "audit:request_handoff",
                "section:files:schema",
                "section:db:schema",
                "section:net:schema",
                "section:files:apply",
                "section:db:apply",
                "section:net:apply",
                "registry:upsert:app-1",
            ],
        },
        Step {
            name: "the final-result hand-off",
            policy: Duration::ZERO,
            slow: |g| {
                g.audit.on(
                    AuditCall::FinalResultHandoff,
                    CallBehavior::ok().with_delay(DELAY),
                );
            },
            events_then: &[
                "pdp:seed",
                "registry:get:app-1",
                "audit:request_handoff",
                "section:files:schema",
                "section:db:schema",
                "section:net:schema",
                "section:files:apply",
                "section:db:apply",
                "section:net:apply",
                "registry:upsert:app-1",
                "audit:final_result_handoff",
            ],
        },
    ]
}

fn sorted_schema_events(events: &[String]) -> Vec<String> {
    // The schema calls start together; the order among them is not pinned.
    let mut out: Vec<String> = events.to_vec();
    let schema: Vec<usize> = out
        .iter()
        .enumerate()
        .filter(|(_, e)| e.ends_with(":schema"))
        .map(|(i, _)| i)
        .collect();
    if let (Some(first), Some(last)) = (schema.first(), schema.last()) {
        out[*first..=*last].sort();
    }
    out
}

fn expected_events(events: &[&str]) -> Vec<String> {
    sorted_schema_events(&events.iter().map(|e| (*e).to_owned()).collect::<Vec<_>>())
}

// DESIGN §4.8 "Request task tests": the handle is dropped during each step of a seed; the task
// completes every remaining step, and the hand-off counts match an undropped request.
#[tokio::test(start_paused = true)]
async fn a_seed_whose_caller_disconnects_at_any_step_still_completes_every_step() {
    for step in seed_steps() {
        let name = step.name;
        let mut audit = FakeAuditPlugin::new();
        audit = audit.with_audit_id("audit-1");
        let gate = base(1).policy_delay(step.policy).audit(audit).build();
        (step.slow)(&gate);
        let handle = gate.start(seed());
        tokio::time::sleep(DROP_AT).await;
        assert_eq!(
            sorted_schema_events(&gate.events.events()),
            expected_events(step.events_then),
            "{name}: the request is inside the step when it is dropped"
        );
        assert_holds_its_permit(&gate, name);

        drop(handle);
        assert_holds_its_permit(&gate, name);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_permit_released(&gate, name).await;
        assert_eq!(
            bounded(gate.orchestrator.drain(&never_cancelled())).await,
            0,
            "{name}"
        );

        assert_eq!(
            audit_kinds(&gate),
            [AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
            "{name}: one hand-off of each kind"
        );
        assert_eq!(applies(&gate), 3, "{name}: every plugin applied");
        let entries = gate.memory.entries();
        assert_eq!(entries.len(), 1, "{name}");
        assert_eq!(
            entries[0].version(),
            "1.0",
            "{name}: overwritten by the seed"
        );
        let (_, content) = final_content(&gate);
        assert_eq!(content.category, OutcomeCategory::Succeeded, "{name}");
        assert_eq!(content.registry_effect, RegistryEffect::Updated, "{name}");
    }
}

fn uninstall_steps() -> Vec<Step> {
    vec![
        Step {
            name: "the permission decision",
            policy: DELAY,
            slow: |_| {},
            events_then: &["pdp:uninstall"],
        },
        Step {
            name: "the registry step",
            policy: Duration::ZERO,
            slow: |g| g.memory.delay(RegistryOp::Get, DELAY),
            events_then: &["pdp:uninstall", "registry:get:app-1"],
        },
        Step {
            name: "the request hand-off",
            policy: Duration::ZERO,
            slow: |g| {
                g.audit.on(
                    AuditCall::RequestHandoff,
                    CallBehavior::ok().with_delay(DELAY),
                );
            },
            events_then: &[
                "pdp:uninstall",
                "registry:get:app-1",
                "audit:request_handoff",
            ],
        },
        Step {
            name: "the registry delete",
            policy: Duration::ZERO,
            slow: |g| g.memory.delay(RegistryOp::Delete, DELAY),
            events_then: &[
                "pdp:uninstall",
                "registry:get:app-1",
                "audit:request_handoff",
                "registry:delete:app-1",
            ],
        },
        Step {
            name: "dispatch",
            policy: Duration::ZERO,
            slow: |g| g.sections[1].on(SectionCall::Remove, CallBehavior::ok().with_delay(DELAY)),
            events_then: &[
                "pdp:uninstall",
                "registry:get:app-1",
                "audit:request_handoff",
                "registry:delete:app-1",
                "section:files:remove",
                "section:db:remove",
            ],
        },
        Step {
            name: "the final-result hand-off",
            policy: Duration::ZERO,
            slow: |g| {
                g.audit.on(
                    AuditCall::FinalResultHandoff,
                    CallBehavior::ok().with_delay(DELAY),
                );
            },
            events_then: &[
                "pdp:uninstall",
                "registry:get:app-1",
                "audit:request_handoff",
                "registry:delete:app-1",
                "section:files:remove",
                "section:db:remove",
                "section:net:remove",
                "audit:final_result_handoff",
            ],
        },
    ]
}

#[tokio::test(start_paused = true)]
async fn an_uninstall_whose_caller_disconnects_at_any_step_still_completes_every_step() {
    for step in uninstall_steps() {
        let name = step.name;
        let gate = base(1).policy_delay(step.policy).build();
        (step.slow)(&gate);
        let handle = gate.start(uninstall());
        tokio::time::sleep(DROP_AT).await;
        assert_eq!(gate.events.events(), step.events_then, "{name}");
        assert_holds_its_permit(&gate, name);

        drop(handle);
        assert_holds_its_permit(&gate, name);
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_permit_released(&gate, name).await;
        assert_eq!(
            bounded(gate.orchestrator.drain(&never_cancelled())).await,
            0,
            "{name}"
        );

        assert_eq!(
            audit_kinds(&gate),
            [AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
            "{name}"
        );
        assert!(gate.memory.entries().is_empty(), "{name}: deleted");
        let removes = gate
            .sections
            .iter()
            .flat_map(|p| p.calls())
            .filter(|c| c.kind == SectionCall::Remove)
            .count();
        assert_eq!(removes, 3, "{name}");
        let (_, content) = final_content(&gate);
        assert_eq!(content.registry_effect, RegistryEffect::Deleted, "{name}");
    }
}

// DESIGN §4.5: "or the gateway deadline passes": the REST handler is cut by the gateway's timeout
// layer, which drops its future and so the join handle, while the work still has time to run.
#[tokio::test(start_paused = true)]
async fn a_request_past_the_gateway_deadline_still_completes() {
    let gate = capped(4).build();
    for plugin in &gate.sections {
        plugin.on(
            SectionCall::Apply,
            CallBehavior::ok().with_delay(Duration::from_millis(400)),
        );
    }
    let started = Instant::now();

    let waited = tokio::time::timeout(Duration::from_millis(500), gate.start(seed())).await;

    assert!(waited.is_err(), "the deadline passed first");
    assert_eq!(started.elapsed(), Duration::from_millis(500));
    assert_eq!(
        bounded(gate.orchestrator.drain(&never_cancelled())).await,
        0
    );
    assert_eq!(applies(&gate), 3);
    assert_eq!(audit_kinds(&gate).len(), 2);
    assert_eq!(gate.memory.entries()[0].version(), "1.0");
    assert_eq!(started.elapsed(), Duration::from_millis(100 + 3 * 400));
}

// The supervisor is started when `start_request` is called, not when the handle is first polled:
// a handle dropped at once cuts nothing.
#[tokio::test(start_paused = true)]
async fn a_handle_dropped_before_it_was_ever_polled_cuts_nothing() {
    let gate = capped(4).build();

    drop(gate.start(seed()));
    assert_eq!(
        bounded(gate.orchestrator.drain(&never_cancelled())).await,
        0
    );

    assert_eq!(audit_kinds(&gate).len(), 2);
    assert_eq!(gate.memory.entries()[0].version(), "1.0");
}

// A request that a caller awaits gives the same result as one the caller dropped: the report
// belongs to the request, not to the wait.
#[tokio::test(start_paused = true)]
async fn the_awaited_report_matches_the_audit_trail_of_the_dropped_one() {
    let awaited = capped(4).build();
    let report = awaited.run(seed()).await;
    let dropped = capped(4).build();

    drop(dropped.start(seed()));
    bounded(dropped.orchestrator.drain(&never_cancelled())).await;

    let (_, awaited_final) = final_content(&awaited);
    let (_, dropped_final) = final_content(&dropped);
    assert_eq!(awaited_final.category, dropped_final.category);
    assert_eq!(awaited_final.sections, dropped_final.sections);
    assert_eq!(awaited_final.registry_effect, dropped_final.registry_effect);
    assert_eq!(
        report.application,
        Some(ApplicationRef {
            id: Some("app-1".to_owned()),
            version: Some("1.0".to_owned()),
        })
    );
}

// ---- drain ----------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn drain_with_no_request_task_returns_zero_at_once() {
    let gate = capped(2).build();
    let started = Instant::now();

    assert_eq!(
        bounded(gate.orchestrator.drain(&never_cancelled())).await,
        0
    );

    assert_eq!(started.elapsed(), Duration::ZERO);
}

// DESIGN §3.2: `drain` waits for the running supervisors; the request completes, hand-offs
// included, before it returns.
#[tokio::test(start_paused = true)]
async fn drain_waits_for_a_running_request_to_finish_all_its_steps() {
    let gate = capped(2).build();
    let handle = gate.start(seed());
    let started = Instant::now();

    let abandoned = bounded(gate.orchestrator.drain(&never_cancelled())).await;

    assert_eq!(abandoned, 0);
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(100),
        "the PDP delay is the whole wait"
    );
    assert_eq!(
        audit_kinds(&gate).len(),
        2,
        "both hand-offs were made before drain returned"
    );
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
    assert!(handle.is_finished());
}

// DESIGN §4.8: "`drain` abandoning a never-ending request task at the stop deadline with a log",
// the count per operation; nothing is cancelled. Two seeds and one uninstall are abandoned: two
// lines, counts 2 and 1, and the return value is the total, 3. The delay is two hours, beyond the
// one-hour bound of `bounded`, so a drain that waited for the work would fail the test.
#[tokio::test(start_paused = true)]
async fn drain_abandons_requests_at_the_deadline_logs_each_operation_and_cancels_nothing() {
    let (log, _guard) = capture_logs();
    let gate = three_section_gate()
        .policy_delay(Duration::from_secs(2 * 3600))
        .entry(stored("app-1", "3.1"))
        .build();
    let handles = [
        gate.start(seed()),
        gate.start(uninstall()),
        gate.start(seed()),
    ];
    let deadline = CancellationToken::new();
    let cancel = deadline.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        cancel.cancel();
    });
    let started = Instant::now();

    let abandoned = bounded(gate.orchestrator.drain(&deadline)).await;

    assert_eq!(abandoned, 3);
    assert_eq!(started.elapsed(), Duration::from_secs(2));
    let lines = log.lines_with("request_tasks_abandoned");
    assert_eq!(lines.len(), 2, "one event per operation: {lines:?}");
    for (operation, count) in [("seed", "2"), ("uninstall", "1")] {
        let line = lines
            .iter()
            .find(|l| has_field(l, "operation", operation))
            .unwrap_or_else(|| panic!("a line for {operation}: {lines:?}"));
        assert_eq!(field_value(line, "count").as_deref(), Some(count), "{line}");
    }
    assert_eq!(
        gate.orchestrator.in_flight_request_tasks(),
        3,
        "nothing was cancelled"
    );
    // Left alone, the abandoned requests still finish (the paused clock runs the two hours).
    for handle in handles {
        handle.await.expect("joins");
    }
    assert_eq!(audit_kinds(&gate).len(), 6);
}

// Only what is still running is abandoned and counted: a request that ended before the deadline
// is not, and an operation with nothing left gets no line. The uninstall ends after the PDP
// delay (100ms); the seed is inside its registry write (bounded by 3s) when the token fires at 1s.
#[tokio::test(start_paused = true)]
async fn drain_counts_only_the_requests_still_running_at_the_deadline() {
    let (log, _guard) = capture_logs();
    let gate = capped(2).build();
    gate.memory
        .delay(RegistryOp::Upsert, Duration::from_secs(2 * 3600));
    let finished = gate.start(uninstall());
    let running = gate.start(seed());
    let deadline = CancellationToken::new();
    let cancel = deadline.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        cancel.cancel();
    });

    let abandoned = bounded(gate.orchestrator.drain(&deadline)).await;

    assert_eq!(abandoned, 1);
    let lines = log.lines_with("request_tasks_abandoned");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "operation", "seed"), "{}", lines[0]);
    assert_eq!(
        field_value(&lines[0], "count").as_deref(),
        Some("1"),
        "{}",
        lines[0]
    );
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 1);
    finished.await.expect("the uninstall ended");
    running.await.expect("the seed finishes when left alone");
}

// Nothing is logged as abandoned when everything ended in time.
#[tokio::test(start_paused = true)]
async fn drain_logs_nothing_when_every_request_ended_in_time() {
    let (log, _guard) = capture_logs();
    let gate = capped(2).build();
    drop(gate.start(seed()));

    bounded(gate.orchestrator.drain(&never_cancelled())).await;

    log.assert_live();
    assert!(log.lines_with("request_tasks_abandoned").is_empty());
}

// The context the harness uses carries a bearer token only the PDP may see.
#[tokio::test(start_paused = true)]
async fn a_request_started_by_the_handler_uses_the_callers_context() {
    let gate = capped(1).build();

    bounded(gate.start(seed())).await.expect("joins");

    let calls = gate.policy.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].ctx.subject_id(), security_context().subject_id());
}
