//! Unit tests for steps 6-7 of a seed through the full request (P7, DESIGN §3.2 Request
//! Orchestrator steps 6-7 and its "After dispatch" table, §3.6 "Seed and Re-Seed", §4.5 rows
//! "A section plugin fails or times out during dispatch", "A plugin's in-flight cap is reached",
//! "Registry upsert fails or times out after dispatch", ADR 0001 no retry, §4.8 "Dispatch order,
//! continuing after failures, per-section results, timeouts: integration, under a paused clock").
//!
//! Planned API (all in `crate::domain::orchestrator` unless stated), used through the harness
//! (`Gate::run_seed` and friends in `test_support`):
//!
//! ```text
//! pub struct OrchestratorSettings { ..., max_request_tasks: usize }   // from_config: the setting
//! pub struct RequestPermit;                    // crate::domain::request; one request-task permit
//! impl RequestPermit { pub fn operation(&self) -> HandoffOperation }  // the permit names its operation
//! #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//! pub struct AtCapacity;                       // crate::domain::request; refusal at the cap (or closed)
//! impl AtCapacity { pub fn report(&self) -> OutcomeReport }
//! impl Orchestrator {
//!     /// What the REST handler does first (DESIGN §3.2 "Request task ownership"): takes a
//!     /// permit without waiting, or refuses at the cap.
//!     pub fn try_admit(&self, operation: HandoffOperation) -> Result<RequestPermit, AtCapacity>;
//!     /// Starts the supervisor, detached, in the orchestrator's tracker; the permit moves into
//!     /// it and gives the operation (`debug_assert`: it is the input's operation). The handle
//!     /// is the only thing the REST handler awaits; dropping it cuts nothing.
//!     pub fn start_request(&self, permit: RequestPermit, ctx: SecurityContext,
//!                          input: RequestInput) -> RequestHandle;  // awaited like a join handle, no abort
//!     pub fn in_flight_request_tasks(&self) -> usize;
//!     /// Closes admission (later `try_admit` is refused like at the cap), waits for the running
//!     /// supervisors until `deadline` is cancelled, returns how many it abandoned.
//!     pub async fn drain(&self, deadline: &CancellationToken) -> usize;
//! }
//! ```
//!
//! Flow pinned here, seed after a Proceed: `apply(caller, envelope, section)` on every section
//! plugin, one at a time, in priority order, whatever an earlier one returned (step 6); then the
//! registry upsert of the entry (step 7); then the final-result hand-off (step 8, pinned in
//! `request_final_handoff_tests`). The report: the after-dispatch category (`dispatch`), `partial`,
//! one `sections` item per plugin in priority order, `application` with id and version, no
//! problems, no `unavailable`, the registry effect `updated` or `not_updated`, the audit id.
//!
//! Registry write bounds (DESIGN §4.5): the orchestrator bounds the upsert by
//! `registry_operation_timeout` itself, as it does the read of step 3, so a store that never
//! answers ends the write as `not_updated` (decision 6 of the P7 test report).
#![allow(
    clippy::duration_suboptimal_units,
    clippy::too_many_lines,
    clippy::type_complexity
)]

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use application_manager_sdk::models::{
    ApplicationRef, Dependency, OutcomeCategory, RegistryEffect, SectionOutcome, UnavailableEntry,
    UnavailableReason,
};
use application_manager_sdk::{ApplicationEnvelope, CallerIdentity, SectionPluginError};
use secrecy::ExposeSecret;
use serde_json::json;
use tokio::time::Instant;

use crate::domain::ports::{PluginCallError, RegistryError};
use crate::domain::registry::{LastResult, RegistryEntry};
use crate::infra::policy::PolicyAdapter;
use crate::test_support::{
    AuthzScript, CallBehavior, FakeAuthz, GateBuilder, ManualClock, PDP_TIMEOUT, PLUGIN_TIMEOUT,
    RECEIVED_AT, REGISTRY_TIMEOUT, RegistryCall, RegistryOp, ScriptedSectionPort, SectionCall,
    bounded, capture_logs, field_value, files_body, files_schema, final_content, has_field, keys,
    outcomes, request_content, security_context, seed_body, stored, three_section_body,
    three_section_gate, three_sections,
};

use SectionOutcome as S;

fn reg(op: RegistryOp, id: &str) -> RegistryCall {
    RegistryCall {
        op,
        id: Some(id.to_owned()),
    }
}

fn internal() -> CallBehavior<SectionPluginError> {
    CallBehavior::err(SectionPluginError::internal("boom"))
}

fn rejected() -> CallBehavior<SectionPluginError> {
    CallBehavior::err(SectionPluginError::rejected("no good", Some("/path")))
}

fn not_permitted() -> CallBehavior<SectionPluginError> {
    CallBehavior::err(SectionPluginError::not_permitted("needs the extra grant"))
}

// ---- order -----------------------------------------------------------------------------------

// DESIGN §3.2 steps 2-8 in order: the gate's calls, then `apply` in priority order, then the
// upsert, then the final-result hand-off; nothing is dispatched before the accepted request
// hand-off.
#[tokio::test(start_paused = true)]
async fn a_seed_dispatches_in_priority_order_then_writes_then_hands_off_the_final_result() {
    let gate = three_section_gate().build();

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(report.category, OutcomeCategory::Succeeded);
    let events = gate.events.events();
    assert_eq!(
        &events[..3],
        ["pdp:seed", "registry:get:app-1", "audit:request_handoff"]
    );
    let mut schemas: Vec<&str> = events[3..6].iter().map(String::as_str).collect();
    schemas.sort_unstable();
    assert_eq!(
        schemas,
        [
            "section:db:schema",
            "section:files:schema",
            "section:net:schema"
        ]
    );
    assert_eq!(
        &events[6..],
        [
            "section:files:apply",
            "section:db:apply",
            "section:net:apply",
            "registry:upsert:app-1",
            "audit:final_result_handoff",
        ]
    );
}

// DESIGN §4.8 "Dispatch order": the plugins are called one at a time, each after the previous one
// returned, so the time of a seed is the sum of the apply times, not the longest one.
#[tokio::test(start_paused = true)]
async fn the_apply_calls_run_one_after_another() {
    let gate = three_section_gate().build();
    for plugin in &gate.sections {
        plugin.on(
            SectionCall::Apply,
            CallBehavior::ok().with_delay(Duration::from_millis(100)),
        );
    }
    let started = Instant::now();

    let report = bounded(gate.run_seed(three_section_body())).await;

    assert_eq!(report.category, OutcomeCategory::Succeeded);
    assert_eq!(started.elapsed(), Duration::from_millis(300));
    for plugin in &gate.sections {
        assert_eq!(plugin.completed(), 2, "one schema and one apply each");
    }
}

// Every plugin gets the caller identity (no bearer token), the envelope, and ITS section
// unchanged: DESIGN §3.1 Section ("passes it unchanged"), §3.3 "Caller identity".
#[tokio::test(start_paused = true)]
async fn each_plugin_gets_the_caller_the_envelope_and_its_own_section_unchanged() {
    let gate = three_section_gate().build();

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(report.category, OutcomeCategory::Succeeded);
    let request_id = request_content(&gate).request_id;
    let sections = three_sections();
    for (plugin, key) in gate.sections.iter().zip(["files", "db", "net"]) {
        let applies: Vec<_> = plugin
            .calls()
            .into_iter()
            .filter(|c| c.kind == SectionCall::Apply)
            .collect();
        assert_eq!(applies.len(), 1, "{key}: one apply, no retry");
        let apply = &applies[0];
        assert_eq!(
            apply.envelope,
            Some(ApplicationEnvelope {
                id: "app-1".to_owned(),
                version: "1.0".to_owned(),
            }),
            "{key}"
        );
        assert_eq!(apply.section.as_ref(), Some(&sections[key]), "{key}");
        let caller = apply.caller.as_ref().expect("a caller");
        assert_eq!(caller.request_id, request_id, "{key}");
        assert_eq!(
            caller.subject_tenant_id,
            crate::test_support::security_context().subject_tenant_id()
        );
        assert!(
            !format!("{caller:?}").contains("pdp-bearer-token"),
            "{key}: the plugin sees no bearer token"
        );
    }
}

// ---- keeps going, worst outcome wins -------------------------------------------------------------

// DESIGN §3.2 "After dispatch": any not_permitted gives not permitted, else any rejected gives
// plugin rejected, else any internal_failure / timed_out / not_sent gives internal failure. Each
// plugin is called once and the dispatch goes on after a failure; the entry is written with the
// derived last result.
#[tokio::test(start_paused = true)]
async fn dispatch_goes_on_after_a_failure_and_the_worst_outcome_wins() {
    type B = CallBehavior<SectionPluginError>;
    let cases: Vec<(
        &str,
        [B; 3],
        OutcomeCategory,
        [SectionOutcome; 3],
        bool,
        LastResult,
    )> = vec![
        (
            "all succeed",
            [B::ok(), B::ok(), B::ok()],
            OutcomeCategory::Succeeded,
            [S::Succeeded, S::Succeeded, S::Succeeded],
            false,
            LastResult::Succeeded,
        ),
        (
            "the middle one rejects",
            [B::ok(), rejected(), B::ok()],
            OutcomeCategory::PluginRejected,
            [S::Succeeded, S::Rejected, S::Succeeded],
            true,
            LastResult::PartiallyFailed,
        ),
        (
            "rejected, then not permitted: not permitted wins",
            [rejected(), not_permitted(), B::ok()],
            OutcomeCategory::NotPermitted,
            [S::Rejected, S::NotPermitted, S::Succeeded],
            true,
            LastResult::PartiallyFailed,
        ),
        (
            "internal failure first, then a rejection: rejected wins",
            [internal(), rejected(), B::ok()],
            OutcomeCategory::PluginRejected,
            [S::InternalFailure, S::Rejected, S::Succeeded],
            true,
            LastResult::PartiallyFailed,
        ),
        (
            "the last one fails internally",
            [B::ok(), B::ok(), internal()],
            OutcomeCategory::InternalFailure,
            [S::Succeeded, S::Succeeded, S::InternalFailure],
            true,
            LastResult::PartiallyFailed,
        ),
        (
            "every plugin rejects",
            [rejected(), rejected(), rejected()],
            OutcomeCategory::PluginRejected,
            [S::Rejected, S::Rejected, S::Rejected],
            false,
            LastResult::PluginRejected,
        ),
        (
            "every plugin refuses the caller",
            [not_permitted(), not_permitted(), not_permitted()],
            OutcomeCategory::NotPermitted,
            [S::NotPermitted, S::NotPermitted, S::NotPermitted],
            false,
            LastResult::NotPermitted,
        ),
        (
            "every plugin fails internally",
            [internal(), internal(), internal()],
            OutcomeCategory::InternalFailure,
            [S::InternalFailure, S::InternalFailure, S::InternalFailure],
            false,
            LastResult::InternalFailure,
        ),
    ];

    for (name, behaviors, category, expected, partial, last_result) in cases {
        let gate = three_section_gate().build();
        for (plugin, behavior) in gate.sections.iter().zip(behaviors) {
            plugin.on(SectionCall::Apply, behavior);
        }

        let report = bounded(gate.run_seed(three_section_body())).await;

        assert_eq!(report.category, category, "{name}");
        assert_eq!(outcomes(&report), expected, "{name}");
        assert_eq!(keys(&report), ["files", "db", "net"], "{name}");
        assert_eq!(report.partial, partial, "{name}");
        assert_eq!(report.registry_effect, RegistryEffect::Updated, "{name}");
        for plugin in &gate.sections {
            let applies = plugin
                .calls()
                .iter()
                .filter(|c| c.kind == SectionCall::Apply)
                .count();
            assert_eq!(applies, 1, "{name}: each plugin is applied once, no retry");
        }
        let entries = gate.memory.entries();
        assert_eq!(entries.len(), 1, "{name}");
        assert_eq!(entries[0].last_result(), last_result, "{name}");
    }
}

// DESIGN §3.3 section plugin API: a time-limit expiry is `timed_out` after one plugin limit and
// the dispatch continues; a hung plugin costs one limit per call and nothing else.
#[tokio::test(start_paused = true)]
async fn a_plugin_that_never_answers_apply_is_timed_out_at_the_plugin_limit_and_dispatch_goes_on() {
    let gate = three_section_gate().build();
    gate.sections[0].on(SectionCall::Apply, CallBehavior::hang());
    gate.sections[1].on(SectionCall::Apply, CallBehavior::hang());
    let started = Instant::now();

    let report = bounded(gate.run_seed(three_section_body())).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(outcomes(&report), [S::TimedOut, S::TimedOut, S::Succeeded]);
    assert!(report.partial);
    assert_eq!(started.elapsed(), PLUGIN_TIMEOUT * 2);
    assert_eq!(
        gate.sections[2].completed(),
        2,
        "the last plugin was applied"
    );
    assert_eq!(report.registry_effect, RegistryEffect::Updated);
}

// DESIGN §3.3: a call the plugin's in-flight cap refuses is never sent and ends at once as
// `not_sent`, an internal failure with the dispatch going on. The real guard gives `not_sent`
// only for a full cap, so the port answer is scripted here (a scripted port outside the guard).
#[tokio::test(start_paused = true)]
async fn a_call_the_guard_did_not_send_is_not_sent_and_dispatch_goes_on() {
    let gate = three_section_gate()
        .wrap_section_port(0, |inner| {
            Arc::new(ScriptedSectionPort::new(
                inner,
                SectionCall::Apply,
                PluginCallError::NotSent,
            ))
        })
        .build();
    let started = Instant::now();

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(outcomes(&report), [S::NotSent, S::Succeeded, S::Succeeded]);
    assert_eq!(started.elapsed(), Duration::ZERO, "not sent ends at once");
    assert!(
        gate.sections[0]
            .calls()
            .iter()
            .all(|c| c.kind != SectionCall::Apply),
        "the plugin never saw the call"
    );
}

// DESIGN §3.3: any other failure (a guard-internal failure) is the section outcome
// `internal_failure`.
#[tokio::test(start_paused = true)]
async fn a_guard_failure_of_apply_is_an_internal_failure_of_that_section() {
    let gate = three_section_gate()
        .wrap_section_port(1, |inner| {
            Arc::new(ScriptedSectionPort::new(
                inner,
                SectionCall::Apply,
                PluginCallError::Internal("plugin call failed".to_owned()),
            ))
        })
        .build();

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(
        outcomes(&report),
        [S::Succeeded, S::InternalFailure, S::Succeeded]
    );
}

// DESIGN §3.1 outcome report: `detail` and `location` only for `rejected` and `not_permitted`,
// the plugin's own text; DESIGN §3.3 "Safe wire errors": the text of an `Internal` error never
// appears in a response.
#[tokio::test(start_paused = true)]
async fn the_report_carries_the_detail_of_rejected_and_not_permitted_sections_only() {
    let gate = three_section_gate().build();
    gate.sections[0].on(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::rejected(
            "path is not absolute",
            Some("/path"),
        )),
    );
    gate.sections[1].on(SectionCall::Apply, not_permitted());
    gate.sections[2].on(
        SectionCall::Apply,
        CallBehavior::err(SectionPluginError::internal("SENTINEL-INTERNAL-DETAIL")),
    );

    let report = gate.run_seed(three_section_body()).await;

    let [files, db, net] = &report.sections[..] else {
        panic!("three sections: {:?}", report.sections);
    };
    assert_eq!(files.detail.as_deref(), Some("path is not absolute"));
    assert_eq!(files.location.as_deref(), Some("/path"));
    assert_eq!(db.detail.as_deref(), Some("needs the extra grant"));
    assert_eq!(db.location, None);
    assert_eq!(net.outcome, S::InternalFailure);
    assert_eq!(net.detail, None);
    assert_eq!(net.location, None);
    let wire = serde_json::to_string(&report).expect("serializes");
    assert!(!wire.contains("SENTINEL-INTERNAL-DETAIL"), "{wire}");
}

// DESIGN §3.1: an empty key set gives a succeeded seed with no plugin call; the entry is still
// written (a seed that passes validation creates or overwrites the entry).
#[tokio::test(start_paused = true)]
async fn a_seed_with_no_section_plugin_succeeds_and_writes_the_entry() {
    let gate = GateBuilder::new().build();

    let report = gate.run_seed(seed_body("app-1", "1.0", &json!({}))).await;

    assert_eq!(report.category, OutcomeCategory::Succeeded);
    assert!(report.sections.is_empty());
    assert!(!report.partial);
    assert_eq!(report.registry_effect, RegistryEffect::Updated);
    let entries = gate.memory.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].last_result(), LastResult::Succeeded);
}

// ---- the report -----------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_report_of_a_dispatched_seed_names_the_application_and_the_audit_id() {
    let gate = three_section_gate().build();

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(
        report.application,
        Some(ApplicationRef {
            id: Some("app-1".to_owned()),
            version: Some("1.0".to_owned()),
        })
    );
    assert_eq!(report.audit_id.as_deref(), Some("audit-1"));
    assert!(report.problems.is_empty());
    assert!(report.unavailable.is_empty());
    assert_eq!(report.registry_effect, RegistryEffect::Updated);
    assert!(!report.partial);
}

// ---- the registry write ---------------------------------------------------------------------

// DESIGN §3.1 entry: the id and version of the envelope, the derived last result, the receive
// time of that seed (not the completion time), and the audit id of that seed.
#[tokio::test(start_paused = true)]
async fn the_entry_holds_the_envelope_the_last_result_the_receive_time_and_the_audit_id() {
    let clock = Arc::new(ManualClock::new(
        UNIX_EPOCH + Duration::from_secs(1_800_000_000),
    ));
    let gate = three_section_gate().clock(Arc::clone(&clock) as _).build();
    gate.sections[1].on(SectionCall::Apply, rejected());
    // The clock moves while the first plugin works: the entry must still carry the receive time.
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

    let report = bounded(gate.run_seed(three_section_body())).await;
    mover.await.expect("the clock mover ends");

    assert_eq!(report.registry_effect, RegistryEffect::Updated);
    let entries = gate.memory.entries();
    assert_eq!(
        entries,
        vec![
            RegistryEntry::new(
                "app-1",
                "1.0",
                LastResult::PartiallyFailed,
                UNIX_EPOCH + Duration::from_secs(1_800_000_000),
                "audit-1",
            )
            .expect("an entry")
        ]
    );
    assert_eq!(request_content(&gate).received_at, RECEIVED_AT);
}

// DESIGN §3.6 re-seed: the upsert overwrites the entry; there is still one entry.
#[tokio::test(start_paused = true)]
async fn a_reseed_overwrites_the_entry() {
    let gate = three_section_gate().entry(stored("app-1", "0.9")).build();

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(report.registry_effect, RegistryEffect::Updated);
    let entries = gate.memory.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].version(), "1.0");
    assert_eq!(entries[0].audit_id(), "audit-1");
    assert_eq!(
        gate.registry_calls(),
        [
            reg(RegistryOp::Get, "app-1"),
            reg(RegistryOp::Upsert, "app-1")
        ]
    );
}

// DESIGN §3.6 Seed and Re-Seed, §4.5: an upsert that fails counts as an internal failure under
// the after-dispatch ranking, with the registry effect `not_updated`; one attempt, no retry.
#[tokio::test(start_paused = true)]
async fn an_upsert_that_fails_is_an_internal_failure_and_not_updated() {
    for error in [RegistryError::Unavailable, RegistryError::TimedOut] {
        let gate = three_section_gate().build();
        gate.memory.fail(RegistryOp::Upsert, error.clone());

        let report = gate.run_seed(three_section_body()).await;

        assert_eq!(
            report.category,
            OutcomeCategory::InternalFailure,
            "{error:?}"
        );
        assert_eq!(
            report.registry_effect,
            RegistryEffect::NotUpdated,
            "{error:?}"
        );
        assert_eq!(outcomes(&report), [S::Succeeded; 3], "{error:?}");
        assert!(!report.partial, "{error:?}");
        assert!(gate.memory.entries().is_empty(), "{error:?}");
        let upserts = gate
            .registry_calls()
            .into_iter()
            .filter(|c| c.op == RegistryOp::Upsert)
            .count();
        assert_eq!(upserts, 1, "{error:?}: no retry");
        let (_, content) = final_content(&gate);
        assert_eq!(content.registry_effect, RegistryEffect::NotUpdated);
        assert_eq!(content.category, OutcomeCategory::InternalFailure);
    }
}

// The registry failure ranks at the internal failure level: a higher rank outranks it, and the
// effect still says what happened to the entry.
#[tokio::test(start_paused = true)]
async fn a_higher_ranked_category_outranks_a_failed_upsert() {
    let cases = [
        (rejected(), OutcomeCategory::PluginRejected),
        (not_permitted(), OutcomeCategory::NotPermitted),
    ];
    for (behavior, category) in cases {
        let gate = three_section_gate().build();
        gate.sections[1].on(SectionCall::Apply, behavior);
        gate.memory
            .fail(RegistryOp::Upsert, RegistryError::Unavailable);

        let report = gate.run_seed(three_section_body()).await;

        assert_eq!(report.category, category);
        assert_eq!(report.registry_effect, RegistryEffect::NotUpdated);
    }
}

// DESIGN §4.8 Integration: "A registry double that commits an upsert ... and then exceeds
// `registry_operation_timeout` gives `not_updated`, and the listing then shows the committed
// state." The effect means not confirmed, not not happened.
#[tokio::test(start_paused = true)]
async fn an_upsert_that_commits_and_then_times_out_is_not_updated_while_the_entry_is_there() {
    let gate = three_section_gate().build();
    gate.memory
        .fail_after_commit(RegistryOp::Upsert, RegistryError::TimedOut);

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(report.registry_effect, RegistryEffect::NotUpdated);
    let entries = gate.memory.entries();
    assert_eq!(entries.len(), 1, "the listing shows the committed state");
    assert_eq!(entries[0].version(), "1.0");
    assert_eq!(final_content_effect(&gate), RegistryEffect::NotUpdated);
}

fn final_content_effect(gate: &crate::test_support::Gate) -> RegistryEffect {
    final_content(gate).1.registry_effect
}

// DESIGN §4.5: the upsert is bounded by `registry_operation_timeout`; a store that never answers
// ends the write as not confirmed after exactly one bound, and the failure is logged.
#[tokio::test(start_paused = true)]
async fn an_upsert_that_never_answers_ends_after_the_registry_operation_timeout() {
    let (log, _guard) = capture_logs();
    let gate = three_section_gate().build();
    gate.memory.hang(RegistryOp::Upsert);
    let started = Instant::now();

    let report = bounded(gate.run_seed(three_section_body())).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(report.registry_effect, RegistryEffect::NotUpdated);
    assert_eq!(started.elapsed(), REGISTRY_TIMEOUT);
    let lines = log.lines_with("registry_write_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "op", "upsert"), "{}", lines[0]);
    assert!(has_field(&lines[0], "reason", "timed_out"), "{}", lines[0]);
}

// DESIGN §4.6: a registry write failure is logged, with the operation and the request id and no
// content.
#[tokio::test(start_paused = true)]
async fn a_failed_upsert_is_logged_with_its_operation_and_reason_and_no_content() {
    let (log, _guard) = capture_logs();
    let gate = three_section_gate().build();
    gate.memory
        .fail(RegistryOp::Upsert, RegistryError::Unavailable);

    gate.run_seed(three_section_body()).await;

    let lines = log.lines_with("registry_write_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "op", "upsert"), "{}", lines[0]);
    assert!(
        has_field(&lines[0], "reason", "unreachable"),
        "{}",
        lines[0]
    );
    assert_eq!(
        field_value(&lines[0], "request_id"),
        Some(request_content(&gate).request_id.to_string()),
        "{}",
        lines[0]
    );
    // The application id is no content (DESIGN §4.6 allows it); the manifest's section values are.
    assert!(!lines[0].contains("/srv/app"), "no content: {}", lines[0]);
}

// ---- nothing else is called -------------------------------------------------------------------

// A seed with one section: the registry sees one read (the gate) and one upsert, and the
// request makes no other call.
#[tokio::test(start_paused = true)]
async fn the_registry_sees_one_read_and_one_upsert_of_a_seed() {
    let gate = GateBuilder::new().section("files", files_schema()).build();

    gate.run_seed(files_body()).await;

    assert_eq!(
        gate.registry_calls(),
        [
            reg(RegistryOp::Get, "app-1"),
            reg(RegistryOp::Upsert, "app-1")
        ]
    );
    assert_eq!(gate.policy.calls().len(), 1, "one permission decision");
}

// ADR 0007 / DESIGN §3.3 "Authorization": through the real policy adapter, the full request of a
// seed and of an uninstall names no context tenant and does not require constraints; the id is
// the resource property; each request asks the PDP once.
#[tokio::test(start_paused = true)]
async fn through_the_real_adapter_a_full_request_names_no_context_tenant() {
    let authz = Arc::new(FakeAuthz::new(AuthzScript::Permit(vec![])));
    let adapter = PolicyAdapter::new(authz.clone(), PDP_TIMEOUT);
    let gate = three_section_gate()
        .policy_port(Arc::new(adapter))
        .entry(stored("app-2", "1"))
        .build();

    let seeded = gate.run_seed(three_section_body()).await;
    let uninstalled = gate.run_uninstall("app-2").await;

    assert_eq!(seeded.category, OutcomeCategory::Succeeded);
    assert_eq!(uninstalled.category, OutcomeCategory::Succeeded);
    let requests = authz.requests();
    assert_eq!(requests.len(), 2, "one decision per request");
    for r in &requests {
        assert!(r.context.tenant_context.is_none(), "no context tenant");
        assert!(!r.context.require_constraints);
    }
    assert_eq!(requests[0].action.name, "seed");
    assert_eq!(requests[1].action.name, "uninstall");
    assert_eq!(
        requests[0].resource.properties.get("application_id"),
        Some(&json!("app-1"))
    );
    assert_eq!(
        requests[1].resource.properties.get("application_id"),
        Some(&json!("app-2"))
    );
}

// DESIGN §3.3: a plugin's in-flight cap counts a call still running after its timeout, and the
// cap refuses without cancelling anything. With a cap of one, the apply of the first seed that
// timed out keeps the slot, so the next seed's `schema` call to that plugin is `not_sent` (a
// schema reason, 503 `section_schema`), and nothing is dispatched for it.
#[tokio::test(start_paused = true)]
async fn a_detached_apply_keeps_the_plugin_slot_so_the_next_seed_gets_not_sent() {
    let gate = three_section_gate().guard_cap(1).build();
    gate.sections[0].on(SectionCall::Apply, CallBehavior::hang());

    let first = bounded(gate.run_seed(three_section_body())).await;
    let second = bounded(gate.run_seed(three_section_body())).await;

    assert_eq!(first.category, OutcomeCategory::InternalFailure);
    assert_eq!(outcomes(&first), [S::TimedOut, S::Succeeded, S::Succeeded]);
    assert_eq!(second.category, OutcomeCategory::TemporarilyUnavailable);
    assert_eq!(
        second.unavailable,
        vec![UnavailableEntry {
            dependency: Dependency::SectionSchema,
            key: Some("files".to_owned()),
            reason: UnavailableReason::NotSent,
        }]
    );
    assert!(second.sections.is_empty());
    let applies: Vec<usize> = gate
        .sections
        .iter()
        .map(|p| {
            p.calls()
                .iter()
                .filter(|c| c.kind == SectionCall::Apply)
                .count()
        })
        .collect();
    assert_eq!(applies, [1, 1, 1], "the second seed dispatched nothing");
    assert_eq!(gate.memory.entries().len(), 1, "and wrote nothing");
}

// ---- the caller across the task boundary --------------------------------------------------------

// DESIGN §3.3 "Caller identity": the request task works on what the handler handed it. The PDP
// gets the caller's `SecurityContext` unchanged, bearer token and scopes included; every audit
// and section call of a seed and of an uninstall gets the WHOLE identity (subject, type, tenant,
// scopes, the request's own id) built from it and no token.
#[tokio::test(start_paused = true)]
async fn the_whole_caller_identity_reaches_every_call_and_the_pdp_gets_the_context_with_its_token()
{
    let gate = three_section_gate().entry(stored("app-2", "1")).build();

    gate.run_seed(three_section_body()).await;
    gate.run_uninstall("app-2").await;

    let ctx = security_context();
    let decisions = gate.policy.calls();
    assert_eq!(decisions.len(), 2);
    for decision in &decisions {
        assert_eq!(decision.ctx.subject_id(), ctx.subject_id());
        assert_eq!(decision.ctx.subject_tenant_id(), ctx.subject_tenant_id());
        assert_eq!(decision.ctx.subject_type(), Some("service"));
        assert_eq!(decision.ctx.token_scopes(), ["scope-1".to_owned()]);
        assert_eq!(
            decision.ctx.bearer_token().map(ExposeSecret::expose_secret),
            Some("pdp-bearer-token"),
            "the PDP gets the token across the task boundary"
        );
    }
    let identity = |request_id| CallerIdentity::from_security_context(&ctx, request_id);
    let audit = gate.audit.calls();
    assert_eq!(audit.len(), 4, "a request and a final-result hand-off each");
    for call in &audit {
        assert_eq!(call.caller, identity(call.caller.request_id), "audit");
    }
    let mut seen = 0;
    for plugin in &gate.sections {
        for call in plugin.calls() {
            let caller = call.caller.expect("a caller");
            assert_eq!(caller, identity(caller.request_id), "{:?}", call.kind);
            seen += 1;
        }
    }
    assert_eq!(
        seen, 9,
        "a schema and an apply of the seed, a remove, per plugin"
    );
}
