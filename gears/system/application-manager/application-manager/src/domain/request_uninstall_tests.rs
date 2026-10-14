//! Unit tests for steps 6-7 of an uninstall through the full request (P7, DESIGN §3.2 "Uninstall
//! runs the delete (step 7) before dispatch (step 6), so the entry is deleted whatever the
//! section plugins return", §3.6 "Uninstall", §4.5 row "Registry delete fails or times out after
//! an accepted uninstall", ADR 0001 no retry, §4.8 "Uninstall, delete failure, not found").
//!
//! Flow pinned here, uninstall after a Proceed (the entry exists, the request hand-off was
//! accepted): `RegistryStore::delete(id)` first, bounded by `registry_operation_timeout`; then
//! `remove(caller, envelope)` on every section plugin in priority order, one at a time, whatever
//! the delete or an earlier plugin returned, with the envelope `{id, version}` where `version`
//! is the last seeded version the gate read; then the final-result hand-off. The report:
//! `application` with the id and that version, the after-dispatch category (a failed or timed
//! out delete is an internal failure of rank 3), `partial`, one `sections` item per plugin, the
//! registry effect `deleted` or `delete_failed`, the audit id. No schema call, no `apply`.
#![allow(
    clippy::duration_suboptimal_units,
    clippy::too_many_lines,
    clippy::type_complexity
)]

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::models::{
    ApplicationRef, OutcomeCategory, RegistryEffect, SectionOutcome,
};
use application_manager_sdk::{ApplicationEnvelope, SectionPluginError};
use tokio::time::Instant;

use crate::domain::ports::{PluginCallError, RegistryError};
use crate::test_support::{
    CallBehavior, GateBuilder, PLUGIN_TIMEOUT, REGISTRY_TIMEOUT, RegistryCall, RegistryOp,
    ScriptedSectionPort, SectionCall, bounded, capture_logs, field_value, final_content, has_field,
    keys, outcomes, request_content, stored, three_section_gate,
};

use SectionOutcome as S;

fn reg(op: RegistryOp, id: &str) -> RegistryCall {
    RegistryCall {
        op,
        id: Some(id.to_owned()),
    }
}

fn removes(gate: &crate::test_support::Gate, index: usize) -> usize {
    gate.sections[index]
        .calls()
        .iter()
        .filter(|c| c.kind == SectionCall::Remove)
        .count()
}

fn three() -> GateBuilder {
    three_section_gate().entry(stored("app-1", "3.1"))
}

// DESIGN §3.2: the delete comes before dispatch; the removes follow in priority order; the
// final-result hand-off comes last; there is no schema call and no apply.
#[tokio::test(start_paused = true)]
async fn an_uninstall_deletes_the_entry_then_removes_in_priority_order() {
    let gate = three().build();

    let report = gate.run_uninstall("app-1").await;

    assert_eq!(report.category, OutcomeCategory::Succeeded);
    assert_eq!(
        gate.events.events(),
        [
            "pdp:uninstall",
            "registry:get:app-1",
            "audit:request_handoff",
            "registry:delete:app-1",
            "section:files:remove",
            "section:db:remove",
            "section:net:remove",
            "audit:final_result_handoff",
        ]
    );
    assert!(gate.memory.entries().is_empty(), "the entry is gone");
}

#[tokio::test(start_paused = true)]
async fn the_report_names_the_application_with_its_last_seeded_version() {
    let gate = three().build();

    let report = gate.run_uninstall("app-1").await;

    assert_eq!(
        report.application,
        Some(ApplicationRef {
            id: Some("app-1".to_owned()),
            version: Some("3.1".to_owned()),
        })
    );
    assert_eq!(report.registry_effect, RegistryEffect::Deleted);
    assert_eq!(outcomes(&report), [S::Succeeded; 3]);
    assert_eq!(keys(&report), ["files", "db", "net"]);
    assert!(!report.partial);
    assert_eq!(report.audit_id.as_deref(), Some("audit-1"));
    assert!(report.problems.is_empty());
    assert!(report.unavailable.is_empty());
}

// DESIGN §3.3 `remove`: the caller identity and the envelope with the id and the last seeded
// version; one call per plugin.
#[tokio::test(start_paused = true)]
async fn each_plugin_gets_the_caller_and_the_id_with_the_last_seeded_version() {
    let gate = three().build();

    gate.run_uninstall("app-1").await;

    let request_id = request_content(&gate).request_id;
    for (index, plugin) in gate.sections.iter().enumerate() {
        assert_eq!(removes(&gate, index), 1, "{index}: one remove, no retry");
        let call = plugin
            .calls()
            .into_iter()
            .find(|c| c.kind == SectionCall::Remove)
            .expect("a remove");
        assert_eq!(
            call.envelope,
            Some(ApplicationEnvelope {
                id: "app-1".to_owned(),
                version: "3.1".to_owned(),
            })
        );
        assert_eq!(call.caller.expect("a caller").request_id, request_id);
        assert_eq!(call.section, None);
        assert!(
            plugin
                .calls()
                .iter()
                .all(|c| c.kind != SectionCall::Schema && c.kind != SectionCall::Apply),
            "{index}: an uninstall fetches no schema and applies nothing"
        );
    }
}

// DESIGN §3.2: removes run one at a time, so the time is the sum.
#[tokio::test(start_paused = true)]
async fn the_remove_calls_run_one_after_another() {
    let gate = three().build();
    for plugin in &gate.sections {
        plugin.on(
            SectionCall::Remove,
            CallBehavior::ok().with_delay(Duration::from_millis(100)),
        );
    }
    let started = Instant::now();

    let report = bounded(gate.run_uninstall("app-1")).await;

    assert_eq!(report.category, OutcomeCategory::Succeeded);
    assert_eq!(started.elapsed(), Duration::from_millis(300));
}

// DESIGN §3.2: "the entry is deleted whatever the section plugins return", and the ranking is the
// after-dispatch one, so the worst outcome wins and every plugin is called once.
#[tokio::test(start_paused = true)]
async fn the_entry_is_deleted_whatever_the_plugins_return_and_the_worst_outcome_wins() {
    type B = CallBehavior<SectionPluginError>;
    let rejected = || B::err(SectionPluginError::rejected("no", None));
    let denied = || B::err(SectionPluginError::not_permitted("needs more"));
    let internal = || B::err(SectionPluginError::internal("boom"));
    let cases: Vec<(&str, [B; 3], OutcomeCategory, [SectionOutcome; 3], bool)> = vec![
        (
            "a rejection in the middle",
            [B::ok(), rejected(), B::ok()],
            OutcomeCategory::PluginRejected,
            [S::Succeeded, S::Rejected, S::Succeeded],
            true,
        ),
        (
            "rejected and not permitted: not permitted wins",
            [rejected(), denied(), B::ok()],
            OutcomeCategory::NotPermitted,
            [S::Rejected, S::NotPermitted, S::Succeeded],
            true,
        ),
        (
            "an internal failure and a rejection: rejected wins",
            [internal(), rejected(), B::ok()],
            OutcomeCategory::PluginRejected,
            [S::InternalFailure, S::Rejected, S::Succeeded],
            true,
        ),
        (
            "only internal failures",
            [internal(), internal(), internal()],
            OutcomeCategory::InternalFailure,
            [S::InternalFailure; 3],
            false,
        ),
        (
            "a hung remove",
            [B::hang(), B::ok(), B::ok()],
            OutcomeCategory::InternalFailure,
            [S::TimedOut, S::Succeeded, S::Succeeded],
            true,
        ),
    ];

    for (name, behaviors, category, expected, partial) in cases {
        let gate = three().build();
        for (plugin, behavior) in gate.sections.iter().zip(behaviors) {
            plugin.on(SectionCall::Remove, behavior);
        }

        let report = bounded(gate.run_uninstall("app-1")).await;

        assert_eq!(report.category, category, "{name}");
        assert_eq!(outcomes(&report), expected, "{name}");
        assert_eq!(report.partial, partial, "{name}");
        assert_eq!(report.registry_effect, RegistryEffect::Deleted, "{name}");
        assert!(gate.memory.entries().is_empty(), "{name}: deleted anyway");
        for index in 0..3 {
            assert_eq!(removes(&gate, index), 1, "{name}: {index} removed once");
        }
    }
}

// DESIGN §4.5 and §3.6: if the delete fails or times out, removal still runs in priority order,
// the effect is `delete_failed`, and the request is an internal failure ranked by the
// after-dispatch order. One delete attempt, no retry.
#[tokio::test(start_paused = true)]
async fn a_failed_delete_still_removes_in_priority_order_and_is_an_internal_failure() {
    for error in [RegistryError::Unavailable, RegistryError::TimedOut] {
        let gate = three().build();
        gate.memory.fail(RegistryOp::Delete, error.clone());

        let report = gate.run_uninstall("app-1").await;

        assert_eq!(
            report.category,
            OutcomeCategory::InternalFailure,
            "{error:?}"
        );
        assert_eq!(
            report.registry_effect,
            RegistryEffect::DeleteFailed,
            "{error:?}"
        );
        assert_eq!(outcomes(&report), [S::Succeeded; 3], "{error:?}");
        assert!(
            !report.partial,
            "{error:?}: `partial` is about the sections"
        );
        assert_eq!(
            gate.memory.entries().len(),
            1,
            "{error:?}: the entry is still there"
        );
        for index in 0..3 {
            assert_eq!(removes(&gate, index), 1, "{error:?}: {index}");
        }
        let deletes = gate
            .registry_calls()
            .into_iter()
            .filter(|c| c.op == RegistryOp::Delete)
            .count();
        assert_eq!(deletes, 1, "{error:?}: no retry");
        let events = gate.events.events();
        let delete_at = events.iter().position(|e| e == "registry:delete:app-1");
        let remove_at = events.iter().position(|e| e == "section:files:remove");
        assert!(delete_at < remove_at, "{events:?}");
        let (_, content) = final_content(&gate);
        assert_eq!(content.registry_effect, RegistryEffect::DeleteFailed);
        assert_eq!(content.category, OutcomeCategory::InternalFailure);
    }
}

// "a section plugin's not permitted or rejected result still ranks higher".
#[tokio::test(start_paused = true)]
async fn a_plugin_refusal_outranks_a_failed_delete() {
    let cases = [
        (
            CallBehavior::err(SectionPluginError::not_permitted("no")),
            OutcomeCategory::NotPermitted,
        ),
        (
            CallBehavior::err(SectionPluginError::rejected("no", None)),
            OutcomeCategory::PluginRejected,
        ),
    ];
    for (behavior, category) in cases {
        let gate = three().build();
        gate.sections[2].on(SectionCall::Remove, behavior);
        gate.memory
            .fail(RegistryOp::Delete, RegistryError::Unavailable);

        let report = gate.run_uninstall("app-1").await;

        assert_eq!(report.category, category);
        assert_eq!(report.registry_effect, RegistryEffect::DeleteFailed);
    }
}

// DESIGN §4.8 Integration: "a registry double that commits ... a delete and then exceeds
// `registry_operation_timeout` gives `delete_failed`, and the listing then shows the committed
// state"; DESIGN §3.6: "If a delete that was not confirmed did commit, the repeated uninstall
// ends as not found."
#[tokio::test(start_paused = true)]
async fn a_delete_that_commits_and_times_out_is_delete_failed_and_a_repeat_is_not_found() {
    let gate = three().build();
    gate.memory
        .fail_after_commit(RegistryOp::Delete, RegistryError::TimedOut);

    let report = gate.run_uninstall("app-1").await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(report.registry_effect, RegistryEffect::DeleteFailed);
    assert!(
        gate.memory.entries().is_empty(),
        "the listing shows the delete"
    );
    gate.memory.heal(RegistryOp::Delete);

    let repeat = gate.run_uninstall("app-1").await;

    assert_eq!(repeat.category, OutcomeCategory::NotFound);
    assert_eq!(repeat.registry_effect, RegistryEffect::Unchanged);
    assert!(repeat.sections.is_empty());
    for index in 0..3 {
        assert_eq!(removes(&gate, index), 1, "no section call for the repeat");
    }
}

// DESIGN §4.5: the delete is bounded by `registry_operation_timeout`; a store that never answers
// ends the delete as not confirmed after exactly one bound, and then the removes run.
#[tokio::test(start_paused = true)]
async fn a_delete_that_never_answers_ends_after_the_registry_operation_timeout() {
    let (log, _guard) = capture_logs();
    let gate = three().build();
    gate.memory.hang(RegistryOp::Delete);
    let started = Instant::now();

    let report = bounded(gate.run_uninstall("app-1")).await;

    assert_eq!(report.category, OutcomeCategory::InternalFailure);
    assert_eq!(report.registry_effect, RegistryEffect::DeleteFailed);
    assert_eq!(outcomes(&report), [S::Succeeded; 3]);
    assert!(!report.partial);
    assert_eq!(started.elapsed(), REGISTRY_TIMEOUT);
    for index in 0..3 {
        assert_eq!(removes(&gate, index), 1);
    }
    let lines = log.lines_with("registry_write_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "op", "delete"), "{}", lines[0]);
    assert!(has_field(&lines[0], "reason", "timed_out"), "{}", lines[0]);
    assert_eq!(
        field_value(&lines[0], "request_id"),
        Some(request_content(&gate).request_id.to_string()),
        "{}",
        lines[0]
    );
}

// DESIGN §3.6: once the delete has succeeded, a repeated uninstall ends as not found with no
// section plugin call.
#[tokio::test(start_paused = true)]
async fn a_repeated_uninstall_is_not_found_and_calls_no_section_plugin() {
    let gate = three().build();
    gate.run_uninstall("app-1").await;

    let repeat = gate.run_uninstall("app-1").await;

    assert_eq!(repeat.category, OutcomeCategory::NotFound);
    assert!(repeat.sections.is_empty());
    for index in 0..3 {
        assert_eq!(removes(&gate, index), 1, "only the first uninstall removed");
    }
    let deletes = gate
        .registry_calls()
        .into_iter()
        .filter(|c| c.op == RegistryOp::Delete)
        .count();
    assert_eq!(deletes, 1);
}

// DESIGN §3.6: an empty key set gives a succeeded uninstall with no section plugin call.
#[tokio::test(start_paused = true)]
async fn an_uninstall_with_no_section_plugin_succeeds_after_the_delete() {
    let gate = GateBuilder::new().entry(stored("app-1", "3.1")).build();

    let report = gate.run_uninstall("app-1").await;

    assert_eq!(report.category, OutcomeCategory::Succeeded);
    assert!(report.sections.is_empty());
    assert_eq!(report.registry_effect, RegistryEffect::Deleted);
    assert!(gate.memory.entries().is_empty());
    assert_eq!(
        gate.registry_calls(),
        [
            reg(RegistryOp::Get, "app-1"),
            reg(RegistryOp::Delete, "app-1")
        ]
    );
}

// DESIGN §4.2: no coordination between requests, but one uninstall touches the registry with
// one read and one delete only, and nothing else.
#[tokio::test(start_paused = true)]
async fn the_registry_sees_one_read_and_one_delete_of_an_uninstall() {
    let gate = three().build();

    gate.run_uninstall("app-1").await;

    assert_eq!(
        gate.registry_calls(),
        [
            reg(RegistryOp::Get, "app-1"),
            reg(RegistryOp::Delete, "app-1")
        ]
    );
}

// A hung remove costs one plugin limit and the request goes on.
#[tokio::test(start_paused = true)]
async fn a_remove_that_never_answers_costs_one_plugin_limit() {
    let gate = three().build();
    gate.sections[1].on(SectionCall::Remove, CallBehavior::hang());
    let started = Instant::now();

    let report = bounded(gate.run_uninstall("app-1")).await;

    assert_eq!(outcomes(&report), [S::Succeeded, S::TimedOut, S::Succeeded]);
    assert_eq!(started.elapsed(), PLUGIN_TIMEOUT);
}

// DESIGN §3.3: a remove the guard did not send, or a guard-internal failure, is `not_sent` or
// `internal_failure` of that section (rank 3 of the after-dispatch order); the removes go on, the
// entry is deleted anyway, and a refusal still outranks them.
#[tokio::test(start_paused = true)]
async fn a_remove_the_guard_did_not_send_or_that_failed_in_the_guard_goes_on_and_ranks_as_internal()
{
    let cases = [
        ("not sent", PluginCallError::NotSent, S::NotSent),
        (
            "guard internal",
            PluginCallError::Internal("plugin call failed".to_owned()),
            S::InternalFailure,
        ),
    ];
    for (name, error, outcome) in cases {
        let gate = three()
            .wrap_section_port(1, move |inner| {
                Arc::new(ScriptedSectionPort::new(inner, SectionCall::Remove, error))
            })
            .build();

        let report = bounded(gate.run_uninstall("app-1")).await;

        assert_eq!(report.category, OutcomeCategory::InternalFailure, "{name}");
        assert_eq!(
            outcomes(&report),
            [S::Succeeded, outcome, S::Succeeded],
            "{name}"
        );
        assert!(report.partial, "{name}");
        assert_eq!(report.registry_effect, RegistryEffect::Deleted, "{name}");
        assert!(gate.memory.entries().is_empty(), "{name}");
        assert_eq!(removes(&gate, 0), 1, "{name}");
        assert_eq!(removes(&gate, 2), 1, "{name}: the dispatch went on");
        assert_eq!(
            removes(&gate, 1),
            0,
            "{name}: the plugin never saw the call"
        );
    }
}

// DESIGN §4.2: requests are not coordinated. Two uninstalls of one id racing end without a
// panic or a hang: the entry is gone, at least one ended as succeeded, and the other either
// also succeeded (both read the entry) or ended as not found (it read after the delete).
#[tokio::test(start_paused = true)]
async fn two_uninstalls_of_one_id_racing_both_end_and_the_entry_is_gone() {
    let gate = three().build();

    let (first, second) =
        bounded(async { tokio::join!(gate.run_uninstall("app-1"), gate.run_uninstall("app-1")) })
            .await;

    let categories = [first.category, second.category];
    assert!(
        categories.contains(&OutcomeCategory::Succeeded),
        "{categories:?}"
    );
    for category in categories {
        assert!(
            category == OutcomeCategory::Succeeded || category == OutcomeCategory::NotFound,
            "{categories:?}"
        );
    }
    assert!(gate.memory.entries().is_empty());
    assert_eq!(
        gate.audit.calls().len(),
        4,
        "both requests got both hand-offs"
    );
}
