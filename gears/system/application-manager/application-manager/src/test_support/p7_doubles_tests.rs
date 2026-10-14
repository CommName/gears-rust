//! Tests of the P7 doubles and harness additions (DESIGN §4.8 "Test seams and data", "Request task
//! tests"): a double that a test relies on to inject a fault must itself be shown to inject it.
//!
//! Planned API in `test_support` (cfg(test) only):
//!
//! ```text
//! InMemoryRegistryStore::delay(op, Duration) / hang(op) / panic_on(op)   // heal(op) ends them
//! FakePolicy::with_delay(Duration);  PolicyScript::Panic
//! FakeTypesRegistry::registration_failing() / registration_rejecting(index)
//!     / registration_without_results() / registration_with_result_count(n)
//!     / registration_answering_other_ids() / registration_hanging()
//!     / registered() -> Vec<Vec<Value>> / registration_calls();  total_calls() counts them too
//! PanickingClock::new(inner, n) / after_event(inner, events, marker) / reads();
//! PanickingAuditSink::new(inner, AuditCall);  ScriptedAuditSink::new(inner, kind, err);
//! PanickingSectionPort::new(inner, SectionCall, nth);  ScriptedSectionPort::new(inner, kind, err)
//! GateBuilder::clock / policy_delay / wrap_audit_sink / wrap_section_port / events
//!     / request_ids_from
//! Gate::run_seed / run_uninstall / run / start / try_start
//! request_helpers: three_section_gate / three_section_body / stored / outcomes / keys
//!     / audit_kinds / request_content / final_content / has_final
//! ```
#![allow(clippy::duration_suboptimal_units)]

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use application_manager_sdk::models::{HandoffOperation, OutcomeCategory, RegistryEffect};
use application_manager_sdk::{CallerIdentity, FinalResultContent, RequestHandoffContent};
use futures::FutureExt as _;
use serde_json::json;
use tokio::time::Instant;
use types_registry_sdk::TypesRegistryClient;
use uuid::Uuid;

use crate::domain::orchestrator::RequestInput;
use crate::domain::ports::{
    AuditSink, Clock, PluginCallError, PolicyAction, PolicyPort, RegistryStore, SectionPluginPort,
};
use crate::test_support::{
    AuditCall, EventLog, FakePolicy, FakeTypesRegistry, FixedClock, GateBuilder,
    InMemoryRegistryStore, PanickingAuditSink, PanickingClock, PanickingSectionPort, PolicyScript,
    RegistryOp, ScriptedAuditSink, ScriptedSectionPort, SectionCall, audit_kinds, bounded,
    final_content, has_final, keys, outcomes, request_content, security_context, stored,
    three_section_body, three_section_gate,
};

fn caller() -> CallerIdentity {
    CallerIdentity {
        subject_id: Uuid::from_u128(1),
        subject_type: None,
        subject_tenant_id: Uuid::from_u128(2),
        token_scopes: vec![],
        request_id: Uuid::from_u128(3),
    }
}

// ---- the registry store ----------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_delayed_registry_operation_waits_on_the_tokio_clock_and_then_goes_on() {
    let store = InMemoryRegistryStore::new().with_entry(stored("a", "1"));
    store.delay(RegistryOp::Get, Duration::from_millis(300));
    let started = Instant::now();

    let found = store.get("a").await.expect("answers");

    assert_eq!(found.map(|e| e.version().to_owned()), Some("1".to_owned()));
    assert_eq!(started.elapsed(), Duration::from_millis(300));
    let started = Instant::now();
    store
        .delete("a")
        .await
        .expect("other operations are not delayed");
    assert_eq!(started.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn a_hanging_registry_operation_records_the_call_and_never_answers() {
    let store = InMemoryRegistryStore::new().with_entry(stored("a", "1"));
    store.hang(RegistryOp::Upsert);

    let waited =
        tokio::time::timeout(Duration::from_secs(3600), store.upsert(&stored("a", "2"))).await;

    assert!(waited.is_err(), "never answers");
    assert_eq!(store.calls().len(), 1, "but the call is recorded");
    assert_eq!(store.entries()[0].version(), "1", "and nothing was changed");
}

#[tokio::test]
async fn a_panicking_registry_operation_panics_after_the_call_is_recorded_and_changes_nothing() {
    let store = InMemoryRegistryStore::new().with_entry(stored("a", "1"));
    store.panic_on(RegistryOp::Delete);

    let result = AssertUnwindSafe(store.delete("a")).catch_unwind().await;

    assert!(result.is_err(), "the operation panicked");
    assert_eq!(store.calls().len(), 1);
    assert_eq!(store.entries().len(), 1, "nothing was deleted");
}

#[tokio::test(start_paused = true)]
async fn heal_ends_a_delay_a_hang_and_a_panic() {
    let store = InMemoryRegistryStore::new();
    store.hang(RegistryOp::Get);
    store.heal(RegistryOp::Get);
    store.panic_on(RegistryOp::List);
    store.heal(RegistryOp::List);

    assert!(store.get("x").await.expect("answers").is_none());
    assert!(store.list().await.expect("answers").is_empty());
}

// ---- the policy ------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn a_delayed_policy_answers_after_the_delay_and_records_the_call_first() {
    let policy = FakePolicy::new(PolicyScript::Permit).with_delay(Duration::from_millis(200));
    let started = Instant::now();

    let decision = policy
        .decide(&security_context(), PolicyAction::Seed, Some("a"))
        .await;

    assert_eq!(decision, crate::domain::ports::PolicyDecision::Permit);
    assert_eq!(started.elapsed(), Duration::from_millis(200));
    assert_eq!(policy.calls().len(), 1);
}

#[tokio::test]
async fn a_panicking_policy_panics_after_it_recorded_the_call() {
    let policy = FakePolicy::new(PolicyScript::Panic);

    let result = AssertUnwindSafe(policy.decide(&security_context(), PolicyAction::Seed, None))
        .catch_unwind()
        .await;

    assert!(result.is_err());
    assert_eq!(policy.calls().len(), 1);
}

// ---- the types registry -----------------------------------------------------------------------

#[tokio::test]
async fn the_fake_registry_accepts_registrations_by_default_and_records_the_batches() {
    let registry = FakeTypesRegistry::new();
    let schemas = vec![
        json!({"$id": "gts://gts.a.b.c.d.v1~"}),
        json!({"$id": "gts://gts.e.f.g.h.v1~"}),
    ];

    let results = registry
        .register_type_schemas(schemas.clone())
        .await
        .expect("accepted");

    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(types_registry_sdk::RegisterResult::is_ok)
    );
    assert_eq!(registry.registered(), vec![schemas]);
    assert_eq!(registry.registration_calls(), 1);
    assert_eq!(registry.list_calls(), 0);
    assert_eq!(registry.total_calls(), 1, "a registration counts as a call");
}

#[tokio::test]
async fn the_fake_registry_scripts_each_way_a_registration_fails() {
    let schemas = vec![
        json!({"$id": "gts://gts.a.b.c.d.v1~"}),
        json!({"$id": "gts://gts.e.f.g.h.v1~"}),
    ];

    let failing = FakeTypesRegistry::new().registration_failing();
    assert!(
        failing
            .register_type_schemas(schemas.clone())
            .await
            .is_err()
    );

    let rejecting = FakeTypesRegistry::new().registration_rejecting(1);
    let results = rejecting
        .register_type_schemas(schemas.clone())
        .await
        .expect("the call itself succeeds");
    assert!(results[0].is_ok() && results[1].is_err());

    let empty = FakeTypesRegistry::new().registration_without_results();
    assert!(
        empty
            .register_type_schemas(schemas.clone())
            .await
            .expect("ok")
            .is_empty()
    );

    for count in [1, 3] {
        let counted = FakeTypesRegistry::new().registration_with_result_count(count);
        let results = counted
            .register_type_schemas(schemas.clone())
            .await
            .expect("ok");
        assert_eq!(results.len(), count);
        assert!(
            results
                .iter()
                .all(types_registry_sdk::RegisterResult::is_ok)
        );
    }

    let other = FakeTypesRegistry::new().registration_answering_other_ids();
    let results = other.register_type_schemas(schemas).await.expect("ok");
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|r| matches!(
        r,
        types_registry_sdk::RegisterResult::Ok { gts_id } if gts_id == "gts.fake.other.type.v1~"
    )));
}

#[tokio::test(start_paused = true)]
async fn a_hanging_fake_registration_never_answers_and_is_still_recorded() {
    let registry = FakeTypesRegistry::new().registration_hanging();

    let waited = tokio::time::timeout(
        Duration::from_secs(3600),
        registry.register_type_schemas(vec![json!({"$id": "gts://gts.a.b.c.d.v1~"})]),
    )
    .await;

    assert!(waited.is_err());
    assert_eq!(registry.registration_calls(), 1);
}

// ---- the panicking wrappers -------------------------------------------------------------------

#[test]
fn the_panicking_clock_panics_on_its_nth_reading_only() {
    let clock = PanickingClock::new(Arc::new(FixedClock::new(UNIX_EPOCH)), 2);

    assert_eq!(clock.now(), UNIX_EPOCH);
    assert!(std::panic::catch_unwind(AssertUnwindSafe(|| clock.now())).is_err());
    assert_eq!(clock.now(), UNIX_EPOCH, "later readings are normal");
    assert_eq!(clock.reads(), 3);
}

#[test]
fn the_panicking_clock_after_an_event_panics_once_on_the_first_reading_after_it() {
    let events = EventLog::new();
    let clock = PanickingClock::after_event(
        Arc::new(FixedClock::new(UNIX_EPOCH)),
        Arc::clone(&events),
        "marker",
    );

    assert_eq!(clock.now(), UNIX_EPOCH, "before the event");
    events.record("other");
    assert_eq!(clock.now(), UNIX_EPOCH, "another event does not count");
    events.record("marker");
    assert!(std::panic::catch_unwind(AssertUnwindSafe(|| clock.now())).is_err());
    assert_eq!(clock.now(), UNIX_EPOCH, "it panics once");
    assert_eq!(clock.reads(), 4);
}

// The wrapper is called directly: through a full request a panic of the task is absorbed by the
// supervisor, which would hide whether the double panics at all.
#[tokio::test(start_paused = true)]
async fn the_panicking_audit_sink_lets_the_plugin_count_the_call_then_panics_for_its_kind_only() {
    let gate = three_section_gate().build();
    let inner: Arc<dyn AuditSink> = Arc::clone(gate.holder.result().expect("published").audit());
    let sink = PanickingAuditSink::new(inner, AuditCall::FinalResultHandoff);
    let content = RequestHandoffContent {
        operation: HandoffOperation::Seed,
        request_id: Uuid::from_u128(3),
        received_at: crate::test_support::RECEIVED_AT.to_owned(),
        application: None,
        manifest: None,
        decided_category: None,
        id_length_bytes: None,
        id_sha256: None,
    };

    let audit_id = sink
        .request_handoff(&caller(), &content)
        .await
        .expect("the request hand-off kind does not panic");
    let result = FinalResultContent {
        audit_id: audit_id.clone(),
        request_id: Uuid::from_u128(3),
        validation: None,
        sections: Vec::new(),
        category: OutcomeCategory::Succeeded,
        registry_effect: RegistryEffect::Unchanged,
        completed_at: crate::test_support::RECEIVED_AT.to_owned(),
    };
    let panicked = AssertUnwindSafe(sink.final_result_handoff(&caller(), &audit_id, &result))
        .catch_unwind()
        .await;

    assert!(panicked.is_err(), "the final-result hand-off panics");
    assert_eq!(
        audit_kinds(&gate),
        [AuditCall::RequestHandoff, AuditCall::FinalResultHandoff],
        "the plugin counted the final hand-off the sink then panicked on"
    );
}

#[tokio::test(start_paused = true)]
async fn the_scripted_audit_sink_fails_its_kind_without_calling_the_plugin() {
    let gate = three_section_gate()
        .wrap_audit_sink(|sink| {
            Arc::new(ScriptedAuditSink::new(
                sink,
                AuditCall::FinalResultHandoff,
                PluginCallError::NotSent,
            ))
        })
        .build();

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(
        report.category,
        OutcomeCategory::Succeeded,
        "the result is unchanged"
    );
    assert_eq!(
        audit_kinds(&gate),
        [AuditCall::RequestHandoff],
        "the plugin never saw the final-result hand-off"
    );
}

#[tokio::test(start_paused = true)]
async fn the_panicking_section_port_panics_at_the_entry_of_the_nth_call_of_its_kind() {
    let gate = three_section_gate().build();
    let inner: Arc<dyn SectionPluginPort> = {
        let started = gate.holder.result().expect("published");
        Arc::clone(&started.section_plugins()[0].plugin)
    };
    let port = PanickingSectionPort::new(inner, SectionCall::Remove, 2);
    let envelope = application_manager_sdk::ApplicationEnvelope {
        id: "a".to_owned(),
        version: "1".to_owned(),
    };

    port.remove(&caller(), &envelope)
        .await
        .expect("the first call goes through");
    let second = AssertUnwindSafe(port.remove(&caller(), &envelope))
        .catch_unwind()
        .await;

    assert!(second.is_err(), "the second call panics");
    assert_eq!(
        gate.sections[0].call_count(),
        1,
        "before reaching the plugin"
    );
    port.apply(&caller(), &envelope, json!({}))
        .await
        .expect("other kinds are not affected");
}

#[tokio::test(start_paused = true)]
async fn the_scripted_section_port_answers_its_kind_without_calling_the_plugin() {
    let gate = three_section_gate().build();
    let inner: Arc<dyn SectionPluginPort> = {
        let started = gate.holder.result().expect("published");
        Arc::clone(&started.section_plugins()[0].plugin)
    };
    let port = ScriptedSectionPort::new(inner, SectionCall::Apply, PluginCallError::NotSent);
    let envelope = application_manager_sdk::ApplicationEnvelope {
        id: "a".to_owned(),
        version: "1".to_owned(),
    };

    let applied = port.apply(&caller(), &envelope, json!({})).await;
    let removed = port.remove(&caller(), &envelope).await;

    assert_eq!(applied, Err(PluginCallError::NotSent));
    assert_eq!(removed, Ok(()));
    assert_eq!(
        gate.sections[0].call_count(),
        1,
        "only the remove reached the plugin"
    );
}

// ---- the harness ------------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn the_harness_runs_a_full_seed_and_exposes_what_each_port_saw() {
    let gate = three_section_gate().build();

    let report = gate.run_seed(three_section_body()).await;

    assert_eq!(report.category, OutcomeCategory::Succeeded);
    assert_eq!(report.registry_effect, RegistryEffect::Updated);
    assert_eq!(keys(&report), ["files", "db", "net"]);
    assert_eq!(outcomes(&report).len(), 3);
    assert!(has_final(&gate));
    assert_eq!(request_content(&gate).operation, HandoffOperation::Seed);
    assert_eq!(final_content(&gate).0, "audit-1");
    assert_eq!(gate.orchestrator.in_flight_request_tasks(), 0);
}

#[tokio::test(start_paused = true)]
async fn the_harness_cap_refuses_the_request_past_max_request_tasks() {
    let gate = GateBuilder::new()
        .policy_delay(Duration::from_millis(10))
        .settings(|s| s.max_request_tasks = 1)
        .build();
    let input = || RequestInput::Uninstall {
        application_id: "a".to_owned(),
    };

    let first = gate.try_start(input()).expect("a free permit");
    let second = gate.try_start(input());

    assert!(second.is_err());
    bounded(first).await.expect("joins");
    assert!(gate.try_start(input()).is_ok());
}

#[tokio::test(start_paused = true)]
async fn the_harness_clock_and_policy_delay_are_the_ones_the_builder_was_given() {
    let clock = Arc::new(FixedClock::new(
        UNIX_EPOCH + Duration::from_secs(1_900_000_000),
    ));
    let gate = three_section_gate()
        .clock(clock as _)
        .policy_delay(Duration::from_millis(40))
        .entry(stored("app-1", "3.1"))
        .build();
    let started = Instant::now();

    gate.run_uninstall("app-1").await;

    assert_eq!(started.elapsed(), Duration::from_millis(40));
    assert_ne!(
        request_content(&gate).received_at,
        crate::test_support::RECEIVED_AT
    );
}

#[tokio::test(start_paused = true)]
async fn the_wrapped_audit_sink_and_section_port_replace_the_guarded_ones_in_the_result() {
    let wrapped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gate = {
        let audit_hits = Arc::clone(&wrapped);
        let section_hits = Arc::clone(&wrapped);
        three_section_gate()
            .wrap_audit_sink(move |sink: Arc<dyn AuditSink>| {
                audit_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                sink
            })
            .wrap_section_port(2, move |port: Arc<dyn SectionPluginPort>| {
                section_hits.fetch_add(10, std::sync::atomic::Ordering::SeqCst);
                port
            })
            .build()
    };

    assert_eq!(
        wrapped.load(std::sync::atomic::Ordering::SeqCst),
        11,
        "each wrap is applied once, to the audit sink and to the third section port"
    );
    drop(gate);
}

// A wrap that no port consumes would silently turn a fault-injection test into a clean run.
#[tokio::test]
#[should_panic(expected = "names a section the gate does not have")]
async fn a_section_wrap_for_a_missing_section_is_not_silently_ignored() {
    let _gate = three_section_gate()
        .wrap_section_port(7, |port: Arc<dyn SectionPluginPort>| port)
        .build();
}

#[tokio::test]
#[should_panic(expected = "a wrap on an unpublished gate would be ignored")]
async fn a_wrap_on_an_unpublished_gate_is_not_silently_ignored() {
    let _gate = three_section_gate()
        .unpublished()
        .wrap_audit_sink(|sink: Arc<dyn AuditSink>| sink)
        .build();
}
