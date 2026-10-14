//! Tests of the P6 registry and policy fakes (DESIGN §3.2 "Ports and Composition" test
//! doubles: "an in-memory store for domain tests"; §4.8 "A registry double that never answers",
//! "a test PDP that permits, refuses, cannot answer").
//!
//! Planned API in `test_support/fakes.rs`:
//!
//! ```text
//! EventLog::new() -> Arc<EventLog>;  record(&self, impl Into<String>);  events() -> Vec<String>
//! enum RegistryOp { Get, Upsert, Delete, List }
//! struct RegistryCall { op: RegistryOp, id: Option<String> }
//! InMemoryRegistryStore::new() / with_event_log / with_entry / fail(op, err)
//!                        / fail_after_commit(op, err) / heal(op)
//!                        / calls() / entries()          // impl RegistryStore
//! NeverAnsweringRegistry::new() / with_event_log / calls()   // impl RegistryStore, never answers
//! enum PolicyScript { Permit, Refuse, Unavailable(UnavailableReason) }
//! FakePolicy::new(script) / with_event_log / set(script) / calls() -> Vec<PolicyCall>   // PolicyPort
//! FakeSectionPlugin::with_event_log / set_schema(Value);  FakeAuditPlugin::with_event_log
//! ```
//!
//! The in-memory store must pass the SAME portable registry cases the SQL lanes run
//! (`registry_cases::portable`, written against `&dyn RegistryStore`); a store that breaks the
//! exact-text rule must fail them (the cases discriminate).
#![allow(clippy::duration_suboptimal_units)]

use std::time::{Duration, UNIX_EPOCH};

use application_manager_sdk::models::{OutcomeCategory, RegistryEffect};
use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginClientV1, CallerIdentity, FinalResultContent,
    SectionPluginClientV1,
};
use async_trait::async_trait;
use serde_json::json;
use uuid::Uuid;

use crate::domain::ports::{
    PdpUnavailable, PolicyAction, PolicyDecision, PolicyPort, RegistryError, RegistryStore,
};
use crate::domain::registry::{LastResult, RegistryEntry};
use crate::test_support::registry_cases;
use crate::test_support::{
    EventLog, FakeAuditPlugin, FakePolicy, FakeSectionPlugin, InMemoryRegistryStore,
    NeverAnsweringRegistry, PolicyScript, RegistryCall, RegistryOp, security_context,
};

fn entry(id: &str, version: &str) -> RegistryEntry {
    RegistryEntry::new(
        id,
        version,
        LastResult::Succeeded,
        UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        "audit-1",
    )
    .expect("a storable entry")
}

fn caller() -> CallerIdentity {
    CallerIdentity {
        subject_id: Uuid::from_u128(1),
        subject_type: None,
        subject_tenant_id: Uuid::from_u128(2),
        token_scopes: vec![],
        request_id: Uuid::from_u128(3),
    }
}

// Runs every portable case on its own store, in its own task so a failing case is named.
async fn run_portable_cases(mut make: impl FnMut() -> Box<dyn RegistryStore>) -> Vec<String> {
    let mut failures = Vec::new();
    for (name, case) in registry_cases::portable::all() {
        let store: std::sync::Arc<dyn RegistryStore> = make().into();
        let task = tokio::spawn(async move { case(&*store).await });
        if let Err(e) = task.await {
            failures.push(format!("{name}: {e}"));
        }
    }
    failures
}

#[tokio::test]
async fn the_in_memory_store_passes_every_portable_registry_case() {
    let failures = run_portable_cases(|| Box::new(InMemoryRegistryStore::new())).await;

    assert!(failures.is_empty(), "portable cases failed: {failures:?}");
}

// A store that folds ids to lower case: the portable cases must notice (DESIGN §3.7 exact
// comparison on every backend).
struct CaseFolding(InMemoryRegistryStore);

#[async_trait]
impl RegistryStore for CaseFolding {
    async fn get(&self, id: &str) -> Result<Option<RegistryEntry>, RegistryError> {
        self.0.get(&id.to_lowercase()).await
    }
    async fn upsert(&self, e: &RegistryEntry) -> Result<(), RegistryError> {
        let folded = RegistryEntry::new(
            e.application_id().to_lowercase(),
            e.version(),
            e.last_result(),
            e.last_seeded_at(),
            e.audit_id(),
        )
        .expect("storable");
        self.0.upsert(&folded).await
    }
    async fn delete(&self, id: &str) -> Result<(), RegistryError> {
        self.0.delete(&id.to_lowercase()).await
    }
    async fn list(&self) -> Result<Vec<RegistryEntry>, RegistryError> {
        self.0.list().await
    }
}

#[tokio::test]
async fn the_portable_cases_fail_a_store_that_folds_case() {
    let failures = run_portable_cases(|| Box::new(CaseFolding(InMemoryRegistryStore::new()))).await;

    assert!(
        failures
            .iter()
            .any(|f| f.starts_with("ids_differing_by_case_space_or_normalization_stay_distinct")),
        "the case-sensitivity case must fail: {failures:?}"
    );
}

// A store whose upsert silently does nothing.
struct DropsWrites;

#[async_trait]
impl RegistryStore for DropsWrites {
    async fn get(&self, _id: &str) -> Result<Option<RegistryEntry>, RegistryError> {
        Ok(None)
    }
    async fn upsert(&self, _e: &RegistryEntry) -> Result<(), RegistryError> {
        Ok(())
    }
    async fn delete(&self, _id: &str) -> Result<(), RegistryError> {
        Ok(())
    }
    async fn list(&self) -> Result<Vec<RegistryEntry>, RegistryError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn the_portable_cases_fail_a_store_that_drops_writes() {
    let failures = run_portable_cases(|| Box::new(DropsWrites)).await;

    assert!(
        failures.len() >= 10,
        "most cases must fail a store that stores nothing: {} failed",
        failures.len()
    );
}

#[tokio::test]
async fn the_in_memory_store_records_every_call_in_order_with_the_id() {
    let store = InMemoryRegistryStore::new();
    let e = entry("app", "1");

    store.get("a").await.expect("get");
    store.upsert(&e).await.expect("upsert");
    store.delete("b").await.expect("delete");
    store.list().await.expect("list");

    let call = |op, id: Option<&str>| RegistryCall {
        op,
        id: id.map(str::to_owned),
    };
    assert_eq!(
        store.calls(),
        vec![
            call(RegistryOp::Get, Some("a")),
            call(RegistryOp::Upsert, Some("app")),
            call(RegistryOp::Delete, Some("b")),
            call(RegistryOp::List, None),
        ]
    );
}

#[tokio::test]
async fn a_scripted_failure_fails_only_its_operation_leaves_data_alone_and_heals() {
    let store = InMemoryRegistryStore::new().with_entry(entry("kept", "1"));
    store.fail(RegistryOp::Get, RegistryError::Unavailable);
    store.fail(RegistryOp::Upsert, RegistryError::TimedOut);

    assert_eq!(store.get("kept").await, Err(RegistryError::Unavailable));
    assert_eq!(
        store.upsert(&entry("new", "1")).await,
        Err(RegistryError::TimedOut)
    );
    assert_eq!(store.list().await.expect("list is not failed").len(), 1);
    assert_eq!(store.entries().len(), 1, "a failed upsert stored nothing");
    assert_eq!(
        store.calls().len(),
        3,
        "get, upsert and list; failed calls are recorded too"
    );

    store.heal(RegistryOp::Get);
    assert!(store.get("kept").await.expect("healed").is_some());
    assert_eq!(
        store.upsert(&entry("new", "1")).await,
        Err(RegistryError::TimedOut),
        "healing one operation does not heal another"
    );
}

#[tokio::test]
async fn with_entry_seeds_without_recording_a_call() {
    let store = InMemoryRegistryStore::new().with_entry(entry("seeded", "7"));

    assert!(store.calls().is_empty());
    assert_eq!(
        store
            .get("seeded")
            .await
            .expect("get")
            .expect("present")
            .version(),
        "7"
    );
}

#[tokio::test(start_paused = true)]
async fn the_never_answering_registry_records_the_call_and_never_answers() {
    let registry = NeverAnsweringRegistry::new();

    let answered = tokio::time::timeout(Duration::from_hours(1), registry.get("app")).await;
    assert!(answered.is_err(), "no answer within an hour of paused time");
    assert!(
        tokio::time::timeout(Duration::from_hours(1), registry.upsert(&entry("a", "1")))
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_hours(1), registry.delete("a"))
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_hours(1), registry.list())
            .await
            .is_err()
    );

    assert_eq!(
        registry.calls().iter().map(|c| c.op).collect::<Vec<_>>(),
        vec![
            RegistryOp::Get,
            RegistryOp::Upsert,
            RegistryOp::Delete,
            RegistryOp::List
        ]
    );
    assert_eq!(registry.calls()[0].id.as_deref(), Some("app"));
}

#[tokio::test]
async fn the_fake_policy_answers_its_script_records_calls_and_can_be_rescripted() {
    let policy = FakePolicy::new(PolicyScript::Permit);

    assert_eq!(
        policy
            .decide(&security_context(), PolicyAction::Seed, Some("app"))
            .await,
        PolicyDecision::Permit
    );
    policy.set(PolicyScript::Refuse);
    assert_eq!(
        policy
            .decide(&security_context(), PolicyAction::Uninstall, None)
            .await,
        PolicyDecision::Refused
    );
    policy.set(PolicyScript::Unavailable(PdpUnavailable::TimedOut));
    assert_eq!(
        policy
            .decide(&security_context(), PolicyAction::List, None)
            .await,
        PolicyDecision::Unavailable(PdpUnavailable::TimedOut)
    );
    policy.set(PolicyScript::Unavailable(PdpUnavailable::Unreachable));
    assert_eq!(
        policy
            .decide(&security_context(), PolicyAction::List, None)
            .await,
        PolicyDecision::Unavailable(PdpUnavailable::Unreachable)
    );

    let calls = policy.calls();
    assert_eq!(calls.len(), 4);
    assert_eq!(calls[0].action, PolicyAction::Seed);
    assert_eq!(calls[0].application_id.as_deref(), Some("app"));
    assert_eq!(calls[1].application_id, None);
    assert_eq!(calls[0].ctx.subject_id(), security_context().subject_id());
}

#[tokio::test]
async fn the_event_log_orders_calls_across_every_fake() {
    let log = EventLog::new();
    let policy = FakePolicy::new(PolicyScript::Permit).with_event_log(log.clone());
    let registry = InMemoryRegistryStore::new().with_event_log(log.clone());
    let section = FakeSectionPlugin::new("files").with_event_log(log.clone());
    let audit = FakeAuditPlugin::new().with_event_log(log.clone());
    let c = caller();

    policy
        .decide(&security_context(), PolicyAction::Uninstall, Some("x"))
        .await;
    registry.get("x").await.expect("get");
    registry.list().await.expect("list");
    audit
        .request_handoff(
            &c,
            &application_manager_sdk::RequestHandoffContent {
                operation: application_manager_sdk::models::HandoffOperation::Seed,
                request_id: c.request_id,
                received_at: "t".to_owned(),
                application: None,
                manifest: None,
                decided_category: None,
                id_length_bytes: None,
                id_sha256: None,
            },
        )
        .await
        .expect("handoff");
    section.schema(&c).await.expect("schema");

    assert_eq!(
        log.events(),
        vec![
            "pdp:uninstall",
            "registry:get:x",
            "registry:list:",
            "audit:request_handoff",
            "section:files:schema",
        ]
    );
}

#[tokio::test]
async fn a_section_fake_serves_the_schema_it_was_last_given() {
    let plugin = FakeSectionPlugin::new("files").with_schema(json!({"type": "string"}));
    let c = caller();

    assert_eq!(
        plugin.schema(&c).await.expect("schema"),
        json!({"type": "string"})
    );
    plugin.set_schema(json!({"type": "integer"}));
    assert_eq!(
        plugin.schema(&c).await.expect("schema"),
        json!({"type": "integer"})
    );
}

// DESIGN §3.2 invariant "no `apply` or `remove` before an accepted request hand-off": the gate
// tests judge dispatch by these labels, so each label is pinned here, one call at a time.
#[tokio::test]
async fn every_section_and_audit_call_records_its_exact_label() {
    let log = EventLog::new();
    let section = FakeSectionPlugin::new("files").with_event_log(log.clone());
    let audit = FakeAuditPlugin::new().with_event_log(log.clone());
    let c = caller();
    let envelope = ApplicationEnvelope {
        id: "app".to_owned(),
        version: "1".to_owned(),
    };
    let handoff = application_manager_sdk::RequestHandoffContent {
        operation: application_manager_sdk::models::HandoffOperation::Uninstall,
        request_id: c.request_id,
        received_at: "t".to_owned(),
        application: None,
        manifest: None,
        decided_category: None,
        id_length_bytes: None,
        id_sha256: None,
    };
    let final_result = FinalResultContent {
        audit_id: "audit-1".to_owned(),
        request_id: c.request_id,
        validation: None,
        sections: Vec::new(),
        category: OutcomeCategory::Succeeded,
        registry_effect: RegistryEffect::Unchanged,
        completed_at: "t".to_owned(),
    };
    let labels = |log: &EventLog| log.events();

    section.key().await.expect("key");
    assert_eq!(labels(&log), ["section:files:key"]);
    section.schema(&c).await.expect("schema");
    assert_eq!(labels(&log)[1], "section:files:schema");
    section
        .apply(&c, &envelope, json!({}))
        .await
        .expect("apply");
    assert_eq!(labels(&log)[2], "section:files:apply");
    section.remove(&c, &envelope).await.expect("remove");
    assert_eq!(labels(&log)[3], "section:files:remove");
    audit.request_handoff(&c, &handoff).await.expect("handoff");
    assert_eq!(labels(&log)[4], "audit:request_handoff");
    audit
        .final_result_handoff(&c, "audit-1", &final_result)
        .await
        .expect("final");
    assert_eq!(labels(&log)[5], "audit:final_result_handoff");
    assert_eq!(labels(&log).len(), 6);
}

#[tokio::test]
async fn a_failure_after_the_commit_applies_the_change_and_still_fails() {
    let store = InMemoryRegistryStore::new().with_entry(entry("old", "1"));
    store.fail_after_commit(RegistryOp::Upsert, RegistryError::TimedOut);
    store.fail_after_commit(RegistryOp::Delete, RegistryError::Unavailable);

    assert_eq!(
        store.upsert(&entry("new", "2")).await,
        Err(RegistryError::TimedOut)
    );
    assert_eq!(store.delete("old").await, Err(RegistryError::Unavailable));

    let ids: Vec<String> = store
        .entries()
        .iter()
        .map(|e| e.application_id().to_owned())
        .collect();
    assert_eq!(ids, ["new"], "the upsert and the delete were both applied");
    store.heal(RegistryOp::Upsert);
    store
        .upsert(&entry("again", "1"))
        .await
        .expect("healed: the commit-then-fail script ended");
    assert_eq!(
        store.delete("again").await,
        Err(RegistryError::Unavailable),
        "healing one operation does not heal another"
    );
}
