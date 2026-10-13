//! Unit tests for the startup result and its holder (DESIGN §3.2 "Startup and
//! Registration" publish step and readiness, "Ports and Composition" startup port, §3.6
//! startup sequence, §4.8 "Startup": "not ready until publication, and not ready again
//! once `stop` sets the stopping flag").
//!
//! Planned API in `crate::domain::startup`:
//!
//! ```text
//! pub struct SectionPluginEntry {      // one resolved section plugin instance
//!     pub instance_id: String,         // full GTS instance id (the ClientHub scope id)
//!     pub priority: i16,
//!     pub key: String,                 // key read at startup, already validated
//!     pub plugin: Arc<dyn SectionPluginPort>,
//! }
//!
//! pub struct StartupResult;            // immutable; the section plugin set + audit sink
//! impl StartupResult {
//!     /// Keeps the order given: the startup component passes the (priority, id) order.
//!     pub fn new(section_plugins: Vec<SectionPluginEntry>, audit_instance_id: String,
//!                audit: Arc<dyn AuditSink>) -> Self;
//!     pub fn section_plugins(&self) -> &[SectionPluginEntry];
//!     pub fn section_keys(&self) -> Vec<&str>;      // in section_plugins order
//!     pub fn audit_instance_id(&self) -> &str;
//!     pub fn audit(&self) -> &Arc<dyn AuditSink>;
//! }
//!
//! #[derive(Debug, PartialEq, Eq)] pub struct AlreadyPublished;
//!
//! /// One-time-initialised holder, published by the startup component, read by the
//! /// orchestrator, the read service and the REST not-ready gate. `Default`.
//! pub struct StartupResultHolder;
//! impl StartupResultHolder {
//!     pub fn new() -> Self;
//!     /// Stores the result once and marks the holder published (ready unless stopping).
//!     pub fn publish_result(&self, r: StartupResult) -> Result<(), AlreadyPublished>;
//!     pub fn result(&self) -> Option<Arc<StartupResult>>;  // None until published
//! }
//! impl StartupPort for StartupResultHolder {
//!     // Readiness derives from the stored result: `is_ready` == result present && !stopping.
//!     // set_stopping(): sticky flag; the stored result stays readable.
//! }
//! ```
//!
//! Decision (user, F-007): the payload lives in this holder, and `publish()` is not on
//! the `StartupPort` trait at all (the port only reads and stops), so no flag-only
//! publication can make a holder ready without a result.
//!
//! `SectionPluginEntry` invariants (fields are public, the startup component is the only
//! producer): `instance_id` is the full GTS instance id of the plugin and the scope id
//! of its `ClientHub` client; `priority` is that instance's registered priority; `key`
//! passed the SDK key rule and is unique in the result; `plugin` is the adapter over
//! the client of `instance_id`. A result's entries are in (priority, instance id) order.
//!
//! Tests:
//! - `fresh_holder_is_not_ready_not_stopping_and_has_no_result` (§3.2 readiness)
//! - `publish_result_makes_the_holder_ready_and_exposes_the_result` (§3.2)
//! - `second_publish_result_is_refused_and_the_first_result_stays` (§3.2 "once", immutable)
//! - `default_holder_equals_new` / `result_is_the_same_arc_on_every_read`
//! - `stopping_makes_the_holder_not_ready_but_keeps_the_result_readable` (§4.8)
//! - `stopping_before_publication_stays_not_ready_after_publication` (§4.8)
//! - `stopping_is_sticky_and_publishing_does_not_clear_it`
//! - `concurrent_publish_result_lets_exactly_one_win` (§3.2 "once")
//! - `holder_coerces_to_arc_dyn_startup_port`
//! - `result_keeps_the_given_section_order_and_exposes_keys` (§3.6 order)
//! - `empty_section_set_is_a_valid_result` (§3.6 "empty plugin set")
//! - `result_exposes_the_audit_instance_and_sink`
//! - `result_ports_are_usable_through_the_holder` (the published ports work after reads)

use std::sync::{Arc, Barrier};

use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginError, CallerIdentity, FinalResultContent,
    RequestHandoffContent, SectionPluginError,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::domain::ports::{AuditSink, PluginCallError, SectionPluginPort, StartupPort};
use crate::domain::startup::{
    AlreadyPublished, SectionPluginEntry, StartupResult, StartupResultHolder,
};

struct NullSection(&'static str);

#[async_trait]
impl SectionPluginPort for NullSection {
    async fn key(&self) -> Result<String, PluginCallError<SectionPluginError>> {
        Ok(self.0.to_owned())
    }

    async fn schema(
        &self,
        _caller: &CallerIdentity,
    ) -> Result<Value, PluginCallError<SectionPluginError>> {
        Ok(json!({"type": "object"}))
    }

    async fn apply(
        &self,
        _caller: &CallerIdentity,
        _envelope: &ApplicationEnvelope,
        _section: Value,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        Ok(())
    }

    async fn remove(
        &self,
        _caller: &CallerIdentity,
        _envelope: &ApplicationEnvelope,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        Ok(())
    }
}

struct NullAudit;

#[async_trait]
impl AuditSink for NullAudit {
    async fn request_handoff(
        &self,
        _caller: &CallerIdentity,
        _content: &RequestHandoffContent,
    ) -> Result<String, PluginCallError<AuditPluginError>> {
        Ok("audit-1".to_owned())
    }

    async fn final_result_handoff(
        &self,
        _caller: &CallerIdentity,
        _audit_id: &str,
        _content: &FinalResultContent,
    ) -> Result<(), PluginCallError<AuditPluginError>> {
        Ok(())
    }
}

fn entry(instance: &str, priority: i16, key: &'static str) -> SectionPluginEntry {
    SectionPluginEntry {
        instance_id: instance.to_owned(),
        priority,
        key: key.to_owned(),
        plugin: Arc::new(NullSection(key)),
    }
}

fn result_with(sections: Vec<SectionPluginEntry>) -> StartupResult {
    StartupResult::new(sections, "audit.instance".to_owned(), Arc::new(NullAudit))
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

// DESIGN §3.2: not ready until the startup result is published.
#[test]
fn fresh_holder_is_not_ready_not_stopping_and_has_no_result() {
    let holder = StartupResultHolder::new();

    assert!(!holder.is_ready());
    assert!(!holder.is_stopping());
    assert!(holder.result().is_none());
}

// DESIGN §3.2: publishing the result makes the gear ready.
#[test]
fn publish_result_makes_the_holder_ready_and_exposes_the_result() {
    let holder = StartupResultHolder::new();

    holder
        .publish_result(result_with(vec![entry("a", 1, "alpha")]))
        .expect("first publication succeeds");

    assert!(holder.is_ready());
    assert!(!holder.is_stopping());
    let result = holder.result().expect("result is published");
    assert_eq!(result.section_plugins().len(), 1);
    assert_eq!(result.section_plugins()[0].instance_id, "a");
}

// DESIGN §3.2: the result is published once and is immutable afterwards.
#[test]
fn second_publish_result_is_refused_and_the_first_result_stays() {
    let holder = StartupResultHolder::new();
    holder
        .publish_result(result_with(vec![entry("a", 1, "alpha")]))
        .expect("first publication succeeds");

    let second = holder.publish_result(result_with(vec![entry("b", 2, "beta")]));

    assert_eq!(second, Err(AlreadyPublished));
    assert!(
        holder.is_ready(),
        "a refused publication does not unready it"
    );
    let result = holder.result().expect("result stays published");
    assert_eq!(result.section_keys(), vec!["alpha"]);
    assert_eq!(result.section_plugins()[0].instance_id, "a");
    assert_eq!(result.audit_instance_id(), "audit.instance");
}

// The holder derives readiness from its result and has no other way to become ready.
#[test]
fn default_holder_equals_new() {
    let holder = StartupResultHolder::default();

    assert!(!holder.is_ready());
    assert!(!holder.is_stopping());
    assert!(holder.result().is_none());
}

// Readers share one immutable result.
#[test]
fn result_is_the_same_arc_on_every_read() {
    let holder = StartupResultHolder::new();
    holder
        .publish_result(result_with(vec![entry("a", 1, "alpha")]))
        .expect("publication succeeds");

    let first = holder.result().expect("published");
    let second = holder.result().expect("published");

    assert!(Arc::ptr_eq(&first, &second));
}

// DESIGN §4.8: not ready again once `stop` sets the stopping flag; reads of the
// published result stay possible for requests that are already running.
#[test]
fn stopping_makes_the_holder_not_ready_but_keeps_the_result_readable() {
    let holder = StartupResultHolder::new();
    holder
        .publish_result(result_with(vec![entry("a", 1, "alpha")]))
        .expect("publication succeeds");

    holder.set_stopping();

    assert!(!holder.is_ready());
    assert!(holder.is_stopping());
    assert!(holder.result().is_some());
}

// DESIGN §4.8: stopping is not undone by a later publication.
#[test]
fn stopping_before_publication_stays_not_ready_after_publication() {
    let holder = StartupResultHolder::new();
    holder.set_stopping();

    holder
        .publish_result(result_with(vec![]))
        .expect("publication succeeds");

    assert!(!holder.is_ready());
    assert!(holder.is_stopping());
}

#[test]
fn stopping_is_sticky_and_publishing_does_not_clear_it() {
    let holder = StartupResultHolder::new();
    holder.set_stopping();
    holder.set_stopping();
    holder
        .publish_result(result_with(vec![]))
        .expect("publication succeeds");

    assert!(holder.is_stopping());
    assert!(!holder.is_ready());
}

// DESIGN §3.2: "publishes the startup result once", also under contention: exactly one
// thread wins and the stored result is the winner's own, not a mix.
#[test]
fn concurrent_publish_result_lets_exactly_one_win() {
    let holder = Arc::new(StartupResultHolder::new());
    let barrier = Arc::new(Barrier::new(8));
    let handles: Vec<_> = (0..8_u8)
        .map(|i| {
            let holder = Arc::clone(&holder);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                let key: &'static str = Box::leak(format!("key{i}").into_boxed_str());
                let won = holder
                    .publish_result(result_with(vec![entry(
                        &format!("inst{i}"),
                        i16::from(i),
                        key,
                    )]))
                    .is_ok();
                (i, won)
            })
        })
        .collect();

    let results: Vec<(u8, bool)> = handles
        .into_iter()
        .map(|h| h.join().expect("thread joins"))
        .collect();

    let winners: Vec<u8> = results
        .iter()
        .filter(|(_, won)| *won)
        .map(|(i, _)| *i)
        .collect();
    assert_eq!(winners.len(), 1);
    assert!(holder.is_ready());
    let stored = holder.result().expect("published");
    assert_eq!(stored.section_plugins().len(), 1);
    assert_eq!(
        stored.section_plugins()[0].instance_id,
        format!("inst{}", winners[0])
    );
    assert_eq!(stored.section_keys(), vec![format!("key{}", winners[0])]);
}

// DESIGN §3.2: domain components and the REST gate read the holder through the port.
#[test]
fn holder_coerces_to_arc_dyn_startup_port() {
    let holder = Arc::new(StartupResultHolder::new());
    let port: Arc<dyn StartupPort> = holder.clone();

    holder
        .publish_result(result_with(vec![]))
        .expect("publication succeeds");

    assert!(port.is_ready());
    holder.set_stopping();
    assert!(port.is_stopping());
}

// DESIGN §3.6: the section plugin set is held in dispatch order, as given.
#[test]
fn result_keeps_the_given_section_order_and_exposes_keys() {
    // Deliberately in neither priority order nor id order: a result that re-sorted its
    // input would show.
    let result = result_with(vec![
        entry("i-m", 7, "mu"),
        entry("i-b", -5, "beta"),
        entry("i-z", 1, "zeta"),
        entry("i-a", 3, "alpha"),
    ]);

    let ids: Vec<&str> = result
        .section_plugins()
        .iter()
        .map(|e| e.instance_id.as_str())
        .collect();
    assert_eq!(ids, ["i-m", "i-b", "i-z", "i-a"]);
    assert_eq!(result.section_keys(), vec!["mu", "beta", "zeta", "alpha"]);
    assert_eq!(result.section_plugins()[1].priority, -5);
}

// DESIGN §3.6: "If the types registry answers with no section plugin instances, the key
// set is empty." An empty set is a valid result and makes the gear ready.
#[test]
fn empty_section_set_is_a_valid_result() {
    let holder = StartupResultHolder::new();

    holder
        .publish_result(result_with(vec![]))
        .expect("an empty plugin set is valid");

    assert!(holder.is_ready());
    let result = holder.result().expect("published");
    assert!(result.section_plugins().is_empty());
    assert!(result.section_keys().is_empty());
}

// DESIGN §3.2: the result carries the audit sink (and its instance, for logs).
#[test]
fn result_exposes_the_audit_instance_and_sink() {
    let result = result_with(vec![]);

    assert_eq!(result.audit_instance_id(), "audit.instance");
    let _sink: &Arc<dyn AuditSink> = result.audit();
}

#[tokio::test]
async fn result_ports_are_usable_through_the_holder() {
    let holder = StartupResultHolder::new();
    holder
        .publish_result(result_with(vec![entry("a", 1, "alpha")]))
        .expect("publication succeeds");
    let result = holder.result().expect("published");

    let key = result.section_plugins()[0]
        .plugin
        .key()
        .await
        .expect("key answers");
    let handoff = result
        .audit()
        .final_result_handoff(
            &caller(),
            "audit-1",
            &FinalResultContent {
                audit_id: "audit-1".to_owned(),
                request_id: Uuid::from_u128(3),
                validation: None,
                sections: vec![],
                category: application_manager_sdk::models::OutcomeCategory::Succeeded,
                registry_effect: application_manager_sdk::models::RegistryEffect::Unchanged,
                completed_at: "2026-10-13T00:00:00Z".to_owned(),
            },
        )
        .await;

    assert_eq!(key, "alpha");
    assert_eq!(handoff, Ok(()));
}
