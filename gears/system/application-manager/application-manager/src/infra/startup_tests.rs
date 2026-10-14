//! Unit tests for the Startup and Registration component (DESIGN §3.2 "Startup and
//! Registration" and "Plugin Call Guard" `drain`, §3.4, §3.6 "Startup and Plugin
//! Discovery", §4.6 "Startup failure catalogue", §4.7, §4.8 "Startup" row and "Clock",
//! §4.12 phasing, ADR 0001 fail at once without retry, ADR 0007 no tenant).
//!
//! The startup harness runs the component against a fake types registry
//! (`FakeTypesRegistry`), a real `ClientHub` holding fake scoped plugin clients, and
//! the real Plugin Call Guard, on a paused tokio clock: tests assert the number of
//! elapsed time limits, never wall time. Log events are captured with the shared
//! per-thread capture of `test_support` (current-thread runtime tests only); every
//! negative log assertion is preceded by a positive control (`assert_live`).
//!
//! Planned API in `crate::infra::startup`:
//!
//! ```text
//! /// The hand-implemented start capability (`impl RunnableCapability`).
//! pub struct StartupComponent;
//! impl StartupComponent {
//!     /// Builds the Plugin Call Guard from `plugin_call_timeout` / `plugin_max_in_flight`.
//!     /// Nothing is resolved from `hub` until `start` (so `init` works on an empty hub).
//!     pub fn new(config: &ApplicationManagerConfig, hub: Arc<ClientHub>,
//!                holder: Arc<StartupResultHolder>) -> Self;
//!     pub fn guard(&self) -> &PluginCallGuard;           // the one guard: startup, the published
//!                                                        // ports and `stop`'s drain all use it
//! }
//! #[async_trait] impl RunnableCapability for StartupComponent {
//!     /// Runs the §3.2 steps in order, once each, no retry; returns once the result is
//!     /// published or a step failed. The error's top-level text starts with the §4.6
//!     /// reason token (`{reason}: ...`), like `init`'s `invalid_setting`.
//!     /// `cancel` is ignored: startup is bounded by `plugin_call_timeout` alone and
//!     /// detached plugin calls are never cancelled (§3.2, §4.10).
//!     async fn start(&self, cancel: CancellationToken) -> anyhow::Result<()>;
//!     /// Sets the stopping flag, then drains the call guard with the `deadline_token`
//!     /// it received (the same token, not a derived or own deadline). Always `Ok`.
//!     async fn stop(&self, deadline_token: CancellationToken) -> anyhow::Result<()>;
//! }
//!
//! /// Readiness healthcheck: Unhealthy until the result is published and again once
//! /// stopping, Healthy otherwise. Never Degraded. Reads only the holder.
//! pub struct StartupHealthcheck;   // impl toolkit::Healthcheck
//! impl StartupHealthcheck { pub fn new(holder: Arc<dyn StartupPort>) -> Self; }
//! ```
//!
//! Log contract pinned here (names are DESIGN-derived, see the manifest decisions):
//! - failure: exactly one ERROR event with message `startup_failed` and fields
//!   `reason` (the §4.6 token) and `instances` (the instance ids involved, text);
//! - budget: one WARN event `startup_budget_exceeded` with fields `n` and `budget_ms`.
//!
//! Decisions (DESIGN is silent): one `list_instances` call serves both plugin
//! families, so its pattern covers both and instances are classified by their type id;
//! an unresolvable types registry client is `types_registry_unreachable`; the equal
//! priority check covers section plugin instances only; every listed plugin
//! instance's priority must be readable; `stop` leaves the request-task drain to the
//! orchestrator phase (P7).
//!
//! Tests, happy path:
//! - `start_publishes_the_result_once_and_becomes_ready` (§3.2, §3.6)
//! - `sections_are_ordered_by_priority_whatever_the_listing_order` (§3.2 "sort")
//! - `key_calls_run_concurrently_so_the_key_phase_takes_one_limit` (§3.2, §3.6)
//! - `empty_plugin_set_is_valid` (§3.6, §4.8)
//! - `one_listing_covers_both_families_and_ignores_other_plugin_types` (§3.6)
//! - `published_ports_reach_the_resolved_plugins` (§3.2 adapters)
//! - `start_never_calls_the_tenant_resolver` (ADR 0007)
//! - `budget_warning_...` x3 (§4.7, §4.8)
//!
//! Failure reasons (each: start fails at once, one call to the failing dependency, no
//! retry, one structured event, no tenant call, not ready):
//! - `types_registry_unreachable`: error, never answers, too slow, client missing
//! - `priority_unreadable`: table of six malformed priorities, sections and audit
//! - `equal_priority`, `no_audit_plugin`, `multiple_audit_plugins`
//! - `plugin_client_unresolved` (section, audit)
//! - `section_key_unavailable`: plugin errors, panic, never answers, no retry, no cancel
//! - `invalid_section_key`: table, plus boundary-valid keys
//! - `duplicate_section_key`
//! - step order (`checks_report_the_first_failing_step_in_order`, §4.6)
//! - `invalid_setting` is `init`'s (P1, `gear_tests.rs`); referenced only.
//!
//! Added after review: i16 boundaries and foreign instances (F-013), vendor agnostic
//! (F-018), `key` not sent at the cap (F-005), late `key` result labels (F-021), a
//! cancelled start token is ignored (F-020), two-limit start bound (F-016).
//! Not tested here, deferred to the integration phase with the §4.6 metrics: the
//! startup duration (F-019).
//!
//! Readiness and stop:
//! - `healthcheck_*` (§3.2), `stop_*` (§3.2 stop order, §4.8)

use std::sync::{Arc, Mutex};
use std::time::Duration;

use application_manager_sdk::{
    AuditPluginClientV1, CallerIdentity, SectionPluginClientV1, SectionPluginError,
};
use serde_json::{Value, json};
use tenant_resolver_sdk::TenantResolverClient;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use toolkit::client_hub::{ClientHub, ClientScope};
use toolkit::{Healthcheck, HealthcheckStatus, RunnableCapability};
use types_registry_sdk::testing::make_test_instance;
use types_registry_sdk::{GtsInstance, TypesRegistryClient};
use uuid::Uuid;

use crate::config::ApplicationManagerConfig;
use crate::domain::ports::{PluginCallError, StartupPort};
use crate::domain::startup::StartupResultHolder;
use crate::infra::call_guard::{CallKind, CallMeta};
use crate::infra::startup::{StartupComponent, StartupHealthcheck};
use crate::test_support::{
    AuditCall, CallBehavior, FakeAuditPlugin, FakeSectionPlugin, FakeTypesRegistry, LogBuffer,
    PluginFamily, SectionCall, TenantResolverTripwire, bounded, capture_logs, field_value,
    has_field, plugin_instance, plugin_instance_with,
};

const TIMEOUT: Duration = Duration::from_millis(500);
const FAILED: &str = "startup_failed";
const BUDGET: &str = "startup_budget_exceeded";
const FOREIGN_TYPE: &str = "gts.cf.toolkit.plugins.plugin.v1~cf.core.authn_resolver.plugin.v1~";

// Exactly one ERROR `startup_failed` event with the reason, whose `instances` field is
// exactly the list `names` (compared as a set: order is not pinned), and which names
// none of `absent`. The capture is proven live first.
fn assert_failure_event(logs: &LogBuffer, reason: &str, names: &[&str], absent: &[&str]) {
    logs.assert_live();
    let lines = logs.lines_with(FAILED);
    assert_eq!(
        lines.len(),
        1,
        "exactly one startup failure event: {lines:?}"
    );
    let line = &lines[0];
    assert!(line.contains("ERROR"), "failure event is an error: {line}");
    assert!(has_field(line, "reason", reason), "reason {reason}: {line}");
    let mut listed: Vec<String> = field_value(line, "instances")
        .unwrap_or_default()
        .split(',')
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .collect();
    listed.sort();
    let mut expected: Vec<String> = names.iter().map(|n| (*n).to_owned()).collect();
    expected.sort();
    assert_eq!(listed, expected, "the instances field: {line}");
    for name in absent {
        assert!(!line.contains(name), "event must not name {name}: {line}");
    }
}

// ---------------------------------------------------------------- harness

fn config_with(plugin: Duration, pdp: Duration, registry: Duration) -> ApplicationManagerConfig {
    ApplicationManagerConfig {
        plugin_call_timeout: plugin,
        plugin_max_in_flight: 8,
        max_request_tasks: 16,
        max_envelope_text_length: 255,
        pdp_call_timeout: pdp,
        registry_operation_timeout: registry,
    }
}

fn base_config() -> ApplicationManagerConfig {
    config_with(TIMEOUT, Duration::from_secs(2), Duration::from_secs(3))
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

fn segment(name: &str) -> String {
    format!("acme.net.core.{name}.v1")
}

struct Section {
    id: String,
    key: String,
    fake: Arc<FakeSectionPlugin>,
}

struct Audit {
    id: String,
    fake: Arc<FakeAuditPlugin>,
}

#[derive(Clone, Copy)]
enum Listing {
    Normal,
    Failing,
    Hanging,
    Delayed(Duration),
}

struct Setup {
    hub: Arc<ClientHub>,
    instances: Vec<GtsInstance>,
    listing: Listing,
    config: ApplicationManagerConfig,
    registry_in_hub: bool,
    fakes: Vec<Arc<FakeSectionPlugin>>,
}

impl Setup {
    fn new() -> Self {
        Self {
            hub: Arc::new(ClientHub::new()),
            instances: Vec::new(),
            listing: Listing::Normal,
            config: base_config(),
            registry_in_hub: true,
            fakes: Vec::new(),
        }
    }

    // A section plugin instance with a scoped client serving `key`. Every fake serves a
    // distinct schema (`{"title": key}`), so a test can tell which client answered.
    fn section(&mut self, name: &str, priority: i16, key: &str) -> Section {
        self.section_with_vendor(name, priority, key, "acme")
    }

    fn section_with_vendor(
        &mut self,
        name: &str,
        priority: i16,
        key: &str,
        vendor: &'static str,
    ) -> Section {
        let (id, instance) = plugin_instance_with(PluginFamily::Section, &segment(name), |v| {
            v["priority"] = json!(priority);
            v["vendor"] = json!(vendor);
        });
        self.instances.push(instance);
        let fake = Arc::new(FakeSectionPlugin::new(key).with_schema(json!({"title": key})));
        let client: Arc<dyn SectionPluginClientV1> = fake.clone();
        self.hub
            .register_scoped::<dyn SectionPluginClientV1>(ClientScope::gts_id(&id), client);
        self.fakes.push(Arc::clone(&fake));
        Section {
            id,
            key: key.to_owned(),
            fake,
        }
    }

    // A section plugin instance that has no scoped client.
    fn section_without_client(&mut self, name: &str, priority: i16) -> String {
        let (id, instance) = plugin_instance(PluginFamily::Section, &segment(name), priority);
        self.instances.push(instance);
        id
    }

    fn audit(&mut self, name: &str, priority: i16) -> Audit {
        self.audit_with_vendor(name, priority, "acme")
    }

    fn audit_with_vendor(&mut self, name: &str, priority: i16, vendor: &'static str) -> Audit {
        let (id, instance) = plugin_instance_with(PluginFamily::Audit, &segment(name), |v| {
            v["priority"] = json!(priority);
            v["vendor"] = json!(vendor);
        });
        self.instances.push(instance);
        let fake = Arc::new(FakeAuditPlugin::new());
        let client: Arc<dyn AuditPluginClientV1> = fake.clone();
        self.hub
            .register_scoped::<dyn AuditPluginClientV1>(ClientScope::gts_id(&id), client);
        Audit { id, fake }
    }

    fn audit_without_client(&mut self, name: &str, priority: i16) -> String {
        let (id, instance) = plugin_instance(PluginFamily::Audit, &segment(name), priority);
        self.instances.push(instance);
        id
    }

    // An instance whose object was edited (for example an unreadable priority).
    fn edited(
        &mut self,
        family: PluginFamily,
        name: &str,
        edit: impl FnOnce(&mut Value),
    ) -> String {
        let (id, instance) = plugin_instance_with(family, &segment(name), edit);
        self.instances.push(instance);
        id
    }

    // An instance of another plugin family that the registry lists but the gear ignores.
    fn foreign(&mut self, name: &str, priority: i16) {
        self.foreign_raw(name, json!(priority));
    }

    // Like `foreign`, with any JSON value as the priority (also an unreadable one).
    fn foreign_raw(&mut self, name: &str, priority: Value) {
        let id = format!("{FOREIGN_TYPE}{}", segment(name));
        self.instances.push(make_test_instance(
            &id,
            json!({"id": id, "vendor": "acme", "priority": priority, "properties": {}}),
        ));
    }

    fn with_listing(mut self, listing: Listing) -> Self {
        self.listing = listing;
        self
    }

    fn with_config(mut self, config: ApplicationManagerConfig) -> Self {
        self.config = config;
        self
    }

    fn without_registry_client(mut self) -> Self {
        self.registry_in_hub = false;
        self
    }

    fn build(self) -> Env {
        let mut registry = FakeTypesRegistry::new().with_instances(self.instances);
        registry = match self.listing {
            Listing::Normal => registry,
            Listing::Failing => registry.failing(),
            Listing::Hanging => registry.hanging(),
            Listing::Delayed(d) => registry.with_delay(d),
        };
        let registry = Arc::new(registry);
        if self.registry_in_hub {
            let client: Arc<dyn TypesRegistryClient> = registry.clone();
            self.hub.register::<dyn TypesRegistryClient>(client);
        }
        let tenant = Arc::new(TenantResolverTripwire::new());
        let tenant_client: Arc<dyn TenantResolverClient> = tenant.clone();
        self.hub.register::<dyn TenantResolverClient>(tenant_client);
        let holder = Arc::new(StartupResultHolder::new());
        let component = StartupComponent::new(&self.config, Arc::clone(&self.hub), holder.clone());
        Env {
            component,
            holder,
            registry,
            tenant,
            fakes: self.fakes,
            last_start: Mutex::new(Duration::ZERO),
        }
    }
}

struct Env {
    component: StartupComponent,
    holder: Arc<StartupResultHolder>,
    registry: Arc<FakeTypesRegistry>,
    tenant: Arc<TenantResolverTripwire>,
    fakes: Vec<Arc<FakeSectionPlugin>>,
    last_start: Mutex<Duration>,
}

impl Env {
    // Bounded, so a regression that makes `start` wait forever fails instead of hanging
    // CI (the paused clock would advance to the bound at once).
    async fn start(&self) -> anyhow::Result<()> {
        bounded(self.component.start(CancellationToken::new())).await
    }

    // Time `start` took in the last `failed_start` (the 60s recount sleep excluded).
    fn last_start_elapsed(&self) -> Duration {
        *self.last_start.lock().expect("lock")
    }

    // Common checks of every failed start (§3.2 fault handling, ADR 0001, ADR 0007).
    // After the failure the clock moves 60s and every call count is read again, so a
    // background retry of any dependency is seen too.
    async fn failed_start(&self, reason: &str) -> String {
        let started = Instant::now();
        let result = self.start().await;
        *self.last_start.lock().expect("lock") = started.elapsed();
        let registry_calls = self.registry.total_calls();
        let key_calls: Vec<usize> = self.fakes.iter().map(|f| f.call_count()).collect();
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(
            self.registry.total_calls(),
            registry_calls,
            "no retry later"
        );
        let later: Vec<usize> = self.fakes.iter().map(|f| f.call_count()).collect();
        assert_eq!(later, key_calls, "no plugin call after the failure");
        let text = format!("{}", result.expect_err("start must fail"));
        assert!(
            text.starts_with(&format!("{reason}:")),
            "error text starts with `{reason}:`, got: {text}"
        );
        assert!(
            !self.holder.is_ready(),
            "a failed start never becomes ready"
        );
        assert!(
            self.holder.result().is_none(),
            "a failed start publishes nothing"
        );
        assert_eq!(self.tenant.calls(), 0, "no tenant resolver call (ADR 0007)");
        text
    }
}

fn expect_key_calls(sections: &[&Section], expected: usize) {
    for section in sections {
        assert_eq!(
            section.fake.call_count(),
            expected,
            "key calls to {}",
            section.id
        );
    }
}

// ---------------------------------------------------------------- happy path

// DESIGN §3.2/§3.6: one listing, keys through the guard, the result published once.
#[tokio::test(start_paused = true)]
async fn start_publishes_the_result_once_and_becomes_ready() {
    let mut setup = Setup::new();
    let net = setup.section("net", 1, "network");
    let dns = setup.section("dns", 2, "dns");
    let audit = setup.audit("audit", 0);
    let env = setup.build();
    assert!(!env.holder.is_ready(), "not ready before start");

    env.start().await.expect("start succeeds");

    assert!(env.holder.is_ready());
    let result = env.holder.result().expect("result published");
    assert_eq!(result.section_keys(), vec!["network", "dns"]);
    assert_eq!(result.audit_instance_id(), audit.id);
    let ids: Vec<&str> = result
        .section_plugins()
        .iter()
        .map(|e| e.instance_id.as_str())
        .collect();
    assert_eq!(ids, [net.id.as_str(), dns.id.as_str()]);
    assert_eq!(result.section_plugins()[0].priority, 1);
    // One registry call in all; one key call per section plugin; none to the audit plugin.
    assert_eq!(env.registry.list_calls(), 1);
    assert_eq!(env.registry.total_calls(), 1);
    expect_key_calls(&[&net, &dns], 1);
    assert_eq!(net.fake.calls()[0].kind, SectionCall::Key);
    assert_eq!(audit.fake.call_count(), 0);
    // Immutable: a second publication by anyone is refused.
    assert!(
        env.holder
            .publish_result(result_clone_for_refusal())
            .is_err()
    );
}

fn result_clone_for_refusal() -> crate::domain::startup::StartupResult {
    // A second result with an empty set; only its refusal matters.
    let sink: Arc<dyn crate::domain::ports::AuditSink> = Arc::new(NoAudit);
    crate::domain::startup::StartupResult::new(vec![], "other".to_owned(), sink)
}

struct NoAudit;

#[async_trait::async_trait]
impl crate::domain::ports::AuditSink for NoAudit {
    async fn request_handoff(
        &self,
        _caller: &CallerIdentity,
        _content: &application_manager_sdk::RequestHandoffContent,
    ) -> Result<String, PluginCallError<application_manager_sdk::AuditPluginError>> {
        Ok(String::new())
    }

    async fn final_result_handoff(
        &self,
        _caller: &CallerIdentity,
        _audit_id: &str,
        _content: &application_manager_sdk::FinalResultContent,
    ) -> Result<(), PluginCallError<application_manager_sdk::AuditPluginError>> {
        Ok(())
    }
}

// DESIGN §3.2: the section plugin set is sorted by (priority, id); the listing order of
// the registry is not a contract. (Equal priorities are a startup failure, so the
// instance id only completes the total order.)
#[tokio::test(start_paused = true)]
async fn sections_are_ordered_by_priority_whatever_the_listing_order() {
    let mut setup = Setup::new();
    setup.section("zulu", 5, "z");
    setup.section("bravo", 1, "b");
    setup.section("mike", -2, "m");
    setup.section("alpha", 3, "a");
    setup.audit("audit", 0);
    let env = setup.build();

    env.start().await.expect("start succeeds");

    let result = env.holder.result().expect("published");
    assert_eq!(result.section_keys(), vec!["m", "b", "a", "z"]);
    let priorities: Vec<i16> = result
        .section_plugins()
        .iter()
        .map(|e| e.priority)
        .collect();
    assert_eq!(priorities, [-2, 1, 3, 5]);
}

// DESIGN §3.6: the key calls run concurrently, so the key phase takes at most one
// time limit, however many plugins there are.
#[tokio::test(start_paused = true)]
async fn key_calls_run_concurrently_so_the_key_phase_takes_one_limit() {
    let mut setup = Setup::new();
    let sections: Vec<Section> = ["a", "b", "c", "d"]
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let section = setup.section(name, i16::try_from(i).expect("small"), name);
            section.fake.on(
                SectionCall::Key,
                CallBehavior::ok().with_delay(Duration::from_millis(400)),
            );
            section
        })
        .collect();
    setup.audit("audit", 0);
    let env = setup.build();
    let started = Instant::now();

    env.start()
        .await
        .expect("each key answers within the limit");

    assert_eq!(started.elapsed(), Duration::from_millis(400));
    expect_key_calls(&sections.iter().collect::<Vec<_>>(), 1);
}

// DESIGN §3.6: "If the types registry answers with no section plugin instances, the
// key set is empty." Valid, ready, no key call.
#[tokio::test(start_paused = true)]
async fn empty_plugin_set_is_valid() {
    let mut setup = Setup::new();
    setup.audit("audit", 0);
    let env = setup.build();

    env.start().await.expect("an empty plugin set is valid");

    assert!(env.holder.is_ready());
    let result = env.holder.result().expect("published");
    assert!(result.section_plugins().is_empty());
    assert!(result.section_keys().is_empty());
}

// DESIGN §3.6: the listing is one bounded call. It must cover both plugin families
// (the section type and the audit type are siblings under the toolkit plugin base) and
// instances of other plugin types in the answer are ignored, not counted.
#[tokio::test(start_paused = true)]
async fn one_listing_covers_both_families_and_ignores_other_plugin_types() {
    let mut setup = Setup::new();
    let net = setup.section("net", 1, "network");
    setup.audit("audit", 0);
    // Same priority as the section and no client: must not matter.
    setup.foreign("other", 1);
    // Another plugin family's unreadable priority is not ours to judge.
    setup.foreign_raw("broken", json!("high"));
    let env = setup.build();

    env.start().await.expect("foreign instances are ignored");

    assert_eq!(env.registry.list_calls(), 1);
    let pattern = env.registry.queries()[0]
        .pattern
        .clone()
        .expect("the listing is narrowed by a pattern");
    let prefix = pattern.trim_end_matches('*');
    let section_type =
        "gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~";
    let audit_type =
        "gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_audit.plugin.v1~";
    assert!(
        section_type.starts_with(prefix) && audit_type.starts_with(prefix),
        "pattern {pattern} must cover both plugin families"
    );
    assert!(
        prefix.starts_with("gts.cf.toolkit.plugins.plugin.v1~"),
        "pattern {pattern} must stay inside the toolkit plugin base type"
    );
    let result = env.holder.result().expect("published");
    assert_eq!(result.section_keys(), vec!["network"]);
    expect_key_calls(&[&net], 1);
}

// DESIGN §3.2/§3.6: every published entry pairs its own instance id, priority, key and
// client. The listing is unsorted and every fake serves a distinct schema, so a
// mis-pairing (a key or a client of another instance) dispatches to the wrong plugin.
#[tokio::test(start_paused = true)]
async fn published_entries_pair_instance_priority_key_and_client() {
    let mut setup = Setup::new();
    let sections = [
        setup.section("zulu", 30, "kz"),
        setup.section("alpha", 10, "ka"),
        setup.section("mike", 20, "km"),
    ];
    setup.audit("audit", 0);
    let env = setup.build();
    env.start().await.expect("start succeeds");
    let result = env.holder.result().expect("published");

    let by_key = |key: &str| sections.iter().find(|s| s.key == key).expect("known");
    let order: Vec<&str> = result
        .section_plugins()
        .iter()
        .map(|e| e.key.as_str())
        .collect();
    assert_eq!(order, ["ka", "km", "kz"], "dispatch order by priority");
    for (entry, priority) in result.section_plugins().iter().zip([10, 20, 30]) {
        let section = by_key(&entry.key);
        assert_eq!(
            entry.instance_id, section.id,
            "instance id of key {}",
            entry.key
        );
        assert_eq!(entry.priority, priority, "priority of key {}", entry.key);
        let schema = entry.plugin.schema(&caller()).await.expect("schema");
        assert_eq!(
            schema,
            json!({"title": entry.key}),
            "client of key {}",
            entry.key
        );
    }
    for section in &sections {
        let schema_calls = section
            .fake
            .calls()
            .iter()
            .filter(|c| c.kind == SectionCall::Schema)
            .count();
        assert_eq!(schema_calls, 1, "exactly one schema call to {}", section.id);
    }
}

// DESIGN §3.2: the startup builds the plugin adapters; the published ports work.
#[tokio::test(start_paused = true)]
async fn published_ports_reach_the_resolved_plugins() {
    let mut setup = Setup::new();
    let net = setup.section("net", 1, "network");
    let audit = setup.audit("audit", 0);
    let env = setup.build();
    env.start().await.expect("start succeeds");
    let result = env.holder.result().expect("published");

    let schema = result.section_plugins()[0]
        .plugin
        .schema(&caller())
        .await
        .expect("schema through the port");
    let handoff = result
        .audit()
        .request_handoff(&caller(), &request_content())
        .await;

    assert_eq!(schema, json!({"title": "network"}));
    assert_eq!(handoff, Ok("audit-1".to_owned()));
    assert_eq!(
        net.fake.calls().last().map(|c| c.kind),
        Some(SectionCall::Schema)
    );
    assert_eq!(
        audit.fake.calls().last().map(|c| c.kind),
        Some(AuditCall::RequestHandoff)
    );
}

fn request_content() -> application_manager_sdk::RequestHandoffContent {
    application_manager_sdk::RequestHandoffContent {
        operation: application_manager_sdk::models::HandoffOperation::Seed,
        request_id: Uuid::from_u128(3),
        received_at: "2026-10-13T00:00:00Z".to_owned(),
        application: None,
        manifest: None,
        decided_category: None,
        id_length_bytes: None,
        id_sha256: None,
    }
}

// DESIGN §3.2: one guard serves startup, the request path and `stop`'s drain. A
// published port that used a private guard would split the in-flight cap and `stop`
// would drain the wrong guard.
#[tokio::test(start_paused = true)]
async fn published_ports_use_the_components_guard() {
    let mut setup = Setup::new();
    let net = setup.section("net", 1, "network");
    let audit = setup.audit("audit", 0);
    let env = setup.build();
    env.start().await.expect("start succeeds");
    let result = env.holder.result().expect("published");
    net.fake.on(SectionCall::Schema, CallBehavior::hang());
    audit
        .fake
        .on(AuditCall::RequestHandoff, CallBehavior::hang());
    assert_eq!(env.component.guard().in_flight(&net.id), 0);

    let schema = bounded(result.section_plugins()[0].plugin.schema(&caller())).await;
    let handoff = bounded(
        result
            .audit()
            .request_handoff(&caller(), &request_content()),
    )
    .await;

    assert_eq!(schema, Err(PluginCallError::TimedOut));
    assert_eq!(handoff, Err(PluginCallError::TimedOut));
    assert_eq!(env.component.guard().in_flight(&net.id), 1);
    assert_eq!(env.component.guard().in_flight(&audit.id), 1);
    // And `stop` therefore waits for them until its deadline.
    let token = CancellationToken::new();
    token.cancel();
    bounded(env.component.stop(token))
        .await
        .expect("stop succeeds");
    assert_eq!(
        env.component.guard().in_flight(&net.id),
        1,
        "never cancelled"
    );
}

// ADR 0007, DESIGN §3.2: no tenant lookup of any kind, on success or on failure.
#[tokio::test(start_paused = true)]
async fn start_never_calls_the_tenant_resolver() {
    let mut ok = Setup::new();
    ok.section("net", 1, "network");
    ok.audit("audit", 0);
    let ok = ok.build();
    ok.start().await.expect("start succeeds");
    assert_eq!(ok.tenant.calls(), 0);

    let mut failing = Setup::new();
    failing.section("net", 1, "network");
    let failing = failing.build();
    failing.failed_start("no_audit_plugin").await;
    assert_eq!(failing.tenant.calls(), 0);
}

// DESIGN §4.7: past the gateway deadline once N is known, the start only warns and
// continues (the `init` check could not know N). The warning count (metric) comes with
// the §4.6 catalogue in P9, so only the event is pinned.
#[tokio::test(start_paused = true)]
async fn budget_above_the_gateway_deadline_warns_once_and_start_continues() {
    let (logs, _guard) = capture_logs();
    // init passes: 2s + 2 x 2s + 4 x 5s = 26s. N = 2: 2s + 2 x 2s + 6 x 5s = 36s (F-001: N + 4
    // plugin limits, the fourth is the whole-manifest validation step).
    let mut setup = Setup::new().with_config(config_with(
        Duration::from_secs(5),
        Duration::from_secs(2),
        Duration::from_secs(2),
    ));
    setup.section("a", 1, "a");
    setup.section("b", 2, "b");
    setup.audit("audit", 0);
    let env = setup.build();

    env.start()
        .await
        .expect("a budget warning never fails start");

    assert!(env.holder.is_ready());
    logs.assert_live();
    let lines = logs.lines_with(BUDGET);
    assert_eq!(lines.len(), 1, "one budget warning: {lines:?}");
    assert!(lines[0].contains("WARN"), "warn level: {}", lines[0]);
    assert!(has_field(&lines[0], "n", "2"), "N is logged: {}", lines[0]);
    assert!(
        has_field(&lines[0], "budget_ms", "36000"),
        "the budget is logged: {}",
        lines[0]
    );
    assert!(logs.lines_with(FAILED).is_empty());
}

// DESIGN §3.2 step order: the budget is checked once N is known, after the key checks.
// A start that fails earlier never reaches it, so no warning is logged.
#[tokio::test(start_paused = true)]
async fn budget_is_not_checked_when_an_earlier_step_fails() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new().with_config(config_with(
        Duration::from_secs(5),
        Duration::from_secs(2),
        Duration::from_secs(2),
    ));
    setup.section("a", 1, "same");
    setup.section("b", 2, "same");
    setup.audit("audit", 0);
    let env = setup.build();

    env.failed_start("duplicate_section_key").await;

    logs.assert_live();
    assert!(logs.lines_with(BUDGET).is_empty());
}

// DESIGN §4.7: "a budget fits the deadline when it is at most the deadline".
#[tokio::test(start_paused = true)]
async fn budget_exactly_at_the_gateway_deadline_does_not_warn() {
    let (logs, _guard) = capture_logs();
    // N = 1: 4s + 2 x 3s + (1 + 4) x 4s = 30s (F-001: N + 4 plugin limits).
    let mut setup = Setup::new().with_config(config_with(
        Duration::from_secs(4),
        Duration::from_secs(4),
        Duration::from_secs(3),
    ));
    setup.section("a", 1, "a");
    setup.audit("audit", 0);
    let env = setup.build();

    env.start().await.expect("start succeeds");

    logs.assert_live();
    assert!(logs.lines_with(BUDGET).is_empty());
}

#[tokio::test(start_paused = true)]
async fn budget_within_the_deadline_does_not_warn() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    setup.section("a", 1, "a");
    setup.section("b", 2, "b");
    setup.audit("audit", 0);
    let env = setup.build();

    env.start().await.expect("start succeeds");

    logs.assert_live();
    assert!(logs.lines_with(BUDGET).is_empty());
}

// ---------------------------------------------------------------- types_registry_unreachable

// DESIGN §3.2: a listing that fails is never read as an empty listing.
#[tokio::test(start_paused = true)]
async fn types_registry_listing_error_fails_start_at_once() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new().with_listing(Listing::Failing);
    let net = setup.section("net", 1, "network");
    setup.audit("audit", 0);
    let env = setup.build();

    env.failed_start("types_registry_unreachable").await;

    assert_eq!(env.last_start_elapsed(), Duration::ZERO, "fails at once");
    assert_eq!(env.registry.list_calls(), 1, "no retry");
    assert_eq!(env.registry.total_calls(), 1);
    expect_key_calls(&[&net], 0);
    assert_failure_event(&logs, "types_registry_unreachable", &[], &[]);
}

// DESIGN §3.2/§4.8: a listing that never answers ends `start` with an error after
// exactly one `plugin_call_timeout`.
#[tokio::test(start_paused = true)]
async fn types_registry_listing_that_never_answers_fails_after_one_limit() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new().with_listing(Listing::Hanging);
    let net = setup.section("net", 1, "network");
    setup.audit("audit", 0);
    let env = setup.build();

    env.failed_start("types_registry_unreachable").await;

    assert_eq!(
        env.last_start_elapsed(),
        TIMEOUT,
        "exactly one plugin_call_timeout"
    );
    assert_eq!(env.registry.list_calls(), 1, "no retry");
    expect_key_calls(&[&net], 0);
    assert_failure_event(&logs, "types_registry_unreachable", &[], &[]);
}

// DESIGN §3.2: the bound has no margin: a listing just inside it is read, one just
// past it is not.
#[tokio::test(start_paused = true)]
async fn types_registry_listing_bound_has_no_margin() {
    let inside = {
        let mut setup =
            Setup::new().with_listing(Listing::Delayed(TIMEOUT - Duration::from_millis(1)));
        setup.audit("audit", 0);
        setup.build()
    };
    inside
        .start()
        .await
        .expect("a listing just inside the limit is read");

    let past = {
        let mut setup =
            Setup::new().with_listing(Listing::Delayed(TIMEOUT + Duration::from_millis(1)));
        setup.audit("audit", 0);
        setup.build()
    };
    past.failed_start("types_registry_unreachable").await;
    assert_eq!(past.last_start_elapsed(), TIMEOUT);
}

// Decision: the registry client is resolved from the hub at `start`; a missing client
// is the same operator fault as an unreachable registry.
#[tokio::test(start_paused = true)]
async fn types_registry_client_missing_from_the_hub_is_unreachable() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new().without_registry_client();
    setup.audit("audit", 0);
    let env = setup.build();

    env.failed_start("types_registry_unreachable").await;

    assert_eq!(env.registry.total_calls(), 0);
    assert_failure_event(&logs, "types_registry_unreachable", &[], &[]);
}

// ---------------------------------------------------------------- priority_unreadable

type Edit = fn(&mut Value);

const BAD_PRIORITIES: [(&str, Edit); 9] = [
    ("absent", |v| {
        v.as_object_mut().expect("object").remove("priority");
    }),
    ("string", |v| v["priority"] = json!("high")),
    ("null", |v| v["priority"] = Value::Null),
    ("fractional", |v| v["priority"] = json!(1.5)),
    ("above i16", |v| v["priority"] = json!(70_000)),
    ("below i16", |v| v["priority"] = json!(-40_000)),
    ("i16::MAX + 1", |v| v["priority"] = json!(32_768)),
    ("i16::MIN - 1", |v| v["priority"] = json!(-32_769)),
    ("boolean", |v| v["priority"] = json!(true)),
];

// DESIGN §3.2/§4.6: an instance with no readable priority fails `start`, for a
// section plugin and for the audit plugin; the event names the instance; the check
// runs before any client is resolved or key is read.
#[tokio::test(start_paused = true)]
async fn unreadable_priority_fails_start_naming_the_instance() {
    for family in [PluginFamily::Section, PluginFamily::Audit] {
        for (label, edit) in BAD_PRIORITIES {
            let (logs, _guard) = capture_logs();
            let mut setup = Setup::new();
            let net = setup.section("net", 1, "network");
            let bad = setup.edited(family, "bad", edit);
            if family == PluginFamily::Section {
                setup.audit("audit", 0);
            }
            let env = setup.build();

            env.failed_start("priority_unreadable").await;

            assert_eq!(env.registry.list_calls(), 1, "{family:?} {label}: one call");
            expect_key_calls(&[&net], 0);
            assert_failure_event(&logs, "priority_unreadable", &[&bad], &[&net.id]);
        }
    }
}

// ---------------------------------------------------------------- equal_priority

#[tokio::test(start_paused = true)]
async fn equal_priority_names_both_instances_and_reads_no_key() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let a = setup.section("a", 3, "a");
    let b = setup.section("b", 3, "b");
    let c = setup.section("c", 4, "c");
    setup.audit("audit", 0);
    let env = setup.build();

    env.failed_start("equal_priority").await;

    assert_eq!(env.registry.list_calls(), 1);
    expect_key_calls(&[&a, &b, &c], 0);
    assert_failure_event(&logs, "equal_priority", &[&a.id, &b.id], &[&c.id]);
}

// Decision: only section plugin priorities are compared; the audit plugin's priority
// does not take part in dispatch order.
#[tokio::test(start_paused = true)]
async fn audit_priority_is_not_compared_with_section_priorities() {
    let mut setup = Setup::new();
    setup.section("a", 3, "a");
    setup.audit("audit", 3);
    let env = setup.build();

    env.start()
        .await
        .expect("an audit priority equal to a section priority is fine");
}

// ---------------------------------------------------------------- audit plugin count

#[tokio::test(start_paused = true)]
async fn no_audit_plugin_fails_start() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let net = setup.section("net", 1, "network");
    let env = setup.build();

    env.failed_start("no_audit_plugin").await;

    assert_eq!(env.registry.list_calls(), 1);
    expect_key_calls(&[&net], 0);
    assert_failure_event(&logs, "no_audit_plugin", &[], &[]);
}

#[tokio::test(start_paused = true)]
async fn multiple_audit_plugins_fail_start_naming_all_of_them() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let net = setup.section("net", 1, "network");
    let first = setup.audit("one", 0);
    let second = setup.audit("two", 1);
    let third = setup.audit("three", 2);
    let env = setup.build();

    env.failed_start("multiple_audit_plugins").await;

    expect_key_calls(&[&net], 0);
    assert_failure_event(
        &logs,
        "multiple_audit_plugins",
        &[&first.id, &second.id, &third.id],
        &[&net.id],
    );
}

// ---------------------------------------------------------------- plugin_client_unresolved

// DESIGN §3.2: every scoped client is resolved before the first key call, so one
// unresolved client stops startup with no call to any plugin.
#[tokio::test(start_paused = true)]
async fn unresolved_section_client_fails_start_before_any_key_call() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let ok = setup.section("ok", 1, "ok");
    let missing = setup.section_without_client("missing", 2);
    setup.audit("audit", 0);
    let env = setup.build();

    env.failed_start("plugin_client_unresolved").await;

    assert_eq!(env.registry.list_calls(), 1);
    expect_key_calls(&[&ok], 0);
    assert_failure_event(&logs, "plugin_client_unresolved", &[&missing], &[&ok.id]);
}

#[tokio::test(start_paused = true)]
async fn unresolved_audit_client_fails_start_before_any_key_call() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let ok = setup.section("ok", 1, "ok");
    let missing = setup.audit_without_client("audit", 0);
    let env = setup.build();

    env.failed_start("plugin_client_unresolved").await;

    expect_key_calls(&[&ok], 0);
    assert_failure_event(&logs, "plugin_client_unresolved", &[&missing], &[&ok.id]);
}

// ---------------------------------------------------------------- section_key_unavailable

fn key_failures() -> Vec<(&'static str, CallBehavior<SectionPluginError>)> {
    vec![
        (
            "internal",
            CallBehavior::err(SectionPluginError::internal("db down")),
        ),
        (
            "rejected",
            CallBehavior::err(SectionPluginError::rejected("no", None)),
        ),
        (
            "not permitted",
            CallBehavior::err(SectionPluginError::not_permitted("nope")),
        ),
        ("panic", CallBehavior::panic()),
    ]
}

// DESIGN §4.6: a `key` call that failed (any plugin error, or a panic isolated by the
// guard) ends start with `section_key_unavailable`; exactly one key call to the failing
// plugin; the healthy plugins were called once, concurrently; no retry.
#[tokio::test(start_paused = true)]
async fn failed_key_call_fails_start_with_one_call_and_no_retry() {
    for (label, behavior) in key_failures() {
        let (logs, _guard) = capture_logs();
        let mut setup = Setup::new();
        let healthy = setup.section("healthy", 1, "healthy");
        let broken = setup.section("broken", 2, "broken");
        broken.fake.on(SectionCall::Key, behavior);
        setup.audit("audit", 0);
        let env = setup.build();

        env.failed_start("section_key_unavailable").await;

        assert_eq!(
            broken.fake.call_count(),
            1,
            "{label}: exactly one key call, no retry"
        );
        expect_key_calls(&[&healthy], 1);
        assert_eq!(env.registry.list_calls(), 1);
        assert_failure_event(
            &logs,
            "section_key_unavailable",
            &[&broken.id],
            &[&healthy.id],
        );
    }
}

// DESIGN §3.6/§4.8: a `key` call that never answers fails start after exactly one
// `plugin_call_timeout`, even when several hang (the calls run concurrently), and
// nothing is cancelled: the detached call keeps its in-flight slot.
#[tokio::test(start_paused = true)]
async fn key_call_that_never_answers_fails_after_exactly_one_limit() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let healthy = setup.section("healthy", 1, "healthy");
    let hung_a = setup.section("huna", 2, "huna");
    let hung_b = setup.section("hunb", 3, "hunb");
    hung_a.fake.on(SectionCall::Key, CallBehavior::hang());
    hung_b.fake.on(SectionCall::Key, CallBehavior::hang());
    setup.audit("audit", 0);
    let env = setup.build();

    env.failed_start("section_key_unavailable").await;

    assert_eq!(
        env.last_start_elapsed(),
        TIMEOUT,
        "one limit, not one per plugin"
    );
    expect_key_calls(&[&healthy, &hung_a, &hung_b], 1);
    assert_eq!(
        env.component.guard().in_flight(&hung_a.id),
        1,
        "not cancelled"
    );
    assert_failure_event(
        &logs,
        "section_key_unavailable",
        &[&hung_a.id, &hung_b.id],
        &[&healthy.id],
    );
    expect_key_calls(&[&hung_a, &hung_b], 1);
}

// DESIGN §3.2: the bound has no margin for `key` either.
#[tokio::test(start_paused = true)]
async fn key_call_just_inside_the_limit_succeeds_and_just_past_it_fails() {
    let inside = {
        let mut setup = Setup::new();
        let s = setup.section("net", 1, "network");
        s.fake.on(
            SectionCall::Key,
            CallBehavior::ok().with_delay(TIMEOUT - Duration::from_millis(1)),
        );
        setup.audit("audit", 0);
        setup.build()
    };
    inside
        .start()
        .await
        .expect("a key just inside the limit is read");

    let past = {
        let mut setup = Setup::new();
        let s = setup.section("net", 1, "network");
        s.fake.on(
            SectionCall::Key,
            CallBehavior::ok().with_delay(TIMEOUT + Duration::from_millis(1)),
        );
        setup.audit("audit", 0);
        setup.build()
    };
    past.failed_start("section_key_unavailable").await;
    assert_eq!(past.last_start_elapsed(), TIMEOUT);
}

// ---------------------------------------------------------------- invalid_section_key

fn invalid_keys() -> Vec<(&'static str, String)> {
    vec![
        ("empty", String::new()),
        ("65 bytes", "a".repeat(65)),
        ("dot", ".".to_owned()),
        ("dot dot", "..".to_owned()),
        ("upper case", "Network".to_owned()),
        ("space", "net work".to_owned()),
        ("slash", "net/work".to_owned()),
        ("non-ascii", "r\u{e9}seau".to_owned()),
    ]
}

// DESIGN §4.6: a key that is empty, too long, `.`/`..` or breaks the token pattern
// fails start; the check follows the key calls, so every plugin was called once.
#[tokio::test(start_paused = true)]
async fn invalid_section_key_fails_start_naming_the_instance() {
    for (label, key) in invalid_keys() {
        let (logs, _guard) = capture_logs();
        let mut setup = Setup::new();
        let good = setup.section("good", 1, "good");
        let bad = setup.section("bad", 2, &key);
        setup.audit("audit", 0);
        let env = setup.build();

        env.failed_start("invalid_section_key").await;

        expect_key_calls(&[&good, &bad], 1);
        assert_eq!(env.registry.list_calls(), 1, "{label}");
        assert_failure_event(&logs, "invalid_section_key", &[&bad.id], &[&good.id]);
    }
}

// DESIGN §3.3: the SDK rule's boundaries are accepted (64 bytes; `a.b-c_d`; digits).
#[tokio::test(start_paused = true)]
async fn boundary_valid_keys_are_accepted() {
    let mut setup = Setup::new();
    setup.section("a", 1, &"a".repeat(64));
    setup.section("b", 2, "a.b-c_d");
    setup.section("c", 3, "0");
    setup.audit("audit", 0);
    let env = setup.build();

    env.start().await.expect("boundary keys are valid");

    let result = env.holder.result().expect("published");
    assert_eq!(result.section_keys()[0].len(), 64);
}

// ---------------------------------------------------------------- duplicate_section_key

#[tokio::test(start_paused = true)]
async fn duplicate_section_key_fails_start_naming_both_instances() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let a = setup.section("a", 1, "same");
    let b = setup.section("b", 2, "same");
    let c = setup.section("c", 3, "other");
    setup.audit("audit", 0);
    let env = setup.build();

    env.failed_start("duplicate_section_key").await;

    expect_key_calls(&[&a, &b, &c], 1);
    assert_failure_event(&logs, "duplicate_section_key", &[&a.id, &b.id], &[&c.id]);
}

// ---------------------------------------------------------------- step order

// DESIGN §4.6: "`start` reports every other reason, in the order of its steps (§3.2),
// and stops at the first failure." Each case breaks several steps; only the first is
// reported, as the only failure event, and later steps do not run.
#[tokio::test(start_paused = true)]
async fn checks_report_the_first_failing_step_in_order() {
    // 1. unreadable priority beats equal priority and a missing audit plugin.
    {
        let (logs, _guard) = capture_logs();
        let mut setup = Setup::new();
        setup.section("a", 1, "a");
        setup.section("b", 1, "b");
        setup.edited(PluginFamily::Section, "bad", |v| v["priority"] = json!("x"));
        let env = setup.build();
        env.failed_start("priority_unreadable").await;
        assert_eq!(logs.lines_with(FAILED).len(), 1);
    }
    // 2. equal priority beats a missing audit plugin.
    {
        let (logs, _guard) = capture_logs();
        let mut setup = Setup::new();
        setup.section("a", 1, "a");
        setup.section("b", 1, "b");
        let env = setup.build();
        env.failed_start("equal_priority").await;
        assert_eq!(logs.lines_with(FAILED).len(), 1);
    }
    // 3. a missing audit plugin beats an unresolved client.
    {
        let (logs, _guard) = capture_logs();
        let mut setup = Setup::new();
        setup.section_without_client("a", 1);
        let env = setup.build();
        env.failed_start("no_audit_plugin").await;
        assert_eq!(logs.lines_with(FAILED).len(), 1);
    }
    // 4. several audit plugins beat an unresolved client.
    {
        let (logs, _guard) = capture_logs();
        let mut setup = Setup::new();
        setup.section_without_client("a", 1);
        setup.audit("one", 0);
        setup.audit("two", 1);
        let env = setup.build();
        env.failed_start("multiple_audit_plugins").await;
        assert_eq!(logs.lines_with(FAILED).len(), 1);
    }
    // 5. an unresolved client beats a failing key: no key call is made.
    {
        let (logs, _guard) = capture_logs();
        let mut setup = Setup::new();
        let broken = setup.section("a", 1, "a");
        broken.fake.on(
            SectionCall::Key,
            CallBehavior::err(SectionPluginError::internal("x")),
        );
        setup.section_without_client("b", 2);
        setup.audit("audit", 0);
        let env = setup.build();
        env.failed_start("plugin_client_unresolved").await;
        expect_key_calls(&[&broken], 0);
        assert_eq!(logs.lines_with(FAILED).len(), 1);
    }
    // 6. a failed key call beats an invalid key and a duplicate key.
    {
        let (logs, _guard) = capture_logs();
        let mut setup = Setup::new();
        let broken = setup.section("a", 1, "a");
        broken.fake.on(
            SectionCall::Key,
            CallBehavior::err(SectionPluginError::internal("x")),
        );
        setup.section("b", 2, "BAD");
        setup.section("c", 3, "dup");
        setup.section("d", 4, "dup");
        setup.audit("audit", 0);
        let env = setup.build();
        env.failed_start("section_key_unavailable").await;
        assert_eq!(logs.lines_with(FAILED).len(), 1);
    }
    // 7. an invalid key beats a duplicate key.
    {
        let (logs, _guard) = capture_logs();
        let mut setup = Setup::new();
        setup.section("a", 1, "BAD");
        setup.section("b", 2, "dup");
        setup.section("c", 3, "dup");
        setup.audit("audit", 0);
        let env = setup.build();
        env.failed_start("invalid_section_key").await;
        assert_eq!(logs.lines_with(FAILED).len(), 1);
    }
}

// ---------------------------------------------------------------- readiness

#[tokio::test(start_paused = true)]
async fn healthcheck_is_unhealthy_before_publication_and_healthy_after() {
    let mut setup = Setup::new();
    let net = setup.section("net", 1, "network");
    setup.audit("audit", 0);
    let env = setup.build();
    let port: Arc<dyn StartupPort> = env.holder.clone();
    let check = StartupHealthcheck::new(port);

    let before = check.check().await.status;
    env.start().await.expect("start succeeds");
    let after = check.check().await.status;

    assert_eq!(
        before,
        HealthcheckStatus::Unhealthy,
        "not ready before publication"
    );
    assert_eq!(after, HealthcheckStatus::Healthy);
    // The healthcheck only reads the holder: no plugin and no registry call (§3.2).
    let calls_before = (env.registry.total_calls(), net.fake.call_count());
    let _ = check.check().await;
    assert_eq!(
        (env.registry.total_calls(), net.fake.call_count()),
        calls_before
    );
}

// DESIGN §3.2/§4.8: not ready again once `stop` sets the stopping flag.
#[tokio::test(start_paused = true)]
async fn healthcheck_is_unhealthy_again_once_stop_began() {
    let mut setup = Setup::new();
    setup.audit("audit", 0);
    let env = setup.build();
    let port: Arc<dyn StartupPort> = env.holder.clone();
    let check = StartupHealthcheck::new(port);
    env.start().await.expect("start succeeds");
    assert_eq!(check.check().await.status, HealthcheckStatus::Healthy);

    bounded(env.component.stop(CancellationToken::new()))
        .await
        .expect("stop succeeds");

    assert_eq!(check.check().await.status, HealthcheckStatus::Unhealthy);
}

// F-016: a published holder that is stopping is not ready, whoever set the flag.
#[tokio::test(start_paused = true)]
async fn healthcheck_is_unhealthy_while_the_holder_is_stopping() {
    let mut setup = Setup::new();
    setup.audit("audit", 0);
    let env = setup.build();
    let port: Arc<dyn StartupPort> = env.holder.clone();
    let check = StartupHealthcheck::new(port);
    env.start().await.expect("start succeeds");
    assert_eq!(check.check().await.status, HealthcheckStatus::Healthy);

    env.holder.set_stopping();

    assert_eq!(check.check().await.status, HealthcheckStatus::Unhealthy);
}

// DESIGN §3.2: "Liveness is unaffected", and there is no degraded state: after a failed
// start the healthcheck stays unhealthy, and it never reports degraded in any state.
#[tokio::test(start_paused = true)]
async fn healthcheck_never_reports_degraded() {
    let mut failing = Setup::new();
    failing.section("net", 1, "network");
    let failing = failing.build();
    let failing_port: Arc<dyn StartupPort> = failing.holder.clone();
    let failing_check = StartupHealthcheck::new(failing_port);
    failing.failed_start("no_audit_plugin").await;
    assert_eq!(
        failing_check.check().await.status,
        HealthcheckStatus::Unhealthy
    );

    let holder = Arc::new(StartupResultHolder::new());
    let port: Arc<dyn StartupPort> = holder.clone();
    let check = StartupHealthcheck::new(port);
    for stopping_first in [false, true] {
        if stopping_first {
            holder.set_stopping();
        }
        assert_ne!(check.check().await.status, HealthcheckStatus::Degraded);
    }
}

// ---------------------------------------------------------------- stop

fn apply_meta() -> CallMeta {
    CallMeta {
        request_id: Some(Uuid::from_u128(3)),
        call_kind: CallKind::Apply,
        section_key: Some("network".to_owned()),
    }
}

// Starts a guarded call that outlives its caller's wait (detached): `lasts` long.
async fn detached_call(env: &Env, lasts: Duration) {
    let guard = env.component.guard().clone();
    let outcome = bounded(guard.call("busy", apply_meta(), async move {
        tokio::time::sleep(lasts).await;
        Ok::<(), ()>(())
    }))
    .await;
    assert_eq!(outcome, Err(PluginCallError::TimedOut));
    assert_eq!(guard.in_flight("busy"), 1);
}

fn cancel_after(after: Duration) -> CancellationToken {
    let token = CancellationToken::new();
    let trigger = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(after).await;
        trigger.cancel();
    });
    token
}

// DESIGN §3.2 "Stop": the stopping flag is set first, then the guard is drained; the
// flag is therefore already set while the drain still waits (admission is closed).
#[tokio::test(start_paused = true)]
async fn stop_sets_the_stopping_flag_before_it_drains() {
    let mut setup = Setup::new();
    setup.audit("audit", 0);
    let env = setup.build();
    env.start().await.expect("start succeeds");
    detached_call(&env, Duration::from_secs(10)).await;
    let token = CancellationToken::new();

    let stop = env.component.stop(token.clone());
    tokio::pin!(stop);
    tokio::select! {
        biased;
        result = &mut stop => panic!("stop must wait for the detached call: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    assert!(
        env.holder.is_stopping(),
        "flag set while the drain is still waiting"
    );
    assert!(!env.holder.is_ready());
    token.cancel();
    bounded(stop)
        .await
        .expect("stop succeeds once the deadline passed");
}

// DESIGN §3.2: the drain receives the `deadline_token` of `stop`, not a deadline of its
// own: `stop` returns when the detached calls end, or when that token is cancelled.
#[tokio::test(start_paused = true)]
async fn stop_drain_ends_when_the_calls_end_or_the_token_is_cancelled() {
    // (a) the call ends first: stop returns then, abandoning nothing.
    {
        let (logs, _guard) = capture_logs();
        let env = Setup::new().build();
        detached_call(&env, Duration::from_secs(10)).await;
        let started = Instant::now();
        bounded(env.component.stop(CancellationToken::new()))
            .await
            .expect("stop succeeds");
        assert_eq!(started.elapsed(), Duration::from_secs(10) - TIMEOUT);
        logs.assert_live();
        assert!(logs.lines_with("plugin_calls_abandoned").is_empty());
    }
    // (b) the token is cancelled first: stop returns at that moment and logs the
    // abandoned call once; the call is not cancelled and finishes later.
    {
        let (logs, _guard) = capture_logs();
        let env = Setup::new().build();
        detached_call(&env, Duration::from_secs(10)).await;
        let started = Instant::now();
        bounded(env.component.stop(cancel_after(Duration::from_secs(2))))
            .await
            .expect("stop succeeds");
        assert_eq!(started.elapsed(), Duration::from_secs(2));
        let lines = logs.lines_with("plugin_calls_abandoned");
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(has_field(&lines[0], "count", "1"));
        assert_eq!(
            env.component.guard().in_flight("busy"),
            1,
            "never cancelled"
        );
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(
            env.component.guard().in_flight("busy"),
            0,
            "it finished by itself"
        );
    }
    // (c) an already cancelled token: stop returns at once.
    {
        let env = Setup::new().build();
        detached_call(&env, Duration::from_secs(10)).await;
        let token = CancellationToken::new();
        token.cancel();
        let started = Instant::now();
        bounded(env.component.stop(token))
            .await
            .expect("stop succeeds");
        assert_eq!(started.elapsed(), Duration::ZERO);
    }
}

#[tokio::test(start_paused = true)]
async fn stop_with_an_idle_guard_returns_at_once_even_with_a_live_token() {
    let mut setup = Setup::new();
    setup.audit("audit", 0);
    let env = setup.build();
    env.start().await.expect("start succeeds");
    let started = Instant::now();

    bounded(env.component.stop(CancellationToken::new()))
        .await
        .expect("stop succeeds");

    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(env.holder.is_stopping());
    assert!(!env.holder.is_ready());
}

// A stop without a successful start (the host stops every started gear; a failed
// start may still be followed by one) sets the flag and succeeds.
#[tokio::test(start_paused = true)]
async fn stop_after_a_failed_start_succeeds() {
    let env = Setup::new().build();
    env.failed_start("no_audit_plugin").await;

    bounded(env.component.stop(CancellationToken::new()))
        .await
        .expect("stop succeeds");

    assert!(env.holder.is_stopping());
    assert!(!env.holder.is_ready());
}

// ---------------------------------------------------------------- added after review

// F-013: the whole i16 range is a readable priority, for both plugin families; another
// family's unreadable priority is ignored (see also the listing test).
#[tokio::test(start_paused = true)]
async fn i16_extremes_are_readable_priorities() {
    let mut setup = Setup::new();
    setup.section("low", i16::MIN, "low");
    setup.section("high", i16::MAX, "high");
    setup.section("zero", 0, "zero");
    setup.audit("audit", i16::MAX);
    let env = setup.build();

    env.start().await.expect("extreme priorities are readable");

    let result = env.holder.result().expect("published");
    assert_eq!(result.section_keys(), vec!["low", "zero", "high"]);
}

// F-018, DESIGN §3.2 "never filters plugin instances by vendor": every vendor is used.
#[tokio::test(start_paused = true)]
async fn instances_of_every_vendor_are_used() {
    let mut setup = Setup::new();
    setup.section_with_vendor("a", 1, "a", "acme");
    setup.section_with_vendor("b", 2, "b", "other");
    setup.section_with_vendor("c", 3, "c", "");
    setup.audit_with_vendor("audit", 0, "third");
    let env = setup.build();

    env.start().await.expect("start succeeds");

    let result = env.holder.result().expect("published");
    assert_eq!(result.section_keys(), vec!["a", "b", "c"]);
}

// F-016: the start phase delays the gears after it by at most two limits (listing, then
// concurrent keys): each at its full limit is accepted, and takes exactly two.
#[tokio::test(start_paused = true)]
async fn start_takes_at_most_two_limits() {
    let mut setup = Setup::new().with_listing(Listing::Delayed(TIMEOUT - Duration::from_millis(1)));
    let a = setup.section("a", 1, "a");
    let b = setup.section("b", 2, "b");
    for s in [&a, &b] {
        s.fake.on(
            SectionCall::Key,
            CallBehavior::ok().with_delay(TIMEOUT - Duration::from_millis(1)),
        );
    }
    setup.audit("audit", 0);
    let env = setup.build();
    let started = Instant::now();

    env.start()
        .await
        .expect("both phases just inside their limit");

    assert_eq!(started.elapsed(), (TIMEOUT - Duration::from_millis(1)) * 2);
    assert!(started.elapsed() < TIMEOUT * 2);
}

// F-020: `start`'s cancel token is not consulted: a cancelled token changes nothing,
// and no plugin work is cancelled.
#[tokio::test(start_paused = true)]
async fn start_ignores_a_cancelled_token() {
    let mut setup = Setup::new();
    let net = setup.section("net", 1, "network");
    setup.audit("audit", 0);
    let env = setup.build();
    let cancel = CancellationToken::new();
    cancel.cancel();

    bounded(env.component.start(cancel))
        .await
        .expect("a cancelled token does not stop startup");

    assert!(env.holder.is_ready());
    expect_key_calls(&[&net], 1);
}

// F-005, DESIGN §3.2/§4.6: a `key` call that the guard did not send (the plugin is at
// its in-flight cap) is `section_key_unavailable` too, and never reaches the plugin.
#[tokio::test(start_paused = true)]
async fn key_call_not_sent_at_the_cap_fails_start() {
    let (logs, _guard) = capture_logs();
    let mut config = base_config();
    config.plugin_max_in_flight = 1;
    let mut setup = Setup::new().with_config(config);
    let healthy = setup.section("healthy", 1, "healthy");
    let held = setup.section("held", 2, "held");
    setup.audit("audit", 0);
    let env = setup.build();
    // Hold the only slot of `held` with a detached call that outlives its caller's wait.
    let guard = env.component.guard().clone();
    let outcome = bounded(guard.call(&held.id, apply_meta(), async {
        tokio::time::sleep(Duration::from_hours(1)).await;
        Ok::<(), ()>(())
    }))
    .await;
    assert_eq!(outcome, Err(PluginCallError::TimedOut));
    assert_eq!(guard.in_flight(&held.id), 1);

    env.failed_start("section_key_unavailable").await;

    assert_eq!(
        held.fake.call_count(),
        0,
        "a refused key call is never sent"
    );
    expect_key_calls(&[&healthy], 1);
    assert_eq!(env.last_start_elapsed(), Duration::ZERO, "refused at once");
    assert_failure_event(
        &logs,
        "section_key_unavailable",
        &[&held.id],
        &[&healthy.id],
    );
}

// F-021: a `key` result that arrives after start gave up is logged once as a late
// result, labelled as a section `key` call of no request and no section key yet.
#[tokio::test(start_paused = true)]
async fn late_key_result_is_logged_with_key_labels() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let slow = setup.section("slow", 1, "slow");
    slow.fake.on(
        SectionCall::Key,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    setup.audit("audit", 0);
    let env = setup.build();

    env.failed_start("section_key_unavailable").await;

    // `failed_start` moved the clock 60s: the late result is in.
    let lines = logs.lines_with("plugin_call_late_result");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "plugin_kind", "section"),
        "{}",
        lines[0]
    );
    assert!(has_field(&lines[0], "call_kind", "key"), "{}", lines[0]);
    assert!(has_field(&lines[0], "cause", "timeout"), "{}", lines[0]);
    assert!(!lines[0].contains("request_id"), "{}", lines[0]);
}

// ---------------------------------------------------------------- review fixes (P3)

// F-003: the type ids and the listing pattern come from the SDK specs and equal the
// documented ids (DESIGN §3.1).
#[test]
fn plugin_type_ids_equal_the_documented_ids() {
    use crate::infra::startup::{audit_type, plugin_pattern, section_type};
    assert_eq!(
        section_type(),
        "gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~"
    );
    assert_eq!(
        audit_type(),
        "gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_audit.plugin.v1~"
    );
    assert_eq!(plugin_pattern(), "gts.cf.toolkit.plugins.plugin.v1~*");
}

// F-002: a second start fails before any call, logs one event, and the gear stays ready.
#[tokio::test(start_paused = true)]
async fn second_start_fails_with_already_started_before_any_call() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let net = setup.section("net", 1, "network");
    setup.audit("audit", 0);
    let env = setup.build();
    env.start().await.expect("first start");
    let registry_calls = env.registry.total_calls();
    let key_calls = net.fake.call_count();

    let text = format!("{}", env.start().await.expect_err("second start fails"));

    assert!(text.starts_with("already_started:"), "{text}");
    assert_eq!(
        env.registry.total_calls(),
        registry_calls,
        "no registry call"
    );
    assert_eq!(net.fake.call_count(), key_calls, "no plugin call");
    assert!(env.holder.is_ready(), "the first result stays published");
    assert_failure_event(&logs, "already_started", &[], &[&net.id]);
}

// F-005: one `startup_published` info event with the counts and the elapsed time.
#[tokio::test(start_paused = true)]
async fn start_logs_one_startup_published_event() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    setup.section("net", 1, "network");
    setup.section("dns", 2, "dns");
    let audit = setup.audit("audit", 0);
    let env = setup.build();
    logs.assert_live();
    env.start().await.expect("start");

    let lines = logs.lines_with("startup_published");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("INFO"), "{}", lines[0]);
    assert!(has_field(&lines[0], "sections", "2"), "{}", lines[0]);
    assert!(has_field(&lines[0], "audit", &audit.id), "{}", lines[0]);
    assert!(
        field_value(&lines[0], "elapsed_ms").is_some(),
        "{}",
        lines[0]
    );
}

// F-004: a key call's late event keeps the span `start` ran in.
#[tokio::test(start_paused = true)]
async fn key_call_events_keep_the_start_span() {
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    let slow = setup.section("slow", 1, "slow");
    slow.fake.on(
        SectionCall::Key,
        CallBehavior::ok().with_delay(Duration::from_secs(2)),
    );
    setup.audit("audit", 0);
    let env = setup.build();

    let span = tracing::info_span!("start_span");
    let result = tracing::Instrument::instrument(env.start(), span).await;
    assert!(result.is_err());
    tokio::time::sleep(Duration::from_secs(5)).await;

    let lines = logs.lines_with("plugin_call_late_result");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("start_span"), "{}", lines[0]);
}

// F-007: the instances field is bounded; a failing listing returns a fixed text and
// logs the cause as a field.
#[tokio::test(start_paused = true)]
async fn instances_field_is_bounded() {
    use crate::domain::limits::LOG_TEXT_MAX_BYTES;
    let (logs, _guard) = capture_logs();
    let mut setup = Setup::new();
    for i in 0..30 {
        setup.audit(&format!("audit{i}{}", "x".repeat(60)), i);
    }
    let env = setup.build();
    env.failed_start("multiple_audit_plugins").await;

    let lines = logs.lines_with(FAILED);
    assert_eq!(lines.len(), 1, "{lines:?}");
    let instances = field_value(&lines[0], "instances").expect("instances");
    assert!(!instances.is_empty());
    assert!(instances.len() <= LOG_TEXT_MAX_BYTES, "{}", instances.len());
}

#[test]
fn bounded_log_text_escapes_and_cuts_at_a_character_boundary() {
    use crate::domain::limits::LOG_TEXT_MAX_BYTES;
    use crate::infra::startup::bounded_log_text;
    assert_eq!(bounded_log_text("a\nb"), "a\\nb");
    let long = "\u{e9}".repeat(LOG_TEXT_MAX_BYTES);
    let cut = bounded_log_text(&long);
    assert!(cut.len() <= LOG_TEXT_MAX_BYTES);
    assert!(cut.chars().all(|c| c == '\u{e9}'), "cut on a boundary");
    assert_eq!(bounded_log_text("short"), "short");
}

#[tokio::test(start_paused = true)]
async fn failing_listing_returns_fixed_text_and_logs_the_cause() {
    let (logs, _guard) = capture_logs();
    let env = Setup::new().with_listing(Listing::Failing).build();
    let text = env.failed_start("types_registry_unreachable").await;
    assert_eq!(text, "types_registry_unreachable: listing failed");
    let lines = logs.lines_with(FAILED);
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("cause="), "{}", lines[0]);
}
