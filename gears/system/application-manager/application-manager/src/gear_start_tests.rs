//! Unit tests for the gear's hand-implemented start capability (DESIGN §3.2 "Startup
//! and Registration": "through a hand-implemented start capability", §3.4: "the
//! capabilities `db`, `rest`, and `stateful`" arrive with their phases, §4.12 phasing).
//!
//! Planned API in `crate::gear`:
//!
//! ```text
//! // #[toolkit::gear(name = "application-manager", deps = [types_registry, authz_resolver],
//! //                capabilities = [stateful])]
//! impl RunnableCapability for ApplicationManagerGear {
//!     // Delegates to the StartupComponent that `init` built from the validated config,
//!     // the context's ClientHub and a fresh StartupResultHolder. Errors before `init`.
//!     async fn start(&self, cancel: CancellationToken) -> anyhow::Result<()>;
//!     // Before `init` there is nothing to stop: Ok.
//!     async fn stop(&self, deadline_token: CancellationToken) -> anyhow::Result<()>;
//! }
//! impl ApplicationManagerGear {
//!     /// The startup result holder; `None` until `init` succeeded.
//!     pub fn startup_holder(&self) -> Option<Arc<StartupResultHolder>>;
//! }
//! ```
//!
//! `init` still performs no discovery and resolves nothing from the hub (the P1 tests
//! in `gear_tests.rs` run it on an empty hub); everything is resolved in `start`.
//!
//! - `gear_declares_the_stateful_capability_and_unchanged_deps` (§3.2, §3.4)
//! - `start_before_init_fails` (§3.2)
//! - `init_then_start_publishes_the_startup_result` (§3.2)
//! - `failed_start_returns_the_reason_through_the_gear` (§3.2 fault handling)
//! - `init_performs_no_discovery`, `configured_plugin_call_timeout_bounds_the_start`,
//!   `registered_capability_is_the_gears_start` (F-023)
//! - `stop_before_init_succeeds` / `stop_after_start_sets_stopping` (§3.2 stop)
//!
//! P6 (registry store): `init` builds the registry store from the context's database handle
//! and `start` fails at once with `database_unavailable` when there is none (see
//! `gear_registry_tests.rs`), so every context in this file carries a migrated test
//! database: `ctx`/`ctx_with` are `async` and return the `RegistryTestDb` to keep alive.

use std::sync::Arc;

use application_manager_sdk::{AuditPluginClientV1, SectionPluginClientV1};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use toolkit::client_hub::{ClientHub, ClientScope};
use toolkit::config::ConfigProvider;
use toolkit::registry::{GearRegistry, RunnableCap};
use toolkit::{Gear, RunnableCapability};
use types_registry_sdk::TypesRegistryClient;
use uuid::Uuid;

use crate::domain::ports::StartupPort;
use crate::gear::ApplicationManagerGear;
use crate::test_support::registry_db::{self, RegistryTestDb};
use crate::test_support::{
    CallBehavior, FakeAuditPlugin, FakeSectionPlugin, FakeTypesRegistry, PluginFamily, SectionCall,
    bounded, plugin_instance,
};

const GEAR_NAME: &str = "application-manager";

struct StaticConfigProvider {
    root: Value,
}

impl ConfigProvider for StaticConfigProvider {
    fn get_gear_config(&self, gear: &str) -> Option<&Value> {
        self.root.get(gear)
    }
}

// The database is created on the real clock (real file IO cannot run under a paused clock:
// the idle runtime would jump over the pool's timeouts); only afterwards does the clock stop.
// A clock paused mid-run is not aligned to the timer wheel (an `advance(ZERO)` right after the
// pause does not align it: a 100ms timeout then still elapses 101ms), so a later timeout elapses
// its length plus up to one millisecond of rounding (`ROUNDING`), and no more.
fn pause_clock() {
    tokio::time::pause();
}

const ROUNDING: Duration = Duration::from_millis(1);

fn valid_config() -> Value {
    json!({
        "plugin_call_timeout": "500ms",
        "plugin_max_in_flight": 8,
        "max_request_tasks": 16,
        "max_envelope_text_length": 255,
        "pdp_call_timeout": "2s",
        "registry_operation_timeout": "3s"
    })
}

// A context whose hub holds a fake types registry listing one section plugin and, when
// `with_audit`, one audit plugin, with their scoped clients, and a migrated test database
// (returned: it must outlive the gear).
async fn ctx(with_audit: bool) -> (toolkit::GearCtx, RegistryTestDb) {
    let (ctx, _registry, _section, db) = ctx_with(with_audit, valid_config()).await;
    (ctx, db)
}

// Also returns the fake registry and the section fake, for call counts and scripting.
async fn ctx_with(
    with_audit: bool,
    config: Value,
) -> (
    toolkit::GearCtx,
    Arc<FakeTypesRegistry>,
    Arc<FakeSectionPlugin>,
    RegistryTestDb,
) {
    let hub = Arc::new(ClientHub::new());
    let mut instances = Vec::new();
    let (section_id, section) = plugin_instance(PluginFamily::Section, "acme.net.core.net.v1", 1);
    instances.push(section);
    let section_fake = Arc::new(FakeSectionPlugin::new("network"));
    let section_client: Arc<dyn SectionPluginClientV1> = section_fake.clone();
    hub.register_scoped::<dyn SectionPluginClientV1>(
        ClientScope::gts_id(&section_id),
        section_client,
    );
    if with_audit {
        let (audit_id, audit) = plugin_instance(PluginFamily::Audit, "acme.audit.core.a.v1", 0);
        instances.push(audit);
        let audit_client: Arc<dyn AuditPluginClientV1> = Arc::new(FakeAuditPlugin::new());
        hub.register_scoped::<dyn AuditPluginClientV1>(
            ClientScope::gts_id(&audit_id),
            audit_client,
        );
    }
    let registry = Arc::new(FakeTypesRegistry::new().with_instances(instances));
    let registry_client: Arc<dyn TypesRegistryClient> = registry.clone();
    hub.register::<dyn TypesRegistryClient>(registry_client);
    let db = registry_db::sqlite().await;
    let ctx = toolkit::GearCtx::new(
        GEAR_NAME,
        Uuid::new_v4(),
        Arc::new(StaticConfigProvider {
            root: json!({ GEAR_NAME: { "config": config } }),
        }),
        hub,
        CancellationToken::new(),
    )
    .with_db(db.provider.clone());
    (ctx, registry, section_fake, db)
}

#[test]
fn gear_declares_the_stateful_capability_and_unchanged_deps() {
    let registry = GearRegistry::discover_and_build().expect("the inventory registry builds");

    let entry = registry
        .gears()
        .iter()
        .find(|e| e.name() == GEAR_NAME)
        .expect("application-manager is registered");

    assert!(
        entry.caps().has::<RunnableCap>(),
        "the gear declares the stateful capability"
    );
    assert_eq!(entry.deps(), ["types-registry", "authz-resolver"]);
}

#[tokio::test]
async fn start_before_init_fails() {
    let gear = ApplicationManagerGear::default();

    let result = gear.start(CancellationToken::new()).await;

    assert!(
        result.is_err(),
        "a gear that was not initialised cannot start"
    );
    assert!(gear.startup_holder().is_none());
}

#[tokio::test]
async fn init_then_start_publishes_the_startup_result() {
    let (ctx, _db) = ctx(true).await;
    pause_clock();
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");
    let holder = gear.startup_holder().expect("holder exists after init");
    assert!(
        !holder.is_ready(),
        "not ready until start published the result"
    );

    bounded(gear.start(CancellationToken::new()))
        .await
        .expect("start succeeds");

    assert!(holder.is_ready());
    let result = holder.result().expect("published");
    assert_eq!(result.section_keys(), vec!["network"]);
}

// DESIGN §3.2: the error of a failed step reaches the host with its reason token.
#[tokio::test]
async fn failed_start_returns_the_reason_through_the_gear() {
    let (ctx, _db) = ctx(false).await;
    pause_clock();
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");

    let err = gear
        .start(CancellationToken::new())
        .await
        .expect_err("a plugin set with no audit plugin must not start");

    assert!(
        format!("{err}").starts_with("no_audit_plugin"),
        "reason token first, got: {err}"
    );
    assert!(!gear.startup_holder().expect("holder").is_ready());
}

#[tokio::test]
async fn stop_before_init_succeeds() {
    let gear = ApplicationManagerGear::default();

    gear.stop(CancellationToken::new())
        .await
        .expect("nothing to stop before init");
}

#[tokio::test]
async fn stop_after_start_sets_stopping() {
    let (ctx, _db) = ctx(true).await;
    pause_clock();
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");
    gear.start(CancellationToken::new())
        .await
        .expect("start succeeds");

    gear.stop(CancellationToken::new())
        .await
        .expect("stop succeeds");

    let holder = gear.startup_holder().expect("holder");
    assert!(holder.is_stopping());
    assert!(!holder.is_ready());
}

// F-023: `init` performs no discovery: nothing is listed and no client is called.
#[tokio::test]
async fn init_performs_no_discovery() {
    let (ctx, registry, section, _db) = ctx_with(true, valid_config()).await;
    pause_clock();
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx).await.expect("init succeeds");

    assert_eq!(registry.total_calls(), 0, "init makes no registry call");
    assert_eq!(section.call_count(), 0, "init makes no plugin call");
}

// F-023: the validated `plugin_call_timeout` reaches the guard and the registry bound:
// with 100ms and a section plugin whose `key` never answers, start fails after 100ms.
#[tokio::test]
async fn configured_plugin_call_timeout_bounds_the_start() {
    let mut config = valid_config();
    config["plugin_call_timeout"] = json!("100ms");
    let (ctx, _registry, section, _db) = ctx_with(true, config).await;
    pause_clock();
    section.on(SectionCall::Key, CallBehavior::hang());
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");
    let started = Instant::now();

    let err = bounded(gear.start(CancellationToken::new()))
        .await
        .expect_err("a hanging key fails start");

    assert!(
        format!("{err}").starts_with("section_key_unavailable:"),
        "{err}"
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(100) && elapsed <= Duration::from_millis(100) + ROUNDING,
        "one plugin_call_timeout, no more: {elapsed:?}"
    );
}

// F-023: the capability the gear macro registers is this gear's own start: through the
// registry entry, a gear that was not initialised refuses to start (a no-op capability
// would answer Ok).
#[tokio::test]
async fn registered_capability_is_the_gears_start() {
    let registry = GearRegistry::discover_and_build().expect("the inventory registry builds");
    let entry = registry
        .gears()
        .iter()
        .find(|e| e.name() == GEAR_NAME)
        .expect("application-manager is registered");
    let capability = entry
        .caps()
        .query::<RunnableCap>()
        .expect("the stateful capability is registered");

    let started = capability.start(CancellationToken::new()).await;

    assert!(
        started.is_err(),
        "a gear that was not initialised cannot start"
    );
}
