//! Unit tests for the gear's registry store wiring (DESIGN §3.2 "Ports and Composition":
//! "`gear.rs` is the composition root ... In `init` it validates the configuration ... and
//! builds the policy adapter, the registry store, and the orchestrator", §3.7, §4.5 and §4.7
//! `registry_operation_timeout`, §4.6 startup failure catalogue, ADR 0001, ADR 0007).
//!
//! Planned API in `crate::gear`:
//!
//! ```text
//! impl ApplicationManagerGear {
//!     /// The request orchestrator `init` built (F-007): over `PolicyAdapter::from_hub(client_hub,
//!     /// pdp_call_timeout)`, the registry store on the context's database handle with
//!     /// `registry_operation_timeout`, the startup holder, the system clock and UUIDv7 request
//!     /// ids. `None` until `init` succeeded, and `None` when the context carried no database
//!     /// handle. The only accessor; the tests below read the store and the policy port
//!     /// through `Orchestrator::registry()` / `policy()` (test-only).
//!     pub(crate) fn orchestrator(&self) -> Option<&Orchestrator>;
//! }
//! ```
//!
//! `init` with a database handle builds `SeaRegistryStore::new(db, registry_operation_timeout)`.
//! `init` with NO handle still succeeds (the P1-P3 tests), builds no store, and makes the
//! start capability fail at once with `database_unavailable` (see `startup_database_tests`):
//! the failure is a start-phase fault with the §4.6 reason, with no retry.
//!
//! - `init_with_a_database_builds_the_registry_store_over_that_database`
//! - `the_store_is_bounded_by_the_configured_registry_operation_timeout`
//! - `init_without_a_database_builds_no_store`
//! - `start_without_a_database_fails_at_once_with_database_unavailable` (through the gear)
//! - `start_with_a_database_passes_the_database_check`
//! - `a_second_init_fails_and_keeps_the_first_store`
//! - `the_store_is_not_available_before_init`
//! - `init_with_an_invalid_config_builds_no_store`
//! - `a_published_result_wins_over_the_database_check`, `stop_still_succeeds_after_a_start_that_failed_for_the_database`
//! - `init_builds_the_orchestrator_with_the_production_clock_and_uuid_v7_ids`
//! - `init_succeeds_with_no_authz_client_and_builds_the_policy_port`,
//!   `the_policy_port_is_not_available_before_init`,
//!   `the_policy_port_is_bounded_by_the_configured_pdp_call_timeout`
#![allow(clippy::duration_suboptimal_units, clippy::non_ascii_literal)]

use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use toolkit::config::ConfigProvider;
use toolkit::{ClientHub, Gear, GearCtx, RunnableCapability};
use types_registry_sdk::TypesRegistryClient;
use uuid::Uuid;

use crate::domain::orchestrator::{GateOutcome, Orchestrator, RequestInput};
use crate::domain::ports::{
    PdpUnavailable, PolicyAction, PolicyDecision, PolicyPort, RegistryError, RegistryStore,
    StartupPort,
};
use crate::domain::registry::{LastResult, RegistryEntry};
use crate::gear::ApplicationManagerGear;
use crate::test_support::registry_db::{self, RegistryTestDb};
use crate::test_support::{
    AuthzScript, FakeAuthz, FakeTypesRegistry, bounded, capture_logs, field_value, has_field,
    security_context, startup_result_without_sections,
};
use authz_resolver_sdk::AuthZResolverApi;

const GEAR_NAME: &str = "application-manager";

/// The store and the policy port, read through the one accessor, the orchestrator.
trait ThroughOrchestrator {
    fn registry_store(&self) -> Option<Arc<dyn RegistryStore>>;
    fn policy_port(&self) -> Option<Arc<dyn PolicyPort>>;
}

impl ThroughOrchestrator for ApplicationManagerGear {
    fn registry_store(&self) -> Option<Arc<dyn RegistryStore>> {
        self.orchestrator().map(Orchestrator::registry)
    }

    fn policy_port(&self) -> Option<Arc<dyn PolicyPort>> {
        self.orchestrator().map(Orchestrator::policy)
    }
}

struct StaticConfigProvider {
    root: Value,
}

impl ConfigProvider for StaticConfigProvider {
    fn get_gear_config(&self, gear: &str) -> Option<&Value> {
        self.root.get(gear)
    }
}

fn config(registry_timeout: &str) -> Value {
    config_with("500ms", "2s", registry_timeout)
}

// Three different timeouts, so a store or adapter bounded by the wrong setting shows.
fn config_with(plugin: &str, pdp: &str, registry_timeout: &str) -> Value {
    json!({
        "plugin_call_timeout": plugin,
        "plugin_max_in_flight": 8,
        "max_request_tasks": 16,
        "max_envelope_text_length": 255,
        "pdp_call_timeout": pdp,
        "registry_operation_timeout": registry_timeout,
    })
}

fn bare_ctx(config: Value) -> (GearCtx, Arc<FakeTypesRegistry>) {
    let hub = Arc::new(ClientHub::new());
    let registry = Arc::new(FakeTypesRegistry::new());
    let client: Arc<dyn TypesRegistryClient> = registry.clone();
    hub.register::<dyn TypesRegistryClient>(client);
    let ctx = GearCtx::new(
        GEAR_NAME,
        Uuid::new_v4(),
        Arc::new(StaticConfigProvider {
            root: json!({ GEAR_NAME: { "config": config } }),
        }),
        hub,
        CancellationToken::new(),
    );
    (ctx, registry)
}

async fn ctx_with_db(config: Value) -> (GearCtx, Arc<FakeTypesRegistry>, RegistryTestDb) {
    let db = registry_db::sqlite().await;
    let (ctx, registry) = bare_ctx(config);
    (ctx.with_db(db.provider.clone()), registry, db)
}

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

#[test]
fn the_store_is_not_available_before_init() {
    assert!(ApplicationManagerGear::default().registry_store().is_none());
}

#[tokio::test]
async fn init_with_a_database_builds_the_registry_store_over_that_database() {
    let (ctx, _registry, db) = ctx_with_db(config("3s")).await;
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx).await.expect("init succeeds");

    let store = gear
        .registry_store()
        .expect("init built the registry store");
    store
        .upsert(&entry("app-1", "1.0"))
        .await
        .expect("upsert through the gear's store");
    // The row is in the database the context carried: a store built by the test over the
    // same provider sees it.
    let seen = db
        .store(Duration::from_secs(3))
        .get("app-1")
        .await
        .expect("get")
        .expect("the row written through the gear's store");
    assert_eq!(seen.version(), "1.0");
    assert_eq!(store.get("app-1").await.expect("get"), Some(seen));
    assert_eq!(store.get("absent").await.expect("get"), None);
}

// DESIGN §4.7: `registry_operation_timeout` bounds every registry operation on every backend.
// A second connection holds the write lock the upsert needs; the gear's store stops at the
// configured bound (real clock: a paused clock cannot coexist with real database IO). The
// plugin and PDP timeouts are 100ms and the registry's is 1500ms, so a store bounded by either
// of the others (or by a fixed value, or a fraction of it) ends before 1500ms or never: the
// lower bound tells which setting bounds it. The row is seeded through a store with a roomy
// bound (5s), so only the contended upsert depends on the gear's store and a slow start cannot
// make the setup fail.
#[tokio::test]
async fn the_store_is_bounded_by_the_configured_registry_operation_timeout() {
    let (ctx, _registry, db) = ctx_with_db(config_with("100ms", "100ms", "1500ms")).await;
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");
    let store = gear.registry_store().expect("store");
    db.store(Duration::from_secs(5))
        .upsert(&entry("locked", "1"))
        .await
        .expect("seed the row");
    let blocker = registry_db::hold_write_lock(&db, "locked").await;
    let started = Instant::now();

    let result = tokio::time::timeout(Duration::from_secs(30), store.upsert(&entry("locked", "2")))
        .await
        .expect("the store must answer within its own bound");

    let elapsed = started.elapsed();
    assert_eq!(result, Err(RegistryError::TimedOut));
    assert!(
        elapsed >= Duration::from_millis(1500),
        "bounded by registry_operation_timeout, not by the 100ms settings: {elapsed:?}"
    );
    assert!(elapsed < Duration::from_secs(10), "{elapsed:?}");
    blocker.rollback().await.expect("release the lock");
}

#[tokio::test]
async fn init_without_a_database_builds_no_store() {
    let (ctx, _registry) = bare_ctx(config("3s"));
    assert!(ctx.db().is_none(), "the context carries no database");
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx)
        .await
        .expect("init still succeeds (§4.6: `start` reports the fault)");

    assert!(gear.registry_store().is_none());
}

#[tokio::test(start_paused = true)]
async fn start_without_a_database_fails_at_once_with_database_unavailable() {
    let (logs, _guard) = capture_logs();
    let (ctx, registry) = bare_ctx(config("3s"));
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");
    let started = tokio::time::Instant::now();

    let err = gear
        .start(CancellationToken::new())
        .await
        .expect_err("start must fail without a database");

    assert!(
        format!("{err}").starts_with("database_unavailable: "),
        "{err}"
    );
    assert_eq!(started.elapsed(), Duration::ZERO, "at once, no retry");
    assert_eq!(registry.total_calls(), 0, "before any discovery call");
    assert!(
        !gear.startup_holder().expect("holder").is_ready(),
        "not ready"
    );
    logs.assert_live();
    let lines = logs.lines_with("startup_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "reason", "database_unavailable"),
        "{}",
        lines[0]
    );
    assert_eq!(field_value(&lines[0], "instances").unwrap_or_default(), "");
}

#[tokio::test]
async fn start_with_a_database_passes_the_database_check() {
    let (ctx, registry, _db) = ctx_with_db(config("3s")).await;
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");

    let err = gear
        .start(CancellationToken::new())
        .await
        .expect_err("an empty plugin set has no audit plugin");

    assert!(
        format!("{err}").starts_with("no_audit_plugin"),
        "the failure is the next startup step's, so the database check passed: {err}"
    );
    assert_eq!(registry.list_calls(), 1);
}

#[tokio::test]
async fn a_second_init_fails_and_keeps_the_first_store() {
    let (ctx, _registry, _db) = ctx_with_db(config("3s")).await;
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("first init");
    let first = gear.registry_store().expect("store");

    gear.init(&ctx).await.expect_err("a second init must fail");

    assert!(
        Arc::ptr_eq(&first, &gear.registry_store().expect("store")),
        "the first store stays"
    );
}

#[tokio::test]
async fn init_with_an_invalid_config_builds_no_store() {
    let mut bad = config("3s");
    bad["plugin_max_in_flight"] = json!(0);
    let (ctx, _registry, _db) = ctx_with_db(bad).await;
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx)
        .await
        .expect_err("an invalid setting fails init");

    assert!(gear.registry_store().is_none());
}

// DESIGN §3.2 "Startup and Registration": `already_started` is checked first, so a published
// result wins over the missing database, at the gear level too.
#[tokio::test(start_paused = true)]
async fn a_published_result_wins_over_the_database_check() {
    let (ctx, registry) = bare_ctx(config("3s"));
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");
    gear.startup_holder()
        .expect("holder")
        .publish_result(startup_result_without_sections())
        .expect("published once");

    let err = gear
        .start(CancellationToken::new())
        .await
        .expect_err("a second start must fail");

    assert!(
        format!("{err}").starts_with("already_started"),
        "already_started, not database_unavailable: {err}"
    );
    assert_eq!(registry.total_calls(), 0);
    assert!(gear.startup_holder().expect("holder").is_ready());
}

#[tokio::test(start_paused = true)]
async fn stop_still_succeeds_after_a_start_that_failed_for_the_database() {
    let (ctx, _registry) = bare_ctx(config("3s"));
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");
    gear.start(CancellationToken::new())
        .await
        .expect_err("start fails without a database");

    gear.stop(CancellationToken::new())
        .await
        .expect("stop succeeds");

    assert!(gear.startup_holder().expect("holder").is_stopping());
}

#[test]
fn the_policy_port_is_not_available_before_init() {
    assert!(ApplicationManagerGear::default().policy_port().is_none());
}

// DESIGN §3.2 "Ports and Composition": `init` builds the policy adapter over the client hub; the
// authz client is resolved at each decision, so `init` needs none (the platform may register it
// later). With no client the adapter cannot answer; once one is registered it answers.
#[tokio::test]
async fn init_succeeds_with_no_authz_client_and_builds_the_policy_port() {
    let (ctx, _registry, _db) = ctx_with_db(config("3s")).await;
    assert!(
        ctx.client_hub().get::<dyn AuthZResolverApi>().is_err(),
        "the hub holds no authz client"
    );
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx).await.expect("init needs no authz client");

    let port = gear.policy_port().expect("init built the policy port");
    assert_eq!(
        port.decide(&security_context(), PolicyAction::Seed, None)
            .await,
        PolicyDecision::Unavailable(PdpUnavailable::Unreachable),
        "no authz client yet"
    );
    let authz = Arc::new(FakeAuthz::new(AuthzScript::Permit(vec![])));
    ctx.client_hub()
        .register::<dyn AuthZResolverApi>(Arc::clone(&authz) as Arc<dyn AuthZResolverApi>);
    assert_eq!(
        port.decide(&security_context(), PolicyAction::Seed, Some("app"))
            .await,
        PolicyDecision::Permit,
        "the client registered after init is used"
    );
    assert_eq!(authz.requests().len(), 1);
}

// The deadline of the policy port is `pdp_call_timeout` (not the plugin or registry timeout):
// a PDP that never answers is `timed_out` after exactly 100ms here.
#[tokio::test]
async fn the_policy_port_is_bounded_by_the_configured_pdp_call_timeout() {
    // The database is created on the real clock; only then is the clock paused.
    let (ctx, _registry, _db) = ctx_with_db(config_with("500ms", "100ms", "3s")).await;
    tokio::time::pause();
    let authz = Arc::new(FakeAuthz::new(AuthzScript::Hang));
    ctx.client_hub()
        .register::<dyn AuthZResolverApi>(Arc::clone(&authz) as Arc<dyn AuthZResolverApi>);
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");
    let port = gear.policy_port().expect("policy port");
    let started = tokio::time::Instant::now();

    let decision = bounded(port.decide(&security_context(), PolicyAction::Seed, None)).await;

    assert_eq!(
        decision,
        PolicyDecision::Unavailable(PdpUnavailable::TimedOut)
    );
    // The clock is paused after real time passed, so the timer wheel may round up one tick.
    let elapsed = started.elapsed();
    assert!(
        (Duration::from_millis(100)..=Duration::from_millis(101)).contains(&elapsed),
        "{elapsed:?}"
    );
}

// F-007: `init` builds the orchestrator, with the production clock and UUIDv7 request ids, over
// the gear's own startup holder. A permitted uninstall of a seeded application proceeds with
// a request id of version 7 and an accepted hand-off.
#[tokio::test]
async fn init_builds_the_orchestrator_with_the_production_clock_and_uuid_v7_ids() {
    let (ctx, _registry, db) = ctx_with_db(config("3s")).await;
    let authz = Arc::new(FakeAuthz::new(AuthzScript::Permit(vec![])));
    ctx.client_hub()
        .register::<dyn AuthZResolverApi>(Arc::clone(&authz) as Arc<dyn AuthZResolverApi>);
    let gear = ApplicationManagerGear::default();
    assert!(gear.orchestrator().is_none(), "nothing before init");
    gear.init(&ctx).await.expect("init succeeds");
    db.store(Duration::from_secs(5))
        .upsert(&entry("app-1", "2.0"))
        .await
        .expect("seed the row");
    gear.startup_holder()
        .expect("holder")
        .publish_result(startup_result_without_sections())
        .expect("published once");
    let orchestrator = gear.orchestrator().expect("init built the orchestrator");

    let first = orchestrator
        .pre_dispatch(
            &security_context(),
            RequestInput::Uninstall {
                application_id: "app-1".to_owned(),
            },
        )
        .await;
    let second = orchestrator
        .pre_dispatch(
            &security_context(),
            RequestInput::Uninstall {
                application_id: "app-1".to_owned(),
            },
        )
        .await;

    for result in [&first, &second] {
        assert_eq!(result.request_id.get_version_num(), 7, "UUIDv7 request id");
        match &result.outcome {
            GateOutcome::Proceed(p) => assert_eq!(p.envelope.version, "2.0"),
            GateOutcome::Decided(_) => panic!("expected a proceed: {result:?}"),
        }
        assert!(result.audit_id.is_some(), "the hand-off was accepted");
    }
    assert_ne!(first.request_id, second.request_id);
}
