#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use toolkit::config::ConfigProvider;
use toolkit::contracts::{DatabaseCapability, RestApiCapability};
use toolkit::{ClientHub, Gear, GearCtx, HealthcheckStatus};
use toolkit_db::migration_runner::run_migrations_for_testing;
use toolkit_db::{ConnectOpts, DBProvider, DbError, connect_db};
use uuid::Uuid;

use super::*;
use crate::domain::error::DomainError;
use crate::test_support::{
    PluginSpec, hub_with, hub_with_registry, platform_ctx, valid_config_json,
};
use types_registry_sdk::testing::{MockTypesRegistryClient, internal};

struct StaticConfigProvider {
    root: Value,
}

impl ConfigProvider for StaticConfigProvider {
    fn get_gear_config(&self, gear: &str) -> Option<&Value> {
        self.root.get(gear)
    }
}

fn valid_config() -> Value {
    valid_config_json()
}

/// A runtime of its own for database connections. Several tests here pause the
/// clock, and a connection opened on a paused clock times out in the pool, so
/// the connection is opened on a runtime whose clock runs.
fn db_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("database runtime")
    })
}

/// A fresh in-memory SQLite database, as the host hands one to the gear.
/// Migrations are the host's job and are not run here.
async fn test_db() -> DBProvider<DbError> {
    let dsn = format!(
        "sqlite:file:application_manager_gear_{}?mode=memory&cache=shared",
        Uuid::new_v4()
    );
    let db = db_runtime()
        .spawn(async move {
            connect_db(
                &dsn,
                ConnectOpts {
                    max_conns: Some(1),
                    min_conns: Some(1),
                    ..Default::default()
                },
            )
            .await
            .expect("connect sqlite")
        })
        .await
        .expect("database runtime task");
    DBProvider::new(db)
}

/// A context with no database configured.
fn ctx_without_db(root: Value, hub: Arc<ClientHub>) -> GearCtx {
    GearCtx::new(
        ApplicationManager::MODULE_NAME,
        Uuid::from_u128(1),
        Arc::new(StaticConfigProvider { root }),
        hub,
        CancellationToken::new(),
    )
}

async fn ctx_with(root: Value) -> GearCtx {
    ctx_with_hub(root, Arc::new(ClientHub::new())).await
}

async fn ctx_with_hub(root: Value, hub: Arc<ClientHub>) -> GearCtx {
    ctx_without_db(root, hub).with_db(test_db().await)
}

async fn ctx_with_config(config: &Value) -> GearCtx {
    ctx_with(json!({ "application-manager": { "config": config } })).await
}

#[tokio::test]
async fn init_valid_static_config_succeeds() {
    let gear = ApplicationManager::default();
    gear.init(&ctx_with_config(&valid_config()).await)
        .await
        .unwrap();
}

#[tokio::test]
async fn init_second_call_fails_already_initialized() {
    let gear = ApplicationManager::default();
    let ctx = ctx_with_config(&valid_config()).await;
    gear.init(&ctx).await.unwrap();
    let err = gear.init(&ctx).await.expect_err("second init");
    assert!(
        format!("{err:#}").contains("already initialized"),
        "{err:#}"
    );
}

#[tokio::test]
async fn init_invalid_config_fails_with_invalid_setting() {
    let mut missing = valid_config();
    missing.as_object_mut().unwrap().remove("pdp_call_timeout");
    let mut unknown = valid_config();
    unknown["surprise"] = json!(1);
    let mut bad_duration = valid_config();
    bad_duration["plugin_call_timeout"] = json!("soon");
    let mut zero = valid_config();
    zero["max_request_tasks"] = json!(0);
    let mut shared_caller = valid_config();
    shared_caller["allowed_callers"][0]["identity"] = json!({ "type": "Shared", "name": "dev" });
    let mut no_actions = valid_config();
    no_actions["allowed_callers"][0]["actions"] = json!([]);
    let cases = vec![
        ("missing_gear_block", ctx_with(json!({})).await),
        (
            "missing_config_key",
            ctx_with(json!({ "application-manager": {} })).await,
        ),
        ("missing_field", ctx_with_config(&missing).await),
        ("unknown_field", ctx_with_config(&unknown).await),
        ("bad_duration", ctx_with_config(&bad_duration).await),
        ("invalid_value", ctx_with_config(&zero).await),
        ("shared_caller", ctx_with_config(&shared_caller).await),
        ("caller_without_actions", ctx_with_config(&no_actions).await),
    ];
    for (name, ctx) in cases {
        let gear = ApplicationManager::default();
        let err = gear.init(&ctx).await.expect_err(name);
        let text = format!("{err:#}");
        assert!(text.contains("invalid_setting"), "{name}: {text}");
        let prefix = match name {
            "invalid_value" => "application-manager: invalid_setting: max_request_tasks: ",
            "caller_without_actions" => "application-manager: invalid_setting: allowed_callers: ",
            _ => "application-manager: invalid_setting: config: ",
        };
        assert!(text.starts_with(prefix), "{name}: {text}");
        assert!(!text.contains("invalid config for gear"), "{name}: {text}");
        let err = gear.start(CancellationToken::new()).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("init must run before start"),
            "{name}: {err:#}"
        );
    }
}

#[tokio::test]
async fn start_before_init_fails() {
    let gear = ApplicationManager::default();
    let err = gear.start(CancellationToken::new()).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("init must run before start"),
        "{err:#}"
    );
}

#[tokio::test]
async fn start_publishes_the_plugin_set_and_stop_succeeds() {
    let gear = ApplicationManager::default();
    let plugins = [PluginSpec::new(
        "cf.test.a.plugin.v1",
        1,
        "cf.test.a.section.v1",
    )];
    let root = json!({ "application-manager": { "config": valid_config() } });
    gear.init(&ctx_with_hub(root, hub_with(&plugins)).await)
        .await
        .unwrap();
    let state = &gear.runtime.get().unwrap().startup;
    assert!(state.plugin_set().is_none(), "not ready before start");
    gear.start(CancellationToken::new()).await.unwrap();
    assert_eq!(state.plugin_set().unwrap().keys(), ["cf.test.a.section"]);
    assert_eq!(
        plugins[0].client.as_ref().unwrap().calls(),
        0,
        "no plugin call"
    );
    gear.stop(CancellationToken::new()).await.unwrap();
}

#[tokio::test]
async fn failed_startup_fails_start_with_its_reason_and_stays_not_ready() {
    let gear = ApplicationManager::default();
    let registry = MockTypesRegistryClient::new().with_list_error(internal("down"));
    let root = json!({ "application-manager": { "config": valid_config() } });
    gear.init(&ctx_with_hub(root, hub_with_registry(&[], registry)).await)
        .await
        .unwrap();
    let err = gear.start(CancellationToken::new()).await.unwrap_err();
    assert!(
        format!("{err:#}")
            .starts_with("application-manager: startup_failed: types_registry_unreachable: "),
        "{err:#}"
    );
    assert!(gear.runtime.get().unwrap().startup.plugin_set().is_none());
    let check = gear.healthcheck(&ctx_with(json!({})).await).unwrap();
    assert_eq!(check.check().await.status, HealthcheckStatus::Unhealthy);
}

/// An initialized gear over `hub`, with `overrides` applied to the valid config.
async fn initialized(overrides: &Value, hub: Arc<ClientHub>) -> ApplicationManager {
    let mut config = valid_config();
    for (name, value) in overrides.as_object().unwrap() {
        config[name] = value.clone();
    }
    let gear = ApplicationManager::default();
    let root = json!({ "application-manager": { "config": config } });
    gear.init(&ctx_with_hub(root, hub).await).await.unwrap();
    gear
}

#[tokio::test(start_paused = true)]
async fn cancelled_start_publishes_nothing() {
    let plugins = [PluginSpec::new(
        "cf.test.a.plugin.v1",
        1,
        "cf.test.a.section.v1",
    )];
    let gear = initialized(&json!({}), hub_with(&plugins)).await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let started = tokio::time::Instant::now();
    let err = gear.start(cancel).await.unwrap_err();
    assert_eq!(format!("{err:#}"), "application-manager: start cancelled");
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(gear.runtime.get().unwrap().startup.plugin_set().is_none());
}

#[tokio::test]
async fn allowed_callers_from_config_decide_read_schema() {
    let gear = initialized(&json!({}), hub_with(&[])).await;
    gear.start(CancellationToken::new()).await.unwrap();
    let read = &gear.runtime.get().unwrap().read;
    assert_eq!(
        read.list_section_keys(&platform_ctx("app-seeder")),
        Ok(vec![])
    );
    assert!(matches!(
        read.list_section_keys(&platform_ctx("someone-else")),
        Err(DomainError::NotPermitted { .. })
    ));
}

#[tokio::test]
async fn the_noop_section_type_compiles_and_serves_its_schema() {
    use noop_section_plugin::section::{SECTION_TYPE, section_type_schema};

    use crate::domain::schema_validator::section_problems;
    use crate::test_support::type_schema;

    let mut spec = PluginSpec::new("cf.test.noop.plugin.v1", 100, "cf.core.noop.section.v1");
    spec.section_type = Some(type_schema(SECTION_TYPE, section_type_schema()));
    let gear = initialized(&json!({}), hub_with(&[spec])).await;
    gear.start(CancellationToken::new()).await.unwrap();
    let read = &gear.runtime.get().unwrap().read;
    let section = read
        .section_schema(&platform_ctx("app-seeder"), "cf.core.noop.section")
        .unwrap();
    assert_eq!(section.section_type(), SECTION_TYPE);
    let sections = [section];
    assert_eq!(
        section_problems(&sections, &json!({ "cf.core.noop.section": { "any": 1 } })),
        []
    );
    assert_eq!(
        section_problems(&sections, &json!({ "cf.core.noop.section": [] }))[0].rule,
        "type"
    );
}

#[test]
fn sdk_types_are_in_inventory_of_the_gear_test_binary() {
    ensure_sdk_types_linked().unwrap();
}

#[test]
fn sdk_type_missing_from_inventory_fails() {
    let section = json!({ "$id": "gts://gts.cf.core.application_manager.section.v1~" });
    let spec = json!({
        "$id": "gts://gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~"
    });
    let report = json!({ "$id": "gts://gts.cf.core.application_manager.outcome_report.v1~" });
    let other = json!({ "$id": "gts://gts.cf.other.type.v1~" });
    for registered in [
        vec![],
        vec![other, json!({})],
        vec![section.clone()],
        vec![spec.clone()],
        vec![section.clone(), spec.clone()],
        vec![report.clone(), spec.clone()],
    ] {
        let err = ensure_sdk_types_registered(&registered).unwrap_err();
        assert!(
            format!("{err:#}").contains("is not in the type inventory"),
            "{err:#}"
        );
    }
    ensure_sdk_types_registered(&[section, report, spec]).unwrap();
}

#[tokio::test]
async fn healthcheck_follows_startup() {
    let root = json!({ "application-manager": { "config": valid_config() } });
    let plugins = [PluginSpec::new(
        "cf.test.a.plugin.v1",
        1,
        "cf.test.a.section.v1",
    )];
    let ctx = ctx_with_hub(root, hub_with(&plugins)).await;
    let gear = ApplicationManager::default();
    assert!(gear.healthcheck(&ctx).is_none(), "no check before init");

    gear.init(&ctx).await.unwrap();
    let check = gear.healthcheck(&ctx).expect("check after init");
    assert_eq!(check.check().await.status, HealthcheckStatus::Unhealthy);

    gear.start(CancellationToken::new()).await.unwrap();
    assert_eq!(check.check().await.status, HealthcheckStatus::Healthy);
}

#[tokio::test]
async fn init_without_a_database_fails_with_invalid_setting_database() {
    let gear = ApplicationManager::default();
    let root = json!({ "application-manager": { "config": valid_config() } });
    let err = gear
        .init(&ctx_without_db(root, Arc::new(ClientHub::new())))
        .await
        .expect_err("a database is required");
    let text = format!("{err:#}");
    assert!(text.contains("invalid_setting: database"), "{text}");
    assert!(text.starts_with("application-manager: "), "{text}");
    // Nothing was published.
    let err = gear.start(CancellationToken::new()).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("init must run before start"),
        "{err:#}"
    );
}

#[tokio::test]
async fn init_with_a_sqlite_database_succeeds_and_provides_the_migrations() {
    let gear = ApplicationManager::default();
    let db = test_db().await;
    let root = json!({ "application-manager": { "config": valid_config() } });
    let ctx = ctx_without_db(root, Arc::new(ClientHub::new())).with_db(db.clone());
    gear.init(&ctx).await.unwrap();

    let migrations = DatabaseCapability::migrations(&gear);
    assert!(!migrations.is_empty());
    // The host runs them; they must apply on the database `init` was given.
    let applied = run_migrations_for_testing(&db.db(), migrations)
        .await
        .expect("the gear's migrations apply");
    assert!(applied.applied > 0, "{applied:?}");
}
