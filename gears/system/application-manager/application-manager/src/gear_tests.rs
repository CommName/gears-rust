//! Unit tests for the `ApplicationManagerGear` declaration (DESIGN §3.2 "Startup and
//! Registration", §4.7, ADR 0007).
//!
//! Planned API: `ApplicationManagerGear` (`Default`, `impl Gear`) in `crate::gear`.
//! `init` reads `gears.application-manager.config` with the strict accessor, so a
//! missing section fails; it validates the config and stores it once.
//!
//! - `init_accepts_valid_config`: valid section, init succeeds (§4.7).
//! - `init_rejects_missing_config_section`: no section, init fails (§4.7).
//! - `init_rejects_invalid_setting`: `validate()` failure carries `invalid_setting`
//!   and the field name (§4.7).
//! - `init_rejects_unknown_config_key`: removed setting `startup_retry_deadline` is
//!   rejected (§3.2, ADR 0007, §4.7).
//! - `init_fails_when_already_initialized`: second init fails (§3.2).
//! - `gear_declares_types_registry_and_authz_resolver_deps`: registered deps (§3.2).
//!
//! Registry persistence (P4, DESIGN §3.2 "Startup and Registration", §3.7): the gear macro
//! gains the `db` capability (`capabilities = [stateful, db]`) and `ApplicationManagerGear`
//! implements `toolkit::DatabaseCapability`, whose `migrations()` returns
//! `crate::infra::storage::migrations::Migrator::migrations()`. `init` resolves no database in
//! P4; since P6 it builds the registry store from the context's database handle (see
//! `gear_registry_tests.rs`). A context WITHOUT a database still initializes (the P1-P3 `init`
//! tests above keep passing); the missing database then fails `start` (§3.2 fault handling).
//!
//! - `gear_declares_the_db_capability_next_to_stateful`: registry capability labels (§3.2).
//! - `gear_returns_the_registry_migrations_from_database_capability`: same names as the
//!   `Migrator` (§3.7).
//! - `gear_migrations_create_the_registry_table_on_an_empty_database`: running the gear's
//!   migrations the way the platform migration phase does creates `application_registry`
//!   and nothing tenant-shaped (§3.7, ADR 0007).

use std::sync::Arc;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use toolkit::config::ConfigProvider;
use toolkit::registry::GearRegistry;
use toolkit::{ClientHub, DatabaseCapability, Gear, GearCtx};
use uuid::Uuid;

use crate::gear::ApplicationManagerGear;

const GEAR_NAME: &str = "application-manager";

struct StaticConfigProvider {
    root: Value,
}

impl ConfigProvider for StaticConfigProvider {
    fn get_gear_config(&self, gear: &str) -> Option<&Value> {
        self.root.get(gear)
    }
}

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

fn make_ctx(root: Value) -> GearCtx {
    GearCtx::new(
        GEAR_NAME,
        Uuid::new_v4(),
        Arc::new(StaticConfigProvider { root }),
        Arc::new(ClientHub::new()),
        CancellationToken::new(),
    )
}

fn ctx_with_config(config: Value) -> GearCtx {
    make_ctx(json!({ GEAR_NAME: { "config": config } }))
}

#[tokio::test]
async fn init_accepts_valid_config() {
    let ctx = ctx_with_config(valid_config());
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx)
        .await
        .expect("init must accept a valid config section");
}

#[tokio::test]
async fn init_rejects_missing_config_section() {
    let ctx = make_ctx(json!({}));
    let gear = ApplicationManagerGear::default();

    let err = gear
        .init(&ctx)
        .await
        .expect_err("init must fail when the application-manager section is absent");

    // The top-level message, as a host logs it with `{}`, carries the reason.
    let text = format!("{err}");
    assert!(
        text.contains("invalid_setting"),
        "a missing section must carry the reason token, got: {text}"
    );
}

#[tokio::test]
async fn init_load_errors_carry_invalid_setting_and_field_name() {
    let mut missing = valid_config();
    missing
        .as_object_mut()
        .expect("object")
        .remove("pdp_call_timeout");
    let mut malformed = valid_config();
    malformed["plugin_call_timeout"] = json!("fast");
    let mut mistyped = valid_config();
    mistyped["max_request_tasks"] = json!("16");

    for (config, field) in [
        (missing, "pdp_call_timeout"),
        (malformed, "plugin_call_timeout"),
        (mistyped, "max_request_tasks"),
    ] {
        let ctx = ctx_with_config(config);
        let gear = ApplicationManagerGear::default();

        let err = gear
            .init(&ctx)
            .await
            .expect_err("init must fail on a config load error");

        // The top-level message, as a host logs it with `{}`, carries the reason.
        let text = format!("{err}");
        assert!(
            text.contains("invalid_setting") && text.contains(field),
            "load error must carry the reason token and {field}, got: {text}"
        );
    }
}

#[tokio::test]
async fn init_rejects_invalid_setting() {
    let mut config = valid_config();
    config["plugin_call_timeout"] = json!("0s");
    let ctx = ctx_with_config(config);
    let gear = ApplicationManagerGear::default();

    let err = gear
        .init(&ctx)
        .await
        .expect_err("init must fail on a zero plugin_call_timeout");

    // The top-level message, as a host logs it with `{}`, carries the reason.
    let text = format!("{err}");
    assert!(
        text.contains("invalid_setting") && text.contains("plugin_call_timeout"),
        "error must carry the reason token and the field name, got: {text}"
    );
}

#[tokio::test]
async fn init_rejects_unknown_config_key() {
    let mut config = valid_config();
    config["startup_retry_deadline"] = json!("30s");
    let ctx = ctx_with_config(config);
    let gear = ApplicationManagerGear::default();

    let err = gear
        .init(&ctx)
        .await
        .expect_err("init must reject the removed startup_retry_deadline key");

    // The top-level message, as a host logs it with `{}`, carries the reason.
    let text = format!("{err}");
    assert!(
        text.contains("invalid_setting") && text.contains("startup_retry_deadline"),
        "an unknown key must carry the reason token and its name, got: {text}"
    );
}

#[tokio::test]
async fn init_fails_when_already_initialized() {
    let ctx = ctx_with_config(valid_config());
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx).await.expect("first init must succeed");

    let result = gear.init(&ctx).await;

    assert!(result.is_err(), "second init must fail");
}

#[test]
fn gear_declares_types_registry_and_authz_resolver_deps() {
    let registry = GearRegistry::discover_and_build()
        .expect("the inventory registry must build with the declared deps linked");

    let entry = registry
        .gears()
        .iter()
        .find(|e| e.name() == GEAR_NAME)
        .expect("application-manager must be registered via the gear macro");

    assert_eq!(
        entry.deps(),
        ["types-registry", "authz-resolver"],
        "declared deps must be types-registry and authz-resolver"
    );
}

#[test]
fn gear_declares_the_db_capability_next_to_stateful() {
    let registry = GearRegistry::discover_and_build()
        .expect("the inventory registry must build with the declared deps linked");
    let entry = registry
        .gears()
        .iter()
        .find(|e| e.name() == GEAR_NAME)
        .expect("application-manager must be registered via the gear macro");

    assert!(
        entry.caps().has_db(),
        "the gear owns a table, so it declares `db`"
    );
    let labels = entry.caps().labels();
    assert!(labels.contains(&"db"), "{labels:?}");
    assert!(labels.contains(&"stateful"), "{labels:?}");
}

#[test]
fn gear_returns_the_registry_migrations_from_database_capability() {
    use sea_orm_migration::MigratorTrait;

    let gear = ApplicationManagerGear::default();

    let from_gear: Vec<String> = DatabaseCapability::migrations(&gear)
        .iter()
        .map(|m| m.name().to_owned())
        .collect();
    let from_migrator: Vec<String> = crate::infra::storage::migrations::Migrator::migrations()
        .iter()
        .map(|m| m.name().to_owned())
        .collect();

    assert_eq!(
        from_gear.len(),
        1,
        "one initial migration (DESIGN section 3.7)"
    );
    assert_eq!(from_gear, from_migrator);
}

#[tokio::test]
async fn gear_migrations_create_the_registry_table_on_an_empty_database() {
    use crate::test_support::registry_db::{self, REGISTRY_TABLE, real_bounded};
    use toolkit_db::ConnectOpts;
    use toolkit_db::migration_runner::run_migrations_for_gear;

    let db = registry_db::sqlite_with(ConnectOpts::default(), false).await;
    assert!(!registry_db::table_exists(&db, REGISTRY_TABLE).await);
    let gear = ApplicationManagerGear::default();

    let result = real_bounded(run_migrations_for_gear(
        &db.provider.db(),
        GEAR_NAME,
        DatabaseCapability::migrations(&gear),
    ))
    .await
    .expect("the gear's migrations apply on an empty database");

    assert_eq!(result.applied, 1);
    assert!(registry_db::table_exists(&db, REGISTRY_TABLE).await);
    assert_eq!(registry_db::columns_named_like(&db, "tenant").await, 0);
}
