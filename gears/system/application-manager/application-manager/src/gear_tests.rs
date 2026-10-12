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

use std::sync::Arc;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use toolkit::config::ConfigProvider;
use toolkit::registry::GearRegistry;
use toolkit::{ClientHub, Gear, GearCtx};
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
