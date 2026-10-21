#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use toolkit::config::ConfigProvider;
use toolkit::{ClientHub, Gear, GearCtx};
use uuid::Uuid;

use super::*;
use crate::test_support::valid_config_json;

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

fn ctx_with(root: Value) -> GearCtx {
    GearCtx::new(
        ApplicationManager::MODULE_NAME,
        Uuid::from_u128(1),
        Arc::new(StaticConfigProvider { root }),
        Arc::new(ClientHub::new()),
        CancellationToken::new(),
    )
}

fn ctx_with_config(config: &Value) -> GearCtx {
    ctx_with(json!({ "application-manager": { "config": config } }))
}

#[tokio::test]
async fn init_valid_static_config_succeeds() {
    let gear = ApplicationManager::default();
    gear.init(&ctx_with_config(&valid_config())).await.unwrap();
}

#[tokio::test]
async fn init_second_call_fails_already_initialized() {
    let gear = ApplicationManager::default();
    let ctx = ctx_with_config(&valid_config());
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
    let cases = vec![
        ("missing_gear_block", ctx_with(json!({}))),
        (
            "missing_config_key",
            ctx_with(json!({ "application-manager": {} })),
        ),
        ("missing_field", ctx_with_config(&missing)),
        ("unknown_field", ctx_with_config(&unknown)),
        ("bad_duration", ctx_with_config(&bad_duration)),
        ("invalid_value", ctx_with_config(&zero)),
    ];
    for (name, ctx) in cases {
        let gear = ApplicationManager::default();
        let err = gear.init(&ctx).await.expect_err(name);
        let text = format!("{err:#}");
        assert!(text.contains("invalid_setting"), "{name}: {text}");
        let prefix = if name == "invalid_value" {
            "application-manager: invalid_setting: max_request_tasks: "
        } else {
            "application-manager: invalid_setting: config: "
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
async fn start_and_stop_after_init_succeed() {
    let gear = ApplicationManager::default();
    gear.init(&ctx_with_config(&valid_config())).await.unwrap();
    gear.start(CancellationToken::new()).await.unwrap();
    gear.stop(CancellationToken::new()).await.unwrap();
}

#[test]
fn section_plugin_type_is_in_inventory_of_the_gear_test_binary() {
    ensure_section_plugin_type_linked().unwrap();
}

#[test]
fn section_plugin_type_missing_from_inventory_fails() {
    let others = vec![json!({ "$id": "gts://gts.cf.other.type.v1~" }), json!({})];
    for registered in [vec![], others] {
        let err = ensure_section_plugin_type_registered(&registered).unwrap_err();
        assert!(
            format!("{err:#}").contains("is not in the type inventory"),
            "{err:#}"
        );
    }
    let spec = json!({
        "$id": "gts://gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~"
    });
    ensure_section_plugin_type_registered(&[spec]).unwrap();
}
