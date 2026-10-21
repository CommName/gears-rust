#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use toolkit::config::ConfigProvider;
use toolkit::contracts::RestApiCapability;
use toolkit::{ClientHub, Gear, GearCtx, HealthcheckStatus};
use uuid::Uuid;

use super::*;
use crate::domain::error::DomainError;
use crate::infra::call_guard::GuardError;
use crate::test_support::{
    FakeAuthZ, KeyAnswer, Pdp, PluginSpec, ctx, hub_with, hub_with_registry, valid_config_json,
};
use authz_resolver_sdk::AuthZResolverApi;
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

fn ctx_with(root: Value) -> GearCtx {
    ctx_with_hub(root, Arc::new(ClientHub::new()))
}

fn ctx_with_hub(root: Value, hub: Arc<ClientHub>) -> GearCtx {
    GearCtx::new(
        ApplicationManager::MODULE_NAME,
        Uuid::from_u128(1),
        Arc::new(StaticConfigProvider { root }),
        hub,
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
    let mut shared_caller = valid_config();
    shared_caller["allowed_callers"][0]["identity"] = json!({ "type": "Shared", "name": "dev" });
    let mut no_actions = valid_config();
    no_actions["allowed_callers"][0]["actions"] = json!([]);
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
        ("shared_caller", ctx_with_config(&shared_caller)),
        ("caller_without_actions", ctx_with_config(&no_actions)),
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
        "cf.test.a.section.v1",
        1,
        KeyAnswer::Key("a"),
    )];
    let root = json!({ "application-manager": { "config": valid_config() } });
    gear.init(&ctx_with_hub(root, hub_with(&plugins)))
        .await
        .unwrap();
    let state = &gear.runtime.get().unwrap().startup;
    assert!(state.plugin_set().is_none(), "not ready before start");
    gear.start(CancellationToken::new()).await.unwrap();
    assert_eq!(state.plugin_set().unwrap().keys(), ["a"]);
    gear.stop(CancellationToken::new()).await.unwrap();
}

#[tokio::test]
async fn failed_startup_fails_start_with_its_reason_and_stays_not_ready() {
    let gear = ApplicationManager::default();
    let registry = MockTypesRegistryClient::new().with_list_error(internal("down"));
    let root = json!({ "application-manager": { "config": valid_config() } });
    gear.init(&ctx_with_hub(root, hub_with_registry(&[], registry)))
        .await
        .unwrap();
    let err = gear.start(CancellationToken::new()).await.unwrap_err();
    assert!(
        format!("{err:#}")
            .starts_with("application-manager: startup_failed: types_registry_unreachable: "),
        "{err:#}"
    );
    assert!(gear.runtime.get().unwrap().startup.plugin_set().is_none());
    let check = gear.healthcheck(&ctx_with(json!({}))).unwrap();
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
    gear.init(&ctx_with_hub(root, hub)).await.unwrap();
    gear
}

#[tokio::test(start_paused = true)]
async fn cancelled_start_publishes_nothing() {
    let plugins = [PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Hang)];
    let gear = initialized(&json!({}), hub_with(&plugins)).await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let started = tokio::time::Instant::now();
    let err = gear.start(cancel).await.unwrap_err();
    assert_eq!(format!("{err:#}"), "application-manager: start cancelled");
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert!(gear.runtime.get().unwrap().startup.plugin_set().is_none());
}

#[tokio::test(start_paused = true)]
async fn plugin_call_timeout_bounds_the_key_call() {
    let plugins = [PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Hang)];
    let gear = initialized(&json!({ "plugin_call_timeout": "3s" }), hub_with(&plugins)).await;
    let started = tokio::time::Instant::now();
    let err = gear.start(CancellationToken::new()).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("startup_failed: section_key_unavailable: "),
        "{err:#}"
    );
    assert_eq!(started.elapsed(), Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn plugin_max_in_flight_caps_each_plugin() {
    let gear = initialized(&json!({ "plugin_max_in_flight": 2 }), hub_with(&[])).await;
    let guard = &gear.runtime.get().unwrap().guard;
    let (release_a, wait_a) = tokio::sync::oneshot::channel::<()>();
    let (release_b, wait_b) = tokio::sync::oneshot::channel::<()>();
    let third = async move {
        let refused = guard.call("p", async {}).await;
        release_a.send(()).unwrap();
        release_b.send(()).unwrap();
        refused
    };
    let (a, b, refused) = tokio::join!(
        guard.call("p", async move { wait_a.await.unwrap() }),
        guard.call("p", async move { wait_b.await.unwrap() }),
        third
    );
    assert_eq!((a, b), (Ok(()), Ok(())));
    assert_eq!(refused, Err(GuardError::NotSent));
}

#[tokio::test(start_paused = true)]
async fn pdp_call_timeout_bounds_the_access_check() {
    let hub = hub_with(&[]);
    let pdp: Arc<dyn AuthZResolverApi> = FakeAuthZ::new(Pdp::Hang);
    hub.register::<dyn AuthZResolverApi>(pdp);
    let gear = initialized(&json!({ "pdp_call_timeout": "1s" }), hub).await;
    gear.start(CancellationToken::new()).await.unwrap();
    let started = tokio::time::Instant::now();
    let refused = gear
        .runtime
        .get()
        .unwrap()
        .read
        .list_section_keys(&ctx())
        .await;
    assert_eq!(refused, Err(DomainError::AccessUnavailable));
    assert_eq!(started.elapsed(), Duration::from_secs(1));
}

#[test]
fn margin_floor_is_n_plus_two_storage_timeouts() {
    let secs = Duration::from_secs;
    assert_eq!(margin_below_floor(secs(30), secs(2), 0), None);
    assert_eq!(margin_below_floor(secs(30), secs(10), 1), None);
    assert_eq!(margin_below_floor(secs(29), secs(10), 1), Some(secs(30)));
    assert_eq!(margin_below_floor(secs(30), secs(2), 14), Some(secs(32)));
    assert_eq!(
        margin_below_floor(secs(30), secs(2), usize::MAX),
        Some(Duration::MAX),
        "an overflowing floor always warns"
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
    let other = json!({ "$id": "gts://gts.cf.other.type.v1~" });
    for registered in [
        vec![],
        vec![other, json!({})],
        vec![section.clone()],
        vec![spec.clone()],
    ] {
        let err = ensure_sdk_types_registered(&registered).unwrap_err();
        assert!(
            format!("{err:#}").contains("is not in the type inventory"),
            "{err:#}"
        );
    }
    ensure_sdk_types_registered(&[section, spec]).unwrap();
}

#[tokio::test]
async fn healthcheck_follows_startup() {
    let root = json!({ "application-manager": { "config": valid_config() } });
    let plugins = [PluginSpec::new(
        "cf.test.a.section.v1",
        1,
        KeyAnswer::Key("a"),
    )];
    let ctx = ctx_with_hub(root, hub_with(&plugins));
    let gear = ApplicationManager::default();
    assert!(gear.healthcheck(&ctx).is_none(), "no check before init");

    gear.init(&ctx).await.unwrap();
    let check = gear.healthcheck(&ctx).expect("check after init");
    assert_eq!(check.check().await.status, HealthcheckStatus::Unhealthy);

    gear.start(CancellationToken::new()).await.unwrap();
    assert_eq!(check.check().await.status, HealthcheckStatus::Healthy);
}
