//! Gear declaration and lifecycle.

use std::sync::{Arc, OnceLock};

use anyhow::anyhow;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use toolkit::config::ConfigError;
use toolkit::contracts::RunnableCapability;
use toolkit::{Gear, GearCtx};

use crate::config::ApplicationManagerConfig;
use crate::sdk::{ApplicationManagerSectionPluginSpecV1, ApplicationManagerSectionV1};

/// The Application Manager gear.
#[toolkit::gear(
    name = "application-manager",
    deps = [types_registry, authz_resolver],
    capabilities = [stateful],
)]
#[derive(Default)]
pub struct ApplicationManager {
    config: OnceLock<Arc<ApplicationManagerConfig>>,
}

/// The SDK's GTS types are registered through the link-time inventory; fail
/// init when the SDK that declares them is not linked into the binary.
fn ensure_sdk_types_linked() -> anyhow::Result<()> {
    let registered = toolkit_gts::all_inventory_type_schemas().map_err(|e| {
        anyhow!(
            "{}: type inventory unreadable: {e}",
            ApplicationManager::MODULE_NAME
        )
    })?;
    ensure_sdk_types_registered(&registered)
}

/// Fail unless `registered` holds the section base type and the section
/// plugin spec schemas.
fn ensure_sdk_types_registered(registered: &[serde_json::Value]) -> anyhow::Result<()> {
    for type_id in [
        <ApplicationManagerSectionV1 as gts::GtsSchema>::TYPE_ID,
        <ApplicationManagerSectionPluginSpecV1 as gts::GtsSchema>::TYPE_ID,
    ] {
        let want = format!("gts://{type_id}");
        if !registered.iter().any(|s| s["$id"] == want) {
            return Err(anyhow!(
                "{}: GTS type {want} is not in the type inventory",
                ApplicationManager::MODULE_NAME
            ));
        }
    }
    Ok(())
}

/// Render a config load failure as `invalid_setting: config: <reason>`, the
/// same `<setting>: <reason>` form as [`ApplicationManagerConfig::validate`].
fn config_load_reason(err: &ConfigError) -> String {
    match err {
        ConfigError::InvalidConfig { cause, .. } => cause.to_string(),
        other => other.to_string(),
    }
}

#[async_trait]
impl Gear for ApplicationManager {
    async fn init(&self, ctx: &GearCtx) -> anyhow::Result<()> {
        let config: ApplicationManagerConfig = ctx.config().map_err(|e| {
            anyhow!(
                "{}: invalid_setting: config: {}",
                Self::MODULE_NAME,
                config_load_reason(&e)
            )
        })?;
        config
            .validate()
            .map_err(|e| anyhow!("{}: {e}", Self::MODULE_NAME))?;

        ensure_sdk_types_linked()?;

        tracing::info!(
            plugin_call_timeout = ?config.plugin_call_timeout,
            run_window_margin = ?config.run_window_margin,
            operation_retention = ?config.operation_retention,
            plugin_max_in_flight = config.plugin_max_in_flight,
            max_request_tasks = config.max_request_tasks,
            max_envelope_text_length = config.max_envelope_text_length,
            pdp_call_timeout = ?config.pdp_call_timeout,
            allowed_callers = config.allowed_callers.len(),
            storage_call_timeout = ?config.storage_call_timeout,
            "application-manager: configuration validated"
        );

        self.config
            .set(Arc::new(config))
            .map_err(|_| anyhow!("{}: already initialized", Self::MODULE_NAME))?;
        Ok(())
    }
}

#[async_trait]
impl RunnableCapability for ApplicationManager {
    async fn start(&self, _cancel: CancellationToken) -> anyhow::Result<()> {
        if self.config.get().is_none() {
            return Err(anyhow!("{}: init must run before start", Self::MODULE_NAME));
        }
        Ok(())
    }

    async fn stop(&self, _deadline_token: CancellationToken) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gear_tests.rs"]
mod tests;
