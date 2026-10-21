//! No-op section plugin gear.

use std::sync::Arc;

use application_manager_sdk::{ApplicationManagerSectionPluginSpecV1, SectionPluginClientV1};
use async_trait::async_trait;
use toolkit::Gear;
use toolkit::client_hub::ClientScope;
use toolkit::context::GearCtx;
use toolkit::gts::PluginV1;
use types_registry_sdk::{RegisterResult, TypesRegistryClient};

use crate::client::NoopSectionClient;
use crate::config::NoopSectionPluginConfig;

/// The instance segment of this plugin's GTS instance id.
pub const INSTANCE_SEGMENT: &str = "cf.core.application_manager_noop.section_plugin.v1";

/// Registers the no-op section plugin: its GTS instance in the types registry
/// and its client in `ClientHub`, scoped by the instance id.
#[toolkit::gear(name = "noop-section-plugin", deps = [types_registry])]
#[derive(Default)]
pub struct NoopSectionPlugin;

/// The instance id and registry payload for `cfg`.
///
/// # Errors
/// When the registration payload cannot be built.
pub fn registration(cfg: &NoopSectionPluginConfig) -> anyhow::Result<(String, serde_json::Value)> {
    let (id, payload) = PluginV1::<ApplicationManagerSectionPluginSpecV1>::build_registration(
        INSTANCE_SEGMENT,
        cfg.vendor.clone(),
        cfg.priority,
    )?;
    Ok((id.as_ref().to_owned(), payload))
}

#[async_trait]
impl Gear for NoopSectionPlugin {
    async fn init(&self, ctx: &GearCtx) -> anyhow::Result<()> {
        let cfg: NoopSectionPluginConfig = ctx.config_or_default()?;
        let (instance_id, payload) = registration(&cfg)?;

        let registry = ctx.client_hub().get::<dyn TypesRegistryClient>()?;
        let results = registry.register(vec![payload]).await?;
        RegisterResult::ensure_all_ok(&results)?;

        let client: Arc<dyn SectionPluginClientV1> = Arc::new(NoopSectionClient);
        ctx.client_hub()
            .register_scoped::<dyn SectionPluginClientV1>(
                ClientScope::gts_id(&instance_id),
                client,
            );

        tracing::info!(
            instance_id = %instance_id,
            priority = cfg.priority,
            "noop-section-plugin: registered"
        );
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gear_tests.rs"]
mod tests;
