//! No-op section plugin gear.

use std::sync::Arc;

use application_manager_sdk::{ApplicationManagerSectionPluginSpecV1, SectionPluginClientV1};
use async_trait::async_trait;
use gts::{GtsInstanceId, GtsSchema};
use toolkit::Gear;
use toolkit::client_hub::ClientScope;
use toolkit::context::GearCtx;
use toolkit::gts::PluginV1;
use types_registry_sdk::{RegisterResult, TypesRegistryClient};

use crate::client::NoopSectionClient;
use crate::config::NoopSectionPluginConfig;
use crate::section::{SECTION_TYPE, section_type_schema};

/// The instance segment of this plugin's GTS instance id.
pub const INSTANCE_SEGMENT: &str = "cf.core.application_manager_noop.section_plugin.v1";

/// Registers the no-op section plugin: its section type and its GTS instance,
/// naming that type, in the types registry, and its client in `ClientHub`,
/// scoped by the instance id.
#[toolkit::gear(name = "noop-section-plugin", deps = [types_registry])]
#[derive(Default)]
pub struct NoopSectionPlugin;

/// The instance id and registry payload for `cfg`.
///
/// # Errors
/// When the registration payload cannot be built.
pub fn registration(cfg: &NoopSectionPluginConfig) -> anyhow::Result<(String, serde_json::Value)> {
    let id = GtsInstanceId::new(
        <ApplicationManagerSectionPluginSpecV1 as GtsSchema>::TYPE_ID,
        INSTANCE_SEGMENT,
    );
    let instance = PluginV1::<ApplicationManagerSectionPluginSpecV1> {
        id: id.clone(),
        vendor: cfg.vendor.clone(),
        priority: cfg.priority,
        properties: ApplicationManagerSectionPluginSpecV1 {
            section_type: SECTION_TYPE.to_owned(),
        },
    };
    Ok((id.as_ref().to_owned(), serde_json::to_value(&instance)?))
}

#[async_trait]
impl Gear for NoopSectionPlugin {
    async fn init(&self, ctx: &GearCtx) -> anyhow::Result<()> {
        let cfg: NoopSectionPluginConfig = ctx.config_or_default()?;
        let (instance_id, payload) = registration(&cfg)?;

        let registry = ctx.client_hub().get::<dyn TypesRegistryClient>()?;
        let results = registry
            .register_type_schemas(vec![section_type_schema()])
            .await?;
        RegisterResult::ensure_all_ok(&results)?;
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
