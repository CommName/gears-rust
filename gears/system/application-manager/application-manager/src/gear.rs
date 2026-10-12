//! Gear declaration and lifecycle for `application-manager` (DESIGN §3.2, §4.7).

use std::sync::OnceLock;

use async_trait::async_trait;
use toolkit::{Gear, GearCtx};

use crate::config::{ApplicationManagerConfig, REASON};

// Linked so the `types_registry` and `authz_resolver` gears named in `deps`
// are present in the inventory of every binary that links this crate.
use authz_resolver as _;
use types_registry as _;
// The SDK is not used by gear code yet (P1); linked so its plugin-spec GTS schemas
// reach the inventory.
use application_manager_sdk as _;

/// Application Manager gear.
///
/// At `init` the gear reads the `application-manager` config section with the
/// strict accessor (a missing section is an error), validates it and stores it
/// once. P1 declares no capabilities and performs no plugin discovery; no tenant
/// service is resolved (ADR 0007).
#[toolkit::gear(
    name = "application-manager",
    deps = [types_registry, authz_resolver]
)]
#[derive(Default)]
pub struct ApplicationManagerGear {
    config: OnceLock<ApplicationManagerConfig>,
}

#[async_trait]
impl Gear for ApplicationManagerGear {
    async fn init(&self, ctx: &GearCtx) -> anyhow::Result<()> {
        let cfg: ApplicationManagerConfig =
            ctx.config::<ApplicationManagerConfig>().map_err(|e| {
                anyhow::anyhow!(
                    "{REASON}: failed to read the application-manager config section: {e}"
                )
            })?;
        // `validate` errors already start with `invalid_setting` and name the setting;
        // no outer context, so a plain `{}` log keeps the reason (DESIGN §4.7).
        cfg.validate()?;
        self.config
            .set(cfg)
            .map_err(|_| anyhow::anyhow!("application-manager gear is already initialized"))?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::needless_pass_by_value)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gear_tests.rs"]
mod gear_tests;
