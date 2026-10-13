//! Gear declaration and lifecycle for `application-manager` (DESIGN §3.2, §4.7).

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use toolkit::{DatabaseCapability, Gear, GearCtx, RunnableCapability};

use crate::config::{ApplicationManagerConfig, REASON};
use crate::domain::startup::StartupResultHolder;
use crate::infra::startup::StartupComponent;

// Linked so the `types_registry` and `authz_resolver` gears named in `deps`
// are present in the inventory of every binary that links this crate.
use authz_resolver as _;
use types_registry as _;

/// Application Manager gear.
///
/// At `init` the gear reads the `application-manager` config section with the
/// strict accessor (a missing section is an error), validates it and stores it
/// once, and builds the startup component. `init` resolves nothing from the hub: the
/// stateful `start` capability discovers the plugins and publishes the startup result.
/// No tenant service is resolved (ADR 0007).
///
/// Migrations come from `DatabaseCapability::migrations()`; the registry store is built
/// when the orchestrator lands.
#[toolkit::gear(
    name = "application-manager",
    deps = [types_registry, authz_resolver],
    capabilities = [stateful, db]
)]
#[derive(Default)]
pub struct ApplicationManagerGear {
    config: OnceLock<ApplicationManagerConfig>,
    startup: OnceLock<Startup>,
}

/// The start capability and the holder it publishes into, built by `init`.
struct Startup {
    component: StartupComponent,
    holder: Arc<StartupResultHolder>,
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
        let holder = Arc::new(StartupResultHolder::new());
        let component = StartupComponent::new(&cfg, ctx.client_hub(), Arc::clone(&holder));
        self.startup
            .set(Startup { component, holder })
            .map_err(|_| anyhow::anyhow!("application-manager gear is already initialized"))?;
        self.config
            .set(cfg)
            .map_err(|_| anyhow::anyhow!("application-manager gear is already initialized"))?;
        Ok(())
    }
}

impl ApplicationManagerGear {
    /// The startup result holder; `None` until `init` succeeded.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code, reason = "used by the orchestrator phase"))]
    pub(crate) fn startup_holder(&self) -> Option<Arc<StartupResultHolder>> {
        self.startup.get().map(|s| Arc::clone(&s.holder))
    }
}

impl DatabaseCapability for ApplicationManagerGear {
    fn migrations(&self) -> Vec<Box<dyn sea_orm_migration::MigrationTrait>> {
        use sea_orm_migration::MigratorTrait;
        crate::infra::storage::migrations::Migrator::migrations()
    }
}

#[async_trait]
impl RunnableCapability for ApplicationManagerGear {
    async fn start(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        let Some(startup) = self.startup.get() else {
            anyhow::bail!("application-manager: start called before init");
        };
        startup.component.start(cancel).await
    }

    async fn stop(&self, deadline_token: CancellationToken) -> anyhow::Result<()> {
        match self.startup.get() {
            Some(startup) => startup.component.stop(deadline_token).await,
            None => Ok(()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::needless_pass_by_value)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gear_tests.rs"]
mod gear_tests;

#[cfg(test)]
#[allow(clippy::needless_pass_by_value)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gear_start_tests.rs"]
mod gear_start_tests;
