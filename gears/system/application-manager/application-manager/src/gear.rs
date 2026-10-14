//! Gear declaration and lifecycle for `application-manager` (DESIGN §3.2, §4.7).

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use toolkit::{DatabaseCapability, Gear, GearCtx, RunnableCapability};

use crate::config::{ApplicationManagerConfig, REASON};
use crate::domain::orchestrator::{Orchestrator, OrchestratorSettings};
use crate::domain::ports::{PolicyPort, RegistryStore, StartupPort};
use crate::domain::startup::StartupResultHolder;
use crate::infra::policy::PolicyAdapter;
use crate::infra::startup::{StartupComponent, register_plugin_types};
use crate::infra::storage::store::SeaRegistryStore;
use crate::infra::system_ports::{SystemClock, UuidV7RequestIds};

// Linked so the `types_registry` and `authz_resolver` gears named in `deps`
// are present in the inventory of every binary that links this crate.
use authz_resolver as _;
use types_registry as _;

/// Application Manager gear.
///
/// At `init` the gear reads the `application-manager` config section with the strict accessor (a
/// missing section is an error), validates it, registers the two plugin GTS types in the types
/// registry (one call, bounded by `plugin_call_timeout`, never retried; a failure is
/// `type_registration_failed`), stores the config once, and builds the startup component and the
/// request orchestrator (over the policy adapter, the registry store on the gear database, the
/// startup result holder, the system clock and `UUIDv7` request ids). `init` resolves nothing from
/// the hub but the types registry client: the policy adapter resolves the authz client at each
/// decision, and the stateful `start` capability discovers the plugins and publishes the startup
/// result. A missing database does not fail `init`: no orchestrator is built and `start` fails
/// with `database_unavailable`. No tenant service is resolved (ADR 0007).
///
/// `stop` sets the stopping flag, drains the request tasks (closing admission, then waiting for
/// the permits taken and the running supervisors) and then the plugin calls, against the one stop
/// deadline token.
///
/// Migrations come from `DatabaseCapability::migrations()`.
#[toolkit::gear(
    name = "application-manager",
    deps = [types_registry, authz_resolver],
    capabilities = [stateful, db]
)]
#[derive(Default)]
pub struct ApplicationManagerGear {
    config: OnceLock<ApplicationManagerConfig>,
    startup: OnceLock<Startup>,
    orchestrator: OnceLock<Orchestrator>,
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
        // A second `init` fails before anything is asked of the types registry or replaced.
        if self.startup.get().is_some() {
            anyhow::bail!("application-manager gear is already initialized");
        }
        register_plugin_types(ctx.client_hub().as_ref(), cfg.plugin_call_timeout).await?;
        let holder = Arc::new(StartupResultHolder::new());
        let mut component = StartupComponent::new(&cfg, ctx.client_hub(), Arc::clone(&holder));
        let orchestrator = if let Some(db) = ctx.db() {
            let policy: Arc<dyn PolicyPort> = Arc::new(PolicyAdapter::from_hub(
                ctx.client_hub(),
                cfg.pdp_call_timeout,
            ));
            let registry: Arc<dyn RegistryStore> =
                Arc::new(SeaRegistryStore::new(db, cfg.registry_operation_timeout));
            Some(Orchestrator::new(
                policy,
                registry,
                Arc::clone(&holder),
                Arc::new(SystemClock),
                Arc::new(UuidV7RequestIds),
                OrchestratorSettings::from_config(&cfg),
            ))
        } else {
            component = component.without_database();
            None
        };
        // The first `set` is the guard of a second `init`: it fails before anything is replaced.
        self.startup
            .set(Startup { component, holder })
            .map_err(|_| anyhow::anyhow!("application-manager gear is already initialized"))?;
        if let Some(orchestrator) = orchestrator {
            self.orchestrator
                .set(orchestrator)
                .map_err(|_| anyhow::anyhow!("application-manager gear is already initialized"))?;
        }
        self.config
            .set(cfg)
            .map_err(|_| anyhow::anyhow!("application-manager gear is already initialized"))?;
        Ok(())
    }
}

impl ApplicationManagerGear {
    /// The startup result holder; `None` until `init` succeeded.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code, reason = "used by the REST phase"))]
    pub(crate) fn startup_holder(&self) -> Option<Arc<StartupResultHolder>> {
        self.startup.get().map(|s| Arc::clone(&s.holder))
    }

    /// The request orchestrator `init` built; `None` until `init` succeeded, and when `init`
    /// found no database handle.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code, reason = "used by the REST phase"))]
    pub(crate) fn orchestrator(&self) -> Option<&Orchestrator> {
        self.orchestrator.get()
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
        let Some(startup) = self.startup.get() else {
            return Ok(());
        };
        // Stopping first, so the REST not-ready gate refuses new requests; then the request
        // tasks drain, then the call guard, both against the one stop deadline.
        startup.holder.set_stopping();
        if let Some(orchestrator) = self.orchestrator.get() {
            orchestrator.drain(&deadline_token).await;
        }
        startup.component.stop(deadline_token).await
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

#[cfg(test)]
#[allow(clippy::needless_pass_by_value)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gear_registry_tests.rs"]
mod gear_registry_tests;

#[cfg(test)]
#[allow(clippy::needless_pass_by_value)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gear_types_registration_tests.rs"]
mod gear_types_registration_tests;

#[cfg(test)]
#[allow(clippy::needless_pass_by_value)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gear_stop_drain_tests.rs"]
mod gear_stop_drain_tests;
