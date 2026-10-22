//! Gear declaration and lifecycle.

use std::sync::{Arc, OnceLock};

use anyhow::anyhow;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use toolkit::api::OpenApiRegistry;
use toolkit::client_hub::ClientHub;
use toolkit::config::ConfigError;
use toolkit::contracts::{DatabaseCapability, RestApiCapability, RunnableCapability};
use toolkit::{Gear, GearCtx, Healthcheck};

use crate::api::rest;
use crate::api::rest::health::StartupHealthcheck;
use crate::config::ApplicationManagerConfig;
use crate::domain::plugin_set::StartupState;
use crate::domain::ports::{OperationRepository, PluginCalls};
use crate::domain::read_service::ReadService;
use crate::domain::schema_validator::SchemaValidator;
use crate::infra::startup;
use crate::sdk::{ApplicationManagerSectionPluginSpecV1, SECTION_BASE_TYPE};
use crate::sdk::{ApplicationManagerSectionPluginSpecV1, ApplicationManagerSectionV1};
use crate::infra::storage::SeaOrmOperationRepository;

/// The Application Manager gear.
#[toolkit::gear(
    name = "application-manager",
    deps = [types_registry, authz_resolver],
    capabilities = [rest, stateful, db],
)]
#[derive(Default)]
pub struct ApplicationManager {
    runtime: OnceLock<Runtime>,
}

/// What `init` builds; `start` and the routes use it.
struct Runtime {
    hub: Arc<ClientHub>,
    read: Arc<ReadService>,
    startup: Arc<StartupState>,
    #[expect(dead_code, reason = "the request orchestrator uses it in PR 5")]
    operations: Arc<dyn OperationRepository>,
    run_window_margin: Duration,
    storage_call_timeout: Duration,
}

impl ApplicationManager {
    fn runtime(&self, phase: &str) -> anyhow::Result<&Runtime> {
        self.runtime
            .get()
            .ok_or_else(|| anyhow!("{}: init must run before {phase}", Self::MODULE_NAME))
    }
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
        SECTION_BASE_TYPE,
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

        let db = ctx
            .db_required()
            .map_err(|e| anyhow!("{}: invalid_setting: database: {e}", Self::MODULE_NAME))?;

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

        let allowed = Arc::new(config.allowed_callers());
        let validator = SchemaValidator::new(config.max_envelope_text_length).map_err(|e| {
            anyhow!(
                "{}: invalid_setting: max_envelope_text_length: {e}",
                Self::MODULE_NAME
            )
        })?;
        let startup = Arc::new(StartupState::default());
        let guard = Arc::new(CallGuard::new(
            config.plugin_call_timeout,
            config.plugin_max_in_flight,
        ));
        let read = ReadService::new(
            access,
            Arc::clone(&startup),
            SectionPluginRouter::new(Arc::clone(&guard) as Arc<dyn PluginCalls>),
            Arc::new(validator),
        );
        let operations = Arc::new(SeaOrmOperationRepository::new(
            Arc::new(db),
            config.storage_call_timeout,
        ));
        let runtime = Runtime {
            hub: ctx.client_hub(),
            read: Arc::new(ReadService::new(
                allowed,
                Arc::clone(&startup),
                Arc::new(validator),
            )),
            startup,
            operations,
            run_window_margin: config.run_window_margin,
            storage_call_timeout: config.storage_call_timeout,
        };
        self.runtime
            .set(runtime)
            .map_err(|_| anyhow!("{}: already initialized", Self::MODULE_NAME))?;
        Ok(())
    }
}

impl DatabaseCapability for ApplicationManager {
    /// The `operations` and `audit_ledger` migrations.
    fn migrations(&self) -> Vec<Box<dyn sea_orm_migration::MigrationTrait>> {
        use sea_orm_migration::MigratorTrait;
        crate::infra::storage::Migrator::migrations()
    }
}

impl RestApiCapability for ApplicationManager {
    fn register_rest(
        &self,
        _ctx: &GearCtx,
        router: axum::Router,
        openapi: &dyn OpenApiRegistry,
    ) -> anyhow::Result<axum::Router> {
        let runtime = self.runtime("register_rest")?;
        Ok(rest::register_routes(
            router,
            openapi,
            Arc::clone(&runtime.read),
        ))
    }

    /// Readiness follows startup: not ready until `start` publishes the
    /// section plugin set (DESIGN 4.3). `None` before `init`.
    fn healthcheck(&self, _ctx: &GearCtx) -> Option<Arc<dyn Healthcheck>> {
        let runtime = self.runtime.get()?;
        Some(Arc::new(StartupHealthcheck::new(Arc::clone(
            &runtime.startup,
        ))))
    }
}

#[async_trait]
impl RunnableCapability for ApplicationManager {
    /// Build the section plugin set once and publish it. Any failed check
    /// fails the start phase, which stops the host; there is no retry.
    /// Cancellation ends startup with nothing published.
    async fn start(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        let runtime = self.runtime("start")?;
        let discovered = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                tracing::warn!("application-manager: start cancelled before the plugin set was fixed");
                return Err(anyhow!("{}: start cancelled", Self::MODULE_NAME));
            }
            discovered = startup::discover(&runtime.hub) => discovered,
        };
        let set = discovered.map_err(|e| {
            tracing::error!(reason = e.reason.as_str(), detail = %e.detail, "application-manager: startup failed");
            anyhow!("{}: {e}", Self::MODULE_NAME)
        })?;
        tracing::info!(keys = ?set.keys(), "application-manager: section plugin set fixed");
        runtime
            .startup
            .publish(set)
            .map_err(|_| anyhow!("{}: already started", Self::MODULE_NAME))?;
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
