//! Host-level startup tests (DESIGN §3.2 "Fault handling", §4.8 "Startup": "the host's
//! start phase failing with `RegistryError::Start` on a failed `start`", §4.12 platform
//! dependency 5).
//!
//! These tests drive the real toolkit `HostRuntime` (all phases) with a registry that
//! holds a no-op core gear and the planned `StartupComponent` as its stateful
//! capability, over a fake types registry. They do not link the `types-registry` or
//! `authz-resolver` gears, so no process is started here.
//!
//! Planned API: `crate::infra::startup::StartupComponent` (see `startup_tests.rs`).
//!
//! - `failed_start_makes_the_host_return_registry_error_start` (§3.2, §4.8): the run
//!   returns `Err` whose root is `RegistryError::Start { gear: "application-manager" }`
//!   and whose source carries the startup reason; the host never waits for shutdown.
//! - `failed_start_is_not_retried_by_the_host` (ADR 0001): one registry call, one key
//!   call per plugin, however long the host is left running.
//! - `good_plugin_set_lets_the_host_start_and_stop_the_gear` (§3.2 stop): the run
//!   waits for cancellation after a successful start, then the stop phase sets the
//!   stopping flag.
//!
//! The process-exit part of platform dependency 5 (the example server exits non-zero)
//! needs the example-server feature planned for P9; its test is
//! `apps/cf-gears-example-server/tests/application_manager_startup_exit_tests.rs`
//! (`#[ignore]`d with that reason). From the code: `run_server` returns the
//! `run_gear_phases` error and `main` returns it, so the exit status is 1 (Rust's
//! `main` returning `Err`); the ignored test proves it end to end.

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::{AuditPluginClientV1, SectionPluginClientV1};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use toolkit::GearCtx;
use toolkit::client_hub::{ClientHub, ClientScope};
use toolkit::config::ConfigProvider;
use toolkit::contracts::{Gear, RunnableCapability};
use toolkit::registry::{RegistryBuilder, RegistryError};
use toolkit::runtime::{DbOptions, HostRuntime};
use types_registry_sdk::TypesRegistryClient;
use uuid::Uuid;

use crate::config::ApplicationManagerConfig;
use crate::domain::ports::StartupPort;
use crate::domain::startup::StartupResultHolder;
use crate::infra::startup::StartupComponent;
use crate::test_support::{
    FakeAuditPlugin, FakeSectionPlugin, FakeTypesRegistry, PluginFamily, plugin_instance,
};

const GEAR: &str = "application-manager";

struct NoConfig;

impl ConfigProvider for NoConfig {
    fn get_gear_config(&self, _gear: &str) -> Option<&serde_json::Value> {
        None
    }
}

// Stands in for the gear's `init`; the startup component is the capability under test.
struct CoreGear;

#[async_trait]
impl Gear for CoreGear {
    async fn init(&self, _ctx: &GearCtx) -> anyhow::Result<()> {
        Ok(())
    }
}

struct Fixture {
    host: HostRuntime,
    cancel: CancellationToken,
    holder: Arc<StartupResultHolder>,
    registry: Arc<FakeTypesRegistry>,
    sections: Vec<Arc<FakeSectionPlugin>>,
}

fn config() -> ApplicationManagerConfig {
    ApplicationManagerConfig {
        plugin_call_timeout: Duration::from_millis(500),
        plugin_max_in_flight: 8,
        max_request_tasks: 16,
        max_envelope_text_length: 255,
        pdp_call_timeout: Duration::from_secs(2),
        registry_operation_timeout: Duration::from_secs(3),
    }
}

// A host over one section plugin; `with_audit` controls whether the plugin set is valid.
fn fixture(with_audit: bool) -> Fixture {
    let hub = Arc::new(ClientHub::new());
    let mut instances = Vec::new();
    let (section_id, section_instance) =
        plugin_instance(PluginFamily::Section, "acme.net.core.net.v1", 1);
    instances.push(section_instance);
    let section = Arc::new(FakeSectionPlugin::new("network"));
    let section_client: Arc<dyn SectionPluginClientV1> = section.clone();
    hub.register_scoped::<dyn SectionPluginClientV1>(
        ClientScope::gts_id(&section_id),
        section_client,
    );
    if with_audit {
        let (audit_id, audit_instance) =
            plugin_instance(PluginFamily::Audit, "acme.audit.core.a.v1", 0);
        instances.push(audit_instance);
        let audit_client: Arc<dyn AuditPluginClientV1> = Arc::new(FakeAuditPlugin::new());
        hub.register_scoped::<dyn AuditPluginClientV1>(
            ClientScope::gts_id(&audit_id),
            audit_client,
        );
    }
    let registry = Arc::new(FakeTypesRegistry::new().with_instances(instances));
    let registry_client: Arc<dyn TypesRegistryClient> = registry.clone();
    hub.register::<dyn TypesRegistryClient>(registry_client);

    let holder = Arc::new(StartupResultHolder::new());
    let component = Arc::new(StartupComponent::new(
        &config(),
        Arc::clone(&hub),
        Arc::clone(&holder),
    ));
    let mut builder = RegistryBuilder::default();
    builder.register_core_with_meta(GEAR, &[], Arc::new(CoreGear));
    builder.register_stateful_with_meta(GEAR, component as Arc<dyn RunnableCapability>);
    let gear_registry = builder.build_topo_sorted().expect("registry builds");

    let cancel = CancellationToken::new();
    let host = HostRuntime::new(
        gear_registry,
        Arc::new(NoConfig),
        DbOptions::None,
        hub,
        cancel.clone(),
        Uuid::new_v4(),
        None,
    );
    Fixture {
        host,
        cancel,
        holder,
        registry,
        sections: vec![section],
    }
}

// DESIGN §3.2 Fault handling, §4.8: the host's start phase returns `RegistryError::Start`
// at once on a failed `start`, with no retry, and the error propagates out of the run.
#[tokio::test]
async fn failed_start_makes_the_host_return_registry_error_start() {
    let fixture = fixture(false);

    // No cancellation is ever sent: the run must end by itself.
    let outcome = tokio::time::timeout(Duration::from_secs(30), fixture.host.run_gear_phases())
        .await
        .expect("a failed start ends the run without waiting for shutdown");

    let err = outcome.expect_err("the run fails");
    match err.downcast_ref::<RegistryError>() {
        Some(RegistryError::Start { gear, source }) => {
            assert_eq!(*gear, GEAR);
            let text = format!("{source}");
            assert!(
                text.starts_with("no_audit_plugin"),
                "the source carries the startup reason, got: {text}"
            );
        }
        other => panic!("expected RegistryError::Start, got {other:?} ({err:#})"),
    }
    assert!(!fixture.holder.is_ready());
}

// ADR 0001: nothing in the host retries a failed start either.
#[tokio::test(start_paused = true)]
async fn failed_start_is_not_retried_by_the_host() {
    let fixture = fixture(false);
    let registry = Arc::clone(&fixture.registry);
    let section = Arc::clone(&fixture.sections[0]);

    let _ = tokio::time::timeout(Duration::from_secs(30), fixture.host.run_gear_phases())
        .await
        .expect("the run ends");
    // A minute on the paused clock: any background retry would have run by now.
    tokio::time::sleep(Duration::from_secs(60)).await;

    assert_eq!(registry.list_calls(), 1);
    assert_eq!(
        section.call_count(),
        0,
        "the audit check failed before any key call"
    );
}

// DESIGN §3.2: a valid plugin set starts; the host then waits for cancellation and its
// stop phase calls the capability's `stop`, which sets the stopping flag.
#[tokio::test]
async fn good_plugin_set_lets_the_host_start_and_stop_the_gear() {
    let fixture = fixture(true);
    let holder = Arc::clone(&fixture.holder);
    let registry = Arc::clone(&fixture.registry);
    let cancel = fixture.cancel.clone();
    let run = tokio::spawn(fixture.host.run_gear_phases());

    tokio::time::timeout(Duration::from_secs(30), async {
        while !holder.is_ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the gear becomes ready after the start phase");
    assert!(
        !run.is_finished(),
        "the host keeps running after a good start"
    );
    cancel.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("the run ends after cancellation")
        .expect("the run task joins");

    outcome.expect("a clean shutdown");
    assert!(
        holder.is_stopping(),
        "the stop phase ran the capability's stop"
    );
    assert_eq!(registry.list_calls(), 1);
}
