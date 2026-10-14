//! Unit tests for the registration of the two plugin GTS types by `init` (P7, plan item "`init`
//! registers the section plugin and audit plugin GTS types in the types registry (DESIGN §3.4,
//! §3.6 startup sequence); test that registration failure fails `init` with no retry"; DESIGN §3.2
//! "Startup and Registration" ("Startup call bound: each startup call is made once, with no retry,
//! and is bounded by `plugin_call_timeout`"), §3.4 "Register the two plugin GTS types during
//! `init`", §3.6 sequence step "register section and audit plugin GTS types", §4.6 "`startup_failed`
//! ... exactly once per failed start, with `reason`, `instances` ... and an optional `cause`. The
//! error that `start` returns is the reason followed by a fixed detail, never the cause", ADR 0001
//! no retry, ADR 0007 no tenant service).
//!
//! Planned behaviour of `ApplicationManagerGear::init` (`crate::gear`), after the configuration
//! was read and validated (an invalid configuration still fails first, with `invalid_setting`,
//! and asks the registry nothing):
//!
//! - ONE call `TypesRegistryClient::register_type_schemas(vec![section, audit])`, made once, no
//!   retry, bounded by `plugin_call_timeout`. The two documents are the ones the SDK's
//!   `#[gts_type_schema]` types generate (`gts_schema_with_refs_as_string`), in this order:
//!   section plugin type, audit plugin type. They are what the link-time inventory registers
//!   too, so the registration is create-if-absent and idempotent for the same document;
//! - success is `Ok` with exactly one `RegisterResult::Ok` per schema, in order, each carrying the
//!   id of its schema. Anything else fails `init`: no types registry client in the hub, a failed
//!   call, a call that does not answer within `plugin_call_timeout`, a result list that does not
//!   have one entry per schema (none, one, three), any `RegisterResult::Err`, an `Ok` with
//!   another id;
//! - a second `init` of one gear instance fails `already initialized` BEFORE it registers;
//! - the new startup failure reason `type_registration_failed` (phase `init`), added to the
//!   catalogue of DESIGN §4.6 (decision 7 of the P7 test report). It is not
//!   `types_registry_unreachable`: that reason is the `start` listing, and a registry that
//!   rejects a type definition is reachable. The error text is the reason followed by a fixed
//!   detail (never the cause); exactly one `startup_failed` event (error level) names the reason
//!   and the two type ids as `instances`;
//! - a failed `init` leaves nothing built: no startup holder, no orchestrator; `start` then
//!   fails as it does before any `init`.
#![allow(clippy::duration_suboptimal_units)]

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::{
    ApplicationManagerAuditPluginSpecV1, ApplicationManagerSectionPluginSpecV1,
};
use serde_json::{Value, json};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use toolkit::config::ConfigProvider;
use toolkit::{ClientHub, Gear, GearCtx, RunnableCapability};
use types_registry_sdk::TypesRegistryClient;
use uuid::Uuid;

use crate::gear::ApplicationManagerGear;
use crate::test_support::registry_db;
use crate::test_support::{
    FakeTypesRegistry, TenantResolverTripwire, bounded, capture_logs, field_value, has_field,
};
use tenant_resolver_sdk::TenantResolverClient;

const GEAR_NAME: &str = "application-manager";

struct StaticConfigProvider {
    root: Value,
}

impl ConfigProvider for StaticConfigProvider {
    fn get_gear_config(&self, gear: &str) -> Option<&Value> {
        self.root.get(gear)
    }
}

fn valid_config() -> Value {
    json!({
        "plugin_call_timeout": "500ms",
        "plugin_max_in_flight": 8,
        "max_request_tasks": 16,
        "max_envelope_text_length": 255,
        "pdp_call_timeout": "2s",
        "registry_operation_timeout": "3s"
    })
}

fn section_type() -> String {
    ApplicationManagerSectionPluginSpecV1::gts_type_id().to_string()
}

fn audit_type() -> String {
    ApplicationManagerAuditPluginSpecV1::gts_type_id().to_string()
}

fn ctx_over(hub: Arc<ClientHub>, config: Value) -> GearCtx {
    GearCtx::new(
        GEAR_NAME,
        Uuid::new_v4(),
        Arc::new(StaticConfigProvider {
            root: json!({ GEAR_NAME: { "config": config } }),
        }),
        hub,
        CancellationToken::new(),
    )
}

fn ctx_with(registry: &Arc<FakeTypesRegistry>, config: Value) -> GearCtx {
    let hub = Arc::new(ClientHub::new());
    hub.register::<dyn TypesRegistryClient>(Arc::clone(registry) as Arc<dyn TypesRegistryClient>);
    ctx_over(hub, config)
}

fn id_of(schema: &Value) -> String {
    let id = schema["$id"].as_str().expect("a $id");
    id.strip_prefix("gts://").unwrap_or(id).to_owned()
}

// DESIGN §3.4 / §3.6: one registration call with both types, section first, each the document
// the SDK type generates.
#[tokio::test]
async fn init_registers_the_section_and_audit_plugin_types_in_one_call() {
    let registry = Arc::new(FakeTypesRegistry::new());
    let ctx = ctx_with(&registry, valid_config());
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx).await.expect("init succeeds");

    assert_eq!(registry.registration_calls(), 1);
    assert_eq!(registry.total_calls(), 1, "no listing, no other call");
    let batches = registry.registered();
    assert_eq!(batches[0].len(), 2);
    assert_eq!(id_of(&batches[0][0]), section_type());
    assert_eq!(id_of(&batches[0][1]), audit_type());
    let section: Value = serde_json::from_str(
        &ApplicationManagerSectionPluginSpecV1::gts_schema_with_refs_as_string(),
    )
    .expect("the SDK schema is JSON");
    let audit: Value = serde_json::from_str(
        &ApplicationManagerAuditPluginSpecV1::gts_schema_with_refs_as_string(),
    )
    .expect("the SDK schema is JSON");
    assert_eq!(
        batches[0][0], section,
        "the document of the SDK type, unchanged"
    );
    assert_eq!(
        batches[0][1], audit,
        "the document of the SDK type, unchanged"
    );
}

// The registration does not depend on the database: with and without a handle, once.
#[tokio::test]
async fn the_registration_is_made_with_and_without_a_database_handle() {
    let without = Arc::new(FakeTypesRegistry::new());
    ApplicationManagerGear::default()
        .init(&ctx_with(&without, valid_config()))
        .await
        .expect("init succeeds");
    let with = Arc::new(FakeTypesRegistry::new());
    let db = registry_db::sqlite().await;
    let ctx = ctx_with(&with, valid_config()).with_db(db.provider.clone());
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx).await.expect("init succeeds");

    assert_eq!(without.registration_calls(), 1);
    assert_eq!(with.registration_calls(), 1);
    assert!(
        gear.orchestrator().is_some(),
        "the orchestrator is built after the registration"
    );
}

// DESIGN §4.7: the configuration is validated first; a bad one asks the registry nothing.
#[tokio::test]
async fn an_invalid_configuration_fails_before_the_registry_is_asked() {
    let registry = Arc::new(FakeTypesRegistry::new());
    let mut config = valid_config();
    config["plugin_call_timeout"] = json!("0s");
    let gear = ApplicationManagerGear::default();

    let err = gear
        .init(&ctx_with(&registry, config))
        .await
        .expect_err("an invalid setting fails init");

    assert!(format!("{err}").starts_with("invalid_setting"), "{err}");
    assert_eq!(registry.total_calls(), 0);
}

/// A types registry whose registration fails in one way, and what `init` must do about it.
fn failing_registries() -> Vec<(&'static str, FakeTypesRegistry)> {
    vec![
        (
            "the call fails",
            FakeTypesRegistry::new().registration_failing(),
        ),
        (
            "the section type is rejected",
            FakeTypesRegistry::new().registration_rejecting(0),
        ),
        (
            "the audit type is rejected",
            FakeTypesRegistry::new().registration_rejecting(1),
        ),
        (
            "no result per schema",
            FakeTypesRegistry::new().registration_without_results(),
        ),
        (
            "one result for two schemas",
            FakeTypesRegistry::new().registration_with_result_count(1),
        ),
        (
            "three results for two schemas",
            FakeTypesRegistry::new().registration_with_result_count(3),
        ),
        (
            "an Ok with another id",
            FakeTypesRegistry::new().registration_answering_other_ids(),
        ),
    ]
}

// DESIGN §3.2 fault handling: every startup fault fails at once with a named reason, once, with
// no retry; the error is the reason and a fixed detail, never the cause. With and without a
// database handle: a failed init leaves no orchestrator and no holder either way.
#[tokio::test]
async fn a_registration_failure_fails_init_once_with_its_reason_and_no_retry() {
    // The database is made on the real clock; the clock is paused for the waits below.
    let db = registry_db::sqlite().await;
    tokio::time::pause();
    for with_db in [false, true] {
        for (name, fake) in failing_registries() {
            let name = format!("{name}, database: {with_db}");
            let (log, _guard) = capture_logs();
            let registry = Arc::new(fake);
            let mut ctx = ctx_with(&registry, valid_config());
            if with_db {
                ctx = ctx.with_db(db.provider.clone());
            }
            let gear = ApplicationManagerGear::default();
            let started = Instant::now();

            let err = bounded(gear.init(&ctx))
                .await
                .expect_err("a registration failure fails init");

            assert!(
                format!("{err}").starts_with("type_registration_failed: "),
                "{name}: {err}"
            );
            // Every cause the fake registry gives starts with its own name: it is logged, never
            // part of the error.
            assert!(
                !format!("{err}").contains("fake types registry"),
                "{name}: the cause is not part of the error: {err}"
            );
            assert_eq!(started.elapsed(), Duration::ZERO, "{name}: at once");
            // No retry: one call now, and still one after a long wait.
            assert_eq!(registry.registration_calls(), 1, "{name}");
            tokio::time::sleep(Duration::from_secs(3600)).await;
            assert_eq!(
                registry.registration_calls(),
                1,
                "{name}: no retry later either"
            );
            assert_eq!(registry.total_calls(), 1, "{name}");
            // Exactly one startup_failed event, with the reason and the two type ids.
            let lines = log.lines_with("startup_failed");
            assert_eq!(lines.len(), 1, "{name}: {lines:?}");
            assert!(
                has_field(&lines[0], "reason", "type_registration_failed"),
                "{name}: {}",
                lines[0]
            );
            let instances = field_value(&lines[0], "instances").unwrap_or_default();
            assert!(
                instances.contains(&section_type()) && instances.contains(&audit_type()),
                "{name}: the instances name both types: {}",
                lines[0]
            );
            assert!(
                lines[0].contains(" ERROR "),
                "{name}: error level: {}",
                lines[0]
            );
            // Nothing half-built.
            assert!(gear.startup_holder().is_none(), "{name}");
            assert!(gear.orchestrator().is_none(), "{name}");
            let started_err = gear
                .start(CancellationToken::new())
                .await
                .expect_err("start before a successful init fails");
            assert!(
                format!("{started_err}").contains("before init"),
                "{name}: {started_err}"
            );
        }
    }
}

// The `startup_failed` event of a rejected or unexpected registration item carries a bounded,
// escaped cause that names the item and the registry's own error; the error text stays the
// reason and a fixed detail.
#[tokio::test]
async fn a_rejected_item_logs_the_registrys_cause_for_that_item() {
    tokio::time::pause();
    let (log, _guard) = capture_logs();
    let registry = Arc::new(FakeTypesRegistry::new().registration_rejecting(1));
    let ctx = ctx_with(&registry, valid_config());
    let gear = ApplicationManagerGear::default();

    let err = bounded(gear.init(&ctx)).await.expect_err("init fails");

    assert_eq!(
        format!("{err}"),
        "type_registration_failed: a plugin type was not registered"
    );
    let lines = log.lines_with("startup_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    let line = &lines[0];
    assert!(line.contains("cause="), "{line}");
    assert!(
        line.contains("schema rejected"),
        "the registry's error: {line}"
    );
    assert!(
        line.contains("item 1"),
        "the item index that was rejected: {line}"
    );
    assert!(!line.contains('\n'), "one line: {line:?}");
}

// A second `init` of one gear instance fails as it did before P7 (`already initialized`) and does
// not register again: the guard comes before the registration.
#[tokio::test]
async fn a_second_init_fails_as_already_initialized_without_registering_again() {
    let registry = Arc::new(FakeTypesRegistry::new());
    let ctx = ctx_with(&registry, valid_config());
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("the first init succeeds");

    let err = gear.init(&ctx).await.expect_err("a second init fails");

    assert!(format!("{err}").contains("already initialized"), "{err}");
    assert_eq!(registry.registration_calls(), 1, "registered once");
    assert_eq!(registry.total_calls(), 1);
}

// DESIGN §3.4: the registration is create-if-absent. Against the real types registry service
// (in memory, the configuration phase the gears' `init` runs in), registering the same two
// documents again, as a second gear instance or the link-time inventory would, succeeds.
#[tokio::test]
async fn the_registration_is_idempotent_against_the_real_types_registry() {
    use types_registry::config::TypesRegistryConfig;
    use types_registry::domain::TypesRegistryService;
    use types_registry::domain::local_client::TypesRegistryLocalClient;
    use types_registry::infra::InMemoryGtsRepository;

    let config = TypesRegistryConfig::default();
    let repo = Arc::new(InMemoryGtsRepository::new(config.to_gts_config()));
    let service = Arc::new(TypesRegistryService::new(repo, config));
    let hub = Arc::new(ClientHub::new());
    hub.register::<dyn TypesRegistryClient>(
        Arc::new(TypesRegistryLocalClient::new(service)) as Arc<dyn TypesRegistryClient>
    );

    for attempt in 1..=2 {
        let gear = ApplicationManagerGear::default();
        gear.init(&ctx_over(Arc::clone(&hub), valid_config()))
            .await
            .unwrap_or_else(|e| panic!("init {attempt} against the real registry: {e}"));
    }
}

// A hub with no types registry client: the registration cannot be made, the same reason, and
// the event is logged once.
#[tokio::test(start_paused = true)]
async fn a_missing_types_registry_client_fails_init_with_the_same_reason() {
    let (log, _guard) = capture_logs();
    let ctx = ctx_over(Arc::new(ClientHub::new()), valid_config());
    let gear = ApplicationManagerGear::default();

    let err = gear
        .init(&ctx)
        .await
        .expect_err("no registry client fails init");

    assert!(
        format!("{err}").starts_with("type_registration_failed: "),
        "{err}"
    );
    let lines = log.lines_with("startup_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "reason", "type_registration_failed"),
        "{}",
        lines[0]
    );
}

// DESIGN §3.2: the call is bounded by `plugin_call_timeout`: a registry that never answers fails
// init after exactly one bound, with one call.
#[tokio::test(start_paused = true)]
async fn a_registration_that_never_answers_fails_init_after_one_plugin_call_timeout() {
    let (log, _guard) = capture_logs();
    let registry = Arc::new(FakeTypesRegistry::new().registration_hanging());
    let mut config = valid_config();
    config["plugin_call_timeout"] = json!("300ms");
    let ctx = ctx_with(&registry, config);
    let gear = ApplicationManagerGear::default();
    let started = Instant::now();

    let err = bounded(gear.init(&ctx))
        .await
        .expect_err("a hung registration fails init");

    assert!(
        format!("{err}").starts_with("type_registration_failed: "),
        "{err}"
    );
    assert_eq!(started.elapsed(), Duration::from_millis(300));
    assert_eq!(registry.registration_calls(), 1);
    let lines = log.lines_with("startup_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "reason", "type_registration_failed"),
        "{}",
        lines[0]
    );
    let instances = field_value(&lines[0], "instances").unwrap_or_default();
    assert!(
        instances.contains(&section_type()) && instances.contains(&audit_type()),
        "{}",
        lines[0]
    );
    assert!(lines[0].contains(" ERROR "), "{}", lines[0]);
}

// ADR 0007: the gear resolves no tenant service, in its registration or anywhere in `init`.
#[tokio::test]
async fn init_calls_no_tenant_service() {
    let tripwire = Arc::new(TenantResolverTripwire::new());
    let registry = Arc::new(FakeTypesRegistry::new());
    let hub = Arc::new(ClientHub::new());
    hub.register::<dyn TypesRegistryClient>(Arc::clone(&registry) as Arc<dyn TypesRegistryClient>);
    hub.register::<dyn TenantResolverClient>(Arc::clone(&tripwire) as Arc<dyn TenantResolverClient>);
    let gear = ApplicationManagerGear::default();

    gear.init(&ctx_over(hub, valid_config()))
        .await
        .expect("init succeeds");

    assert_eq!(tripwire.calls(), 0);
}
