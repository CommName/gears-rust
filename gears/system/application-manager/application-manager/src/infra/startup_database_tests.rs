//! Unit tests for the start capability's database check (DESIGN §3.2 "Startup and
//! Registration": "every startup fault fails the start phase at once ... no retry",
//! "`init` ... builds the registry store" (§3.2 "Ports and Composition"), §4.6 "Startup
//! failure catalogue", ADR 0001, ADR 0007).
//!
//! Planned API in `crate::infra::startup`:
//!
//! ```text
//! impl StartupComponent {
//!     /// Marks the gear database as missing: `start` then fails AT ONCE with the reason
//!     /// `database_unavailable` (the error text starts `database_unavailable: `, one ERROR
//!     /// `startup_failed` event with `reason=database_unavailable` and no instances), AFTER the
//!     /// `already_started` check (a published result wins) and BEFORE any call: no types registry listing, no plugin
//!     /// call, nothing published, not ready. No retry: one failed start is final.
//!     /// A component built with `new` alone has its database (existing behaviour unchanged).
//!     pub fn without_database(self) -> Self;
//! }
//! ```
//!
//! Decisions: the reason token `database_unavailable` joins the §4.6 catalogue (phase `start`;
//! trigger: the gear context carries no database handle for the registry store; operator
//! action: configure the gear database); the check is an `init`-time fact (`ctx.db()` is
//! `None`) reported by `start`, because the §4.6 reasons other than `invalid_setting` are
//! `start`'s.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use toolkit::RunnableCapability;
use toolkit::client_hub::ClientHub;
use types_registry_sdk::TypesRegistryClient;

use crate::config::ApplicationManagerConfig;
use crate::domain::ports::StartupPort;
use crate::domain::startup::StartupResultHolder;
use crate::infra::startup::StartupComponent;
use crate::test_support::{
    FakeTypesRegistry, bounded, capture_logs, field_value, has_field,
    startup_result_without_sections,
};

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

struct Env {
    component: StartupComponent,
    holder: Arc<StartupResultHolder>,
    registry: Arc<FakeTypesRegistry>,
}

fn env(without_database: bool) -> Env {
    let hub = Arc::new(ClientHub::new());
    let registry = Arc::new(FakeTypesRegistry::new());
    let client: Arc<dyn TypesRegistryClient> = registry.clone();
    hub.register::<dyn TypesRegistryClient>(client);
    let holder = Arc::new(StartupResultHolder::new());
    let component = StartupComponent::new(&config(), hub, Arc::clone(&holder));
    Env {
        component: if without_database {
            component.without_database()
        } else {
            component
        },
        holder,
        registry,
    }
}

#[tokio::test(start_paused = true)]
async fn start_without_a_database_fails_at_once_with_the_reason_first() {
    let env = env(true);
    let started = Instant::now();

    let err = bounded(env.component.start(CancellationToken::new()))
        .await
        .expect_err("a start with no database must fail");

    assert!(
        format!("{err}").starts_with("database_unavailable: "),
        "reason token first, as the other start reasons: {err}"
    );
    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "at once: no wait, no retry"
    );
}

#[tokio::test(start_paused = true)]
async fn the_database_check_precedes_every_call_and_publishes_nothing() {
    let env = env(true);

    bounded(env.component.start(CancellationToken::new()))
        .await
        .expect_err("fails");

    assert_eq!(
        env.registry.total_calls(),
        0,
        "no types registry call of any kind"
    );
    assert!(env.holder.result().is_none(), "nothing published");
    assert!(!env.holder.is_ready(), "not ready");
}

#[tokio::test(start_paused = true)]
async fn the_failure_is_logged_exactly_once_with_the_reason_and_no_instances() {
    let (logs, _guard) = capture_logs();
    let env = env(true);

    bounded(env.component.start(CancellationToken::new()))
        .await
        .expect_err("fails");

    logs.assert_live();
    let lines = logs.lines_with("startup_failed");
    assert_eq!(
        lines.len(),
        1,
        "exactly one startup failure event: {lines:?}"
    );
    assert!(lines[0].contains("ERROR"), "{}", lines[0]);
    assert!(
        has_field(&lines[0], "reason", "database_unavailable"),
        "{}",
        lines[0]
    );
    assert_eq!(
        field_value(&lines[0], "instances").unwrap_or_default(),
        "",
        "no instance is involved: {}",
        lines[0]
    );
    assert!(logs.lines_with("startup_published").is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_second_start_fails_the_same_way_one_failure_per_start_and_no_retry_inside() {
    let (logs, _guard) = capture_logs();
    let env = env(true);

    let first = bounded(env.component.start(CancellationToken::new())).await;
    let second = bounded(env.component.start(CancellationToken::new())).await;

    assert!(first.is_err() && second.is_err());
    assert_eq!(
        logs.lines_with("startup_failed").len(),
        2,
        "one event per failed start"
    );
    assert_eq!(env.registry.total_calls(), 0);
}

// The default component keeps its database: the missing-database failure is opt-in, so the
// P3 behaviour (discovery and publication) is unchanged.
#[tokio::test(start_paused = true)]
async fn a_component_with_its_database_goes_on_to_discovery() {
    let env = env(false);

    let err = bounded(env.component.start(CancellationToken::new()))
        .await
        .expect_err("an empty plugin set has no audit plugin");

    assert!(format!("{err}").starts_with("no_audit_plugin"), "{err}");
    assert_eq!(
        env.registry.list_calls(),
        1,
        "the listing ran: the database check passed"
    );
}

// ADR 0007: no tenant service is involved in this check either (the component has no
// tenant resolver in its hub).
#[tokio::test(start_paused = true)]
async fn the_database_check_needs_nothing_from_the_hub() {
    let hub = Arc::new(ClientHub::new());
    let holder = Arc::new(StartupResultHolder::new());
    let component = StartupComponent::new(&config(), hub, holder).without_database();

    let err = bounded(component.start(CancellationToken::new()))
        .await
        .expect_err("fails");

    assert!(
        format!("{err}").starts_with("database_unavailable"),
        "the database check comes before the types registry lookup: {err}"
    );
}

// DESIGN §3.2 "Startup and Registration": `already_started` is the first check, so a start over
// an already published result reports that, not the missing database. The published result is
// left untouched, and one failed start is one `startup_failed` event with that reason.
#[tokio::test(start_paused = true)]
async fn a_published_result_wins_over_the_database_check() {
    let (logs, _guard) = capture_logs();
    let env = env(true);
    env.holder
        .publish_result(startup_result_without_sections())
        .expect("published once");

    let err = bounded(env.component.start(CancellationToken::new()))
        .await
        .expect_err("a second start must fail");

    assert!(
        format!("{err}").starts_with("already_started"),
        "already_started, not database_unavailable: {err}"
    );
    assert!(env.holder.result().is_some(), "the published result stays");
    assert_eq!(env.registry.total_calls(), 0);
    logs.assert_live();
    let lines = logs.lines_with("startup_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        has_field(&lines[0], "reason", "already_started"),
        "{}",
        lines[0]
    );
}

// A failed database start leaves nothing to undo: stop still succeeds.
#[tokio::test(start_paused = true)]
async fn stop_still_succeeds_after_a_start_that_failed_for_the_database() {
    let env = env(true);
    bounded(env.component.start(CancellationToken::new()))
        .await
        .expect_err("fails");

    env.component
        .stop(CancellationToken::new())
        .await
        .expect("stop succeeds");

    assert!(env.holder.is_stopping(), "stop marks the holder stopping");
}
