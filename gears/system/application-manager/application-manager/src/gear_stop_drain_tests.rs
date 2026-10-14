//! Unit tests for the gear's `stop` draining the request tasks (P7, DESIGN §3.2 "Request task
//! ownership" ("`drain` ... The startup component calls it from `stop`"), "Stop: the start
//! capability's `stop` sets the stopping flag in the startup result holder, which the REST
//! not-ready gate treats as not ready. It then drains the call guard with the `deadline_token`
//! that `stop` receives. The orchestrator's `drain` joins in the dispatch phase, before the
//! guard's and with the same token, so the two drains share one deadline", §4.8 Startup row
//! "`stop` draining the request tasks and the detached calls until the stop deadline").
//!
//! Pinned through the gear (`ApplicationManagerGear::stop`): the stopping flag is set at once; a
//! request task in flight is waited for until the deadline token is cancelled; at the deadline
//! the gear's stop returns `Ok` without waiting further and the abandoned request is logged
//! (`request_tasks_abandoned`, per operation, with the count). Stop before `init` still
//! succeeds (the P3 test).
#![allow(clippy::duration_suboptimal_units)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use toolkit::config::ConfigProvider;
use toolkit::{ClientHub, Gear, GearCtx, RunnableCapability};
use types_registry_sdk::TypesRegistryClient;
use uuid::Uuid;

use crate::domain::orchestrator::RequestInput;
use crate::domain::ports::StartupPort;
use crate::domain::request::AtCapacity;
use crate::gear::ApplicationManagerGear;
use crate::infra::call_guard::{CallKind, CallMeta};
use crate::test_support::registry_db::{self, RegistryTestDb};
use crate::test_support::{
    AuthzScript, FakeAuthz, FakeTypesRegistry, bounded, capture_logs, field_value, has_field,
    security_context, seed_body, startup_result_without_sections,
};
use application_manager_sdk::models::HandoffOperation;
use authz_resolver_sdk::AuthZResolverApi;

const GEAR_NAME: &str = "application-manager";
const ROUNDING: Duration = Duration::from_millis(1);

struct StaticConfigProvider {
    root: Value,
}

impl ConfigProvider for StaticConfigProvider {
    fn get_gear_config(&self, gear: &str) -> Option<&Value> {
        self.root.get(gear)
    }
}

// A gear with a database, a types registry and a PDP that never answers, so a request stays in
// flight for `pdp_call_timeout` (100ms); published, so a request reaches the permission step.
// The database is created on the real clock, then the clock is paused.
//
// The assumption these tests rely on: the gear has a database handle (the orchestrator exists
// only then; without one `init` builds none and `start` fails `database_unavailable`), which
// `with_db` gives it here. `gear.orchestrator().expect(..)` below fails loudly if that changes.
async fn running_gear() -> (ApplicationManagerGear, RegistryTestDb) {
    running_gear_with_cap(4).await
}

async fn running_gear_with_cap(
    max_request_tasks: usize,
) -> (ApplicationManagerGear, RegistryTestDb) {
    let db = registry_db::sqlite().await;
    let hub = Arc::new(ClientHub::new());
    hub.register::<dyn TypesRegistryClient>(Arc::new(FakeTypesRegistry::new()));
    hub.register::<dyn AuthZResolverApi>(Arc::new(FakeAuthz::new(AuthzScript::Hang)));
    let ctx = GearCtx::new(
        GEAR_NAME,
        Uuid::new_v4(),
        Arc::new(StaticConfigProvider {
            root: json!({ GEAR_NAME: { "config": {
                "plugin_call_timeout": "500ms",
                "plugin_max_in_flight": 8,
                "max_request_tasks": max_request_tasks,
                "max_envelope_text_length": 255,
                "pdp_call_timeout": "100ms",
                "registry_operation_timeout": "3s"
            } } }),
        }),
        hub,
        CancellationToken::new(),
    )
    .with_db(db.provider.clone());
    tokio::time::pause();
    let gear = ApplicationManagerGear::default();
    gear.init(&ctx).await.expect("init succeeds");
    gear.startup_holder()
        .expect("holder")
        .publish_result(startup_result_without_sections())
        .expect("published once");
    (gear, db)
}

fn start_seed(gear: &ApplicationManagerGear) -> crate::domain::request::RequestHandle {
    let orchestrator = gear.orchestrator().expect("an orchestrator");
    let permit = orchestrator
        .try_admit(HandoffOperation::Seed)
        .expect("a free permit");
    orchestrator.start_request(
        permit,
        security_context(),
        RequestInput::Seed {
            body: seed_body("app-1", "1.0", &json!({})),
        },
    )
}

// DESIGN §3.2 Stop: the stopping flag is set at once (the REST gate then refuses new requests),
// then the request tasks are drained: `stop` returns only after the request in flight completed.
#[tokio::test]
async fn stop_sets_the_stopping_flag_first_and_waits_for_the_running_request() {
    let (gear, _db) = running_gear().await;
    let handle = start_seed(&gear);
    let started = tokio::time::Instant::now();
    let stop = gear.stop(CancellationToken::new());
    tokio::pin!(stop);

    tokio::select! {
        result = &mut stop => panic!("stop returned while a request was in flight: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(10)) => {}
    }
    assert!(
        gear.startup_holder().expect("holder").is_stopping(),
        "the flag is set before the drain waits"
    );
    assert!(!handle.is_finished(), "the request is still in flight");
    bounded(stop).await.expect("stop succeeds");

    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(100) && elapsed <= Duration::from_millis(100) + ROUNDING,
        "one pdp_call_timeout, the request's whole wait: {elapsed:?}"
    );
    assert!(
        handle.is_finished(),
        "the request ended before stop returned"
    );
    assert_eq!(
        gear.orchestrator()
            .expect("orchestrator")
            .in_flight_request_tasks(),
        0
    );
}

// At the deadline `stop` does not wait for ever: it returns `Ok` and logs the abandoned request.
#[tokio::test]
async fn stop_at_its_deadline_abandons_the_running_request_and_logs_it() {
    let (log, _guard) = capture_logs();
    let (gear, _db) = running_gear().await;
    let handle = start_seed(&gear);
    let deadline = CancellationToken::new();
    deadline.cancel();
    let started = tokio::time::Instant::now();

    bounded(gear.stop(deadline)).await.expect("stop succeeds");

    assert!(
        started.elapsed() <= ROUNDING,
        "no wait at an expired deadline"
    );
    let lines = log.lines_with("request_tasks_abandoned");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(has_field(&lines[0], "operation", "seed"), "{}", lines[0]);
    assert_eq!(
        field_value(&lines[0], "count").as_deref(),
        Some("1"),
        "{}",
        lines[0]
    );
    assert!(!handle.is_finished(), "nothing was cancelled");
    bounded(handle)
        .await
        .expect("the abandoned request still ends");
}

// DESIGN §3.2 / §4.7: the gear's request-task cap is the `max_request_tasks` setting (here 2,
// not the per-plugin 8): two permits, the third request refused at capacity, a dropped permit
// frees its slot.
#[tokio::test]
async fn the_gear_admits_exactly_max_request_tasks_requests() {
    let (gear, _db) = running_gear_with_cap(2).await;
    let orchestrator = gear.orchestrator().expect("an orchestrator");

    let first = orchestrator.try_admit(HandoffOperation::Seed);
    let second = orchestrator.try_admit(HandoffOperation::Uninstall);
    let third = orchestrator.try_admit(HandoffOperation::Seed);

    assert!(first.is_ok() && second.is_ok(), "two permits");
    assert_eq!(third.err(), Some(AtCapacity), "the third is refused");
    drop(first);
    assert!(
        orchestrator.try_admit(HandoffOperation::Seed).is_ok(),
        "a dropped permit frees a slot"
    );
    drop(second);
}

// DESIGN §3.2 Stop: "The orchestrator's `drain` joins ... before the guard's and with the same
// token". With one request task and one detached plugin call both still running at an expired
// deadline, `stop` returns `Ok`, logs BOTH abandonments, and the request-task line comes first.
#[tokio::test]
async fn stop_drains_the_request_tasks_before_the_call_guard_with_one_deadline() {
    let (log, _guard) = capture_logs();
    let (gear, _db) = running_gear().await;
    // A guarded plugin call that never ends: past the plugin limit it is detached and keeps
    // counting in the guard (the guard the stop's second drain waits for).
    let guard = gear
        .startup
        .get()
        .expect("initialized")
        .component
        .guard()
        .clone();
    let hung = tokio::spawn(async move {
        guard
            .call(
                "section-1",
                CallMeta {
                    request_id: Some(Uuid::from_u128(9)),
                    call_kind: CallKind::Apply,
                    section_key: Some("files".to_owned()),
                },
                std::future::pending::<Result<(), ()>>(),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(hung.is_finished(), "the call timed out and was detached");
    let handle = start_seed(&gear);
    let deadline = CancellationToken::new();
    deadline.cancel();

    bounded(gear.stop(deadline)).await.expect("stop succeeds");

    let text = log.text();
    let requests = text.find("request_tasks_abandoned");
    let calls = text.find("plugin_calls_abandoned");
    assert!(requests.is_some(), "the request task was abandoned: {text}");
    assert!(
        calls.is_some(),
        "the detached plugin call was abandoned: {text}"
    );
    assert!(
        requests < calls,
        "the request-task drain comes first: {text}"
    );
    assert!(!handle.is_finished(), "nothing was cancelled");
    bounded(handle)
        .await
        .expect("the abandoned request still ends");
}
