#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use toolkit::api::OpenApiRegistryImpl;
use toolkit::api::openapi_registry::OpenApiInfo;
use toolkit_security::PlatformSecurityContext;
use tower::ServiceExt;

use super::register_routes;
use crate::constants::REFUSAL_RETRY_AFTER_SECONDS;
use crate::domain::actions::CallerAction;
use crate::domain::plugin_set::{PluginSet, StartupState};
use crate::domain::read_service::ReadService;
use crate::domain::schema_validator::SchemaValidator;
use crate::test_support::{FakePlugin, allowed_callers, platform_ctx, section_plugin};
use super::{IN_FLIGHT_LIMIT_ZONE, RATE_LIMIT_ZONE, register_routes};
use crate::api::rest::gate::RequestGate;
use crate::constants::REFUSAL_RETRY_AFTER_SECONDS;
use crate::domain::audit::{AuditEvent, AuditKind};
use crate::domain::orchestrator::RequestOrchestrator;
use crate::domain::plugin_set::StartupState;
use crate::domain::ports::AccessError;
use crate::domain::ports::OperationRepository;
use crate::infra::storage::SeaOrmOperationRepository;
use crate::test_support::{
    ApplyAnswer, CALL_LIMIT, FakeAccess, FakeClock, FakePlugin, Fault, HookedRepo, KeyAnswer,
    SchemaAnswer, T0, ctx, orchestrator, read_service, read_service_over, ready, section,
    sqlite_repo,
};
use axum::http::Method;
use std::time::Duration;
use uuid::Uuid;

const PATH: &str = "/application-manager/v1/section-keys";
const PROBLEM_MEMBERS: [&str; 7] = [
    "type", "title", "status", "detail", "instance", "trace_id", "context",
];

/// An orchestrator and a gate for the schema-read tests, which never reach them.
fn idle_orchestrator() -> Arc<RequestOrchestrator> {
    orchestrator(
        FakeAccess::new(Ok(())),
        Arc::new(StartupState::default()),
        HookedRepo::none(),
        FakeClock::new(T0),
        CALL_LIMIT,
    )
}

fn idle_gate() -> Arc<RequestGate> {
    Arc::new(RequestGate::new(Arc::new(StartupState::default()), 1))
}

struct Harness {
    router: Router,
    plugin: Arc<FakePlugin>,
}

/// Routes over a startup state, ready or not, where `app-seeder` holds
/// `actions`.
fn harness(ready: bool, actions: Vec<CallerAction>) -> Harness {
    let plugin = FakePlugin::new();
    let state = Arc::new(StartupState::default());
    if ready {
        state
            .publish(PluginSet::new(vec![
                section_plugin("cf.test.late.section", 9, &plugin),
                section_plugin("cf.core.noop.section", 1, &plugin),
            ]))
            .unwrap();
fn harness(ready_state: bool, answer: Result<(), AccessError>) -> Harness {
    serving(
        ready_state,
        answer,
        SchemaAnswer::serve(json!({ "type": "object" }), "1"),
    )
}

/// Two plugins, `noop` (priority 1) and `late` (priority 9), both answering
/// `schema` with `schema`.
fn serving(ready_state: bool, answer: Result<(), AccessError>, schema: SchemaAnswer) -> Harness {
    let plugin = FakePlugin::serving(KeyAnswer::Key("unused"), schema);
    let state = if ready_state {
        ready(vec![
            section("late", 9, &plugin),
            section("noop", 1, &plugin),
        ])
    } else {
        Arc::new(StartupState::default())
    };
    let access = FakeAccess::new(answer);
    let read = Arc::new(read_service(Arc::clone(&access), state));
    let router = register_routes(
        Router::new(),
        &OpenApiRegistryImpl::new(),
        read,
        idle_orchestrator(),
        idle_gate(),
    );
    Harness {
        router,
        access,
        plugin,
    }
    let read = Arc::new(ReadService::new(
        Arc::new(allowed_callers("app-seeder", actions)),
        state,
        Arc::new(SchemaValidator::new(255).unwrap()),
    ));
    let router = register_routes(Router::new(), &OpenApiRegistryImpl::new(), read);
    Harness { router, plugin }
}

async fn get(
    h: &Harness,
    ctx: PlatformSecurityContext,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    get_at(h, PATH, ctx).await
}

async fn get_at(
    h: &Harness,
    path: &str,
    ctx: PlatformSecurityContext,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut req = Request::builder().uri(path).body(Body::empty()).unwrap();
    req.extensions_mut().insert(ctx);
    let resp = h.router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    (status, headers, serde_json::from_slice(&bytes).unwrap())
}

fn assert_problem(body: &Value, kind: &str) {
    let kind_suffix = format!("cf.core.err.{kind}.v1~");
    assert!(
        body["type"].as_str().unwrap().ends_with(&kind_suffix),
        "{body}"
    );
    for member in body.as_object().unwrap().keys() {
        assert!(
            PROBLEM_MEMBERS.contains(&member.as_str()),
            "extension member {member}: {body}"
        );
    }
}

#[tokio::test]
async fn lists_keys_in_priority_order() {
    let h = harness(true, vec![CallerAction::ReadSchema]);
    let (status, _, body) = get(&h, platform_ctx("app-seeder")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({ "items": ["cf.core.noop.section", "cf.test.late.section"] })
    );
    assert_eq!(h.plugin.calls(), 0);
}

#[tokio::test]
async fn caller_not_allowed_is_403_with_no_plugin_call() {
    let cases = [
        ("someone-else", vec![CallerAction::ReadSchema]),
        ("app-seeder", vec![CallerAction::Seed]),
    ];
    for (caller, actions) in cases {
        let h = harness(true, actions);
        let (status, _, body) = get(&h, platform_ctx(caller)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{caller}");
        assert_problem(&body, "permission_denied");
        assert!(
            body["context"]["reason"]
                .as_str()
                .unwrap()
                .contains("read_schema"),
            "{body}"
        );
        assert_eq!(h.plugin.calls(), 0);
    }
}

#[tokio::test]
async fn not_ready_is_503_with_retry_after() {
    let h = harness(false, vec![CallerAction::ReadSchema]);
    let (status, headers, body) = get(&h, platform_ctx("app-seeder")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_problem(&body, "service_unavailable");
    assert_eq!(
        headers[header::RETRY_AFTER],
        REFUSAL_RETRY_AFTER_SECONDS.to_string().as_str()
    );
    assert!(
        body["detail"].as_str().unwrap().starts_with("request "),
        "{body}"
    );
    assert_eq!(h.plugin.calls(), 0);
}

const SECTION_PATH: &str = "/application-manager/v1/section-keys/cf.core.noop.section/schema";
const MANIFEST_PATH: &str = "/application-manager/v1/manifest-schema";

#[tokio::test]
async fn reads_one_section_schema_with_its_section_type() {
    let h = harness(true, vec![CallerAction::ReadSchema]);
    let (status, _, body) = get_at(&h, SECTION_PATH, platform_ctx("app-seeder")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "key": "cf.core.noop.section",
            "section_type": "gts.cf.core.application_manager.section.v1~cf.core.noop.section.v1~",
            "schema": {
                "$id": "urn:cf:application-manager:section:cf.core.noop.section",
                "type": "object"
            }
        })
    );
    assert_eq!(h.plugin.calls(), 0);
}

#[tokio::test]
async fn an_unknown_section_key_is_404_without_echoing_it() {
    let h = harness(true, vec![CallerAction::ReadSchema]);
    let path = "/application-manager/v1/section-keys/secret.key/schema";
    let (status, _, body) = get_at(&h, path, platform_ctx("app-seeder")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_problem(&body, "not_found");
    assert!(!body.to_string().contains("secret.key"), "{body}");
}

#[tokio::test]
async fn reads_the_combined_schema_with_every_section_type() {
    let h = harness(true, vec![CallerAction::ReadSchema]);
    let (status, _, body) = get_at(&h, MANIFEST_PATH, platform_ctx("app-seeder")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["sections"],
        json!([
            {
                "key": "cf.core.noop.section",
                "section_type": "gts.cf.core.application_manager.section.v1~cf.core.noop.section.v1~"
            },
            {
                "key": "cf.test.late.section",
                "section_type": "gts.cf.core.application_manager.section.v1~cf.test.late.section.v1~"
            }
        ])
    );
    assert_eq!(
        body["schema"]["$schema"],
        "http://json-schema.org/draft-07/schema#"
    );
    assert_eq!(
        body["schema"]["properties"]["sections"]["required"],
        json!(["cf.core.noop.section", "cf.test.late.section"])
    );
    assert_eq!(h.plugin.calls(), 0);
}

#[tokio::test]
async fn schema_reads_refuse_like_the_key_list() {
    for path in [SECTION_PATH, MANIFEST_PATH] {
        let h = harness(true, vec![CallerAction::Seed]);
        let (status, _, body) = get_at(&h, path, platform_ctx("app-seeder")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
        assert_problem(&body, "permission_denied");

        let h = harness(false, vec![CallerAction::ReadSchema]);
        let (status, headers, _) = get_at(&h, path, platform_ctx("app-seeder")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert!(headers.contains_key(header::RETRY_AFTER), "{path}");
#[tokio::test]
async fn unavailable_refusals_are_503_with_retry_after() {
    for path in [KEYS, NOOP_SCHEMA, MANIFEST_SCHEMA] {
        for (ready_state, answer, access_calls) in
            [(false, Ok(()), 0), (true, Err(AccessError::Unavailable), 1)]
        {
            let h = harness(ready_state, answer);
            let (status, headers, body) = get(&h, path).await;
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "{path} ready={ready_state}"
            );
            assert_problem(&body, "service_unavailable");
            assert_retry_after(&headers);
            assert!(
                body["detail"].as_str().unwrap().starts_with("request "),
                "{body}"
            );
            assert_eq!(h.access.calls(), access_calls, "{path} ready={ready_state}");
            assert_eq!(h.plugin.calls(), 0);
        }
    }
}

#[test]
fn only_the_combined_schema_read_is_throttled() {
    let registry = OpenApiRegistryImpl::new();
    let svc = Arc::new(ReadService::new(
        Arc::new(allowed_callers("app-seeder", vec![])),
        Arc::new(StartupState::default()),
        Arc::new(SchemaValidator::new(255).unwrap()),
    ));
    drop(register_routes(Router::new(), &registry, svc));
    let spec =
        serde_json::to_value(registry.build_openapi(&OpenApiInfo::default()).unwrap()).unwrap();
    let paths = &spec["paths"];
    let has_429 = |path: &str| paths[path]["get"]["responses"].get("429").is_some();
    assert!(has_429(MANIFEST_PATH), "{paths}");
    assert!(!has_429(
        "/application-manager/v1/section-keys/{section_key}/schema"
    ));
    assert!(!has_429(PATH));
fn schema_reads_are_documented_and_throttled() {
    let openapi = OpenApiRegistryImpl::new();
    let read = Arc::new(read_service(
        FakeAccess::new(Ok(())),
        Arc::new(StartupState::default()),
    ));
    let _router = register_routes(
        Router::new(),
        &openapi,
        read,
        idle_orchestrator(),
        idle_gate(),
    );
    let doc =
        serde_json::to_value(openapi.build_openapi(&OpenApiInfo::default()).unwrap()).unwrap();
    let paths = &doc["paths"];
    let single = &paths["/application-manager/v1/section-keys/{section_key}/schema"]["get"];
    assert_eq!(
        single["operationId"],
        "application_manager.get_section_schema"
    );
    for code in ["404", "429"] {
        assert!(single["responses"][code].is_object(), "{code}: {single}");
    }
    assert_eq!(single["x-throttling-rate-limit-zone"], RATE_LIMIT_ZONE);
    assert_eq!(
        single["x-throttling-in-flight-limit-zone"],
        IN_FLIGHT_LIMIT_ZONE
    );
    let combined = &paths[MANIFEST_SCHEMA]["get"];
    assert_eq!(
        combined["operationId"],
        "application_manager.get_manifest_schema"
    );
    assert_eq!(combined["x-throttling-rate-limit-zone"], RATE_LIMIT_ZONE);
    assert_eq!(
        combined["x-throttling-in-flight-limit-zone"],
        IN_FLIGHT_LIMIT_ZONE
    );
    assert!(combined["responses"]["429"].is_object(), "{combined}");
}

// ── Seed, uninstall and operation reads ─────────────────────────────────────

const APPS: &str = "/application-manager/v1/applications";
const UNINSTALL: &str = "/application-manager/v1/applications:uninstall";
const OPERATIONS: &str = "/application-manager/v1/operations";

/// The whole gear over a real in-memory repository, one `noop` plugin, and
/// the gate with two request-task permits.
struct Full {
    router: Router,
    access: Arc<FakeAccess>,
    plugin: Arc<FakePlugin>,
    repo: Arc<HookedRepo>,
    store: Arc<SeaOrmOperationRepository>,
    gate: Arc<RequestGate>,
}

async fn full(ready_state: bool, answer: Result<(), AccessError>) -> Full {
    let plugin = FakePlugin::serving(
        KeyAnswer::Key("unused"),
        SchemaAnswer::serve(json!({ "type": "object" }), "1"),
    );
    let state = if ready_state {
        ready(vec![section("noop", 1, &plugin)])
    } else {
        Arc::new(StartupState::default())
    };
    let access = FakeAccess::new(answer);
    let store = sqlite_repo().await;
    let repo = HookedRepo::over(Arc::clone(&store) as Arc<dyn OperationRepository>);
    let clock = FakeClock::new(T0);
    let read = Arc::new(read_service_over(
        Arc::clone(&access),
        Arc::clone(&state),
        Arc::clone(&repo),
        Arc::clone(&clock),
    ));
    let orch = orchestrator(
        Arc::clone(&access),
        Arc::clone(&state),
        Arc::clone(&repo),
        clock,
        CALL_LIMIT,
    );
    let gate = Arc::new(RequestGate::new(state, 2));
    let router = register_routes(
        Router::new(),
        &OpenApiRegistryImpl::new(),
        read,
        orch,
        Arc::clone(&gate),
    );
    Full {
        router,
        access,
        plugin,
        repo,
        store,
        gate,
    }
}

async fn send(
    h: &Full,
    method: Method,
    uri: &str,
    body: &[u8],
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_vec()))
        .unwrap();
    req.extensions_mut().insert(ctx());
    let resp = h.router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, headers, body)
}

fn seed_json(app: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "application": { "id": app, "version": "1" },
        "sections": { "noop": {} }
    }))
    .unwrap()
}

/// The three write routes with a valid request for application `app`.
type Write = (&'static str, Method, String, Vec<u8>);

fn writes(app: &str) -> Vec<Write> {
    vec![
        ("seed", Method::POST, APPS.to_owned(), seed_json(app)),
        (
            "delete",
            Method::DELETE,
            format!("{APPS}/{app}"),
            Vec::new(),
        ),
        (
            "uninstall",
            Method::POST,
            UNINSTALL.to_owned(),
            serde_json::to_vec(&json!({ "application_id": app })).unwrap(),
        ),
    ]
}

fn operation_id(body: &Value) -> Uuid {
    body["operation_id"].as_str().unwrap().parse().unwrap()
}

/// Poll the operation until it is final.
async fn final_operation(h: &Full, id: Uuid) -> Value {
    for _ in 0..1000 {
        let (status, _, body) = send(h, Method::GET, &format!("{OPERATIONS}/{id}"), b"").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if body["status"] == "completed" || body["status"] == "interrupted" {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("operation {id} did not become final");
}

fn only_refused_event(h: &Full) -> AuditEvent {
    let events = h.repo.refused_events.lock().unwrap();
    assert_eq!(events.len(), 1, "one refused event: {events:?}");
    assert_eq!(events[0].kind, AuditKind::Refused);
    events[0].clone()
}

fn refused_category(event: &AuditEvent) -> &str {
    event.content["result"]["category"].as_str().unwrap()
}

#[tokio::test]
async fn a_seed_is_202_with_the_operation_id_and_a_location() {
    let h = full(true, Ok(())).await;
    let (status, headers, body) = send(&h, Method::POST, APPS, &seed_json("acme")).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["status"], "accepted");
    let id = operation_id(&body);
    let mut members: Vec<_> = body.as_object().unwrap().keys().cloned().collect();
    members.sort();
    assert_eq!(members, ["operation_id", "status"]);
    assert_eq!(
        headers[header::LOCATION],
        format!("/application-manager/v1/operations/{id}").as_str()
    );

    let op = final_operation(&h, id).await;
    assert_eq!(op["category"], "succeeded");
    assert_eq!(op["application_id"], "acme");
    assert_eq!(op["action"], "seed");
    assert_eq!(op["operation_id"], id.to_string());
    assert_eq!(op["report"]["sections"][0]["key"], "noop");
    assert_eq!(op["report"]["sections"][0]["outcome"], "succeeded");
}

#[tokio::test]
async fn an_uninstall_by_path_decodes_one_percent_encoded_segment() {
    let h = full(true, Ok(())).await;
    let (status, headers, body) =
        send(&h, Method::DELETE, &format!("{APPS}/acme%2Fapp%201"), b"").await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["status"], "accepted");
    let id = operation_id(&body);
    assert_eq!(
        headers[header::LOCATION],
        format!("{OPERATIONS}/{id}").as_str()
    );
    let op = final_operation(&h, id).await;
    assert_eq!(op["application_id"], "acme/app 1");
    assert_eq!(op["action"], "uninstall");
    assert_eq!(op["category"], "succeeded");
}

#[tokio::test]
async fn an_uninstall_by_body_is_202() {
    let h = full(true, Ok(())).await;
    let (status, headers, body) =
        send(&h, Method::POST, UNINSTALL, br#"{"application_id":"acme"}"#).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let id = operation_id(&body);
    assert_eq!(
        headers[header::LOCATION],
        format!("{OPERATIONS}/{id}").as_str()
    );
    let op = final_operation(&h, id).await;
    assert_eq!(op["application_id"], "acme");
    assert_eq!(op["action"], "uninstall");
}

#[tokio::test]
async fn the_gate_refuses_with_503_and_records_a_refused_event() {
    // not ready, stopping, at capacity
    for case in ["not ready", "stopping", "at capacity"] {
        for (name, method, uri, body) in writes("acme") {
            let h = full(case != "not ready", Ok(())).await;
            let _held;
            match case {
                "stopping" => h.gate.stop(),
                "at capacity" => {
                    _held = (h.gate.admit().unwrap(), h.gate.admit().unwrap());
                }
                _ => {}
            }
            let (status, headers, problem) = send(&h, method, &uri, &body).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{case} {name}");
            assert_problem(&problem, "service_unavailable");
            assert_retry_after(&headers);
            assert!(
                problem["detail"].as_str().unwrap().starts_with("request "),
                "{problem}"
            );
            assert_eq!(h.plugin.calls(), 0, "{case} {name}");
            assert_eq!(h.access.calls(), 0, "{case} {name}: the gate comes first");
            let event = only_refused_event(&h);
            assert_eq!(refused_category(&event), "temporarily_unavailable");
        }
    }
}

#[tokio::test]
async fn not_permitted_is_403_naming_the_action() {
    for (name, method, uri, body) in writes("acme") {
        let h = full(true, Err(AccessError::NotPermitted)).await;
        let (status, _, problem) = send(&h, method, &uri, &body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{name}");
        assert_problem(&problem, "permission_denied");
        let action = if name == "seed" { "seed" } else { "uninstall" };
        let reason = problem["context"]["reason"].as_str().unwrap();
        assert!(reason.contains(action), "{name}: {reason}");
        assert!(reason.contains("request "), "{reason}");
        assert_eq!(h.plugin.calls(), 0);
        assert_eq!(refused_category(&only_refused_event(&h)), "not_permitted");
    }
}

#[tokio::test]
async fn access_control_that_cannot_answer_is_503() {
    for (name, method, uri, body) in writes("acme") {
        let h = full(true, Err(AccessError::Unavailable)).await;
        let (status, headers, problem) = send(&h, method, &uri, &body).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{name}");
        assert_problem(&problem, "service_unavailable");
        assert_retry_after(&headers);
        assert_eq!(h.plugin.calls(), 0);
    }
}

#[tokio::test]
async fn storage_that_cannot_answer_is_503() {
    for (name, method, uri, body) in writes("acme") {
        let h = full(true, Ok(())).await;
        h.repo.inject("open_operation", Fault::Unavailable);
        let (status, headers, problem) = send(&h, method, &uri, &body).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{name}");
        assert_problem(&problem, "service_unavailable");
        assert_retry_after(&headers);
        assert_eq!(h.plugin.calls(), 0);
    }
}

#[tokio::test]
async fn a_conflict_is_409_aborted_naming_the_open_operation() {
    let h = full(true, Ok(())).await;
    h.plugin.set_apply(ApplyAnswer::Hang);
    let (status, _, first) = send(&h, Method::POST, APPS, &seed_json("acme")).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{first}");
    let busy = operation_id(&first);
    for _ in 0..1000 {
        if !h.plugin.apply_args.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    for (name, method, uri, body) in writes("acme") {
        let (status, _, problem) = send(&h, method, &uri, &body).await;
        assert_eq!(status, StatusCode::CONFLICT, "{name}: {problem}");
        assert_problem(&problem, "aborted");
        assert_eq!(problem["context"]["reason"], "application_busy");
        let detail = problem["detail"].as_str().unwrap();
        assert!(detail.contains(&busy.to_string()), "{detail}");
        assert!(detail.contains("request "), "{detail}");
    }
    assert_eq!(
        h.plugin.apply_args.lock().unwrap().len(),
        1,
        "no second apply"
    );
}

#[tokio::test]
async fn an_invalid_envelope_is_400_listing_field_violations() {
    let h = full(true, Ok(())).await;
    let body = br#"{"application":{"id":""},"sections":{}}"#;
    let (status, _, problem) = send(&h, Method::POST, APPS, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{problem}");
    assert_problem(&problem, "invalid_argument");
    let violations = problem["context"]["field_violations"].as_array().unwrap();
    assert!(!violations.is_empty(), "{problem}");
    for v in violations {
        for member in ["field", "description", "reason"] {
            assert!(v[member].is_string(), "{member}: {v}");
        }
    }
    assert!(
        problem["detail"].as_str().unwrap().starts_with("request "),
        "{problem}"
    );
    assert_eq!(h.plugin.calls(), 0);
    assert_eq!(
        refused_category(&only_refused_event(&h)),
        "invalid_manifest"
    );
}

#[tokio::test]
async fn a_body_that_is_not_json_is_400_and_records_a_refused_event() {
    for (uri, label) in [(APPS, "seed"), (UNINSTALL, "uninstall")] {
        let h = full(true, Ok(())).await;
        let (status, _, problem) = send(&h, Method::POST, uri, b"{not json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {problem}");
        assert_problem(&problem, "invalid_argument");
        let violations = problem["context"]["field_violations"].as_array().unwrap();
        assert_eq!(violations.len(), 1, "{problem}");
        assert_eq!(violations[0]["field"], "");
        assert_eq!(violations[0]["reason"], "type");
        let event = only_refused_event(&h);
        assert_eq!(refused_category(&event), "invalid_manifest");
        assert_eq!(event.content["result"]["rules"], json!(["type"]));
    }
}

#[tokio::test]
async fn an_uninstall_id_outside_the_limits_is_400() {
    let h = full(true, Ok(())).await;
    let too_long = "x".repeat(300);
    let (status, _, problem) = send(&h, Method::DELETE, &format!("{APPS}/{too_long}"), b"").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{problem}");
    assert_problem(&problem, "invalid_argument");
    let (status, _, problem) = send(&h, Method::DELETE, &format!("{APPS}/a%07b"), b"").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{problem}");
    let (status, _, problem) = send(&h, Method::POST, UNINSTALL, br#"{"application_id":""}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{problem}");
    assert_eq!(h.plugin.calls(), 0);
}

#[tokio::test]
async fn a_gear_defect_is_500_internal_with_a_refused_event() {
    let h = full(true, Ok(())).await;
    h.repo.inject("open_operation", Fault::Panic);
    let (status, _, problem) = send(&h, Method::POST, APPS, &seed_json("acme")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{problem}");
    assert_problem(&problem, "internal");
    assert_eq!(
        refused_category(&only_refused_event(&h)),
        "internal_failure"
    );
    assert_eq!(h.plugin.calls(), 0);
}

#[tokio::test]
async fn an_open_operation_reads_without_a_report_and_a_final_one_with_it() {
    let h = full(true, Ok(())).await;
    h.plugin.set_apply(ApplyAnswer::Hang);
    let (_, _, accepted) = send(&h, Method::POST, APPS, &seed_json("acme")).await;
    let id = operation_id(&accepted);
    for _ in 0..1000 {
        if !h.plugin.apply_args.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (status, _, open) = send(&h, Method::GET, &format!("{OPERATIONS}/{id}"), b"").await;
    assert_eq!(status, StatusCode::OK, "{open}");
    assert_eq!(open["status"], "running");
    assert_eq!(open["operation_id"], id.to_string());
    assert_eq!(open["application_id"], "acme");
    assert_eq!(open["action"], "seed");
    assert!(open["accepted_at"].is_string() && open["run_deadline"].is_string());
    assert!(open.get("report").is_none(), "no report while open: {open}");
    assert!(open.get("category").is_none_or(Value::is_null), "{open}");
    assert_eq!(
        h.access.actions.lock().unwrap().last(),
        Some(&crate::domain::actions::Action::ReadOperation)
    );

    h.plugin.set_apply(ApplyAnswer::Ok);
    let (_, _, second) = send(&h, Method::POST, APPS, &seed_json("other")).await;
    let done = final_operation(&h, operation_id(&second)).await;
    assert_eq!(done["status"], "completed");
    assert!(done["report"].is_object(), "{done}");
    assert_eq!(done["report"]["category"], "succeeded");
}

#[tokio::test]
async fn a_malformed_or_unknown_operation_is_404() {
    let h = full(true, Ok(())).await;
    for id in ["not-a-uuid".to_owned(), Uuid::now_v7().to_string()] {
        let (status, _, problem) = send(&h, Method::GET, &format!("{OPERATIONS}/{id}"), b"").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{id}");
        assert_problem(&problem, "not_found");
        assert!(
            problem["detail"].as_str().unwrap().starts_with("request "),
            "{problem}"
        );
    }
}

#[tokio::test]
async fn an_operation_read_follows_access_control_and_storage() {
    let h = full(true, Err(AccessError::NotPermitted)).await;
    let id = Uuid::now_v7();
    let (status, _, problem) = send(&h, Method::GET, &format!("{OPERATIONS}/{id}"), b"").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_problem(&problem, "permission_denied");
    assert!(
        problem["context"]["reason"]
            .as_str()
            .unwrap()
            .contains("read_operation")
    );
    assert_eq!(h.repo.open_operation_ids.lock().unwrap().len(), 0);

    let h = full(true, Err(AccessError::Unavailable)).await;
    let (status, headers, problem) =
        send(&h, Method::GET, &format!("{OPERATIONS}/{id}"), b"").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_problem(&problem, "service_unavailable");
    assert_retry_after(&headers);

    let h = full(true, Ok(())).await;
    h.repo.inject("read_operation", Fault::Unavailable);
    let (status, headers, _) = send(&h, Method::GET, &format!("{OPERATIONS}/{id}"), b"").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_retry_after(&headers);
    let _ = &h.store;
}

#[test]
fn the_operation_routes_are_documented_and_throttled() {
    let openapi = OpenApiRegistryImpl::new();
    let read = Arc::new(read_service(
        FakeAccess::new(Ok(())),
        Arc::new(StartupState::default()),
    ));
    let _router = register_routes(
        Router::new(),
        &openapi,
        read,
        idle_orchestrator(),
        idle_gate(),
    );
    let doc =
        serde_json::to_value(openapi.build_openapi(&OpenApiInfo::default()).unwrap()).unwrap();
    let paths = &doc["paths"];
    let routes = [
        (APPS, "post", "application_manager.seed", true),
        (
            "/application-manager/v1/applications/{application_id}",
            "delete",
            "application_manager.uninstall_by_id",
            true,
        ),
        (UNINSTALL, "post", "application_manager.uninstall", true),
        (
            "/application-manager/v1/operations/{operation_id}",
            "get",
            "application_manager.get_operation",
            false,
        ),
    ];
    for (path, method, operation_id, accepts) in routes {
        let op = &paths[path][method];
        assert_eq!(op["operationId"], operation_id, "{path}");
        assert_eq!(
            op["x-throttling-rate-limit-zone"], RATE_LIMIT_ZONE,
            "{path}"
        );
        assert_eq!(
            op["x-throttling-in-flight-limit-zone"], IN_FLIGHT_LIMIT_ZONE,
            "{path}"
        );
        assert!(op["responses"]["429"].is_object(), "{path}: {op}");
        assert!(op["responses"]["503"].is_object(), "{path}: {op}");
        if accepts {
            for code in ["202", "400", "403", "409"] {
                assert!(op["responses"][code].is_object(), "{path} {code}: {op}");
            }
        } else {
            for code in ["200", "403", "404"] {
                assert!(op["responses"][code].is_object(), "{path} {code}: {op}");
            }
        }
    }
}
