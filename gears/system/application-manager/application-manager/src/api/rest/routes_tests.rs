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

use std::time::Duration;

use super::register_routes;
use crate::api::rest::gate::RequestGate;
use crate::constants::REFUSAL_RETRY_AFTER_SECONDS;
use crate::domain::actions::CallerAction;
use crate::domain::actions::TenantAction;
use crate::domain::orchestrator::{OrchestratorSettings, RequestOrchestrator};
use crate::domain::plugin_set::{PluginSet, StartupState};
use crate::domain::ports::{AccessControl, AccessError, OperationRepository, PluginCalls};
use crate::domain::read_service::ReadService;
use crate::domain::router::SectionPluginRouter;
use crate::domain::schema_validator::SchemaValidator;
use crate::infra::call_guard::CallGuard;
use crate::test_support::{
    FakeAccess, FakePlugin, Hook, HookedRepo, allowed_callers, platform_ctx, section_plugin,
};

const PATH: &str = "/application-manager/v1/section-keys";
const PROBLEM_MEMBERS: [&str; 7] = [
    "type", "title", "status", "detail", "instance", "trace_id", "context",
];

struct Harness {
    router: Router,
    plugin: Arc<FakePlugin>,
    repo: Arc<HookedRepo>,
    access: Arc<FakeAccess>,
}

/// The routes over their real components and a repository on `SQLite`.
fn routes(
    registry: &OpenApiRegistryImpl,
    state: Arc<StartupState>,
    actions: Vec<CallerAction>,
    repo: Arc<HookedRepo>,
    max_request_tasks: u32,
    access: Arc<dyn AccessControl>,
) -> Router {
    let allowed = Arc::new(allowed_callers("app-seeder", actions));
    let validator = Arc::new(SchemaValidator::new(255).unwrap());
    let repo = repo as Arc<dyn OperationRepository>;
    let guard: Arc<dyn PluginCalls> = Arc::new(CallGuard::new(Duration::from_secs(5), 4));
    let orchestrator = Arc::new(RequestOrchestrator::new(
        Arc::clone(&allowed),
        Arc::clone(&repo),
        Arc::new(SectionPluginRouter::new(guard)),
        Arc::clone(&validator),
        Arc::clone(&state),
        OrchestratorSettings {
            plugin_call_timeout: Duration::from_secs(5),
            storage_call_timeout: Duration::from_secs(1),
            run_window_margin: Duration::from_secs(30),
            max_envelope_text_length: 255,
        },
    ));
    let gate = Arc::new(RequestGate::new(Arc::clone(&state), max_request_tasks));
    let read = Arc::new(ReadService::new(allowed, state, validator, repo, access));
    register_routes(Router::new(), registry, read, orchestrator, gate)
}

/// Routes over a startup state, ready or not, where `app-seeder` holds
/// `actions`.
async fn harness_with(ready: bool, actions: Vec<CallerAction>, max_request_tasks: u32) -> Harness {
    harness_over(ready, actions, max_request_tasks, FakeAccess::permit()).await
}

/// [`harness_with`] whose PDP answers as `access` does.
async fn harness_over(
    ready: bool,
    actions: Vec<CallerAction>,
    max_request_tasks: u32,
    access: Arc<FakeAccess>,
) -> Harness {
    let plugin = FakePlugin::new();
    let state = Arc::new(StartupState::default());
    if ready {
        state
            .publish(PluginSet::new(vec![
                section_plugin("cf.test.late.section", 9, &plugin),
                section_plugin("cf.core.noop.section", 1, &plugin),
            ]))
            .unwrap();
    }
    let repo = HookedRepo::new().await;
    let router = routes(
        &OpenApiRegistryImpl::new(),
        state,
        actions,
        Arc::clone(&repo),
        max_request_tasks,
        Arc::clone(&access) as Arc<dyn AccessControl>,
    );
    Harness {
        router,
        plugin,
        repo,
        access,
    }
}

async fn harness(ready: bool, actions: Vec<CallerAction>) -> Harness {
    harness_with(ready, actions, 16).await
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
    let h = harness(true, vec![CallerAction::ReadSchema]).await;
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
        let h = harness(true, actions).await;
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
    let h = harness(false, vec![CallerAction::ReadSchema]).await;
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
    let h = harness(true, vec![CallerAction::ReadSchema]).await;
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
    let h = harness(true, vec![CallerAction::ReadSchema]).await;
    let path = "/application-manager/v1/section-keys/secret.key/schema";
    let (status, _, body) = get_at(&h, path, platform_ctx("app-seeder")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_problem(&body, "not_found");
    assert!(!body.to_string().contains("secret.key"), "{body}");
}

#[tokio::test]
async fn reads_the_combined_schema_with_every_section_type() {
    let h = harness(true, vec![CallerAction::ReadSchema]).await;
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
        let h = harness(true, vec![CallerAction::Seed]).await;
        let (status, _, body) = get_at(&h, path, platform_ctx("app-seeder")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
        assert_problem(&body, "permission_denied");

        let h = harness(false, vec![CallerAction::ReadSchema]).await;
        let (status, headers, _) = get_at(&h, path, platform_ctx("app-seeder")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        assert!(headers.contains_key(header::RETRY_AFTER), "{path}");
    }
}

#[tokio::test]
async fn the_bounded_routes_are_throttled_and_the_other_reads_are_not() {
    let registry = OpenApiRegistryImpl::new();
    drop(routes(
        &registry,
        Arc::new(StartupState::default()),
        vec![],
        HookedRepo::new().await,
        1,
        FakeAccess::permit(),
    ));
    let spec =
        serde_json::to_value(registry.build_openapi(&OpenApiInfo::default()).unwrap()).unwrap();
    let paths = &spec["paths"];
    let has_429 = |path: &str| paths[path]["get"]["responses"].get("429").is_some();
    assert!(has_429(MANIFEST_PATH), "{paths}");
    for path in [
        "/application-manager/v1/applications",
        "/application-manager/v1/applications:uninstall",
    ] {
        assert!(
            paths[path]["post"]["responses"].get("429").is_some(),
            "{path}"
        );
    }
    assert!(
        paths["/application-manager/v1/applications/{application_id}"]["delete"]["responses"]
            .get("429")
            .is_some()
    );
    assert!(has_429("/application-manager/v1/operations/{operation_id}"));
    assert!(has_429(HISTORY_PATH));
    assert!(has_429(SEED_PATH));
    assert!(!has_429(
        "/application-manager/v1/section-keys/{section_key}/schema"
    ));
    assert!(!has_429(PATH));
}

// ---- seed, uninstall, and the operation read ----

const SEED_PATH: &str = "/application-manager/v1/applications";

async fn send(
    h: &Harness,
    method: &str,
    path: &str,
    body: &[u8],
    ctx: PlatformSecurityContext,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_vec()))
        .unwrap();
    req.extensions_mut().insert(ctx);
    let resp = h.router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, headers, body)
}

fn seed_body() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "application": { "id": "acme.app", "version": "1.0.0" },
        "sections": { "cf.core.noop.section": {}, "cf.test.late.section": {} }
    }))
    .unwrap()
}

fn writer() -> Vec<CallerAction> {
    vec![
        CallerAction::Seed,
        CallerAction::Uninstall,
        CallerAction::ReadOperation,
    ]
}

/// The operation id of a 202 answer, after checking its body and `Location`.
fn accepted_id(status: StatusCode, headers: &axum::http::HeaderMap, body: &Value) -> uuid::Uuid {
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["status"], json!("accepted"));
    let id: uuid::Uuid = body["operation_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        headers[header::LOCATION],
        format!("/application-manager/v1/operations/{id}").as_str()
    );
    assert_eq!(body.as_object().unwrap().len(), 2, "{body}");
    id
}

#[tokio::test]
async fn a_seed_is_202_with_the_operation_id_and_a_location() {
    let h = harness(true, writer()).await;
    let (status, headers, body) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    let id = accepted_id(status, &headers, &body);
    let op = h.repo.completed(id).await;
    assert_eq!(
        op.category,
        Some(application_manager_sdk::RequestCategory::Succeeded)
    );
    assert_eq!(h.plugin.calls(), 2);
}

#[tokio::test]
async fn an_uninstall_by_path_decodes_one_percent_encoded_segment() {
    let h = harness(true, writer()).await;
    let path = "/application-manager/v1/applications/acme%2Fapp%20one";
    let (status, headers, body) = send(&h, "DELETE", path, b"", platform_ctx("app-seeder")).await;
    let id = accepted_id(status, &headers, &body);
    assert_eq!(h.repo.completed(id).await.application_id, "acme/app one");
}

#[tokio::test]
async fn an_uninstall_by_body_is_202() {
    let h = harness(true, writer()).await;
    let (status, headers, body) = send(
        &h,
        "POST",
        "/application-manager/v1/applications:uninstall",
        br#"{"application_id":"acme.app"}"#,
        platform_ctx("app-seeder"),
    )
    .await;
    accepted_id(status, &headers, &body);
}

#[tokio::test]
async fn the_gate_refuses_with_503_and_records_a_refused_event() {
    for (ready, max) in [(false, 16), (true, 0)] {
        let h = harness_with(ready, writer(), max).await;
        let (status, headers, body) = send(
            &h,
            "POST",
            SEED_PATH,
            &seed_body(),
            platform_ctx("app-seeder"),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "ready={ready}");
        assert_problem(&body, "service_unavailable");
        assert!(headers.contains_key(header::RETRY_AFTER));
        let events = h.repo.history_page(None, None, 10).await.unwrap();
        assert_eq!(events.len(), 1, "ready={ready}");
        assert_eq!(h.plugin.calls(), 0);
    }
}

#[tokio::test]
async fn a_caller_not_allowed_is_403_naming_the_action() {
    let h = harness(true, vec![CallerAction::ReadOperation]).await;
    let (status, _, body) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_problem(&body, "permission_denied");
    assert!(
        body["context"]["reason"].as_str().unwrap().contains("seed"),
        "{body}"
    );
}

#[tokio::test]
async fn storage_that_cannot_answer_is_503() {
    let h = harness(true, writer()).await;
    h.repo.hook("open_operation", Hook::Fail);
    let (status, headers, body) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_problem(&body, "service_unavailable");
    assert!(body["detail"].as_str().unwrap().starts_with("request "));
    assert!(headers.contains_key(header::RETRY_AFTER));
}

#[tokio::test]
async fn a_conflict_is_409_aborted_naming_the_open_operation() {
    let h = harness(true, writer()).await;
    // The first seed's plugins answer at once, so hold the operation open by
    // failing its final write.
    h.repo.hook("finish", Hook::Fail);
    let (status, headers, body) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    let busy = accepted_id(status, &headers, &body);
    let (status, _, body) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_problem(&body, "aborted");
    assert_eq!(body["context"]["reason"], json!("application_busy"));
    let detail = body["detail"].as_str().unwrap();
    assert!(detail.starts_with("request "), "{detail}");
    assert!(detail.contains(&busy.to_string()), "{detail}");
}

#[tokio::test]
async fn an_invalid_envelope_is_400_with_the_request_id_first() {
    let h = harness(true, writer()).await;
    let body = serde_json::to_vec(&json!({
        "application": { "id": "", "version": "1" },
        "sections": {}
    }))
    .unwrap();
    let (status, _, body) = send(&h, "POST", SEED_PATH, &body, platform_ctx("app-seeder")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_problem(&body, "invalid_argument");
    let violations = body["context"]["field_violations"].as_array().unwrap();
    assert_eq!(violations[0]["field"], json!("request_id"));
    assert_eq!(violations[0]["reason"], json!("request_id"));
    assert!(
        violations[0]["description"]
            .as_str()
            .unwrap()
            .parse::<uuid::Uuid>()
            .is_ok()
    );
    assert_eq!(violations[1]["field"], json!("/application/id"));
    assert_eq!(violations[1]["reason"], json!("minLength"));
}

#[tokio::test]
async fn a_body_that_is_not_json_is_400_and_not_recorded() {
    let h = harness(true, writer()).await;
    let (status, _, body) = send(&h, "POST", SEED_PATH, b"{nope", platform_ctx("app-seeder")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(h.repo.history_page(None, None, 10).await.unwrap(), []);
    assert_eq!(h.plugin.calls(), 0);
}

#[tokio::test]
async fn an_uninstall_body_of_another_shape_is_refused_and_not_recorded() {
    let h = harness(true, writer()).await;
    let unprocessable = StatusCode::UNPROCESSABLE_ENTITY;
    for (request, expected) in [
        (&b"not json"[..], StatusCode::BAD_REQUEST),
        (b"[]", unprocessable),
        (b"{}", unprocessable),
        (br#"{"application_id":7}"#, unprocessable),
        (br#"{"application_id":"a","x":1}"#, unprocessable),
    ] {
        let (status, _, body) = send(
            &h,
            "POST",
            "/application-manager/v1/applications:uninstall",
            request,
            platform_ctx("app-seeder"),
        )
        .await;
        assert_eq!(status, expected, "{body}");
    }
    assert_eq!(h.repo.history_page(None, None, 10).await.unwrap(), []);
    assert_eq!(h.plugin.calls(), 0);
}

#[tokio::test]
async fn an_uninstall_id_outside_the_limits_is_400() {
    let h = harness(true, writer()).await;
    for path in [
        format!("/application-manager/v1/applications/{}", "a".repeat(256)),
        "/application-manager/v1/applications/a%07b".to_owned(),
    ] {
        let (status, _, body) = send(&h, "DELETE", &path, b"", platform_ctx("app-seeder")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
    }
}

#[tokio::test]
async fn a_gear_defect_is_500_internal() {
    let h = harness(true, writer()).await;
    h.repo.hook("open_operation", Hook::Panic);
    let (status, _, body) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_problem(&body, "internal");
}

#[tokio::test]
async fn the_operation_read_returns_the_record_and_the_report_once_completed() {
    let h = harness(true, writer()).await;
    let (status, headers, body) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    let id = accepted_id(status, &headers, &body);
    h.repo.completed(id).await;
    let (status, _, body) = get_at(
        &h,
        &format!("/application-manager/v1/operations/{id}"),
        platform_ctx("app-seeder"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], json!("completed"));
    assert_eq!(body["category"], json!("succeeded"));
    assert_eq!(body["action"], json!("seed"));
    assert_eq!(body["report"]["category"], json!("succeeded"));
    assert_eq!(body["report"]["sections"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn an_open_operation_reads_without_a_report() {
    let h = harness(true, writer()).await;
    h.repo.hook("mark_running", Hook::Fail);
    let (status, headers, body) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    let id = accepted_id(status, &headers, &body);
    let (status, _, body) = get_at(
        &h,
        &format!("/application-manager/v1/operations/{id}"),
        platform_ctx("app-seeder"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], json!("accepted"));
    assert!(body.get("report").is_none_or(Value::is_null), "{body}");
}

#[tokio::test]
async fn a_malformed_or_unknown_operation_is_404_and_a_caller_not_allowed_is_403() {
    let h = harness(true, writer()).await;
    for id in ["not-a-uuid".to_owned(), uuid::Uuid::now_v7().to_string()] {
        let (status, _, body) = get_at(
            &h,
            &format!("/application-manager/v1/operations/{id}"),
            platform_ctx("app-seeder"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{id}: {body}");
        assert_problem(&body, "not_found");
    }
    let (status, _, _) = get_at(
        &h,
        "/application-manager/v1/operations/not-a-uuid",
        platform_ctx("someone-else"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "the caller check comes first"
    );
}

// ---- the tenant-plane reads ----

const HISTORY_PATH: &str = "/application-manager/v1/operation-history";

async fn get_tenant(
    h: &Harness,
    path_and_query: &str,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut req = Request::builder()
        .uri(path_and_query)
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(crate::test_support::tenant_ctx());
    let resp = h.router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    (status, headers, serde_json::from_slice(&bytes).unwrap())
}

async fn tenant_harness(answer: Result<(), AccessError>) -> Harness {
    harness_over(true, vec![], 16, FakeAccess::new(answer)).await
}

#[tokio::test]
async fn a_seed_shows_in_the_history_and_the_application_list() {
    let h = harness(true, writer()).await;
    let (status, _, body) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let operation_id = body["operation_id"].as_str().unwrap().to_owned();
    let operation_id = uuid::Uuid::parse_str(&operation_id).unwrap();
    h.repo.completed(operation_id).await;

    let filter = format!("$filter=operation_id%20eq%20{operation_id}");
    let (status, _, body) = get_tenant(&h, &format!("{HISTORY_PATH}?{filter}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let kinds: Vec<&str> = body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["accepted", "finished"], "{body}");
    assert_eq!(body["items"][0]["action"], "seed");
    assert_eq!(body["items"][0]["correlation_id"], operation_id.to_string());
    assert_eq!(body["page_info"]["limit"], 100);
    assert!(body["page_info"]["next_cursor"].is_null());

    let (status, _, body) = get_tenant(&h, SEED_PATH).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["items"][0]["application_id"], "acme.app", "{body}");
    assert_eq!(body["items"][0]["category"], "succeeded");
    assert_eq!(body["items"][0]["operation_id"], operation_id.to_string());
    assert_eq!(
        *h.access.actions.lock().unwrap(),
        vec![TenantAction::ReadHistory, TenantAction::ReadApplications]
    );
}

#[tokio::test]
async fn an_empty_store_is_an_empty_page_with_the_limit_capped() {
    let h = tenant_harness(Ok(())).await;
    for path in [HISTORY_PATH, SEED_PATH] {
        let (status, _, body) = get_tenant(&h, &format!("{path}?limit=1000")).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert_eq!(body["items"], json!([]), "{path}");
        assert_eq!(body["page_info"]["limit"], 100, "{path}");
    }
}

#[tokio::test]
async fn query_options_the_reads_do_not_serve_are_400() {
    let h = tenant_harness(Ok(())).await;
    let cases = [
        (
            format!("{HISTORY_PATH}?$orderby=event_id%20desc"),
            "$orderby",
            "INVALID_ORDERBY_FIELD",
        ),
        (
            format!("{HISTORY_PATH}?$select=kind"),
            "$select",
            "INVALID_SELECT",
        ),
        (
            format!("{HISTORY_PATH}?$filter=kind%20eq%20'refused'"),
            "$filter",
            "INVALID_FILTER",
        ),
        (
            format!("{SEED_PATH}?$filter=application_id%20ne%20'a'"),
            "$filter",
            "INVALID_FILTER",
        ),
    ];
    for (path, field, reason) in cases {
        let (status, _, body) = get_tenant(&h, &path).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_problem(&body, "invalid_argument");
        let violations = body["context"]["field_violations"].as_array().unwrap();
        assert!(
            violations
                .iter()
                .any(|v| v["field"] == field && v["reason"] == reason),
            "{path}: {body}"
        );
    }
    assert_eq!(h.repo.calls("history_page"), 0);
    assert_eq!(h.repo.calls("applications_page"), 0);
}

#[tokio::test]
async fn a_pdp_refusal_is_403_and_a_pdp_failure_is_503() {
    for (answer, status, kind) in [
        (
            Err(AccessError::NotPermitted),
            StatusCode::FORBIDDEN,
            "permission_denied",
        ),
        (
            Err(AccessError::Unavailable),
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
        ),
    ] {
        let h = tenant_harness(answer).await;
        for path in [HISTORY_PATH, SEED_PATH] {
            let (got, headers, body) = get_tenant(&h, path).await;
            assert_eq!(got, status, "{path}: {body}");
            assert_problem(&body, kind);
            if status == StatusCode::SERVICE_UNAVAILABLE {
                assert!(headers.contains_key(header::RETRY_AFTER), "{path}");
            }
        }
        assert_eq!(h.repo.calls("history_page"), 0);
        assert_eq!(h.repo.calls("applications_page"), 0);
        assert_eq!(h.plugin.calls(), 0);
    }
}

#[tokio::test]
async fn storage_that_cannot_answer_a_tenant_read_is_503() {
    let h = tenant_harness(Ok(())).await;
    h.repo.hook("history_page", Hook::Fail);
    h.repo.hook("applications_page", Hook::Fail);
    for path in [HISTORY_PATH, SEED_PATH] {
        let (status, headers, body) = get_tenant(&h, path).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}: {body}");
        assert!(headers.contains_key(header::RETRY_AFTER), "{path}");
    }
}

#[tokio::test]
async fn the_platform_routes_make_no_pdp_call() {
    let h = harness(true, writer()).await;
    let (status, _, _) = send(
        &h,
        "POST",
        SEED_PATH,
        &seed_body(),
        platform_ctx("app-seeder"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _, _) = get(&h, platform_ctx("someone-else")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(h.access.calls(), 0);
}

#[tokio::test]
async fn the_tenant_reads_document_their_filter_and_pages() {
    let registry = OpenApiRegistryImpl::new();
    drop(routes(
        &registry,
        Arc::new(StartupState::default()),
        vec![],
        HookedRepo::new().await,
        1,
        FakeAccess::permit(),
    ));
    let spec =
        serde_json::to_value(registry.build_openapi(&OpenApiInfo::default()).unwrap()).unwrap();
    for (path, field) in [
        (HISTORY_PATH, "operation_id"),
        (SEED_PATH, "application_id"),
    ] {
        let get = &spec["paths"][path]["get"];
        assert!(get.is_object(), "{path}");
        let text = get.to_string();
        assert!(text.contains(field), "{path}: {text}");
        assert!(get["responses"].get("200").is_some(), "{path}");
        assert!(get["responses"].get("400").is_some(), "{path}");
        assert!(get["responses"].get("403").is_some(), "{path}");
    }
}
