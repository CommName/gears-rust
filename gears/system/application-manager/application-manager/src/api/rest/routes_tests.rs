#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use toolkit::api::OpenApiRegistryImpl;
use toolkit_security::PlatformSecurityContext;
use tower::ServiceExt;

use super::register_routes;
use crate::constants::REFUSAL_RETRY_AFTER_SECONDS;
use crate::domain::actions::CallerAction;
use crate::domain::plugin_set::{PluginSet, StartupState};
use crate::domain::read_service::ReadService;
use crate::test_support::{FakePlugin, allowed_callers, platform_ctx, section_plugin};

const PATH: &str = "/application-manager/v1/section-keys";
const PROBLEM_MEMBERS: [&str; 7] = [
    "type", "title", "status", "detail", "instance", "trace_id", "context",
];

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
    }
    let read = Arc::new(ReadService::new(
        Arc::new(allowed_callers("app-seeder", actions)),
        state,
    ));
    let router = register_routes(Router::new(), &OpenApiRegistryImpl::new(), read);
    Harness { router, plugin }
}

async fn get(
    h: &Harness,
    ctx: PlatformSecurityContext,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut req = Request::builder().uri(PATH).body(Body::empty()).unwrap();
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
