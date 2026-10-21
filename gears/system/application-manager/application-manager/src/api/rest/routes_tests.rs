#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use serde_json::{Value, json};
use toolkit::api::OpenApiRegistryImpl;
use tower::ServiceExt;

use super::register_routes;
use crate::constants::REFUSAL_RETRY_AFTER_SECONDS;
use crate::domain::plugin_set::{PluginSet, SectionPlugin, StartupState};
use crate::domain::ports::AccessError;
use crate::domain::read_service::ReadService;
use crate::test_support::{FakeAccess, FakePlugin, KeyAnswer, ctx};

const PATH: &str = "/application-manager/v1/section-keys";
const PROBLEM_MEMBERS: [&str; 7] = [
    "type", "title", "status", "detail", "instance", "trace_id", "context",
];

struct Harness {
    router: Router,
    access: Arc<FakeAccess>,
    plugin: Arc<FakePlugin>,
}

fn harness(ready: bool, answer: Result<(), AccessError>) -> Harness {
    let plugin = FakePlugin::new(KeyAnswer::Key("unused"));
    let state = Arc::new(StartupState::default());
    if ready {
        let make = |key: &str, priority| SectionPlugin {
            key: key.to_owned(),
            priority,
            instance_id: format!("id-{key}"),
            client: plugin.clone(),
        };
        state
            .publish(PluginSet::new(vec![make("late", 9), make("noop", 1)]))
            .unwrap();
    }
    let access = FakeAccess::new(answer);
    let read = Arc::new(ReadService::new(access.clone(), state));
    let router = register_routes(Router::new(), &OpenApiRegistryImpl::new(), read);
    Harness {
        router,
        access,
        plugin,
    }
}

async fn get(h: &Harness) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut req = Request::builder().uri(PATH).body(Body::empty()).unwrap();
    req.extensions_mut().insert(ctx());
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
    let h = harness(true, Ok(()));
    let (status, _, body) = get(&h).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({ "items": ["noop", "late"] }));
    assert_eq!(h.access.calls(), 1);
    assert_eq!(h.plugin.calls(), 0);
}

#[tokio::test]
async fn not_permitted_is_403_with_no_plugin_call() {
    let h = harness(true, Err(AccessError::NotPermitted));
    let (status, _, body) = get(&h).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
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

#[tokio::test]
async fn unavailable_refusals_are_503_with_retry_after() {
    for (ready, answer, access_calls) in
        [(false, Ok(()), 0), (true, Err(AccessError::Unavailable), 1)]
    {
        let h = harness(ready, answer);
        let (status, headers, body) = get(&h).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "ready={ready}");
        assert_problem(&body, "service_unavailable");
        assert_eq!(
            headers[header::RETRY_AFTER],
            REFUSAL_RETRY_AFTER_SECONDS.to_string().as_str(),
            "ready={ready}"
        );
        assert!(
            body["detail"].as_str().unwrap().starts_with("request "),
            "{body}"
        );
        assert_eq!(h.access.calls(), access_calls, "ready={ready}");
        assert_eq!(h.plugin.calls(), 0);
    }
}
