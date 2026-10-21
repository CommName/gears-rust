use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::oneshot;

use super::*;
use crate::constants::LOG_TEXT_MAX_BYTES;
use crate::infra::call_guard::CallGuard;
use crate::test_support::{FakePlugin, KeyAnswer, SchemaAnswer, ctx, section};

const LIMIT: Duration = Duration::from_secs(10);

fn caller() -> CallerIdentity {
    CallerIdentity::from_security_context(&ctx())
}

fn answering(answer: SchemaAnswer) -> Arc<FakePlugin> {
    FakePlugin::serving(KeyAnswer::Key("unused"), answer)
}

fn router(guard: &Arc<CallGuard>) -> SectionPluginRouter {
    SectionPluginRouter::new(Arc::clone(guard) as Arc<dyn PluginCalls>)
}

/// The answers alone, after checking each is paired with its plugin.
fn answers(set: &[SectionPlugin], fetched: Vec<(&SectionPlugin, SchemaFetch)>) -> Vec<SchemaFetch> {
    assert_eq!(fetched.len(), set.len());
    fetched
        .into_iter()
        .zip(set)
        .map(|((plugin, fetch), expected)| {
            assert_eq!(plugin.key, expected.key, "paired with its plugin");
            fetch
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn classifies_every_answer_in_plugin_order() {
    let plugins = [
        answering(SchemaAnswer::serve(json!({ "type": "object" }), "1")),
        answering(SchemaAnswer::NotPermitted("no".to_owned())),
        answering(SchemaAnswer::Rejected),
        answering(SchemaAnswer::Internal),
        answering(SchemaAnswer::Panic),
        answering(SchemaAnswer::Hang),
    ];
    let set: Vec<_> = plugins
        .iter()
        .enumerate()
        .map(|(i, p)| section(&format!("k{i}"), i16::try_from(i).unwrap(), p))
        .collect();
    let guard = Arc::new(CallGuard::new(LIMIT, 1));
    let started = tokio::time::Instant::now();
    let fetched = answers(&set, router(&guard).fetch_schemas(&set, &caller()).await);
    assert_eq!(
        fetched,
        [
            SchemaFetch::Served(SectionSchema::new(json!({ "type": "object" }), "1")),
            SchemaFetch::NotPermitted {
                detail: "no".to_owned()
            },
            SchemaFetch::Unavailable(UnavailableReason::Unreachable),
            SchemaFetch::Unavailable(UnavailableReason::Unreachable),
            SchemaFetch::Unavailable(UnavailableReason::Unreachable),
            SchemaFetch::Unavailable(UnavailableReason::TimedOut),
        ]
    );
    assert_eq!(started.elapsed(), LIMIT);
    for p in &plugins {
        assert_eq!(p.calls(), 1, "each plugin called exactly once");
    }
}

#[tokio::test(start_paused = true)]
async fn calls_run_concurrently() {
    let a = answering(SchemaAnswer::Hang);
    let b = answering(SchemaAnswer::Hang);
    let set = [section("a", 1, &a), section("b", 2, &b)];
    let guard = Arc::new(CallGuard::new(LIMIT, 1));
    let started = tokio::time::Instant::now();
    let fetched = answers(&set, router(&guard).fetch_schemas(&set, &caller()).await);
    assert_eq!(
        fetched,
        [
            SchemaFetch::Unavailable(UnavailableReason::TimedOut),
            SchemaFetch::Unavailable(UnavailableReason::TimedOut),
        ]
    );
    assert_eq!(started.elapsed(), LIMIT, "one time limit, not two");
}

#[tokio::test(start_paused = true)]
async fn a_full_cap_is_not_sent() {
    let plugin = answering(SchemaAnswer::serve(json!({}), "1"));
    let set = [section("a", 1, &plugin)];
    let guard = Arc::new(CallGuard::new(LIMIT, 1));
    let (release, wait) = oneshot::channel::<()>();
    let held = guard.call("id-a", async move { wait.await.unwrap() });
    let refused = async {
        let fetched = answers(&set, router(&guard).fetch_schemas(&set, &caller()).await);
        release.send(()).unwrap();
        fetched
    };
    let (held, fetched) = tokio::join!(held, refused);
    assert_eq!(held, Ok(()));
    assert_eq!(
        fetched,
        [SchemaFetch::Unavailable(UnavailableReason::NotSent)]
    );
    assert_eq!(plugin.calls(), 0, "not called");
}

#[test]
fn reasons_use_the_design_names() {
    let names: Vec<_> = [
        UnavailableReason::Unreachable,
        UnavailableReason::TimedOut,
        UnavailableReason::Unusable,
        UnavailableReason::NotSent,
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    assert_eq!(names, ["unreachable", "timed_out", "unusable", "not_sent"]);
}

/// Plugin calls whose failure text is longer than [`LOG_TEXT_MAX_BYTES`].
struct LongFailure;

#[async_trait::async_trait]
impl PluginCalls for LongFailure {
    async fn schema(
        &self,
        plugin: &SectionPlugin,
        _caller: &CallerIdentity,
    ) -> Result<Result<SectionSchema, SectionPluginError>, CallFailure> {
        if plugin.key == "panics" {
            return Err(CallFailure::Panicked("P".repeat(LOG_TEXT_MAX_BYTES + 5)));
        }
        Ok(Err(SectionPluginError::Internal {
            message: "M".repeat(LOG_TEXT_MAX_BYTES + 5),
        }))
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn plugin_text_is_logged_cut_at_the_limit() {
    let plugin = answering(SchemaAnswer::Internal);
    let set = [section("fails", 1, &plugin), section("panics", 2, &plugin)];
    let router = SectionPluginRouter::new(Arc::new(LongFailure));
    router.fetch_schemas(&set, &caller()).await;
    let kept = "M".repeat(LOG_TEXT_MAX_BYTES - "internal plugin error: ".len());
    assert!(logs_contain(&format!(
        "error=\"internal plugin error: {kept}\""
    )));
    assert!(!logs_contain(&format!("{kept}M")), "cut at the limit");
    let panic_text = "P".repeat(LOG_TEXT_MAX_BYTES);
    assert!(logs_contain(&format!("panic=\"{panic_text}\"")));
    assert!(!logs_contain(&format!("{panic_text}P")), "cut at the limit");
    assert!(logs_contain("key=\"fails\" call=\"schema\""));
    assert!(logs_contain("key=\"panics\" call=\"schema\""));
}

#[tokio::test(start_paused = true)]
#[tracing_test::traced_test]
async fn refusals_and_unanswered_calls_are_logged_with_their_key() {
    let plugins = [
        answering(SchemaAnswer::NotPermitted(
            "needs the seeder role".to_owned(),
        )),
        answering(SchemaAnswer::Hang),
    ];
    let set = [
        section("refuses", 1, &plugins[0]),
        section("hangs", 2, &plugins[1]),
    ];
    let guard = Arc::new(CallGuard::new(LIMIT, 1));
    router(&guard).fetch_schemas(&set, &caller()).await;
    assert!(logs_contain(
        "key=\"refuses\" call=\"schema\" detail=\"needs the seeder role\""
    ));
    assert!(logs_contain(
        "key=\"hangs\" call=\"schema\" reason=\"timed_out\""
    ));
}
