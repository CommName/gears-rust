#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::SectionPluginError;
use serde_json::{Map, json};

use super::*;
use crate::domain::ports::CallFailure;
use crate::infra::call_guard::CallGuard;
use crate::test_support::{Answer, FakePlugin, platform_ctx, section_plugin};

const LIMIT: Duration = Duration::from_secs(10);
const OPERATION: Uuid = Uuid::from_u128(42);

fn caller() -> CallerIdentity {
    CallerIdentity::from_platform_context(&platform_ctx("app-seeder"))
}

fn envelope() -> Envelope {
    Envelope {
        id: "app".to_owned(),
        version: "1".to_owned(),
    }
}

fn router_over(max_in_flight: u32) -> (SectionPluginRouter, Arc<CallGuard>) {
    let guard = Arc::new(CallGuard::new(LIMIT, max_in_flight));
    (
        SectionPluginRouter::new(Arc::clone(&guard) as Arc<dyn PluginCalls>),
        guard,
    )
}

/// A set of one plugin per answer, keyed `k0`, `k1`, … in that priority
/// order, listed out of order.
fn set_of(plugins: &[Arc<FakePlugin>]) -> PluginSet {
    let mut sections: Vec<_> = plugins
        .iter()
        .enumerate()
        .map(|(i, p)| section_plugin(&format!("k{i}"), i16::try_from(i).unwrap(), p))
        .collect();
    sections.reverse();
    PluginSet::new(sections)
}

fn sections_for(set: &PluginSet) -> Map<String, serde_json::Value> {
    set.plugins()
        .iter()
        .map(|p| (p.key.clone(), json!({ "for": p.key })))
        .collect()
}

/// Each routed answer, as a token.
fn tokens(routed: &[Routed]) -> Vec<(String, &'static str)> {
    routed
        .iter()
        .map(|(key, answer)| {
            let token = match answer {
                Ok(Ok(())) => "ok",
                Ok(Err(SectionPluginError::Rejected { .. })) => "rejected",
                Ok(Err(SectionPluginError::NotPermitted { .. })) => "not_permitted",
                Ok(Err(_)) => "internal",
                Err(CallFailure::TimedOut) => "timed_out",
                Err(CallFailure::NotSent) => "not_sent",
                Err(CallFailure::Panicked(_)) => "panicked",
            };
            (key.clone(), token)
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn apply_runs_in_priority_order_and_continues_after_every_failure() {
    let plugins = [
        Answer::Ok,
        Answer::Rejected,
        Answer::NotPermitted,
        Answer::Internal,
        Answer::Panic,
        Answer::Hang,
        Answer::Ok,
    ]
    .map(FakePlugin::answering);
    let set = set_of(&plugins);
    let (sut, _) = router_over(1);
    let started = tokio::time::Instant::now();
    let answers = sut
        .apply_all(
            &set,
            &caller(),
            OPERATION,
            &envelope(),
            &sections_for(&set),
            |_| true,
        )
        .await;
    assert_eq!(
        tokens(&answers),
        [
            ("k0".to_owned(), "ok"),
            ("k1".to_owned(), "rejected"),
            ("k2".to_owned(), "not_permitted"),
            ("k3".to_owned(), "internal"),
            ("k4".to_owned(), "panicked"),
            ("k5".to_owned(), "timed_out"),
            ("k6".to_owned(), "ok"),
        ]
    );
    assert_eq!(started.elapsed(), LIMIT, "only the hanging call waited");
    for plugin in &plugins {
        assert_eq!(plugin.calls(), 1, "called once, never retried");
        let seen = plugin.seen.lock().unwrap().clone();
        assert_eq!(seen, [("apply", OPERATION, caller())]);
    }
}

#[tokio::test]
async fn remove_runs_in_priority_order_with_the_caller_and_operation() {
    let plugins = [Answer::Internal, Answer::Ok].map(FakePlugin::answering);
    let set = set_of(&plugins);
    let (sut, _) = router_over(1);
    let answers = sut
        .remove_all(&set, &caller(), OPERATION, "app", |_| true)
        .await;
    assert_eq!(
        tokens(&answers),
        [("k0".to_owned(), "internal"), ("k1".to_owned(), "ok")]
    );
    for plugin in &plugins {
        assert_eq!(
            plugin.seen.lock().unwrap().clone(),
            [("remove", OPERATION, caller())]
        );
    }
}

#[tokio::test]
async fn may_call_stops_the_dispatch_before_the_refused_plugin() {
    let plugins = [Answer::Ok, Answer::Ok, Answer::Ok].map(FakePlugin::answering);
    let set = set_of(&plugins);
    let (sut, _) = router_over(1);
    let mut asked = Vec::new();
    let answers = sut
        .apply_all(
            &set,
            &caller(),
            OPERATION,
            &envelope(),
            &sections_for(&set),
            |p| {
                asked.push(p.key.clone());
                p.key != "k1"
            },
        )
        .await;
    assert_eq!(tokens(&answers), [("k0".to_owned(), "ok")]);
    assert_eq!(
        asked,
        ["k0", "k1"],
        "asked before each call, stopped at the first no"
    );
    assert_eq!(
        plugins.iter().map(|p| p.calls()).collect::<Vec<_>>(),
        [1, 0, 0]
    );
}

#[tokio::test(start_paused = true)]
async fn a_full_in_flight_cap_is_not_sent_and_the_dispatch_continues() {
    let plugins = [Answer::Ok, Answer::Ok].map(FakePlugin::answering);
    let set = set_of(&plugins);
    let (sut, guard) = router_over(1);
    let busy = &set.plugins()[0];
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let held = guard.call(&busy.instance_id, async move { wait.await.unwrap() });
    let dispatch = async {
        let answers = sut
            .remove_all(&set, &caller(), OPERATION, "app", |_| true)
            .await;
        release.send(()).unwrap();
        answers
    };
    let (held, answers) = tokio::join!(held, dispatch);
    assert_eq!(held, Ok(()));
    assert_eq!(
        tokens(&answers),
        [("k0".to_owned(), "not_sent"), ("k1".to_owned(), "ok")]
    );
    assert_eq!(plugins[0].calls(), 0);
}

#[tokio::test]
async fn an_empty_set_calls_nothing() {
    let (sut, _) = router_over(1);
    let set = PluginSet::new(vec![]);
    assert!(
        sut.apply_all(&set, &caller(), OPERATION, &envelope(), &Map::new(), |_| {
            true
        })
        .await
        .is_empty()
    );
}
