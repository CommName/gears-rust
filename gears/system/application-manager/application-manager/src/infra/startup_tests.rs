#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use types_registry_sdk::testing::{MockTypesRegistryClient, internal, make_test_instance};

use super::*;
use crate::test_support::{KeyAnswer, PluginSpec, hub_with, hub_with_registry, instance_id};

fn guard() -> CallGuard {
    CallGuard::new(Duration::from_secs(10), 4)
}

async fn failure_of(plugins: &[PluginSpec]) -> StartupFailure {
    discover(&hub_with(plugins), &guard())
        .await
        .expect_err("startup must fail")
}

async fn reason_of(plugins: &[PluginSpec]) -> StartupReason {
    failure_of(plugins).await.reason
}

/// The failure names every instance it is about.
fn assert_detail_names(failure: &StartupFailure, segments: &[&str]) {
    for segment in segments {
        assert!(
            failure.detail.contains(&instance_id(segment)),
            "{segment} missing from {:?}",
            failure.detail
        );
    }
}

#[tokio::test]
async fn empty_registry_gives_an_empty_set() {
    let set = discover(&hub_with(&[]), &guard()).await.unwrap();
    assert!(set.plugins().is_empty());
}

#[tokio::test]
async fn plugins_are_ordered_by_priority_with_their_keys() {
    let plugins = [
        PluginSpec::new("cf.test.b.section.v1", 30, KeyAnswer::Key("b")),
        PluginSpec::new("cf.test.a.section.v1", -5, KeyAnswer::Key("a")),
        PluginSpec::new("cf.test.c.section.v1", 10, KeyAnswer::Key("c")),
    ];
    let set = discover(&hub_with(&plugins), &guard()).await.unwrap();
    assert_eq!(set.keys(), ["a", "c", "b"]);
    let priorities: Vec<i16> = set.plugins().iter().map(|p| p.priority).collect();
    assert_eq!(priorities, [-5, 10, 30]);
    assert_eq!(
        set.plugins()[0].instance_id,
        instance_id("cf.test.a.section.v1")
    );
    for p in &plugins {
        assert_eq!(
            p.client.as_ref().unwrap().calls(),
            1,
            "key read once per plugin"
        );
    }
}

#[tokio::test]
async fn registry_is_listed_once_with_the_section_plugin_pattern() {
    let registry = Arc::new(MockTypesRegistryClient::new());
    let hub = ClientHub::new();
    hub.register::<dyn TypesRegistryClient>(registry.clone());
    discover(&hub, &guard()).await.unwrap();
    assert_eq!(registry.list_instance_calls(), 1);
    let queries = registry.received_instance_queries();
    let type_id = <ApplicationManagerSectionPluginSpecV1 as GtsSchema>::TYPE_ID;
    assert_eq!(
        queries[0].pattern.as_deref(),
        Some(format!("{type_id}*").as_str())
    );
}

#[tokio::test]
async fn instances_of_other_types_are_ignored() {
    let other = make_test_instance(
        "gts.cf.toolkit.plugins.plugin.v1~cf.core.other.plugin.v1~cf.test.x.other.v1",
        json!({}),
    );
    let registry = MockTypesRegistryClient::new().with_instances([other]);
    let plugins = [PluginSpec::new(
        "cf.test.a.section.v1",
        1,
        KeyAnswer::Key("a"),
    )];
    let set = discover(&hub_with_registry(&plugins, registry), &guard())
        .await
        .unwrap();
    assert_eq!(set.keys(), ["a"]);
}

#[tokio::test]
async fn unreachable_registry_is_not_an_empty_set() {
    let registry = MockTypesRegistryClient::new().with_list_error(internal("registry down"));
    let err = discover(&hub_with_registry(&[], registry), &guard())
        .await
        .unwrap_err();
    assert_eq!(err.reason, StartupReason::TypesRegistryUnreachable);
}

#[tokio::test]
async fn missing_registry_client_is_unreachable() {
    let err = discover(&ClientHub::new(), &guard()).await.unwrap_err();
    assert_eq!(err.reason, StartupReason::TypesRegistryUnreachable);
}

#[tokio::test]
async fn unreadable_priority_fails() {
    let mut bad_priority = PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Key("a"));
    bad_priority.content["priority"] = json!("first");
    let mut wrong_id = PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Key("a"));
    wrong_id.content["id"] = json!(instance_id("cf.test.other.section.v1"));
    for spec in [bad_priority, wrong_id] {
        let failure = failure_of(&[spec]).await;
        assert_eq!(failure.reason, StartupReason::PriorityUnreadable);
        assert_detail_names(&failure, &["cf.test.a.section.v1"]);
    }
}

#[tokio::test]
async fn missing_client_fails() {
    let mut spec = PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Key("a"));
    spec.client = None;
    let failure = failure_of(&[spec]).await;
    assert_eq!(failure.reason, StartupReason::PluginClientUnresolved);
    assert_detail_names(&failure, &["cf.test.a.section.v1"]);
}

#[tokio::test(start_paused = true)]
async fn unavailable_key_fails() {
    for answer in [KeyAnswer::Error, KeyAnswer::Panic, KeyAnswer::Hang] {
        let spec = PluginSpec::new("cf.test.a.section.v1", 1, answer);
        let failure = failure_of(&[spec]).await;
        assert_eq!(failure.reason, StartupReason::SectionKeyUnavailable);
        assert_detail_names(&failure, &["cf.test.a.section.v1"]);
    }
}

#[tokio::test]
async fn invalid_key_fails() {
    for key in ["", "Noop", "a/b", "a b"] {
        let spec = PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Key(key));
        let failure = failure_of(&[spec]).await;
        assert_eq!(failure.reason, StartupReason::InvalidSectionKey, "{key:?}");
        assert_detail_names(&failure, &["cf.test.a.section.v1"]);
    }
}

#[tokio::test]
async fn duplicate_key_fails() {
    let plugins = [
        PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Key("same")),
        PluginSpec::new("cf.test.b.section.v1", 2, KeyAnswer::Key("same")),
    ];
    let failure = failure_of(&plugins).await;
    assert_eq!(failure.reason, StartupReason::DuplicateSectionKey);
    assert_detail_names(&failure, &["cf.test.a.section.v1", "cf.test.b.section.v1"]);
    assert!(failure.detail.contains("\"same\""), "{:?}", failure.detail);
}

#[tokio::test]
async fn equal_priority_fails() {
    let plugins = [
        PluginSpec::new("cf.test.a.section.v1", 7, KeyAnswer::Key("a")),
        PluginSpec::new("cf.test.b.section.v1", 7, KeyAnswer::Key("b")),
    ];
    let failure = failure_of(&plugins).await;
    assert_eq!(failure.reason, StartupReason::EqualPriority);
    assert_detail_names(&failure, &["cf.test.a.section.v1", "cf.test.b.section.v1"]);
    assert!(failure.detail.starts_with("7: "), "{:?}", failure.detail);
}

#[tokio::test(start_paused = true)]
async fn checks_run_in_design_order() {
    // Both an unresolved client and an equal priority: the earlier check wins.
    let mut no_client = PluginSpec::new("cf.test.a.section.v1", 7, KeyAnswer::Key("a"));
    no_client.client = None;
    let plugins = [
        no_client,
        PluginSpec::new("cf.test.b.section.v1", 7, KeyAnswer::Key("b")),
    ];
    assert_eq!(
        reason_of(&plugins).await,
        StartupReason::PluginClientUnresolved
    );

    // An invalid key and a duplicate key: the grammar check comes first.
    let plugins = [
        PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Key("dup")),
        PluginSpec::new("cf.test.b.section.v1", 2, KeyAnswer::Key("dup")),
        PluginSpec::new("cf.test.c.section.v1", 3, KeyAnswer::Key("BAD")),
    ];
    assert_eq!(reason_of(&plugins).await, StartupReason::InvalidSectionKey);

    // An invalid key and an unavailable key: reading every key comes first.
    let plugins = [
        PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Key("BAD")),
        PluginSpec::new("cf.test.b.section.v1", 2, KeyAnswer::Error),
    ];
    assert_eq!(
        reason_of(&plugins).await,
        StartupReason::SectionKeyUnavailable
    );

    // Every client is resolved before any key is read.
    let mut no_client = PluginSpec::new("cf.test.b.section.v1", 2, KeyAnswer::Key("b"));
    no_client.client = None;
    let plugins = [
        PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Error),
        no_client,
    ];
    assert_eq!(
        reason_of(&plugins).await,
        StartupReason::PluginClientUnresolved
    );

    // Every priority is read before any client is resolved.
    let mut no_client = PluginSpec::new("cf.test.a.section.v1", 1, KeyAnswer::Key("a"));
    no_client.client = None;
    let mut bad_priority = PluginSpec::new("cf.test.b.section.v1", 2, KeyAnswer::Key("b"));
    bad_priority.content["priority"] = json!("first");
    assert_eq!(
        reason_of(&[no_client, bad_priority]).await,
        StartupReason::PriorityUnreadable
    );

    // A duplicate key and an equal priority: the key check comes first.
    let plugins = [
        PluginSpec::new("cf.test.a.section.v1", 4, KeyAnswer::Key("dup")),
        PluginSpec::new("cf.test.b.section.v1", 4, KeyAnswer::Key("dup")),
    ];
    assert_eq!(
        reason_of(&plugins).await,
        StartupReason::DuplicateSectionKey
    );
}

#[test]
fn failure_display_names_the_reason() {
    let failure = StartupFailure {
        reason: StartupReason::EqualPriority,
        detail: "7: x and y".to_owned(),
    };
    assert_eq!(StartupReason::EqualPriority.to_string(), "equal_priority");
    assert_eq!(
        failure.to_string(),
        "startup_failed: equal_priority: 7: x and y"
    );
}

#[test]
fn reasons_use_the_design_names() {
    let names = [
        (
            StartupReason::TypesRegistryUnreachable,
            "types_registry_unreachable",
        ),
        (StartupReason::PriorityUnreadable, "priority_unreadable"),
        (
            StartupReason::PluginClientUnresolved,
            "plugin_client_unresolved",
        ),
        (
            StartupReason::SectionKeyUnavailable,
            "section_key_unavailable",
        ),
        (StartupReason::InvalidSectionKey, "invalid_section_key"),
        (StartupReason::DuplicateSectionKey, "duplicate_section_key"),
        (StartupReason::EqualPriority, "equal_priority"),
    ];
    for (reason, name) in names {
        assert_eq!(reason.as_str(), name);
    }
}
