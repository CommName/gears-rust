#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use serde_json::json;
use types_registry_sdk::testing::{MockTypesRegistryClient, internal, make_test_instance};

use super::*;
use crate::test_support::{
    PluginSpec, hub_with, hub_with_registry, instance_id, section_type, type_schema,
};

async fn failure_of(plugins: &[PluginSpec]) -> StartupFailure {
    discover(&hub_with(plugins))
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

fn no_client(mut spec: PluginSpec) -> PluginSpec {
    spec.client = None;
    spec
}

fn unregistered_type(mut spec: PluginSpec) -> PluginSpec {
    spec.section_type = None;
    spec
}

fn bad_priority(mut spec: PluginSpec) -> PluginSpec {
    spec.content["priority"] = json!("first");
    spec
}

#[tokio::test]
async fn empty_registry_gives_an_empty_set() {
    let set = discover(&hub_with(&[])).await.unwrap();
    assert!(set.plugins().is_empty());
}

#[tokio::test]
async fn plugins_are_ordered_by_priority_with_their_derived_keys() {
    let plugins = [
        PluginSpec::new("cf.test.b.plugin.v1", 30, "cf.test.b.section.v1"),
        PluginSpec::new("cf.test.a.plugin.v1", -5, "cf.test.a.section.v1.2"),
        PluginSpec::new("cf.test.c.plugin.v1", 10, "cf.test.c.section.v1"),
    ];
    let set = discover(&hub_with(&plugins)).await.unwrap();
    assert_eq!(
        set.keys(),
        [
            "cf.test.a.section",
            "cf.test.c.section",
            "cf.test.b.section"
        ]
    );
    let priorities: Vec<i16> = set.plugins().iter().map(|p| p.priority).collect();
    assert_eq!(priorities, [-5, 10, 30]);
    let first = &set.plugins()[0];
    assert_eq!(first.instance_id, instance_id("cf.test.a.plugin.v1"));
    assert_eq!(first.section_type, section_type("cf.test.a.section.v1.2"));
    assert_eq!(first.schema["type"], "object");
    for p in &plugins {
        assert_eq!(p.client.as_ref().unwrap().calls(), 0, "no plugin call");
    }
}

#[tokio::test]
async fn the_kept_schema_inlines_the_base_type() {
    let type_id = section_type("cf.test.a.section.v1");
    let mut spec = PluginSpec::new("cf.test.a.plugin.v1", 1, "cf.test.a.section.v1");
    spec.section_type = Some(type_schema(
        &type_id,
        json!({
            "allOf": [{ "$ref": format!("gts://{}", crate::test_support::base_type_id()) }],
            "properties": { "roles": { "type": "array" } }
        }),
    ));
    let set = discover(&hub_with(&[spec])).await.unwrap();
    let schema = &set.plugins()[0].schema;
    assert_eq!(schema["properties"]["roles"]["type"], "array");
    assert!(
        !schema.to_string().contains("\"$ref\""),
        "the parent is inlined: {schema}"
    );
}

#[tokio::test]
async fn registry_is_listed_once_with_the_section_plugin_pattern() {
    let registry = Arc::new(MockTypesRegistryClient::new());
    let hub = ClientHub::new();
    hub.register::<dyn TypesRegistryClient>(registry.clone());
    discover(&hub).await.unwrap();
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
        "cf.test.a.plugin.v1",
        1,
        "cf.test.a.section.v1",
    )];
    let set = discover(&hub_with_registry(&plugins, registry))
        .await
        .unwrap();
    assert_eq!(set.keys(), ["cf.test.a.section"]);
}

#[tokio::test]
async fn unreachable_registry_is_not_an_empty_set() {
    let registry = MockTypesRegistryClient::new().with_list_error(internal("registry down"));
    let err = discover(&hub_with_registry(&[], registry))
        .await
        .unwrap_err();
    assert_eq!(err.reason, StartupReason::TypesRegistryUnreachable);
}

#[tokio::test]
async fn missing_registry_client_is_unreachable() {
    let err = discover(&ClientHub::new()).await.unwrap_err();
    assert_eq!(err.reason, StartupReason::TypesRegistryUnreachable);
}

#[tokio::test]
async fn unreadable_properties_fail() {
    let spec = || PluginSpec::new("cf.test.a.plugin.v1", 1, "cf.test.a.section.v1");
    let mut no_section_type = spec();
    no_section_type.content["properties"] = json!({});
    let mut wrong_id = spec();
    wrong_id.content["id"] = json!(instance_id("cf.test.other.plugin.v1"));
    for spec in [bad_priority(spec()), no_section_type, wrong_id] {
        let failure = failure_of(&[spec]).await;
        assert_eq!(failure.reason, StartupReason::PluginPropertiesUnreadable);
        assert_detail_names(&failure, &["cf.test.a.plugin.v1"]);
    }
}

#[tokio::test]
async fn unresolved_section_type_fails() {
    let spec = unregistered_type(PluginSpec::new(
        "cf.test.a.plugin.v1",
        1,
        "cf.test.a.section.v1",
    ));
    let failure = failure_of(&[spec]).await;
    assert_eq!(failure.reason, StartupReason::SectionTypeUnresolved);
    assert_detail_names(&failure, &["cf.test.a.plugin.v1"]);
    let mut empty = PluginSpec::new("cf.test.a.plugin.v1", 1, "cf.test.a.section.v1");
    empty.content["properties"]["section_type"] = json!("");
    assert_eq!(
        reason_of(&[empty]).await,
        StartupReason::SectionTypeUnresolved
    );
}

#[tokio::test]
async fn section_type_of_the_wrong_shape_fails() {
    let deeper = format!(
        "{}cf.test.a.section.v1~cf.test.a.deeper.v1~",
        crate::test_support::base_type_id()
    );
    for type_id in [
        "gts.cf.core.other_base.section.v1~cf.test.a.section.v1~".to_owned(),
        deeper,
    ] {
        let spec = PluginSpec::naming("cf.test.a.plugin.v1", 1, &type_id);
        let failure = failure_of(&[spec]).await;
        assert_eq!(
            failure.reason,
            StartupReason::InvalidSectionType,
            "{type_id}"
        );
        assert_detail_names(&failure, &["cf.test.a.plugin.v1"]);
    }
}

#[tokio::test]
async fn missing_client_fails() {
    let spec = no_client(PluginSpec::new(
        "cf.test.a.plugin.v1",
        1,
        "cf.test.a.section.v1",
    ));
    let failure = failure_of(&[spec]).await;
    assert_eq!(failure.reason, StartupReason::PluginClientUnresolved);
    assert_detail_names(&failure, &["cf.test.a.plugin.v1"]);
}

#[tokio::test]
async fn duplicate_key_fails_including_two_versions_of_one_type() {
    let plugins = [
        PluginSpec::new("cf.test.a.plugin.v1", 1, "cf.test.same.section.v1"),
        PluginSpec::new("cf.test.b.plugin.v1", 2, "cf.test.same.section.v2"),
    ];
    let failure = failure_of(&plugins).await;
    assert_eq!(failure.reason, StartupReason::DuplicateSectionKey);
    assert_detail_names(&failure, &["cf.test.a.plugin.v1", "cf.test.b.plugin.v1"]);
    assert!(
        failure.detail.contains("\"cf.test.same.section\""),
        "{:?}",
        failure.detail
    );
}

#[tokio::test]
async fn equal_priority_fails() {
    let plugins = [
        PluginSpec::new("cf.test.a.plugin.v1", 7, "cf.test.a.section.v1"),
        PluginSpec::new("cf.test.b.plugin.v1", 7, "cf.test.b.section.v1"),
    ];
    let failure = failure_of(&plugins).await;
    assert_eq!(failure.reason, StartupReason::EqualPriority);
    assert_detail_names(&failure, &["cf.test.a.plugin.v1", "cf.test.b.plugin.v1"]);
    assert!(failure.detail.starts_with("7: "), "{:?}", failure.detail);
}

#[tokio::test]
async fn checks_run_in_design_order() {
    let spec = |segment: &'static str, priority, ty: &str| PluginSpec::new(segment, priority, ty);
    let invalid = |segment: &'static str, priority| {
        PluginSpec::naming(
            segment,
            priority,
            "gts.cf.core.other_base.section.v1~cf.test.x.section.v1~",
        )
    };

    // Every property is read before any section type is resolved.
    let plugins = [
        unregistered_type(spec("cf.test.a.plugin.v1", 1, "cf.test.a.section.v1")),
        bad_priority(spec("cf.test.b.plugin.v1", 2, "cf.test.b.section.v1")),
    ];
    assert_eq!(
        reason_of(&plugins).await,
        StartupReason::PluginPropertiesUnreadable
    );

    // Every section type is resolved before any shape is checked.
    let plugins = [
        invalid("cf.test.a.plugin.v1", 1),
        unregistered_type(spec("cf.test.b.plugin.v1", 2, "cf.test.b.section.v1")),
    ];
    assert_eq!(
        reason_of(&plugins).await,
        StartupReason::SectionTypeUnresolved
    );

    // Every shape is checked before any client is resolved.
    let plugins = [
        no_client(spec("cf.test.a.plugin.v1", 1, "cf.test.a.section.v1")),
        invalid("cf.test.b.plugin.v1", 2),
    ];
    assert_eq!(reason_of(&plugins).await, StartupReason::InvalidSectionType);

    // Every client is resolved before keys and priorities are compared.
    let plugins = [
        spec("cf.test.a.plugin.v1", 7, "cf.test.same.section.v1"),
        no_client(spec("cf.test.b.plugin.v1", 7, "cf.test.same.section.v2")),
    ];
    assert_eq!(
        reason_of(&plugins).await,
        StartupReason::PluginClientUnresolved
    );

    // A duplicate key and an equal priority: the key check comes first.
    let plugins = [
        spec("cf.test.a.plugin.v1", 4, "cf.test.same.section.v1"),
        spec("cf.test.b.plugin.v1", 4, "cf.test.same.section.v2"),
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
        (
            StartupReason::PluginPropertiesUnreadable,
            "plugin_properties_unreadable",
        ),
        (
            StartupReason::SectionTypeUnresolved,
            "section_type_unresolved",
        ),
        (StartupReason::InvalidSectionType, "invalid_section_type"),
        (
            StartupReason::PluginClientUnresolved,
            "plugin_client_unresolved",
        ),
        (StartupReason::DuplicateSectionKey, "duplicate_section_key"),
        (StartupReason::EqualPriority, "equal_priority"),
    ];
    for (reason, name) in names {
        assert_eq!(reason.as_str(), name);
    }
}
