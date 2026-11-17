#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use serde_json::json;

use super::*;
use crate::domain::allowed_callers::AllowedCallers;
use crate::domain::plugin_set::PluginSet;
use crate::test_support::{FakePlugin, allowed_callers, platform_ctx, section_plugin};

fn validator() -> Arc<SchemaValidator> {
    Arc::new(SchemaValidator::new(64).unwrap())
}

fn ready_state(plugin: &Arc<FakePlugin>) -> Arc<StartupState> {
    let state = Arc::new(StartupState::default());
    state
        .publish(PluginSet::new(vec![
            section_plugin("second", 2, plugin),
            section_plugin("first", 1, plugin),
        ]))
        .unwrap();
    state
}

fn service(actions: Vec<CallerAction>, plugin: &Arc<FakePlugin>) -> ReadService {
    ReadService::new(
        Arc::new(allowed_callers("app-seeder", actions)),
        ready_state(plugin),
        validator(),
    )
}

#[test]
fn not_ready_refuses_every_read_before_the_caller_check() {
    let svc = ReadService::new(
        Arc::new(AllowedCallers::default()),
        Arc::new(StartupState::default()),
        validator(),
    );
    let ctx = platform_ctx("app-seeder");
    assert_eq!(svc.list_section_keys(&ctx), Err(DomainError::NotReady));
    assert_eq!(
        svc.section_schema(&ctx, "first").err(),
        Some(DomainError::NotReady)
    );
    assert_eq!(svc.manifest_schema(&ctx).err(), Some(DomainError::NotReady));
}

#[test]
fn keys_follow_the_allowed_callers_and_never_call_a_plugin() {
    let not_permitted = Err(DomainError::NotPermitted {
        action: CallerAction::ReadSchema,
    });
    let cases = [
        (
            "app-seeder",
            vec![CallerAction::ReadSchema],
            Ok(vec!["first".to_owned(), "second".to_owned()]),
        ),
        (
            "someone-else",
            vec![CallerAction::ReadSchema],
            not_permitted.clone(),
        ),
        ("app-seeder", vec![CallerAction::Seed], not_permitted),
    ];
    for (caller, actions, expected) in cases {
        let plugin = FakePlugin::new();
        let svc = service(actions, &plugin);
        assert_eq!(
            svc.list_section_keys(&platform_ctx(caller)),
            expected,
            "{caller}"
        );
        assert_eq!(
            plugin.calls(),
            0,
            "no plugin call (nfr-authorized-before-plugin)"
        );
    }
}

#[test]
fn one_section_schema_comes_with_its_section_type() {
    let plugin = FakePlugin::new();
    let svc = service(vec![CallerAction::ReadSchema], &plugin);
    let section = svc
        .section_schema(&platform_ctx("app-seeder"), "first")
        .unwrap();
    assert_eq!(section.key(), "first");
    assert_eq!(
        section.section_type(),
        "gts.cf.core.application_manager.section.v1~first.v1~"
    );
    assert_eq!(
        section.schema(),
        &json!({ "$id": "urn:cf:application-manager:section:first", "type": "object" })
    );
    assert_eq!(plugin.calls(), 0);
}

#[test]
fn an_unknown_key_is_checked_after_the_caller() {
    let plugin = FakePlugin::new();
    let allowed = service(vec![CallerAction::ReadSchema], &plugin);
    assert_eq!(
        allowed
            .section_schema(&platform_ctx("app-seeder"), "nope")
            .err(),
        Some(DomainError::UnknownSectionKey)
    );
    let refused = service(vec![CallerAction::Seed], &plugin);
    assert_eq!(
        refused
            .section_schema(&platform_ctx("app-seeder"), "nope")
            .err(),
        Some(DomainError::NotPermitted {
            action: CallerAction::ReadSchema
        })
    );
}

#[test]
fn the_combined_schema_lists_every_section_type_in_priority_order() {
    let plugin = FakePlugin::new();
    let svc = service(vec![CallerAction::ReadSchema], &plugin);
    let read = svc.manifest_schema(&platform_ctx("app-seeder")).unwrap();
    assert_eq!(
        read.sections,
        [
            (
                "first".to_owned(),
                "gts.cf.core.application_manager.section.v1~first.v1~".to_owned()
            ),
            (
                "second".to_owned(),
                "gts.cf.core.application_manager.section.v1~second.v1~".to_owned()
            ),
        ]
    );
    let sections = &read.schema["properties"]["sections"];
    assert_eq!(sections["required"], json!(["first", "second"]));
    assert_eq!(
        sections["properties"]["second"]["$id"],
        "urn:cf:application-manager:section:second"
    );
    assert_eq!(
        read.schema["properties"]["application"]["properties"]["id"]["maxLength"],
        64
    );
    assert_eq!(plugin.calls(), 0);
}
