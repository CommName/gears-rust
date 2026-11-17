#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use super::*;
use crate::domain::plugin_set::PluginSet;
use crate::test_support::{FakePlugin, allowed_callers, platform_ctx, section_plugin};

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

#[test]
fn not_ready_refuses_before_the_caller_check() {
    let svc = ReadService::new(
        Arc::new(AllowedCallers::default()),
        Arc::new(StartupState::default()),
    );
    assert_eq!(
        svc.list_section_keys(&platform_ctx("app-seeder")),
        Err(DomainError::NotReady)
    );
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
        let svc = ReadService::new(
            Arc::new(allowed_callers("app-seeder", actions)),
            ready_state(&plugin),
        );
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
