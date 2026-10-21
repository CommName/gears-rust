#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use super::*;
use crate::domain::plugin_set::{PluginSet, SectionPlugin};
use crate::test_support::{FakeAccess, FakePlugin, KeyAnswer, ctx};

fn ready_state(plugin: &Arc<FakePlugin>) -> Arc<StartupState> {
    let state = Arc::new(StartupState::default());
    let make = |key: &str, priority| SectionPlugin {
        key: key.to_owned(),
        priority,
        instance_id: format!("id-{key}"),
        client: plugin.clone(),
    };
    state
        .publish(PluginSet::new(vec![make("second", 2), make("first", 1)]))
        .unwrap();
    state
}

#[tokio::test]
async fn not_ready_refuses_without_an_access_check() {
    let access = FakeAccess::new(Ok(()));
    let svc = ReadService::new(access.clone(), Arc::new(StartupState::default()));
    assert_eq!(
        svc.list_section_keys(&ctx()).await,
        Err(DomainError::NotReady)
    );
    assert_eq!(access.calls(), 0);
}

#[tokio::test]
async fn keys_follow_access_control_and_never_call_a_plugin() {
    let cases = [
        (Ok(()), Ok(vec!["first".to_owned(), "second".to_owned()])),
        (
            Err(AccessError::Unavailable),
            Err(DomainError::AccessUnavailable),
        ),
        (
            Err(AccessError::NotPermitted),
            Err(DomainError::NotPermitted {
                action: Action::ReadSchema,
            }),
        ),
    ];
    for (answer, expected) in cases {
        let plugin = FakePlugin::new(KeyAnswer::Key("unused"));
        let access = FakeAccess::new(answer);
        let svc = ReadService::new(access.clone(), ready_state(&plugin));
        assert_eq!(svc.list_section_keys(&ctx()).await, expected, "{answer:?}");
        assert_eq!(access.calls(), 1);
        assert_eq!(
            plugin.calls(),
            0,
            "no plugin call (nfr-authorized-before-plugin)"
        );
    }
}
