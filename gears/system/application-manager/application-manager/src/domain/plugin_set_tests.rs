#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::test_support::{FakePlugin, KeyAnswer};

fn set_of(key: &str) -> PluginSet {
    PluginSet::new(vec![SectionPlugin {
        key: key.to_owned(),
        priority: 1,
        instance_id: format!("id-{key}"),
        client: FakePlugin::new(KeyAnswer::Key("unused")),
    }])
}

#[test]
fn the_plugin_set_is_published_once() {
    let state = StartupState::default();
    assert!(state.plugin_set().is_none());
    state.publish(set_of("first")).unwrap();
    let rejected = state.publish(set_of("second")).unwrap_err();
    assert_eq!(rejected.keys(), ["second"]);
    assert_eq!(state.plugin_set().unwrap().keys(), ["first"]);
}
