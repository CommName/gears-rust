#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::test_support::{FakePlugin, section_plugin};

fn set_of(key: &str) -> PluginSet {
    PluginSet::new(vec![section_plugin(key, 1, &FakePlugin::new())])
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

#[test]
fn section_key_derivation() {
    let base = "gts.cf.core.application_manager.section.v1~";
    let cases = [
        ("acme.iam.access.roles.v1~", Some("acme.iam.access.roles")),
        ("acme.iam.access.roles.v1.2~", Some("acme.iam.access.roles")),
        ("cf.core.noop.section.v1~", Some("cf.core.noop.section")),
        (
            "acme.iam_2.access.roles.v10.0~",
            Some("acme.iam_2.access.roles"),
        ),
        // Deeper derivation.
        ("acme.iam.access.roles.v1~acme.iam.access.more.v1~", None),
        // Missing version, missing token, extra token.
        ("acme.iam.access.roles~", None),
        ("acme.iam.access.v1~", None),
        ("acme.iam.access.roles.extra.v1~", None),
        // Bad version and bad tokens.
        ("acme.iam.access.roles.1~", None),
        ("acme.iam.access.roles.vx~", None),
        ("acme.iam.access.roles.v1.x~", None),
        ("Acme.iam.access.roles.v1~", None),
        ("acme.iam.access.ro-les.v1~", None),
        ("acme..access.roles.v1~", None),
        // Not a type id.
        ("acme.iam.access.roles.v1", None),
    ];
    for (segment, want) in cases {
        let id = format!("{base}{segment}");
        assert_eq!(section_key_of(&id).as_deref(), want, "{id}");
    }
    // Wrong base, and the base itself.
    assert_eq!(
        section_key_of("gts.cf.core.other.section.v1~acme.iam.access.roles.v1~"),
        None
    );
    assert_eq!(section_key_of(base), None);
}
