#![allow(clippy::unwrap_used, clippy::expect_used)]

use serde_json::json;

use super::*;

#[test]
fn defaults_apply_to_an_empty_block() {
    let cfg: NoopSectionPluginConfig = serde_json::from_value(json!({})).unwrap();
    assert_eq!(cfg, NoopSectionPluginConfig::default());
    assert_eq!(cfg.vendor, "constructorfabric");
    assert_eq!(cfg.priority, 100);
}

#[test]
fn unknown_field_is_refused() {
    assert!(serde_json::from_value::<NoopSectionPluginConfig>(json!({ "surprise": 1 })).is_err());
}
