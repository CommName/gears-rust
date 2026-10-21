#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

const SPEC_TYPE_ID: &str =
    "gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~";

#[test]
fn gts_inventory_holds_section_plugin_spec_schema() {
    let registered = toolkit_gts::all_inventory_type_schemas().expect("inventory schemas parse");
    let want = serde_json::json!(format!("gts://{SPEC_TYPE_ID}"));
    assert!(registered.iter().any(|s| s["$id"] == want));
}

#[test]
fn gts_build_registration_chains_instance_id_from_spec() {
    let (id, payload) = PluginV1::<ApplicationManagerSectionPluginSpecV1>::build_registration(
        "acme.billing.section.v1",
        "acme",
        100,
    )
    .unwrap();
    let want = format!("{SPEC_TYPE_ID}acme.billing.section.v1");
    assert_eq!(id.to_string(), want);
    assert_eq!(payload["id"], serde_json::json!(want));
    assert_eq!(payload["vendor"], serde_json::json!("acme"));
    assert_eq!(payload["priority"], serde_json::json!(100));
}

#[test]
fn gts_application_resource_type_is_exact() {
    assert_eq!(
        APPLICATION_RESOURCE_TYPE,
        "gts.cf.core.application_manager.application.v1~"
    );
}
