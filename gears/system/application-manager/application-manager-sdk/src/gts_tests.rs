#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

const SECTION_TYPE_ID: &str = "gts.cf.core.application_manager.section.v1~";
const SPEC_TYPE_ID: &str =
    "gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~";

fn inventory_schema(type_id: &str) -> serde_json::Value {
    let want = serde_json::json!(format!("gts://{type_id}"));
    toolkit_gts::all_inventory_type_schemas()
        .expect("inventory schemas parse")
        .into_iter()
        .find(|s| s["$id"] == want)
        .unwrap_or_else(|| panic!("{type_id} is not in the inventory"))
}

#[test]
fn gts_inventory_holds_an_open_abstract_section_base_type() {
    assert_eq!(SECTION_BASE_TYPE, SECTION_TYPE_ID);
    let schema = inventory_schema(SECTION_TYPE_ID);
    assert_eq!(schema, section_base_type_schema());
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["x-gts-abstract"], true);
    assert!(
        schema.get("additionalProperties").is_none(),
        "a closed base refuses every derived member: {schema}"
    );
}

#[test]
fn gts_plugin_spec_section_type_refers_to_section_base_type() {
    let schema = inventory_schema(SPEC_TYPE_ID);
    let text = schema.to_string();
    assert!(text.contains("\"section_type\""), "{text}");
    assert!(
        text.contains(r#""x-gts-type-ref":"gts.cf.core.application_manager.section.v1~*""#),
        "{text}"
    );
}

#[test]
fn gts_build_registration_chains_instance_id_from_spec() {
    let (id, payload) = PluginV1::<ApplicationManagerSectionPluginSpecV1>::build_registration(
        "acme.iam.access_section_plugin.v1",
        "acme",
        100,
    )
    .unwrap();
    let want = format!("{SPEC_TYPE_ID}acme.iam.access_section_plugin.v1");
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
