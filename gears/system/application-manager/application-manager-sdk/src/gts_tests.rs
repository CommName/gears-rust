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

const OUTCOME_REPORT_TYPE_ID: &str = "gts.cf.core.application_manager.outcome_report.v1~";

fn outcome_report_schema() -> serde_json::Value {
    let want = serde_json::json!(format!("gts://{OUTCOME_REPORT_TYPE_ID}"));
    toolkit_gts::all_inventory_type_schemas()
        .expect("inventory schemas parse")
        .into_iter()
        .find(|s| s["$id"] == want)
        .expect("the outcome report schema is in the inventory")
}

fn sample_report() -> crate::models::OutcomeReport {
    use crate::models::{
        Dependency, OperationAction, OutcomeReport, ReportApplication, ReportProblem,
        RequestCategory, SectionOutcome, SectionOutcomeEntry, UnavailableItem,
        UnavailableReasonCode,
    };
    OutcomeReport {
        operation_id: uuid::Uuid::from_u128(9),
        action: OperationAction::Seed,
        category: Some(RequestCategory::InvalidManifest),
        application: ReportApplication {
            id: "acme.app".to_owned(),
            version: Some("1.0.0".to_owned()),
        },
        sections: vec![SectionOutcomeEntry {
            key: "billing".to_owned(),
            section_type: Some(
                "gts.cf.core.application_manager.section.v1~acme.billing.plan.v1~".to_owned(),
            ),
            outcome: Some(SectionOutcome::Rejected),
            detail: Some("no".to_owned()),
            location: Some("/plan".to_owned()),
        }],
        problems: vec![ReportProblem {
            location: "/sections/billing".to_owned(),
            rule: "required".to_owned(),
            message: "missing".to_owned(),
        }],
        unavailable: vec![UnavailableItem {
            dependency: Dependency::OperationStorage,
            reason: UnavailableReasonCode::TimedOut,
        }],
    }
}

#[test]
fn outcome_report_type_constant_is_exact() {
    assert_eq!(OUTCOME_REPORT_TYPE, OUTCOME_REPORT_TYPE_ID);
}

#[test]
fn gts_inventory_holds_outcome_report_schema() {
    let schema = outcome_report_schema();
    assert_eq!(
        schema["$id"],
        serde_json::json!(format!("gts://{OUTCOME_REPORT_TYPE_ID}"))
    );
}

#[test]
fn outcome_report_schema_accepts_a_sample_report() {
    let schema = outcome_report_schema();
    let validator = jsonschema::validator_for(&schema).expect("the schema compiles");
    let completed = serde_json::to_value(sample_report()).unwrap();
    assert!(validator.is_valid(&completed), "{completed}");

    let mut open = sample_report();
    open.category = None;
    open.sections[0].outcome = None;
    open.sections[0].section_type = None;
    let open = serde_json::to_value(open).unwrap();
    assert!(validator.is_valid(&open), "{open}");

    let mut lost = sample_report();
    lost.sections[0].outcome = Some(crate::models::SectionOutcome::Lost);
    let lost = serde_json::to_value(lost).unwrap();
    assert!(validator.is_valid(&lost), "{lost}");
}

#[test]
fn outcome_report_schema_refuses_an_unknown_category() {
    let schema = outcome_report_schema();
    let validator = jsonschema::validator_for(&schema).expect("the schema compiles");
    let mut report = serde_json::to_value(sample_report()).unwrap();
    report["category"] = serde_json::json!("not_a_category");
    assert!(!validator.is_valid(&report));
}
