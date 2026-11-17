//! The no-op section type.

use application_manager_sdk::ApplicationManagerSectionV1;
use gts::GtsSchema;
use serde_json::{Value, json};

/// The section type of the no-op plugin, derived directly from the section
/// base type. Its key is `cf.core.noop.section`.
pub const SECTION_TYPE: &str =
    "gts.cf.core.application_manager.section.v1~cf.core.noop.section.v1~";

/// The schema of [`SECTION_TYPE`]: any JSON object, as Draft-07.
#[must_use]
pub fn section_type_schema() -> Value {
    json!({
        "$id": format!("gts://{SECTION_TYPE}"),
        "$schema": "http://json-schema.org/draft-07/schema#",
        "description": "No-op section: any JSON object.",
        "allOf": [{
            "$ref": format!("gts://{}", <ApplicationManagerSectionV1 as GtsSchema>::TYPE_ID)
        }],
        "type": "object"
    })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "section_tests.rs"]
mod tests;
