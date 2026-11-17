#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

#[test]
fn the_section_type_derives_from_the_section_base_type() {
    let base = <ApplicationManagerSectionV1 as GtsSchema>::TYPE_ID;
    assert_eq!(
        SECTION_TYPE.strip_prefix(base),
        Some("cf.core.noop.section.v1~")
    );
    let schema = section_type_schema();
    assert_eq!(schema["$id"], format!("gts://{SECTION_TYPE}"));
    assert_eq!(schema["$schema"], "http://json-schema.org/draft-07/schema#");
    assert_eq!(schema["allOf"][0]["$ref"], format!("gts://{base}"));
    assert_eq!(schema["type"], "object");
}
