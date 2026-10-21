use super::*;

#[test]
fn limits_pin_design_values() {
    assert_eq!(SCHEMA_VERSION_MAX_BYTES, 256);
    assert_eq!(PLUGIN_DETAIL_MAX_BYTES, 1024);
    assert_eq!(SERVED_SCHEMA_MAX_BYTES, 1_048_576);
}
