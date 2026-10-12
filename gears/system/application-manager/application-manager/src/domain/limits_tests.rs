//! Pin tests for the domain-level limits (DESIGN §3.7, §4.7).

use crate::domain::limits::REGISTRY_TEXT_MAX_BYTES;

// DESIGN §3.7: the registry id and version columns are 255 bytes wide. A change of
// this value is a schema change and needs a migration.
#[test]
fn registry_text_max_bytes_is_pinned_to_255() {
    assert_eq!(
        REGISTRY_TEXT_MAX_BYTES, 255,
        "registry text columns are 255 bytes wide (DESIGN §3.7); changing it needs a migration"
    );
}
