//! Pin tests for the domain-level limits (DESIGN §3.7, §4.7).

use application_manager_sdk::models::HANDOFF_MANIFEST_MAX_DEPTH;

use crate::domain::limits::{
    LOCATION_MAX_BYTES, LOG_TEXT_MAX_BYTES, MAX_JSON_DEPTH, MAX_VALIDATION_ERRORS_SCANNED,
    MAX_VALIDATION_PROBLEMS, REGISTRY_TEXT_MAX_BYTES, SCHEMA_MAX_DEPTH, SCHEMA_MAX_PATHS,
    SCHEMA_MAX_PATTERNS, SCHEMA_PATTERNS_MAX_BYTES, SCHEMA_REGEX_DFA_SIZE_LIMIT,
    SCHEMA_REGEX_SIZE_LIMIT,
};

// DESIGN §3.7: the registry id and version columns are 255 bytes wide. A change of
// this value is a schema change and needs a migration.
#[test]
fn registry_text_max_bytes_is_pinned_to_255() {
    assert_eq!(
        REGISTRY_TEXT_MAX_BYTES, 255,
        "registry text columns are 255 bytes wide (DESIGN §3.7); changing it needs a migration"
    );
}

// DESIGN §1.3 constants, §3.2 step 1: the parse depth bound is a deliberate pin at exactly the
// SDK's hand-off bound, so a manifest the gear accepts always reads back from the hand-off
// content. Moving it is a decision (DESIGN, seeds and tests move with it), not a tweak; it must
// never exceed the SDK bound.
#[test]
fn max_json_depth_is_pinned_to_the_handoff_bound() {
    assert_eq!(MAX_JSON_DEPTH, 126);
    assert_eq!(MAX_JSON_DEPTH, HANDOFF_MANIFEST_MAX_DEPTH);
}

// DESIGN §3.2, §4.3: a served schema nested deeper than this is unusable. The bound is
// small enough for the recursive compile and clone (a 2 MiB test thread) and large enough
// for real schemas; pinned so a change is a decision.
#[test]
fn schema_max_depth_is_pinned_to_64() {
    assert_eq!(SCHEMA_MAX_DEPTH, 64);
}

// DESIGN §3.2 Schema Validator: explicit compile limits of the linear-time pattern engine.
// Both are real limits (not zero, not unbounded) and pinned at 1 MiB.
#[test]
fn the_schema_regex_limits_are_pinned_to_one_mebibyte() {
    assert_eq!(SCHEMA_REGEX_SIZE_LIMIT, 1024 * 1024);
    assert_eq!(SCHEMA_REGEX_DFA_SIZE_LIMIT, 1024 * 1024);
}

// DESIGN §4.3: a node of a served schema reached by more reference paths than this makes the
// schema unusable. 4096 allows 10 levels of a two-way fan-out (the `$defs` place counts one path); pinned so a change is a decision.
#[test]
fn schema_max_paths_is_pinned_to_4096() {
    assert_eq!(SCHEMA_MAX_PATHS, 4096);
}

// DESIGN §3.3, §4.3: one validation answers at most 100 problems (plus the `too_many_problems`
// marker); the scanned-error bound is a multiple of it, never below it.
#[test]
fn the_problem_bounds_are_pinned() {
    assert_eq!(MAX_VALIDATION_PROBLEMS, 100);
    assert_eq!(MAX_VALIDATION_ERRORS_SCANNED, 10_000);
    const { assert!(MAX_VALIDATION_ERRORS_SCANNED >= MAX_VALIDATION_PROBLEMS) };
}

// DESIGN §3.3: a problem location is as long as one log text at most.
#[test]
fn the_location_bound_is_the_log_text_bound() {
    assert_eq!(LOCATION_MAX_BYTES, LOG_TEXT_MAX_BYTES);
    assert_eq!(LOCATION_MAX_BYTES, 1024);
}

// DESIGN §4.3: the pattern count and byte bounds of one served schema.
#[test]
fn the_pattern_bounds_are_pinned() {
    assert_eq!(SCHEMA_MAX_PATTERNS, 256);
    assert_eq!(SCHEMA_PATTERNS_MAX_BYTES, 64 * 1024);
}
