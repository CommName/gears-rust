//! Unit tests for the Schema Validator: the combined schema and whole-manifest validation
//! (DESIGN §3.2 "Schema Validator", §3.3 problem shapes, §4.1, §4.3 "A served schema is
//! hostile", §4.8 unit and security rows, ADR 0002, ADR 0003).
//!
//! Planned API in `crate::domain::schema_validator` (new file, P5). The module ends with:
//!
//! ```text
//! #[cfg(test)]
//! #[cfg_attr(coverage_nightly, coverage(off))]
//! #[allow(clippy::non_ascii_literal)]
//! #[path = "schema_validator_tests.rs"]
//! mod schema_validator_tests;
//! ```
//!
//! ```text
//! /// The schema one section plugin served for `key`, exactly as served.
//! #[derive(Debug, Clone, PartialEq)]
//! pub struct SectionSchema { pub key: String, pub schema: serde_json::Value }
//!
//! /// Why a served schema is `unusable` (the section schema reason of the report). When a
//! /// schema has several defects the cause is the first of this precedence: TooLarge,
//! /// TooDeep, Number, Dialect, ExternalReference, Pattern, Compile.
//! #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//! pub enum UnusableCause {
//!     TooLarge,           // longer than SERVED_SCHEMA_MAX_BYTES (compact serialization)
//!     TooDeep,            // JSON nesting (arrays and objects, the root counts 1) beyond SCHEMA_MAX_DEPTH
//!     Number,             // outside the IEEE-754 double range, or an integer beyond 2^53 in magnitude
//!     Dialect,            // `$schema` (anywhere in the document) names another dialect than 2020-12
//!     ExternalReference,  // a `$ref`/`$dynamicRef` that does not start with `#` (incl. one relying on the served `$id`)
//!     Pattern,            // a pattern (anywhere, `$defs` included) the linear-time engine cannot compile within its limits
//!     Compile,            // any other compile failure, incl. a dangling local reference and
//!                         // an in-place `$ref` cycle in a schema that uses unevaluatedItems/unevaluatedProperties
//! }
//! #[derive(Debug, Clone, PartialEq, Eq)]
//! pub struct UnusableSchema { pub key: String, pub cause: UnusableCause }
//!
//! /// `urn:cf:application-manager:section:<key>`.
//! pub fn section_schema_id(key: &str) -> String;
//!
//! /// Checks every served schema WITHOUT a manifest: the one rule discovery and seed
//! /// validation share. Iterative and bounded before anything recursive (clone, compile) runs:
//! /// a hostile schema returns an error, never aborts.
//! ///
//! /// # Errors
//! /// Every key whose served schema is unusable, in key order.
//! pub fn check_schemas(sections: &[SectionSchema]) -> Result<(), Vec<UnusableSchema>>;
//! /// Test seam of `check_schemas`: the reference retriever is injected (production passes
//! /// one that refuses every reference). A counting retriever must never be called.
//! pub(crate) fn check_schemas_with(
//!     sections: &[SectionSchema],
//!     retriever: impl Fn() -> Box<dyn jsonschema::Retrieve>,
//! ) -> Result<(), Vec<UnusableSchema>>;
//!
//! /// The combined schema (DESIGN §3.2): see the tests below for the pinned shape. Defined
//! /// for USABLE sections only (`check_schemas` is Ok); the result for an unusable one is
//! /// unspecified (it may not even compile).
//! pub fn combined_schema(sections: &[SectionSchema]) -> serde_json::Value;
//!
//! /// `check_schemas` first, then ONE validation of the whole manifest against the combined
//! /// schema, returning EVERY problem sorted by (location, rule, message). Pure and
//! /// synchronous: the caller runs it on the blocking pool (DESIGN §3.2). Nothing but the
//! /// sections' schemas is consulted.
//! ///
//! /// # Errors
//! /// What `check_schemas` returns. Nothing is validated then: the request ends as
//! /// temporarily unavailable with reason `unusable` (DESIGN §3.2 step 4).
//! pub fn validate_manifest(sections: &[SectionSchema], manifest: &serde_json::Value)
//!     -> Result<Vec<Problem>, Vec<UnusableSchema>>;
//! ```
//!
//! New constant in `crate::domain::limits`: `SCHEMA_MAX_DEPTH: usize = 64` (see `limits_tests`).
//!
//! Locations and rules of problems (decisions, see the test-author report): the location is
//! the instance JSON Pointer of the failing value (RFC 6901 escaped). `required` points at the
//! OBJECT; `additionalProperties` and `unevaluatedProperties` give ONE problem PER unexpected
//! member, at `/<object>/<escaped member name>` (also for a bare `additionalProperties:
//! false`, which the crate reports once at the object). In `sections`, DESIGN's key-set rule
//! gives `missing_section` / `unknown_section` at `/sections/<key>`. The rule is the failed
//! keyword (`false` for a false schema); see `every_keyword_has_its_exact_location_and_rule`.
//! The envelope limits (`max_length`, `control_character`, emptiness) are NOT checked here:
//! they are checked at receive time (`manifest::EnvelopeFields::limit_problems`) and joined
//! to this result by the orchestrator.

use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use application_manager_sdk::models::Problem;
use application_manager_sdk::section_plugin_api::SERVED_SCHEMA_MAX_BYTES;
use jsonschema::{Retrieve, Uri};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
use serde_json::{Value, json};

use crate::domain::limits::{
    LOCATION_MAX_BYTES, MAX_JSON_DEPTH, MAX_VALIDATION_PROBLEMS, SCHEMA_MAX_DEPTH,
    SCHEMA_MAX_PATTERNS, SCHEMA_PATTERNS_MAX_BYTES, SCHEMA_REGEX_SIZE_LIMIT,
};
use crate::domain::schema_validator::{
    SectionSchema, UnusableCause, UnusableSchema, check_schemas, check_schemas_with,
    combined_schema, section_schema_id, validate_manifest,
};
use crate::test_support::manifest_gen::arb_json_value;

const DRAFT_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";

fn sec(key: &str, schema: Value) -> SectionSchema {
    SectionSchema {
        key: key.to_owned(),
        schema,
    }
}

fn net_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "host": {"type": "string", "pattern": "^[a-z]+$"}
        },
        "required": ["port"],
        "additionalProperties": false
    })
}

fn db_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"name": {"type": "string", "minLength": 3}},
        "required": ["name"]
    })
}

fn two() -> Vec<SectionSchema> {
    vec![sec("net", net_schema()), sec("db", db_schema())]
}

fn manifest(sections: &Value) -> Value {
    json!({"application": {"id": "app", "version": "1"}, "sections": sections})
}

fn ok_manifest() -> Value {
    manifest(&json!({"net": {"port": 80, "host": "shop"}, "db": {"name": "orders"}}))
}

fn problems(sections: &[SectionSchema], manifest: &Value) -> Vec<Problem> {
    validate_manifest(sections, manifest).expect("every schema is usable")
}

fn keys(ps: &[Problem]) -> Vec<(&str, &str)> {
    ps.iter()
        .map(|p| (p.location.as_str(), p.rule.as_str()))
        .collect()
}

/// The unusable keys of `sections`; discovery (`check_schemas`, no manifest) and seed
/// validation (`validate_manifest`, any manifest) agree on them.
fn unusable(sections: &[SectionSchema]) -> Vec<UnusableSchema> {
    let got = validate_manifest(sections, &ok_manifest()).expect_err("some schema is unusable");
    assert_eq!(
        check_schemas(sections),
        Err(got.clone()),
        "discovery and seed validation follow one rule"
    );
    got
}

fn causes(schema: &Value) -> Vec<UnusableSchema> {
    unusable(&[sec("s", schema.clone())])
}

/// Both entry points accept the schema.
fn assert_usable(schema: &Value) {
    let sections = [sec("s", schema.clone())];
    assert_eq!(check_schemas(&sections), Ok(()), "schema {schema}");
    assert!(
        validate_manifest(&sections, &manifest(&json!({"s": 1}))).is_ok(),
        "schema {schema}"
    );
}

/// Property tests: a fixed case count and no regression files written into the tree.
fn cfg(cases: u32) -> ProptestConfig {
    ProptestConfig {
        failure_persistence: None,
        ..ProptestConfig::with_cases(cases)
    }
}

// ---------------------------------------------------------------------------------------
// Valid manifests, key set
// ---------------------------------------------------------------------------------------

#[test]
fn a_valid_manifest_has_no_problem() {
    assert_eq!(problems(&two(), &ok_manifest()), vec![]);
}

// DESIGN fr-required-sections: with an empty key set, `sections` must be an empty object.
#[test]
fn an_empty_key_set_accepts_only_an_empty_sections_object() {
    assert_eq!(problems(&[], &manifest(&json!({}))), vec![]);
    let got = problems(&[], &manifest(&json!({"x": {}})));
    assert_eq!(keys(&got), vec![("/sections/x", "unknown_section")]);
}

// DESIGN fr-required-sections: a missing key is a problem at `/sections/<key>`.
#[test]
fn a_missing_section_is_reported_at_its_key() {
    let got = problems(&two(), &manifest(&json!({"net": {"port": 80}})));
    assert_eq!(keys(&got), vec![("/sections/db", "missing_section")]);
    let got = problems(&two(), &manifest(&json!({})));
    assert_eq!(
        keys(&got),
        vec![
            ("/sections/db", "missing_section"),
            ("/sections/net", "missing_section")
        ]
    );
}

// DESIGN fr-required-sections: an unknown key is a problem at `/sections/<key>`.
#[test]
fn an_unknown_section_is_reported_at_its_key() {
    let got = problems(
        &two(),
        &manifest(&json!({"net": {"port": 80}, "db": {"name": "abc"}, "zzz": 1, "aaa": {}})),
    );
    assert_eq!(
        keys(&got),
        vec![
            ("/sections/aaa", "unknown_section"),
            ("/sections/zzz", "unknown_section")
        ]
    );
}

// The unknown key comes from the caller, so the location escapes it (RFC 6901).
#[test]
fn an_unknown_section_key_is_pointer_escaped() {
    let got = problems(
        &[],
        &manifest(&json!({"a/b": 1, "c~d": 2, "": 3, "e f": 4})),
    );
    assert_eq!(
        keys(&got),
        vec![
            ("/sections/", "unknown_section"),
            ("/sections/a~1b", "unknown_section"),
            ("/sections/c~0d", "unknown_section"),
            ("/sections/e f", "unknown_section"),
        ]
    );
}

#[test]
fn missing_and_unknown_sections_are_reported_together_with_other_problems() {
    let got = problems(
        &two(),
        &manifest(&json!({"net": {"port": "x"}, "extra": true})),
    );
    assert_eq!(
        keys(&got),
        vec![
            ("/sections/db", "missing_section"),
            ("/sections/extra", "unknown_section"),
            ("/sections/net/port", "type"),
        ]
    );
}

// `sections` that is not an object is one `type` problem at `/sections`.
#[test]
fn sections_that_is_not_an_object_is_a_type_problem() {
    for bad in [json!([]), json!(null), json!("s"), json!(3)] {
        let got = problems(&two(), &manifest(&bad));
        assert_eq!(keys(&got), vec![("/sections", "type")], "sections {bad}");
    }
}

// ---------------------------------------------------------------------------------------
// Envelope schema
// ---------------------------------------------------------------------------------------

// DESIGN fr-manifest-envelope: `application` is an object with exactly `id` and `version`.
// `required` points at the object; an unexpected member is located at the member.
#[test]
fn the_envelope_requires_exactly_id_and_version() {
    let m = |app: Value| json!({"application": app, "sections": {}});
    assert_eq!(
        problems(&[], &m(json!({"id": "a", "version": "1"}))),
        vec![]
    );
    assert_eq!(
        keys(&problems(&[], &m(json!({"id": "a"})))),
        vec![("/application", "required")]
    );
    assert_eq!(
        keys(&problems(&[], &m(json!({"version": "1"})))),
        vec![("/application", "required")]
    );
    assert_eq!(
        keys(&problems(
            &[],
            &m(json!({"id": "a", "version": "1", "x": 1}))
        )),
        vec![("/application/x", "additionalProperties")]
    );
    assert_eq!(
        keys(&problems(&[], &m(json!({"id": 1, "version": "1"})))),
        vec![("/application/id", "type")]
    );
    assert_eq!(
        keys(&problems(&[], &m(json!({"id": "a", "version": null})))),
        vec![("/application/version", "type")]
    );
    assert_eq!(
        keys(&problems(
            &[],
            &m(json!({"id": ["a"], "version": {"v": 1}}))
        )),
        vec![
            ("/application/id", "type"),
            ("/application/version", "type")
        ]
    );
    for bad in [json!("a"), json!(null), json!([]), json!(1)] {
        assert_eq!(
            keys(&problems(&[], &m(bad))),
            vec![("/application", "type")]
        );
    }
}

// Missing `application` or `sections` is a `required` problem at the root; both missing is
// two problems.
#[test]
fn a_manifest_without_envelope_members_has_a_required_problem_per_member() {
    let got = problems(&[], &json!({}));
    assert_eq!(keys(&got), vec![("", "required"), ("", "required")]);
    assert_ne!(
        got[0].message, got[1].message,
        "each names its schema-side member"
    );
    let got = problems(&[], &json!({"application": {"id": "a", "version": "1"}}));
    assert_eq!(keys(&got), vec![("", "required")]);
    let got = problems(&[], &json!({"sections": {}}));
    assert_eq!(keys(&got), vec![("", "required")]);
}

// Another top-level member is located at that member, one problem each.
#[test]
fn a_manifest_with_another_top_level_member_is_a_problem() {
    let mut m = ok_manifest();
    m["extra"] = json!(1);
    assert_eq!(
        keys(&problems(&two(), &m)),
        vec![("/extra", "additionalProperties")]
    );
    m["a/b"] = json!(2);
    assert_eq!(
        keys(&problems(&two(), &m)),
        vec![
            ("/a~1b", "additionalProperties"),
            ("/extra", "additionalProperties")
        ]
    );
}

// A body that is valid JSON but not an object: one `type` problem for the whole body.
#[test]
fn a_manifest_that_is_not_an_object_is_one_type_problem() {
    for bad in [json!([]), json!(null), json!("x"), json!(1), json!(true)] {
        assert_eq!(
            keys(&problems(&two(), &bad)),
            vec![("", "type")],
            "manifest {bad}"
        );
    }
}

// The envelope LIMITS are not the validator's: they are checked at receive time and joined
// by the orchestrator (DESIGN §3.2 step 1 and the end of §3.3), so they are not reported twice.
#[test]
fn the_validator_does_not_check_the_envelope_limits() {
    for id in ["", "a\u{0}b", &"x".repeat(300), "\u{7f}"] {
        let m = json!({"application": {"id": id, "version": id}, "sections": {}});
        assert_eq!(problems(&[], &m), vec![], "id {id:?}");
    }
}

// ---------------------------------------------------------------------------------------
// Every problem, locations, escaping
// ---------------------------------------------------------------------------------------

// DESIGN fr-validate-before-dispatch: every problem with its JSON Pointer location.
#[test]
fn every_problem_is_returned_with_its_location() {
    let m = json!({
        "application": {"id": 7, "extra": 1},
        "sections": {
            "net": {"port": "x", "host": "ABC", "other": true},
            "db": {"name": "a"}
        }
    });
    let got = problems(&two(), &m);
    assert_eq!(
        keys(&got),
        vec![
            ("/application", "required"),
            ("/application/extra", "additionalProperties"),
            ("/application/id", "type"),
            ("/sections/db/name", "minLength"),
            ("/sections/net/host", "pattern"),
            ("/sections/net/other", "additionalProperties"),
            ("/sections/net/port", "type"),
        ]
    );
}

// DESIGN §3.3, §4.3: the problem list is capped. One thousand wrong items give the first
// `MAX_VALIDATION_PROBLEMS` problems and one final `too_many_problems` problem at the root
// location. (The list was uncapped before the P5 review fixes: F-004.)
#[test]
fn problems_are_capped_with_a_final_marker() {
    let schema = json!({"type": "array", "items": {"type": "integer"}});
    let items: Vec<Value> = (0..1000).map(|i| json!(format!("s{i}"))).collect();
    let got = problems(&[sec("list", schema)], &manifest(&json!({"list": items})));
    assert_eq!(got.len(), MAX_VALIDATION_PROBLEMS + 1);
    let (marker, rest) = got.split_last().expect("a marker");
    assert!(rest.iter().all(|p| p.rule == "type"));
    assert_eq!(
        (marker.location.as_str(), marker.rule.as_str()),
        ("", "too_many_problems")
    );
    assert!(!marker.message.is_empty());
    let mut sorted = rest.to_vec();
    sorted.sort_by(|a, b| {
        (&a.location, &a.rule, &a.message).cmp(&(&b.location, &b.rule, &b.message))
    });
    assert_eq!(rest, sorted.as_slice(), "the listed problems stay sorted");
}

// Exactly the cap is not truncated: no marker. One more problem is.
#[test]
fn exactly_the_cap_has_no_marker_and_one_more_has() {
    let schema = json!({"type": "array", "items": {"type": "integer"}});
    for (n, marked) in [
        (MAX_VALIDATION_PROBLEMS - 1, false),
        (MAX_VALIDATION_PROBLEMS, false),
        (MAX_VALIDATION_PROBLEMS + 1, true),
    ] {
        let items: Vec<Value> = (0..n).map(|i| json!(format!("s{i}"))).collect();
        let got = problems(
            &[sec("list", schema.clone())],
            &manifest(&json!({"list": items})),
        );
        assert_eq!(
            got.iter().any(|p| p.rule == "too_many_problems"),
            marked,
            "{n} items"
        );
        assert_eq!(
            got.len(),
            n.min(MAX_VALIDATION_PROBLEMS) + usize::from(marked),
            "{n} items"
        );
    }
}

// F-004: problems that repeat (location, rule, message) are one problem: the same failure
// reached through two paths of the schema is not listed twice and does not count twice.
#[test]
fn a_failure_reached_by_two_schema_paths_is_one_problem() {
    let schema = json!({"$defs": {"i": {"type": "integer"}},
                         "allOf": [{"$ref": "#/$defs/i"}, {"$ref": "#/$defs/i"}, {"type": "integer"}]});
    let got = problems(&[sec("s", schema)], &manifest(&json!({"s": "x"})));
    assert_eq!(keys(&got), vec![("/sections/s", "type")]);
}

// F-004: a problem location is cut at a segment boundary at `LOCATION_MAX_BYTES`: here a member
// name of 5000 bytes (its own segment is longer than the bound, so the location is its parent).
#[test]
fn a_long_location_is_cut_at_a_segment_boundary() {
    let schema = json!({"type": "object", "additionalProperties": false});
    let long = "k".repeat(5000);
    let got = problems(&[sec("s", schema)], &manifest(&json!({"s": {long: 1}})));
    assert_eq!(keys(&got), vec![("/sections/s", "additionalProperties")]);
    // Short segments: the cut keeps whole segments only and stays within the bound.
    let nested = json!({"$defs": {"n": {"type": ["object", "integer"],
                                         "additionalProperties": {"$ref": "#/$defs/n"}}},
                        "$ref": "#/$defs/n"});
    let mut inst = json!("bad");
    for _ in 0..40 {
        inst = wrap(&"s".repeat(100), inst);
    }
    let got = problems(&[sec("s", nested)], &manifest(&json!({"s": inst})));
    assert_eq!(got.len(), 1, "{got:?}");
    let loc = &got[0].location;
    assert!(loc.len() <= LOCATION_MAX_BYTES, "{}", loc.len());
    assert!(loc.starts_with("/sections/s/sss"));
    assert!(
        loc.split('/').skip(3).all(|seg| seg.len() == 100),
        "whole segments only"
    );
    assert_eq!(got[0].rule, "type");
}

// One value breaking two keywords gives two problems at the same location.
#[test]
fn one_value_breaking_two_keywords_has_two_problems() {
    let schema = json!({"type": "string", "minLength": 5, "pattern": "^[0-9]+$"});
    let got = problems(&[sec("s", schema)], &manifest(&json!({"s": "ab"})));
    assert_eq!(
        keys(&got),
        vec![("/sections/s", "minLength"), ("/sections/s", "pattern")]
    );
}

// Array positions are indexes in the pointer.
#[test]
fn array_items_are_located_by_index() {
    let schema = json!({"type": "array", "items": {"type": "integer"}});
    let got = problems(
        &[sec("list", schema)],
        &manifest(&json!({"list": [1, "a", 2, "b"]})),
    );
    assert_eq!(
        keys(&got),
        vec![("/sections/list/1", "type"), ("/sections/list/3", "type")]
    );
}

// RFC 6901: `~` is `~0`, `/` is `~1` in a location, for member names of the instance.
#[test]
fn locations_escape_tilde_and_slash() {
    let schema = json!({
        "type": "object",
        "properties": {
            "a/b": {"type": "integer"},
            "m~n": {"type": "integer"},
            "~1": {"type": "integer"},
            "": {"type": "integer"},
            "é 😀%": {"type": "integer"}
        }
    });
    let m = manifest(&json!({"s": {"a/b": "x", "m~n": "x", "~1": "x", "": "x", "é 😀%": "x"}}));
    let got = problems(&[sec("s", schema)], &m);
    assert_eq!(
        keys(&got),
        vec![
            ("/sections/s/", "type"),
            ("/sections/s/a~1b", "type"),
            ("/sections/s/m~0n", "type"),
            ("/sections/s/~01", "type"),
            ("/sections/s/é 😀%", "type"),
        ]
    );
}

// `required` points at the object; `additionalProperties` gives one problem per unexpected
// member, located at the member, so the caller knows what to delete. Both nest through arrays.
#[test]
fn required_points_at_the_object_and_an_unexpected_member_at_the_member() {
    let schema = json!({
        "type": "array",
        "items": {"type": "object", "required": ["k"], "properties": {"k": {"type": "string"}},
                  "additionalProperties": false}
    });
    let got = problems(
        &[sec("l", schema)],
        &manifest(&json!({"l": [{"k": "a"}, {}, {"k": "a", "z": 1}]})),
    );
    assert_eq!(
        keys(&got),
        vec![
            ("/sections/l/1", "required"),
            ("/sections/l/2/z", "additionalProperties")
        ]
    );
}

// Two unexpected members are two problems, each escaped; the same for `unevaluatedProperties`
// and for the bare `additionalProperties: false` (no sibling `properties`), which the crate
// reports once for the whole object.
#[test]
fn every_unexpected_member_is_its_own_problem() {
    let with_props =
        json!({"type": "object", "properties": {"ok": {}}, "additionalProperties": false});
    let bare = json!({"type": "object", "additionalProperties": false});
    let pattern_only =
        json!({"type": "object", "patternProperties": {"^o": {}}, "additionalProperties": false});
    let unevaluated =
        json!({"type": "object", "properties": {"ok": {}}, "unevaluatedProperties": false});
    let nested = json!({"type": "object", "properties": {"in": {"type": "object", "additionalProperties": false}}});
    let members = json!({"ok": 1, "a/b": 2, "c~d": 3});
    for (name, schema, rule) in [
        ("with properties", with_props, "additionalProperties"),
        ("bare", bare, "additionalProperties"),
        ("pattern only", pattern_only, "additionalProperties"),
        ("unevaluated", unevaluated, "unevaluatedProperties"),
    ] {
        let got = problems(&[sec("s", schema)], &manifest(&json!({"s": members})));
        let want: Vec<(&str, &str)> = if name == "bare" {
            vec![
                ("/sections/s/a~1b", rule),
                ("/sections/s/c~0d", rule),
                ("/sections/s/ok", rule),
            ]
        } else {
            vec![("/sections/s/a~1b", rule), ("/sections/s/c~0d", rule)]
        };
        assert_eq!(keys(&got), want, "{name}");
    }
    let got = problems(
        &[sec("s", nested)],
        &manifest(&json!({"s": {"in": {"x": 1, "y": 2}}})),
    );
    assert_eq!(
        keys(&got),
        vec![
            ("/sections/s/in/x", "additionalProperties"),
            ("/sections/s/in/y", "additionalProperties")
        ]
    );
}

// The member-level problems do not leak: `unknown_section` is the key-set rule only.
#[test]
fn only_the_sections_key_set_uses_the_unknown_section_rule() {
    let got = problems(
        &two(),
        &manifest(&json!({"net": {"port": 80, "nope": 1}, "db": {"name": "abc"}, "zzz": 1})),
    );
    assert_eq!(
        keys(&got),
        vec![
            ("/sections/net/nope", "additionalProperties"),
            ("/sections/zzz", "unknown_section")
        ]
    );
}

// ---------------------------------------------------------------------------------------
// The rule of every keyword
// ---------------------------------------------------------------------------------------

type Row = (
    &'static str,
    Value,
    Value,
    Vec<(&'static str, &'static str)>,
);

/// `(name, section schema, section instance, [(location relative to /sections/s, rule)])`.
#[allow(clippy::too_many_lines)] // a table of data, one row per keyword
fn keyword_table() -> Vec<Row> {
    let o = |name, schema, instance, want| -> Row { (name, schema, instance, want) };
    vec![
        o(
            "type",
            json!({"type": "integer"}),
            json!("x"),
            vec![("", "type")],
        ),
        o(
            "enum",
            json!({"enum": ["a"]}),
            json!("b"),
            vec![("", "enum")],
        ),
        o(
            "const",
            json!({"const": "a"}),
            json!("b"),
            vec![("", "const")],
        ),
        o(
            "minimum",
            json!({"minimum": 5}),
            json!(1),
            vec![("", "minimum")],
        ),
        o(
            "exclusiveMinimum",
            json!({"exclusiveMinimum": 5}),
            json!(5),
            vec![("", "exclusiveMinimum")],
        ),
        o(
            "maximum",
            json!({"maximum": 5}),
            json!(10),
            vec![("", "maximum")],
        ),
        o(
            "exclusiveMaximum",
            json!({"exclusiveMaximum": 5}),
            json!(5),
            vec![("", "exclusiveMaximum")],
        ),
        o(
            "multipleOf",
            json!({"multipleOf": 7}),
            json!(8),
            vec![("", "multipleOf")],
        ),
        o(
            "minLength",
            json!({"minLength": 3}),
            json!("a"),
            vec![("", "minLength")],
        ),
        o(
            "maxLength",
            json!({"maxLength": 1}),
            json!("ab"),
            vec![("", "maxLength")],
        ),
        o(
            "pattern",
            json!({"pattern": "^a$"}),
            json!("b"),
            vec![("", "pattern")],
        ),
        o(
            "minItems",
            json!({"minItems": 2}),
            json!([1]),
            vec![("", "minItems")],
        ),
        o(
            "maxItems",
            json!({"maxItems": 0}),
            json!([1]),
            vec![("", "maxItems")],
        ),
        o(
            "uniqueItems",
            json!({"uniqueItems": true}),
            json!([1, 1]),
            vec![("", "uniqueItems")],
        ),
        o(
            "minProperties",
            json!({"minProperties": 2}),
            json!({"a": 1}),
            vec![("", "minProperties")],
        ),
        o(
            "maxProperties",
            json!({"maxProperties": 0}),
            json!({"a": 1}),
            vec![("", "maxProperties")],
        ),
        o(
            "required",
            json!({"required": ["r"]}),
            json!({}),
            vec![("", "required")],
        ),
        o(
            "dependentRequired",
            json!({"dependentRequired": {"a": ["b"]}}),
            json!({"a": 1}),
            vec![("", "required")],
        ),
        o(
            "anyOf",
            json!({"anyOf": [{"type": "integer"}, {"type": "boolean"}]}),
            json!("x"),
            vec![("", "anyOf")],
        ),
        o(
            "oneOf none",
            json!({"oneOf": [{"type": "integer"}, {"type": "boolean"}]}),
            json!("x"),
            vec![("", "oneOf")],
        ),
        o(
            "oneOf several",
            json!({"oneOf": [{"type": "integer"}, {"type": "number"}]}),
            json!(1),
            vec![("", "oneOf")],
        ),
        o(
            "not",
            json!({"not": {"type": "string"}}),
            json!("x"),
            vec![("", "not")],
        ),
        o(
            "contains",
            json!({"contains": {"type": "integer"}}),
            json!(["x"]),
            vec![("", "contains")],
        ),
        o(
            "minContains",
            json!({"contains": {"type": "integer"}, "minContains": 2}),
            json!([1, "x"]),
            vec![("", "contains")],
        ),
        o(
            "propertyNames",
            json!({"propertyNames": {"maxLength": 1}}),
            json!({"ab": 1}),
            vec![("", "propertyNames")],
        ),
        o(
            "allOf",
            json!({"allOf": [{"type": "integer"}, {"minimum": 5}]}),
            json!(1),
            vec![("", "minimum")],
        ),
        o(
            "if-then",
            json!({"if": {"type": "string"}, "then": {"minLength": 3}}),
            json!("x"),
            vec![("", "minLength")],
        ),
        o(
            "dependentSchemas",
            json!({"dependentSchemas": {"a": {"required": ["b"]}}}),
            json!({"a": 1}),
            vec![("", "required")],
        ),
        o(
            "prefixItems",
            json!({"prefixItems": [{"type": "integer"}]}),
            json!(["x"]),
            vec![("/0", "type")],
        ),
        o(
            "items",
            json!({"items": {"type": "integer"}}),
            json!(["x"]),
            vec![("/0", "type")],
        ),
        o(
            "patternProperties",
            json!({"patternProperties": {"^a": {"type": "integer"}}}),
            json!({"ab": "x"}),
            vec![("/ab", "type")],
        ),
        o(
            "additionalProperties schema",
            json!({"additionalProperties": {"type": "integer"}}),
            json!({"a": "x"}),
            vec![("/a", "type")],
        ),
        o(
            "additionalProperties",
            json!({"properties": {"a": {}}, "additionalProperties": false}),
            json!({"b": 1}),
            vec![("/b", "additionalProperties")],
        ),
        o(
            "unevaluatedProperties",
            json!({"unevaluatedProperties": false}),
            json!({"a": 1}),
            vec![("/a", "unevaluatedProperties")],
        ),
        o(
            "unevaluatedItems",
            json!({"unevaluatedItems": false}),
            json!([1]),
            vec![("", "unevaluatedItems")],
        ),
        o(
            "$ref-routed type",
            json!({"$ref": "#/$defs/d", "$defs": {"d": {"type": "integer"}}}),
            json!("x"),
            vec![("", "type")],
        ),
        o(
            "false schema",
            json!({"properties": {"f": false}}),
            json!({"f": 1}),
            vec![("/f", "false")],
        ),
        o(
            "false items",
            json!({"items": false}),
            json!([1]),
            vec![("/0", "false")],
        ),
    ]
}

// DESIGN §3.3: the rule is the failed keyword, the location the failing value. A wrong
// implementation that reports a neighbouring keyword (`enum` as `const`, the property name for
// a false schema, a dropped combinator) fails here.
#[test]
fn every_keyword_has_its_exact_location_and_rule() {
    for (name, schema, instance, want) in keyword_table() {
        let got = problems(&[sec("s", schema)], &manifest(&json!({"s": instance})));
        let want: Vec<(String, &str)> = want
            .into_iter()
            .map(|(l, r)| (format!("/sections/s{l}"), r))
            .collect();
        let got: Vec<(String, &str)> = got
            .iter()
            .map(|p| (p.location.clone(), p.rule.as_str()))
            .collect();
        assert_eq!(got, want, "keyword {name}");
    }
}

// The table above is the list: dropping a row (and its guard) is caught here.
#[test]
fn the_keyword_table_lists_exactly_the_pinned_rules() {
    let mut rules: Vec<&str> = keyword_table()
        .into_iter()
        .flat_map(|(_, _, _, want)| want.into_iter().map(|(_, r)| r))
        .collect();
    rules.sort_unstable();
    rules.dedup();
    assert_eq!(
        rules,
        vec![
            "additionalProperties",
            "anyOf",
            "const",
            "contains",
            "enum",
            "exclusiveMaximum",
            "exclusiveMinimum",
            "false",
            "maxItems",
            "maxLength",
            "maxProperties",
            "maximum",
            "minItems",
            "minLength",
            "minProperties",
            "minimum",
            "multipleOf",
            "not",
            "oneOf",
            "pattern",
            "propertyNames",
            "required",
            "type",
            "unevaluatedItems",
            "unevaluatedProperties",
            "uniqueItems",
        ]
    );
}

// `format` is an annotation, not an assertion, in 2020-12: a value that breaks it is valid.
#[test]
fn format_is_annotation_only() {
    for (format, value) in [("email", "not an email"), ("regex", "(?=a)"), ("date", "x")] {
        let schema = json!({"type": "string", "format": format});
        assert_eq!(
            problems(&[sec("s", schema)], &manifest(&json!({"s": value}))),
            vec![],
            "format {format}"
        );
    }
}

// ---------------------------------------------------------------------------------------
// Determinism and order
// ---------------------------------------------------------------------------------------

#[test]
fn problems_are_sorted_by_location_rule_and_message() {
    let m = json!({
        "application": {"id": 7},
        "sections": {"net": {"port": "x", "host": "ABC", "o": 1}, "db": {"name": "a"}, "zz": 1}
    });
    let got = problems(&two(), &m);
    let mut sorted = got.clone();
    sorted.sort_by(|a, b| {
        (&a.location, &a.rule, &a.message).cmp(&(&b.location, &b.rule, &b.message))
    });
    assert_eq!(got, sorted);
    assert!(got.len() >= 6);
}

// The result does not depend on the order the sections are passed in, nor on the run.
#[test]
fn the_result_is_deterministic() {
    let m = json!({
        "application": {"id": 7},
        "sections": {"net": {"port": "x", "host": "ABC"}, "zz": 1}
    });
    let a = two();
    let mut b = two();
    b.reverse();
    let first = problems(&a, &m);
    assert_eq!(first, problems(&a, &m));
    assert_eq!(first, problems(&b, &m));
}

// ---------------------------------------------------------------------------------------
// Messages: no instance values
// ---------------------------------------------------------------------------------------

// DESIGN §3.2: "Writes problem messages from the failed keyword and the schema-side
// constraint, never from instance values". A sentinel in every kind of value, an unknown
// section key and unique 12-digit numbers never reach a message.
#[test]
fn messages_are_written_from_the_keyword_and_never_from_instance_values() {
    let schema = json!({
        "type": "object",
        "properties": {
            "t": {"type": "integer"},
            "e": {"enum": ["a", "b"]},
            "c": {"const": "fixed"},
            "cn": {"const": 5},
            "mn": {"minimum": 999_999_999_999_999_u64},
            "p": {"type": "string", "pattern": "^[0-9]+$"},
            "ml": {"type": "string", "maxLength": 3},
            "n": {"type": "number", "maximum": 5},
            "m": {"type": "number", "multipleOf": 7},
            "u": {"type": "array", "uniqueItems": true},
            "any": {"anyOf": [{"type": "integer"}, {"type": "boolean"}]},
            "one": {"oneOf": [{"type": "integer"}, {"type": "number"}]},
            "not": {"not": {"type": "string"}},
            "pn": {"propertyNames": {"maxLength": 3}},
            "f": false
        },
        "required": ["r"],
        "additionalProperties": false
    });
    let m = manifest(&json!({"s": {
        "t": "SENTINEL-T",
        "e": "SENTINEL-E",
        "c": "SENTINEL-C",
        "cn": 123_456_789_012_u64,
        "mn": 234_567_890_123_u64,
        "p": "SENTINEL-P",
        "ml": "SENTINEL-ML",
        "n": 987_654_321_098_u64,
        "m": 876_543_210_987_u64,
        "u": ["SENTINEL-U", "SENTINEL-U"],
        "any": "SENTINEL-ANY",
        "one": 1,
        "not": "SENTINEL-NOT",
        "pn": {"SENTINEL-PN": 1},
        "f": "SENTINEL-F",
        "SENTINEL-KEY": "SENTINEL-EXTRA"
    }, "SENTINEL-UNKNOWN": {"k": "SENTINEL-UNKNOWN-VALUE"}}));
    let got = problems(&[sec("s", schema)], &m);
    assert!(got.len() >= 14, "got {got:?}");
    assert!(got.iter().any(|p| p.rule == "unknown_section"));
    let numbers = [
        "123456789012",
        "234567890123",
        "987654321098",
        "876543210987",
    ];
    for p in &got {
        assert!(!p.message.is_empty(), "{p:?}");
        assert!(!p.message.contains("SENTINEL"), "message leaks: {p:?}");
        assert!(!p.rule.contains("SENTINEL"), "rule leaks: {p:?}");
        for n in numbers {
            assert!(!p.message.contains(n), "message leaks {n}: {p:?}");
            assert!(!p.rule.contains(n), "rule leaks {n}: {p:?}");
        }
    }
}

// The message is written from the failed keyword.
#[test]
fn a_keyword_message_names_its_keyword() {
    let schema = json!({"type": "object", "required": ["r"], "properties": {
        "t": {"type": "integer"}, "m": {"minimum": 5}, "p": {"pattern": "^a$"}}});
    let got = problems(
        &[sec("s", schema)],
        &manifest(&json!({"s": {"t": "x", "m": 1, "p": "b"}})),
    );
    assert_eq!(got.len(), 4);
    for p in got {
        assert!(
            p.message.contains(&p.rule),
            "message {:?} should name rule {:?}",
            p.message,
            p.rule
        );
    }
}

// ---------------------------------------------------------------------------------------
// Section schemas inside the combination
// ---------------------------------------------------------------------------------------

// DESIGN §3.2: references local to the served document resolve inside that resource, and
// never collide across plugins that use the same names.
#[test]
fn local_references_resolve_inside_their_own_section() {
    let a = json!({"type": "object", "properties": {"v": {"$ref": "#/$defs/item"}}, "$defs": {"item": {"type": "integer"}}});
    let b = json!({"type": "object", "properties": {"v": {"$ref": "#/$defs/item"}}, "$defs": {"item": {"type": "string"}}});
    let schemas = [sec("a", a), sec("b", b)];
    assert_eq!(
        problems(
            &schemas,
            &manifest(&json!({"a": {"v": 1}, "b": {"v": "s"}}))
        ),
        vec![]
    );
    let got = problems(
        &schemas,
        &manifest(&json!({"a": {"v": "s"}, "b": {"v": 1}})),
    );
    assert_eq!(
        keys(&got),
        vec![("/sections/a/v", "type"), ("/sections/b/v", "type")]
    );
}

// A served `$id` is replaced; a schema that merely has one is still usable, even when two
// plugins serve the same `$id`.
#[test]
fn a_served_id_is_replaced_and_may_repeat_across_sections() {
    let s = |t: &str| json!({"$id": "https://example.com/same", "$defs": {"d": {"type": t}}, "$ref": "#/$defs/d"});
    let schemas = [sec("a", s("integer")), sec("b", s("string"))];
    assert_eq!(
        problems(&schemas, &manifest(&json!({"a": 1, "b": "x"}))),
        vec![]
    );
    assert_eq!(
        keys(&problems(&schemas, &manifest(&json!({"a": "x", "b": 1})))),
        vec![("/sections/a", "type"), ("/sections/b", "type")]
    );
}

// DESIGN §3.2: a schema that names 2020-12 itself is usable.
#[test]
fn a_schema_naming_draft_2020_12_is_usable() {
    let schema = json!({"$schema": DRAFT_2020_12, "type": "string"});
    assert_eq!(
        problems(&[sec("s", schema)], &manifest(&json!({"s": "x"}))),
        vec![]
    );
}

// A pattern on the linear-time engine returns promptly on an input that would explode a
// backtracking engine. The check runs on a thread so a regression fails instead of hanging.
#[test]
fn a_catastrophic_pattern_runs_in_linear_time() {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let schema = json!({"type": "string", "pattern": "^(a+)+$"});
        let input = format!("{}!", "a".repeat(200_000));
        let got = validate_manifest(&[sec("s", schema)], &manifest(&json!({"s": input})));
        tx.send(got).expect("the receiver waits");
    });
    let got = rx
        .recv_timeout(Duration::from_mins(1))
        .expect("the validation ends within its bound")
        .expect("usable");
    assert_eq!(keys(&got), vec![("/sections/s", "pattern")]);
}

// ---------------------------------------------------------------------------------------
// Combined schema
// ---------------------------------------------------------------------------------------

#[test]
fn the_section_id_is_the_gear_owned_urn() {
    assert_eq!(
        section_schema_id("net"),
        "urn:cf:application-manager:section:net"
    );
    assert_eq!(
        section_schema_id("a.b-c_d"),
        "urn:cf:application-manager:section:a.b-c_d"
    );
}

// DESIGN §3.2 "Composes the combined schema": the envelope schema with a `sections` object
// whose properties are every current section schema under its key, all keys required, no
// other members allowed; each embedded as its own resource under the assigned `$id`. The
// combined schema is defined for usable section schemas only (see `check_schemas`).
#[test]
fn the_combined_schema_composes_every_section_under_its_key() {
    let combined = combined_schema(&two());
    assert_eq!(combined["$schema"], DRAFT_2020_12);
    let sections = &combined["properties"]["sections"];
    assert_eq!(sections["type"], "object");
    assert_eq!(sections["additionalProperties"], json!(false));
    assert_eq!(
        sections["required"],
        json!(["db", "net"]),
        "all keys, in key order"
    );
    // The order of the members of `properties` is not part of the contract: a workspace
    // feature (`preserve_order`) decides it. The key set is.
    let props = sections["properties"].as_object().expect("properties");
    let mut names: Vec<&str> = props.keys().map(String::as_str).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["db", "net"]);
    for s in two() {
        let mut want = s.schema.clone();
        want["$id"] = json!(section_schema_id(&s.key));
        assert_eq!(props[&s.key], want, "section {}", s.key);
    }
    // The envelope: an `application` object with exactly `id` and `version`.
    let app = &combined["properties"]["application"];
    assert_eq!(app["required"], json!(["id", "version"]));
    assert_eq!(app["additionalProperties"], json!(false));
    assert_eq!(app["properties"]["id"]["type"], "string");
    assert_eq!(app["properties"]["version"]["type"], "string");
    assert_eq!(combined["additionalProperties"], json!(false));
    let top = combined["required"].as_array().expect("required");
    assert!(top.contains(&json!("application")) && top.contains(&json!("sections")));
}

// The envelope id and version carry NO length or character keyword in the combined schema:
// their limits are checked at receive time (`EnvelopeFields::limit_problems`, rules
// `minLength`, `max_length`, `control_character`) and joined by the orchestrator, so a
// served schema never reports an empty id a second time under the same location and rule.
#[test]
fn the_combined_envelope_carries_no_length_or_character_keywords() {
    let combined = combined_schema(&two());
    let app = &combined["properties"]["application"]["properties"];
    for member in ["id", "version"] {
        assert_eq!(app[member], json!({"type": "string"}), "member {member}");
    }
}

// A top-level `$id` of the served schema is replaced by the assigned one; the rest is kept.
#[test]
fn a_served_top_level_id_is_replaced_in_the_combination() {
    let served = json!({"$id": "https://example.com/mine", "type": "object", "title": "keep"});
    let combined = combined_schema(&[sec("net", served)]);
    let embedded = &combined["properties"]["sections"]["properties"]["net"];
    assert_eq!(embedded["$id"], "urn:cf:application-manager:section:net");
    assert_eq!(embedded["title"], "keep");
    assert_eq!(embedded["type"], "object");
}

#[test]
fn the_combination_does_not_depend_on_the_order_of_the_sections() {
    let mut rev = two();
    rev.reverse();
    assert_eq!(combined_schema(&two()), combined_schema(&rev));
}

#[test]
fn an_empty_key_set_combines_to_an_empty_sections_object() {
    let combined = combined_schema(&[]);
    let sections = &combined["properties"]["sections"];
    assert_eq!(sections["required"], json!([]));
    assert_eq!(sections["properties"], json!({}));
    assert_eq!(sections["additionalProperties"], json!(false));
}

// DESIGN §1.1 and fr-validate-before-dispatch: discovery serves the schema seed validates
// against. A manifest is valid for `validate_manifest` exactly when a stock 2020-12 validator
// accepts it against `combined_schema`.
#[test]
fn validation_and_the_combined_schema_are_one_rule() {
    let sections = two();
    let combined = combined_schema(&sections);
    let stock = jsonschema::validator_for(&combined).expect("the combined schema compiles");
    let manifests = vec![
        ok_manifest(),
        manifest(&json!({})),
        manifest(&json!({"net": {"port": 80}})),
        manifest(&json!({"net": {"port": 80}, "db": {"name": "abc"}})),
        manifest(&json!({"net": {"port": 0}, "db": {"name": "abc"}})),
        manifest(&json!({"net": {"port": 80}, "db": {"name": "abc"}, "x": {}})),
        manifest(&json!([])),
        json!({"application": {"id": "a"}, "sections": {}}),
        json!({"sections": {}}),
        json!([]),
        json!(null),
    ];
    let mut outcomes = (0, 0);
    for m in manifests {
        let ours = problems(&sections, &m).is_empty();
        assert_eq!(ours, stock.is_valid(&m), "manifest {m}");
        if ours {
            outcomes.0 += 1;
        } else {
            outcomes.1 += 1;
        }
    }
    assert!(
        outcomes.0 >= 2 && outcomes.1 >= 5,
        "both outcomes occur: {outcomes:?}"
    );
}

// A manifest one edit away from a valid one: a leaf replaced, a member dropped or a member
// added, at a chosen place. Valid and invalid outcomes both occur.
#[derive(Debug, Clone)]
enum Edit {
    Replace(usize, Value),
    Drop(usize),
    Add(usize, String, Value),
}

const SPOTS: [&str; 9] = [
    "/application/id",
    "/application/version",
    "/application",
    "/sections/net/port",
    "/sections/net/host",
    "/sections/net",
    "/sections/db/name",
    "/sections/db",
    "/sections",
];

fn arb_edit() -> impl Strategy<Value = Edit> {
    prop_oneof![
        (0..SPOTS.len(), arb_json_value()).prop_map(|(i, v)| Edit::Replace(i, v)),
        (0..SPOTS.len()).prop_map(Edit::Drop),
        (0..SPOTS.len(), "[a-z]{0,3}", arb_json_value()).prop_map(|(i, k, v)| Edit::Add(i, k, v)),
    ]
}

fn mutate(mut m: Value, edits: &[Edit]) -> Value {
    for e in edits {
        match e {
            Edit::Replace(i, v) => {
                if let Some(slot) = m.pointer_mut(SPOTS[*i]) {
                    *slot = v.clone();
                }
            }
            Edit::Drop(i) => {
                let (parent, last) = SPOTS[*i].rsplit_once('/').expect("a pointer");
                if let Some(Value::Object(o)) = m.pointer_mut(parent) {
                    o.remove(last);
                }
            }
            Edit::Add(i, k, v) => {
                if let Some(Value::Object(o)) = m.pointer_mut(SPOTS[*i]) {
                    o.insert(k.clone(), v.clone());
                }
            }
        }
    }
    m
}

fn arb_near_valid_manifest() -> impl Strategy<Value = Value> {
    prop::collection::vec(arb_edit(), 0..3).prop_map(|edits| mutate(ok_manifest(), &edits))
}

proptest! {
    #![proptest_config(cfg(128))]

    // The same agreement for manifests derived from a valid one (so valid and invalid ones both
    // occur), and no panic whatever the manifest.
    #[test]
    fn near_valid_manifests_agree_with_the_combined_schema(m in arb_near_valid_manifest()) {
        let sections = two();
        let stock = jsonschema::validator_for(&combined_schema(&sections)).expect("compiles");
        prop_assert_eq!(problems(&sections, &m).is_empty(), stock.is_valid(&m));
    }

    // Arbitrary values agree too (almost always invalid, but never a panic).
    #[test]
    fn arbitrary_manifests_agree_with_the_combined_schema(v in arb_json_value(), s in arb_json_value()) {
        let sections = two();
        let stock = jsonschema::validator_for(&combined_schema(&sections)).expect("compiles");
        for m in [v.clone(), json!({"application": s, "sections": v}),
                  json!({"application": {"id": "a", "version": "1"}, "sections": {"net": v, "db": s}})] {
            prop_assert_eq!(problems(&sections, &m).is_empty(), stock.is_valid(&m));
        }
    }
}

// The near-valid generator produces both valid and invalid manifests, so the agreement above
// is not a tautology on invalid input alone.
#[test]
fn the_near_valid_generator_produces_valid_and_invalid_manifests() {
    let mut runner = TestRunner::new_with_rng(
        Config {
            failure_persistence: None,
            ..Config::default()
        },
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    let strategy = arb_near_valid_manifest();
    let sections = two();
    let (mut valid, mut invalid) = (0, 0);
    for _ in 0..300 {
        let m = strategy.new_tree(&mut runner).expect("tree").current();
        if problems(&sections, &m).is_empty() {
            valid += 1;
        } else {
            invalid += 1;
        }
    }
    assert!(valid >= 30, "valid {valid}, invalid {invalid}");
    assert!(invalid >= 30, "valid {valid}, invalid {invalid}");
}

// ---------------------------------------------------------------------------------------
// Unusable schemas (invalid schema from a plugin)
// ---------------------------------------------------------------------------------------

fn cause_of(schema: &Value) -> UnusableCause {
    let got = causes(schema);
    assert_eq!(got.len(), 1, "schema {schema}");
    assert_eq!(got[0].key, "s");
    got[0].cause
}

/// `{key: v}`, moving `v` (the `json!` macro would deep-copy it, recursively).
fn wrap(key: &str, v: Value) -> Value {
    Value::Object(std::iter::once((key.to_owned(), v)).collect())
}

/// A schema `n` containers deep (the root counts 1): `{"not": {"not": ... {} ...}}`.
fn deep_not(n: usize) -> Value {
    let mut v = json!({});
    for _ in 1..n {
        v = wrap("not", v);
    }
    v
}

/// A schema of nesting depth `1 + 2 * levels`: `{"properties": {"a": ... {} ...}}`.
fn deep_properties(levels: usize) -> Value {
    let mut v = json!({});
    for _ in 0..levels {
        v = wrap("properties", wrap("a", v));
    }
    v
}

// DESIGN §3.2: a `$schema` that names another dialect, at the root or anywhere in the
// document.
#[test]
fn another_dialect_is_unusable() {
    for dialect in [
        "http://json-schema.org/draft-07/schema#",
        "http://json-schema.org/draft-04/schema#",
        "https://json-schema.org/draft/2019-09/schema",
        "https://example.com/my-dialect",
    ] {
        assert_eq!(
            cause_of(&json!({"$schema": dialect, "type": "object"})),
            UnusableCause::Dialect,
            "{dialect}"
        );
        let nested =
            json!({"$defs": {"d": {"$schema": dialect, "type": "string"}}, "type": "object"});
        assert_eq!(
            cause_of(&nested),
            UnusableCause::Dialect,
            "nested {dialect}"
        );
    }
}

// DESIGN §3.2, §4.8 security (a): a reference that leaves the served document: any `$ref` or
// `$dynamicRef` that does not start with `#`, wherever a schema may stand, also in `$defs`
// nobody uses (a scan of the whole document, not of what the compile happens to resolve).
#[test]
fn a_reference_that_leaves_the_document_is_unusable() {
    for r in [
        "https://example.com/other.json",
        "http://127.0.0.1:9/x",
        "file:///etc/passwd",
        "other.json",
        "urn:other:schema",
        // The 2020-12 metaschema is not served, and another section is not ours to read.
        DRAFT_2020_12,
        "https://json-schema.org/draft/2020-12/meta/validation",
        "urn:cf:application-manager:section:db",
        "urn:cf:application-manager:section:s",
    ] {
        let schema = json!({"type": "object", "properties": {"a": {"$ref": r}}});
        assert_eq!(cause_of(&schema), UnusableCause::ExternalReference, "{r}");
    }
    for schema in [
        json!({"properties": {"a": {"$dynamicRef": "https://example.com/x"}}}),
        json!({"$defs": {"unused": {"$ref": "https://example.com/x"}}}),
        json!({"$defs": {"d": {"$ref": "https://example.com/x#/$defs/y"}}, "properties": {"a": {"$ref": "#/$defs/d"}}}),
        json!({"items": {"$ref": "other.json"}}),
        json!({"allOf": [{"type": "object"}, {"$ref": "other.json"}]}),
        json!({"prefixItems": [{"$ref": "other.json"}]}),
        json!({"patternProperties": {"^a": {"$ref": "other.json"}}}),
        json!({"dependentSchemas": {"a": {"$ref": "other.json"}}}),
        json!({"if": {"$ref": "other.json"}, "then": {}}),
        json!({"not": {"not": {"$ref": "other.json"}}}),
    ] {
        assert_eq!(
            cause_of(&schema),
            UnusableCause::ExternalReference,
            "{schema}"
        );
    }
}

// A `$ref` string that is only DATA is no reference: `const`, `enum`, `default`, `examples`,
// a property named `$ref`, a description.
#[test]
fn reference_strings_in_data_positions_are_not_references() {
    for schema in [
        json!({"const": {"$ref": "https://example.com/x"}}),
        json!({"enum": [{"$ref": "other.json"}]}),
        json!({"default": {"$ref": "http://127.0.0.1:9/x"}}),
        json!({"examples": [{"$ref": "other.json"}]}),
        json!({"properties": {"$ref": {"type": "string"}, "$dynamicRef": {"type": "string"}}}),
        json!({"description": "$ref https://example.com/x", "title": "$ref"}),
        json!({"$comment": "$ref other.json"}),
    ] {
        assert_usable(&schema);
    }
}

// References inside the served document are fine: `#`, a JSON Pointer, an `$anchor`.
#[test]
fn local_references_are_usable() {
    for schema in [
        json!({"type": "object", "properties": {"c": {"$ref": "#"}}}),
        json!({"$ref": "#/$defs/d", "$defs": {"d": {"type": "string"}}}),
        json!({"$defs": {"a": {"$anchor": "x", "type": "string"}}, "$ref": "#x"}),
        json!({"$defs": {"n": {"type": ["integer", "array"], "items": {"$ref": "#/$defs/n"}}}, "$ref": "#/$defs/n"}),
        // A cycle that stays at one instance level is only refused together with the
        // unevaluated keywords (the one combination that overflows); alone it terminates.
        json!({"$ref": "#"}),
        json!({"allOf": [{"$ref": "#"}]}),
        // A recursive schema is usable: the cycle goes through an instance level.
        json!({"properties": {"a": {"$ref": "#"}}, "unevaluatedProperties": false}),
        json!({"items": {"$ref": "#"}, "unevaluatedItems": false}),
    ] {
        assert_usable(&schema);
    }
}

// DESIGN §3.2: a `$ref` that relies on the served `$id` (an absolute URI built from it)
// becomes an external reference once the gear replaces the `$id`.
#[test]
fn a_reference_relying_on_the_served_id_is_unusable() {
    let schema = json!({
        "$id": "https://example.com/section",
        "$defs": {"item": {"type": "string"}},
        "type": "object",
        "properties": {"a": {"$ref": "https://example.com/section#/$defs/item"}}
    });
    assert_eq!(cause_of(&schema), UnusableCause::ExternalReference);
    let relative = json!({
        "$id": "https://example.com/section",
        "$defs": {"item": {"type": "string"}},
        "properties": {"a": {"$ref": "section#/$defs/item"}}
    });
    assert_eq!(cause_of(&relative), UnusableCause::ExternalReference);
}

/// Counts every call; refuses like the gear's own retriever.
struct Counting(Arc<AtomicUsize>);

impl Retrieve for Counting {
    fn retrieve(&self, _: &Uri<String>) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err("refused".into())
    }
}

// DESIGN §4.8 security (a): no network access, no file access. Defence in depth: the scan
// refuses every reference that leaves the document before the compile could ask the
// retriever; a counting retriever injected through the seam is never called, whatever the
// reference looks like, and a listener nobody may connect to, and a file that holds a valid
// schema, are never touched.
#[test]
fn no_network_and_no_file_is_touched_for_a_reference() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let port = listener.local_addr().expect("addr").port();
    let file = tempfile::NamedTempFile::new().expect("tempfile");
    std::fs::write(file.path(), r#"{"type":"string"}"#).expect("write");
    let file_uri = format!("file://{}", file.path().display());
    let calls = Arc::new(AtomicUsize::new(0));
    let mut refs = vec![format!("http://127.0.0.1:{port}/schema.json"), file_uri];
    refs.extend(
        [
            "https://example.com/x",
            "other.json",
            "urn:other:x",
            DRAFT_2020_12,
        ]
        .map(str::to_owned),
    );
    for r in refs {
        let schema = json!({"properties": {"a": {"$ref": r}}});
        let counter = Arc::clone(&calls);
        let got = check_schemas_with(&[sec("s", schema.clone())], || {
            Box::new(Counting(counter.clone()))
        });
        assert_eq!(
            got,
            Err(vec![UnusableSchema {
                key: "s".to_owned(),
                cause: UnusableCause::ExternalReference
            }])
        );
        assert_eq!(cause_of(&schema), UnusableCause::ExternalReference);
    }
    // Schemas whose own `$id` machinery could tempt a compile into fetching something.
    for schema in [
        json!({"$id": "https://example.com/a", "$ref": "#/$defs/x", "$defs": {"x": {}}}),
        json!({"$defs": {"a": {"$id": "https://example.com/a", "$ref": "#/$defs/b"}}, "$ref": "#/$defs/a"}),
        json!({"$defs": {"a": {"$id": "other.json", "type": "string"}}, "properties": {"x": {"$ref": "#/$defs/a"}}}),
    ] {
        let counter = Arc::clone(&calls);
        check_schemas_with(&[sec("s", schema)], || Box::new(Counting(counter.clone()))).ok();
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the retriever was asked for a document"
    );
    assert!(
        matches!(listener.accept(), Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock),
        "no connection may have been made"
    );
}

// DESIGN §3.2, §4.8 security (b): the linear-time engine has no lookaround and no
// backreference; a pattern beyond the compile limits, or no pattern at all, is unusable.
#[test]
fn a_pattern_the_linear_time_engine_cannot_compile_is_unusable() {
    for pattern in [
        "^(?=a)",
        "(?!a)b",
        "(?<=a)b",
        "(a)\\1",
        "[",
        "(",
        "a{",
        "((a{100}){100}){100}",
    ] {
        let schema =
            json!({"type": "object", "properties": {"a": {"type": "string", "pattern": pattern}}});
        assert_eq!(
            cause_of(&schema),
            UnusableCause::Pattern,
            "pattern {pattern}"
        );
    }
    let pp = json!({"type": "object", "patternProperties": {"(?=x)": {"type": "string"}}});
    assert_eq!(cause_of(&pp), UnusableCause::Pattern);
    let pp_big = json!({"type": "object", "patternProperties": {"((a{100}){100}){100}": {}}});
    assert_eq!(cause_of(&pp_big), UnusableCause::Pattern);
    // Every pattern is compiled, also in `$defs` nobody uses and in nested schemas.
    let unused = json!({"$defs": {"d": {"pattern": "(?=a)"}}});
    assert_eq!(cause_of(&unused), UnusableCause::Pattern);
    let nested = json!({"items": {"allOf": [{"properties": {"x": {"pattern": "["}}}]}});
    assert_eq!(cause_of(&nested), UnusableCause::Pattern);
    // A property called `pattern` is no pattern.
    assert_usable(&json!({"properties": {"pattern": {"type": "string"}}}));
}

/// A pattern of `n` repetitions of one letter: its compiled size grows linearly with `n`
/// (about 32 bytes each in the regex engine of the workspace, so `n` ~ 32768 is 1 MiB).
fn repeated(n: usize) -> Value {
    json!({"type": "string", "pattern": format!("^a{{{n}}}$")})
}

// The compile size limit of one pattern is `SCHEMA_REGEX_SIZE_LIMIT` (1 MiB, pinned in
// `limits_tests`): a pattern at about a quarter and three quarters of it compiles, one at
// 1.5 times and at 4 times does not. This kills a limit that is far too small (1 KiB, 512 KiB)
// and one that is too large (2 MiB, 8 MiB, 10 MiB, none). The lazy-DFA limit
// (`SCHEMA_REGEX_DFA_SIZE_LIMIT`) is pinned as a constant only: it bounds a runtime cache
// and no stable pattern calibrates it.
#[test]
fn the_pattern_size_limit_is_the_documented_one() {
    const PER_REPEAT: usize = 32;
    let near = SCHEMA_REGEX_SIZE_LIMIT.div_euclid(PER_REPEAT);
    for n in [near.div_euclid(4), (near * 3).div_euclid(4)] {
        assert_usable(&repeated(n));
    }
    for n in [(near * 3).div_euclid(2), near * 4] {
        assert_eq!(
            cause_of(&repeated(n)),
            UnusableCause::Pattern,
            "{n} repeats"
        );
    }
    let pp = json!({"patternProperties": {format!("^a{{{}}}$", near * 4): {}}});
    assert_eq!(cause_of(&pp), UnusableCause::Pattern);
    assert_usable(&json!({"patternProperties": {format!("^a{{{}}}$", near.div_euclid(4)): {}}}));
}

// DESIGN §3.2: an integer beyond 2^53 in magnitude, whether it parses as an integer or as a
// float with an integral value (F-010: the rule is monotone: 2^53 + 1 and 2^64 are both
// refused). 2^53 itself and any float with a fraction are fine. The number may sit anywhere
// in the schema.
#[test]
fn an_integer_beyond_two_to_the_53_is_unusable() {
    let beyond = 9_007_199_254_740_993_u64; // 2^53 + 1
    for schema in [
        json!({"type": "integer", "maximum": beyond}),
        json!({"type": "integer", "minimum": -9_007_199_254_740_993_i64}),
        json!({"const": u64::MAX}),
        json!({"enum": [1, [2, {"x": beyond}]]}),
        json!({"examples": [beyond]}),
        json!({"properties": {"a": {"default": i64::MIN}}}),
        json!({"$defs": {"d": {"maximum": beyond}}}),
        // 2^64 parses as a float with an integral value, as does any large integral float.
        serde_json::from_str("{\"maximum\": 18446744073709551616}").expect("json"),
        json!({"maximum": 1.0e300}),
        json!({"minimum": -1.0e19}),
    ] {
        assert_eq!(cause_of(&schema), UnusableCause::Number, "schema {schema}");
    }
    for schema in [
        json!({"maximum": 9_007_199_254_740_992_u64}),
        json!({"minimum": -9_007_199_254_740_992_i64}),
        json!({"maximum": 9_007_199_254_740_992.0}),
        json!({"maximum": 0.1, "minimum": -1.5e-300}),
        json!({"maximum": 4_503_599_627_370_496.5}),
    ] {
        assert_usable(&schema);
    }
}

/// A schema `{"description": "<pad>"}` whose compact serialization is exactly `len` bytes.
fn schema_of_compact_len(len: usize) -> Value {
    let overhead = serde_json::to_string(&json!({"description": ""}))
        .expect("serialize")
        .len();
    let schema = json!({"description": "x".repeat(len - overhead)});
    assert_eq!(
        serde_json::to_string(&schema).expect("serialize").len(),
        len
    );
    schema
}

// DESIGN §3.2, §4.8 security (a): a schema larger than `SERVED_SCHEMA_MAX_BYTES` (compact
// serialization) is refused before it is compiled; exactly the bound is fine.
#[test]
fn the_served_schema_bound_is_exact() {
    assert_usable(&schema_of_compact_len(SERVED_SCHEMA_MAX_BYTES));
    assert_eq!(
        cause_of(&schema_of_compact_len(SERVED_SCHEMA_MAX_BYTES + 1)),
        UnusableCause::TooLarge
    );
    assert_eq!(
        cause_of(&json!({"type": "string", "description": "x".repeat(SERVED_SCHEMA_MAX_BYTES)})),
        UnusableCause::TooLarge
    );
}

// The bound counts the compact serialization (what a plugin sends and the gear stores), not
// whitespace: a schema whose pretty-printed text is over the bound but whose compact form is
// under it is usable.
#[test]
fn the_served_schema_bound_counts_compact_bytes() {
    let props: serde_json::Map<String, Value> = (0..25_000)
        .map(|i| (format!("p{i}"), json!({"type": "integer"})))
        .collect();
    let schema = json!({"type": "object", "properties": props});
    let compact = serde_json::to_string(&schema).expect("serialize");
    let pretty = serde_json::to_string_pretty(&schema).expect("serialize");
    assert!(
        compact.len() < SERVED_SCHEMA_MAX_BYTES,
        "compact {}",
        compact.len()
    );
    assert!(
        pretty.len() > SERVED_SCHEMA_MAX_BYTES,
        "pretty {}",
        pretty.len()
    );
    let parsed: Value = serde_json::from_str(&pretty).expect("parse");
    assert_usable(&parsed);
}

// DESIGN §3.2 "any other compile failure".
#[test]
fn any_other_compile_failure_is_unusable() {
    for schema in [
        json!([]),
        json!("string"),
        json!(3),
        json!(null),
        json!({"type": "nonsense"}),
        json!({"minimum": "x"}),
        json!({"properties": []}),
        json!({"$schema": 5}),
        // A local reference that points nowhere, or at an anchor nobody declares.
        json!({"properties": {"a": {"$ref": "#/$defs/missing"}}}),
        json!({"$ref": "#nope"}),
    ] {
        assert_eq!(cause_of(&schema), UnusableCause::Compile, "schema {schema}");
    }
}

// DESIGN §4.3 "A served schema is hostile": nesting beyond `SCHEMA_MAX_DEPTH` is unusable
// (checked without recursion before anything recursive runs); exactly the bound is fine. The
// depth counts every array and object of the served document, data included.
#[test]
fn schema_nesting_beyond_the_bound_is_too_deep() {
    assert_usable(&deep_not(SCHEMA_MAX_DEPTH));
    assert_eq!(
        cause_of(&deep_not(SCHEMA_MAX_DEPTH + 1)),
        UnusableCause::TooDeep
    );
    assert_eq!(
        cause_of(&deep_not(SCHEMA_MAX_DEPTH + 2)),
        UnusableCause::TooDeep
    );
    // The same through `properties`: two JSON levels per schema level.
    let levels = (SCHEMA_MAX_DEPTH - 1).div_euclid(2);
    assert_usable(&deep_properties(levels));
    assert_eq!(
        cause_of(&deep_properties(levels + 1)),
        UnusableCause::TooDeep
    );
    // And through data: an `enum` member that nests arrays.
    // (the root and the `enum` array take two levels)
    for (depth, usable) in [(SCHEMA_MAX_DEPTH - 2, true), (SCHEMA_MAX_DEPTH - 1, false)] {
        let data: Value =
            serde_json::from_slice(&crate::test_support::manifest_gen::nested_arrays(depth))
                .expect("nested data");
        let schema = json!({"enum": [data]});
        if usable {
            assert_usable(&schema);
        } else {
            assert_eq!(cause_of(&schema), UnusableCause::TooDeep);
        }
    }
}

// An in-place `$ref` cycle (no instance level in between) together with unevaluatedItems or
// unevaluatedProperties sends the validation library into unbounded recursion (a stack
// overflow that cannot be caught): such a schema is unusable (`Compile`), without being run.
// These schemas are only ever checked in the child process of
// `a_hostile_served_schema_is_refused_without_aborting_the_process`, never in this process.
fn hostile_cycles() -> Vec<Value> {
    vec![
        json!({"$ref": "#", "unevaluatedItems": false}),
        json!({"allOf": [{"$ref": "#"}], "unevaluatedItems": false}),
        json!({"anyOf": [{"$ref": "#"}], "unevaluatedProperties": false}),
        json!({"$defs": {"a": {"anyOf": [{"$ref": "#/$defs/b"}], "unevaluatedItems": false},
                         "b": {"allOf": [{"$ref": "#/$defs/a"}]}}, "$ref": "#/$defs/a"}),
        json!({"$defs": {"a": {"$ref": "#/$defs/b"}, "b": {"$ref": "#/$defs/a"}},
               "$ref": "#/$defs/a", "unevaluatedProperties": false}),
        json!({"$defs": {"a": {"$anchor": "x", "allOf": [{"$ref": "#x"}]}},
               "$ref": "#x", "unevaluatedItems": false}),
    ]
}

// Several defects: the cause is the first of the documented precedence, TooLarge, TooDeep,
// Number, Dialect, ExternalReference, Pattern, Compile. The same schema without its first
// defect has the next cause.
#[test]
fn the_cause_of_a_schema_with_several_defects_follows_the_precedence() {
    let defects: [(UnusableCause, &str, Value); 7] = [
        (
            UnusableCause::TooLarge,
            "description",
            json!("x".repeat(SERVED_SCHEMA_MAX_BYTES)),
        ),
        (
            UnusableCause::TooDeep,
            "not",
            deep_not(SCHEMA_MAX_DEPTH + 5),
        ),
        (
            UnusableCause::Number,
            "maximum",
            json!(9_007_199_254_740_993_u64),
        ),
        (
            UnusableCause::Dialect,
            "$schema",
            json!("http://json-schema.org/draft-07/schema#"),
        ),
        (
            UnusableCause::ExternalReference,
            "properties",
            json!({"a": {"$ref": "https://example.com/x"}}),
        ),
        (UnusableCause::Pattern, "pattern", json!("(")),
        (UnusableCause::Compile, "type", json!("nonsense")),
    ];
    for first in 0..defects.len() {
        let schema: serde_json::Map<String, Value> = defects[first..]
            .iter()
            .map(|(_, key, value)| ((*key).to_owned(), value.clone()))
            .collect();
        assert_eq!(
            cause_of(&Value::Object(schema)),
            defects[first].0,
            "defects from #{first}"
        );
    }
}

// ADR 0003 validate-all-then-dispatch: every unusable key is listed, in key order, and
// nothing is validated: not even a manifest that is itself invalid.
#[test]
fn every_unusable_key_is_listed_in_key_order_and_no_problem_is_returned() {
    let sections = [
        sec("mid", json!({"type": "nonsense"})),
        sec(
            "zeta",
            json!({"$schema": "http://json-schema.org/draft-07/schema#"}),
        ),
        sec("good", net_schema()),
        sec(
            "alpha",
            json!({"properties": {"a": {"$ref": "https://example.com/x"}}}),
        ),
        sec("deep", deep_not(SCHEMA_MAX_DEPTH + 1)),
    ];
    let invalid = manifest(&json!({"x": 1}));
    let got = validate_manifest(&sections, &invalid).expect_err("unusable");
    let want = vec![
        UnusableSchema {
            key: "alpha".to_owned(),
            cause: UnusableCause::ExternalReference,
        },
        UnusableSchema {
            key: "deep".to_owned(),
            cause: UnusableCause::TooDeep,
        },
        UnusableSchema {
            key: "mid".to_owned(),
            cause: UnusableCause::Compile,
        },
        UnusableSchema {
            key: "zeta".to_owned(),
            cause: UnusableCause::Dialect,
        },
    ];
    assert_eq!(got, want);
    assert_eq!(check_schemas(&sections), Err(want), "no manifest needed");
}

// DESIGN §3.2: discovery reports `unusable` with no manifest at hand and follows the rule
// seed validation follows: `check_schemas` is Ok exactly when `validate_manifest` can run.
#[test]
fn check_schemas_needs_no_manifest_and_agrees_with_validate_manifest() {
    assert_eq!(check_schemas(&[]), Ok(()));
    assert_eq!(check_schemas(&two()), Ok(()));
    assert!(validate_manifest(&two(), &ok_manifest()).is_ok());
    for schema in [
        json!({"type": "nonsense"}),
        json!({"properties": {"a": {"$ref": "other.json"}}}),
        json!({"pattern": "("}),
        json!({"maximum": 9_007_199_254_740_993_u64}),
        deep_not(SCHEMA_MAX_DEPTH + 1),
    ] {
        let mut sections = two();
        sections.push(sec("bad", schema));
        let from_check = check_schemas(&sections).expect_err("unusable");
        assert_eq!(from_check.len(), 1);
        assert_eq!(from_check[0].key, "bad");
        for m in [ok_manifest(), json!(null), manifest(&json!({"x": 1}))] {
            assert_eq!(validate_manifest(&sections, &m), Err(from_check.clone()));
        }
    }
}

// The unusable result does not depend on the manifest.
#[test]
fn unusability_does_not_depend_on_the_manifest() {
    let sections = [sec("s", json!({"type": "nonsense"}))];
    for m in [ok_manifest(), json!(null), manifest(&json!({}))] {
        assert_eq!(
            validate_manifest(&sections, &m)
                .expect_err("unusable")
                .len(),
            1
        );
    }
}

// ---------------------------------------------------------------------------------------
// Hostile schemas must not abort the process
// ---------------------------------------------------------------------------------------

const HOSTILE_ENV: &str = "AM_HOSTILE_SCHEMA_CASE";
const HOSTILE_OK: &str = "HOSTILE-SCHEMA-OK";
const HOSTILE_TIMEOUT: Duration = Duration::from_mins(2);
const HOSTILE_CASES: [&str; 55] = [
    "not-20000",
    "properties-1000",
    "items-5000",
    "depth-65",
    "cycle-0",
    "cycle-1",
    "cycle-2",
    "cycle-3",
    "cycle-4",
    "cycle-5",
    // F-001: references the scan must resolve as the library does, or refuse.
    "pct-cycle-0",
    "pct-cycle-1",
    "pct-cycle-2",
    "nested-id-0",
    "nested-id-1",
    "nested-id-2",
    "dyn-anchor",
    "anchor-ref-unev",
    "unresolved-ref-unev",
    "ref-into-data",
    "legit-recursive",
    "legit-escapes",
    // F-002: reference chains and DAGs.
    "chain-50",
    "chain-62",
    "chain-63",
    "chain-65",
    "chain-500",
    "chain-600",
    "chain-5000",
    "chain-30000",
    "chain-600-unev",
    "chain-5000-dyn",
    "dag-10",
    "dag-11",
    "dag-12",
    "dag-13",
    "dag-28",
    "dag-40",
    "dag-24-false",
    "dag-40-unev",
    "wide-10000",
    "shared-def-1000",
    // F-004: bounded answers.
    "instance-exp-12",
    "instance-exp-14",
    "array-500k",
    "required-50k",
    "members-50k",
    "long-keys-deep",
    // F-005: patterns.
    "patterns-256",
    "patterns-257",
    "patterns-bytes",
    "pattern-20k-copies",
    "pattern-20k-copies-heavy",
    "patterns-400-heavy",
    "pattern-property-copies",
];

/// What the child expects of a case.
#[derive(Clone, Copy)]
enum Want {
    Unusable(UnusableCause),
    /// Usable; `truncated` tells whether the problem list is cut.
    Usable {
        truncated: bool,
    },
}

struct Hostile {
    schema: Value,
    manifest: Value,
    want: Want,
}

/// `$defs` entries `0..n` chained by `$ref`, the last one `leaf`; the root references the first.
fn ref_chain(links: usize, leaf: &Value, root_extra: Option<(&str, Value)>) -> Value {
    let mut defs = serde_json::Map::new();
    for i in 0..links {
        let next = if i + 1 < links {
            json!({"$ref": format!("#/$defs/{}", i + 1)})
        } else {
            leaf.clone()
        };
        defs.insert(i.to_string(), next);
    }
    let mut root = json!({"$defs": Value::Object(defs), "$ref": "#/$defs/0"});
    if let Some((k, v)) = root_extra {
        root[k] = v;
    }
    root
}

/// A reference DAG: `$defs` `0..=levels`, each level pointing at the next one twice, so the last
/// one is reached by `2^levels` paths.
fn ref_dag(levels: usize, leaf: Value, root_extra: Option<(&str, Value)>) -> Value {
    let mut defs = serde_json::Map::new();
    for i in 0..levels {
        let to = format!("#/$defs/{}", i + 1);
        defs.insert(
            i.to_string(),
            json!({"allOf": [{"$ref": to.clone()}, {"$ref": to}]}),
        );
    }
    defs.insert(levels.to_string(), leaf);
    let mut root = json!({"$defs": Value::Object(defs), "$ref": "#/$defs/0"});
    if let Some((k, v)) = root_extra {
        root[k] = v;
    }
    root
}

fn nested_c(depth: usize, leaf: Value) -> Value {
    let mut v = leaf;
    for _ in 0..depth {
        v = wrap("c", v);
    }
    v
}

fn pattern_props(n: usize, pattern: impl Fn(usize) -> String) -> Value {
    let props: serde_json::Map<String, Value> = (0..n)
        .map(|i| {
            (
                format!("p{i}"),
                json!({"type": "string", "pattern": pattern(i)}),
            )
        })
        .collect();
    json!({"type": "object", "properties": props})
}

fn instance_exp(levels: usize, truncated: bool) -> Hostile {
    Hostile {
        schema: json!({"$defs": {"n": {"type": ["object", "null"],
            "properties": {"c": {"$ref": "#/$defs/n"}},
            "allOf": [{"properties": {"c": {"$ref": "#/$defs/n"}}}]}},
            "$ref": "#/$defs/n"}),
        manifest: manifest(&json!({"s": nested_c(levels, json!(5))})),
        want: Want::Usable { truncated },
    }
}

#[allow(clippy::too_many_lines)]
fn hostile_case(name: &str) -> Hostile {
    let items = |n: usize| {
        let mut v = json!({"type": "integer"});
        for _ in 0..n {
            v = wrap("items", v);
        }
        v
    };
    let cycles = hostile_cycles();
    let section = |v: Value| manifest(&json!({"s": v}));
    let unusable = |schema: Value, cause: UnusableCause| Hostile {
        schema,
        manifest: ok_manifest(),
        want: Want::Unusable(cause),
    };
    let usable = |schema: Value| Hostile {
        schema,
        manifest: section(json!({})),
        want: Want::Usable { truncated: false },
    };
    let unev = || Some(("unevaluatedProperties", json!(false)));
    let obj = || json!({"type": "object"});
    match name {
        "not-20000" => unusable(deep_not(20_000), UnusableCause::TooDeep),
        "properties-1000" => unusable(deep_properties(1000), UnusableCause::TooDeep),
        "items-5000" => unusable(items(5000), UnusableCause::TooDeep),
        "depth-65" => unusable(deep_not(SCHEMA_MAX_DEPTH + 1), UnusableCause::TooDeep),
        // F-001: a percent-encoded fragment is decoded before the lookup (RFC 3986, then RFC 6901).
        "pct-cycle-0" => unusable(
            json!({"$defs": {"a": {"$ref": "#/%24defs/a"}}, "$ref": "#/$defs/a",
                   "unevaluatedProperties": false}),
            UnusableCause::Compile,
        ),
        "pct-cycle-1" => unusable(
            json!({"$defs": {"a b": {"$ref": "#/$defs/a%20b"}}, "$ref": "#/$defs/a%20b",
                   "unevaluatedProperties": false}),
            UnusableCause::Compile,
        ),
        "pct-cycle-2" => unusable(
            json!({"$defs": {"a": {"allOf": [{"$ref": "#/%24defs/a"}]}},
                   "$ref": "#/%24defs/a", "unevaluatedItems": false}),
            UnusableCause::Compile,
        ),
        // A nested `$id` starts a new resource: `#` and the pointers mean something else there.
        "nested-id-0" => unusable(
            json!({"type": "object", "properties": {"p": {"$id": "urn:x:sub",
                   "allOf": [{"$ref": "#"}], "unevaluatedProperties": false}}}),
            UnusableCause::Compile,
        ),
        "nested-id-1" => unusable(
            json!({"type": "object", "properties": {"p": {"$id": "urn:x:sub",
                   "$defs": {"b": {"$ref": "#/$defs/b"}}, "$ref": "#/$defs/b",
                   "unevaluatedProperties": false}}}),
            UnusableCause::Compile,
        ),
        "nested-id-2" => unusable(
            json!({"properties": {"p": {"$id": "urn:x:sub", "type": "object"}}}),
            UnusableCause::Compile,
        ),
        "dyn-anchor" => unusable(
            json!({"$dynamicAnchor": "n", "allOf": [{"$dynamicRef": "#n"}],
                   "unevaluatedProperties": false}),
            UnusableCause::Compile,
        ),
        "anchor-ref-unev" => unusable(
            json!({"$defs": {"a": {"$anchor": "x", "type": "object"}}, "$ref": "#x",
                   "unevaluatedProperties": false}),
            UnusableCause::Compile,
        ),
        "unresolved-ref-unev" => unusable(
            json!({"$ref": "#/nope", "unevaluatedProperties": false}),
            UnusableCause::Compile,
        ),
        "ref-into-data" => unusable(
            json!({"const": {"x": {"$ref": "#"}}, "$ref": "#/const/x",
                   "unevaluatedProperties": false}),
            UnusableCause::Compile,
        ),
        // Legitimate recursion stays usable: instance-consuming recursion, `~0`/`~1` escapes and
        // percent-encoding in a fragment.
        "legit-recursive" => usable(json!({
            "type": "object", "properties": {"c": {"$ref": "#"}},
            "unevaluatedProperties": false, "items": {"$ref": "#"},
            "$defs": {"t": {"type": "object", "properties": {"n": {"$ref": "#/$defs/t"}},
                            "unevaluatedProperties": false}},
            "allOf": [{"$ref": "#/$defs/t"}]})),
        "legit-escapes" => usable(json!({
            "$defs": {"a/b": {"type": "object", "properties": {"n": {"$ref": "#/$defs/a~1b"}},
                              "unevaluatedProperties": false},
                      "c~d": {"type": "object"}, "e f": {"type": "object"}},
            "$ref": "#/$defs/a~1b",
            "allOf": [{"$ref": "#/$defs/c~0d"}, {"$ref": "#/$defs/e%20f"}]})),
        // F-002.
        "chain-50" => usable(ref_chain(50, &obj(), None)),
        // The root reaches the first definition through `$defs` (2), then one hop per link: a
        // chain of `n` links has the effective depth `n + 2`.
        "chain-62" => usable(ref_chain(62, &obj(), None)),
        "chain-63" => unusable(ref_chain(63, &obj(), None), UnusableCause::TooDeep),
        "chain-65" => unusable(ref_chain(65, &obj(), None), UnusableCause::TooDeep),
        "chain-500" => unusable(ref_chain(500, &obj(), None), UnusableCause::TooDeep),
        "chain-5000" => unusable(ref_chain(5000, &obj(), None), UnusableCause::TooDeep),
        "chain-600" => unusable(ref_chain(600, &obj(), None), UnusableCause::TooDeep),
        "chain-30000" => unusable(ref_chain(30_000, &obj(), None), UnusableCause::TooDeep),
        "chain-600-unev" => unusable(ref_chain(600, &obj(), unev()), UnusableCause::TooDeep),
        "chain-5000-dyn" => {
            let mut v = ref_chain(5000, &obj(), None);
            v["$dynamicRef"] = json!("#/$defs/0");
            unusable(v, UnusableCause::TooDeep)
        }
        // Level `k` is reached by `3 * 2^k - 1` paths (two references per level above it, and
        // the `$defs` place of every level): `k = 10` is within 4096, `k = 11` is not.
        "dag-10" => usable(ref_dag(10, obj(), None)),
        "dag-11" => unusable(ref_dag(11, obj(), None), UnusableCause::TooDeep),
        "dag-12" => unusable(ref_dag(12, obj(), None), UnusableCause::TooDeep),
        "dag-13" => unusable(ref_dag(13, obj(), None), UnusableCause::TooDeep),
        "dag-28" => unusable(ref_dag(28, obj(), None), UnusableCause::TooDeep),
        "dag-40" => unusable(ref_dag(40, obj(), None), UnusableCause::TooDeep),
        "dag-24-false" => unusable(ref_dag(24, json!(false), None), UnusableCause::TooDeep),
        "dag-40-unev" => unusable(ref_dag(40, obj(), unev()), UnusableCause::TooDeep),
        // A wide schema, and one definition used by a thousand properties, are not fan-out.
        "wide-10000" => usable({
            let props: serde_json::Map<String, Value> = (0..10_000)
                .map(|i| (format!("p{i}"), json!({"type": "string", "maxLength": 5})))
                .collect();
            json!({"type": "object", "properties": props})
        }),
        "shared-def-1000" => usable({
            let props: serde_json::Map<String, Value> = (0..1000)
                .map(|i| (format!("p{i}"), json!({"$ref": "#/$defs/d"})))
                .collect();
            json!({"$defs": {"d": {"type": "string"}}, "type": "object", "properties": props})
        }),
        // F-004. The recursion is reached by two applicators per instance level: `2^levels`
        // errors, all at one location. The schema is usable (the doubling is driven by the
        // instance, see the residual risk in the module docs); the answer stays one problem, and
        // the enumeration stops after `MAX_VALIDATION_ERRORS_SCANNED` errors.
        "instance-exp-12" => instance_exp(12, false),
        "instance-exp-14" => instance_exp(14, true),
        "array-500k" => Hostile {
            schema: json!({"type": "array", "items": {"type": "string"}}),
            manifest: section(Value::Array((0..500_000).map(|i| json!(i)).collect())),
            want: Want::Usable { truncated: true },
        },
        "required-50k" => Hostile {
            schema: json!({"type": "object", "required": (0..50_000).map(|i| format!("r{i}")).collect::<Vec<_>>()}),
            manifest: section(json!({})),
            want: Want::Usable { truncated: true },
        },
        "members-50k" => Hostile {
            schema: json!({"type": "object", "additionalProperties": false}),
            manifest: section(Value::Object(
                (0..50_000).map(|i| (format!("m{i}"), json!(0))).collect(),
            )),
            want: Want::Usable { truncated: true },
        },
        "long-keys-deep" => Hostile {
            schema: json!({"$defs": {"n": {"type": ["object", "integer"],
                "additionalProperties": {"$ref": "#/$defs/n"}}}, "$ref": "#/$defs/n"}),
            manifest: section({
                let mut v = json!("bad");
                for _ in 0..100 {
                    v = wrap(&"k".repeat(8000), v);
                }
                v
            }),
            want: Want::Usable { truncated: false },
        },
        // F-005.
        "patterns-256" => usable(pattern_props(SCHEMA_MAX_PATTERNS, |i| format!("^a{i}$"))),
        "patterns-257" => unusable(
            pattern_props(SCHEMA_MAX_PATTERNS + 1, |i| format!("^a{i}$")),
            UnusableCause::Pattern,
        ),
        "patterns-bytes" => unusable(
            json!({"type": "string", "pattern": format!("^{}$", "a".repeat(SCHEMA_PATTERNS_MAX_BYTES))}),
            UnusableCause::Pattern,
        ),
        "pattern-20k-copies" => usable(pattern_props(20_000, |_| "^a+$".to_owned())),
        "pattern-20k-copies-heavy" => usable(pattern_props(20_000, |_| r"\w{40}".to_owned())),
        "patterns-400-heavy" => unusable(
            pattern_props(400, |i| format!(r"\w{{40}}{i}")),
            UnusableCause::Pattern,
        ),
        "pattern-property-copies" => usable({
            let pp: serde_json::Map<String, Value> = (0..1)
                .map(|i| (format!("^p{i}$"), json!({"type": "string"})))
                .collect();
            json!({"type": "object", "patternProperties": pp, "pattern": "^p0$"})
        }),
        other => match other
            .strip_prefix("cycle-")
            .and_then(|n| n.parse::<usize>().ok())
        {
            Some(n) => unusable(cycles[n].clone(), UnusableCause::Compile),
            None => panic!("unknown hostile case {other}"),
        },
    }
}

/// The most time a refused hostile case may take in the child (check and validation together):
/// the analysis is linear, so a refusal is far below it. A usable case builds and validates a
/// large value in a debug build: a looser bound that still catches an exponential blow-up.
const HOSTILE_REFUSAL_BUDGET: Duration = Duration::from_secs(1);
const HOSTILE_USABLE_BUDGET: Duration = Duration::from_secs(5);

// Not a test of its own: the body of `a_hostile_served_schema_is_refused_without_aborting_the_
// process`, run by that test in a child process of this very test binary (a stack overflow
// aborts the process and cannot be caught). Without the environment variable it does nothing.
// It checks the schema on a 2 MiB thread (what a worker thread has), through both entry points.
#[test]
#[ignore = "child process of a_hostile_served_schema_is_refused_without_aborting_the_process"]
fn hostile_schema_child() {
    let Ok(name) = std::env::var(HOSTILE_ENV) else {
        return;
    };
    let Hostile {
        schema,
        manifest: instance,
        want,
    } = hostile_case(&name);
    let sections = [sec("s", schema)];
    let started = Instant::now();
    let (checked, validated) = std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn_scoped(scope, || {
                (
                    check_schemas(&sections),
                    validate_manifest(&sections, &instance),
                )
            })
            .expect("spawn the worker")
            .join()
            .expect("the worker does not panic")
    });
    let elapsed = started.elapsed();
    let budget = match want {
        Want::Unusable(_) => HOSTILE_REFUSAL_BUDGET,
        Want::Usable { .. } => HOSTILE_USABLE_BUDGET,
    };
    assert!(elapsed < budget, "case {name} took {elapsed:?}");
    match want {
        Want::Unusable(cause) => {
            let want = vec![UnusableSchema {
                key: "s".to_owned(),
                cause,
            }];
            assert_eq!(checked, Err(want.clone()), "case {name}");
            assert_eq!(validated, Err(want), "case {name}");
        }
        Want::Usable { truncated } => {
            assert_eq!(checked, Ok(()), "case {name}");
            let got = validated.unwrap_or_else(|e| panic!("case {name}: {e:?}"));
            assert!(
                got.len() <= MAX_VALIDATION_PROBLEMS + 1,
                "case {name}: {}",
                got.len()
            );
            assert!(
                got.iter().all(|p| p.location.len() <= LOCATION_MAX_BYTES),
                "case {name}: a location is over the bound"
            );
            assert_eq!(
                got.last().is_some_and(|p| p.rule == "too_many_problems"),
                truncated,
                "case {name}: truncation"
            );
        }
    }
    println!("{HOSTILE_OK} {name} {} ms", elapsed.as_millis());
    // Dropping a very deep value is recursive too: do it with room to spare.
    std::thread::Builder::new()
        .stack_size(512 << 20)
        .spawn(move || drop((sections, instance)))
        .expect("spawn")
        .join()
        .expect("drop");
}

// DESIGN §4.3 "A served schema is hostile": a schema a plugin serves must not abort the
// process (stack overflow in clone, compile or validation is not catchable) nor run forever.
// Each hostile schema runs in a child process of this test binary under a watchdog, so an
// abort or a hang FAILS this test instead of killing or stalling the whole run.
#[test]
fn a_hostile_served_schema_is_refused_without_aborting_the_process() {
    let exe = std::env::current_exe().expect("the test binary");
    let module = module_path!().split_once("::").expect("a crate prefix").1;
    let test = format!("{module}::hostile_schema_child");
    let cycle_cases = HOSTILE_CASES
        .iter()
        .filter(|c| c.starts_with("cycle-"))
        .count();
    assert_eq!(
        cycle_cases,
        hostile_cycles().len(),
        "every cycle has its case"
    );
    for name in HOSTILE_CASES {
        let mut child = Command::new(&exe)
            .args([
                "--exact",
                &test,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(HOSTILE_ENV, name)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the child test process");
        let deadline = Instant::now() + HOSTILE_TIMEOUT;
        let status = loop {
            if let Some(status) = child.try_wait().expect("poll the child") {
                break status;
            }
            if Instant::now() > deadline {
                child.kill().expect("kill the child");
                child.wait().expect("reap the child");
                panic!("hostile schema {name} did not finish within {HOSTILE_TIMEOUT:?}");
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        let out = child.wait_with_output().expect("collect the output");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            status.success(),
            "hostile schema {name}: child ended with {status}\n{stdout}\n{stderr}"
        );
        assert!(
            stdout.contains(&format!("{HOSTILE_OK} {name}")),
            "hostile schema {name}: the child did not run its case\n{stdout}\n{stderr}"
        );
    }
}

// ---------------------------------------------------------------------------------------
// P5 review fixes (F-003, F-005, F-007, F-011)
// ---------------------------------------------------------------------------------------

// F-003: a property NAMED like a keyword is not that keyword. The schema below has the property
// `additionalProperties` (false: it must not exist); the member makes the section invalid, with
// a problem at the member, exactly as for any other forbidden property.
#[test]
fn a_property_named_like_a_keyword_does_not_hide_a_violation() {
    let m = |section: Value| manifest(&json!({"k": section}));
    for name in [
        "additionalProperties",
        "properties",
        "patternProperties",
        "dependentSchemas",
        "$defs",
        "allOf",
        "items",
    ] {
        let schema = json!({"type": "object", "properties": {name: false}});
        let got = problems(&[sec("k", schema)], &m(json!({name: 1})));
        assert!(!got.is_empty(), "property {name}: the violation is lost");
        assert_eq!(
            got[0].location,
            format!("/sections/k/{name}"),
            "property {name}"
        );
        assert_eq!(got[0].rule, "false", "property {name}");
    }
    // Under `dependentSchemas` and `patternProperties` too.
    let dep = json!({"type": "object", "dependentSchemas": {"additionalProperties": false}});
    let got = problems(
        &[sec("k", dep)],
        &m(json!({"additionalProperties": 1, "z": 2})),
    );
    assert!(!got.is_empty(), "dependentSchemas: {got:?}");
    let pp = json!({"type": "object", "patternProperties": {"^additionalProperties$": false}});
    let got = problems(&[sec("k", pp)], &m(json!({"additionalProperties": 1})));
    assert!(!got.is_empty(), "patternProperties: {got:?}");
    // The real keyword nested under a property of the same name keeps its per-member problems.
    let real = json!({"type": "object", "properties": {"properties":
                       {"type": "object", "additionalProperties": false}}});
    let got = problems(
        &[sec("k", real)],
        &m(json!({"properties": {"x": 1, "y": 2}})),
    );
    assert_eq!(
        keys(&got),
        vec![
            ("/sections/k/properties/x", "additionalProperties"),
            ("/sections/k/properties/y", "additionalProperties")
        ]
    );
    // The keyword on a non-object instance is a violation the library names differently.
    let kw = json!({"additionalProperties": false});
    let got = problems(&[sec("k", kw)], &m(json!({"a": 1})));
    assert_eq!(keys(&got), vec![("/sections/k/a", "additionalProperties")]);
}

// F-003 invariant: whatever the schema and the manifest, the verdict of `validate_manifest` is
// the one of the stock library: invalid manifests always have at least one problem.
#[test]
fn every_library_error_yields_a_problem() {
    let schemas = [
        json!({"type": "object", "properties": {"additionalProperties": false}}),
        json!({"type": "object", "additionalProperties": false}),
        json!({"type": "object", "properties": {"a": {}}, "additionalProperties": false}),
        json!({"type": "object", "unevaluatedProperties": false}),
        json!({"type": "array", "items": false}),
        json!({"type": "array", "prefixItems": [{}], "unevaluatedItems": false}),
        json!({"not": {}}),
        json!({"oneOf": [{}, {}]}),
        json!({"dependentSchemas": {"x": {"additionalProperties": false}}}),
        json!({"propertyNames": {"maxLength": 1}}),
        json!({"contains": {"type": "string"}}),
    ];
    let instances = [
        json!(1),
        json!([]),
        json!([1, 2]),
        json!({}),
        json!({"a": 1}),
        json!({"x": 1, "additionalProperties": 2, "longer": 3}),
        json!({"properties": 1, "additionalProperties": 1}),
    ];
    for schema in &schemas {
        let sections = [sec("k", schema.clone())];
        let combined = combined_schema(&sections);
        let stock = jsonschema::validator_for(&combined).expect("compiles");
        for inst in &instances {
            let m = manifest(&json!({"k": inst}));
            let got = problems(&sections, &m);
            assert_eq!(
                got.is_empty(),
                stock.is_valid(&m),
                "schema {schema} instance {inst}"
            );
        }
    }
}

// F-005: the pattern bounds are exact. At the count bound the schema is usable, one more distinct
// pattern is not; repeats of one pattern count once; the byte bound is checked on the distinct ones.
#[test]
fn the_pattern_count_and_byte_bounds_are_exact() {
    let n = SCHEMA_MAX_PATTERNS;
    assert_usable(&pattern_props(n, |i| format!("^a{i}$")));
    assert_eq!(
        cause_of(&pattern_props(n + 1, |i| format!("^a{i}$"))),
        UnusableCause::Pattern
    );
    // `pattern` and `patternProperties` names share the budget.
    let mut mixed = pattern_props(n, |i| format!("^a{i}$"));
    mixed["patternProperties"] = json!({"^extra$": {}});
    assert_eq!(cause_of(&mixed), UnusableCause::Pattern);
    // Repeats of one pattern are one pattern.
    assert_usable(&pattern_props(n * 4, |_| "^a+$".to_owned()));
    // Bytes: the distinct patterns together at the bound are usable, one byte more is not.
    let items = |n: usize| -> Vec<Value> {
        (0..n)
            .map(|i| json!({"pattern": format!("{i:03}{}", "a".repeat(SCHEMA_PATTERNS_MAX_BYTES.div_euclid(128) - 3))}))
            .collect()
    };
    assert_usable(&json!({"allOf": items(128)}));
    let mut over = items(128);
    over.push(json!({"pattern": "b"}));
    assert_eq!(cause_of(&json!({"allOf": over})), UnusableCause::Pattern);
    // Before anything is compiled: an uncompilable pattern is a Pattern either way, and the bound
    // beats a Compile defect elsewhere.
    let both = json!({"$ref": "#/nope", "allOf": [{"pattern": "a".repeat(SCHEMA_PATTERNS_MAX_BYTES + 1)}]});
    assert_eq!(cause_of(&both), UnusableCause::Pattern);
}

// F-007: `check_schemas` refuses what the combined build cannot serve: a key outside the section
// key rule and a key served twice are unusable (Compile), named by the key.
#[test]
fn an_invalid_or_repeated_section_key_is_unusable() {
    let ok = json!({"type": "string"});
    let bad = |sections: &[SectionSchema]| check_schemas(sections).expect_err("unusable");
    let unusable_key = |key: &str| UnusableSchema {
        key: key.to_owned(),
        cause: UnusableCause::Compile,
    };
    for key in ["", "A", "a b", "../x", ".", "..", "a/b", &"k".repeat(65)] {
        assert_eq!(
            bad(&[sec(key, ok.clone())]),
            vec![unusable_key(key)],
            "key {key:?}"
        );
    }
    // Served twice: one entry for the key, whatever the schemas are.
    assert_eq!(
        bad(&[sec("k", ok.clone()), sec("k", json!({"type": "number"}))]),
        vec![unusable_key("k")]
    );
    assert_eq!(
        bad(&[
            sec("k", ok.clone()),
            sec("j", ok.clone()),
            sec("k", deep_not(80))
        ]),
        vec![unusable_key("k")]
    );
    // Both entry points agree.
    let sections = [sec("k", ok.clone()), sec("k", ok.clone())];
    assert_eq!(
        validate_manifest(&sections, &ok_manifest()),
        Err(vec![unusable_key("k")])
    );
    // Valid distinct keys still pass.
    assert_eq!(
        check_schemas(&[sec("a-b_c.d", ok.clone()), sec(&"k".repeat(64), ok)]),
        Ok(())
    );
}

// F-011: a `$ref` or `$dynamicRef` that is not a string is a malformed schema (Compile), not an
// external reference. The boolean root and a non-string `$schema` stay Compile.
#[test]
fn a_non_string_reference_is_a_compile_failure() {
    for schema in [
        json!({"$ref": 5}),
        json!({"$ref": null}),
        json!({"$ref": ["#"]}),
        json!({"$dynamicRef": {}}),
        json!({"properties": {"a": {"$ref": true}}}),
        json!(true),
        json!({"$schema": 5}),
        json!({"properties": {"a": {"$schema": ["x"]}}}),
    ] {
        assert_eq!(cause_of(&schema), UnusableCause::Compile, "schema {schema}");
    }
    // A string that leaves the document is still an external reference.
    assert_eq!(
        cause_of(&json!({"$ref": "other.json"})),
        UnusableCause::ExternalReference
    );
}

// F-001: references that resolve are usable, percent-decoded and `~`-unescaped; `#` and an empty
// fragment mean the root; a boolean target is a leaf. Without `unevaluated*` an anchor is fine.
#[test]
fn local_references_resolve_like_the_library() {
    assert_usable(&json!({"$defs": {"a b": {"type": "string"}}, "$ref": "#/$defs/a%20b"}));
    assert_usable(&json!({"$defs": {"a/b": {"type": "string"}}, "$ref": "#/$defs/a~1b"}));
    assert_usable(&json!({"$defs": {"t": true}, "allOf": [{"$ref": "#/$defs/t"}]}));
    assert_usable(&json!({"$defs": {"a": {"$anchor": "x", "type": "string"}}, "$ref": "#x"}));
    assert_usable(&json!({"properties": {"c": {"$ref": "#"}}}));
    // Percent-decoding gone wrong is unresolvable.
    for bad in ["#/$defs/a%2", "#/$defs/a%zz", "#/$defs/a%ff"] {
        assert_eq!(
            cause_of(&json!({"$defs": {"a": {}}, "$ref": bad})),
            UnusableCause::Compile,
            "{bad}"
        );
    }
}

// ---------------------------------------------------------------------------------------
// Bounds
// ---------------------------------------------------------------------------------------

/// Runs `f` on a 2 MiB thread, the stack of a pool worker.
fn on_a_small_stack<T: Send>(f: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn_scoped(scope, f)
            .expect("spawn")
            .join()
            .expect("the validation does not panic")
    })
}

// A manifest at the deepest accepted nesting, validated by a RECURSIVE section schema
// (the validation really descends every level), neither exhausts a 2 MiB stack nor loses
// the location of a bad leaf at the bottom.
#[test]
fn a_manifest_at_the_deepest_accepted_nesting_validates_on_a_small_stack() {
    use crate::domain::manifest::parse_manifest;
    let levels = MAX_JSON_DEPTH - 2; // the root and `sections` take two levels
    let array_schema = json!({"$defs": {"n": {"type": ["integer", "array"], "items": {"$ref": "#/$defs/n"}}},
                               "$ref": "#/$defs/n"});
    let object_schema = json!({"$defs": {"n": {"type": ["integer", "object"], "additionalProperties": {"$ref": "#/$defs/n"}}},
                                "$ref": "#/$defs/n"});
    for (name, schema, open, close, step) in [
        ("arrays", array_schema, "[", "]", "/0"),
        ("objects", object_schema, "{\"a\":", "}", "/a"),
    ] {
        let body = |leaf: &str| {
            format!(
                "{{\"application\":{{\"id\":\"a\",\"version\":\"1\"}},\"sections\":{{\"s\":{}{leaf}{}}}}}",
                open.repeat(levels),
                close.repeat(levels)
            )
        };
        let sections = [sec("s", schema)];
        let good = parse_manifest(body("1").as_bytes()).expect("accepted at the bound");
        assert_eq!(
            on_a_small_stack(|| validate_manifest(&sections, &good)),
            Ok(vec![]),
            "{name}"
        );
        let bad = parse_manifest(body("\"bad\"").as_bytes()).expect("accepted at the bound");
        let got = on_a_small_stack(|| validate_manifest(&sections, &bad)).expect("usable");
        assert_eq!(got.len(), 1, "{name}: {got:?}");
        assert_eq!(got[0].rule, "type", "{name}");
        assert_eq!(
            got[0].location,
            format!("/sections/s{}", step.repeat(levels)),
            "{name}"
        );
    }
}

// A deeply nested section with a shallow schema: no descent, no problem, no stack use.
#[test]
fn a_deeply_nested_section_validates() {
    let mut deep = json!(1);
    for _ in 0..(MAX_JSON_DEPTH - 2) {
        deep = Value::Array(vec![deep]);
    }
    let got = problems(
        &[sec("s", json!({"type": "array"}))],
        &manifest(&json!({"s": deep})),
    );
    assert_eq!(got, vec![]);
    let bad = problems(
        &[sec("s", json!({"type": "object"}))],
        &manifest(&json!({"s": [1]})),
    );
    assert_eq!(keys(&bad), vec![("/sections/s", "type")]);
}
