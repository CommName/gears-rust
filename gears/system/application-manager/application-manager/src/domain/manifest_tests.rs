//! Unit tests for the seed manifest parser and the envelope rules (DESIGN §3.1
//! "Manifest", §3.2 step 1 "Receive", §3.3 problem shapes, §4.1 "Control character" and
//! "Never-storable id", §4.8, PRD manifest requirements).
//!
//! Planned API in `crate::domain::manifest` (new file, P5). The module ends with:
//!
//! ```text
//! #[cfg(test)]
//! #[cfg_attr(coverage_nightly, coverage(off))]
//! #[allow(clippy::non_ascii_literal)]
//! #[path = "manifest_tests.rs"]
//! mod manifest_tests;
//! ```
//!
//! ```text
//! /// Escapes one reference token of an RFC 6901 JSON Pointer: `~` becomes `~0`, then
//! /// `/` becomes `~1`; every other character is unchanged.
//! pub fn escape_pointer_segment(segment: &str) -> String;
//!
//! /// Why a seed body is not a manifest. No variant carries body content other than the
//! /// JSON Pointer of a repeated-member object.
//! #[derive(Debug, Clone, PartialEq, Eq)]
//! pub enum ManifestParseFailure {
//!     Syntax,                                  // not JSON (includes trailing bytes, empty body)
//!     NotUtf8,                                 // the bytes are not UTF-8
//!     TooDeep,                                 // nesting beyond `MAX_JSON_DEPTH`
//!     DuplicateMember { location: String },    // an object repeats a name after unescaping
//! }
//! impl ManifestParseFailure {
//!     /// `malformed_json` for Syntax, NotUtf8 and TooDeep; `duplicate_member` otherwise.
//!     pub fn rule(&self) -> &'static str;
//!     /// `""` (the whole body) except for DuplicateMember: the pointer of the object.
//!     pub fn location(&self) -> &str;
//!     /// A `Problem` with that location and rule and a fixed message: no body content.
//!     pub fn to_problem(&self) -> Problem;
//! }
//!
//! /// One parse of a seed body (DESIGN §3.2 step 1). A failure yields NO value: an
//! /// invalid body is not a manifest and is not recorded in the request hand-off.
//! /// Several faults: UTF-8 is checked over the whole body first (`NotUtf8` wins); then
//! /// the FIRST failure in document order of one streaming pass is the failure: a repeated
//! /// name is detected when the repeated KEY is read (before its value and before any later
//! /// syntax error or depth excess), nesting beyond the bound when the container opens.
//! pub fn parse_manifest(body: &[u8]) -> Result<serde_json::Value, ManifestParseFailure>;
//!
//! #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//! pub enum TextLimitViolation { Empty, TooLong, ControlCharacter }
//! impl TextLimitViolation {
//!     /// `minLength`, `max_length`, `control_character`.
//!     pub fn rule(self) -> &'static str;
//! }
//! /// Every limit `text` breaks, in this order: `Empty` or `TooLong` (bytes of UTF-8,
//! /// against `max_len`), then `ControlCharacter` (any `char::is_control`).
//! pub fn text_limit_violations(text: &str, max_len: usize) -> Vec<TextLimitViolation>;
//!
//! /// `application.id` and `application.version` of a parsed manifest, each present only
//! /// when it is a JSON string. Never trimmed, case-folded or normalised.
//! #[derive(Debug, Clone, PartialEq, Eq, Default)]
//! pub struct EnvelopeFields { pub id: Option<String>, pub version: Option<String> }
//! pub fn extract_envelope(manifest: &serde_json::Value) -> EnvelopeFields;
//! impl EnvelopeFields {
//!     /// One `Problem` per broken limit: id first, then version, at
//!     /// `/application/id` and `/application/version`, rule from `TextLimitViolation::rule`,
//!     /// message without the value. Absent fields give none.
//!     pub fn limit_problems(&self, max_len: usize) -> Vec<Problem>;
//!     /// The fields within the limits; a field that breaks one is dropped (the request
//!     /// hand-off and the report carry an id or version "only when within limits").
//!     pub fn within_limits(&self, max_len: usize) -> EnvelopeFields;
//! }
//! ```
//!
//! New constant in `crate::domain::limits`: `MAX_JSON_DEPTH: usize` (see `limits_tests`).
//!
//! Fuzz seeds: `fuzz/corpus/manifest_parser/` next to this crate's `Cargo.toml`, named
//! `<kind>-<what>.seed` with kind `valid`, `syntax`, `utf8`, `depth` or `duplicate` (the
//! `.seed` extension keeps `gts-validator` from scanning invalid JSON as documents; the
//! directory is byte-exact, see `.gitattributes`). Files without a kind prefix (a fuzzer's
//! own output) are not seeds and are ignored by the tests.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use application_manager_sdk::models::{HandoffOperation, RequestHandoffContent};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::domain::limits::{LOCATION_MAX_BYTES, MAX_JSON_DEPTH, REGISTRY_TEXT_MAX_BYTES};
use crate::domain::manifest::{
    EnvelopeFields, ManifestParseFailure, TextLimitViolation, cap_location, escape_pointer_segment,
    extract_envelope, parse_manifest, text_limit_violations,
};
use crate::domain::registry::is_storable_text;
use crate::test_support::manifest_gen::{
    arb_json_value, nested_arrays, nested_by, nested_mixed, nested_objects,
};

const MAX: usize = REGISTRY_TEXT_MAX_BYTES;

/// Property tests: a fixed case count and no regression files written into the tree.
fn cfg(cases: u32) -> ProptestConfig {
    ProptestConfig {
        failure_persistence: None,
        ..ProptestConfig::with_cases(cases)
    }
}

fn dup(location: &str) -> ManifestParseFailure {
    ManifestParseFailure::DuplicateMember {
        location: location.to_owned(),
    }
}

// ---------------------------------------------------------------------------------------
// parse_manifest: values
// ---------------------------------------------------------------------------------------

// DESIGN §3.2 step 1: the body is parsed once into the same value serde_json produces.
#[test]
fn a_valid_manifest_parses_to_its_json_value() {
    let body = br#"{"application":{"id":"app","version":"1"},"sections":{"net":{"port":80}}}"#;
    let want: Value = serde_json::from_slice(body).expect("reference parse");
    assert_eq!(parse_manifest(body), Ok(want));
}

// DESIGN §3.1: a body that is the JSON value `null` is a value (carried as `null` in
// the hand-off), distinct from an invalid body. Other non-object values parse too; the
// envelope schema, not the parser, rejects them.
#[test]
fn any_json_value_parses_including_null_and_scalars() {
    for (body, want) in [
        ("null", Value::Null),
        ("true", json!(true)),
        ("-12.5", json!(-12.5)),
        ("\"x\"", json!("x")),
        ("[]", json!([])),
        ("{}", json!({})),
        ("  \t\r\n{}\n ", json!({})),
    ] {
        assert_eq!(parse_manifest(body.as_bytes()), Ok(want), "body {body:?}");
    }
}

// Escapes are unescaped in the parsed value.
#[test]
fn escapes_are_unescaped_in_the_parsed_value() {
    let v = parse_manifest(br#"{"a\/b":"\u00e9\ud83d\ude00\n"}"#).expect("parse");
    assert_eq!(v, json!({"a/b": "é😀\n"}));
}

// Big numbers are kept as serde_json keeps them (no loss of the u64 range).
#[test]
fn integers_in_the_u64_range_parse_exactly() {
    let v = parse_manifest(b"[18446744073709551615,-9223372036854775808]").expect("parse");
    assert_eq!(v, json!([18_446_744_073_709_551_615_u64, i64::MIN]));
}

// Numbers are the numbers serde_json reads (an f64 is never narrowed, a literal is never
// rewritten). Integers beyond the u64 range become floats: that is serde_json's default
// (no `arbitrary_precision`) and it is accepted; the Schema Validator then treats a schema
// number beyond 2^53 as unusable, and a manifest value keeps its float.
#[test]
fn numbers_parse_exactly_as_serde_json_reads_them() {
    for text in [
        "1.0",
        "1e2",
        "-0",
        "0.1",
        "1E+2",
        "0.30000000000000004",
        "1.7976931348623157e308",
        "5e-324",
        "1e-400",
        "18446744073709551616",
        "-9223372036854775809",
        "123456789012345678901234567890",
    ] {
        let want: Value = serde_json::from_str(text).expect("reference parse");
        for body in [text.to_owned(), format!("[{text},{{\"k\":{text}}}]")] {
            let got = parse_manifest(body.as_bytes()).expect("parse");
            let reference: Value = serde_json::from_str(&body).expect("reference parse");
            assert_eq!(got, reference, "body {body}");
            // `-0` equals `0` as a value: compare the text too.
            assert_eq!(
                serde_json::to_string(&got).expect("serialize"),
                serde_json::to_string(&reference).expect("serialize"),
                "body {body}"
            );
        }
        assert_eq!(parse_manifest(text.as_bytes()), Ok(want));
    }
    // A number beyond the f64 range is not JSON serde_json reads: `malformed_json`.
    assert_eq!(parse_manifest(b"1e400"), Err(ManifestParseFailure::Syntax));
}

// ---------------------------------------------------------------------------------------
// parse_manifest: syntax and encoding
// ---------------------------------------------------------------------------------------

// DESIGN §3.2 step 1: a syntax error ends the parse: no value. Every one of these is
// `Syntax` with the rule `malformed_json`.
#[test]
fn syntax_errors_fail_with_malformed_json() {
    let cases: &[&[u8]] = &[
        b"",
        b"   ",
        b"{",
        b"[1,2",
        br#"{"a":}"#,
        br#"{"a":1,}"#,
        br"{a:1}",
        b"{'a':1}",
        br#"{"a":1} x"#,
        br#"{"a":1}{"b":2}"#,
        b"nul",
        b"NaN",
        b"[01]",
        b"\xEF\xBB\xBF{\"a\":1}",
        br#"["\ud800"]"#,
        br#"["\q"]"#,
        b"[\"a\nb\"]",
        br#"{"a":1}//c"#,
        // A scalar root followed by anything is no value (not only after an object).
        b"0t",
        b"1 x",
        b"truex",
        br#""a"b"#,
        b"[]]",
        b"nullnull",
        b"1 2",
    ];
    for body in cases {
        let got = parse_manifest(body);
        assert_eq!(
            got,
            Err(ManifestParseFailure::Syntax),
            "body {:?}",
            String::from_utf8_lossy(body)
        );
    }
    let f = ManifestParseFailure::Syntax;
    assert_eq!(f.rule(), "malformed_json");
    assert_eq!(f.location(), "");
}

// DESIGN §3.2 step 1: non-UTF-8 bytes end the parse like a syntax error (`malformed_json`)
// but are told apart so the log can say which.
#[test]
fn bytes_that_are_not_utf8_fail_as_not_utf8() {
    let cases: &[&[u8]] = &[
        b"{\"a\":\"\xff\"}",
        b"{\"a\":\"\xc3\"}",
        b"{\"a\":\"\xc0\xaf\"}",
        b"{\"\xfe\":1}",
        b"[\"\xed\xa0\x80\"]",
        b"\xff",
    ];
    for body in cases {
        assert_eq!(
            parse_manifest(body),
            Err(ManifestParseFailure::NotUtf8),
            "body {body:?}"
        );
    }
    let f = ManifestParseFailure::NotUtf8;
    assert_eq!(f.rule(), "malformed_json");
    assert_eq!(f.location(), "");
}

// ---------------------------------------------------------------------------------------
// parse_manifest: depth
// ---------------------------------------------------------------------------------------

// `MAX_JSON_DEPTH` itself is pinned in `limits_tests` (exactly the SDK hand-off bound).

/// Nesting depth of a parsed value (`[]` and `{}` count 1, a scalar 0). Recursive: only for
/// shallow, bounded values.
fn depth_of(v: &Value) -> usize {
    match v {
        Value::Array(a) => 1 + a.iter().map(depth_of).max().unwrap_or(0),
        Value::Object(o) => 1 + o.values().map(depth_of).max().unwrap_or(0),
        _ => 0,
    }
}

type Shape = (&'static str, fn(usize, usize) -> bool);

/// Bracket patterns for the depth tests: `(n, i)` is whether level `i` of `n` is an object.
const SHAPES: [Shape; 6] = [
    ("arrays", |_, _| false),
    ("objects", |_, _| true),
    ("object first, alternating", |_, i| i % 2 == 0),
    ("array first, alternating", |_, i| i % 2 == 1),
    ("runs of three", |_, i| i.div_euclid(3) % 2 == 0),
    ("object innermost only", |n, i| i + 1 == n),
];

fn shaped(n: usize, shape: fn(usize, usize) -> bool) -> Vec<u8> {
    nested_by(n, |i| shape(n, i))
}

// The generator really produces the stated depth (the boundary tests below rest on it).
#[test]
fn the_nesting_generators_produce_the_stated_depth() {
    for (name, shape) in SHAPES {
        for n in [1, 2, 3, 63, 64, 125, 126] {
            let v: Value = serde_json::from_slice(&shaped(n, shape)).expect("serde_json parse");
            assert_eq!(depth_of(&v), n, "{name} depth {n}");
        }
    }
    for n in [1, 2, 7, 126] {
        for body in [nested_arrays(n), nested_objects(n), nested_mixed(n)] {
            let v: Value = serde_json::from_slice(&body).expect("serde_json parse");
            assert_eq!(depth_of(&v), n);
        }
    }
    assert_eq!(nested_by(0, |_| true), b"1");
}

// DESIGN §3.2 step 1: nesting up to MAX_JSON_DEPTH is accepted (`[]` counts as 1), for
// arrays, objects and every mixture...
#[test]
fn nesting_up_to_the_bound_is_accepted() {
    for (name, shape) in SHAPES {
        for n in [1, 2, MAX_JSON_DEPTH - 1, MAX_JSON_DEPTH] {
            assert!(
                parse_manifest(&shaped(n, shape)).is_ok(),
                "{name} at depth {n}"
            );
        }
    }
    assert!(parse_manifest(&nested_mixed(MAX_JSON_DEPTH)).is_ok());
}

// ...and one level more ends the parse as `malformed_json` (`TooDeep`), whatever the mixture.
#[test]
fn nesting_beyond_the_bound_fails_as_too_deep() {
    for (name, shape) in SHAPES {
        for n in [MAX_JSON_DEPTH + 1, MAX_JSON_DEPTH + 2, 2 * MAX_JSON_DEPTH] {
            assert_eq!(
                parse_manifest(&shaped(n, shape)),
                Err(ManifestParseFailure::TooDeep),
                "{name} at depth {n}"
            );
        }
    }
    assert_eq!(
        parse_manifest(&nested_mixed(MAX_JSON_DEPTH + 1)),
        Err(ManifestParseFailure::TooDeep)
    );
    let f = ManifestParseFailure::TooDeep;
    assert_eq!(f.rule(), "malformed_json");
    assert_eq!(f.location(), "");
}

// DESIGN §4.3 "deeply nested body": a body as deep as the gateway allows must not exhaust
// the stack. It ends as `TooDeep` (the first failure in document order), balanced or not.
#[test]
fn very_deep_bodies_fail_without_exhausting_the_stack() {
    let n = 200_000;
    for body in [
        nested_arrays(n),
        nested_objects(n),
        nested_mixed(n),
        vec![b'['; n],
        "{\"a\":".repeat(n).into_bytes(),
    ] {
        assert_eq!(parse_manifest(&body), Err(ManifestParseFailure::TooDeep));
    }
}

// DESIGN §1.3, §3.1: a manifest accepted at the deepest level reads back from the hand-off
// content an audit plugin stores as JSON, whatever its brackets.
#[test]
fn the_deepest_accepted_manifest_round_trips_inside_the_handoff_content() {
    for body in [
        nested_arrays(MAX_JSON_DEPTH),
        nested_objects(MAX_JSON_DEPTH),
        nested_mixed(MAX_JSON_DEPTH),
    ] {
        let manifest = parse_manifest(&body).expect("accepted");
        let content = RequestHandoffContent {
            operation: HandoffOperation::Seed,
            request_id: Uuid::nil(),
            received_at: "2026-10-13T00:00:00Z".to_owned(),
            application: None,
            manifest: Some(manifest),
            decided_category: None,
            id_length_bytes: None,
            id_sha256: None,
        };
        let wire = serde_json::to_string(&content).expect("serialize");
        let back: RequestHandoffContent = serde_json::from_str(&wire).expect("read back");
        assert_eq!(back, content);
    }
}

// ---------------------------------------------------------------------------------------
// parse_manifest: repeated members
// ---------------------------------------------------------------------------------------

// DESIGN §3.2 step 1, §3.3: a repeated member name in any object ends the parse with rule
// `duplicate_member`; the location is the object that repeats it (RFC 6901 escaped).
#[test]
fn a_repeated_member_name_fails_at_the_object_that_repeats_it() {
    let cases: &[(&str, &str)] = &[
        (r#"{"a":1,"a":2}"#, ""),
        (r#"{"a":1,"a":1}"#, ""),
        (r#"{"a":1,"b":2,"a":3}"#, ""),
        (r#"{"a":1,"a":2,"a":3}"#, ""),
        (r#"{"a":1,"b":2,"c":3,"b":4}"#, ""),
        (
            r#"{"application":{"id":"a","version":"1"},"application":{"id":"b","version":"1"}}"#,
            "",
        ),
        (
            r#"{"application":{"id":"a","id":"b","version":"1"},"sections":{}}"#,
            "/application",
        ),
        (r#"{"sections":{"net":{"k":1,"k":2}}}"#, "/sections/net"),
        (
            r#"{"sections":{"net":{"a":1,"b":2,"a":3}}}"#,
            "/sections/net",
        ),
        (r#"{"sections":{},"sections":{}}"#, ""),
        (r#"[{"x":1,"x":2}]"#, "/0"),
        (r#"[1,[2,{"x":1,"x":2}]]"#, "/1/1"),
        (r#"{"a/b":{"x":1,"x":2}}"#, "/a~1b"),
        (r#"{"a~b":{"x":1,"x":2}}"#, "/a~0b"),
        (r#"{"":{"x":1,"x":2}}"#, "/"),
        (r#"{"a":{"x":1},"b":{"y":1,"y":2}}"#, "/b"),
        (r#"{"a":[1,{"b":[{"c":1,"c":2}]}]}"#, "/a/1/b/0"),
    ];
    for (body, location) in cases {
        assert_eq!(
            parse_manifest(body.as_bytes()),
            Err(dup(location)),
            "body {body}"
        );
    }
    let f = dup("/a");
    assert_eq!(f.rule(), "duplicate_member");
    assert_eq!(f.location(), "/a");
}

// A repeat is found however many members precede it: far from its first occurrence, after a
// long run of other members, and at every depth up to the bound.
#[test]
fn a_repeat_is_found_at_any_distance_and_depth() {
    let run = (0..50)
        .map(|i| format!("\"k{i}\":{i},"))
        .collect::<Vec<_>>()
        .concat();
    assert_eq!(
        parse_manifest(format!("{{{run}\"k0\":0}}").as_bytes()),
        Err(dup("")),
        "repeat of the first of 50"
    );
    assert_eq!(
        parse_manifest(format!("{{{run}\"k49\":0}}").as_bytes()),
        Err(dup("")),
        "repeat of the last of 50"
    );
    assert_eq!(
        parse_manifest(format!("{{\"o\":{{{run}\"k7\":0}}}}").as_bytes()),
        Err(dup("/o")),
        "repeat after a run, nested"
    );
    // At the deepest accepted level, with the full pointer.
    let mut body = "{\"a\":".repeat(MAX_JSON_DEPTH - 1);
    body.push_str(r#"{"x":1,"x":2}"#);
    body.push_str(&"}".repeat(MAX_JSON_DEPTH - 1));
    assert_eq!(
        parse_manifest(body.as_bytes()),
        Err(dup(&"/a".repeat(MAX_JSON_DEPTH - 1)))
    );
    // One level less, the same.
    let mut body = "{\"a\":".repeat(3);
    body.push_str(r#"{"x":1,"x":2}"#);
    body.push_str(&"}".repeat(3));
    assert_eq!(parse_manifest(body.as_bytes()), Err(dup("/a/a/a")));
}

// A valid body is not slowed or refused by its size: many sibling containers and many
// members are no repeat (no global counter, no per-body limit on containers).
#[test]
fn many_siblings_and_many_members_are_valid() {
    let arrays = format!("[{}]", vec!["[]"; 400].join(","));
    let v = parse_manifest(arrays.as_bytes()).expect("400 sibling arrays");
    assert_eq!(v.as_array().map(Vec::len), Some(400));
    let members: String = (0..500)
        .map(|i| format!("\"s{i}\":{{\"x\":{i}}}"))
        .collect::<Vec<_>>()
        .join(",");
    let body = format!(
        "{{\"application\":{{\"id\":\"a\",\"version\":\"1\"}},\"sections\":{{{members}}}}}"
    );
    let v = parse_manifest(body.as_bytes()).expect("500 sections");
    assert_eq!(
        v["sections"].as_object().map(serde_json::Map::len),
        Some(500)
    );
    let same_name: String = vec![r#"{"x":1}"#; 300].join(",");
    assert!(parse_manifest(format!("[{same_name}]").as_bytes()).is_ok());
}

// DESIGN §3.2 step 1, §4.3 parser differential: names are compared after unescaping.
#[test]
fn member_names_are_compared_after_unescaping() {
    let cases: &[&str] = &[
        r#"{"id":1,"\u0069d":2}"#,
        r#"{"\u0069d":1,"id":2}"#,
        r#"{"\u0069\u0064":1,"id":2}"#,
        r#"{"sections":1,"\u0073ections":2}"#,
        r#"{"a/b":1,"a\/b":2}"#,
        r#"{"a/b":1,"a\u002fb":2}"#,
        r#"{"\u00e9":1,"é":2}"#,
        r#"{"é":1,"\u00e9":2}"#,
        r#"{"\u00E9":1,"\u00e9":2}"#,
        r#"{"\ud83d\ude00":1,"😀":2}"#,
        r#"{"😀":1,"\ud83d\ude00":2}"#,
        r#"{"\uD83D\uDE00":1,"\ud83d\ude00":2}"#,
        r#"{"a\nb":1,"a\u000ab":2}"#,
        r#"{"a\"b":1,"a\u0022b":2}"#,
        r#"{"a\\b":1,"a\u005cb":2}"#,
        r#"{"x":{"\u0069d":1,"id":2}}"#,
    ];
    for body in cases {
        let got = parse_manifest(body.as_bytes());
        assert!(
            matches!(got, Err(ManifestParseFailure::DuplicateMember { .. })),
            "body {body} gave {got:?}"
        );
    }
    // The location is the unescaped, pointer-escaped path of the object.
    assert_eq!(
        parse_manifest(br#"{"\u0061\/b":{"x":1,"\u0078":2}}"#),
        Err(dup("/a~1b"))
    );
}

// Names that differ by case, normalization form or escapes of different characters are
// distinct; the same name in sibling objects is no repeat.
#[test]
fn names_that_differ_are_not_repeats() {
    let cases: &[&str] = &[
        r#"{"id":1,"Id":2,"ID":3}"#,
        "{\"e\u{301}\":1,\"\u{e9}\":2}",
        r#"{"a":{"x":1},"b":{"x":2},"c":[{"x":3},{"x":4}]}"#,
        r#"{"a":1," a":2,"a ":3}"#,
        r#"{"a":{"a":{"a":1}}}"#,
        r#"{"\ud83d\ude00":1,"\ud83d\ude01":2}"#,
        r#"{"😀":1,"😁":2}"#,
        r#"{"\u00e9":1,"\u00ea":2}"#,
        r#"{"é":1,"ê":2}"#,
        r#"{"\u0069d":1,"\u0049d":2}"#,
        r#"{"a/b":1,"a~1b":2}"#,
        r#"{"a\u0000":1,"a":2}"#,
    ];
    for body in cases {
        assert!(parse_manifest(body.as_bytes()).is_ok(), "body {body}");
    }
}

// DESIGN §3.2 step 1: the first repeat in document order ends the parse.
#[test]
fn the_first_repeat_in_document_order_is_reported() {
    let got = parse_manifest(br#"{"a":{"x":1,"x":2},"b":{"y":1,"y":2}}"#);
    assert_eq!(got, Err(dup("/a")));
}

// DESIGN §3.2 step 1: a repeat is detected when the repeated KEY is read: the object that is
// the earlier value is complete, so the location is the object that holds both members.
#[test]
fn a_repeat_is_located_when_its_key_is_read() {
    assert_eq!(
        parse_manifest(br#"{"a":{"x":1},"a":{"x":1,"x":2}}"#),
        Err(dup(""))
    );
    assert_eq!(parse_manifest(br#"{"a":{"x":1},"a":"#), Err(dup("")));
    assert_eq!(parse_manifest(br#"{"a":{"x":1},"a""#), Err(dup("")));
}

// ---------------------------------------------------------------------------------------
// Several faults: which one is reported
// ---------------------------------------------------------------------------------------

// DESIGN §3.2 step 1: UTF-8 is checked over the whole body first, so `NotUtf8` wins; then
// the first failure in document order of one streaming pass is the failure.
#[test]
fn the_failure_of_a_body_with_several_faults_follows_a_fixed_precedence() {
    let deep = "[".repeat(MAX_JSON_DEPTH + 1);
    let cases: Vec<(Vec<u8>, ManifestParseFailure)> = vec![
        // Non-UTF-8 wins over a repeat, a syntax error and a depth excess, wherever it is.
        (
            b"{\"a\":1,\"a\":2}\xff".to_vec(),
            ManifestParseFailure::NotUtf8,
        ),
        (
            b"{\"a\":@,\"b\":\"\xff\"}".to_vec(),
            ManifestParseFailure::NotUtf8,
        ),
        (
            [deep.as_bytes(), b"\"\xff\""].concat(),
            ManifestParseFailure::NotUtf8,
        ),
        (b"\xff{".to_vec(), ManifestParseFailure::NotUtf8),
        // Then the document order: the repeated key is read before a later syntax error...
        (br#"{"a":1,"a":@"#.to_vec(), dup("")),
        (br#"{"a":1,"a":2,}"#.to_vec(), dup("")),
        (br#"{"a":1,"a":2} x"#.to_vec(), dup("")),
        // ...a syntax error before a repeat is the failure...
        (br#"{"a":@,"a":2}"#.to_vec(), ManifestParseFailure::Syntax),
        (br#"{"a":1 "a":2}"#.to_vec(), ManifestParseFailure::Syntax),
        // ...a repeat before the depth excess wins, a depth excess before a repeat loses.
        (
            [br#"{"a":1,"a":"#.as_slice(), deep.as_bytes()].concat(),
            dup(""),
        ),
        (
            [deep.as_bytes(), br#"{"x":1,"x":2}"#].concat(),
            ManifestParseFailure::TooDeep,
        ),
        // Too deep, then a syntax error after it: too deep.
        (
            [deep.as_bytes(), b"@"].concat(),
            ManifestParseFailure::TooDeep,
        ),
        // A syntax error before the depth excess is the failure.
        (
            [b"@".as_slice(), deep.as_bytes()].concat(),
            ManifestParseFailure::Syntax,
        ),
    ];
    for (body, want) in cases {
        assert_eq!(
            parse_manifest(&body),
            Err(want),
            "body {:?}",
            String::from_utf8_lossy(&body[..body.len().min(40)])
        );
    }
}

// ---------------------------------------------------------------------------------------
// Problem shape and content leakage
// ---------------------------------------------------------------------------------------

// DESIGN §3.3: the problem of a failed parse has the failure's location and rule and a
// non-empty message. DESIGN §4.4, §4.8 (c): no body content in the message or the debug text.
#[test]
fn problems_of_failed_parses_carry_no_body_content() {
    let bodies: &[&[u8]] = &[
        br#"{"SENTINEL-KEY": SENTINEL-VALUE}"#,
        b"{\"SENTINEL\":\"\xff\"}",
        br#"{"k":"SENTINEL","k":"SENTINEL"}"#,
        br#"{"SENTINEL":[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[["#,
    ];
    let mut deep = b"{\"SENTINEL\":".to_vec();
    deep.extend(nested_arrays(MAX_JSON_DEPTH + 5));
    deep.push(b'}');
    let all: Vec<&[u8]> = bodies.iter().copied().chain([deep.as_slice()]).collect();
    for body in all {
        let err = parse_manifest(body).expect_err("invalid body");
        let problem = err.to_problem();
        assert_eq!(problem.rule, err.rule());
        assert_eq!(problem.location, err.location());
        assert!(!problem.message.is_empty());
        assert!(
            !problem.message.contains("SENTINEL"),
            "message leaks: {}",
            problem.message
        );
        if !matches!(err, ManifestParseFailure::DuplicateMember { .. }) {
            assert!(
                !format!("{err:?}").contains("SENTINEL"),
                "debug leaks: {err:?}"
            );
        }
    }
}

// A repeat nested under a name that holds content points at it by location only; the
// location is the one place a name appears, and the message stays free of it.
#[test]
fn a_duplicate_location_is_a_pointer_and_the_message_has_no_names() {
    let err = parse_manifest(br#"{"SENTINEL":{"x":1,"x":2}}"#).expect_err("repeat");
    assert_eq!(err.location(), "/SENTINEL");
    assert!(!err.to_problem().message.contains("SENTINEL"));
}

// F-004, F-009: the duplicate location is bounded. Objects nested 125 deep, each under a key of
// 8000 bytes, then a repeat at the bottom: the location is a whole-segment prefix of the path,
// at most `LOCATION_MAX_BYTES`, found without keeping a copy of the path while reading.
#[test]
fn a_long_duplicate_location_is_cut_at_a_segment_boundary() {
    let key = "a".repeat(8000);
    let mut body = format!("{{\"{key}\":").repeat(125);
    body.push_str("{\"x\":1,\"x\":2}");
    body.push_str(&"}".repeat(125));
    let err = parse_manifest(body.as_bytes()).expect_err("a repeat");
    assert_eq!(err.rule(), "duplicate_member");
    assert_eq!(
        err.location(),
        "",
        "the first segment alone is over the bound"
    );
    // Short segments: whole segments up to the bound.
    let key = "b".repeat(99);
    let mut body = format!("{{\"{key}\":").repeat(100);
    body.push_str("{\"x\":1,\"x\":2}");
    body.push_str(&"}".repeat(100));
    let err = parse_manifest(body.as_bytes()).expect_err("a repeat");
    let loc = err.location();
    assert!(
        loc.len() <= LOCATION_MAX_BYTES && loc.len() > LOCATION_MAX_BYTES - 100,
        "{}",
        loc.len()
    );
    assert!(
        loc.split('/').skip(1).all(|seg| seg == key),
        "whole segments only"
    );
    assert_eq!(err.to_problem().location, loc);
    // Escaped segments (`~` and `/` in keys) are cut on the escaped form, never inside it.
    let key = "~/".repeat(4000);
    let mut body = format!("{{\"{key}\":").repeat(125);
    body.push_str("{\"a\":1,\"a\":2}");
    body.push_str(&"}".repeat(125));
    let err = parse_manifest(body.as_bytes()).expect_err("a repeat");
    assert!(err.location().len() <= LOCATION_MAX_BYTES);
}

// A repeat below arrays names the indices on the way, and the one below a member the member.
#[test]
fn a_duplicate_location_below_arrays_names_indices_and_members() {
    let err = parse_manifest(br#"{"a":[0,[1,{"b~/":[{"x":1,"x":2}]}]]}"#).expect_err("repeat");
    assert_eq!(err.location(), "/a/1/1/b~0~1/0");
    let err = parse_manifest(br#"[0,1,{"x":1,"x":2}]"#).expect_err("repeat");
    assert_eq!(err.location(), "/2");
    // The first failure in document order is still the one reported.
    let err = parse_manifest(br#"{"a":{"x":1,"x":2},"b":{"y":1,"y":2}}"#).expect_err("repeat");
    assert_eq!(err.location(), "/a");
}

// `cap_location` keeps a short location, cuts a long one at the last `/` within the bound.
#[test]
fn cap_location_cuts_at_a_segment_boundary() {
    assert_eq!(cap_location(String::new()), "");
    assert_eq!(cap_location("/a/b".to_owned()), "/a/b");
    let exact = format!("/{}", "a".repeat(LOCATION_MAX_BYTES - 1));
    assert_eq!(exact.len(), LOCATION_MAX_BYTES);
    assert_eq!(
        cap_location(exact.clone()),
        exact,
        "at the bound: unchanged"
    );
    let over = format!("{exact}/b");
    assert_eq!(
        cap_location(over),
        exact,
        "a whole segment over the bound is dropped"
    );
    let long_first = format!("/{}", "a".repeat(LOCATION_MAX_BYTES));
    assert_eq!(cap_location(long_first), "");
    // A multi-byte character straddling the bound is never split.
    let multi = format!("/x/{}", "\u{e9}".repeat(LOCATION_MAX_BYTES));
    assert_eq!(cap_location(multi), "/x");
}

// ---------------------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------------------

/// One byte edit of a serialized body: replace, delete or insert (`byte` is from a set that
/// mostly keeps a body well-formed enough to reach deep into the parse).
#[derive(Debug, Clone)]
struct Edit {
    op: u8,
    at: usize,
    byte: u8,
}

fn arb_edit() -> impl Strategy<Value = Edit> {
    (
        0..3u8,
        any::<usize>(),
        prop::sample::select(b"{}[]\",:0123456789 truefalsn\\ua\xff\t-.eE".to_vec()),
    )
        .prop_map(|(op, at, byte)| Edit { op, at, byte })
}

fn apply(mut body: Vec<u8>, edits: &[Edit]) -> Vec<u8> {
    for e in edits {
        let len = body.len() + 1;
        let at = e.at % len;
        match e.op {
            0 if at < body.len() => body[at] = e.byte,
            1 if at < body.len() => {
                body.remove(at);
            }
            _ => body.insert(at, e.byte),
        }
    }
    body
}

/// Near-valid bodies: a serialized value (compact or pretty) with zero to three byte edits.
fn arb_near_valid_body() -> impl Strategy<Value = Vec<u8>> {
    (
        arb_json_value(),
        any::<bool>(),
        prop::collection::vec(arb_edit(), 0..4),
    )
        .prop_map(|(v, pretty, edits)| {
            let body = if pretty {
                serde_json::to_vec_pretty(&v)
            } else {
                serde_json::to_vec(&v)
            }
            .expect("serialize");
            apply(body, &edits)
        })
}

/// Both directions against `serde_json` for a shallow body: the gear's parse never accepts
/// what `serde_json` rejects, accepts the same value otherwise, and refuses only for a typed
/// reason that `serde_json` does not contradict (a repeat is the one thing it tolerates).
fn agrees_with_serde_json(body: &[u8]) -> Result<(), TestCaseError> {
    let ours = parse_manifest(body);
    let reference = serde_json::from_slice::<Value>(body);
    match (&ours, &reference) {
        (Ok(v), Ok(r)) => prop_assert_eq!(v, r),
        (Ok(_), Err(e)) => prop_assert!(false, "accepted a body serde_json rejects: {e}"),
        (Err(ManifestParseFailure::Syntax | ManifestParseFailure::NotUtf8), Ok(_)) => {
            prop_assert!(false, "refused a body serde_json accepts: {ours:?}");
        }
        (Err(ManifestParseFailure::TooDeep), _) => prop_assert!(false, "a shallow body is deep"),
        (Err(_), _) => {}
    }
    Ok(())
}

proptest! {
    #![proptest_config(cfg(128))]

    // DESIGN §4.3: no byte sequence panics the parse, and a failure is one of the typed ones.
    #[test]
    fn arbitrary_bytes_never_panic(body in prop::collection::vec(any::<u8>(), 0..300)) {
        parse_manifest(&body).ok();
    }

    // A value without repeats survives the parse, compact or pretty-printed.
    #[test]
    fn a_serialized_value_parses_back_to_itself(v in arb_json_value()) {
        let compact = serde_json::to_vec(&v).expect("serialize");
        prop_assert_eq!(parse_manifest(&compact), Ok(v.clone()));
        let pretty = serde_json::to_vec_pretty(&v).expect("serialize");
        prop_assert_eq!(parse_manifest(&pretty), Ok(v));
    }

    // Bodies a few bytes away from valid agree with serde_json in both directions.
    #[test]
    fn near_valid_bodies_agree_with_serde_json(body in arb_near_valid_body()) {
        agrees_with_serde_json(&body)?;
    }

    // Token soup (almost never valid) is never accepted unless serde_json accepts it too.
    #[test]
    fn token_soup_agrees_with_serde_json(body in prop::collection::vec(
        prop::sample::select(b"{}[]\",:0123456789 truefalsn\\ua".to_vec()), 0..60)) {
        agrees_with_serde_json(&body)?;
    }
}

// The near-valid generator really reaches parsed values and refusals alike (a property
// that never sees a parsed value pins nothing).
#[test]
fn the_near_valid_generator_reaches_parsed_values_and_refusals() {
    let mut runner = TestRunner::deterministic();
    let strategy = arb_near_valid_body();
    let (mut ok, mut syntax, mut non_utf8) = (0, 0, 0);
    let total = 400;
    for _ in 0..total {
        let body = strategy.new_tree(&mut runner).expect("tree").current();
        match parse_manifest(&body) {
            Ok(_) => ok += 1,
            Err(ManifestParseFailure::Syntax) => syntax += 1,
            Err(ManifestParseFailure::NotUtf8) => non_utf8 += 1,
            Err(ManifestParseFailure::DuplicateMember { .. }) => {}
            Err(ManifestParseFailure::TooDeep) => panic!("a shallow body is deep"),
        }
    }
    assert!(ok * 5 > total, "only {ok} of {total} bodies parse");
    assert!(
        syntax > 0 && non_utf8 > 0,
        "syntax {syntax}, utf8 {non_utf8}"
    );
}

/// The JSON text of a member name: plain (`style` 0), or with every UTF-16 unit as a lower (1) or
/// upper (2) case `u` escape.
fn repeated_key(name: &str, style: u8) -> String {
    use std::fmt::Write;
    if style == 0 {
        return serde_json::to_string(name).expect("key");
    }
    let mut out = String::from("\"");
    for unit in name.encode_utf16() {
        if style == 1 {
            write!(out, "\\u{unit:04x}").expect("write to a string");
        } else {
            write!(out, "\\u{unit:04X}").expect("write to a string");
        }
    }
    out.push('"');
    out
}

/// Serializes `v`, appending to the object at `target` a repeat of its member `dup_key`:
/// plain (`style` 0), or with every UTF-16 unit as a lower (1) or upper (2) case `\u` escape.
fn write_with_repeat(
    v: &Value,
    path: &mut Vec<String>,
    target: &[String],
    (dup_key, style): (&str, u8),
    out: &mut String,
) {
    match v {
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                path.push(i.to_string());
                write_with_repeat(item, path, target, (dup_key, style), out);
                path.pop();
            }
            out.push(']');
        }
        Value::Object(members) => {
            out.push('{');
            for (i, (key, item)) in members.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).expect("key"));
                out.push(':');
                path.push(key.clone());
                write_with_repeat(item, path, target, (dup_key, style), out);
                path.pop();
            }
            if path.as_slice() == target {
                let key = repeated_key(dup_key, style);
                out.push(',');
                out.push_str(&key);
                out.push_str(":null");
            }
            out.push('}');
        }
        other => out.push_str(&serde_json::to_string(other).expect("scalar")),
    }
}

/// The paths (member names and array indexes) of every non-empty object in `v`.
fn object_paths(v: &Value, path: &mut Vec<String>, out: &mut Vec<Vec<String>>) {
    match v {
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                path.push(i.to_string());
                object_paths(item, path, out);
                path.pop();
            }
        }
        Value::Object(members) => {
            if !members.is_empty() {
                out.push(path.clone());
            }
            for (key, item) in members {
                path.push(key.clone());
                object_paths(item, path, out);
                path.pop();
            }
        }
        _ => {}
    }
}

fn pointer_of(path: &[String]) -> String {
    path.iter()
        .map(|s| format!("/{}", escape_pointer_segment(s)))
        .collect::<Vec<_>>()
        .concat()
}

proptest! {
    #![proptest_config(cfg(256))]

    // A repeat injected into any non-empty object of a random value, plain or written with
    // `\u` escapes (surrogate pairs included), is found, with the pointer of that object.
    #[test]
    fn an_injected_repeat_is_reported_at_its_object(
        v in arb_json_value(), pick in any::<usize>(), which in any::<usize>(), style in 0..3u8
    ) {
        let root = json!({"w": v, "n": {"k": 1}});
        let mut paths = Vec::new();
        object_paths(&root, &mut Vec::new(), &mut paths);
        let target = paths[pick % paths.len()].clone();
        let object = root.pointer(&pointer_of(&target)).expect("the object");
        let keys: Vec<&String> = object.as_object().expect("an object").keys().collect();
        let dup_key = keys[which % keys.len()].clone();
        let mut body = String::new();
        write_with_repeat(&root, &mut Vec::new(), &target, (&dup_key, style), &mut body);
        prop_assert_eq!(
            parse_manifest(body.as_bytes()),
            Err(dup(&pointer_of(&target))),
            "body {}", body
        );
    }
}

// ---------------------------------------------------------------------------------------
// escape_pointer_segment
// ---------------------------------------------------------------------------------------

// RFC 6901 section 3: `~` is `~0` and `/` is `~1`; the order of the replacements matters.
#[test]
fn pointer_segments_escape_tilde_and_slash() {
    for (raw, want) in [
        ("", ""),
        ("a", "a"),
        ("a/b", "a~1b"),
        ("a~b", "a~0b"),
        ("~/", "~0~1"),
        ("/~", "~1~0"),
        ("~1", "~01"),
        ("~0", "~00"),
        ("//", "~1~1"),
        ("é 😀%\\\"", "é 😀%\\\""),
    ] {
        assert_eq!(escape_pointer_segment(raw), want, "segment {raw:?}");
    }
}

proptest! {
    #![proptest_config(cfg(128))]

    #[test]
    fn escaping_a_pointer_segment_is_reversible(s in ".{0,20}") {
        let escaped = escape_pointer_segment(&s);
        prop_assert!(!escaped.contains('/'));
        let back = escaped.replace("~1", "/").replace("~0", "~");
        prop_assert_eq!(back, s);
    }
}

// ---------------------------------------------------------------------------------------
// Envelope extraction
// ---------------------------------------------------------------------------------------

fn fields(id: Option<&str>, version: Option<&str>) -> EnvelopeFields {
    EnvelopeFields {
        id: id.map(str::to_owned),
        version: version.map(str::to_owned),
    }
}

// DESIGN §3.2 step 1: the id is taken from the single parse; a field is present only when
// `application.<field>` is a string.
#[test]
fn envelope_fields_are_taken_when_they_are_strings() {
    let cases: Vec<(Value, EnvelopeFields)> = vec![
        (
            json!({"application": {"id": "a", "version": "1"}}),
            fields(Some("a"), Some("1")),
        ),
        (json!({"application": {"id": "a"}}), fields(Some("a"), None)),
        (
            json!({"application": {"version": "1"}}),
            fields(None, Some("1")),
        ),
        (json!({"application": {}}), fields(None, None)),
        (json!({}), fields(None, None)),
        (json!({"sections": {}}), fields(None, None)),
        (
            json!({"application": {"id": 7, "version": null}}),
            fields(None, None),
        ),
        (
            json!({"application": {"id": ["a"], "version": {"v": 1}}}),
            fields(None, None),
        ),
        (
            json!({"application": {"id": true, "version": 1.5}}),
            fields(None, None),
        ),
        (json!({"application": "a"}), fields(None, None)),
        (json!({"application": null}), fields(None, None)),
        (json!({"application": ["a", "1"]}), fields(None, None)),
        (json!([]), fields(None, None)),
        (json!(null), fields(None, None)),
        (json!("a"), fields(None, None)),
        (json!(1), fields(None, None)),
        (json!({"id": "top", "version": "top"}), fields(None, None)),
        (
            json!({"application": {"id": "a", "version": "1", "extra": 1}, "x": 2}),
            fields(Some("a"), Some("1")),
        ),
        (
            json!({"application": {"id": "", "version": ""}}),
            fields(Some(""), Some("")),
        ),
    ];
    for (manifest, want) in cases {
        assert_eq!(extract_envelope(&manifest), want, "manifest {manifest}");
    }
}

// DESIGN §3.1: the id is never case-folded, trimmed, or normalised.
#[test]
fn envelope_text_is_kept_exactly() {
    let id = " App.Id\u{e9}/\u{301} \t";
    let version = "\u{e9}\u{301} V1 ";
    let got = extract_envelope(&json!({"application": {"id": id, "version": version}}));
    assert_eq!(got, fields(Some(id), Some(version)));
    // Two ids that differ only by normalization form stay distinct.
    let nfc = extract_envelope(&json!({"application": {"id": "\u{e9}"}}));
    let nfd = extract_envelope(&json!({"application": {"id": "e\u{301}"}}));
    assert_ne!(nfc.id, nfd.id);
}

// DESIGN §3.2 invariant "one parse, one id": the id of a body is the id of the value its
// single parse produced, also when the id is written with escapes.
#[test]
fn the_envelope_id_comes_from_the_parsed_value() {
    let parsed =
        parse_manifest(br#"{"application":{"id":"\u0078y","version":"\u0031"}}"#).expect("parse");
    let got = extract_envelope(&parsed);
    assert_eq!(got.id.as_deref(), Some("xy"));
    assert_eq!(got.version.as_deref(), Some("1"));
}

// ---------------------------------------------------------------------------------------
// Text limits
// ---------------------------------------------------------------------------------------

// DESIGN §3.1 Manifest, §4.7: non-empty, within `max_envelope_text_length` bytes of
// UTF-8, no control character.
#[test]
fn text_within_the_limits_has_no_violation() {
    for text in [
        "a",
        "app.id",
        &"x".repeat(MAX),
        " leading and trailing ",
        "é",
        "😀",
    ] {
        assert_eq!(text_limit_violations(text, MAX), vec![], "text {text:?}");
    }
}

#[test]
fn empty_text_is_empty_and_nothing_else() {
    assert_eq!(
        text_limit_violations("", MAX),
        vec![TextLimitViolation::Empty]
    );
    assert_eq!(
        text_limit_violations("", 1),
        vec![TextLimitViolation::Empty]
    );
}

// DESIGN §4.7: the length counts bytes of UTF-8, not characters.
#[test]
fn length_counts_bytes_of_utf8() {
    assert_eq!(
        text_limit_violations(&"x".repeat(MAX + 1), MAX),
        vec![TextLimitViolation::TooLong]
    );
    assert_eq!(text_limit_violations(&"\u{e9}".repeat(127), MAX), vec![]); // 254 bytes
    assert_eq!(
        text_limit_violations(&"\u{e9}".repeat(128), MAX), // 256 bytes, 128 chars
        vec![TextLimitViolation::TooLong]
    );
    assert_eq!(text_limit_violations("\u{1F600}\u{1F600}", 8), vec![]);
    assert_eq!(
        text_limit_violations("\u{1F600}\u{1F600}", 7),
        vec![TextLimitViolation::TooLong]
    );
}

// The configured maximum is the bound, not the registry's.
#[test]
fn the_configured_maximum_is_the_bound() {
    assert_eq!(
        text_limit_violations("12345678901", 10),
        vec![TextLimitViolation::TooLong]
    );
    assert_eq!(text_limit_violations("1234567890", 10), vec![]);
    assert_eq!(text_limit_violations("a", 1), vec![]);
    assert_eq!(
        text_limit_violations("ab", 1),
        vec![TextLimitViolation::TooLong]
    );
}

// DESIGN §4.1 "Control character": category Cc, U+0000..=U+001F and U+007F..=U+009F, and
// nothing else (NBSP, line separator, zero-width, BOM are not control characters).
#[test]
fn control_characters_are_exactly_category_cc() {
    let control = ('\u{0}'..='\u{1f}').chain('\u{7f}'..='\u{9f}');
    for c in control {
        let text = format!("a{c}b");
        assert_eq!(
            text_limit_violations(&text, MAX),
            vec![TextLimitViolation::ControlCharacter],
            "U+{:04X}",
            u32::from(c)
        );
        assert_eq!(
            text_limit_violations(&c.to_string(), MAX),
            vec![TextLimitViolation::ControlCharacter]
        );
    }
    for c in [
        '\u{20}', '\u{7e}', '\u{a0}', '\u{ad}', '\u{200b}', '\u{2028}', '\u{2029}', '\u{feff}',
        '\u{e000}',
    ] {
        assert_eq!(
            text_limit_violations(&format!("a{c}b"), MAX),
            vec![],
            "U+{:04X} is not a control character",
            u32::from(c)
        );
    }
}

// Both problems are reported (every problem, not the first), in the documented order.
#[test]
fn a_long_text_with_a_control_character_has_both_violations_in_order() {
    let text = format!("{}\u{0}", "x".repeat(MAX));
    assert_eq!(
        text_limit_violations(&text, MAX),
        vec![
            TextLimitViolation::TooLong,
            TextLimitViolation::ControlCharacter
        ]
    );
}

// DESIGN §3.3 problems: the rule tokens.
#[test]
fn violation_rules_are_the_documented_tokens() {
    assert_eq!(TextLimitViolation::TooLong.rule(), "max_length");
    assert_eq!(
        TextLimitViolation::ControlCharacter.rule(),
        "control_character"
    );
    assert_eq!(TextLimitViolation::Empty.rule(), "minLength");
}

// DESIGN §4.1 "Never-storable id": the envelope limits at the registry bound and
// `is_storable_text` are the one rule.
#[test]
fn the_limits_at_the_registry_bound_agree_with_is_storable_text() {
    let long = "x".repeat(MAX + 1);
    let max = "x".repeat(MAX);
    for id in [
        "",
        "a",
        max.as_str(),
        long.as_str(),
        "a\u{0}",
        "\u{7f}",
        "\u{9f}x",
        "tab\t",
        "é",
        " ",
        "..",
        "a/b",
    ] {
        assert_eq!(
            text_limit_violations(id, MAX).is_empty(),
            is_storable_text(id),
            "id {id:?}"
        );
    }
}

proptest! {
    #![proptest_config(cfg(128))]

    #[test]
    fn the_limits_and_is_storable_text_agree_for_any_text(
        id in "[a-z\u{0}-\u{1f}\u{7f}-\u{9f}\u{e9}]{0,300}"
    ) {
        prop_assert_eq!(text_limit_violations(&id, MAX).is_empty(), is_storable_text(&id));
    }
}

// ---------------------------------------------------------------------------------------
// Envelope limit problems
// ---------------------------------------------------------------------------------------

// DESIGN §3.3: a problem has a JSON Pointer location, a rule and a message.
#[test]
fn limit_problems_name_the_member_and_the_rule() {
    let long = "x".repeat(MAX + 1);
    let got = fields(Some(&long), Some("v\u{1}")).limit_problems(MAX);
    let key: Vec<_> = got
        .iter()
        .map(|p| (p.location.as_str(), p.rule.as_str()))
        .collect();
    assert_eq!(
        key,
        vec![
            ("/application/id", "max_length"),
            ("/application/version", "control_character")
        ]
    );
    assert!(got.iter().all(|p| !p.message.is_empty()));
}

#[test]
fn empty_values_are_reported_as_min_length() {
    let got = fields(Some(""), Some("")).limit_problems(MAX);
    let key: Vec<_> = got
        .iter()
        .map(|p| (p.location.as_str(), p.rule.as_str()))
        .collect();
    assert_eq!(
        key,
        vec![
            ("/application/id", "minLength"),
            ("/application/version", "minLength")
        ]
    );
}

#[test]
fn a_value_that_breaks_two_limits_has_two_problems_in_order() {
    let id = format!("{}\u{0}", "x".repeat(MAX));
    let got = fields(Some(&id), None).limit_problems(MAX);
    let rules: Vec<_> = got.iter().map(|p| p.rule.as_str()).collect();
    assert_eq!(rules, vec!["max_length", "control_character"]);
    assert!(got.iter().all(|p| p.location == "/application/id"));
}

// Missing or non-string members are the envelope schema's problems, not the limits'.
#[test]
fn absent_fields_and_valid_fields_have_no_limit_problem() {
    assert_eq!(fields(None, None).limit_problems(MAX), vec![]);
    assert_eq!(fields(Some("a"), Some("1")).limit_problems(MAX), vec![]);
    assert_eq!(fields(Some("a"), None).limit_problems(MAX), vec![]);
}

#[test]
fn the_configured_maximum_applies_to_both_members() {
    let got = fields(Some("abcdef"), Some("abcdef")).limit_problems(5);
    let key: Vec<_> = got
        .iter()
        .map(|p| (p.location.as_str(), p.rule.as_str()))
        .collect();
    assert_eq!(
        key,
        vec![
            ("/application/id", "max_length"),
            ("/application/version", "max_length")
        ]
    );
}

// DESIGN §3.3 "problems": messages come from the rule, never from the instance value.
#[test]
fn limit_problem_messages_never_carry_the_value() {
    let id = format!("SENTINEL{}\u{0}", "x".repeat(MAX));
    let version = "SENTINEL-V\u{1}";
    for p in fields(Some(&id), Some(version)).limit_problems(MAX) {
        assert!(
            !p.message.contains("SENTINEL"),
            "message leaks: {}",
            p.message
        );
    }
}

// ---------------------------------------------------------------------------------------
// within_limits
// ---------------------------------------------------------------------------------------

// DESIGN §3.3 `application`: "id and version, each only when it was extracted and is
// within the envelope limits".
#[test]
fn within_limits_drops_only_the_member_that_breaks_a_limit() {
    let long = "x".repeat(MAX + 1);
    assert_eq!(
        fields(Some(&long), Some("1")).within_limits(MAX),
        fields(None, Some("1"))
    );
    assert_eq!(
        fields(Some("a"), Some("v\u{0}")).within_limits(MAX),
        fields(Some("a"), None)
    );
    assert_eq!(
        fields(Some(""), Some("1")).within_limits(MAX),
        fields(None, Some("1"))
    );
    assert_eq!(
        fields(Some("a"), Some("1")).within_limits(MAX),
        fields(Some("a"), Some("1"))
    );
    assert_eq!(fields(None, None).within_limits(MAX), fields(None, None));
    assert_eq!(
        fields(Some("abcdef"), Some("abc")).within_limits(5),
        fields(None, Some("abc"))
    );
}

// ---------------------------------------------------------------------------------------
// Fuzz seed corpus (P9 fuzz target `manifest_parser`)
// ---------------------------------------------------------------------------------------

const KINDS: [&str; 5] = ["valid", "syntax", "utf8", "depth", "duplicate"];

fn seed_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/manifest_parser")
}

/// The kind of a seed file name (`<kind>-<what>.seed`), or `None` for a file that is not a
/// seed: a fuzzer's own output in the same directory (hash-named) is not ours to test.
fn kind_of(name: &str) -> Option<&'static str> {
    KINDS.into_iter().find(|k| {
        name.strip_prefix(k)
            .is_some_and(|rest| rest.starts_with('-'))
    })
}

fn seeds() -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = std::fs::read_dir(seed_dir())
        .expect("the seed corpus directory exists")
        .filter_map(|e| {
            let e = e.expect("entry");
            let name = e.file_name().into_string().expect("utf-8 name");
            kind_of(&name)?;
            let bytes = std::fs::read(e.path()).expect("read seed");
            Some((name, bytes))
        })
        .collect();
    out.sort();
    out
}

fn seed(name: &str) -> Vec<u8> {
    seeds()
        .into_iter()
        .find(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("seed {name} exists"))
        .1
}

/// Where each `duplicate-*` seed repeats a name (the JSON Pointer of the object).
fn duplicate_locations() -> BTreeMap<&'static str, String> {
    let at_max = "/a".repeat(MAX_JSON_DEPTH - 1);
    BTreeMap::from([
        ("duplicate-after-many-members.seed", String::new()),
        ("duplicate-application.seed", String::new()),
        ("duplicate-at-max-depth.seed", at_max),
        ("duplicate-before-syntax.seed", String::new()),
        ("duplicate-before-too-deep.seed", String::new()),
        ("duplicate-envelope-id.seed", "/application".to_owned()),
        ("duplicate-escaped-name.seed", String::new()),
        ("duplicate-escaped-solidus.seed", String::new()),
        ("duplicate-identical-values.seed", String::new()),
        ("duplicate-in-array.seed", "/0".to_owned()),
        ("duplicate-in-section.seed", "/sections/net".to_owned()),
        ("duplicate-nested-then-top.seed", String::new()),
        ("duplicate-non-adjacent.seed", String::new()),
        ("duplicate-sections.seed", String::new()),
        ("duplicate-surrogate-pair.seed", String::new()),
        ("duplicate-top-level.seed", String::new()),
        ("duplicate-triple.seed", String::new()),
        ("duplicate-uppercase-hex.seed", String::new()),
    ])
}

// Every seed parses or fails with exactly the typed failure its name states, never panics;
// a valid seed equals what serde_json reads, and also runs through the envelope rules.
#[test]
fn every_seed_parses_or_fails_as_its_name_says() {
    let seeds = seeds();
    assert!(seeds.len() >= 50, "a useful corpus, found {}", seeds.len());
    let locations = duplicate_locations();
    for (name, bytes) in &seeds {
        let got = parse_manifest(bytes);
        match (kind_of(name), got) {
            (Some("valid"), Ok(v)) => {
                let reference: Value = serde_json::from_slice(bytes).expect("serde_json reads it");
                assert_eq!(v, reference, "seed {name}");
                let env = extract_envelope(&v);
                std::hint::black_box(env.limit_problems(MAX));
                std::hint::black_box(env.within_limits(MAX));
            }
            (Some("syntax"), Err(ManifestParseFailure::Syntax))
            | (Some("utf8"), Err(ManifestParseFailure::NotUtf8))
            | (Some("depth"), Err(ManifestParseFailure::TooDeep)) => {}
            (Some("duplicate"), Err(ManifestParseFailure::DuplicateMember { location })) => {
                assert_eq!(Some(&location), locations.get(name.as_str()), "seed {name}");
            }
            (kind, got) => panic!("seed {name} (kind {kind:?}) gave {got:?}"),
        }
    }
    let dup_seeds: BTreeSet<&str> = seeds
        .iter()
        .map(|(n, _)| n.as_str())
        .filter(|n| kind_of(n) == Some("duplicate"))
        .collect();
    assert_eq!(
        dup_seeds,
        locations.keys().copied().collect::<BTreeSet<_>>(),
        "every duplicate seed has an expected location"
    );
}

// The corpus covers every outcome of the parse and the boundary of the depth bound.
#[test]
fn the_seed_corpus_covers_every_parse_outcome() {
    let seeds = seeds();
    let kinds: BTreeSet<&str> = seeds.iter().filter_map(|(n, _)| kind_of(n)).collect();
    assert_eq!(
        kinds,
        BTreeSet::from(KINDS),
        "every seed is named <kind>-<what>.seed"
    );
    for kind in &kinds {
        let n = seeds
            .iter()
            .filter(|(name, _)| kind_of(name) == Some(*kind))
            .count();
        assert!(n >= 3, "kind {kind} has {n} seeds");
    }
    // The bound seeds track MAX_JSON_DEPTH (arrays, objects and a mixture): regenerate them
    // when the constant changes.
    for (name, want) in [
        ("valid-depth-max.seed", nested_arrays(MAX_JSON_DEPTH)),
        ("depth-over-max.seed", nested_arrays(MAX_JSON_DEPTH + 1)),
        (
            "valid-depth-max-objects.seed",
            nested_objects(MAX_JSON_DEPTH),
        ),
        (
            "depth-over-max-objects.seed",
            nested_objects(MAX_JSON_DEPTH + 1),
        ),
        ("valid-depth-max-mixed.seed", nested_mixed(MAX_JSON_DEPTH)),
        (
            "depth-over-max-mixed.seed",
            nested_mixed(MAX_JSON_DEPTH + 1),
        ),
    ] {
        assert_eq!(seed(name), want, "seed {name}");
    }
    // Seeds are small, plain, LF-only files with the `.seed` extension: a corpus the fuzzer
    // starts from, not a dump, and no CRLF that a checkout could have introduced.
    for (name, bytes) in &seeds {
        assert!(
            bytes.len() <= 64 * 1024,
            "seed {name} is {} bytes",
            bytes.len()
        );
        assert!(
            std::path::Path::new(name)
                .extension()
                .is_some_and(|e| e == "seed"),
            "seed {name}"
        );
        assert!(!bytes.contains(&b'\r'), "seed {name} has a carriage return");
    }
}

// Seeds whose exact bytes matter (a BOM, padding, a raw control character, no final newline,
// pretty printing) are pinned byte for byte: a checkout, an editor or `.gitattributes`
// changing them would change what they test.
#[test]
fn the_whitespace_and_encoding_sensitive_seeds_keep_their_exact_bytes() {
    let exact: [(&str, &[u8]); 9] = [
        (
            "valid-whitespace-padded.seed",
            b"  \t\n {\"application\":{\"id\":\"a\",\"version\":\"1\"},\"sections\":{}} \n\n\t",
        ),
        ("syntax-whitespace-only.seed", b" \n\t"),
        ("syntax-empty.seed", b""),
        ("syntax-bom.seed", b"\xEF\xBB\xBF{\"a\":1}"),
        ("syntax-raw-control-in-string.seed", b"[\"a\nb\"]"),
        (
            "valid-minimal.seed",
            br#"{"application":{"id":"app","version":"1"},"sections":{}}"#,
        ),
        ("valid-null.seed", b"null"),
        ("utf8-invalid-byte.seed", b"{\"a\":\"\xff\"}"),
        ("utf8-after-duplicate.seed", b"{\"a\":1,\"a\":2}\xff"),
    ];
    for (name, want) in exact {
        assert_eq!(seed(name), want, "seed {name}");
    }
    // The pretty seed is exactly what serde_json's pretty printer writes (LF, two spaces)
    // and one final newline.
    let pretty = seed("valid-pretty.seed");
    let value: Value = serde_json::from_slice(&pretty).expect("valid");
    let mut want = serde_json::to_vec_pretty(&value).expect("serialize");
    want.push(b'\n');
    assert_eq!(want, pretty);
}

// Scalar roots with trailing bytes are seeds too, so the fuzzer starts near the end-of-input
// check that only a parse of the whole body gives.
#[test]
fn the_scalar_trailing_garbage_cases_are_seeds() {
    for (name, want) in [
        ("syntax-scalar-trailing.seed", &b"0t"[..]),
        ("syntax-scalar-two-tokens.seed", b"1 x"),
        ("syntax-literal-trailing.seed", b"truex"),
        ("syntax-string-trailing.seed", b"\"a\"b"),
        ("syntax-extra-closing.seed", b"[]]"),
    ] {
        assert_eq!(seed(name), want, "seed {name}");
    }
}

// Files without a kind prefix (what a fuzz run writes into the directory) are not seeds.
#[test]
fn files_without_a_kind_prefix_are_not_seeds() {
    assert_eq!(kind_of("valid-minimal.seed"), Some("valid"));
    assert_eq!(kind_of("duplicate-top-level.seed"), Some("duplicate"));
    assert_eq!(kind_of("validity.seed"), None);
    assert_eq!(kind_of("0a1b2c3d4e5f"), None);
    assert_eq!(kind_of(".gitkeep"), None);
    assert_eq!(kind_of("crash-1234"), None);
}
