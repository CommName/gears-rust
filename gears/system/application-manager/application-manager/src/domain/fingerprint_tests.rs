//! Unit tests for the schema fingerprint (P7, DESIGN §3.1 "Schema fingerprint" and the
//! final-result hand-off content: each section carries "the fingerprint of the schema used";
//! §4.8 "Unit": "fingerprint stability across formatting and member order").
//!
//! Planned API in `crate::domain::fingerprint` (new module):
//!
//! ```text
//! /// `sha256:` and the lower-case hex SHA-256 of the RFC 8785 canonical JSON of `schema`
//! /// (members sorted, no insignificant whitespace, numbers as IEEE-754 doubles).
//! pub fn schema_fingerprint(schema: &Value) -> String;
//! ```
//!
//! Scope: P7 needs the fingerprint for the final-result content only. The module is a
//! hand-rolled RFC 8785 (no vetted JCS crate is in the workspace), so its vectors live here,
//! pinned against exact canonical strings: the member order of RFC 8785 section 3.2.3 (UTF-16
//! code units, which an astral key and U+FFFD tell from UTF-8 order), nested objects, the
//! number vectors of Appendix B, and the string escaping of section 3.2.2.2. Also pinned: the
//! format, a formatting or member order change never moves the digest, a content change does,
//! and the `1.0` / `1` / `1E0` double equivalence.
//!
//! Precondition (not tested here): a schema reaching the fingerprint passed `check_schemas`,
//! so no integer beyond 2^53 reaches it (a double cannot hold one; the schema validator
//! reports it as cause `number`).

use aws_lc_rs::digest::{SHA256, digest};
use serde_json::{Value, json};

use crate::domain::fingerprint::schema_fingerprint;

fn sha256_of(text: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::from("sha256:");
    for byte in digest(&SHA256, text.as_bytes()).as_ref() {
        write!(out, "{byte:02x}").expect("writing to a string");
    }
    out
}

fn parse(text: &str) -> Value {
    serde_json::from_str(text).expect("valid JSON")
}

#[test]
fn the_fingerprint_is_sha256_and_64_lower_case_hex_digits() {
    let fingerprint = schema_fingerprint(&json!({"type": "object"}));

    let hex = fingerprint
        .strip_prefix("sha256:")
        .expect("the sha256: prefix");
    assert_eq!(hex.len(), 64);
    assert!(
        hex.chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        "{fingerprint}"
    );
}

// The digest is over the canonical text: members sorted, no whitespace.
#[test]
fn the_fingerprint_is_the_digest_of_the_canonical_form() {
    let schema = parse(r#"{ "b": 1, "a": [true, null, "x"] }"#);

    assert_eq!(
        schema_fingerprint(&schema),
        sha256_of(r#"{"a":[true,null,"x"],"b":1}"#)
    );
    assert_eq!(schema_fingerprint(&json!({})), sha256_of("{}"));
    assert_eq!(schema_fingerprint(&json!([])), sha256_of("[]"));
}

#[test]
fn formatting_and_member_order_do_not_change_the_fingerprint() {
    let compact = parse(
        r#"{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}},"required":["a","b"]}"#,
    );
    let pretty = parse(
        r#"
        {
          "required": [ "a", "b" ],
          "properties": {
            "b": { "type": "integer" },
            "a": { "type": "string" }
          },
          "type": "object"
        }
        "#,
    );

    assert_eq!(schema_fingerprint(&compact), schema_fingerprint(&pretty));
}

#[test]
fn any_change_of_content_changes_the_fingerprint() {
    let base = json!({"type": "object", "properties": {"a": {"type": "string"}}});
    let variants = [
        json!({"type": "object", "properties": {"a": {"type": "integer"}}}),
        json!({"type": "object", "properties": {"b": {"type": "string"}}}),
        json!({"type": "object", "properties": {"a": {"type": "string"}}, "x": 1}),
        json!({"type": "array", "properties": {"a": {"type": "string"}}}),
        json!({}),
    ];

    for variant in variants {
        assert_ne!(
            schema_fingerprint(&base),
            schema_fingerprint(&variant),
            "{variant}"
        );
    }
}

// Array order is content; only object member order is not.
#[test]
fn array_order_is_part_of_the_fingerprint() {
    assert_ne!(
        schema_fingerprint(&json!({"required": ["a", "b"]})),
        schema_fingerprint(&json!({"required": ["b", "a"]}))
    );
}

// RFC 8785: a number is read as a double and written in its shortest form, so `1.0`, `1` and
// `1E0` are one value.
#[test]
fn numbers_that_read_as_the_same_double_have_one_fingerprint() {
    let one = schema_fingerprint(&parse(r#"{"maximum": 1}"#));

    assert_eq!(one, schema_fingerprint(&parse(r#"{"maximum": 1.0}"#)));
    assert_eq!(one, schema_fingerprint(&parse(r#"{"maximum": 1E0}"#)));
    assert_eq!(
        schema_fingerprint(&parse(r#"{"maximum": 100}"#)),
        schema_fingerprint(&parse(r#"{"maximum": 1E2}"#))
    );
    assert_ne!(
        one,
        schema_fingerprint(&parse(r#"{"maximum": 2}"#)),
        "a different number is a different schema"
    );
}

#[test]
fn the_same_schema_gives_the_same_fingerprint_on_every_call() {
    let schema = json!({"type": "object", "additionalProperties": false});

    assert_eq!(
        schema_fingerprint(&schema),
        schema_fingerprint(&schema.clone())
    );
}

fn fingerprint_is_digest_of(schema: &Value, canonical: &str) {
    assert_eq!(
        schema_fingerprint(schema),
        sha256_of(canonical),
        "expected the canonical form {canonical}"
    );
}

/// The six-character JSON escape of a code unit below 0x10000, lower-case hex.
fn u_escape(code: u32) -> String {
    format!("\\{}{code:04x}", 'u')
}

fn ch(code: u32) -> char {
    char::from_u32(code).expect("a scalar value")
}

// RFC 8785 section 3.2.3: members sort by their UTF-16 code units. U+1F600 is the pair D83D
// DE00 and so sorts before U+FB33 (UTF-8 and code point order put it after), and an astral key
// sorts before U+FFFD. The members are inserted in descending order.
#[test]
fn members_sort_by_utf16_code_units() {
    let keys = [
        ch(0x0d).to_string(),
        "1".to_owned(),
        ch(0x80).to_string(),
        ch(0xf6).to_string(),
        ch(0x20ac).to_string(),
        ch(0x1f600).to_string(),
        ch(0xfb33).to_string(),
    ];
    let mut object = serde_json::Map::new();
    for (i, key) in keys.iter().enumerate().rev() {
        object.insert(key.clone(), json!(i));
    }

    let canonical = format!(
        "{{\"\\r\":0,\"1\":1,\"{}\":2,\"{}\":3,\"{}\":4,\"{}\":5,\"{}\":6}}",
        keys[2], keys[3], keys[4], keys[5], keys[6]
    );
    fingerprint_is_digest_of(&Value::Object(object), &canonical);
}

#[test]
fn an_astral_key_sorts_before_u_e000_and_u_fffd() {
    let astral = ch(0x10000).to_string();
    let replacement = ch(0xfffd).to_string();
    let private_use = ch(0xe000).to_string();
    let mut object = serde_json::Map::new();
    object.insert(replacement.clone(), json!(1));
    object.insert(private_use.clone(), json!(2));
    object.insert(astral.clone(), json!(3));
    object.insert("a".to_owned(), json!(4));

    let canonical = format!("{{\"a\":4,\"{astral}\":3,\"{private_use}\":2,\"{replacement}\":1}}");
    fingerprint_is_digest_of(&Value::Object(object), &canonical);
}

#[test]
fn nested_objects_sort_at_every_level_and_arrays_keep_their_order() {
    let schema =
        parse(r#"{"z":{"y":1,"x":{"b":[{"d":1,"c":2},{"b":1,"a":2}],"a":null}},"a":[3,1,2]}"#);

    fingerprint_is_digest_of(
        &schema,
        r#"{"a":[3,1,2],"z":{"x":{"a":null,"b":[{"c":2,"d":1},{"a":2,"b":1}]},"y":1}}"#,
    );
}

/// The double nearest to `text` (the standard library parse is exact).
fn double(text: &str) -> Value {
    let number = serde_json::Number::from_f64(text.parse::<f64>().expect("a double"));
    Value::Number(number.expect("a finite double"))
}

// RFC 8785 section 3.2.2.3 and Appendix B: the shortest round-trip digits in the ECMAScript
// `Number::toString` layout, for the double nearest to each number.
#[test]
fn numbers_are_written_as_ecmascript_doubles() {
    let cases = [
        ("0", "0"),
        ("-0", "0"),
        ("-0.0", "0"),
        ("1", "1"),
        ("-1.5", "-1.5"),
        ("0.1", "0.1"),
        ("4.5", "4.5"),
        ("100", "100"),
        ("1E2", "100"),
        ("1e20", "100000000000000000000"),
        ("1e21", "1e+21"),
        ("1E30", "1e+30"),
        ("1.5e300", "1.5e+300"),
        ("1e-6", "0.000001"),
        ("0.000001", "0.000001"),
        ("1e-7", "1e-7"),
        ("0.00000012345", "1.2345e-7"),
        ("333333333.33333329", "333333333.3333333"),
        ("1.7976931348623157e308", "1.7976931348623157e+308"),
        ("5e-324", "5e-324"),
        ("9007199254740992", "9007199254740992"),
        ("123456789012345680000", "123456789012345680000"),
    ];

    for (text, canonical) in cases {
        let schema = json!({"n": double(text)});
        fingerprint_is_digest_of(&schema, &format!("{{\"n\":{canonical}}}"));
    }
    let all = json!([
        double("1e21"),
        double("1e-7"),
        double("-1e21"),
        double("-1e-7")
    ]);
    fingerprint_is_digest_of(&all, "[1e+21,1e-7,-1e+21,-1e-7]");
}

// RFC 8785 section 3.2.2.3 / ECMAScript 7.1.12.1: when two shortest digit strings are equally
// close to the value (an exact tie), the even digit is chosen. These doubles are exact binary
// fractions whose shortest form ends in an exact decimal tie.
#[test]
fn an_exact_tie_of_the_last_digit_rounds_to_even() {
    let cases = [
        ("1658206780088562.25", "1658206780088562.2"),
        ("1658206780088562.75", "1658206780088562.8"),
        ("13804445983131.0625", "13804445983131.062"),
    ];

    for (text, canonical) in cases {
        let schema = json!({"n": double(text)});
        fingerprint_is_digest_of(&schema, &format!("{{\"n\":{canonical}}}"));
    }
}

// The gear enables the `float_roundtrip` feature of `serde_json`: a schema parsed from text gives
// the double nearest to the text, so the fingerprint is the one of the shortest text of that
// double. Without the feature the shortest text of about a third of the doubles (these among
// them) parses one ulp off, and the fingerprint would carry another number.
#[test]
fn a_parsed_float_is_the_nearest_double_and_keeps_its_shortest_text() {
    for text in [
        "1.0715660391465826e-75",
        "-1.81996730402717e-179",
        "-1.603964615428183e143",
    ] {
        let parsed = parse(&format!("{{\"n\":{text}}}"));

        assert_eq!(
            parsed["n"].as_f64(),
            Some(text.parse::<f64>().expect("a double")),
            "{text}"
        );
        let canonical = text.replace('e', "e+").replace("e+-", "e-");
        fingerprint_is_digest_of(&parsed, &format!("{{\"n\":{canonical}}}"));
    }
}

// RFC 8785 section 3.2.2.2: `\b \t \n \f \r \" \\` as two characters, every other control
// character as a lower-case `\u00xx`, and everything else, `/`, DEL, non-ASCII, U+2028, raw.
// Member names are strings too.
#[test]
fn strings_and_member_names_are_escaped_as_rfc_8785_requires() {
    let mut raw = String::new();
    let mut escaped = String::new();
    for (control, short) in [
        (0x08, "\\b"),
        (0x09, "\\t"),
        (0x0a, "\\n"),
        (0x0c, "\\f"),
        (0x0d, "\\r"),
    ] {
        raw.push(ch(control));
        escaped.push_str(short);
    }
    raw.push_str("\"\\");
    escaped.push_str("\\\"\\\\");
    for control in [0x00, 0x01, 0x0b, 0x0e, 0x1f] {
        raw.push(ch(control));
        escaped.push_str(&u_escape(control));
    }
    for same in [
        '/',
        ch(0x7f),
        ch(0xe9),
        ch(0x2028),
        ch(0x2029),
        ch(0x1f600),
        'a',
    ] {
        raw.push(same);
        escaped.push(same);
    }
    let mut object = serde_json::Map::new();
    object.insert(raw.clone(), Value::String(raw));

    fingerprint_is_digest_of(
        &Value::Object(object),
        &format!("{{\"{escaped}\":\"{escaped}\"}}"),
    );
}
