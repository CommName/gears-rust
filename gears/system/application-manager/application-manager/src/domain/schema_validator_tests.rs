use proptest::prelude::*;
use serde_json::{Map, Value, json};

use super::*;

const LIMIT: u32 = 8;

/// A manifest and the (location, rule) of each problem it has.
type EnvelopeCase = (Value, Vec<(&'static str, &'static str)>);

fn validator() -> SchemaValidator {
    SchemaValidator::new(LIMIT).unwrap()
}

fn manifest(application: &Value) -> Value {
    json!({ "application": application, "sections": {} })
}

fn rules(problems: &[Problem]) -> Vec<(&str, &str)> {
    problems
        .iter()
        .map(|p| (p.location.as_str(), p.rule.as_str()))
        .collect()
}

async fn prepared(key: &str, schema: Value) -> PreparedSection {
    prepare_section(key, SectionSchema::new(schema, "1"))
        .await
        .unwrap()
}

/// A schema whose innermost object is `depth` levels below the root.
fn nested(depth: usize) -> Value {
    (0..depth).fold(json!({}), |inner, _| json!({ "not": inner }))
}

/// A root referring to `$defs/d0`, each `d{i}` evaluating `d{i+1}` twice.
fn doubling(levels: usize) -> Value {
    let mut defs = Map::new();
    for i in 0..levels {
        let next = json!({ "$ref": format!("#/$defs/d{}", i + 1) });
        defs.insert(format!("d{i}"), json!({ "allOf": [next.clone(), next] }));
    }
    defs.insert(format!("d{levels}"), json!({ "type": "string" }));
    json!({ "$ref": "#/$defs/d0", "$defs": defs })
}

/// A root starting a chain of `links` references, ending in a string schema.
fn ref_chain(links: usize) -> Value {
    let mut defs = Map::new();
    for i in 0..links {
        defs.insert(
            format!("a{i}"),
            json!({ "$ref": format!("#/$defs/a{}", i + 1) }),
        );
    }
    defs.insert(format!("a{links}"), json!({ "type": "string" }));
    json!({ "$ref": "#/$defs/a0", "$defs": defs })
}

// ── Envelope ────────────────────────────────────────────────────────────────

#[test]
fn a_valid_envelope_has_no_problem() {
    let ok = manifest(&json!({ "id": "app", "version": "1.0" }));
    assert_eq!(validator().check_envelope(&ok), []);
    let at_limit = manifest(&json!({ "id": "a".repeat(8), "version": "\u{e9}".repeat(4) }));
    assert_eq!(validator().check_envelope(&at_limit), [], "8 bytes each");
    let nbsp = manifest(&json!({ "id": "a\u{a0}b", "version": "1" }));
    assert_eq!(validator().check_envelope(&nbsp), [], "U+00A0 is not Cc");
}

#[test]
fn every_envelope_rule_is_reported_at_its_location() {
    let cases: Vec<EnvelopeCase> = vec![
        (json!([]), vec![("", "type")]),
        (
            json!({ "application": { "id": "a", "version": "1" } }),
            vec![("", "required")],
        ),
        (
            json!({ "application": { "id": "a", "version": "1" }, "sections": {}, "x": 1 }),
            vec![("", "additionalProperties")],
        ),
        (
            manifest(&json!({ "version": "1" })),
            vec![("/application", "required")],
        ),
        (
            manifest(&json!({ "id": "a" })),
            vec![("/application", "required")],
        ),
        (
            manifest(&json!({ "id": "a", "version": "1", "extra": true })),
            vec![("/application", "additionalProperties")],
        ),
        (
            manifest(&json!({ "id": 7, "version": "1" })),
            vec![("/application/id", "type")],
        ),
        (
            manifest(&json!({ "id": "a", "version": "" })),
            vec![("/application/version", "minLength")],
        ),
        (
            manifest(&json!({ "id": "a".repeat(9), "version": "1" })),
            vec![("/application/id", "maxLength")],
        ),
        (
            manifest(&json!({ "id": "a", "version": "\u{e9}".repeat(5) })),
            vec![("/application/version", "maxLength")],
        ),
    ];
    for (input, expected) in cases {
        let problems = validator().check_envelope(&input);
        assert_eq!(rules(&problems), expected, "{input}");
    }
}

#[test]
fn control_characters_are_refused() {
    for c in ['\u{0}', '\u{1f}', '\u{7f}', '\u{9f}', '\n'] {
        let input = manifest(&json!({ "id": format!("a{c}b"), "version": "1" }));
        assert_eq!(
            rules(&validator().check_envelope(&input)),
            [("/application/id", "pattern")],
            "{c:?}"
        );
    }
}

#[test]
fn messages_never_contain_manifest_values() {
    let secret = "SECRET-VALUE";
    let input = json!({
        "application": { "id": format!("{secret}{secret}"), "version": 3, secret: secret },
        "sections": {},
        secret: secret,
    });
    let problems = validator().check_envelope(&input);
    assert!(!problems.is_empty());
    for p in problems {
        assert!(!p.message.contains("SECRET"), "{p:?}");
    }
}

#[test]
fn the_published_envelope_limits_characters_to_a_quarter_of_the_bytes() {
    let schema = SchemaValidator::new(255).unwrap();
    for field in ["id", "version"] {
        assert_eq!(
            schema.envelope_schema()["properties"][field]["maxLength"],
            63,
            "{field}"
        );
    }
}

#[test]
fn the_smallest_limit_publishes_one_character() {
    let validator = SchemaValidator::new(UTF8_MAX_CHAR_BYTES).unwrap();
    let published = compile(&validator.combined_schema(Vec::new())).unwrap();
    let text = &validator.envelope_schema()["properties"]["id"];
    assert_eq!(text["minLength"], 1);
    assert_eq!(text["maxLength"], 1);
    // One character of the most bytes fits both the schema and the limit.
    let widest = manifest(&json!({ "id": "\u{1F600}", "version": "a" }));
    assert!(published.is_valid(&widest));
    assert!(validator.check_envelope(&widest).is_empty());
    let over = manifest(&json!({ "id": "abcde", "version": "a" }));
    assert!(!published.is_valid(&over));
    assert_eq!(
        rules(&validator.check_envelope(&over)),
        [("/application/id", "maxLength")]
    );
}

// ── Unusable schemas ────────────────────────────────────────────────────────

#[tokio::test]
async fn every_unusable_cause_is_detected() {
    let draft7 = "http://json-schema.org/draft-07/schema#";
    let big = "x".repeat(SERVED_SCHEMA_MAX_BYTES);
    let cases: Vec<(&str, Value, String, UnusableCause)> = vec![
        (
            "too large",
            json!({ "description": big }),
            "1".into(),
            UnusableCause::TooLarge,
        ),
        (
            "too deep",
            nested(SERVED_SCHEMA_MAX_DEPTH),
            "1".into(),
            UnusableCause::TooLarge,
        ),
        (
            "empty version",
            json!({}),
            String::new(),
            UnusableCause::BadSchemaVersion,
        ),
        (
            "long version",
            json!({}),
            "v".repeat(SCHEMA_VERSION_MAX_BYTES + 1),
            UnusableCause::BadSchemaVersion,
        ),
        (
            "not a schema",
            json!(42),
            "1".into(),
            UnusableCause::OtherDialect,
        ),
        (
            "draft-07 root",
            json!({ "$schema": draft7, "type": "object" }),
            "1".into(),
            UnusableCause::OtherDialect,
        ),
        (
            "draft-07 in $defs",
            json!({ "$defs": { "x": { "$schema": draft7 } } }),
            "1".into(),
            UnusableCause::OtherDialect,
        ),
        (
            "draft-07 under a property named like a data keyword",
            json!({ "properties": { "default": { "$schema": draft7 } } }),
            "1".into(),
            UnusableCause::OtherDialect,
        ),
        (
            "unknown dialect",
            json!({ "$schema": "https://example.com/my-dialect" }),
            "1".into(),
            UnusableCause::OtherDialect,
        ),
        (
            "remote ref",
            json!({ "$ref": "https://example.com/s.json" }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "relative ref",
            json!({ "properties": { "a": { "$ref": "other.json" } } }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "dynamic ref",
            json!({ "items": { "$dynamicRef": "urn:other#node" } }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "ref under a property named const",
            json!({ "properties": { "const": { "$ref": "https://example.com" } } }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "ref into a location that is not a subschema",
            json!({
                "$ref": "#/hidden",
                "hidden": { "$schema": draft7, "type": "integer" },
            }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "ref to a map of subschemas",
            json!({ "$defs": { "x": {} }, "$ref": "#/$defs" }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "ref to a missing property",
            json!({ "properties": { "a": {} }, "$ref": "#/properties/x" }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "ref to an undeclared anchor",
            json!({ "$ref": "#nope" }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "ref to an anchor outside every subschema",
            json!({ "$ref": "#hidden", "hidden": { "$anchor": "hidden" } }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "dynamic ref to an undeclared anchor",
            json!({ "items": { "$dynamicRef": "#meta" } }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "ref with a bad percent-encoding",
            json!({ "properties": { "a": {} }, "$ref": "#/properties/%FF" }),
            "1".into(),
            UnusableCause::ExternalRef,
        ),
        (
            "recursive schema",
            json!({ "properties": { "next": { "$ref": "#" } } }),
            "1".into(),
            UnusableCause::TooLarge,
        ),
        (
            "self-referencing definition",
            json!({ "$ref": "#/$defs/a", "$defs": { "a": { "$ref": "#/$defs/a" } } }),
            "1".into(),
            UnusableCause::TooLarge,
        ),
        (
            "two definitions referring to each other",
            json!({
                "$ref": "#/$defs/a",
                "$defs": { "a": { "$ref": "#/$defs/b" }, "b": { "not": { "$ref": "#/$defs/a" } } },
            }),
            "1".into(),
            UnusableCause::TooLarge,
        ),
        (
            "doubling references",
            doubling(40),
            "1".into(),
            UnusableCause::TooLarge,
        ),
        (
            "reference chain over the limit",
            ref_chain(SCHEMA_REF_CHAIN_MAX),
            "1".into(),
            UnusableCause::TooLarge,
        ),
        (
            "2000-link reference chain",
            ref_chain(2000),
            "1".into(),
            UnusableCause::TooLarge,
        ),
        (
            "nested $id claiming another section",
            json!({ "$defs": { "x": { "$id": "urn:cf:application-manager:section:other" } } }),
            "1".into(),
            UnusableCause::EmbeddedResource,
        ),
        (
            "nested foreign $id",
            json!({ "anyOf": [{ "$id": "https://example.com/x" }] }),
            "1".into(),
            UnusableCause::EmbeddedResource,
        ),
        (
            "bad type",
            json!({ "type": "integerish" }),
            "1".into(),
            UnusableCause::CompileFailed,
        ),
        (
            "look-around pattern",
            json!({ "pattern": "(?=a)b" }),
            "1".into(),
            UnusableCause::CompileFailed,
        ),
    ];
    for (name, schema, version, cause) in cases {
        let result = prepare_section("k", SectionSchema::new(schema, version)).await;
        assert_eq!(result.err(), Some(cause), "{name}");
    }
}

#[tokio::test]
async fn usable_schemas_are_prepared() {
    // Exactly at the size limit: `{"description":"…"}` is 18 bytes of framing.
    let at_limit = json!({ "description": "x".repeat(SERVED_SCHEMA_MAX_BYTES - 18) });
    assert_eq!(
        serde_json::to_vec(&at_limit).unwrap().len(),
        SERVED_SCHEMA_MAX_BYTES
    );
    let cases = [
        json!({}),
        json!(true),
        json!({ "$id": "urn:plugin:own", "type": "object" }),
        json!({ "$schema": "https://json-schema.org/draft/2020-12/schema#" }),
        json!({ "$defs": { "x": { "type": "string" } }, "properties": { "a": { "$ref": "#/$defs/x" } } }),
        json!({ "const": { "$ref": "https://example.com" }, "examples": [{ "$schema": "x", "$id": "y" }] }),
        json!({ "default": { "$id": "data, not a schema" } }),
        json!({ "properties": { "$ref": { "type": "string" }, "$schema": { "type": "string" } } }),
        json!({ "properties": { "a/b": { "type": "string" } }, "$ref": "#/properties/a~1b" }),
        json!({ "properties": { "a b": { "type": "string" } }, "$ref": "#/properties/a%20b" }),
        json!({ "prefixItems": [true], "items": { "$ref": "#/prefixItems/0" } }),
        json!({ "prefixItems": [{}, { "type": "string" }], "items": { "$ref": "#/prefixItems/1" } }),
        json!({ "$defs": { "x": {} }, "properties": { "a": { "$ref": "#/$defs/x" }, "b": { "$ref": "#/$defs/x" } } }),
        // The root's reference and each link are one hop.
        ref_chain(SCHEMA_REF_CHAIN_MAX - 1),
        doubling(10),
        json!({ "$defs": { "x": { "$anchor": "name" } }, "$ref": "#name" }),
        json!({ "$defs": { "m": { "$dynamicAnchor": "meta" } }, "items": { "$dynamicRef": "#meta" } }),
        nested(SERVED_SCHEMA_MAX_DEPTH - 1),
        at_limit,
    ];
    for schema in cases {
        let version = "v".repeat(SCHEMA_VERSION_MAX_BYTES);
        let result = prepare_section("k", SectionSchema::new(schema.clone(), version)).await;
        assert!(result.is_ok(), "{:?}", result.err());
    }
}

#[test]
fn long_keys_and_many_leaves_prepare_in_linear_space() {
    // 50 levels of 2000-byte keys, thousands of leaf properties.
    let mut schema = json!({});
    for level in 0..50 {
        let mut properties = Map::new();
        properties.insert(format!("{level:0>2000}"), schema);
        for leaf in 0..100 {
            properties.insert(format!("leaf{leaf}"), json!({}));
        }
        schema = json!({ "properties": properties });
    }
    let started = std::time::Instant::now();
    let section = prepare_blocking("k".to_owned(), schema, "1".to_owned());
    assert!(section.is_ok(), "{:?}", section.err());
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    // A reference reaching the deepest key resolves without a pointer index.
    let deep = (0..3).fold(
        json!({}),
        |inner, _| json!({ "properties": { "x".repeat(2000): inner } }),
    );
    let mut with_ref = deep;
    with_ref["$ref"] = json!(format!("#/properties/{0}/properties/{0}", "x".repeat(2000)));
    assert!(prepare_blocking("k".to_owned(), with_ref, "1".to_owned()).is_ok());
}

#[test]
fn pointers_follow_subschema_positions_only() {
    let usable = [
        "#/properties/a",
        "#/properties/a/items",
        "#/prefixItems/0",
        "#/prefixItems/1/not",
        "#/$defs/x",
        "#/properties/a~1b",
    ];
    let schema = json!({
        "properties": { "a": { "items": {} }, "a/b": {} },
        "prefixItems": [true, { "not": {} }],
        "$defs": { "x": {} },
    });
    for target in usable {
        let mut with_ref = schema.clone();
        with_ref["$ref"] = json!(target);
        assert!(
            prepare_blocking("k".to_owned(), with_ref, "1".to_owned()).is_ok(),
            "{target}"
        );
    }
    for target in [
        "#/prefixItems/00",
        "#/prefixItems/01",
        "#/prefixItems/-1",
        "#/prefixItems/2",
        "#/prefixItems",
        "#/properties",
        "#/properties/a/items/x",
        "#/",
        "#/$defs/x/",
    ] {
        let mut with_ref = schema.clone();
        with_ref["$ref"] = json!(target);
        assert_eq!(
            prepare_blocking("k".to_owned(), with_ref, "1".to_owned()).err(),
            Some(UnusableCause::ExternalRef),
            "{target}"
        );
    }
}

// ── Problem limits ──────────────────────────────────────────────────────────

#[tokio::test]
async fn problems_are_capped() {
    let sections = vec![prepared("s", json!({ "items": { "type": "string" } })).await];
    let many_errors = json!({ "s": vec![1; 5000] });
    assert_eq!(
        section_problems(&sections, &many_errors).len(),
        PROBLEMS_MAX
    );

    let unknown: Map<String, Value> = (0..5000).map(|i| (format!("u{i}"), json!({}))).collect();
    let many_unknown = Value::Object(unknown);
    assert_eq!(
        section_problems(&sections, &many_unknown).len(),
        PROBLEMS_MAX
    );

    // Missing and per-section problems share the cap with unknown keys.
    let mut mixed = Map::new();
    mixed.insert("s".to_owned(), json!(vec![1; 60]));
    mixed.extend((0..500).map(|i| (format!("u{i}"), json!({}))));
    let problems = section_problems(&sections, &Value::Object(mixed));
    assert_eq!(problems.len(), PROBLEMS_MAX);
    assert_eq!(problems.iter().filter(|p| p.rule == "type").count(), 60);
}

#[test]
fn the_envelope_problems_are_capped() {
    let mut input = manifest(&json!({ "id": "a".repeat(9), "version": "a".repeat(9) }));
    for i in 0..500 {
        input[format!("extra{i}")] = json!(i);
    }
    assert!(validator().check_envelope(&input).len() <= PROBLEMS_MAX);
}

#[test]
fn a_long_location_is_cut_on_a_character_boundary() {
    let key = "\u{e9}".repeat(PROBLEM_LOCATION_MAX_BYTES);
    let problems = section_problems(&[], &json!({ key: {} }));
    assert_eq!(problems.len(), 1);
    let location = &problems[0].location;
    assert!(location.len() <= PROBLEM_LOCATION_MAX_BYTES);
    assert!(location.len() > PROBLEM_LOCATION_MAX_BYTES - UTF8_MAX_CHAR_BYTES as usize);
    assert!(location.starts_with("/sections/\u{e9}"));
}

/// Served JSON whose keys are often schema keywords.
fn served_json() -> impl Strategy<Value = Value> {
    let key = prop_oneof![
        Just("$ref".to_owned()),
        Just("$schema".to_owned()),
        Just("$id".to_owned()),
        Just("properties".to_owned()),
        Just("items".to_owned()),
        Just("allOf".to_owned()),
        Just("type".to_owned()),
        Just("pattern".to_owned()),
        Just("default".to_owned()),
        "[a-z]{1,6}",
    ];
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        "[a-z$#:/(?=)*.\\\\]{0,12}".prop_map(Value::String),
    ];
    leaf.prop_recursive(6, 64, 6, move |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
            prop::collection::btree_map(key.clone(), inner, 0..6)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
    })
}

proptest! {
    #[test]
    fn any_served_json_is_either_prepared_or_unusable(
        schema in served_json(),
        version in ".{0,8}",
    ) {
        // Never panics; a prepared schema is always composed under its key.
        if let Ok(section) = prepare_blocking("k".to_owned(), schema, version) {
            prop_assert_eq!(&section.schema()["$id"], "urn:cf:application-manager:section:k");
        }
    }
}

// ── Composition ─────────────────────────────────────────────────────────────

#[test]
fn compose_section_sets_the_section_id() {
    assert_eq!(
        compose_section("app.cfg", json!({ "$id": "urn:plugin", "type": "object" })),
        json!({ "$id": "urn:cf:application-manager:section:app.cfg", "type": "object" })
    );
    assert_eq!(
        compose_section("k", json!(false)),
        json!({ "$id": "urn:cf:application-manager:section:k", "allOf": [false] })
    );
}

#[tokio::test]
async fn the_combined_schema_embeds_each_composed_section() {
    let raw = json!({ "type": "object", "required": ["n"] });
    let sections = vec![
        prepared("a", raw.clone()).await,
        prepared("b", json!({})).await,
    ];
    let composed_a = sections[0].schema().clone();
    let combined = validator().combined_schema(sections);
    assert_eq!(combined["$schema"], DIALECT);
    assert_eq!(
        combined["properties"]["application"],
        *validator().envelope_schema()
    );
    let s = &combined["properties"]["sections"];
    assert_eq!(s["required"], json!(["a", "b"]));
    assert_eq!(s["additionalProperties"], json!(false));
    assert_eq!(s["properties"]["a"], compose_section("a", raw));
    assert_eq!(s["properties"]["a"], composed_a);
}

// ── Sections ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn section_problems_name_missing_unknown_and_invalid_sections() {
    let sections = vec![
        prepared(
            "num",
            json!({ "type": "object", "properties": { "a": { "properties": { "b": { "minimum": 5 } } } } }),
        )
        .await,
        prepared("gone", json!({})).await,
    ];
    let input = json!({ "num": { "a": { "b": 1 } }, "x/y~z": {} });
    let problems = section_problems(&sections, &input);
    assert_eq!(
        rules(&problems),
        [
            ("/sections/num/a/b", "minimum"),
            ("/sections/gone", MISSING_SECTION),
            ("/sections/x~1y~0z", UNKNOWN_SECTION),
        ]
    );
}

#[tokio::test]
async fn every_section_problem_is_listed_without_values() {
    let sections = vec![
        prepared(
            "s",
            json!({
                "type": "object",
                "required": ["need"],
                "additionalProperties": false,
                "properties": { "n": { "type": "integer" }, "e": { "enum": ["x"] } }
            }),
        )
        .await,
    ];
    let input = json!({ "s": { "n": "SECRET", "e": "SECRET", "SECRET": "SECRET" } });
    let problems = section_problems(&sections, &input);
    let mut found = rules(&problems);
    found.sort_unstable();
    assert_eq!(
        found,
        [
            ("/sections/s", "additionalProperties"),
            ("/sections/s", "required"),
            ("/sections/s/e", "enum"),
            ("/sections/s/n", "type"),
        ]
    );
    for p in &problems {
        assert!(!p.message.contains("SECRET"), "{p:?}");
    }
}

#[test]
fn sections_must_be_an_object() {
    assert_eq!(
        rules(&section_problems(&[], &json!([]))),
        [("/sections", "type")]
    );
}

#[test]
fn with_no_plugins_only_empty_sections_are_valid() {
    assert_eq!(section_problems(&[], &json!({})), []);
    assert_eq!(
        rules(&section_problems(&[], &json!({ "a": {} }))),
        [("/sections/a", UNKNOWN_SECTION)]
    );
}

#[tokio::test]
async fn validate_sections_returns_the_section_problems() {
    let sections: Arc<[PreparedSection]> =
        Arc::from(vec![prepared("s", json!({ "type": "object" })).await]);
    let input = json!({ "s": 1, "extra": {} });
    let expected = section_problems(&sections, &input);
    assert_eq!(expected.len(), 2);
    assert_eq!(validate_sections(sections, input).await, Ok(expected));
}

// ── One rule for discovery and seed (PRD goal) ──────────────────────────────

#[tokio::test]
async fn validation_agrees_with_the_combined_schema() {
    let cfg = json!({ "type": "object", "required": ["name"], "properties": { "name": { "type": "string" } } });
    let noop = json!({ "type": "object" });
    let sections = vec![
        prepared("cfg", cfg.clone()).await,
        prepared("noop", noop.clone()).await,
    ];
    let v = validator();
    let published = v.combined_schema(vec![
        prepared("cfg", cfg).await,
        prepared("noop", noop).await,
    ]);
    let combined = jsonschema::draft202012::new(&published).unwrap();
    let with = |application: Value, sections: Value| json!({ "application": application, "sections": sections });
    let good_sections = json!({ "cfg": { "name": "x" }, "noop": {} });

    // Whatever the published schema admits, validation accepts: ASCII and
    // multibyte texts alike.
    for (id, version) in [
        ("ap", "1"),
        ("\u{e9}\u{e9}", "\u{1f600}\u{1f600}"),
        ("ab", "\u{e9}"),
    ] {
        let built = with(
            json!({ "id": id, "version": version }),
            good_sections.clone(),
        );
        assert!(combined.is_valid(&built), "{built}");
        assert_eq!(v.check_envelope(&built), [], "{built}");
        assert_eq!(
            section_problems(&sections, &built["sections"]),
            [],
            "{built}"
        );
    }

    // Whatever validation refuses, the published schema refuses too.
    let application = json!({ "id": "ap", "version": "1" });
    for bad_sections in [
        json!({ "cfg": { "name": 1 }, "noop": {} }),
        json!({ "cfg": { "name": "x" } }),
        json!({ "cfg": { "name": "x" }, "noop": {}, "other": {} }),
    ] {
        let bad = with(application.clone(), bad_sections);
        assert!(
            !section_problems(&sections, &bad["sections"]).is_empty(),
            "{bad}"
        );
        assert!(!combined.is_valid(&bad), "{bad}");
    }
    for bad_application in [
        json!({ "id": "a".repeat(9), "version": "1" }),
        json!({ "id": "\u{e9}".repeat(5), "version": "1" }),
        json!({ "id": "a\nb", "version": "1" }),
    ] {
        let bad = with(bad_application, good_sections.clone());
        assert!(!v.check_envelope(&bad).is_empty(), "{bad}");
        assert!(!combined.is_valid(&bad), "{bad}");
    }
}

#[test]
fn causes_have_stable_names() {
    let names: Vec<_> = [
        UnusableCause::TooLarge,
        UnusableCause::BadSchemaVersion,
        UnusableCause::OtherDialect,
        UnusableCause::ExternalRef,
        UnusableCause::EmbeddedResource,
        UnusableCause::CompileFailed,
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    assert_eq!(
        names,
        [
            "too_large",
            "bad_schema_version",
            "other_dialect",
            "external_ref",
            "embedded_resource",
            "compile_failed"
        ]
    );
}
