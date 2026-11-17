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

fn compiled(key: &str, schema: &Value) -> Arc<CompiledSection> {
    Arc::new(compile_section(key, &format!("gts.test.{key}.v1~"), schema).unwrap())
}

fn cause_of(schema: &Value) -> Option<UnusableCause> {
    compile_section("k", "gts.test.k.v1~", schema).err()
}

/// `schema` with a property `r` referring to `target`: a root `$ref` is
/// refused on its own, so references are tested one level down.
fn referring(schema: &Value, target: &str) -> Value {
    let mut with_ref = schema.clone();
    with_ref["properties"]["r"] = json!({ "$ref": target });
    with_ref
}

/// A property referring to `definitions/d0`, each `d{i}` evaluating `d{i+1}`
/// twice.
fn doubling(levels: usize) -> Value {
    let mut defs = Map::new();
    for i in 0..levels {
        let next = json!({ "$ref": format!("#/definitions/d{}", i + 1) });
        defs.insert(format!("d{i}"), json!({ "allOf": [next.clone(), next] }));
    }
    defs.insert(format!("d{levels}"), json!({ "type": "string" }));
    referring(&json!({ "definitions": defs }), "#/definitions/d0")
}

/// A property starting a chain of `links` references, ending in a string
/// schema.
fn ref_chain(links: usize) -> Value {
    let mut defs = Map::new();
    for i in 0..links {
        defs.insert(
            format!("a{i}"),
            json!({ "$ref": format!("#/definitions/a{}", i + 1) }),
        );
    }
    defs.insert(format!("a{links}"), json!({ "type": "string" }));
    referring(&json!({ "definitions": defs }), "#/definitions/a0")
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
fn the_published_envelope_limits_characters_to_the_byte_limit() {
    let schema = SchemaValidator::new(255).unwrap();
    for field in ["id", "version"] {
        let text = &schema.envelope_schema()["properties"][field];
        assert_eq!(text["maxLength"], 255, "{field}");
        assert_eq!(text["minLength"], 1, "{field}");
    }
}

#[test]
fn the_smallest_limit_publishes_one_character_and_checks_one_byte() {
    let validator = SchemaValidator::new(1).unwrap();
    let published = compile(&validator.combined_schema(&[])).unwrap();
    let ascii = manifest(&json!({ "id": "a", "version": "b" }));
    assert!(published.is_valid(&ascii));
    assert!(validator.check_envelope(&ascii).is_empty());
    // One two-byte character fits the published character limit but not the
    // byte limit, which validation enforces.
    let wide = manifest(&json!({ "id": "\u{e9}", "version": "b" }));
    assert!(published.is_valid(&wide));
    assert_eq!(
        rules(&validator.check_envelope(&wide)),
        [("/application/id", "maxLength")]
    );
}

// ── Unusable schemas ────────────────────────────────────────────────────────

#[test]
fn every_unusable_cause_is_detected() {
    let cases: Vec<(&str, Value, UnusableCause)> = vec![
        ("not a schema", json!(42), UnusableCause::CompileFailed),
        (
            "root ref",
            json!({ "definitions": { "x": {} }, "$ref": "#/definitions/x" }),
            UnusableCause::RootRef,
        ),
        (
            "remote ref",
            referring(&json!({}), "https://example.com/s.json"),
            UnusableCause::ExternalRef,
        ),
        (
            "registry ref",
            referring(&json!({}), "gts://gts.cf.core.other.v1~"),
            UnusableCause::ExternalRef,
        ),
        (
            "relative ref",
            referring(&json!({}), "other.json"),
            UnusableCause::ExternalRef,
        ),
        (
            "ref under a property named const",
            json!({ "properties": { "const": { "$ref": "https://example.com" } } }),
            UnusableCause::ExternalRef,
        ),
        (
            "ref into a location that is not a subschema",
            referring(&json!({ "hidden": { "type": "integer" } }), "#/hidden"),
            UnusableCause::ExternalRef,
        ),
        (
            "ref to a map of subschemas",
            referring(&json!({ "definitions": { "x": {} } }), "#/definitions"),
            UnusableCause::ExternalRef,
        ),
        (
            "ref to a missing property",
            referring(&json!({ "properties": { "a": {} } }), "#/properties/x"),
            UnusableCause::ExternalRef,
        ),
        (
            "ref to an undeclared anchor",
            referring(&json!({}), "#nope"),
            UnusableCause::ExternalRef,
        ),
        (
            "ref to an anchor outside every subschema",
            referring(&json!({ "hidden": { "$id": "#hidden" } }), "#hidden"),
            UnusableCause::ExternalRef,
        ),
        (
            "ref into dependencies given as property names",
            referring(
                &json!({ "dependencies": { "a": ["b"] } }),
                "#/dependencies/a",
            ),
            UnusableCause::ExternalRef,
        ),
        (
            "ref with a bad percent-encoding",
            referring(&json!({ "properties": { "a": {} } }), "#/properties/%FF"),
            UnusableCause::ExternalRef,
        ),
        (
            "nested $id claiming another section",
            json!({ "definitions": { "x": { "$id": "urn:cf:application-manager:section:other" } } }),
            UnusableCause::EmbeddedResource,
        ),
        (
            "nested foreign $id",
            json!({ "anyOf": [{ "$id": "https://example.com/x" }] }),
            UnusableCause::EmbeddedResource,
        ),
        (
            "recursive schema",
            json!({ "properties": { "next": { "$ref": "#" } } }),
            UnusableCause::TooComplex,
        ),
        (
            "self-referencing definition",
            referring(
                &json!({ "definitions": { "a": { "$ref": "#/definitions/a" } } }),
                "#/definitions/a",
            ),
            UnusableCause::TooComplex,
        ),
        (
            "two definitions referring to each other",
            referring(
                &json!({
                    "definitions": {
                        "a": { "$ref": "#/definitions/b" },
                        "b": { "not": { "$ref": "#/definitions/a" } }
                    }
                }),
                "#/definitions/a",
            ),
            UnusableCause::TooComplex,
        ),
        (
            "doubling references",
            doubling(40),
            UnusableCause::TooComplex,
        ),
        (
            "reference chain over the limit",
            ref_chain(SCHEMA_REF_CHAIN_MAX),
            UnusableCause::TooComplex,
        ),
        (
            "2000-link reference chain",
            ref_chain(2000),
            UnusableCause::TooComplex,
        ),
        (
            "bad type",
            json!({ "type": "integerish" }),
            UnusableCause::CompileFailed,
        ),
        (
            "look-around pattern",
            json!({ "pattern": "(?=a)b" }),
            UnusableCause::CompileFailed,
        ),
        (
            "pattern over the regex size limit",
            json!({ "pattern": "[\\s\\S]{100000}" }),
            UnusableCause::CompileFailed,
        ),
    ];
    for (name, schema, cause) in cases {
        assert_eq!(cause_of(&schema), Some(cause), "{name}");
    }
}

#[test]
fn usable_schemas_compile() {
    let cases = [
        json!({}),
        json!(true),
        json!({ "$id": "gts://gts.cf.core.application_manager.section.v1~acme.x.y.z.v1~", "type": "object" }),
        json!({ "$schema": DIALECT, "type": "object" }),
        // The base type as the registry inlines it.
        json!({
            "$id": "gts://gts.cf.core.application_manager.section.v1~acme.x.y.z.v1~",
            "$schema": DIALECT,
            "allOf": [{ "description": "base", "type": "object", "x-gts-abstract": true }],
            "properties": { "roles": { "type": "array", "items": { "type": "string" } } }
        }),
        referring(
            &json!({ "definitions": { "x": { "type": "string" } } }),
            "#/definitions/x",
        ),
        json!({ "const": { "$ref": "https://example.com" }, "examples": [{ "$schema": "x", "$id": "y" }] }),
        json!({ "default": { "$id": "data, not a schema" } }),
        json!({ "properties": { "$ref": { "type": "string" }, "$schema": { "type": "string" } } }),
        referring(
            &json!({ "properties": { "a/b": { "type": "string" } } }),
            "#/properties/a~1b",
        ),
        referring(
            &json!({ "properties": { "a b": { "type": "string" } } }),
            "#/properties/a%20b",
        ),
        referring(
            &json!({ "items": [true, { "type": "string" }] }),
            "#/items/1",
        ),
        referring(&json!({ "items": { "type": "string" } }), "#/items"),
        referring(
            &json!({ "dependencies": { "a": { "required": ["b"] } } }),
            "#/dependencies/a",
        ),
        json!({
            "definitions": { "x": {} },
            "properties": { "a": { "$ref": "#/definitions/x" }, "b": { "$ref": "#/definitions/x" } }
        }),
        // The property's reference and each link are one hop.
        ref_chain(SCHEMA_REF_CHAIN_MAX - 1),
        doubling(10),
        referring(
            &json!({ "definitions": { "x": { "$id": "#name" } } }),
            "#name",
        ),
    ];
    for schema in cases {
        let result = compile_section("k", "gts.test.k.v1~", &schema);
        assert!(result.is_ok(), "{schema}: {:?}", result.err());
    }
}

#[test]
fn a_compiled_section_keeps_its_key_type_and_composed_schema() {
    let schema = json!({ "$id": "gts://x", "$schema": DIALECT, "type": "object" });
    let section = compile_section("acme.x", "gts.test.acme.x.v1~", &schema).unwrap();
    assert_eq!(section.key(), "acme.x");
    assert_eq!(section.section_type(), "gts.test.acme.x.v1~");
    assert_eq!(
        section.schema(),
        &json!({ "$id": "urn:cf:application-manager:section:acme.x", "type": "object" })
    );
}

#[test]
fn long_keys_and_many_leaves_are_checked_in_linear_space() {
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
    // The reference walk alone: compiling such a schema is `jsonschema`'s cost.
    let started = std::time::Instant::now();
    assert_eq!(check_registered(&schema), Ok(()));
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    // A reference reaching the deepest key resolves without a pointer index.
    let deep = (0..3).fold(
        json!({}),
        |inner, _| json!({ "properties": { "x".repeat(2000): inner } }),
    );
    let target = format!("#/properties/{0}/properties/{0}", "x".repeat(2000));
    assert_eq!(check_registered(&referring(&deep, &target)), Ok(()));
}

#[test]
fn pointers_follow_subschema_positions_only() {
    let schema = json!({
        "properties": { "a": { "items": {} }, "a/b": {} },
        "items": [true, { "not": {} }],
        "definitions": { "x": {} },
    });
    for target in [
        "#/properties/a",
        "#/properties/a/items",
        "#/items/0",
        "#/items/1/not",
        "#/definitions/x",
        "#/properties/a~1b",
    ] {
        assert!(cause_of(&referring(&schema, target)).is_none(), "{target}");
    }
    for target in [
        "#/items/00",
        "#/items/01",
        "#/items/-1",
        "#/items/2",
        "#/items",
        "#/properties",
        "#/properties/a/items/x",
        "#/",
        "#/definitions/x/",
    ] {
        assert_eq!(
            cause_of(&referring(&schema, target)),
            Some(UnusableCause::ExternalRef),
            "{target}"
        );
    }
}

// ── Problem limits ──────────────────────────────────────────────────────────

#[test]
fn problems_are_capped() {
    let sections = vec![compiled("s", &json!({ "items": { "type": "string" } }))];
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
    assert!(location.len() > PROBLEM_LOCATION_MAX_BYTES - 2);
    assert!(location.starts_with("/sections/\u{e9}"));
}

/// Registered JSON whose keys are often schema keywords.
fn registered_json() -> impl Strategy<Value = Value> {
    let key = prop_oneof![
        Just("$ref".to_owned()),
        Just("$schema".to_owned()),
        Just("$id".to_owned()),
        Just("properties".to_owned()),
        Just("items".to_owned()),
        Just("allOf".to_owned()),
        Just("definitions".to_owned()),
        Just("dependencies".to_owned()),
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
    fn any_registered_json_is_either_compiled_or_unusable(schema in registered_json()) {
        // Never panics; a compiled schema is always composed under its key.
        if let Ok(section) = compile_section("k", "gts.test.k.v1~", &schema) {
            prop_assert_eq!(&section.schema()["$id"], "urn:cf:application-manager:section:k");
            prop_assert!(section.schema().get("$schema").is_none());
        }
    }
}

// ── Composition ─────────────────────────────────────────────────────────────

#[test]
fn compose_section_sets_the_section_id_and_drops_the_dialect() {
    assert_eq!(
        compose_section(
            "app.cfg",
            json!({ "$id": "gts://x", "$schema": DIALECT, "type": "object" })
        ),
        json!({ "$id": "urn:cf:application-manager:section:app.cfg", "type": "object" })
    );
    assert_eq!(
        compose_section("k", json!(false)),
        json!({ "$id": "urn:cf:application-manager:section:k", "allOf": [false] })
    );
}

#[test]
fn the_combined_schema_embeds_each_composed_section() {
    let raw = json!({ "type": "object", "required": ["n"] });
    let sections = vec![compiled("a", &raw), compiled("b", &json!({}))];
    let combined = validator().combined_schema(&sections);
    assert_eq!(combined["$schema"], DIALECT);
    assert_eq!(
        combined["properties"]["application"],
        *validator().envelope_schema()
    );
    let s = &combined["properties"]["sections"];
    assert_eq!(s["required"], json!(["a", "b"]));
    assert_eq!(s["additionalProperties"], json!(false));
    assert_eq!(s["properties"]["a"], compose_section("a", raw));
    assert_eq!(s["properties"]["a"], *sections[0].schema());
}

// ── Sections ────────────────────────────────────────────────────────────────

#[test]
fn section_problems_name_missing_unknown_and_invalid_sections() {
    let sections = vec![
        compiled(
            "num",
            &json!({ "type": "object", "properties": { "a": { "properties": { "b": { "minimum": 5 } } } } }),
        ),
        compiled("gone", &json!({})),
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

#[test]
fn every_section_problem_is_listed_without_values() {
    let sections = vec![compiled(
        "s",
        &json!({
            "type": "object",
            "required": ["need"],
            "additionalProperties": false,
            "properties": { "n": { "type": "integer" }, "e": { "enum": ["x"] } }
        }),
    )];
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
    let sections: Arc<[Arc<CompiledSection>]> =
        Arc::from(vec![compiled("s", &json!({ "type": "object" }))]);
    let input = json!({ "s": 1, "extra": {} });
    let expected = section_problems(&sections, &input);
    assert_eq!(expected.len(), 2);
    assert_eq!(validate_sections(sections, input).await, Ok(expected));
}

// ── One rule for discovery and seed (PRD goal) ──────────────────────────────

#[test]
fn validation_agrees_with_the_combined_schema() {
    let cfg = json!({ "type": "object", "required": ["name"], "properties": { "name": { "type": "string" } } });
    let sections = vec![
        compiled("cfg", &cfg),
        compiled("noop", &json!({ "type": "object" })),
    ];
    let v = validator();
    let combined = jsonschema::draft7::new(&v.combined_schema(&sections)).unwrap();
    let with = |application: Value, sections: Value| json!({ "application": application, "sections": sections });
    let good_sections = json!({ "cfg": { "name": "x" }, "noop": {} });

    // Within the byte limit, whatever the published schema admits,
    // validation accepts.
    for (id, version) in [
        ("ap", "1"),
        ("\u{e9}\u{e9}", "\u{1f600}\u{1f600}"),
        ("abcdefgh", "\u{e9}"),
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

    // Whatever validation refuses, the published schema refuses too, except a
    // text within the character limit but over the byte limit.
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
        json!({ "id": "a\nb", "version": "1" }),
    ] {
        let bad = with(bad_application, good_sections.clone());
        assert!(!v.check_envelope(&bad).is_empty(), "{bad}");
        assert!(!combined.is_valid(&bad), "{bad}");
    }
    let wide = with(
        json!({ "id": "\u{e9}".repeat(5), "version": "1" }),
        good_sections,
    );
    assert!(
        combined.is_valid(&wide),
        "5 characters are within the published limit"
    );
    assert_eq!(
        rules(&v.check_envelope(&wide)),
        [("/application/id", "maxLength")],
        "10 bytes are over the byte limit"
    );
}

#[test]
fn causes_have_stable_names() {
    let names: Vec<_> = [
        UnusableCause::RootRef,
        UnusableCause::ExternalRef,
        UnusableCause::EmbeddedResource,
        UnusableCause::TooComplex,
        UnusableCause::CompileFailed,
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    assert_eq!(
        names,
        [
            "root_ref",
            "external_ref",
            "embedded_resource",
            "too_complex",
            "compile_failed"
        ]
    );
}

#[test]
fn an_uninstall_id_follows_the_envelope_text_rule() {
    let v = validator();
    assert_eq!(v.check_application_id("app"), []);
    assert_eq!(v.check_application_id(&"a".repeat(8)), []);
    assert_eq!(
        rules(&v.check_application_id("")),
        [("/application_id", "minLength")]
    );
    assert_eq!(
        rules(&v.check_application_id(&"\u{e9}".repeat(5))),
        [("/application_id", "maxLength")],
        "10 bytes"
    );
    for c in ['\u{0}', '\u{7}', '\u{9f}', '\n'] {
        assert_eq!(
            rules(&v.check_application_id(&format!("a{c}b"))),
            [("/application_id", "pattern")],
            "{c:?}"
        );
    }
    assert_eq!(v.check_application_id("a\u{a0}b"), [], "U+00A0 is not Cc");
}
