//! Schema validator (`cpt-cf-application-manager-component-schema-validator`).
//!
//! Owns the envelope schema and the one composition rule that seed validation
//! and the discovery combined schema share: each section schema is placed
//! under its key with the `$id` `urn:cf:application-manager:section:<key>`.
//! Each registered section schema is compiled once at `start`, as JSON Schema
//! Draft-07, and kept in memory until the next start. One that refers outside
//! its own document or does not compile is `unusable` and fails startup. No
//! problem message contains a manifest value.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::iter;
use std::sync::Arc;

use jsonschema::{PatternOptions, Retrieve, Uri, Validator};
use serde_json::{Map, Value, json};
use toolkit_macros::domain_model;

use crate::constants::{
    PROBLEM_LOCATION_MAX_BYTES, PROBLEMS_MAX, SCHEMA_EVALUATION_PATHS_MAX, SCHEMA_REF_CHAIN_MAX,
    SCHEMA_REGEX_DFA_SIZE_LIMIT, SCHEMA_REGEX_SIZE_LIMIT, cut,
};

/// The dialect of every section schema and of the combined schema.
pub const DIALECT: &str = "http://json-schema.org/draft-07/schema#";

/// Prefix of the `$id` a section schema is composed under.
pub const SECTION_ID_PREFIX: &str = "urn:cf:application-manager:section:";

/// Rule token of a section key with no section in the manifest.
pub const MISSING_SECTION: &str = "missing_section";

/// Rule token of a manifest section no section plugin owns.
pub const UNKNOWN_SECTION: &str = "unknown_section";

/// Text with no Unicode control character (general category Cc: U+0000 to
/// U+001F and U+007F to U+009F), as an ECMA-262 and `regex` pattern.
const NO_CONTROL_CHARACTER: &str = r"^[^\u0000-\u001F\u007F-\u009F]*$";

/// Draft-07 keywords whose value is one subschema. `items` is one subschema
/// only when it is not an array.
const SUBSCHEMA_KEYWORDS: [&str; 9] = [
    "additionalItems",
    "additionalProperties",
    "contains",
    "else",
    "if",
    "items",
    "not",
    "propertyNames",
    "then",
];

/// Draft-07 keywords whose value is an array of subschemas. `items` is one
/// only when it is an array.
const SUBSCHEMA_ARRAY_KEYWORDS: [&str; 4] = ["allOf", "anyOf", "items", "oneOf"];

/// Draft-07 keywords whose value maps names to subschemas. A `dependencies`
/// member that is an array of property names is not a subschema.
const SUBSCHEMA_MAP_KEYWORDS: [&str; 4] = [
    "definitions",
    "dependencies",
    "patternProperties",
    "properties",
];

/// One manifest problem.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    /// JSON Pointer into the manifest.
    pub location: String,
    /// A JSON Schema keyword, [`MISSING_SECTION`], or [`UNKNOWN_SECTION`].
    pub rule: String,
    /// Names the rule only; never contains a manifest value.
    pub message: String,
}

impl Problem {
    fn new(mut location: String, rule: &str) -> Self {
        let end = cut(&location, PROBLEM_LOCATION_MAX_BYTES).len();
        location.truncate(end);
        let message = match rule {
            MISSING_SECTION => "no section for this section key".to_owned(),
            UNKNOWN_SECTION => "no section plugin owns this section key".to_owned(),
            keyword => format!("the value does not satisfy `{keyword}`"),
        };
        Self {
            location,
            rule: rule.to_owned(),
            message,
        }
    }
}

/// Why a registered section schema is `unusable`, in check order.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnusableCause {
    /// A `$ref` at the root: Draft-07 ignores its siblings, so the composed
    /// `$id` would not apply.
    RootRef,
    /// A reference to anything but a subschema of the schema's own document.
    ExternalRef,
    /// A subschema with its own non-fragment `$id`, which would start a second
    /// resource that the reference check never saw.
    EmbeddedResource,
    /// More than `SCHEMA_EVALUATION_PATHS_MAX` evaluation paths, or a chain of
    /// more than `SCHEMA_REF_CHAIN_MAX` references. A schema that reaches
    /// itself through `$ref` has no bound on its evaluation paths and is also
    /// `TooComplex`.
    TooComplex,
    /// Not an object or boolean schema, or it does not compile, including a
    /// pattern over the regex limits.
    CompileFailed,
}

impl UnusableCause {
    /// The cause as written to logs and startup failures.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RootRef => "root_ref",
            Self::ExternalRef => "external_ref",
            Self::EmbeddedResource => "embedded_resource",
            Self::TooComplex => "too_complex",
            Self::CompileFailed => "compile_failed",
        }
    }
}

impl fmt::Display for UnusableCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A section schema composed under its key and compiled once at `start`. The
/// parts cannot change after compilation, so the schema published and the
/// schema validated are always the same.
#[domain_model]
pub struct CompiledSection {
    key: String,
    section_type: String,
    schema: Value,
    validator: Validator,
}

impl CompiledSection {
    /// The section key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The exact GTS id of the section type.
    #[must_use]
    pub fn section_type(&self) -> &str {
        &self.section_type
    }

    /// The section type's schema composed under the key
    /// ([`compose_section`]).
    #[must_use]
    pub fn schema(&self) -> &Value {
        &self.schema
    }
}

impl fmt::Debug for CompiledSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledSection")
            .field("key", &self.key)
            .field("section_type", &self.section_type)
            .finish_non_exhaustive()
    }
}

/// Section validation stopped before it produced a result. Its panic text is
/// not kept: it may quote the manifest.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("section validation aborted")]
pub struct ValidationAborted;

/// The gear's own envelope schema could not be compiled.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("envelope schema does not compile: {0}")]
pub struct EnvelopeSchemaError(String);

/// The envelope schema and the composition of the combined schema.
#[domain_model]
pub struct SchemaValidator {
    envelope_schema: Value,
    manifest_shape: Validator,
    max_text_bytes: usize,
}

impl fmt::Debug for SchemaValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaValidator")
            .field("max_text_bytes", &self.max_text_bytes)
            .finish_non_exhaustive()
    }
}

impl SchemaValidator {
    /// A validator whose envelope texts are at most `max_envelope_text_length`
    /// bytes.
    ///
    /// # Errors
    /// [`EnvelopeSchemaError`] when the envelope schema does not compile.
    pub fn new(max_envelope_text_length: u32) -> Result<Self, EnvelopeSchemaError> {
        // Published: the same number as a character limit, which JSON Schema
        // can express; a text within it may still be longer in bytes.
        // Checked: the byte limit itself, in `check_envelope`.
        let envelope_schema = envelope(Some(max_envelope_text_length));
        let shape = manifest_schema(&envelope(None), &Value::Bool(true));
        let manifest_shape = compile(&shape).map_err(|e| EnvelopeSchemaError(e.to_string()))?;
        Ok(Self {
            envelope_schema,
            manifest_shape,
            max_text_bytes: usize::try_from(max_envelope_text_length).unwrap_or(usize::MAX),
        })
    }

    /// The published envelope schema: `application` with exactly `id` and
    /// `version`, each at most `max_envelope_text_length` characters.
    #[must_use]
    pub fn envelope_schema(&self) -> &Value {
        &self.envelope_schema
    }

    /// Problems of the manifest shape and envelope: an object with exactly
    /// `application` and `sections`, and a valid `application` whose texts
    /// are at most the byte limit. The sections themselves are not checked
    /// here.
    #[must_use]
    pub fn check_envelope(&self, manifest: &Value) -> Vec<Problem> {
        let mut problems = problems_of(&self.manifest_shape, manifest, "", PROBLEMS_MAX);
        for field in ["id", "version"] {
            if problems.len() < PROBLEMS_MAX
                && manifest["application"][field]
                    .as_str()
                    .is_some_and(|text| text.len() > self.max_text_bytes)
            {
                problems.push(Problem::new(format!("/application/{field}"), "maxLength"));
            }
        }
        problems
    }

    /// The combined schema of a whole manifest: the envelope schema and every
    /// section schema in `sections`, each required, and no other section.
    #[must_use]
    pub fn combined_schema(&self, sections: &[Arc<CompiledSection>]) -> Value {
        let mut required = Vec::with_capacity(sections.len());
        let mut properties = Map::with_capacity(sections.len());
        for section in sections {
            required.push(Value::String(section.key.clone()));
            properties.insert(section.key.clone(), section.schema.clone());
        }
        let sections_schema = json!({
            "type": "object",
            "required": required,
            "additionalProperties": false,
            "properties": properties,
        });
        manifest_schema(&self.envelope_schema, &sections_schema)
    }
}

/// The `application` schema, with a character limit when one is published.
fn envelope(max_chars: Option<u32>) -> Value {
    let mut text = json!({
        "type": "string",
        "minLength": 1,
        "pattern": NO_CONTROL_CHARACTER,
    });
    if let Some(max) = max_chars {
        text["maxLength"] = json!(max);
    }
    json!({
        "type": "object",
        "required": ["id", "version"],
        "additionalProperties": false,
        "properties": { "id": text, "version": text },
    })
}

/// The manifest schema around `application` and `sections` schemas.
fn manifest_schema(application: &Value, sections: &Value) -> Value {
    json!({
        "$schema": DIALECT,
        "type": "object",
        "required": ["application", "sections"],
        "additionalProperties": false,
        "properties": { "application": application, "sections": sections },
    })
}

/// The one composition rule: `schema` placed under `key`, with the `$id`
/// `urn:cf:application-manager:section:<key>` replacing the registered `$id`,
/// and no `$schema` of its own.
#[must_use]
pub fn compose_section(key: &str, schema: Value) -> Value {
    let id = Value::String(format!("{SECTION_ID_PREFIX}{key}"));
    match schema {
        Value::Object(mut members) => {
            members.remove("$schema");
            members.insert("$id".to_owned(), id);
            Value::Object(members)
        }
        other => json!({ "$id": id, "allOf": [other] }),
    }
}

/// Check, compose, and compile the registered schema of `section_type`, whose
/// key is `key`. Blocking: the caller runs it off the async executor.
///
/// # Errors
/// The [`UnusableCause`] of the first failed check.
pub fn compile_section(
    key: &str,
    section_type: &str,
    schema: &Value,
) -> Result<CompiledSection, UnusableCause> {
    check_registered(schema)?;
    let schema = compose_section(key, schema.clone());
    let validator = compile(&schema).map_err(|_| UnusableCause::CompileFailed)?;
    Ok(CompiledSection {
        key: key.to_owned(),
        section_type: section_type.to_owned(),
        schema,
        validator,
    })
}

/// The checks before compilation, in order.
fn check_registered(schema: &Value) -> Result<(), UnusableCause> {
    let Some(root) = schema.as_object() else {
        return if schema.is_boolean() {
            Ok(())
        } else {
            Err(UnusableCause::CompileFailed)
        };
    };
    if root.contains_key("$ref") {
        return Err(UnusableCause::RootRef);
    }
    let walk = Subschemas::of(schema);
    if walk.objects.iter().any(|(_, members)| {
        members
            .get("$ref")
            .is_some_and(|target| walk.resolve(target).is_none())
    }) {
        return Err(UnusableCause::ExternalRef);
    }
    if walk.objects.iter().any(|(depth, members)| {
        *depth > 0
            && members
                .get("$id")
                .is_some_and(|id| !id.as_str().is_some_and(|id| id.starts_with('#')))
    }) {
        return Err(UnusableCause::EmbeddedResource);
    }
    walk.check_evaluation()
}

/// Whether `value` can be a subschema: an object or a boolean.
fn is_schema(value: &Value) -> bool {
    matches!(value, Value::Object(_) | Value::Bool(_))
}

/// The values in subschema positions of one schema object.
fn subschemas_of(members: &Map<String, Value>) -> impl Iterator<Item = &Value> {
    members.iter().flat_map(|(keyword, value)| {
        let keyword = keyword.as_str();
        let found: Box<dyn Iterator<Item = &Value>> =
            if SUBSCHEMA_KEYWORDS.contains(&keyword) && is_schema(value) {
                Box::new(iter::once(value))
            } else if SUBSCHEMA_ARRAY_KEYWORDS.contains(&keyword) {
                Box::new(value.as_array().into_iter().flatten())
            } else if SUBSCHEMA_MAP_KEYWORDS.contains(&keyword) {
                Box::new(
                    value
                        .as_object()
                        .into_iter()
                        .flat_map(Map::values)
                        .filter(|v| is_schema(v)),
                )
            } else {
                Box::new(iter::empty())
            };
        found
    })
}

/// The subschemas of a registered schema: the root and every value in a
/// Draft-07 subschema position. Annotation and data keywords such as
/// `default` or `examples` are not schemas and are skipped, while property
/// names are never mistaken for keywords. Nothing is copied out of the
/// document: references are resolved on demand.
struct Subschemas<'a> {
    root: &'a Value,
    /// Each object subschema with its depth below the root.
    objects: Vec<(usize, &'a Map<String, Value>)>,
    /// The subschema each fragment-only `$id` (`#name`) names.
    anchors: HashMap<&'a str, &'a Value>,
}

impl<'a> Subschemas<'a> {
    fn of(schema: &'a Value) -> Self {
        let mut walk = Self {
            root: schema,
            objects: Vec::new(),
            anchors: HashMap::new(),
        };
        let mut pending = vec![(schema, 0_usize)];
        while let Some((value, depth)) = pending.pop() {
            let Value::Object(members) = value else {
                continue;
            };
            let below = depth.saturating_add(1);
            pending.extend(subschemas_of(members).map(|child| (child, below)));
            if let Some(name) = members
                .get("$id")
                .and_then(Value::as_str)
                .and_then(|id| id.strip_prefix('#'))
                .filter(|name| !name.is_empty())
            {
                walk.anchors.entry(name).or_insert(value);
            }
            walk.objects.push((depth, members));
        }
        walk
    }

    /// The subschema the reference `target` names: a fragment holding a JSON
    /// Pointer that follows subschema positions from the root, or an anchor.
    /// Any other target could reach content the checks never saw.
    fn resolve(&self, target: &Value) -> Option<&'a Value> {
        let fragment = target.as_str()?.strip_prefix('#')?;
        let fragment = percent_encoding::percent_decode_str(fragment)
            .decode_utf8()
            .ok()?;
        if fragment.is_empty() {
            Some(self.root)
        } else if fragment.starts_with('/') {
            self.at_pointer(&fragment)
        } else {
            self.anchors.get(fragment.as_ref()).copied()
        }
    }

    /// The subschema at `pointer` (RFC 6901), every step of which is a
    /// subschema position.
    fn at_pointer(&self, pointer: &str) -> Option<&'a Value> {
        let mut tokens = pointer
            .strip_prefix('/')?
            .split('/')
            .map(|token| token.replace("~1", "/").replace("~0", "~"));
        let mut current = self.root;
        while let Some(keyword) = tokens.next() {
            let member = current.as_object()?.get(&keyword)?;
            current = if SUBSCHEMA_KEYWORDS.contains(&keyword.as_str()) && is_schema(member) {
                member
            } else if SUBSCHEMA_ARRAY_KEYWORDS.contains(&keyword.as_str()) {
                member.as_array()?.get(array_index(&tokens.next()?)?)?
            } else if SUBSCHEMA_MAP_KEYWORDS.contains(&keyword.as_str()) {
                member.as_object()?.get(&tokens.next()?)?
            } else {
                return None;
            };
        }
        is_schema(current).then_some(current)
    }

    /// Reject a schema whose evaluation paths or reference chains exceed the
    /// limits, or that reaches itself through a reference. An iterative
    /// post-order over the subschema graph: a subschema costs one path plus
    /// the paths of its subschemas and of each target it refers to.
    fn check_evaluation(&self) -> Result<(), UnusableCause> {
        let Value::Object(root) = self.root else {
            return Ok(());
        };
        let mut done: HashMap<*const Map<String, Value>, (u64, usize)> = HashMap::new();
        let mut active: HashSet<*const Map<String, Value>> =
            HashSet::from([std::ptr::from_ref(root)]);
        let mut stack = vec![self.frame(root, 0)];
        while let Some(top) = stack.last_mut() {
            if let Some(edge) = top.edges.get(top.next).copied() {
                top.next += 1;
                let key: *const Map<String, Value> = edge.target;
                if let Some(&(paths, chain)) = done.get(&key) {
                    top.add(paths, chain, edge.hops);
                } else if active.insert(key) {
                    stack.push(self.frame(edge.target, edge.hops));
                } else {
                    return Err(UnusableCause::TooComplex);
                }
            } else {
                let frame = stack.pop().ok_or(UnusableCause::TooComplex)?;
                if frame.paths > SCHEMA_EVALUATION_PATHS_MAX || frame.chain > SCHEMA_REF_CHAIN_MAX {
                    return Err(UnusableCause::TooComplex);
                }
                let key: *const Map<String, Value> = frame.node;
                active.remove(&key);
                done.insert(key, (frame.paths, frame.chain));
                match stack.last_mut() {
                    Some(parent) => parent.add(frame.paths, frame.chain, frame.hops),
                    None => return Ok(()),
                }
            }
        }
        Ok(())
    }

    /// A fresh frame of `node`, reached over `hops` references.
    fn frame(&self, node: &'a Map<String, Value>, hops: usize) -> Frame<'a> {
        let mut frame = Frame {
            node,
            hops,
            edges: Vec::new(),
            next: 0,
            paths: 1,
            chain: 0,
        };
        for (value, hops) in subschemas_of(node).map(|v| (v, 0)).chain(
            node.get("$ref")
                .and_then(|target| self.resolve(target))
                .map(|target| (target, 1)),
        ) {
            match value {
                Value::Object(target) => frame.edges.push(Edge { target, hops }),
                _ => frame.add(1, 0, hops),
            }
        }
        frame
    }
}

/// An object subschema reached from another, over `hops` references (0 or 1).
#[derive(Clone, Copy)]
struct Edge<'a> {
    target: &'a Map<String, Value>,
    hops: usize,
}

/// A subschema whose cost is being added up.
struct Frame<'a> {
    node: &'a Map<String, Value>,
    /// References between the parent and this subschema (0 or 1).
    hops: usize,
    edges: Vec<Edge<'a>>,
    next: usize,
    /// Evaluation paths so far.
    paths: u64,
    /// Longest chain of references below.
    chain: usize,
}

impl Frame<'_> {
    /// Add a finished subschema of `paths` paths and a chain of `chain`
    /// references, reached over `hops` references.
    fn add(&mut self, paths: u64, chain: usize, hops: usize) {
        self.paths = self.paths.saturating_add(paths);
        self.chain = self.chain.max(chain.saturating_add(hops));
    }
}

/// `token` as an array index: decimal digits with no leading zero.
fn array_index(token: &str) -> Option<usize> {
    let valid = !token.is_empty()
        && token.bytes().all(|b| b.is_ascii_digit())
        && (token == "0" || !token.starts_with('0'));
    if valid { token.parse().ok() } else { None }
}

/// Refuses every retrieval: a section schema may not refer outside itself.
struct RefuseRetriever;

impl Retrieve for RefuseRetriever {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!("external reference {uri} is not allowed").into())
    }
}

/// Compile a Draft-07 schema with the linear-time `regex` engine under the
/// regex limits, and no external retrieval.
fn compile(schema: &Value) -> Result<Validator, jsonschema::ValidationError<'static>> {
    jsonschema::draft7::options()
        .with_pattern_options(
            PatternOptions::regex()
                .size_limit(SCHEMA_REGEX_SIZE_LIMIT)
                .dfa_size_limit(SCHEMA_REGEX_DFA_SIZE_LIMIT),
        )
        .with_retriever(RefuseRetriever)
        .build(schema)
}

/// At most `budget` problems of `instance` under `validator`, located below
/// `prefix`.
fn problems_of(
    validator: &Validator,
    instance: &Value,
    prefix: &str,
    budget: usize,
) -> Vec<Problem> {
    let evaluation = validator.evaluate(instance);
    evaluation
        .iter_errors()
        .take(budget)
        .map(|e| {
            Problem::new(
                format!("{prefix}{}", e.instance_location.as_str()),
                e.error.keyword(),
            )
        })
        .collect()
}

/// `key` as one JSON Pointer reference token (RFC 6901).
fn pointer_token(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

/// The problems of `sections` against the compiled section schemas, at most
/// `PROBLEMS_MAX`: missing keys and each section against its schema, then
/// unknown keys.
#[must_use]
pub fn section_problems(compiled: &[Arc<CompiledSection>], sections: &Value) -> Vec<Problem> {
    let Some(members) = sections.as_object() else {
        return vec![Problem::new("/sections".to_owned(), "type")];
    };
    let mut problems = Vec::new();
    for section in compiled {
        let budget = PROBLEMS_MAX.saturating_sub(problems.len());
        if budget == 0 {
            return problems;
        }
        let location = format!("/sections/{}", pointer_token(&section.key));
        match members.get(&section.key) {
            None => problems.push(Problem::new(location, MISSING_SECTION)),
            Some(value) => {
                problems.extend(problems_of(&section.validator, value, &location, budget));
            }
        }
    }
    for key in members.keys() {
        if problems.len() >= PROBLEMS_MAX {
            break;
        }
        if !compiled.iter().any(|s| &s.key == key) {
            problems.push(Problem::new(
                format!("/sections/{}", pointer_token(key)),
                UNKNOWN_SECTION,
            ));
        }
    }
    problems
}

/// [`section_problems`] off the async executor.
///
/// # Errors
/// [`ValidationAborted`] when the validation task did not finish.
pub async fn validate_sections(
    compiled: Arc<[Arc<CompiledSection>]>,
    sections: Value,
) -> Result<Vec<Problem>, ValidationAborted> {
    tokio::task::spawn_blocking(move || section_problems(&compiled, &sections))
        .await
        .map_err(|_| ValidationAborted)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "schema_validator_tests.rs"]
mod tests;
