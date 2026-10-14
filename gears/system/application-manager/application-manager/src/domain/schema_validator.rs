//! Schema validator: combined schema and whole-manifest validation (DESIGN §3.2 "Schema
//! Validator").
//!
//! # What is bounded, and what is not
//!
//! Every served schema is analysed iteratively (no recursion, no compile, no clone) before the
//! validation library sees it, and is refused (`unusable`, [`UnusableCause`]) unless all of
//! these hold:
//!
//! - the compact serialization is at most `SERVED_SCHEMA_MAX_BYTES` (the SDK's counter);
//! - the JSON nesting is at most `SCHEMA_MAX_DEPTH`, and so is the effective depth: the longest
//!   path through the graph of schema children and local `$ref`/`$dynamicRef` edges, a child
//!   counting its JSON nesting and a reference hop counting 1 (cycles are cut at the edge that
//!   closes them); chains of references therefore cannot exhaust the stack of the recursive
//!   compile;
//! - no schema node is reached by more than `SCHEMA_MAX_PATHS` distinct paths through the
//!   acyclic part of that graph, so reference DAGs cannot make compile or validation
//!   exponential in the depth;
//! - numbers are inside the double range and integers/integral floats are within 2^53;
//! - references are local, resolve inside the served document exactly as the library resolves
//!   them (percent-decoded, then RFC 6901) and the schema has no nested `$id` (a nested
//!   resource changes what `#` and the pointers mean);
//! - when the schema uses `unevaluatedItems`/`unevaluatedProperties`: no anchor-based reference
//!   and no in-place reference cycle (the library recurses without bound on those);
//! - at most `SCHEMA_MAX_PATTERNS` distinct patterns of at most `SCHEMA_PATTERNS_MAX_BYTES`
//!   bytes together, each compiled once under explicit size limits.
//!
//! References are never resolved from the network or files: the retriever refuses every
//! external document.
//!
//! The answer is bounded too: a validation returns at most `MAX_VALIDATION_PROBLEMS` problems
//! (identical (location, rule, message) problems count once) plus a final `too_many_problems`
//! problem when the list was cut, examines at most `MAX_VALIDATION_ERRORS_SCANNED` library
//! errors, and no location is longer than `LOCATION_MAX_BYTES`.
//!
//! # Residual risk
//!
//! The cost of validating one manifest against a usable schema is not bounded here. A
//! recursive schema that reaches the recursion through two applicators per instance level
//! (`properties` and `allOf`, say) costs `2^d` steps for a manifest `d` levels deep when the
//! instance is valid (an invalid one stops at the problem cap), and the memory of the compiled
//! patterns is bounded by the pattern bounds above times the library's per-pattern limits, not
//! by a global budget. Both are accepted: the orchestrator runs the validation on the blocking
//! pool under its step timeout, which abandons the request but cannot stop a validation that is
//! already running (it keeps its `max_request_tasks` slot until it ends; one still queued is
//! cancelled), and a manifest is at most `MAX_JSON_DEPTH` deep.
//!
//! # Causes
//!
//! A schema with several defects gets the first cause of this precedence: `TooLarge`,
//! `TooDeep`, `Number`, `Dialect`, `ExternalReference`, `Pattern`, `Compile`. `Compile` is the
//! catch-all: a malformed keyword value, a boolean or other non-object root, a non-string
//! `$schema`, `$ref` or `$dynamicRef`, a dangling or unresolvable local reference, a nested
//! `$id`, a duplicate or invalid section key, and any failure of the library itself. In
//! problems the rule token `schema` is the catch-all for a library error with no token of its
//! own.

use std::collections::{BTreeMap, HashMap, HashSet};

use application_manager_sdk::models::Problem;
use application_manager_sdk::section_plugin_api::{
    served_schema_within_limit, validate_section_key,
};
use jsonschema::error::ValidationErrorKind as K;
use jsonschema::{Draft, PatternOptions, Retrieve, Uri};
use serde_json::{Value, json};
use toolkit_macros::domain_model;

use crate::domain::limits::{
    MAX_VALIDATION_ERRORS_SCANNED, MAX_VALIDATION_PROBLEMS, SCHEMA_MAX_DEPTH, SCHEMA_MAX_PATHS,
    SCHEMA_MAX_PATTERNS, SCHEMA_PATTERNS_MAX_BYTES, SCHEMA_REGEX_DFA_SIZE_LIMIT,
    SCHEMA_REGEX_SIZE_LIMIT,
};
use crate::domain::manifest::{cap_location, escape_pointer_segment};

const DRAFT_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";
const MAX_SAFE_INT: u64 = 1 << 53;

/// The schema one section plugin served for a key, exactly as served.
#[domain_model]
#[derive(Debug, Clone, PartialEq)]
pub struct SectionSchema {
    /// The section key.
    pub key: String,
    /// The served schema.
    pub schema: Value,
}

/// Why a served schema is `unusable`.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnusableCause {
    /// `$schema` names another dialect.
    Dialect,
    /// A reference leaves the served document.
    ExternalReference,
    /// A pattern the linear-time engine cannot compile within its limits, or more patterns
    /// than `SCHEMA_MAX_PATTERNS` / `SCHEMA_PATTERNS_MAX_BYTES` allow.
    Pattern,
    /// A number outside the IEEE-754 double range, or an integer (or integral float) beyond 2^53.
    Number,
    /// Larger than `SERVED_SCHEMA_MAX_BYTES`.
    TooLarge,
    /// Nested deeper than `SCHEMA_MAX_DEPTH`, through references too, or a reference graph that
    /// fans out beyond `SCHEMA_MAX_PATHS`.
    TooDeep,
    /// Any other failure: the catch-all (see the module docs).
    Compile,
}

/// A key whose served schema is unusable.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnusableSchema {
    /// The section key.
    pub key: String,
    /// The cause.
    pub cause: UnusableCause,
}

/// The `$id` the gear assigns to the schema resource of `key`.
#[must_use]
pub fn section_schema_id(key: &str) -> String {
    format!("urn:cf:application-manager:section:{key}")
}

/// The combined schema of the manifest (DESIGN §3.2 "Schema Validator"). Defined for usable
/// schemas only: callers go through [`check_schemas`] first.
#[must_use]
pub(crate) fn combined_schema(sections: &[SectionSchema]) -> Value {
    let mut sorted: Vec<&SectionSchema> = sections.iter().collect();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));
    let mut props = serde_json::Map::new();
    let mut required = Vec::new();
    for s in sorted {
        props.insert(s.key.clone(), with_id(&s.key, &s.schema));
        required.push(Value::String(s.key.clone()));
    }
    json!({
        "$schema": DRAFT_2020_12,
        "type": "object",
        "properties": {
            "application": {
                "type": "object",
                "properties": {"id": {"type": "string"}, "version": {"type": "string"}},
                "required": ["id", "version"],
                "additionalProperties": false
            },
            "sections": {
                "type": "object",
                "properties": props,
                "required": required,
                "additionalProperties": false
            }
        },
        "required": ["application", "sections"],
        "additionalProperties": false
    })
}

fn with_id(key: &str, schema: &Value) -> Value {
    let mut schema = schema.clone();
    if let Value::Object(o) = &mut schema {
        o.insert("$id".to_owned(), Value::String(section_schema_id(key)));
    }
    schema
}

struct Refuse;

impl Retrieve for Refuse {
    fn retrieve(&self, _: &Uri<String>) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external references are refused".into())
    }
}

fn options(retriever: impl Retrieve + 'static) -> jsonschema::ValidationOptions {
    jsonschema::options()
        .with_draft(Draft::Draft202012)
        .with_retriever(retriever)
        .with_pattern_options(
            PatternOptions::regex()
                .size_limit(SCHEMA_REGEX_SIZE_LIMIT)
                .dfa_size_limit(SCHEMA_REGEX_DFA_SIZE_LIMIT),
        )
}

// ---- iterative analysis: no recursion before every bound is known to hold ----

fn depth_exceeds(root: &Value, max: usize) -> bool {
    let mut stack = vec![(root, 1usize)];
    while let Some((v, d)) = stack.pop() {
        match v {
            Value::Array(a) => {
                if d > max {
                    return true;
                }
                stack.extend(a.iter().map(|c| (c, d + 1)));
            }
            Value::Object(o) => {
                if d > max {
                    return true;
                }
                stack.extend(o.values().map(|c| (c, d + 1)));
            }
            _ => {}
        }
    }
    false
}

/// 2^53 as a float, the largest magnitude of an integer the platform accepts.
const MAX_SAFE_FLOAT: f64 = 9_007_199_254_740_992.0;

/// A number outside the double range, or an integer beyond 2^53 in magnitude: whether it
/// parses as an integer or as a float with an integral value (`18446744073709551616`, `1e300`),
/// so the rule does not depend on the spelling.
fn has_bad_number(root: &Value) -> bool {
    let mut stack = vec![root];
    while let Some(v) = stack.pop() {
        match v {
            Value::Number(n) => {
                let bad = if let Some(u) = n.as_u64() {
                    u > MAX_SAFE_INT
                } else if let Some(i) = n.as_i64() {
                    i.unsigned_abs() > MAX_SAFE_INT
                } else {
                    n.as_f64().is_none_or(|f| {
                        !f.is_finite() || (f.fract() == 0.0 && f.abs() > MAX_SAFE_FLOAT)
                    })
                };
                if bad {
                    return true;
                }
            }
            Value::Array(a) => stack.extend(a.iter()),
            Value::Object(o) => stack.extend(o.values()),
            _ => {}
        }
    }
    false
}

/// A schema position below a node: the child, the JSON nesting it adds (1 for a keyword whose
/// value is the schema, 2 for a list or a map of schemas) and whether it applies to the same
/// instance location (`allOf`, `not`, ...).
struct Child<'a> {
    node: &'a Value,
    weight: usize,
    in_place: bool,
}

/// What one walk over the schema positions of a served schema found.
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct Scan<'a> {
    other_dialect: bool,
    /// A `$schema`, `$ref` or `$dynamicRef` that is not a string.
    bad_member: bool,
    external_ref: bool,
    /// A `$id` below the root: a nested resource.
    nested_id: bool,
    duplicate_anchor: bool,
    has_unevaluated: bool,
    patterns: Vec<&'a str>,
    /// The schema nodes (objects in a schema position); the root is first.
    nodes: Vec<&'a Value>,
    /// The schema positions below each node, by node index.
    children: Vec<Vec<Child<'a>>>,
    /// Local references: the node index and the reference, starting with `#`.
    refs: Vec<(usize, &'a str)>,
    anchors: HashMap<&'a str, &'a Value>,
}

/// Keywords whose value is one schema.
const SINGLE: [&str; 11] = [
    "items",
    "additionalProperties",
    "unevaluatedItems",
    "unevaluatedProperties",
    "contains",
    "propertyNames",
    "not",
    "if",
    "then",
    "else",
    "contentSchema",
];
/// Keywords whose value is a list of schemas.
const LISTS: [&str; 4] = ["allOf", "anyOf", "oneOf", "prefixItems"];
/// Keywords whose value is a map from names to schemas.
const MAPS: [&str; 5] = [
    "properties",
    "patternProperties",
    "$defs",
    "definitions",
    "dependentSchemas",
];
const IN_PLACE_SINGLE: [&str; 4] = ["not", "if", "then", "else"];
const IN_PLACE_LISTS: [&str; 3] = ["allOf", "anyOf", "oneOf"];

#[allow(clippy::cognitive_complexity)]
fn scan(root: &Value) -> Scan<'_> {
    let mut out = Scan::default();
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        let Value::Object(obj) = node else { continue };
        let idx = out.nodes.len();
        out.nodes.push(node);
        let mut kids: Vec<Child<'_>> = Vec::new();
        match obj.get("$schema") {
            None => {}
            Some(Value::String(s)) if s == DRAFT_2020_12 => {}
            Some(Value::String(_)) => out.other_dialect = true,
            Some(_) => out.bad_member = true,
        }
        for kw in ["$ref", "$dynamicRef"] {
            match obj.get(kw) {
                None => {}
                Some(Value::String(r)) if r.starts_with('#') => out.refs.push((idx, r)),
                Some(Value::String(_)) => out.external_ref = true,
                Some(_) => out.bad_member = true,
            }
        }
        if idx > 0 && obj.contains_key("$id") {
            out.nested_id = true;
        }
        for kw in ["$anchor", "$dynamicAnchor"] {
            if let Some(Value::String(a)) = obj.get(kw)
                && out.anchors.insert(a, node).is_some()
            {
                out.duplicate_anchor = true;
            }
        }
        if let Some(Value::String(p)) = obj.get("pattern") {
            out.patterns.push(p);
        }
        if let Some(Value::Object(pp)) = obj.get("patternProperties") {
            out.patterns.extend(pp.keys().map(String::as_str));
        }
        if obj.contains_key("unevaluatedItems") || obj.contains_key("unevaluatedProperties") {
            out.has_unevaluated = true;
        }
        for kw in SINGLE {
            if let Some(child) = obj.get(kw) {
                stack.push(child);
                kids.push(Child {
                    node: child,
                    weight: 1,
                    in_place: IN_PLACE_SINGLE.contains(&kw),
                });
            }
        }
        for kw in LISTS {
            if let Some(Value::Array(a)) = obj.get(kw) {
                for child in a {
                    stack.push(child);
                    kids.push(Child {
                        node: child,
                        weight: 2,
                        in_place: IN_PLACE_LISTS.contains(&kw),
                    });
                }
            }
        }
        for kw in MAPS {
            if let Some(Value::Object(m)) = obj.get(kw) {
                for child in m.values() {
                    stack.push(child);
                    kids.push(Child {
                        node: child,
                        weight: 2,
                        in_place: kw == "dependentSchemas",
                    });
                }
            }
        }
        out.children.push(kids);
    }
    out
}

/// An edge of the schema graph.
struct Edge {
    to: usize,
    weight: usize,
    /// Keeps the instance location (`allOf`, `not`, a reference, ...).
    in_place: bool,
    /// Closes a cycle (set by [`graph_exceeds_bounds`]); cycles consume instance depth, so
    /// they are cut for the depth and path counts.
    back: bool,
}

/// What building the graph found besides the edges.
#[derive(Default)]
struct RefFacts {
    /// A local reference whose target is not a schema position of this document.
    unresolved: bool,
    /// A reference by anchor name (`#name`).
    by_anchor: bool,
}

/// Percent-decodes a URI fragment (RFC 3986); `None` for a malformed escape or text that is not
/// UTF-8 afterwards.
fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hi = char::from(*b.get(i + 1)?).to_digit(16)?;
            let lo = char::from(*b.get(i + 2)?).to_digit(16)?;
            out.push(u8::try_from(hi * 16 + lo).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The target of a local reference as the validation library resolves it: `#` is the root, a
/// fragment starting with `/` is percent-decoded and then read as an RFC 6901 pointer, any
/// other fragment names an anchor.
fn resolve<'a>(root: &'a Value, sc: &Scan<'a>, fragment: &str) -> Option<&'a Value> {
    if fragment.is_empty() {
        Some(root)
    } else if fragment.starts_with('/') {
        root.pointer(&percent_decode(fragment)?)
    } else {
        sc.anchors.get(fragment).copied()
    }
}

/// The graph of schema nodes: an edge per schema child and per local reference.
fn build_graph<'a>(root: &'a Value, sc: &Scan<'a>) -> (Vec<Vec<Edge>>, RefFacts) {
    let index: HashMap<*const Value, usize> = sc
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (std::ptr::from_ref(*n), i))
        .collect();
    let mut adj: Vec<Vec<Edge>> = sc
        .children
        .iter()
        .map(|kids| {
            kids.iter()
                .filter_map(|c| {
                    index.get(&std::ptr::from_ref(c.node)).map(|&to| Edge {
                        to,
                        weight: c.weight,
                        in_place: c.in_place,
                        back: false,
                    })
                })
                .collect()
        })
        .collect();
    let mut facts = RefFacts::default();
    for (from, r) in &sc.refs {
        let fragment = r.strip_prefix('#').unwrap_or_default();
        if !fragment.is_empty() && !fragment.starts_with('/') {
            facts.by_anchor = true;
        }
        match resolve(root, sc, fragment) {
            Some(target @ Value::Object(_)) => {
                if let Some(&to) = index.get(&std::ptr::from_ref(target)) {
                    adj[*from].push(Edge {
                        to,
                        weight: 1,
                        in_place: true,
                        back: false,
                    });
                } else {
                    // An object that is not a schema position (inside `const`, the `$defs`
                    // container, ...): the library may read it as a schema, we cannot follow it.
                    facts.unresolved = true;
                }
            }
            Some(Value::Bool(_)) => {}
            _ => facts.unresolved = true,
        }
    }
    (adj, facts)
}

/// Whether the graph is too deep or fans out too much for the library: the longest path from
/// the root (JSON nesting of the children plus one per reference hop, cycles cut at the edge
/// that closes them) is over `SCHEMA_MAX_DEPTH`, or some node is reached by more than
/// `SCHEMA_MAX_PATHS` paths of the acyclic part. One iterative depth-first search, memoised: the
/// work is linear in the size of the graph. Marks the cycle-closing edges.
fn graph_exceeds_bounds(adj: &mut [Vec<Edge>]) -> bool {
    let n = adj.len();
    if n == 0 {
        return false;
    }
    let mut state = vec![0u8; n]; // 0 new, 1 on the path, 2 done
    let mut height = vec![0usize; n];
    let mut order = Vec::with_capacity(n);
    let mut stack: Vec<(usize, usize)> = vec![(0, 0)];
    state[0] = 1;
    while let Some(&(v, i)) = stack.last() {
        if i < adj[v].len() {
            let top = stack.len() - 1;
            stack[top].1 = i + 1;
            let to = adj[v][i].to;
            match state[to] {
                0 => {
                    state[to] = 1;
                    stack.push((to, 0));
                }
                1 => adj[v][i].back = true,
                _ => {}
            }
        } else {
            stack.pop();
            state[v] = 2;
            order.push(v);
            height[v] = adj[v]
                .iter()
                .filter(|e| !e.back)
                .map(|e| e.weight + height[e.to])
                .max()
                .unwrap_or(0);
        }
    }
    if 1 + height[0] > SCHEMA_MAX_DEPTH {
        return true;
    }
    // Reverse finishing order is a topological order of the acyclic part.
    let mut paths = vec![0usize; n];
    paths[0] = 1;
    for &v in order.iter().rev() {
        let here = paths[v];
        if here > SCHEMA_MAX_PATHS {
            return true;
        }
        for e in adj[v].iter().filter(|e| !e.back) {
            paths[e.to] = paths[e.to].saturating_add(here).min(SCHEMA_MAX_PATHS + 1);
        }
    }
    false
}

/// Whether the in-place edges (applicators that keep the instance location, and local
/// references) have a cycle. Iterative three-colour search.
fn has_in_place_cycle(adj: &[Vec<Edge>]) -> bool {
    let mut state = vec![0u8; adj.len()];
    for start in 0..adj.len() {
        if state[start] != 0 {
            continue;
        }
        state[start] = 1;
        let mut stack: Vec<(usize, usize)> = vec![(start, 0)];
        while let Some(&(v, i)) = stack.last() {
            let next = (i..adj[v].len()).find(|&j| adj[v][j].in_place);
            if let Some(j) = next {
                let top = stack.len() - 1;
                stack[top].1 = j + 1;
                let to = adj[v][j].to;
                match state[to] {
                    0 => {
                        state[to] = 1;
                        stack.push((to, 0));
                    }
                    1 => return true,
                    _ => {}
                }
            } else {
                stack.pop();
                state[v] = 2;
            }
        }
    }
    false
}

/// Whether the distinct patterns are within the count and byte bounds, checked before any is
/// compiled. Returns the distinct patterns.
fn distinct_patterns<'a>(patterns: &[&'a str]) -> Option<Vec<&'a str>> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut distinct = Vec::new();
    let mut bytes = 0usize;
    for p in patterns {
        if seen.insert(p) {
            bytes = bytes.saturating_add(p.len());
            if distinct.len() >= SCHEMA_MAX_PATTERNS || bytes > SCHEMA_PATTERNS_MAX_BYTES {
                return None;
            }
            distinct.push(*p);
        }
    }
    Some(distinct)
}

fn classify(
    key: &str,
    schema: &Value,
    retriever: impl Fn() -> Box<dyn Retrieve>,
) -> Option<UnusableCause> {
    if !served_schema_within_limit(schema) {
        return Some(UnusableCause::TooLarge);
    }
    if depth_exceeds(schema, SCHEMA_MAX_DEPTH) {
        return Some(UnusableCause::TooDeep);
    }
    let sc = scan(schema);
    let (mut adj, refs) = build_graph(schema, &sc);
    if graph_exceeds_bounds(&mut adj) {
        return Some(UnusableCause::TooDeep);
    }
    if has_bad_number(schema) {
        return Some(UnusableCause::Number);
    }
    if sc.other_dialect {
        return Some(UnusableCause::Dialect);
    }
    if sc.external_ref {
        return Some(UnusableCause::ExternalReference);
    }
    let Some(patterns) = distinct_patterns(&sc.patterns) else {
        return Some(UnusableCause::Pattern);
    };
    for p in patterns {
        if options(Boxed(retriever()))
            .build(&json!({"pattern": p}))
            .is_err()
        {
            return Some(UnusableCause::Pattern);
        }
    }
    if !schema.is_object()
        || sc.bad_member
        || sc.nested_id
        || sc.duplicate_anchor
        || refs.unresolved
    {
        return Some(UnusableCause::Compile);
    }
    // The library recurses without bound on an in-place cycle under `unevaluated*`, and the
    // scan cannot tell which anchor it would pick: both are refused rather than guessed.
    if sc.has_unevaluated && (refs.by_anchor || has_in_place_cycle(&adj)) {
        return Some(UnusableCause::Compile);
    }
    match options(Boxed(retriever())).build(&with_id(key, schema)) {
        Ok(_) => None,
        Err(_) => Some(UnusableCause::Compile),
    }
}

struct Boxed(Box<dyn Retrieve>);

impl Retrieve for Boxed {
    fn retrieve(&self, u: &Uri<String>) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.0.retrieve(u)
    }
}

/// Checks every served schema; the one rule discovery and seed validation share. A key that is
/// not a valid section key, and a key served twice, are unusable too (`Compile`): the combined
/// schema is keyed by it.
///
/// # Errors
///
/// Every key whose served schema is unusable, in key order, once each.
pub fn check_schemas(sections: &[SectionSchema]) -> Result<(), Vec<UnusableSchema>> {
    check_all(sections, || Box::new(Refuse))
}

/// [`check_schemas`] with the reference retriever injected (tests count its calls).
#[cfg(test)]
pub(crate) fn check_schemas_with(
    sections: &[SectionSchema],
    retriever: impl Fn() -> Box<dyn Retrieve>,
) -> Result<(), Vec<UnusableSchema>> {
    check_all(sections, retriever)
}

fn check_all(
    sections: &[SectionSchema],
    retriever: impl Fn() -> Box<dyn Retrieve>,
) -> Result<(), Vec<UnusableSchema>> {
    let mut bad: BTreeMap<&str, UnusableCause> = BTreeMap::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for s in sections {
        if !seen.insert(&s.key) || validate_section_key(&s.key).is_err() {
            bad.insert(&s.key, UnusableCause::Compile);
        } else if let Some(cause) = classify(&s.key, &s.schema, &retriever) {
            bad.entry(&s.key).or_insert(cause);
        }
    }
    if bad.is_empty() {
        return Ok(());
    }
    Err(bad
        .into_iter()
        .map(|(key, cause)| UnusableSchema {
            key: key.to_owned(),
            cause,
        })
        .collect())
}

fn token(kind: &K) -> &'static str {
    match kind {
        K::AdditionalItems { .. } => "additionalItems",
        K::AdditionalProperties { .. } => "additionalProperties",
        K::AnyOf { .. } => "anyOf",
        K::Constant { .. } => "const",
        K::Contains => "contains",
        K::ContentEncoding { .. } => "contentEncoding",
        K::ContentMediaType { .. } => "contentMediaType",
        K::Enum { .. } => "enum",
        K::ExclusiveMaximum { .. } => "exclusiveMaximum",
        K::ExclusiveMinimum { .. } => "exclusiveMinimum",
        K::FalseSchema => "false",
        K::Format { .. } => "format",
        K::MaxItems { .. } => "maxItems",
        K::Maximum { .. } => "maximum",
        K::MaxLength { .. } => "maxLength",
        K::MaxProperties { .. } => "maxProperties",
        K::MinItems { .. } => "minItems",
        K::Minimum { .. } => "minimum",
        K::MinLength { .. } => "minLength",
        K::MinProperties { .. } => "minProperties",
        K::MultipleOf { .. } => "multipleOf",
        K::Not { .. } => "not",
        K::OneOfMultipleValid { .. } | K::OneOfNotValid { .. } => "oneOf",
        K::Pattern { .. } => "pattern",
        K::PropertyNames { .. } => "propertyNames",
        K::Required { .. } => "required",
        K::Type { .. } => "type",
        K::UnevaluatedItems { .. } => "unevaluatedItems",
        K::UnevaluatedProperties { .. } => "unevaluatedProperties",
        K::UniqueItems => "uniqueItems",
        _ => "schema",
    }
}

/// The problem of a rule at a location (capped to `LOCATION_MAX_BYTES`).
fn problem(location: String, rule: &str, message: String) -> Problem {
    Problem {
        location: cap_location(location),
        rule: rule.to_owned(),
        message,
    }
}

/// One problem per member name, at `<location>/<name>`. Stops after one more than the cap: no
/// more can be listed, and one more tells the list is cut.
fn push_members<'a>(
    out: &mut Vec<Problem>,
    location: &str,
    names: impl IntoIterator<Item = &'a String>,
    rule: &str,
    message: &str,
) {
    for name in names.into_iter().take(MAX_VALIDATION_PROBLEMS + 1) {
        out.push(problem(
            format!("{location}/{}", escape_pointer_segment(name)),
            rule,
            message.to_owned(),
        ));
    }
}

/// Whether the last keyword of a schema path (the evaluation path of an error, `/properties/a/
/// additionalProperties`) is `want`, reading the path as keywords and the names and indices
/// that follow `properties`, `allOf`, ...: a property NAMED `additionalProperties` is not it.
fn path_ends_with_keyword(path: &str, want: &str) -> bool {
    const NAMED: [&str; 7] = [
        "properties",
        "patternProperties",
        "dependentSchemas",
        "$defs",
        "definitions",
        "dependentRequired",
        "dependencies",
    ];
    let mut segments = path.split('/').skip(1);
    let mut last_is_want = false;
    while let Some(segment) = segments.next() {
        last_is_want = segment == want;
        if NAMED.contains(&segment) || LISTS.contains(&segment) {
            // The name or the index of the schema below the keyword.
            if segments.next().is_none() {
                return false;
            }
            last_is_want = false;
        }
    }
    last_is_want
}

/// The problems of one library error: at least one, whatever the error.
fn problems_of(err: &jsonschema::ValidationError<'_>, manifest: &Value) -> Vec<Problem> {
    let location = err.instance_path().to_string();
    let mut out = Vec::new();
    match err.kind() {
        K::Required { property } if location == "/sections" => {
            let name = property.as_str().unwrap_or_default();
            out.push(problem(
                format!("/sections/{}", escape_pointer_segment(name)),
                "missing_section",
                "the section is required".to_owned(),
            ));
        }
        K::AdditionalProperties { unexpected } if location == "/sections" => {
            push_members(
                &mut out,
                "/sections",
                unexpected,
                "unknown_section",
                "no section plugin owns this section",
            );
        }
        K::AdditionalProperties { unexpected } => push_members(
            &mut out,
            &location,
            unexpected,
            "additionalProperties",
            "the member is not allowed by the `additionalProperties` constraint",
        ),
        // `additionalProperties: false` with no sibling `properties` is reported by the crate as
        // one false schema at the object (naming no member): every member of that object is
        // unexpected. Only the keyword itself counts, not a property of that name.
        K::FalseSchema
            if path_ends_with_keyword(&err.schema_path().to_string(), "additionalProperties") =>
        {
            if let Some(Value::Object(o)) = manifest.pointer(&location) {
                push_members(
                    &mut out,
                    &location,
                    o.keys(),
                    "additionalProperties",
                    "the member is not allowed by the `additionalProperties` constraint",
                );
            }
        }
        K::UnevaluatedProperties { unexpected } => push_members(
            &mut out,
            &location,
            unexpected,
            "unevaluatedProperties",
            "the member is not allowed by the `unevaluatedProperties` constraint",
        ),
        K::Required { property } => out.push(problem(
            location.clone(),
            "required",
            format!("required property {property} is missing"),
        )),
        _ => {}
    }
    if out.is_empty() {
        let rule = token(err.kind());
        out.push(problem(
            location,
            rule,
            format!("the value violates the `{rule}` constraint"),
        ));
    }
    out
}

/// Validates `manifest` against the combined schema and returns its problems, sorted by
/// location, rule and message: identical problems once, at most `MAX_VALIDATION_PROBLEMS`, and
/// when the list is cut (more problems, or more library errors than
/// `MAX_VALIDATION_ERRORS_SCANNED`) one final problem with the rule `too_many_problems` at the
/// location `""`.
///
/// # Errors
///
/// Every key whose served schema is unusable, in key order; nothing is validated then.
pub fn validate_manifest(
    sections: &[SectionSchema],
    manifest: &Value,
) -> Result<Vec<Problem>, Vec<UnusableSchema>> {
    check_schemas(sections)?;
    let combined = combined_schema(sections);
    // The combination of usable schemas compiles: this is a defect path. It reports every
    // section key as unusable rather than panicking.
    let validator = options(Refuse).build(&combined).map_err(|_| {
        let mut keys: Vec<&str> = sections.iter().map(|s| s.key.as_str()).collect();
        keys.sort_unstable();
        keys.dedup();
        if keys.is_empty() {
            keys.push("");
        }
        keys.into_iter()
            .map(|key| UnusableSchema {
                key: key.to_owned(),
                cause: UnusableCause::Compile,
            })
            .collect::<Vec<_>>()
    })?;
    let mut out: Vec<Problem> = Vec::new();
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    let mut truncated = false;
    'errors: for (scanned, err) in validator.iter_errors(manifest).enumerate() {
        if scanned >= MAX_VALIDATION_ERRORS_SCANNED {
            truncated = true;
            break;
        }
        for p in problems_of(&err, manifest) {
            if seen.insert((p.location.clone(), p.rule.clone(), p.message.clone())) {
                if out.len() >= MAX_VALIDATION_PROBLEMS {
                    truncated = true;
                    break 'errors;
                }
                out.push(p);
            }
        }
    }
    out.sort_by(|a, b| (&a.location, &a.rule, &a.message).cmp(&(&b.location, &b.rule, &b.message)));
    if truncated {
        out.push(problem(
            String::new(),
            "too_many_problems",
            format!(
                "the manifest has more than {MAX_VALIDATION_PROBLEMS} problems; only the first are listed"
            ),
        ));
    }
    Ok(out)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(clippy::non_ascii_literal)]
#[path = "schema_validator_tests.rs"]
mod schema_validator_tests;
