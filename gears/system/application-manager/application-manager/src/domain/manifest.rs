//! Seed manifest parsing and envelope rules (DESIGN §3.1 "Manifest", §3.2 step 1, §4.1).
//!
//! The parser reads the body once, as UTF-8 text, through a streaming visitor: it detects a
//! repeated member name when the key is read (names compared after unescaping), rejects
//! nesting beyond `MAX_JSON_DEPTH` when a container opens, and reports the first failure in
//! document order. Failure messages never carry body content.

use std::cell::RefCell;
use std::fmt;

use application_manager_sdk::models::Problem;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};
use toolkit_macros::domain_model;

use crate::domain::limits::{LOCATION_MAX_BYTES, MAX_JSON_DEPTH};

/// Escapes one reference token of an RFC 6901 JSON Pointer.
#[must_use]
pub fn escape_pointer_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

/// Cuts a problem location to [`LOCATION_MAX_BYTES`] at a segment boundary: the longest prefix
/// of whole segments that fits (the location of an ancestor of the failing value), `""` when
/// even the first segment does not. A location within the bound is returned unchanged.
#[must_use]
pub fn cap_location(mut location: String) -> String {
    if location.len() > LOCATION_MAX_BYTES {
        // `/` is ASCII, so the cut is on a character boundary.
        let cut = location.as_bytes()[..=LOCATION_MAX_BYTES]
            .iter()
            .rposition(|b| *b == b'/')
            .unwrap_or(0);
        location.truncate(cut);
    }
    location
}

/// Why a seed body is not a manifest.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestParseFailure {
    /// Not JSON.
    Syntax,
    /// The bytes are not UTF-8.
    NotUtf8,
    /// Nested deeper than `MAX_JSON_DEPTH`.
    TooDeep,
    /// An object repeats a member name; `location` is that object.
    DuplicateMember {
        /// JSON Pointer of the object.
        location: String,
    },
}

impl ManifestParseFailure {
    /// The problem rule token.
    #[must_use]
    pub fn rule(&self) -> &'static str {
        match self {
            Self::DuplicateMember { .. } => "duplicate_member",
            _ => "malformed_json",
        }
    }

    /// The JSON Pointer of the problem.
    #[must_use]
    pub fn location(&self) -> &str {
        match self {
            Self::DuplicateMember { location } => location,
            _ => "",
        }
    }

    /// The validation problem for this failure. The message never carries body content.
    #[must_use]
    pub fn to_problem(&self) -> Problem {
        let message = match self {
            Self::Syntax => "the body is not valid JSON",
            Self::NotUtf8 => "the body is not valid UTF-8",
            Self::TooDeep => "the body is nested too deeply",
            Self::DuplicateMember { .. } => "an object repeats a member name",
        };
        Problem {
            location: self.location().to_owned(),
            rule: self.rule().to_owned(),
            message: message.to_owned(),
        }
    }
}

/// What the visitors share. The failure is recorded where it is found; the path to it is built
/// on the way out: each container that sees its child fail adds its own key or index, so no
/// path is kept (or copied) while the document is read, and the pointer is built once, only
/// for a duplicate member.
struct Ctx {
    failure: RefCell<Option<ManifestParseFailure>>,
    /// Segments of the path to the failure, innermost first (duplicate member only).
    path: RefCell<Vec<String>>,
}

impl Ctx {
    fn fail<E: serde::de::Error>(&self, f: ManifestParseFailure) -> E {
        let mut slot = self.failure.borrow_mut();
        if slot.is_none() {
            *slot = Some(f);
        }
        E::custom("manifest parse failure")
    }

    /// Called by a container whose child failed: adds the segment of that child to the path.
    fn unwinding(&self, segment: impl FnOnce() -> String) {
        if matches!(
            *self.failure.borrow(),
            Some(ManifestParseFailure::DuplicateMember { .. })
        ) {
            self.path.borrow_mut().push(segment());
        }
    }

    /// The failure with its location built from the collected path, capped.
    fn into_failure(self) -> Option<ManifestParseFailure> {
        let path = self.path.into_inner();
        self.failure.into_inner().map(|f| match f {
            ManifestParseFailure::DuplicateMember { .. } => {
                let mut location = String::new();
                for seg in path.iter().rev() {
                    location.push('/');
                    location.push_str(&escape_pointer_segment(seg));
                    if location.len() > LOCATION_MAX_BYTES {
                        break;
                    }
                }
                ManifestParseFailure::DuplicateMember {
                    location: cap_location(location),
                }
            }
            other => other,
        })
    }
}

struct Seed<'a> {
    ctx: &'a Ctx,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = Value;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Seed<'_> {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }
    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(v.into())
    }
    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        Ok(v.into())
    }
    fn visit_f64<E>(self, v: f64) -> Result<Value, E> {
        Ok(serde_json::Number::from_f64(v).map_or(Value::Null, Value::Number))
    }
    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.to_owned()))
    }
    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }
    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let depth = self.depth + 1;
        if depth > MAX_JSON_DEPTH {
            return Err(self.ctx.fail(ManifestParseFailure::TooDeep));
        }
        let mut out = Vec::new();
        loop {
            let next = seq.next_element_seed(Seed {
                ctx: self.ctx,
                depth,
            });
            match next {
                Ok(Some(v)) => out.push(v),
                Ok(None) => break,
                Err(e) => {
                    let index = out.len();
                    self.ctx.unwinding(|| index.to_string());
                    return Err(e);
                }
            }
        }
        Ok(Value::Array(out))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let depth = self.depth + 1;
        if depth > MAX_JSON_DEPTH {
            return Err(self.ctx.fail(ManifestParseFailure::TooDeep));
        }
        let mut out = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if out.contains_key(&key) {
                return Err(self.ctx.fail(ManifestParseFailure::DuplicateMember {
                    location: String::new(),
                }));
            }
            match map.next_value_seed(Seed {
                ctx: self.ctx,
                depth,
            }) {
                Ok(v) => {
                    out.insert(key, v);
                }
                Err(e) => {
                    self.ctx.unwinding(|| key);
                    return Err(e);
                }
            }
        }
        Ok(Value::Object(out))
    }
}

/// Parses a seed body once: one JSON value, no repeated member in any object (names
/// compared after unescaping), nesting at most `MAX_JSON_DEPTH`.
///
/// # Errors
///
/// The [`ManifestParseFailure`]; no value is produced then.
pub fn parse_manifest(body: &[u8]) -> Result<Value, ManifestParseFailure> {
    let text = std::str::from_utf8(body).map_err(|_| ManifestParseFailure::NotUtf8)?;
    let ctx = Ctx {
        failure: RefCell::new(None),
        path: RefCell::new(Vec::new()),
    };
    let mut de = serde_json::Deserializer::from_str(text);
    let res = Seed {
        ctx: &ctx,
        depth: 0,
    }
    .deserialize(&mut de)
    .and_then(|v| de.end().map(|()| v));
    if let Some(f) = ctx.into_failure() {
        return Err(f);
    }
    res.map_err(|_| ManifestParseFailure::Syntax)
}

/// A way an envelope text value breaks its limits.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextLimitViolation {
    /// The text is empty.
    Empty,
    /// The text is longer than the maximum in bytes.
    TooLong,
    /// The text has a control character.
    ControlCharacter,
}

impl TextLimitViolation {
    /// The problem rule token.
    #[must_use]
    pub fn rule(self) -> &'static str {
        match self {
            Self::Empty => "minLength",
            Self::TooLong => "max_length",
            Self::ControlCharacter => "control_character",
        }
    }
}

/// Every limit `text` breaks: emptiness or length first, then control characters.
#[must_use]
pub fn text_limit_violations(text: &str, max_len: usize) -> Vec<TextLimitViolation> {
    let mut out = Vec::new();
    if text.is_empty() {
        out.push(TextLimitViolation::Empty);
    } else if text.len() > max_len {
        out.push(TextLimitViolation::TooLong);
    }
    if text.chars().any(char::is_control) {
        out.push(TextLimitViolation::ControlCharacter);
    }
    out
}

/// The envelope members of a parsed manifest, each present only when it is a string.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EnvelopeFields {
    /// `application.id`.
    pub id: Option<String>,
    /// `application.version`.
    pub version: Option<String>,
}

/// Takes `application.id` and `application.version` from a parsed manifest.
#[must_use]
pub fn extract_envelope(manifest: &Value) -> EnvelopeFields {
    let text = |name: &str| {
        manifest
            .get("application")
            .and_then(|a| a.get(name))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    EnvelopeFields {
        id: text("id"),
        version: text("version"),
    }
}

impl EnvelopeFields {
    /// One problem per broken limit, id first, at `/application/id` and
    /// `/application/version`. Absent members give none.
    #[must_use]
    pub fn limit_problems(&self, max_len: usize) -> Vec<Problem> {
        let mut out = Vec::new();
        for (name, value) in [("id", &self.id), ("version", &self.version)] {
            let Some(value) = value else { continue };
            for v in text_limit_violations(value, max_len) {
                out.push(Problem {
                    location: format!("/application/{name}"),
                    rule: v.rule().to_owned(),
                    message: match v {
                        TextLimitViolation::Empty => format!("{name} must not be empty"),
                        TextLimitViolation::TooLong => {
                            format!("{name} is longer than {max_len} bytes")
                        }
                        TextLimitViolation::ControlCharacter => {
                            format!("{name} must not contain control characters")
                        }
                    },
                });
            }
        }
        out
    }

    /// The fields that are within the limits; a field that is not drops out.
    #[must_use]
    pub fn within_limits(&self, max_len: usize) -> EnvelopeFields {
        let ok = |v: &Option<String>| {
            v.clone()
                .filter(|t| text_limit_violations(t, max_len).is_empty())
        };
        EnvelopeFields {
            id: ok(&self.id),
            version: ok(&self.version),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(clippy::non_ascii_literal)]
#[path = "manifest_tests.rs"]
mod manifest_tests;
