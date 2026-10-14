//! Section plugin API (DESIGN §3.3).

use async_trait::async_trait;
use serde_json::Value;

use crate::errors::SectionPluginError;
use crate::models::{ApplicationEnvelope, CallerIdentity};

/// Longest allowed section key, in bytes.
pub const SECTION_KEY_MAX_LEN: usize = 64;
/// Largest served schema, in serialized JSON bytes.
pub const SERVED_SCHEMA_MAX_BYTES: usize = 1024 * 1024;

/// Section plugin client, registered as a scoped client (DESIGN §3.3).
///
/// Obligations of every implementation:
/// - `apply` and `remove` are idempotent: the gear calls them again on a re-seed or a
///   repeated uninstall.
/// - Error text names locations and rules, never manifest values or secrets; the
///   constructors of [`SectionPluginError`] bound it to
///   [`crate::errors::PLUGIN_DETAIL_MAX_BYTES`].
#[async_trait]
pub trait SectionPluginClientV1: Send + Sync {
    /// The section key this plugin owns.
    async fn key(&self) -> Result<String, SectionPluginError>;

    /// The JSON Schema of the section.
    ///
    /// May fail only with [`SectionPluginError::Internal`]; `Rejected` and
    /// `NotPermitted` here break the contract and the schema is reported `unusable`.
    async fn schema(&self, caller: &CallerIdentity) -> Result<Value, SectionPluginError>;

    /// Applies the section of an application.
    async fn apply(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
        section: Value,
    ) -> Result<(), SectionPluginError>;

    /// Removes the section of an application.
    async fn remove(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
    ) -> Result<(), SectionPluginError>;
}

/// Checks a section key: 1..=[`SECTION_KEY_MAX_LEN`] of lower-case ASCII letters, digits,
/// `-`, `_` and `.`, other than the path segments `.` and `..`.
///
/// # Errors
///
/// Returns a description of the violated rule.
pub fn validate_section_key(key: &str) -> Result<(), String> {
    if key.is_empty() {
        return Err("section key is empty".to_owned());
    }
    if key.len() > SECTION_KEY_MAX_LEN {
        return Err(format!("section key exceeds {SECTION_KEY_MAX_LEN} bytes"));
    }
    // `.` and `..` cannot be carried in the schema-read path segment.
    if key == "." || key == ".." {
        return Err("section key must not be `.` or `..`".to_owned());
    }
    let allowed = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || "-_.".contains(c);
    if key.chars().all(allowed) {
        Ok(())
    } else {
        Err("section key has a character outside [a-z0-9-_.]".to_owned())
    }
}

/// Whether the serialized schema is at most [`SERVED_SCHEMA_MAX_BYTES`].
///
/// Counts the compact serialization without recursion and without building text, so a very
/// deep or very large value is measured with a bounded stack and the count stops at the first
/// byte past the bound. The one size rule of the platform: the gear's schema validator uses it.
#[must_use]
pub fn served_schema_within_limit(schema: &Value) -> bool {
    !compact_len_exceeds(schema, SERVED_SCHEMA_MAX_BYTES)
}

/// Bytes of `s` as a JSON string, quotes included, escaped as `serde_json` writes it.
fn json_string_len(s: &str) -> usize {
    let mut n: usize = 2;
    for b in s.bytes() {
        n = n.saturating_add(match b {
            b'"' | b'\\' | 0x08 | 0x0c | b'\n' | b'\r' | b'\t' => 2,
            0x00..=0x1f => 6,
            _ => 1,
        });
    }
    n
}

/// Counts the bytes a value formats to, without storing them.
#[derive(Default)]
struct ByteCount(usize);

impl std::fmt::Write for ByteCount {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.0 = self.0.saturating_add(s.len());
        Ok(())
    }
}

fn number_len(n: &serde_json::Number) -> usize {
    use std::fmt::Write as _;
    let mut count = ByteCount::default();
    // Writing to a counter cannot fail.
    if write!(count, "{n}").is_err() {
        return usize::MAX;
    }
    count.0
}

/// Whether the compact serialization of `root` is longer than `limit` bytes: an explicit
/// stack, no allocation per string or number, an early exit past the limit.
pub(crate) fn compact_len_exceeds(root: &Value, limit: usize) -> bool {
    let mut total: usize = 0;
    let mut stack = vec![root];
    while let Some(v) = stack.pop() {
        total = total.saturating_add(match v {
            Value::Null | Value::Bool(true) => 4,
            Value::Bool(false) => 5,
            Value::Number(n) => number_len(n),
            Value::String(s) => json_string_len(s),
            Value::Array(items) => {
                stack.extend(items.iter());
                // Brackets and the commas between elements.
                2 + items.len().saturating_sub(1)
            }
            Value::Object(members) => {
                let mut keys: usize = 0;
                for (k, child) in members {
                    // The key string and its colon.
                    keys = keys.saturating_add(json_string_len(k) + 1);
                    stack.push(child);
                }
                2 + members.len().saturating_sub(1) + keys
            }
        });
        if total > limit {
            return true;
        }
    }
    false
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(
    clippy::assertions_on_constants,
    clippy::non_ascii_literal,
    clippy::trivially_copy_pass_by_ref,
    clippy::needless_pass_by_value
)]
#[path = "section_plugin_api_tests.rs"]
mod section_plugin_api_tests;
