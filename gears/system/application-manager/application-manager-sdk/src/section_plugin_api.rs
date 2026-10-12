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
#[must_use]
pub fn served_schema_within_limit(schema: &Value) -> bool {
    serde_json::to_writer(LimitedSink::default(), schema).is_ok()
}

/// Counts written bytes and fails once they pass [`SERVED_SCHEMA_MAX_BYTES`], so an
/// oversized schema is never serialized in full.
#[derive(Default)]
struct LimitedSink {
    written: usize,
}

impl std::io::Write for LimitedSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.written = self.written.saturating_add(buf.len());
        if self.written > SERVED_SCHEMA_MAX_BYTES {
            return Err(std::io::Error::other("served schema over limit"));
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
