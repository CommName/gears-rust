//! Section plugin contract.

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::SectionPluginError;
use crate::models::{CallerIdentity, Envelope, SectionSchema};

/// The normative contract a section plugin implements.
///
/// Plugins use their own service credentials and never receive a caller
/// token. In their own `init` they register a GTS instance of
/// [`crate::ApplicationManagerSectionPluginSpecV1`] and a scoped `ClientHub`
/// client for this trait. Their priority is the instance `priority`, unique
/// across plugins.
///
/// Every call is bounded by the operator's `plugin_call_timeout`. A call past
/// it is dropped and recorded as `timed_out`, and dispatch continues; a `key`
/// timeout fails startup. A per-plugin in-flight cap may refuse a call, which
/// is recorded as `not_sent`.
#[async_trait]
pub trait SectionPluginClientV1: Send + Sync {
    /// The section key.
    ///
    /// Called once at `start`. One key of lower-case letters, digits, `-`,
    /// `_`, `.`. Must not change for the process lifetime.
    ///
    /// # Errors
    /// Any [`SectionPluginError`]; the gear fails startup.
    async fn key(&self) -> Result<String, SectionPluginError>;

    /// The schema of the section.
    ///
    /// Called on every seed and schema read. Returns a self-contained
    /// Draft 2020-12 schema with no `$ref` outside the document, plus a schema
    /// version that changes whenever the content changes. The schema version
    /// is opaque text of 1 to [`crate::SCHEMA_VERSION_MAX_BYTES`] bytes, recorded as
    /// sent and never compared.
    ///
    /// A schema is `unusable` when it declares another dialect than
    /// Draft 2020-12, refers outside the served document, exceeds
    /// [`crate::SERVED_SCHEMA_MAX_BYTES`], has a bad schema version, or fails to
    /// compile.
    ///
    /// # Errors
    /// [`SectionPluginError::NotPermitted`] is plugin-level not permitted.
    /// Any other error, or a transport error, makes the schema unavailable
    /// with reason `unreachable`; a timeout gives `timed_out`.
    async fn schema(&self, caller: &CallerIdentity) -> Result<SectionSchema, SectionPluginError>;

    /// Apply a section.
    ///
    /// Called in priority order. The section is passed unchanged. The plugin
    /// owns rollback, idempotency, and version meaning, must tolerate repeated
    /// calls, and must keep its data consistent when the call is dropped at
    /// the time limit.
    ///
    /// # Errors
    /// [`SectionPluginError::Rejected`] refuses the content,
    /// [`SectionPluginError::NotPermitted`] refuses the caller,
    /// [`SectionPluginError::Internal`] is logged only.
    async fn apply(
        &self,
        caller: &CallerIdentity,
        operation_id: Uuid,
        envelope: &Envelope,
        section: &serde_json::Value,
    ) -> Result<(), SectionPluginError>;

    /// Remove an application's data.
    ///
    /// Called in priority order whether or not the application was seeded.
    /// Same obligations as [`SectionPluginClientV1::apply`].
    ///
    /// # Errors
    /// As [`SectionPluginClientV1::apply`].
    async fn remove(
        &self,
        caller: &CallerIdentity,
        operation_id: Uuid,
        application_id: &str,
    ) -> Result<(), SectionPluginError>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "section_plugin_api_tests.rs"]
mod tests;
