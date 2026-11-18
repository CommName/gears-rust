//! Section plugin contract.

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::SectionPluginError;
use crate::models::{CallerIdentity, Envelope};

/// The normative contract a section plugin implements.
///
/// - Plugins use their own service credentials and never receive a caller
///   credential.
/// - In their own `init` they register a type derived from
///   [`crate::ApplicationManagerSectionV1`], whose schema is the section's
///   content rules as JSON Schema Draft-07, a GTS instance of
///   [`crate::ApplicationManagerSectionPluginSpecV1`] naming that type in
///   `section_type`, and a scoped `ClientHub` client for this trait.
/// - Their priority is the instance `priority`, unique across plugins.
/// - The section key is the derived type's segment without its version, so
///   two versions of one section type share a key.
/// - A plugin that needs a permission of its own decides it from the
///   [`CallerIdentity`] and refuses with [`SectionPluginError::NotPermitted`];
///   the PDP never evaluates a platform identity.
/// - Additive changes within `v1` are compatible; any other change is a new
///   GTS type version.
///
/// Every call is bounded by the operator's `plugin_call_timeout`. A call past
/// it is dropped and recorded as `timed_out`, and dispatch continues. A
/// per-plugin in-flight cap may refuse a call, which is recorded as
/// `not_sent`.
#[async_trait]
pub trait SectionPluginClientV1: Send + Sync {
    /// Apply a section.
    ///
    /// Called in priority order during a seed. The section is passed
    /// unchanged. The plugin owns rollback, idempotency, and version meaning,
    /// must tolerate repeated calls, and must keep its data consistent when
    /// the call is dropped at the time limit.
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
    /// Called in priority order during an uninstall, whether or not the
    /// application was seeded. Same obligations as
    /// [`SectionPluginClientV1::apply`].
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
