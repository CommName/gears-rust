//! Audit plugin API (DESIGN §3.3).

use async_trait::async_trait;

use crate::errors::AuditPluginError;
use crate::models::{CallerIdentity, FinalResultContent, RequestHandoffContent};

/// Audit plugin client, registered as a scoped client (DESIGN §3.3).
///
/// Obligations of every implementation:
/// - **Accepted means recorded eventually.** Returning `Ok` from a hand-off means the
///   plugin has taken responsibility for it and retries until it is recorded. The
///   gear never retries.
/// - **Deduplicate on the request id.** `content.request_id` is the stable key of the
///   request's audit event; a repeated hand-off for the same id is one event.
/// - **Exposure is off by default.** Manifest content is exposed to readers only when
///   an operator enables it, and enabling it is documented as security-relevant.
/// - Retention, purge, erasure and immutability are the plugin's concern.
#[async_trait]
pub trait AuditPluginClientV1: Send + Sync {
    /// Records the request; returns the opaque audit id.
    async fn request_handoff(
        &self,
        caller: &CallerIdentity,
        content: &RequestHandoffContent,
    ) -> Result<String, AuditPluginError>;

    /// Records the final result for `audit_id`, the id returned by
    /// [`Self::request_handoff`]. `content.audit_id` always equals `audit_id`; the gear
    /// guarantees it.
    async fn final_result_handoff(
        &self,
        caller: &CallerIdentity,
        audit_id: &str,
        content: &FinalResultContent,
    ) -> Result<(), AuditPluginError>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "audit_plugin_api_tests.rs"]
mod audit_plugin_api_tests;
