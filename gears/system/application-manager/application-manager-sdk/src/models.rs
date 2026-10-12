//! Shared domain models (DESIGN §3.1).

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use toolkit_security::SecurityContext;
use uuid::Uuid;

/// Deepest parsed manifest, in nested arrays or objects (`[]` is 1), that still
/// round-trips through `serde_json` inside [`RequestHandoffContent`]. The hand-off
/// wraps the manifest in its own object, which costs levels under the `serde_json`
/// recursion limit of 128, so the gear's manifest parse depth must be at most this
/// value (DESIGN §3.1, §4.1).
pub const HANDOFF_MANIFEST_MAX_DEPTH: usize = 126;

/// Keeps an explicit JSON `null` as `Some(Value::Null)`; an absent member stays `None`.
fn present_value<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

/// The application envelope of a manifest: its id and version.
///
/// Tolerant of extra members; rejecting them is the gear validator's rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationEnvelope {
    /// Application id.
    pub id: String,
    /// Application version.
    pub version: String,
}

/// Application reference in reports and hand-offs; each member is present only when known.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationRef {
    /// Application id, when extracted and within limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Application version, when extracted and within limits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Caller identity passed to plugins. It never carries a bearer token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallerIdentity {
    /// Subject id.
    pub subject_id: Uuid,
    /// Subject type, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_type: Option<String>,
    /// Tenant of the subject.
    pub subject_tenant_id: Uuid,
    /// Token scopes, copied verbatim.
    pub token_scopes: Vec<String>,
    /// Request id.
    pub request_id: Uuid,
}

impl CallerIdentity {
    /// Copies subject id, type, tenant and scopes from `ctx`. The bearer token is dropped.
    #[must_use]
    pub fn from_security_context(ctx: &SecurityContext, request_id: Uuid) -> Self {
        Self {
            subject_id: ctx.subject_id(),
            subject_type: ctx.subject_type().map(str::to_owned),
            subject_tenant_id: ctx.subject_tenant_id(),
            token_scopes: ctx.token_scopes().to_vec(),
            request_id,
        }
    }

    /// Rebuilds a security context from the identity fields. It carries no bearer token.
    #[must_use]
    pub fn to_security_context(&self) -> SecurityContext {
        let mut builder = SecurityContext::builder()
            .subject_id(self.subject_id)
            .subject_tenant_id(self.subject_tenant_id)
            .token_scopes(self.token_scopes.clone());
        if let Some(kind) = &self.subject_type {
            builder = builder.subject_type(kind);
        }
        // `build` fails only when the subject id or tenant is unset, and both are
        // always set above, so this branch is unreachable. It still fails closed:
        // the anonymous context has nil ids and no scopes, so it grants nothing.
        builder
            .build()
            .unwrap_or_else(|_| SecurityContext::anonymous())
    }
}

/// Per-section outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SectionOutcome {
    /// The section plugin applied or removed the section.
    Succeeded,
    /// The section plugin rejected the section.
    Rejected,
    /// The caller lacks a permission the plugin requires.
    NotPermitted,
    /// The plugin failed internally.
    InternalFailure,
    /// The plugin call timed out; the effect is unknown.
    TimedOut,
    /// The plugin's in-flight cap was full, so the call never reached the plugin
    /// and nothing was applied or removed.
    NotSent,
    /// The section was never dispatched.
    NotDispatched,
}

/// Request-level outcome category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeCategory {
    /// The request succeeded.
    Succeeded,
    /// A section plugin rejected the request.
    PluginRejected,
    /// The caller is not permitted.
    NotPermitted,
    /// The application was not found.
    NotFound,
    /// The manifest is invalid.
    InvalidManifest,
    /// Internal failure.
    InternalFailure,
    /// A dependency is temporarily unavailable.
    TemporarilyUnavailable,
}

/// What happened to the stored registry entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistryEffect {
    /// The entry was updated.
    Updated,
    /// The update is not confirmed: the write failed, or it timed out or was
    /// interrupted after it started and may still have taken effect.
    NotUpdated,
    /// The entry is unchanged.
    Unchanged,
    /// The entry was deleted.
    Deleted,
    /// The delete is not confirmed: it failed, or it timed out or was interrupted
    /// after it started and may still have taken effect.
    DeleteFailed,
}

/// A dependency that may be unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dependency {
    /// Access control (PDP).
    AccessControl,
    /// Registry storage.
    RegistryStorage,
    /// The audit plugin.
    AuditPlugin,
    /// A section schema.
    SectionSchema,
}

/// Why a dependency was unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    /// The dependency could not be reached.
    Unreachable,
    /// The call timed out.
    TimedOut,
    /// The dependency refused the request.
    Refused,
    /// The dependency answered with something unusable.
    Unusable,
    /// The call was not sent.
    NotSent,
    /// The gear refused the request before readiness or while stopping.
    NotReady,
    /// The gear refused the request at the request-task cap.
    AtCapacity,
}

/// Operation recorded by the request hand-off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffOperation {
    /// Seed an application.
    Seed,
    /// Uninstall an application.
    Uninstall,
}

/// A validation problem with a JSON Pointer location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Problem {
    /// JSON Pointer to the offending value.
    pub location: String,
    /// Violated rule.
    pub rule: String,
    /// Human-readable message.
    pub message: String,
}

/// Outcome of one section in the caller-facing report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SectionReport {
    /// Section key.
    pub key: String,
    /// Section outcome.
    pub outcome: SectionOutcome,
    /// Diagnostic detail, when the plugin gave one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// JSON Pointer location, when the plugin gave one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// An unavailable dependency in the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnavailableEntry {
    /// The dependency.
    pub dependency: Dependency,
    /// Section key, for section schemas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Why it was unavailable.
    pub reason: UnavailableReason,
}

/// Caller-facing result of a seed or uninstall.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeReport {
    /// Request-level category.
    pub category: OutcomeCategory,
    /// The application, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application: Option<ApplicationRef>,
    /// Whether the request was applied only in part.
    pub partial: bool,
    /// Per-section outcomes.
    pub sections: Vec<SectionReport>,
    /// Validation problems.
    pub problems: Vec<Problem>,
    /// Unavailable dependencies.
    pub unavailable: Vec<UnavailableEntry>,
    /// Effect on the registry.
    pub registry_effect: RegistryEffect,
    /// Audit id, when the request hand-off was accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_id: Option<String>,
}

/// Content of the request hand-off to the audit plugin.
///
/// The caller identity is an argument of the hand-off, never part of its content.
/// `Debug` redacts the manifest: manifest content never appears in logs, traces,
/// error text or diagnostics (DESIGN §2.2).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestHandoffContent {
    /// Operation.
    pub operation: HandoffOperation,
    /// Request id.
    pub request_id: Uuid,
    /// Receive time, RFC 3339.
    pub received_at: String,
    /// The application, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application: Option<ApplicationRef>,
    /// The parsed manifest (seed only). Absent when the body is invalid (the gear's
    /// parse produced no value: syntax error, non-UTF-8, too deep, or a repeated
    /// member name); its content is not recorded. A body that is the JSON value
    /// `null` stays `Some(Value::Null)`.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub manifest: Option<Value>,
    /// Category decided before the hand-off, when any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decided_category: Option<OutcomeCategory>,
    /// Byte length of a never-storable uninstall id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_length_bytes: Option<u64>,
    /// `sha256:` digest of a never-storable uninstall id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_sha256: Option<String>,
}

impl std::fmt::Debug for RequestHandoffContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestHandoffContent")
            .field("operation", &self.operation)
            .field("request_id", &self.request_id)
            .field("received_at", &self.received_at)
            .field("application", &self.application)
            .field("manifest", &self.manifest.as_ref().map(|_| "<redacted>"))
            .field("decided_category", &self.decided_category)
            .field("id_length_bytes", &self.id_length_bytes)
            .field("id_sha256", &self.id_sha256)
            .finish()
    }
}

/// Validation result recorded in the final result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether the manifest is valid.
    pub valid: bool,
    /// Problems found.
    pub problems: Vec<Problem>,
}

/// Section outcome recorded in the final result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalSection {
    /// Section key.
    pub key: String,
    /// Section outcome.
    pub outcome: SectionOutcome,
    /// `sha256:` fingerprint of the served schema, when one was fetched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_fingerprint: Option<String>,
}

/// Content of the final-result hand-off to the audit plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalResultContent {
    /// Audit id returned by the request hand-off.
    pub audit_id: String,
    /// Request id.
    pub request_id: Uuid,
    /// Validation result; omitted when validation did not run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<ValidationResult>,
    /// Section outcomes.
    pub sections: Vec<FinalSection>,
    /// Request-level category.
    pub category: OutcomeCategory,
    /// Effect on the registry.
    pub registry_effect: RegistryEffect,
    /// Completion time, RFC 3339.
    pub completed_at: String,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(
    clippy::assertions_on_constants,
    clippy::non_ascii_literal,
    clippy::trivially_copy_pass_by_ref,
    clippy::needless_pass_by_value
)]
#[path = "models_tests.rs"]
mod models_tests;
