//! Models passed to section plugins.

use serde::{Deserialize, Serialize};
use toolkit_macros::domain_model;
use toolkit_security::{PlatformIdentity, PlatformSecurityContext};
use uuid::Uuid;

/// The caller as a plugin sees it: the validated platform identity. It holds
/// no credential.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallerIdentity(PlatformIdentity);

impl CallerIdentity {
    /// Copy the validated identity from a platform security context.
    #[must_use]
    pub fn from_platform_context(ctx: &PlatformSecurityContext) -> Self {
        Self(ctx.identity().clone())
    }

    /// The validated platform identity.
    #[must_use]
    pub fn identity(&self) -> &PlatformIdentity {
        &self.0
    }
}

/// The manifest's `application` object; the content is opaque to the gear.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// Application id.
    pub id: String,
    /// Application version.
    pub version: String,
}

/// What an operation does.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OperationAction {
    /// Submit a manifest.
    Seed,
    /// Remove an application.
    Uninstall,
}

/// The single overall result of a request that is not interrupted (PRD
/// `fr-outcome-categories`).
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RequestCategory {
    /// Every section, or every section plugin for uninstall, succeeded.
    Succeeded,
    /// The manifest failed validation.
    InvalidManifest,
    /// A section plugin refused its section as unacceptable.
    PluginRejected,
    /// Platform access control or a section plugin refused the caller.
    NotPermitted,
    /// Another operation for the same application is open.
    Conflict,
    /// A plugin or the system itself failed unexpectedly.
    InternalFailure,
    /// A needed dependency could not answer.
    TemporarilyUnavailable,
}

/// The outcome of one section.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SectionOutcome {
    /// Applied.
    Succeeded,
    /// Refused as unacceptable.
    Rejected,
    /// Refused for a permission of the plugin's own.
    NotPermitted,
    /// The plugin failed.
    InternalFailure,
    /// The call was dropped at its time limit; its effect is unknown.
    TimedOut,
    /// Refused by the in-flight cap.
    NotSent,
    /// Not called: the request task failed or stopped before the call, the
    /// call no longer fit before the run deadline, or an earlier
    /// section-outcome write failed.
    NotDispatched,
    /// The plugin may have been called when the request task stopped; its
    /// effect is unknown.
    Lost,
}

/// A dependency that could not answer.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Dependency {
    /// Operation storage.
    OperationStorage,
    /// The gear itself.
    ApplicationManager,
}

/// Why a dependency is unavailable.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum UnavailableReasonCode {
    /// Not reachable.
    Unreachable,
    /// No answer in time.
    TimedOut,
    /// Startup has not finished.
    NotReady,
    /// At the request-task cap.
    AtCapacity,
}

/// The application an operation concerns: seed has `id` and `version`,
/// uninstall only `id`.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ReportApplication {
    /// Application id.
    pub id: String,
    /// Application version; absent for uninstall.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// One section plugin of a running operation, in priority order.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SectionOutcomeEntry {
    /// Section key.
    pub key: String,
    /// The exact GTS id of the section type of a seed section, written by
    /// mark running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_type: Option<String>,
    /// The outcome, once recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<SectionOutcome>,
    /// Plugin detail of a `rejected` or `not_permitted` outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Plugin location of a `rejected` or `not_permitted` outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
}

/// One section validation problem.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ReportProblem {
    /// JSON Pointer of the problem.
    pub location: String,
    /// A JSON Schema keyword, `missing_section`, or `unknown_section`.
    pub rule: String,
    /// Human-readable message.
    pub message: String,
}

/// One unavailable dependency.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct UnavailableItem {
    /// The dependency.
    pub dependency: Dependency,
    /// Why it is unavailable.
    pub reason: UnavailableReasonCode,
}

/// The operation report, written into the operation record as the operation
/// runs (DESIGN 3.1).
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct OutcomeReport {
    /// The operation id.
    pub operation_id: Uuid,
    /// `seed` or `uninstall`.
    pub action: OperationAction,
    /// Category of a completed operation; absent while open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<RequestCategory>,
    /// The application.
    pub application: ReportApplication,
    /// Per plugin once the operation is running, in priority order.
    pub sections: Vec<SectionOutcomeEntry>,
    /// Section validation problems.
    pub problems: Vec<ReportProblem>,
    /// Unavailable items.
    pub unavailable: Vec<UnavailableItem>,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "models_tests.rs"]
mod tests;
