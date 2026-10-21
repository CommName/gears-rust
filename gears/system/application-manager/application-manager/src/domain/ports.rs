//! Ports the domain calls; `infra` implements them.

use application_manager_sdk::{CallerIdentity, SectionPluginError, SectionSchema};
use async_trait::async_trait;
use toolkit_macros::domain_model;
use toolkit_security::SecurityContext;

use crate::domain::actions::Action;
use crate::domain::plugin_set::SectionPlugin;

/// Why platform access control did not permit an action.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AccessError {
    /// Access control cannot answer: an evaluation failure or no answer
    /// within `pdp_call_timeout` (outcome rank 1).
    #[error("access control cannot answer")]
    Unavailable,
    /// Denied, a constraint compile failure, or a permit with any constraint
    /// (outcome rank 2).
    #[error("not permitted")]
    NotPermitted,
}

/// Platform access control for the gear-level actions of
/// [`crate::domain::actions`].
#[async_trait]
pub trait AccessControl: Send + Sync {
    /// Decide whether the caller may perform `action`, as a platform-level
    /// permission with no context tenant (ADR 0005).
    ///
    /// # Errors
    /// [`AccessError`] when the action is not permitted or cannot be decided.
    async fn authorize(&self, ctx: &SecurityContext, action: Action) -> Result<(), AccessError>;
}

/// Why a bounded plugin call produced no plugin answer
/// (`cpt-cf-application-manager-component-plugin-call-guard`).
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallFailure {
    /// The plugin's in-flight cap was full; the plugin was not called.
    #[error("not_sent")]
    NotSent,
    /// The call was dropped at `plugin_call_timeout`; its effect is unknown.
    #[error("timed_out")]
    TimedOut,
    /// The call panicked.
    #[error("panicked: {0}")]
    Panicked(String),
}

/// Section plugin calls, each bounded by the plugin call guard.
#[async_trait]
pub trait PluginCalls: Send + Sync {
    /// Ask `plugin` for its live schema on behalf of `caller`.
    ///
    /// # Errors
    /// [`CallFailure`] when the call produced no plugin answer; the plugin's
    /// own result is returned unchanged in `Ok`.
    async fn schema(
        &self,
        plugin: &SectionPlugin,
        caller: &CallerIdentity,
    ) -> Result<Result<SectionSchema, SectionPluginError>, CallFailure>;
}
