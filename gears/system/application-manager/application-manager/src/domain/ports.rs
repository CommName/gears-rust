//! Ports the domain calls; `infra` implements them.

use application_manager_sdk::{CallerIdentity, Envelope, SectionPluginError};
use async_trait::async_trait;
use toolkit_macros::domain_model;
use uuid::Uuid;

use crate::domain::plugin_set::SectionPlugin;

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

/// What one bounded plugin call gave: the plugin's own answer, unchanged, or
/// why there was none.
pub type CallResult = Result<Result<(), SectionPluginError>, CallFailure>;

/// Section plugin calls, each bounded by the plugin call guard.
#[async_trait]
pub trait PluginCalls: Send + Sync {
    /// Apply `section` with `envelope` through `plugin` on behalf of
    /// `caller`, within operation `operation_id`.
    async fn apply(
        &self,
        plugin: &SectionPlugin,
        caller: &CallerIdentity,
        operation_id: Uuid,
        envelope: &Envelope,
        section: &serde_json::Value,
    ) -> CallResult;

    /// Remove the application `application_id` through `plugin` on behalf of
    /// `caller`, within operation `operation_id`.
    async fn remove(
        &self,
        plugin: &SectionPlugin,
        caller: &CallerIdentity,
        operation_id: Uuid,
        application_id: &str,
    ) -> CallResult;
}
