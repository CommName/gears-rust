//! Section plugin router (`cpt-cf-application-manager-component-section-plugin-router`).
//!
//! Routes only to the startup plugin set, so a request cannot select a
//! plugin. Every call goes through the plugin call guard. The router never
//! retries and does not record outcomes.

use std::fmt;
use std::sync::Arc;

use application_manager_sdk::{CallerIdentity, SectionPluginError, SectionSchema};
use toolkit_macros::domain_model;

use crate::constants::{LOG_TEXT_MAX_BYTES, cut};
use crate::domain::plugin_set::SectionPlugin;
use crate::domain::ports::{CallFailure, PluginCalls};

/// Why a section schema is not available (DESIGN 3.3).
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnavailableReason {
    /// The plugin failed, panicked, or could not be reached.
    Unreachable,
    /// The call was dropped at `plugin_call_timeout`.
    TimedOut,
    /// The served schema cannot be used.
    Unusable,
    /// The plugin's in-flight cap was full; it was not called.
    NotSent,
}

impl UnavailableReason {
    /// The reason as DESIGN names it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unreachable => "unreachable",
            Self::TimedOut => "timed_out",
            Self::Unusable => "unusable",
            Self::NotSent => "not_sent",
        }
    }
}

impl fmt::Display for UnavailableReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What one plugin answered to `schema`.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaFetch {
    /// The plugin served a schema; it is not checked yet.
    Served(SectionSchema),
    /// The plugin refused the caller.
    NotPermitted {
        /// The plugin's detail, as sent.
        detail: String,
    },
    /// No schema.
    Unavailable(UnavailableReason),
}

/// Routes calls to the startup plugin set.
#[domain_model]
pub struct SectionPluginRouter {
    calls: Arc<dyn PluginCalls>,
}

impl fmt::Debug for SectionPluginRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SectionPluginRouter")
            .finish_non_exhaustive()
    }
}

impl SectionPluginRouter {
    /// A router over bounded plugin calls.
    #[must_use]
    pub fn new(calls: Arc<dyn PluginCalls>) -> Self {
        Self { calls }
    }

    /// Ask `plugin` for its schema.
    pub async fn fetch_schema(
        &self,
        plugin: &SectionPlugin,
        caller: &CallerIdentity,
    ) -> SchemaFetch {
        classify(&plugin.key, self.calls.schema(plugin, caller).await)
    }

    /// Ask every plugin in `plugins` for its schema, concurrently. Each
    /// answer comes with the plugin that gave it, in the order of `plugins`.
    pub async fn fetch_schemas<'p>(
        &self,
        plugins: &'p [SectionPlugin],
        caller: &CallerIdentity,
    ) -> Vec<(&'p SectionPlugin, SchemaFetch)> {
        futures::future::join_all(
            plugins
                .iter()
                .map(|plugin| async move { (plugin, self.fetch_schema(plugin, caller).await) }),
        )
        .await
    }
}

fn classify(
    key: &str,
    answer: Result<Result<SectionSchema, SectionPluginError>, CallFailure>,
) -> SchemaFetch {
    match answer {
        Ok(Ok(schema)) => SchemaFetch::Served(schema),
        Ok(Err(SectionPluginError::NotPermitted { detail })) => {
            tracing::info!(
                key,
                call = "schema",
                detail = cut(&detail, LOG_TEXT_MAX_BYTES),
                "application-manager: section plugin refused the caller"
            );
            SchemaFetch::NotPermitted { detail }
        }
        Ok(Err(err)) => {
            tracing::warn!(
                key,
                call = "schema",
                error = cut(&err.to_string(), LOG_TEXT_MAX_BYTES),
                "application-manager: section plugin failed"
            );
            SchemaFetch::Unavailable(UnavailableReason::Unreachable)
        }
        Err(failure) => SchemaFetch::Unavailable(unanswered(key, &failure)),
    }
}

/// Log a call that produced no plugin answer, and name its reason.
fn unanswered(key: &str, failure: &CallFailure) -> UnavailableReason {
    let reason = match failure {
        CallFailure::TimedOut => UnavailableReason::TimedOut,
        CallFailure::NotSent => UnavailableReason::NotSent,
        CallFailure::Panicked(_) => UnavailableReason::Unreachable,
    };
    if let CallFailure::Panicked(message) = failure {
        tracing::error!(
            key,
            call = "schema",
            panic = cut(message, LOG_TEXT_MAX_BYTES),
            "application-manager: section plugin call panicked"
        );
    } else {
        tracing::warn!(
            key,
            call = "schema",
            reason = reason.as_str(),
            "application-manager: section plugin call not answered"
        );
    }
    reason
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "router_tests.rs"]
mod tests;
