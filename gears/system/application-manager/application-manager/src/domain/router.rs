//! Section plugin router (`cpt-cf-application-manager-component-section-plugin-router`).
//!
//! Routes only through the startup plugin set, so a manifest cannot select a
//! plugin. Calls `apply` or `remove` one at a time in priority order through
//! the plugin call guard, continuing after failures, with the caller identity
//! and operation id. It never retries, never consults the types registry, and
//! neither records outcomes nor checks the run deadline: before each call it
//! asks its caller whether the call may still be made.

use std::fmt;
use std::sync::Arc;

use application_manager_sdk::{CallerIdentity, Envelope};
use serde_json::Value;
use application_manager_sdk::{
    CallerIdentity, Envelope, SectionOutcomeEntry, SectionPluginError, SectionSchema,
};
use toolkit_macros::domain_model;
use uuid::Uuid;

use crate::domain::plugin_set::{PluginSet, SectionPlugin};
use crate::domain::ports::{CallResult, PluginCalls};

/// The answer of one plugin the router called, by section key.
pub type Routed = (String, CallResult);

/// Dispatches seed and uninstall calls over the plugin set.
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
    /// A router whose calls go through `calls`.
    #[must_use]
    pub fn new(calls: Arc<dyn PluginCalls>) -> Self {
        Self { calls }
    }

    /// Call `apply` on every plugin in priority order, each with its own
    /// section from `sections` (a validated seed has one per key). Before each
    /// call `may_call` is asked; the first `false` stops the dispatch. The
    /// answers of the plugins called come back in order.
    pub async fn apply_all(
        &self,
        set: &PluginSet,
        caller: &CallerIdentity,
        operation_id: Uuid,
        envelope: &Envelope,
        sections: &serde_json::Map<String, Value>,
        mut may_call: impl FnMut(&SectionPlugin) -> bool,
    ) -> Vec<Routed> {
        let mut routed = Vec::with_capacity(set.plugins().len());
        for plugin in set.plugins() {
            if !may_call(plugin) {
                break;
            }
            let section = sections.get(&plugin.key).unwrap_or(&Value::Null);
            let answer = self
                .calls
                .apply(plugin, caller, operation_id, envelope, section)
                .await;
            routed.push((plugin.key.clone(), answer));
        }
        routed
    }

    /// Call `remove` for `application_id` on every plugin in priority order,
    /// asking `may_call` before each call as [`Self::apply_all`] does.
    pub async fn remove_all(
        &self,
        set: &PluginSet,
        caller: &CallerIdentity,
        operation_id: Uuid,
        application_id: &str,
        mut may_call: impl FnMut(&SectionPlugin) -> bool,
    ) -> Vec<Routed> {
        let mut routed = Vec::with_capacity(set.plugins().len());
        for plugin in set.plugins() {
            if !may_call(plugin) {
                break;
            }
            let answer = self
                .calls
                .remove(plugin, caller, operation_id, application_id)
                .await;
            routed.push((plugin.key.clone(), answer));
    ) -> Vec<(&'p SectionPlugin, SchemaFetch)> {
        futures::future::join_all(
            plugins
                .iter()
                .map(|plugin| async move { (plugin, self.fetch_schema(plugin, caller).await) }),
        )
        .await
    }

    /// Call `apply` of `plugin` with its `section`, unchanged. The entry has
    /// the plugin's key and no schema version: `Rejected` gives `rejected`
    /// (detail cut at `PLUGIN_DETAIL_MAX_BYTES`, with its location),
    /// `NotPermitted` gives `not_permitted` (detail cut), `Internal` or a
    /// panic gives `internal_failure` (logged only), a timeout `timed_out`, a
    /// full in-flight cap `not_sent`. Does no deadline check.
    pub async fn apply_one(
        &self,
        _plugin: &SectionPlugin,
        _caller: &CallerIdentity,
        _operation_id: Uuid,
        _envelope: &Envelope,
        _section: &serde_json::Value,
    ) -> SectionOutcomeEntry {
        todo!("PR 5: apply mapping")
    }

    /// Call `remove` of `plugin`; mapped like [`Self::apply_one`].
    pub async fn remove_one(
        &self,
        _plugin: &SectionPlugin,
        _caller: &CallerIdentity,
        _operation_id: Uuid,
        _application_id: &str,
    ) -> SectionOutcomeEntry {
        todo!("PR 5: remove mapping")
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
        routed
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "router_tests.rs"]
mod tests;
