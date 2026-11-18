//! Section plugin router (`cpt-cf-application-manager-component-section-plugin-router`).
//!
//! Routes only through the startup plugin set, so a manifest cannot select a
//! plugin. Calls `apply` or `remove` one at a time in priority order through
//! the plugin call guard, continuing after failures, with the caller identity
//! and operation id. It never retries, never consults the types registry, and
//! neither records outcomes nor checks the run deadline: its caller's
//! [`DispatchObserver`] is asked before each call whether it may still be
//! made, and told each answer, and either can stop the dispatch.

use std::fmt;
use std::sync::Arc;

use application_manager_sdk::{CallerIdentity, Envelope};
use async_trait::async_trait;
use serde_json::Value;
use toolkit_macros::domain_model;
use uuid::Uuid;

use crate::domain::plugin_set::{PluginSet, SectionPlugin};
use crate::domain::ports::{CallResult, PluginCalls};

/// The answer of one plugin the router called, by section key.
pub type Routed = (String, CallResult);

/// The caller's side of one dispatch: the run-deadline check before each call
/// and the outcome write after it.
#[async_trait]
pub trait DispatchObserver: Send {
    /// May `plugin` be called now? `false` stops the dispatch before it.
    async fn before(&mut self, plugin: &SectionPlugin) -> bool;

    /// `plugin` answered `answer`; `false` stops the dispatch after it.
    async fn after(&mut self, plugin: &SectionPlugin, answer: &CallResult) -> bool;
}

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
    /// section from `sections` (a validated seed has one per key). `observer`
    /// is asked before each call and told each answer; a `false` from either
    /// stops the dispatch. The answers of the plugins called come back in
    /// order.
    pub async fn apply_all(
        &self,
        set: &PluginSet,
        caller: &CallerIdentity,
        operation_id: Uuid,
        envelope: &Envelope,
        sections: &serde_json::Map<String, Value>,
        observer: &mut dyn DispatchObserver,
    ) -> Vec<Routed> {
        let mut routed = Vec::with_capacity(set.plugins().len());
        for plugin in set.plugins() {
            if !observer.before(plugin).await {
                break;
            }
            let section = sections.get(&plugin.key).unwrap_or(&Value::Null);
            let answer = self
                .calls
                .apply(plugin, caller, operation_id, envelope, section)
                .await;
            let go_on = observer.after(plugin, &answer).await;
            routed.push((plugin.key.clone(), answer));
            if !go_on {
                break;
            }
        }
        routed
    }

    /// Call `remove` for `application_id` on every plugin in priority order,
    /// with `observer` as in [`Self::apply_all`].
    pub async fn remove_all(
        &self,
        set: &PluginSet,
        caller: &CallerIdentity,
        operation_id: Uuid,
        application_id: &str,
        observer: &mut dyn DispatchObserver,
    ) -> Vec<Routed> {
        let mut routed = Vec::with_capacity(set.plugins().len());
        for plugin in set.plugins() {
            if !observer.before(plugin).await {
                break;
            }
            let answer = self
                .calls
                .remove(plugin, caller, operation_id, application_id)
                .await;
            let go_on = observer.after(plugin, &answer).await;
            routed.push((plugin.key.clone(), answer));
            if !go_on {
                break;
            }
        }
        routed
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "router_tests.rs"]
mod tests;
