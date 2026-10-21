//! The section plugin set, built once at startup and fixed for the process
//! lifetime (`cpt-cf-application-manager-principle-startup-fixed-plugin-set`).

use std::fmt;
use std::sync::{Arc, OnceLock};

use application_manager_sdk::SectionPluginClientV1;
use toolkit_macros::domain_model;

/// One section plugin as startup found it.
#[domain_model]
pub struct SectionPlugin {
    /// The key the plugin announced at startup.
    pub key: String,
    /// The priority of its GTS instance; lower dispatches first.
    pub priority: i16,
    /// Its GTS instance id, also the scope of its `ClientHub` client.
    pub instance_id: String,
    /// Its scoped `ClientHub` client.
    pub client: Arc<dyn SectionPluginClientV1>,
}

impl fmt::Debug for SectionPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SectionPlugin")
            .field("key", &self.key)
            .field("priority", &self.priority)
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

/// Every section plugin, in priority order.
#[domain_model]
#[derive(Debug, Default)]
pub struct PluginSet {
    plugins: Vec<SectionPlugin>,
}

impl PluginSet {
    /// A set ordered by ascending priority.
    #[must_use]
    pub fn new(mut plugins: Vec<SectionPlugin>) -> Self {
        plugins.sort_by_key(|p| p.priority);
        Self { plugins }
    }

    /// The plugins in priority order.
    #[must_use]
    pub fn plugins(&self) -> &[SectionPlugin] {
        &self.plugins
    }

    /// The section keys in priority order.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.plugins.iter().map(|p| p.key.clone()).collect()
    }
}

/// The startup result, published once by `start` and read by requests.
#[domain_model]
#[derive(Debug, Default)]
pub struct StartupState {
    plugin_set: OnceLock<Arc<PluginSet>>,
}

impl StartupState {
    /// Publish the plugin set; requests are served from then on.
    ///
    /// # Errors
    /// Returns the set back when a result was already published.
    pub fn publish(&self, set: PluginSet) -> Result<(), Arc<PluginSet>> {
        self.plugin_set.set(Arc::new(set))
    }

    /// The published plugin set, or `None` while startup has not finished.
    #[must_use]
    pub fn plugin_set(&self) -> Option<Arc<PluginSet>> {
        self.plugin_set.get().cloned()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "plugin_set_tests.rs"]
mod tests;
