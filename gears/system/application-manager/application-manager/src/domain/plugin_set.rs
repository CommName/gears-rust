//! The section plugin set, built once at startup and fixed for the process
//! lifetime (`cpt-cf-application-manager-principle-startup-fixed-plugin-set`).

use std::fmt;
use std::sync::{Arc, OnceLock};

use application_manager_sdk::{SECTION_BASE_TYPE, SectionPluginClientV1};
use toolkit_macros::domain_model;

use crate::domain::schema_validator::CompiledSection;

/// The section key of `section_type`, or `None` when the type is not derived
/// directly from the section base type as one segment
/// `<vendor>.<package>.<namespace>.<type>.v<major>[.<minor>]~` (DESIGN 3.3).
///
/// The key is that segment without its version and trailing `~`:
/// `gts.cf.core.application_manager.section.v1~acme.iam.access.roles.v1.2~`
/// gives `acme.iam.access.roles`.
#[must_use]
pub fn section_key_of(section_type: &str) -> Option<String> {
    let segment = section_type
        .strip_prefix(SECTION_BASE_TYPE)?
        .strip_suffix('~')?;
    if segment.contains('~') {
        return None;
    }
    let parts: Vec<&str> = segment.split('.').collect();
    let (name, version) = match parts.len() {
        5 | 6 => parts.split_at(4),
        _ => return None,
    };
    let is_token = |t: &str| {
        t.bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
            && t.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    };
    let is_number = |t: &str| !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit());
    let major = version[0].strip_prefix('v')?;
    if !name.iter().all(|t| is_token(t))
        || !is_number(major)
        || !version[1..].iter().all(|t| is_number(t))
    {
        return None;
    }
    Some(name.join("."))
}

/// One section plugin as startup found it.
#[domain_model]
pub struct SectionPlugin {
    /// The section key, derived from the section type.
    pub key: String,
    /// The section type named by the plugin instance.
    pub section_type: String,
    /// The priority of its GTS instance; lower dispatches first.
    pub priority: i16,
    /// Its GTS instance id, also the scope of its `ClientHub` client.
    pub instance_id: String,
    /// Its scoped `ClientHub` client.
    pub client: Arc<dyn SectionPluginClientV1>,
    /// The section type's schema, composed under the key and compiled at
    /// startup.
    pub section: Arc<CompiledSection>,
}

impl fmt::Debug for SectionPlugin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SectionPlugin")
            .field("key", &self.key)
            .field("section_type", &self.section_type)
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

    /// The compiled section schemas in priority order.
    #[must_use]
    pub fn sections(&self) -> Vec<Arc<CompiledSection>> {
        self.plugins
            .iter()
            .map(|p| Arc::clone(&p.section))
            .collect()
    }

    /// The plugin owning `key`, if any.
    #[must_use]
    pub fn plugin(&self, key: &str) -> Option<&SectionPlugin> {
        self.plugins.iter().find(|p| p.key == key)
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
