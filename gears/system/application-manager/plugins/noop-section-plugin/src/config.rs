//! Plugin configuration.

use serde::Deserialize;

/// No-op section plugin configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NoopSectionPluginConfig {
    /// Vendor of the GTS instance.
    pub vendor: String,
    /// Priority of the GTS instance; it must differ from every other section
    /// plugin's (lower dispatches first).
    pub priority: i16,
}

impl Default for NoopSectionPluginConfig {
    fn default() -> Self {
        Self {
            vendor: "constructorfabric".to_owned(),
            priority: 100,
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "config_tests.rs"]
mod tests;
