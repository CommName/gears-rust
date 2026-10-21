//! Gear configuration (DESIGN section 4.2).

use std::time::Duration;

use serde::Deserialize;

use crate::constants::{APPLICATION_ID_MAX_BYTES, GATEWAY_REQUEST_DEADLINE};

/// Operator configuration. Every field is required; there are no defaults.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationManagerConfig {
    /// Time limit of every plugin call; unit of the run window (`humantime`).
    #[serde(with = "toolkit_utils::humantime_serde")]
    pub plugin_call_timeout: Duration,
    /// Added to the run window; at least twice `storage_call_timeout`.
    #[serde(with = "toolkit_utils::humantime_serde")]
    pub run_window_margin: Duration,
    /// Lifetime of a final operation record.
    #[serde(with = "toolkit_utils::humantime_serde")]
    pub operation_retention: Duration,
    /// Calls in flight per plugin instance.
    pub plugin_max_in_flight: u32,
    /// Seed and uninstall request tasks per replica.
    pub max_request_tasks: u32,
    /// Maximum bytes of the application id and version.
    pub max_envelope_text_length: u32,
    /// Deadline of one PDP decision.
    #[serde(with = "toolkit_utils::humantime_serde")]
    pub pdp_call_timeout: Duration,
    /// Bound of every storage access.
    #[serde(with = "toolkit_utils::humantime_serde")]
    pub storage_call_timeout: Duration,
}

/// A rejected setting: names the setting and the broken rule.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid_setting: {setting}: {reason}")]
pub struct InvalidSetting {
    /// The setting name.
    pub setting: &'static str,
    /// The broken rule.
    pub reason: &'static str,
}

impl ApplicationManagerConfig {
    /// Check every rule, reporting the first failure in table order.
    ///
    /// # Errors
    /// [`InvalidSetting`] for the first failing rule.
    pub fn validate(&self) -> Result<(), InvalidSetting> {
        let reject = |setting, reason| Err(InvalidSetting { setting, reason });

        if self.plugin_call_timeout.is_zero() {
            return reject("plugin_call_timeout", "must be greater than zero");
        }
        let margin_ok = !self.run_window_margin.is_zero()
            && self
                .storage_call_timeout
                .checked_mul(2)
                .is_some_and(|floor| self.run_window_margin >= floor);
        if !margin_ok {
            return reject(
                "run_window_margin",
                "must be greater than zero and at least twice storage_call_timeout",
            );
        }
        if self.operation_retention.is_zero() {
            return reject("operation_retention", "must be greater than zero");
        }
        if self.plugin_max_in_flight < 1 {
            return reject("plugin_max_in_flight", "must be at least 1");
        }
        if self.max_request_tasks < 1 {
            return reject("max_request_tasks", "must be at least 1");
        }
        if !(1..=APPLICATION_ID_MAX_BYTES).contains(&self.max_envelope_text_length) {
            return reject(
                "max_envelope_text_length",
                "must be between 1 and 255 (APPLICATION_ID_MAX_BYTES)",
            );
        }
        if self.pdp_call_timeout.is_zero() {
            return reject("pdp_call_timeout", "must be greater than zero");
        }
        if self.storage_call_timeout.is_zero() {
            return reject("storage_call_timeout", "must be greater than zero");
        }
        let within_budget = self
            .storage_call_timeout
            .checked_mul(3)
            .and_then(|storage| storage.checked_add(self.pdp_call_timeout))
            .is_some_and(|total| total <= GATEWAY_REQUEST_DEADLINE);
        if !within_budget {
            return reject(
                "pdp_call_timeout",
                "pdp_call_timeout plus three storage_call_timeout must not exceed 30s (GATEWAY_REQUEST_DEADLINE)",
            );
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "config_tests.rs"]
mod tests;
