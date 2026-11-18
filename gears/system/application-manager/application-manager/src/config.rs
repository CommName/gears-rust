//! Gear configuration (DESIGN section 4.2).

use std::borrow::Cow;
use std::collections::HashSet;
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
    /// Added to the run window for validation and scheduling delay.
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
    /// Platform identities, each with the actions it may take.
    pub allowed_callers: Vec<AllowedCallerEntry>,
    /// Bound of every storage access.
    #[serde(with = "toolkit_utils::humantime_serde")]
    pub storage_call_timeout: Duration,
}

/// One allowed caller: a platform identity and its actions.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedCallerEntry {
    /// The platform identity this entry matches.
    pub identity: AllowedIdentity,
    /// The actions the identity may take; never empty.
    pub actions: Vec<CallerAction>,
}

/// A platform identity an entry matches. Any other identity kind, `Shared`
/// included, fails to parse.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum AllowedIdentity {
    /// A Kubernetes service account; matches any pod.
    KubernetesServiceAccount {
        /// Namespace of the service account.
        namespace: String,
        /// Service account name.
        service_account: String,
    },
    /// A SPIFFE workload identity; matches any version.
    Spiffe {
        /// SPIFFE trust domain.
        trust_domain: String,
        /// Workload name.
        name: String,
    },
}

/// A platform-plane action granted by the allowed callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallerAction {
    /// Seed an application.
    Seed,
    /// Uninstall an application.
    Uninstall,
    /// Read the section keys and schemas.
    ReadSchema,
    /// Read an operation.
    ReadOperation,
}

/// A rejected setting: names the setting and the broken rule.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid_setting: {setting}: {reason}")]
pub struct InvalidSetting {
    /// The setting name.
    pub setting: &'static str,
    /// The broken rule; never echoes configured values beyond durations,
    /// counts, and entry indexes.
    pub reason: Cow<'static, str>,
}

impl ApplicationManagerConfig {
    /// Check every rule, reporting the first failure in table order.
    ///
    /// # Errors
    /// [`InvalidSetting`] for the first failing rule.
    pub fn validate(&self) -> Result<(), InvalidSetting> {
        let reject = |setting, reason: &'static str| {
            Err(InvalidSetting {
                setting,
                reason: Cow::Borrowed(reason),
            })
        };

        if self.plugin_call_timeout.is_zero() {
            return reject("plugin_call_timeout", "must be greater than zero");
        }
        if self.run_window_margin.is_zero() {
            return reject("run_window_margin", "must be greater than zero");
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
        self.validate_allowed_callers()?;
        if self.storage_call_timeout.is_zero() {
            return reject("storage_call_timeout", "must be greater than zero");
        }
        let within_budget = self
            .storage_call_timeout
            .checked_mul(3)
            .is_some_and(|total| total <= GATEWAY_REQUEST_DEADLINE);
        if !within_budget {
            return reject(
                "storage_call_timeout",
                "three storage_call_timeout must not exceed 30s (GATEWAY_REQUEST_DEADLINE)",
            );
        }
        Ok(())
    }

    /// Every entry has actions and no two entries share an identity. Errors
    /// name the entry index, not the identity.
    fn validate_allowed_callers(&self) -> Result<(), InvalidSetting> {
        let mut seen = HashSet::new();
        for (index, entry) in self.allowed_callers.iter().enumerate() {
            let reason = if entry.actions.is_empty() {
                "has no actions"
            } else if !seen.insert(&entry.identity) {
                "repeats the identity of an earlier entry"
            } else {
                continue;
            };
            return Err(InvalidSetting {
                setting: "allowed_callers",
                reason: Cow::Owned(format!("entry {index} {reason}")),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "config_tests.rs"]
mod tests;
