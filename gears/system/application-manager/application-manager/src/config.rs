//! Gear configuration (DESIGN §4.7).
//!
//! The `gears.application-manager.config` section is strict: every setting is
//! required, there are no defaults, and unknown keys are rejected. Deserialization
//! errors name the offending setting; every [`ApplicationManagerConfig::validate`]
//! failure carries the reason token `invalid_setting`.

use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::domain::limits::REGISTRY_TEXT_MAX_BYTES;

/// Request deadline of the api-gateway timeout layer (DESIGN §4.7, §4.8): the
/// `.timeout(Duration::from_secs(30))` in `gears/system/api-gateway/src/gear.rs`
/// (around line 542). Update both together.
pub const GATEWAY_REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// Upper bound for the concurrency counts. Far below
/// `tokio::sync::Semaphore::MAX_PERMITS`, so a count is always a valid permit count.
const MAX_CONCURRENCY: usize = 1_000_000;

/// Reason token of every `init` configuration failure (DESIGN §4.6).
pub(crate) const REASON: &str = "invalid_setting";

fn duration_field<'de, D: Deserializer<'de>>(
    name: &str,
    deserializer: D,
) -> Result<Duration, D::Error> {
    humantime_serde::deserialize(deserializer)
        .map_err(|e| serde::de::Error::custom(format!("{name}: {e}")))
}

fn count_field<'de, D: Deserializer<'de>>(name: &str, deserializer: D) -> Result<usize, D::Error> {
    usize::deserialize(deserializer).map_err(|e| serde::de::Error::custom(format!("{name}: {e}")))
}

macro_rules! field_deserializer {
    ($fn_name:ident, $helper:ident, $ty:ty, $name:literal) => {
        fn $fn_name<'de, D: Deserializer<'de>>(deserializer: D) -> Result<$ty, D::Error> {
            $helper($name, deserializer)
        }
    };
}

field_deserializer!(
    de_plugin_call_timeout,
    duration_field,
    Duration,
    "plugin_call_timeout"
);
field_deserializer!(
    de_pdp_call_timeout,
    duration_field,
    Duration,
    "pdp_call_timeout"
);
field_deserializer!(
    de_registry_operation_timeout,
    duration_field,
    Duration,
    "registry_operation_timeout"
);
field_deserializer!(
    de_plugin_max_in_flight,
    count_field,
    usize,
    "plugin_max_in_flight"
);
field_deserializer!(
    de_max_request_tasks,
    count_field,
    usize,
    "max_request_tasks"
);
field_deserializer!(
    de_max_envelope_text_length,
    count_field,
    usize,
    "max_envelope_text_length"
);

/// Gear configuration for `gears.application-manager.config` (DESIGN §4.7).
///
/// All settings are required. Durations are humantime strings with a unit.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationManagerConfig {
    /// Timeout of one section/audit plugin call.
    #[serde(deserialize_with = "de_plugin_call_timeout")]
    pub plugin_call_timeout: Duration,
    /// Maximum number of concurrent in-flight plugin calls.
    #[serde(deserialize_with = "de_plugin_max_in_flight")]
    pub plugin_max_in_flight: usize,
    /// Maximum number of concurrently running request tasks.
    #[serde(deserialize_with = "de_max_request_tasks")]
    pub max_request_tasks: usize,
    /// Maximum length in bytes of an envelope text field; at most
    /// [`REGISTRY_TEXT_MAX_BYTES`].
    #[serde(deserialize_with = "de_max_envelope_text_length")]
    pub max_envelope_text_length: usize,
    /// Timeout of one PDP (authorization) call.
    #[serde(deserialize_with = "de_pdp_call_timeout")]
    pub pdp_call_timeout: Duration,
    /// Timeout of one registry operation.
    #[serde(deserialize_with = "de_registry_operation_timeout")]
    pub registry_operation_timeout: Duration,
}

impl ApplicationManagerConfig {
    /// Validates the configuration at `init` (DESIGN §4.7, §4.6).
    ///
    /// # Errors
    ///
    /// Returns an `invalid_setting` error naming the offending setting(s) when a
    /// duration or count is zero, a count is above its bound, or the request time
    /// budget `pdp + 2 x registry + 4 x plugin` exceeds [`GATEWAY_REQUEST_DEADLINE`].
    pub fn validate(&self) -> anyhow::Result<()> {
        for (name, value) in [
            ("plugin_call_timeout", self.plugin_call_timeout),
            ("pdp_call_timeout", self.pdp_call_timeout),
            (
                "registry_operation_timeout",
                self.registry_operation_timeout,
            ),
        ] {
            if value.is_zero() {
                anyhow::bail!("{REASON}: {name} must be a positive duration");
            }
        }
        for (name, value) in [
            ("plugin_max_in_flight", self.plugin_max_in_flight),
            ("max_request_tasks", self.max_request_tasks),
        ] {
            if value == 0 || value > MAX_CONCURRENCY {
                anyhow::bail!("{REASON}: {name} must be between 1 and {MAX_CONCURRENCY}");
            }
        }
        if self.max_envelope_text_length == 0
            || self.max_envelope_text_length > REGISTRY_TEXT_MAX_BYTES
        {
            anyhow::bail!(
                "{REASON}: max_envelope_text_length must be between 1 and {REGISTRY_TEXT_MAX_BYTES}"
            );
        }
        // Each term is bounded by the deadline first, so the sum below cannot overflow.
        let terms = [
            self.pdp_call_timeout,
            self.registry_operation_timeout,
            self.plugin_call_timeout,
        ];
        let within_budget = terms.iter().all(|t| *t <= GATEWAY_REQUEST_DEADLINE)
            && self.pdp_call_timeout
                + self.registry_operation_timeout * 2
                + self.plugin_call_timeout * 4
                <= GATEWAY_REQUEST_DEADLINE;
        if !within_budget {
            anyhow::bail!(
                "{REASON}: pdp_call_timeout + 2 x registry_operation_timeout + 4 x \
                 plugin_call_timeout must not exceed the gateway request deadline of {GATEWAY_REQUEST_DEADLINE:?}"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(
    clippy::unchecked_time_subtraction,
    clippy::duration_suboptimal_units,
    clippy::non_ascii_literal
)]
#[path = "config_tests.rs"]
mod config_tests;
