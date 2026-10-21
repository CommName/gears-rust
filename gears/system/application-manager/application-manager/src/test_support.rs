//! Test fixtures shared by the config and gear tests.

use serde_json::{Value, json};

/// The valid operator configuration, defined once.
pub fn valid_config_json() -> Value {
    json!({
        "plugin_call_timeout": "10s",
        "run_window_margin": "30s",
        "operation_retention": "7d",
        "plugin_max_in_flight": 4,
        "max_request_tasks": 16,
        "max_envelope_text_length": 255,
        "pdp_call_timeout": "2s",
        "storage_call_timeout": "2s"
    })
}
