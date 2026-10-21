#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use serde_json::{Value, json};

use super::*;
use crate::constants::{APPLICATION_ID_MAX_BYTES, GATEWAY_REQUEST_DEADLINE};

fn valid_json() -> Value {
    crate::test_support::valid_config_json()
}

fn parse(value: Value) -> Result<ApplicationManagerConfig, serde_json::Error> {
    serde_json::from_value(value)
}

fn with(field: &str, value: Value) -> Value {
    let mut v = valid_json();
    v[field] = value;
    v
}

fn with_all(fields: &[(&str, Value)]) -> Value {
    let mut v = valid_json();
    for (k, val) in fields {
        v[*k] = val.clone();
    }
    v
}

fn ms(n: u64) -> Value {
    json!(format!("{n}ms"))
}

#[test]
fn config_parse_complete_block_with_humantime_values() {
    let cfg = parse(valid_json()).unwrap();
    assert_eq!(cfg.plugin_call_timeout, Duration::from_secs(10));
    assert_eq!(cfg.run_window_margin, Duration::from_secs(30));
    assert_eq!(cfg.operation_retention, Duration::from_hours(7 * 24));
    assert_eq!(cfg.plugin_max_in_flight, 4);
    assert_eq!(cfg.max_request_tasks, 16);
    assert_eq!(cfg.max_envelope_text_length, 255);
    assert_eq!(cfg.pdp_call_timeout, Duration::from_secs(2));
    assert_eq!(cfg.storage_call_timeout, Duration::from_secs(2));
}

#[test]
fn config_parse_missing_one_field_fails() {
    let fields = [
        "plugin_call_timeout",
        "run_window_margin",
        "operation_retention",
        "plugin_max_in_flight",
        "max_request_tasks",
        "max_envelope_text_length",
        "pdp_call_timeout",
        "storage_call_timeout",
    ];
    for field in fields {
        let mut v = valid_json();
        v.as_object_mut().unwrap().remove(field);
        let err = parse(v).expect_err(field);
        assert!(err.to_string().contains(field), "{field}: {err}");
    }
}

#[test]
fn config_parse_unknown_field_fails() {
    let mut v = valid_json();
    v["surprise"] = json!(1);
    let err = parse(v).unwrap_err();
    assert!(err.to_string().contains("surprise"), "{err}");
}

#[test]
fn config_parse_bad_duration_fails() {
    assert!(parse(with("plugin_call_timeout", json!("soon"))).is_err());
}

#[test]
fn config_validate_valid_block_passes() {
    parse(valid_json()).unwrap().validate().unwrap();
}

#[test]
fn config_validate_rules_table() {
    // (name, overrides, expected failing setting or None for ok)
    let deadline_ms = u64::try_from(GATEWAY_REQUEST_DEADLINE.as_millis()).unwrap();
    // pdp + 3 * storage == deadline: storage 2s, pdp 24s.
    let pdp_at_budget = deadline_ms - 3 * 2000;
    let cases: Vec<(&str, Value, Option<&str>)> = vec![
        (
            "plugin_call_zero",
            with("plugin_call_timeout", ms(0)),
            Some("plugin_call_timeout"),
        ),
        (
            "plugin_call_one_ms",
            with("plugin_call_timeout", ms(1)),
            None,
        ),
        (
            "margin_zero",
            with("run_window_margin", ms(0)),
            Some("run_window_margin"),
        ),
        (
            "margin_zero_with_storage_zero",
            with_all(&[
                ("run_window_margin", ms(0)),
                ("storage_call_timeout", ms(0)),
            ]),
            Some("run_window_margin"),
        ),
        (
            "margin_and_storage_max_overflow_doubling",
            with_all(&[
                ("run_window_margin", json!("18446744073709551615s")),
                ("storage_call_timeout", json!("18446744073709551615s")),
            ]),
            Some("run_window_margin"),
        ),
        (
            "margin_equals_twice_storage",
            with_all(&[
                ("run_window_margin", ms(4000)),
                ("storage_call_timeout", ms(2000)),
            ]),
            None,
        ),
        (
            "margin_one_ms_below_twice_storage",
            with_all(&[
                ("run_window_margin", ms(3999)),
                ("storage_call_timeout", ms(2000)),
            ]),
            Some("run_window_margin"),
        ),
        (
            "retention_zero",
            with("operation_retention", ms(0)),
            Some("operation_retention"),
        ),
        (
            "in_flight_zero",
            with("plugin_max_in_flight", json!(0)),
            Some("plugin_max_in_flight"),
        ),
        (
            "in_flight_one",
            with("plugin_max_in_flight", json!(1)),
            None,
        ),
        (
            "request_tasks_zero",
            with("max_request_tasks", json!(0)),
            Some("max_request_tasks"),
        ),
        (
            "request_tasks_one",
            with("max_request_tasks", json!(1)),
            None,
        ),
        (
            "text_length_zero",
            with("max_envelope_text_length", json!(0)),
            Some("max_envelope_text_length"),
        ),
        (
            "text_length_one",
            with("max_envelope_text_length", json!(1)),
            None,
        ),
        (
            "text_length_at_max",
            with("max_envelope_text_length", json!(APPLICATION_ID_MAX_BYTES)),
            None,
        ),
        (
            "text_length_above_max",
            with(
                "max_envelope_text_length",
                json!(APPLICATION_ID_MAX_BYTES + 1),
            ),
            Some("max_envelope_text_length"),
        ),
        (
            "pdp_zero",
            with("pdp_call_timeout", ms(0)),
            Some("pdp_call_timeout"),
        ),
        (
            "storage_zero",
            with("storage_call_timeout", ms(0)),
            Some("storage_call_timeout"),
        ),
        (
            "budget_equals_deadline",
            with_all(&[
                ("pdp_call_timeout", ms(pdp_at_budget)),
                ("storage_call_timeout", ms(2000)),
            ]),
            None,
        ),
        (
            "budget_one_ms_over_deadline",
            with_all(&[
                ("pdp_call_timeout", ms(pdp_at_budget + 1)),
                ("storage_call_timeout", ms(2000)),
            ]),
            Some("pdp_call_timeout"),
        ),
    ];
    for (name, input, expected) in cases {
        let cfg = parse(input).unwrap_or_else(|e| panic!("{name}: {e}"));
        match (cfg.validate(), expected) {
            (Ok(()), None) => {}
            (Err(e), Some(setting)) => {
                assert_eq!(e.setting, setting, "{name}: {e}");
                assert!(
                    e.to_string()
                        .starts_with(&format!("invalid_setting: {setting}")),
                    "{name}: {e}"
                );
            }
            (got, want) => panic!("{name}: got {got:?}, want {want:?}"),
        }
    }
}

#[test]
fn config_validate_reports_first_failing_rule_in_table_order() {
    // Every rule fails; fix one rule at a time in DESIGN 4.2 order and the
    // next setting must be reported.
    let mut cfg = ApplicationManagerConfig {
        plugin_call_timeout: Duration::ZERO,
        run_window_margin: Duration::ZERO,
        operation_retention: Duration::ZERO,
        plugin_max_in_flight: 0,
        max_request_tasks: 0,
        max_envelope_text_length: 0,
        pdp_call_timeout: Duration::ZERO,
        storage_call_timeout: Duration::ZERO,
    };
    assert_eq!(cfg.validate().unwrap_err().setting, "plugin_call_timeout");
    cfg.plugin_call_timeout = Duration::from_secs(1);
    assert_eq!(cfg.validate().unwrap_err().setting, "run_window_margin");
    cfg.run_window_margin = Duration::from_secs(1);
    assert_eq!(cfg.validate().unwrap_err().setting, "operation_retention");
    cfg.operation_retention = Duration::from_secs(1);
    assert_eq!(cfg.validate().unwrap_err().setting, "plugin_max_in_flight");
    cfg.plugin_max_in_flight = 1;
    assert_eq!(cfg.validate().unwrap_err().setting, "max_request_tasks");
    cfg.max_request_tasks = 1;
    assert_eq!(
        cfg.validate().unwrap_err().setting,
        "max_envelope_text_length"
    );
    cfg.max_envelope_text_length = 1;
    assert_eq!(cfg.validate().unwrap_err().setting, "pdp_call_timeout");
    cfg.pdp_call_timeout = Duration::from_secs(1);
    assert_eq!(cfg.validate().unwrap_err().setting, "storage_call_timeout");
    cfg.storage_call_timeout = Duration::from_millis(500);
    cfg.validate().unwrap();
}

#[test]
fn config_validate_huge_durations_error_at_budget_without_panic() {
    let base = ApplicationManagerConfig {
        plugin_call_timeout: Duration::from_secs(1),
        run_window_margin: Duration::from_secs(30),
        operation_retention: Duration::from_secs(1),
        plugin_max_in_flight: 1,
        max_request_tasks: 1,
        max_envelope_text_length: 1,
        pdp_call_timeout: Duration::from_secs(1),
        storage_call_timeout: Duration::from_secs(1),
    };
    // Case A: 2x storage fits so the margin rule passes; 3x overflows.
    let a = ApplicationManagerConfig {
        run_window_margin: Duration::MAX,
        pdp_call_timeout: Duration::from_secs(1),
        storage_call_timeout: Duration::from_secs(u64::MAX >> 1),
        ..base
    };
    assert_eq!(a.validate().unwrap_err().setting, "pdp_call_timeout");
    // Case B: 3x storage fits, adding the pdp timeout overflows.
    let b = ApplicationManagerConfig {
        pdp_call_timeout: Duration::MAX,
        ..base
    };
    assert_eq!(b.validate().unwrap_err().setting, "pdp_call_timeout");
}

#[test]
fn config_invalid_setting_display_has_prefix_setting_and_reason() {
    let e = InvalidSetting {
        setting: "max_request_tasks",
        reason: "must be at least 1",
    };
    assert_eq!(
        e.to_string(),
        "invalid_setting: max_request_tasks: must be at least 1"
    );
}

#[test]
fn config_reasons_state_the_limits_of_the_constants() {
    let too_long = parse(with(
        "max_envelope_text_length",
        json!(APPLICATION_ID_MAX_BYTES + 1),
    ))
    .unwrap()
    .validate()
    .unwrap_err();
    assert!(
        too_long
            .reason
            .contains(&format!("1 and {APPLICATION_ID_MAX_BYTES} ")),
        "{too_long}"
    );

    let over_budget = parse(with("pdp_call_timeout", json!("1h")))
        .unwrap()
        .validate()
        .unwrap_err();
    assert_eq!(over_budget.setting, "pdp_call_timeout");
    assert!(
        over_budget
            .reason
            .contains(&format!("{}s ", GATEWAY_REQUEST_DEADLINE.as_secs())),
        "{over_budget}"
    );
}
