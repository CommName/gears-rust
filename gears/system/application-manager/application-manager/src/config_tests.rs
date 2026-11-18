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

fn ms(n: u64) -> Value {
    json!(format!("{n}ms"))
}

fn service_account(namespace: &str, name: &str) -> Value {
    json!({
        "type": "KubernetesServiceAccount",
        "namespace": namespace,
        "service_account": name
    })
}

fn entry(identity: Value, actions: Value) -> Value {
    let mut v = json!({});
    v["identity"] = identity;
    v["actions"] = actions;
    v
}

fn with_callers(entries: Value) -> Value {
    with("allowed_callers", entries)
}

fn valid_struct() -> ApplicationManagerConfig {
    parse(valid_json()).unwrap()
}

#[test]
fn config_parse_complete_block_with_humantime_values() {
    let cfg = valid_struct();
    assert_eq!(cfg.plugin_call_timeout, Duration::from_secs(10));
    assert_eq!(cfg.run_window_margin, Duration::from_secs(30));
    assert_eq!(cfg.operation_retention, Duration::from_hours(7 * 24));
    assert_eq!(cfg.plugin_max_in_flight, 4);
    assert_eq!(cfg.max_request_tasks, 16);
    assert_eq!(cfg.max_envelope_text_length, 255);
    assert_eq!(cfg.pdp_call_timeout, Duration::from_secs(2));
    assert_eq!(
        cfg.allowed_callers,
        vec![AllowedCallerEntry {
            identity: AllowedIdentity::KubernetesServiceAccount {
                namespace: "deploy".to_owned(),
                service_account: "app-seeder".to_owned(),
            },
            actions: vec![
                CallerAction::Seed,
                CallerAction::Uninstall,
                CallerAction::ReadSchema,
                CallerAction::ReadOperation,
            ],
        }]
    );
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
        "allowed_callers",
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
    valid_struct().validate().unwrap();
}

#[test]
fn config_validate_rules_table() {
    // (name, overrides, expected failing setting or None for ok)
    // 3 x 10s is exactly the 30s deadline.
    assert_eq!(GATEWAY_REQUEST_DEADLINE, Duration::from_secs(30));
    let storage_at_budget = 10_000;
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
        ("margin_one_ms", with("run_window_margin", ms(1)), None),
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
            "pdp_large_does_not_count_in_budget",
            with("pdp_call_timeout", json!("1h")),
            None,
        ),
        ("callers_empty_list", with_callers(json!([])), None),
        (
            "callers_empty_actions",
            with_callers(json!([entry(service_account("deploy", "a"), json!([]))])),
            Some("allowed_callers"),
        ),
        (
            "callers_duplicate_identity",
            with_callers(json!([
                entry(service_account("deploy", "a"), json!(["seed"])),
                entry(service_account("deploy", "a"), json!(["uninstall"])),
            ])),
            Some("allowed_callers"),
        ),
        (
            "callers_same_name_other_namespace",
            with_callers(json!([
                entry(service_account("deploy", "a"), json!(["seed"])),
                entry(service_account("ops", "a"), json!(["seed"])),
            ])),
            None,
        ),
        (
            "storage_zero",
            with("storage_call_timeout", ms(0)),
            Some("storage_call_timeout"),
        ),
        (
            "budget_equals_deadline",
            with("storage_call_timeout", ms(storage_at_budget)),
            None,
        ),
        (
            "budget_one_ms_over_deadline",
            with("storage_call_timeout", ms(storage_at_budget + 1)),
            Some("storage_call_timeout"),
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
        allowed_callers: vec![AllowedCallerEntry {
            identity: AllowedIdentity::Spiffe {
                trust_domain: "example.org".to_owned(),
                name: "seeder".to_owned(),
            },
            actions: vec![],
        }],
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
    assert_eq!(cfg.validate().unwrap_err().setting, "allowed_callers");
    cfg.allowed_callers.clear();
    assert_eq!(cfg.validate().unwrap_err().setting, "storage_call_timeout");
    cfg.storage_call_timeout = Duration::from_millis(500);
    cfg.validate().unwrap();
}

#[test]
fn config_validate_huge_storage_timeout_errors_without_panic() {
    let cfg = ApplicationManagerConfig {
        storage_call_timeout: Duration::MAX,
        ..valid_struct()
    };
    assert_eq!(cfg.validate().unwrap_err().setting, "storage_call_timeout");
}

#[test]
fn config_parse_spiffe_entry() {
    let cfg = parse(with_callers(json!([entry(
        json!({ "type": "Spiffe", "trust_domain": "example.org", "name": "seeder" }),
        json!(["read_operation"])
    )])))
    .unwrap();
    assert_eq!(
        cfg.allowed_callers[0].identity,
        AllowedIdentity::Spiffe {
            trust_domain: "example.org".to_owned(),
            name: "seeder".to_owned(),
        }
    );
    assert_eq!(
        cfg.allowed_callers[0].actions,
        vec![CallerAction::ReadOperation]
    );
}

#[test]
fn config_parse_refuses_other_identity_kinds_actions_and_fields() {
    let cases = [
        (
            "shared_identity",
            entry(json!({ "type": "Shared", "name": "dev" }), json!(["seed"])),
        ),
        (
            "unknown_identity",
            entry(json!({ "type": "Robot", "name": "r" }), json!(["seed"])),
        ),
        (
            "unknown_action",
            entry(service_account("deploy", "a"), json!(["delete_everything"])),
        ),
        (
            "unknown_identity_field",
            entry(
                json!({
                    "type": "KubernetesServiceAccount",
                    "namespace": "deploy",
                    "service_account": "a",
                    "pod": "p"
                }),
                json!(["seed"]),
            ),
        ),
        (
            "unknown_entry_field",
            json!({
                "identity": service_account("deploy", "a"),
                "actions": ["seed"],
                "note": "x"
            }),
        ),
    ];
    for (name, bad) in cases {
        assert!(parse(with_callers(json!([bad]))).is_err(), "{name}");
    }
}

#[test]
fn config_allowed_callers_error_names_index_not_identity() {
    let err = parse(with_callers(json!([
        entry(service_account("deploy", "secret-seeder"), json!(["seed"])),
        entry(service_account("deploy", "secret-seeder"), json!(["seed"])),
    ])))
    .unwrap()
    .validate()
    .unwrap_err();
    let text = err.to_string();
    assert!(
        text.starts_with("invalid_setting: allowed_callers: entry 1 "),
        "{text}"
    );
    assert!(!text.contains("secret-seeder"), "{text}");
    assert!(!text.contains("deploy"), "{text}");

    let err = parse(with_callers(json!([
        entry(service_account("deploy", "a"), json!(["seed"])),
        entry(service_account("deploy", "b"), json!([])),
    ])))
    .unwrap()
    .validate()
    .unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid_setting: allowed_callers: entry 1 has no actions"
    );
}

#[test]
fn config_invalid_setting_display_has_prefix_setting_and_reason() {
    let e = InvalidSetting {
        setting: "max_request_tasks",
        reason: "must be at least 1".into(),
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

    let over_budget = parse(with("storage_call_timeout", json!("1h")))
        .unwrap()
        .validate()
        .unwrap_err();
    assert_eq!(over_budget.setting, "storage_call_timeout");
    assert!(
        over_budget
            .reason
            .contains(&format!("{}s ", GATEWAY_REQUEST_DEADLINE.as_secs())),
        "{over_budget}"
    );
}
