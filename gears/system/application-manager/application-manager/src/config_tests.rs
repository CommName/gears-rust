//! Unit tests for the `gears.application-manager.config` surface (DESIGN §4.7, §4.6,
//! §4.8).
//!
//! Planned API: `ApplicationManagerConfig` (strict: unknown keys are rejected) with
//! `validate()`, and `GATEWAY_REQUEST_DEADLINE` in `crate::config`. The limit
//! `REGISTRY_TEXT_MAX_BYTES` lives in `crate::domain::limits` (DESIGN §3.7).
//!
//! Every failure message names the offending setting. Every `validate()` failure also
//! carries the reason token `invalid_setting`.

use std::time::Duration;

use proptest::prelude::*;
use serde_json::{Value, json};

use crate::config::{ApplicationManagerConfig, GATEWAY_REQUEST_DEADLINE};
use crate::domain::limits::REGISTRY_TEXT_MAX_BYTES;

const REASON: &str = "invalid_setting";
const SETTINGS: [&str; 6] = [
    "plugin_call_timeout",
    "plugin_max_in_flight",
    "max_request_tasks",
    "max_envelope_text_length",
    "pdp_call_timeout",
    "registry_operation_timeout",
];
const DURATION_SETTINGS: [&str; 3] = [
    "plugin_call_timeout",
    "pdp_call_timeout",
    "registry_operation_timeout",
];
const COUNT_SETTINGS: [&str; 3] = [
    "plugin_max_in_flight",
    "max_request_tasks",
    "max_envelope_text_length",
];
/// Largest duration the humantime string grammar and `Duration` both hold.
const HUGE_DURATION: &str = "18446744073709551615s";
/// The smallest valid value used to isolate one budget term.
const MIN_TERM: Duration = Duration::from_millis(1);

fn valid_json() -> Value {
    json!({
        "plugin_call_timeout": "500ms",
        "plugin_max_in_flight": 8,
        "max_request_tasks": 16,
        "max_envelope_text_length": REGISTRY_TEXT_MAX_BYTES.min(256),
        "pdp_call_timeout": "2s",
        "registry_operation_timeout": "3s"
    })
}

fn with(field: &str, value: Value) -> Value {
    let mut v = valid_json();
    v[field] = value;
    v
}

fn parse(v: Value) -> ApplicationManagerConfig {
    serde_json::from_value(v).expect("config should deserialize")
}

fn valid_config(v: Value) -> ApplicationManagerConfig {
    let cfg = parse(v);
    cfg.validate().expect("valid config");
    cfg
}

/// The error text of a configuration that fails at deserialization or validation.
fn load_error(v: Value) -> String {
    match serde_json::from_value::<ApplicationManagerConfig>(v) {
        Err(e) => e.to_string(),
        Ok(cfg) => cfg
            .validate()
            .expect_err("configuration must be rejected")
            .to_string(),
    }
}

/// The error text of a configuration that deserializes but fails `validate()`.
fn validation_error(v: Value) -> String {
    parse(v)
        .validate()
        .expect_err("validate must fail")
        .to_string()
}

fn ms(d: Duration) -> String {
    format!("{}ms", d.as_millis())
}

fn budget_json(pdp: Duration, registry: Duration, plugin: Duration) -> Value {
    let mut v = valid_json();
    v["pdp_call_timeout"] = json!(ms(pdp));
    v["registry_operation_timeout"] = json!(ms(registry));
    v["plugin_call_timeout"] = json!(ms(plugin));
    v
}

fn budget_ok(pdp: Duration, registry: Duration, plugin: Duration) -> bool {
    parse(budget_json(pdp, registry, plugin)).validate().is_ok()
}

// --- Pins -------------------------------------------------------------------------

// DESIGN §4.7, §4.8: the request budget is checked against the api-gateway's request
// deadline.
#[test]
fn gateway_request_deadline_is_pinned_to_30s() {
    assert_eq!(
        GATEWAY_REQUEST_DEADLINE,
        Duration::from_secs(30),
        "must equal the api-gateway timeout layer: gears/system/api-gateway/src/gear.rs \
         `.timeout(Duration::from_secs(30))` (around line 542); update both together"
    );
}

// --- Happy path -------------------------------------------------------------------

// DESIGN §4.7: a complete configuration validates, and every setting is parsed into its
// own field (distinct values catch swapped fields and unit mistakes).
#[test]
fn config_accepts_complete_json_and_parses_every_field() {
    let cfg = valid_config(valid_json());
    assert_eq!(cfg.plugin_call_timeout, Duration::from_millis(500));
    assert_eq!(cfg.plugin_max_in_flight, 8);
    assert_eq!(cfg.max_request_tasks, 16);
    assert_eq!(
        cfg.max_envelope_text_length,
        REGISTRY_TEXT_MAX_BYTES.min(256)
    );
    assert_eq!(cfg.pdp_call_timeout, Duration::from_secs(2));
    assert_eq!(cfg.registry_operation_timeout, Duration::from_secs(3));
}

// DESIGN §4.7: durations are humantime strings with a unit.
#[test]
fn durations_accept_humantime_strings_with_units() {
    for (text, expected) in [
        ("500ms", Duration::from_millis(500)),
        ("2s", Duration::from_secs(2)),
        ("1m", Duration::from_secs(60)),
    ] {
        for field in DURATION_SETTINGS {
            // Keep the request budget valid: only `1m` would exceed it as a plugin
            // timeout, so it is checked at deserialization only.
            let cfg = parse(with(field, json!(text)));
            let parsed = match field {
                "plugin_call_timeout" => cfg.plugin_call_timeout,
                "pdp_call_timeout" => cfg.pdp_call_timeout,
                _ => cfg.registry_operation_timeout,
            };
            assert_eq!(parsed, expected, "{field} = {text}");
        }
    }
}

// --- Required, strict -------------------------------------------------------------

// DESIGN §4.7: every setting is required, with no default.
#[test]
fn config_rejects_each_missing_setting() {
    for field in SETTINGS {
        let mut v = valid_json();
        v.as_object_mut().expect("object").remove(field);
        let msg = load_error(v);
        assert!(msg.contains(field), "{field}: {msg}");
    }
}

#[test]
fn config_rejects_empty_object() {
    let r = serde_json::from_value::<ApplicationManagerConfig>(json!({}));
    assert!(r.is_err(), "an empty section has no defaults");
}

// DESIGN §4.7: unknown keys are rejected, so a stale or misspelled setting never
// silently applies. `startup_retry_deadline` was retired (no startup retry; PRD).
#[test]
fn config_rejects_retired_startup_retry_deadline() {
    let msg = load_error(with("startup_retry_deadline", json!("30s")));
    assert!(msg.contains("startup_retry_deadline"), "{msg}");
}

#[test]
fn config_rejects_arbitrary_unknown_key() {
    let msg = load_error(with("no_such_setting", json!(1)));
    assert!(msg.contains("no_such_setting"), "{msg}");
}

// --- Per-setting edge values ------------------------------------------------------

// DESIGN §4.7: each duration is a positive humantime duration with a unit. Every
// failure names the setting; a rejection by `validate()` carries `invalid_setting`.
#[test]
fn duration_settings_reject_edge_values_with_field_name() {
    for field in DURATION_SETTINGS {
        for bad in [
            json!("5"),
            json!("0"),
            json!("0s"),
            json!("fast"),
            json!(""),
            json!(5),
            json!(-1),
            json!(null),
        ] {
            let msg = load_error(with(field, bad.clone()));
            assert!(msg.contains(field), "{field} = {bad}: {msg}");
        }
    }
}

#[test]
fn zero_durations_fail_validation_with_invalid_setting_and_field_name() {
    for field in DURATION_SETTINGS {
        let msg = validation_error(with(field, json!("0s")));
        assert!(msg.contains(REASON), "{field}: {msg}");
        assert!(msg.contains(field), "{field}: {msg}");
    }
}

// DESIGN §4.7: each count is a positive integer.
#[test]
fn count_settings_reject_edge_values_with_field_name() {
    for field in COUNT_SETTINGS {
        for bad in [
            json!(0),
            json!(-1),
            json!(1.5),
            json!("many"),
            json!("8"),
            json!(""),
            json!(null),
        ] {
            let msg = load_error(with(field, bad.clone()));
            assert!(msg.contains(field), "{field} = {bad}: {msg}");
        }
    }
}

#[test]
fn zero_counts_fail_validation_with_invalid_setting_and_field_name() {
    for field in COUNT_SETTINGS {
        let msg = validation_error(with(field, json!(0)));
        assert!(msg.contains(REASON), "{field}: {msg}");
        assert!(msg.contains(field), "{field}: {msg}");
    }
}

// DESIGN §4.7: no upper bound is specified for the in-flight and task counts, so
// absurdly large values must end as a clean `invalid_setting`, never a panic (a tokio
// semaphore panics above its maximum permit count) or a silently unbounded limit.
#[test]
fn concurrency_counts_reject_huge_values_with_invalid_setting() {
    let over_semaphore_max = (tokio::sync::Semaphore::MAX_PERMITS as u64) + 1;
    for field in ["plugin_max_in_flight", "max_request_tasks"] {
        for huge in [json!(over_semaphore_max), json!(u64::MAX), json!(i64::MAX)] {
            let msg = load_error(with(field, huge.clone()));
            assert!(msg.contains(REASON), "{field} = {huge}: {msg}");
            assert!(msg.contains(field), "{field} = {huge}: {msg}");
        }
    }
}

// DESIGN §4.7: `max_envelope_text_length` is at most `REGISTRY_TEXT_MAX_BYTES` (§3.7).
#[test]
fn envelope_length_accepts_registry_bound() {
    let cfg = valid_config(with(
        "max_envelope_text_length",
        json!(REGISTRY_TEXT_MAX_BYTES),
    ));
    assert_eq!(cfg.max_envelope_text_length, REGISTRY_TEXT_MAX_BYTES);
}

#[test]
fn envelope_length_rejects_above_registry_bound() {
    let msg = validation_error(with(
        "max_envelope_text_length",
        json!(REGISTRY_TEXT_MAX_BYTES + 1),
    ));
    assert!(msg.contains(REASON), "{msg}");
    assert!(msg.contains("max_envelope_text_length"), "{msg}");
}

// --- Request time budget (DESIGN §4.7, §4.6) --------------------------------------
//
// budget = pdp + 2 x registry + 4 x plugin (F-001: the 4th plugin limit is the
// whole-manifest validation step, which runs under the same limit), which must not exceed
// `GATEWAY_REQUEST_DEADLINE`. Each term is isolated by setting the other two to
// `MIN_TERM` and solving for the term under test.

fn pdp_limit() -> Duration {
    GATEWAY_REQUEST_DEADLINE - 2 * MIN_TERM - 4 * MIN_TERM
}

fn registry_limit() -> Duration {
    (GATEWAY_REQUEST_DEADLINE - MIN_TERM - 4 * MIN_TERM) / 2
}

fn plugin_limit() -> Duration {
    (GATEWAY_REQUEST_DEADLINE - MIN_TERM - 2 * MIN_TERM) / 4
}

#[test]
fn budget_accepts_exactly_the_gateway_deadline() {
    // 6s + 2 x 3s + 4 x 4.5s == 30s.
    let (pdp, registry, plugin) = (
        Duration::from_secs(6),
        Duration::from_secs(3),
        Duration::from_millis(4500),
    );
    assert_eq!(pdp + 2 * registry + 4 * plugin, GATEWAY_REQUEST_DEADLINE);
    assert!(budget_ok(pdp, registry, plugin), "exactly the deadline");
}

// DESIGN §4.7 and §4.6: one millisecond above the deadline fails `init` with
// `invalid_setting` and names the three settings.
#[test]
fn budget_rejects_one_millisecond_above_the_deadline_naming_all_three_settings() {
    let msg = validation_error(budget_json(
        Duration::from_millis(6001),
        Duration::from_secs(3),
        Duration::from_millis(4500),
    ));
    assert!(msg.contains(REASON), "{msg}");
    for field in DURATION_SETTINGS {
        assert!(msg.contains(field), "{field}: {msg}");
    }
}

// The pdp term has weight 1.
#[test]
fn budget_pdp_term_is_bounded_alone() {
    let (registry, plugin) = (MIN_TERM, MIN_TERM);
    assert!(budget_ok(pdp_limit(), registry, plugin), "pdp at its limit");
    assert!(
        !budget_ok(pdp_limit() + MIN_TERM, registry, plugin),
        "pdp 1ms above its limit"
    );
}

// The registry term has weight 2 (limit 15s less the other terms' share).
#[test]
fn budget_registry_term_is_bounded_alone() {
    let (pdp, plugin) = (MIN_TERM, MIN_TERM);
    assert!(
        budget_ok(pdp, registry_limit(), plugin),
        "registry at its limit"
    );
    assert!(
        !budget_ok(pdp, registry_limit() + MIN_TERM, plugin),
        "registry 1ms above its limit"
    );
}

// The plugin term has weight 4 (limit 7.5s less the other terms' share).
#[test]
fn budget_plugin_term_is_bounded_alone() {
    let (pdp, registry) = (MIN_TERM, MIN_TERM);
    assert!(
        budget_ok(pdp, registry, plugin_limit()),
        "plugin at its limit"
    );
    assert!(
        !budget_ok(pdp, registry, plugin_limit() + MIN_TERM),
        "plugin 1ms above its limit"
    );
}

// Durations near `u64::MAX` seconds must end as `invalid_setting` (checked
// arithmetic), never as an arithmetic panic at `init`.
#[test]
fn budget_rejects_near_max_durations_with_invalid_setting() {
    for field in DURATION_SETTINGS {
        let msg = validation_error(with(field, json!(HUGE_DURATION)));
        assert!(msg.contains(REASON), "{field}: {msg}");
    }
    let mut all = valid_json();
    for field in DURATION_SETTINGS {
        all[field] = json!(HUGE_DURATION);
    }
    assert!(validation_error(all).contains(REASON));
}

proptest! {
    // The validator agrees with a `u128` reference over durations on both sides of
    // the deadline.
    #[test]
    fn budget_validate_agrees_with_u128_reference(
        pdp in 1u64..40_000,
        registry in 1u64..20_000,
        plugin in 1u64..15_000,
    ) {
        let reference =
            u128::from(pdp) + 2 * u128::from(registry) + 4 * u128::from(plugin)
                <= GATEWAY_REQUEST_DEADLINE.as_millis();
        let actual = budget_ok(
            Duration::from_millis(pdp),
            Duration::from_millis(registry),
            Duration::from_millis(plugin),
        );
        prop_assert_eq!(actual, reference);
    }
}
