#![allow(clippy::unwrap_used, clippy::expect_used)]

use application_manager_sdk::{
    Dependency, ReportApplication, SectionOutcome, SectionOutcomeEntry, UnavailableReasonCode,
};
use serde_json::{Value, json};
use time::macros::datetime;

use super::*;
use crate::test_support::ctx;

const RECEIVED: OffsetDateTime = datetime!(2026-10-22 10:00:00 UTC);
const RECEIVED_TEXT: &str = "2026-10-22T10:00:00Z";

/// SHA-256 of `abc` (FIPS 180-4 test vector).
const ABC_SHA256: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
/// SHA-256 of the empty input.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn caller() -> CallerIdentity {
    CallerIdentity::from_security_context(&ctx())
}

fn caller_json() -> Value {
    serde_json::to_value(caller()).unwrap()
}

fn keys(content: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = content
        .as_object()
        .expect("content is an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    keys
}

fn within(id: &str, version: Option<&str>) -> ApplicationRef {
    ApplicationRef::Within {
        id: id.to_owned(),
        version: version.map(str::to_owned),
    }
}

fn section_keys() -> SectionKeys {
    SectionKeys {
        keys: vec!["billing".to_owned(), "crm".to_owned()],
        other_keys: 3,
    }
}

fn problem(rule: &str, location: &str, message: &str) -> ReportProblem {
    ReportProblem {
        location: location.to_owned(),
        rule: rule.to_owned(),
        message: message.to_owned(),
    }
}

fn report(action: OperationAction) -> OutcomeReport {
    OutcomeReport {
        operation_id: Uuid::from_u128(1),
        action,
        category: Some(RequestCategory::Succeeded),
        interrupted: false,
        application: ReportApplication {
            id: "acme.app".to_owned(),
            version: (action == OperationAction::Seed).then(|| "1.0.0".to_owned()),
        },
        sections: vec![],
        problems: vec![],
        unavailable: vec![],
    }
}

fn entry(key: &str, outcome: SectionOutcome, version: Option<&str>) -> SectionOutcomeEntry {
    SectionOutcomeEntry {
        key: key.to_owned(),
        outcome,
        schema_version: version.map(str::to_owned),
        detail: None,
        location: None,
    }
}

// ---- ApplicationRef ----

#[test]
fn bounded_within_the_limit_keeps_id_and_version() {
    assert_eq!(
        ApplicationRef::bounded("abc", Some("1.0"), 3),
        within("abc", Some("1.0"))
    );
    assert_eq!(ApplicationRef::bounded("abc", None, 3), within("abc", None));
}

#[test]
fn bounded_counts_bytes_not_characters() {
    // `\u{e9}` is two bytes: 2 characters, 4 bytes.
    assert_eq!(
        ApplicationRef::bounded("\u{e9}\u{e9}", None, 4),
        within("\u{e9}\u{e9}", None)
    );
    let ApplicationRef::Digest { id_bytes, .. } = ApplicationRef::bounded("\u{e9}\u{e9}", None, 3)
    else {
        panic!("4 bytes exceed a limit of 3");
    };
    assert_eq!(id_bytes, 4);
}

#[test]
fn bounded_long_id_becomes_byte_length_and_sha256() {
    assert_eq!(
        ApplicationRef::bounded("abc", None, 2),
        ApplicationRef::Digest {
            id_bytes: 3,
            id_sha256: ABC_SHA256.to_owned(),
            version_bytes: None,
            version_sha256: None,
        }
    );
}

#[test]
fn bounded_long_version_becomes_a_digest_of_both() {
    assert_eq!(
        ApplicationRef::bounded("", Some("abc"), 2),
        ApplicationRef::Digest {
            id_bytes: 0,
            id_sha256: EMPTY_SHA256.to_owned(),
            version_bytes: Some(3),
            version_sha256: Some(ABC_SHA256.to_owned()),
        }
    );
}

#[test]
fn bounded_digest_is_lower_case_hex_of_64_digits() {
    let ApplicationRef::Digest { id_sha256, .. } =
        ApplicationRef::bounded(&"x".repeat(300), None, 255)
    else {
        panic!("300 bytes exceed 255");
    };
    assert_eq!(id_sha256.len(), 64);
    assert!(
        id_sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
}

// ---- accepted ----

#[test]
fn accepted_seed_emits_exactly_the_design_fields() {
    let content = accepted(
        &within("acme.app", Some("1.0.0")),
        Some(&section_keys()),
        &caller(),
        RECEIVED,
    );
    assert_eq!(
        keys(&content),
        ["application", "caller", "received_at", "section_keys"]
    );
    assert_eq!(
        content["application"],
        json!({ "id": "acme.app", "version": "1.0.0" })
    );
    assert_eq!(
        content["section_keys"],
        json!({ "keys": ["billing", "crm"], "other_keys": 3 })
    );
    assert_eq!(content["caller"], caller_json());
    assert_eq!(content["received_at"], json!(RECEIVED_TEXT));
}

#[test]
fn accepted_uninstall_has_the_id_and_no_section_keys() {
    let content = accepted(&within("acme.app", None), None, &caller(), RECEIVED);
    assert_eq!(keys(&content), ["application", "caller", "received_at"]);
    assert_eq!(content["application"], json!({ "id": "acme.app" }));
}

#[test]
fn accepted_has_no_result() {
    let content = accepted(
        &within("a", Some("1")),
        Some(&section_keys()),
        &caller(),
        RECEIVED,
    );
    assert!(content.get("result").is_none());
}

#[test]
fn caller_content_has_no_token() {
    let with_token = SecurityContextWithToken::build();
    let identity = CallerIdentity::from_security_context(&with_token);
    let content = accepted(&within("a", Some("1")), None, &identity, RECEIVED);
    assert!(!content.to_string().contains("secret-token"));
}

struct SecurityContextWithToken;

impl SecurityContextWithToken {
    fn build() -> toolkit_security::SecurityContext {
        toolkit_security::SecurityContext::builder()
            .subject_id(Uuid::from_u128(1))
            .subject_tenant_id(Uuid::from_u128(2))
            .bearer_token("secret-token".to_owned())
            .build()
            .unwrap()
    }
}

// ---- finished ----

#[test]
fn finished_succeeded_emits_exactly_the_design_fields() {
    let mut done = report(OperationAction::Seed);
    done.sections = vec![
        entry("billing", SectionOutcome::Succeeded, Some("3")),
        entry("crm", SectionOutcome::Succeeded, Some("7")),
    ];
    let content = finished(&done);
    assert_eq!(keys(&content), ["application", "result"]);
    assert_eq!(
        content["application"],
        json!({ "id": "acme.app", "version": "1.0.0" })
    );
    assert_eq!(
        keys(&content["result"]),
        [
            "category",
            "interrupted",
            "problem_count",
            "rules",
            "sections",
            "unavailable"
        ]
    );
    assert_eq!(content["result"]["category"], json!("succeeded"));
    assert_eq!(content["result"]["interrupted"], json!(false));
    assert_eq!(content["result"]["problem_count"], json!(0));
    assert_eq!(content["result"]["rules"], json!([]));
    assert_eq!(
        content["result"]["sections"],
        json!([
            { "key": "billing", "outcome": "succeeded", "schema_version": "3" },
            { "key": "crm", "outcome": "succeeded", "schema_version": "7" },
        ])
    );
    assert_eq!(content["result"]["unavailable"], json!([]));
}

#[test]
fn finished_uninstall_application_has_only_the_id() {
    let content = finished(&report(OperationAction::Uninstall));
    assert_eq!(content["application"], json!({ "id": "acme.app" }));
}

#[test]
fn finished_interrupted_carries_the_marker_and_no_category() {
    let mut cut = report(OperationAction::Seed);
    cut.category = None;
    cut.interrupted = true;
    cut.sections = vec![entry("billing", SectionOutcome::Succeeded, Some("3"))];
    let content = finished(&cut);
    assert_eq!(content["result"]["interrupted"], json!(true));
    assert!(content["result"].get("category").is_none(), "{content}");
    assert_eq!(content["result"]["sections"].as_array().unwrap().len(), 1);
}

#[test]
fn finished_records_problem_count_and_distinct_sorted_rule_tokens() {
    let mut invalid = report(OperationAction::Seed);
    invalid.category = Some(RequestCategory::InvalidManifest);
    invalid.problems = vec![
        problem("type", "/sections/a/x", "x must be a number"),
        problem("required", "/sections/a", "y is required"),
        problem("type", "/sections/b/z", "z must be a string"),
        problem("missing_section", "/sections/c", "section c is missing"),
    ];
    let content = finished(&invalid);
    assert_eq!(content["result"]["problem_count"], json!(4));
    assert_eq!(
        content["result"]["rules"],
        json!(["missing_section", "required", "type"])
    );
}

#[test]
fn finished_records_unavailable_items() {
    let mut unavailable = report(OperationAction::Seed);
    unavailable.category = Some(RequestCategory::TemporarilyUnavailable);
    unavailable.unavailable = vec![UnavailableItem {
        dependency: Dependency::SectionSchema,
        key: Some("billing".to_owned()),
        reason: UnavailableReasonCode::TimedOut,
    }];
    let content = finished(&unavailable);
    assert_eq!(
        content["result"]["unavailable"],
        json!([{ "dependency": "section_schema", "key": "billing", "reason": "timed_out" }])
    );
}

#[test]
fn finished_section_entry_has_only_key_outcome_and_schema_version() {
    let mut rejected = report(OperationAction::Seed);
    rejected.category = Some(RequestCategory::PluginRejected);
    rejected.sections = vec![SectionOutcomeEntry {
        key: "billing".to_owned(),
        outcome: SectionOutcome::Rejected,
        schema_version: Some("3".to_owned()),
        detail: Some("plan too small".to_owned()),
        location: Some("/plan".to_owned()),
    }];
    let content = finished(&rejected);
    assert_eq!(
        content["result"]["sections"],
        json!([{ "key": "billing", "outcome": "rejected", "schema_version": "3" }])
    );
}

// ---- refused ----

fn refusal(category: RequestCategory) -> RefusedResult {
    RefusedResult {
        category,
        problems: vec![],
        busy_operation_id: None,
        unavailable: vec![],
    }
}

#[test]
fn refused_seed_emits_exactly_the_design_fields() {
    let content = refused(
        &within("acme.app", Some("1.0.0")),
        Some(&section_keys()),
        &caller(),
        RECEIVED,
        &refusal(RequestCategory::NotPermitted),
    );
    assert_eq!(
        keys(&content),
        [
            "application",
            "caller",
            "received_at",
            "result",
            "section_keys"
        ]
    );
    assert_eq!(
        content["application"],
        json!({ "id": "acme.app", "version": "1.0.0" })
    );
    assert_eq!(content["caller"], caller_json());
    assert_eq!(content["received_at"], json!(RECEIVED_TEXT));
    assert_eq!(
        keys(&content["result"]),
        ["category", "problem_count", "rules"]
    );
    assert_eq!(content["result"]["category"], json!("not_permitted"));
}

#[test]
fn refused_without_an_object_of_sections_has_no_section_keys() {
    let content = refused(
        &within("acme.app", None),
        None,
        &caller(),
        RECEIVED,
        &refusal(RequestCategory::InvalidManifest),
    );
    assert!(content.get("section_keys").is_none());
}

#[test]
fn refused_conflict_names_the_busy_operation() {
    let mut conflict = refusal(RequestCategory::Conflict);
    conflict.busy_operation_id = Some(Uuid::from_u128(0xB0B));
    let content = refused(&within("a", None), None, &caller(), RECEIVED, &conflict);
    assert_eq!(
        content["result"]["busy_operation_id"],
        json!(Uuid::from_u128(0xB0B).to_string())
    );
}

#[test]
fn refused_unavailable_lists_its_items() {
    let mut unavailable = refusal(RequestCategory::TemporarilyUnavailable);
    unavailable.unavailable = vec![UnavailableItem {
        dependency: Dependency::OperationStorage,
        key: None,
        reason: UnavailableReasonCode::TimedOut,
    }];
    let content = refused(&within("a", None), None, &caller(), RECEIVED, &unavailable);
    assert_eq!(
        content["result"]["unavailable"],
        json!([{ "dependency": "operation_storage", "reason": "timed_out" }])
    );
}

#[test]
fn refused_invalid_manifest_records_problem_count_and_rules() {
    let mut invalid = refusal(RequestCategory::InvalidManifest);
    invalid.problems = vec![
        problem("minLength", "/application/id", "id is empty"),
        problem("minLength", "/application/version", "version is empty"),
    ];
    let content = refused(&within("", Some("")), None, &caller(), RECEIVED, &invalid);
    assert_eq!(content["result"]["problem_count"], json!(2));
    assert_eq!(content["result"]["rules"], json!(["minLength"]));
}

#[test]
fn refused_out_of_limit_application_is_length_and_digest() {
    let application = ApplicationRef::bounded("abc", None, 2);
    let content = refused(
        &application,
        None,
        &caller(),
        RECEIVED,
        &refusal(RequestCategory::InvalidManifest),
    );
    assert_eq!(
        content["application"],
        json!({ "id_bytes": 3, "id_sha256": ABC_SHA256 })
    );
    assert!(!content.to_string().contains("\"abc\""), "{content}");
}

#[test]
fn refused_out_of_limit_version_adds_version_length_and_digest() {
    let application = ApplicationRef::bounded("abc", Some("abc"), 2);
    let content = refused(
        &application,
        Some(&section_keys()),
        &caller(),
        RECEIVED,
        &refusal(RequestCategory::InvalidManifest),
    );
    assert_eq!(
        content["application"],
        json!({
            "id_bytes": 3,
            "id_sha256": ABC_SHA256,
            "version_bytes": 3,
            "version_sha256": ABC_SHA256,
        })
    );
}

// ---- no manifest content ----

const SENTINEL: &str = "SENTINEL-7f3a91";

#[test]
fn finished_content_has_no_problem_location_message_or_plugin_detail() {
    let mut leaky = report(OperationAction::Seed);
    leaky.category = Some(RequestCategory::PluginRejected);
    leaky.sections = vec![SectionOutcomeEntry {
        key: "billing".to_owned(),
        outcome: SectionOutcome::Rejected,
        schema_version: Some("3".to_owned()),
        detail: Some(format!("detail {SENTINEL}")),
        location: Some(format!("/loc/{SENTINEL}")),
    }];
    leaky.problems = vec![problem(
        "type",
        &format!("/sections/{SENTINEL}"),
        &format!("message {SENTINEL}"),
    )];
    let text = finished(&leaky).to_string();
    assert!(!text.contains(SENTINEL), "{text}");
}

#[test]
fn refused_content_has_no_problem_location_or_message() {
    let mut invalid = refusal(RequestCategory::InvalidManifest);
    invalid.problems = vec![problem(
        "type",
        &format!("/sections/{SENTINEL}"),
        &format!("message {SENTINEL}"),
    )];
    let text = refused(
        &within("a", Some("1")),
        Some(&section_keys()),
        &caller(),
        RECEIVED,
        &invalid,
    )
    .to_string();
    assert!(!text.contains(SENTINEL), "{text}");
}

#[test]
fn section_keys_carry_the_key_set_and_a_count_only() {
    // A manifest key outside the key set is counted, never named.
    let content = accepted(
        &within("a", Some("1")),
        Some(&SectionKeys {
            keys: vec!["billing".to_owned()],
            other_keys: 1,
        }),
        &caller(),
        RECEIVED,
    );
    assert_eq!(
        content["section_keys"],
        json!({ "keys": ["billing"], "other_keys": 1 })
    );
}
