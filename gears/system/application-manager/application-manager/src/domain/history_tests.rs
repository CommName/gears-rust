#![allow(clippy::unwrap_used, clippy::expect_used)]

use application_manager_sdk::{
    Dependency, ReportApplication, SectionOutcome, SectionOutcomeEntry, UnavailableReasonCode,
};
use serde_json::{Value, json};
use time::macros::datetime;

use super::*;
use crate::domain::operation::CompletionReason;
use crate::test_support::platform_ctx;

const RECEIVED: OffsetDateTime = datetime!(2026-10-22 10:00:00 UTC);
const RECEIVED_TEXT: &str = "2026-10-22T10:00:00Z";

/// SHA-256 of `abc` (FIPS 180-4 test vector).
const ABC_SHA256: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

fn caller() -> CallerIdentity {
    CallerIdentity::from_platform_context(&platform_ctx("app-seeder"))
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
    ApplicationRef {
        id: BoundedText::Within(id.to_owned()),
        version: version.map(|v| BoundedText::Within(v.to_owned())),
    }
}

fn digest(bytes: usize, sha256: &str) -> BoundedText {
    BoundedText::Digest {
        bytes,
        sha256: sha256.to_owned(),
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
        application: ReportApplication {
            id: "acme.app".to_owned(),
            version: (action == OperationAction::Seed).then(|| "1.0.0".to_owned()),
        },
        sections: vec![],
        problems: vec![],
        unavailable: vec![],
    }
}

fn entry(key: &str, outcome: SectionOutcome, section_type: Option<&str>) -> SectionOutcomeEntry {
    SectionOutcomeEntry {
        key: key.to_owned(),
        section_type: section_type.map(str::to_owned),
        outcome: Some(outcome),
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
    assert_eq!(
        ApplicationRef::bounded("\u{e9}\u{e9}", None, 3).id,
        BoundedText::of("\u{e9}\u{e9}", 3)
    );
    let BoundedText::Digest { bytes, .. } = BoundedText::of("\u{e9}\u{e9}", 3) else {
        panic!("4 bytes exceed a limit of 3");
    };
    assert_eq!(bytes, 4);
}

#[test]
fn bounded_long_id_becomes_byte_length_and_sha256() {
    assert_eq!(
        ApplicationRef::bounded("abc", None, 2),
        ApplicationRef {
            id: digest(3, ABC_SHA256),
            version: None,
        }
    );
}

#[test]
fn bounded_digests_each_field_on_its_own() {
    assert_eq!(
        ApplicationRef::bounded("", Some("abc"), 2),
        ApplicationRef {
            id: BoundedText::Within(String::new()),
            version: Some(digest(3, ABC_SHA256)),
        }
    );
    assert_eq!(
        ApplicationRef::bounded("abc", Some(""), 2),
        ApplicationRef {
            id: digest(3, ABC_SHA256),
            version: Some(BoundedText::Within(String::new())),
        }
    );
    assert_eq!(BoundedText::of("", 0), BoundedText::Within(String::new()));
    assert_eq!(BoundedText::of("x", 0), digest(1, &sha_of("x")));
}

fn sha_of(text: &str) -> String {
    let BoundedText::Digest { sha256, .. } = BoundedText::of(text, 0) else {
        panic!("a non-empty text exceeds a limit of 0");
    };
    sha256
}

#[test]
fn bounded_digest_is_lower_case_hex_of_64_digits() {
    let sha256 = sha_of(&"x".repeat(300));
    assert_eq!(sha256.len(), 64);
    assert!(
        sha256
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
fn caller_content_is_the_platform_identity() {
    let content = accepted(&within("a", Some("1")), None, &caller(), RECEIVED);
    assert_eq!(
        content["caller"],
        serde_json::to_value(platform_ctx("app-seeder").identity()).unwrap()
    );
}

// ---- finished ----

#[test]
fn finished_succeeded_emits_exactly_the_design_fields() {
    let mut done = report(OperationAction::Seed);
    done.sections = vec![
        entry(
            "billing",
            SectionOutcome::Succeeded,
            Some("gts.t.billing.v1~"),
        ),
        entry("crm", SectionOutcome::Succeeded, Some("gts.t.crm.v1~")),
    ];
    let content = finished(&done, None);
    assert_eq!(keys(&content), ["application", "result"]);
    assert_eq!(
        content["application"],
        json!({ "id": "acme.app", "version": "1.0.0" })
    );
    assert_eq!(
        keys(&content["result"]),
        [
            "category",
            "problem_count",
            "rules",
            "sections",
            "unavailable"
        ]
    );
    assert_eq!(content["result"]["category"], json!("succeeded"));
    assert_eq!(content["result"]["problem_count"], json!(0));
    assert_eq!(content["result"]["rules"], json!([]));
    assert_eq!(
        content["result"]["sections"],
        json!([
            { "key": "billing", "section_type": "gts.t.billing.v1~", "outcome": "succeeded" },
            { "key": "crm", "section_type": "gts.t.crm.v1~", "outcome": "succeeded" },
        ])
    );
    assert_eq!(content["result"]["unavailable"], json!([]));
}

#[test]
fn finished_uninstall_application_has_only_the_id() {
    let content = finished(&report(OperationAction::Uninstall), None);
    assert_eq!(content["application"], json!({ "id": "acme.app" }));
}

#[test]
fn finished_expiry_completion_carries_the_reason_expired() {
    let mut expired = report(OperationAction::Seed);
    expired.category = Some(RequestCategory::InternalFailure);
    expired.sections = vec![
        entry(
            "billing",
            SectionOutcome::Succeeded,
            Some("gts.t.billing.v1~"),
        ),
        entry("crm", SectionOutcome::Lost, Some("gts.t.crm.v1~")),
    ];
    let content = finished(&expired, Some(CompletionReason::Expired));
    assert_eq!(content["result"]["reason"], json!("expired"));
    assert_eq!(content["result"]["category"], json!("internal_failure"));
    assert_eq!(content["result"]["sections"][1]["outcome"], json!("lost"));
    assert!(
        finished(&expired, None)["result"].get("reason").is_none(),
        "only an expiry completion has a reason"
    );
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
    let content = finished(&invalid, None);
    assert_eq!(content["result"]["problem_count"], json!(4));
    assert_eq!(
        content["result"]["rules"],
        json!(["missing_section", "required", "type"])
    );
}

#[test]
fn finished_records_unavailable_items() {
    let mut unavailable = report(OperationAction::Seed);
    unavailable.category = Some(RequestCategory::InternalFailure);
    unavailable.unavailable = vec![UnavailableItem {
        dependency: Dependency::OperationStorage,
        reason: UnavailableReasonCode::TimedOut,
    }];
    let content = finished(&unavailable, None);
    assert_eq!(
        content["result"]["unavailable"],
        json!([{ "dependency": "operation_storage", "reason": "timed_out" }])
    );
}

#[test]
fn finished_section_entry_has_only_key_section_type_and_outcome() {
    let mut rejected = report(OperationAction::Seed);
    rejected.category = Some(RequestCategory::PluginRejected);
    rejected.sections = vec![SectionOutcomeEntry {
        key: "billing".to_owned(),
        section_type: Some("gts.t.billing.v1~".to_owned()),
        outcome: Some(SectionOutcome::Rejected),
        detail: Some("plan too small".to_owned()),
        location: Some("/plan".to_owned()),
    }];
    let content = finished(&rejected, None);
    assert_eq!(
        content["result"]["sections"],
        json!([{ "key": "billing", "section_type": "gts.t.billing.v1~", "outcome": "rejected" }])
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
fn refused_out_of_limit_version_is_digested_on_its_own() {
    let application = ApplicationRef::bounded("ab", Some("abc"), 2);
    let content = refused(
        &application,
        None,
        &caller(),
        RECEIVED,
        &refusal(RequestCategory::InvalidManifest),
    );
    assert_eq!(
        content["application"],
        json!({ "id": "ab", "version_bytes": 3, "version_sha256": ABC_SHA256 })
    );
}

#[test]
fn refused_out_of_limit_id_and_version_are_both_digested() {
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
        section_type: Some("gts.t.billing.v1~".to_owned()),
        outcome: Some(SectionOutcome::Rejected),
        detail: Some(format!("detail {SENTINEL}")),
        location: Some(format!("/loc/{SENTINEL}")),
    }];
    leaky.problems = vec![problem(
        "type",
        &format!("/sections/{SENTINEL}"),
        &format!("message {SENTINEL}"),
    )];
    let text = finished(&leaky, None).to_string();
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
