//! Serde contract tests for the SDK models (DESIGN §3.1, §3.3, §4.8).
//!
//! Planned API: the model types of `crate::models`, plus
//! `CallerIdentity::{from_security_context, to_security_context}`.
//!
//! Wire decisions pinned here:
//! - The hand-off `manifest` is the parsed JSON value, absent when the seed body is
//!   invalid (did not parse). There are no raw bytes and no format flag.
//! - Optional members are omitted when absent, never `null`. The one exception is
//!   `manifest`, where a body that is the JSON `null` is a value and stays `null`.
//! - A final result where validation did not run has no `validation` member; "not run"
//!   is distinguishable from "valid".
//! - `ApplicationEnvelope` is tolerant of extra members. Rejecting them is the gear
//!   validator's rule (P5, rule `additionalProperties`), not the SDK type's.
//! - `CallerIdentity` is strict and carries no bearer token. It is passed to the audit
//!   plugin only as an argument and never appears in the hand-off content.
//! - `request_id` in the hand-off content (and `audit_id` in the final-result content)
//!   must equal the argument values. That is a gear-side rule.

use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use toolkit_security::SecurityContext;
use uuid::Uuid;

use crate::models::{
    ApplicationEnvelope, CallerIdentity, Dependency, FinalResultContent,
    HANDOFF_MANIFEST_MAX_DEPTH, HandoffOperation, OutcomeCategory, OutcomeReport, RegistryEffect,
    RequestHandoffContent, SectionOutcome, UnavailableReason,
};

const REQUEST_ID: &str = "00000000-0000-7000-8000-000000000003";
/// SHA-256 of the 3 bytes `x`, NUL, `y` (a never-storable uninstall id).
const NEVER_STORABLE_ID_SHA256: &str =
    "sha256:ce3890a816f5237a17aa7e1436113bbac398dfe216cf965537cd035bdbad900a";
/// SHA-256 of the schema text `{"type":"object"}`.
const FINGERPRINT_OBJECT: &str =
    "sha256:a2c799262a3ce3c19ef5cdd983bf3d12b43ab3c426227091b909dcb7054738c0";
/// SHA-256 of the schema text `{"type":"string"}`.
const FINGERPRINT_STRING: &str =
    "sha256:00404e686415370f1711c4d7acfa2905444d3cf23cef2e10c47d445ebe690f96";

/// Deserialize `value`, serialize it back, and require the exact same JSON.
fn assert_lossless<T: Serialize + DeserializeOwned>(value: &Value) {
    let parsed: T = serde_json::from_value(value.clone()).expect("deserialize");
    let back = serde_json::to_value(&parsed).expect("serialize");
    assert_eq!(&back, value, "round-trip must be lossless");
}

fn assert_rejected<T: DeserializeOwned>(value: Value, why: &str) {
    assert!(
        serde_json::from_value::<T>(value).is_err(),
        "must be rejected: {why}"
    );
}

/// Check every variant's wire spelling in both directions, and that wrong casing and
/// unknown values are rejected. `spelling` must be an exhaustive `match`, so a new
/// variant fails to compile until it is listed.
fn assert_spellings<T>(all: &[T], spelling: fn(&T) -> &'static str)
where
    T: Serialize + DeserializeOwned + PartialEq + Debug,
{
    for variant in all {
        let text = spelling(variant);
        assert_eq!(
            serde_json::to_value(variant).expect("ser"),
            json!(text),
            "serialize {variant:?}"
        );
        assert_eq!(
            &serde_json::from_value::<T>(json!(text)).expect("de"),
            variant,
            "deserialize {text:?}"
        );
        let mut chars = text.chars();
        let first = chars.next().expect("non-empty spelling");
        let capitalized = format!("{}{}", first.to_uppercase(), chars.as_str());
        assert_rejected::<T>(json!(capitalized), "wrong casing");
        assert_rejected::<T>(json!(text.to_uppercase()), "upper case");
    }
    assert_rejected::<T>(json!("bogus"), "unknown value");
    assert_rejected::<T>(json!(""), "empty value");
    assert_rejected::<T>(json!(1), "non-string value");
}

fn envelope_json() -> Value {
    json!({"id": "app-1", "version": "1.2.3"})
}

// --- ApplicationEnvelope (DESIGN §3.1 entity-manifest) ----------------------------

#[test]
fn envelope_round_trips_with_exact_field_names() {
    assert_lossless::<ApplicationEnvelope>(&envelope_json());
}

// DESIGN §3.1, §3.3: the SDK type is tolerant so v1 can evolve additively. The gear
// validator rejects extra members (P5, rule `additionalProperties`).
#[test]
fn envelope_accepts_extra_member_and_drops_it_on_serialize() {
    let parsed: ApplicationEnvelope =
        serde_json::from_value(json!({"id": "a", "version": "1", "extra": true}))
            .expect("extra member must deserialize");
    assert_eq!(
        serde_json::to_value(&parsed).expect("ser"),
        json!({"id": "a", "version": "1"}),
        "the extra member is not re-serialized"
    );
}

#[test]
fn envelope_rejects_missing_or_mistyped_members() {
    assert_rejected::<ApplicationEnvelope>(json!({"version": "1"}), "missing id");
    assert_rejected::<ApplicationEnvelope>(json!({"id": "a"}), "missing version");
    assert_rejected::<ApplicationEnvelope>(json!({"id": 1, "version": "1"}), "numeric id");
    assert_rejected::<ApplicationEnvelope>(json!({"id": "a", "version": null}), "null version");
    assert_rejected::<ApplicationEnvelope>(json!({"id": null, "version": "1"}), "null id");
    assert_rejected::<ApplicationEnvelope>(json!({}), "empty object");
}

// --- Enum spellings (DESIGN §3.1) -------------------------------------------------

fn section_outcome_spelling(v: &SectionOutcome) -> &'static str {
    match v {
        SectionOutcome::Succeeded => "succeeded",
        SectionOutcome::Rejected => "rejected",
        SectionOutcome::NotPermitted => "not_permitted",
        SectionOutcome::InternalFailure => "internal_failure",
        SectionOutcome::TimedOut => "timed_out",
        SectionOutcome::NotSent => "not_sent",
        SectionOutcome::NotDispatched => "not_dispatched",
    }
}

fn outcome_category_spelling(v: &OutcomeCategory) -> &'static str {
    match v {
        OutcomeCategory::Succeeded => "succeeded",
        OutcomeCategory::PluginRejected => "plugin_rejected",
        OutcomeCategory::NotPermitted => "not_permitted",
        OutcomeCategory::NotFound => "not_found",
        OutcomeCategory::InvalidManifest => "invalid_manifest",
        OutcomeCategory::InternalFailure => "internal_failure",
        OutcomeCategory::TemporarilyUnavailable => "temporarily_unavailable",
    }
}

fn registry_effect_spelling(v: &RegistryEffect) -> &'static str {
    match v {
        RegistryEffect::Updated => "updated",
        RegistryEffect::NotUpdated => "not_updated",
        RegistryEffect::Unchanged => "unchanged",
        RegistryEffect::Deleted => "deleted",
        RegistryEffect::DeleteFailed => "delete_failed",
    }
}

fn dependency_spelling(v: &Dependency) -> &'static str {
    match v {
        Dependency::AccessControl => "access_control",
        Dependency::RegistryStorage => "registry_storage",
        Dependency::AuditPlugin => "audit_plugin",
        Dependency::SectionSchema => "section_schema",
    }
}

fn unavailable_reason_spelling(v: &UnavailableReason) -> &'static str {
    match v {
        UnavailableReason::Unreachable => "unreachable",
        UnavailableReason::TimedOut => "timed_out",
        UnavailableReason::Refused => "refused",
        UnavailableReason::Unusable => "unusable",
        UnavailableReason::NotSent => "not_sent",
        UnavailableReason::NotReady => "not_ready",
        UnavailableReason::AtCapacity => "at_capacity",
    }
}

fn handoff_operation_spelling(v: &HandoffOperation) -> &'static str {
    match v {
        HandoffOperation::Seed => "seed",
        HandoffOperation::Uninstall => "uninstall",
    }
}

// DESIGN §3.1 entity-outcome-category: the seven section outcome spellings.
#[test]
fn section_outcome_round_trips_snake_case_spellings() {
    assert_spellings(
        &[
            SectionOutcome::Succeeded,
            SectionOutcome::Rejected,
            SectionOutcome::NotPermitted,
            SectionOutcome::InternalFailure,
            SectionOutcome::TimedOut,
            SectionOutcome::NotSent,
            SectionOutcome::NotDispatched,
        ],
        section_outcome_spelling,
    );
}

// DESIGN §3.1 entity-outcome-category: the seven request-level categories.
#[test]
fn outcome_category_round_trips_snake_case_spellings() {
    assert_spellings(
        &[
            OutcomeCategory::Succeeded,
            OutcomeCategory::PluginRejected,
            OutcomeCategory::NotPermitted,
            OutcomeCategory::NotFound,
            OutcomeCategory::InvalidManifest,
            OutcomeCategory::InternalFailure,
            OutcomeCategory::TemporarilyUnavailable,
        ],
        outcome_category_spelling,
    );
}

// DESIGN §3.1 entity-outcome-report: `registry_effect` values.
#[test]
fn registry_effect_round_trips_snake_case_spellings() {
    assert_spellings(
        &[
            RegistryEffect::Updated,
            RegistryEffect::NotUpdated,
            RegistryEffect::Unchanged,
            RegistryEffect::Deleted,
            RegistryEffect::DeleteFailed,
        ],
        registry_effect_spelling,
    );
}

// DESIGN §3.1 entity-outcome-report: `unavailable.dependency` values.
#[test]
fn dependency_round_trips_snake_case_spellings() {
    assert_spellings(
        &[
            Dependency::AccessControl,
            Dependency::RegistryStorage,
            Dependency::AuditPlugin,
            Dependency::SectionSchema,
        ],
        dependency_spelling,
    );
}

// DESIGN §3.1 entity-outcome-report: `unavailable.reason` values.
#[test]
fn unavailable_reason_round_trips_snake_case_spellings() {
    assert_spellings(
        &[
            UnavailableReason::Unreachable,
            UnavailableReason::TimedOut,
            UnavailableReason::Refused,
            UnavailableReason::Unusable,
            UnavailableReason::NotSent,
            UnavailableReason::NotReady,
            UnavailableReason::AtCapacity,
        ],
        unavailable_reason_spelling,
    );
}

// DESIGN §3.1 entity-audit-event-content: operation values of the request hand-off.
#[test]
fn handoff_operation_round_trips_snake_case_spellings() {
    assert_spellings(
        &[HandoffOperation::Seed, HandoffOperation::Uninstall],
        handoff_operation_spelling,
    );
}

// DESIGN §3.1 entity-audit-event-content: manifest format flag values.
// --- OutcomeReport (DESIGN §3.1 entity-outcome-report) ----------------------------
//
// One coherent fixture per outcome category (PRD categories, DESIGN §3.1).

#[test]
fn outcome_report_round_trips_succeeded_seed() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "succeeded",
        "application": {"id": "app-1", "version": "2"},
        "partial": false,
        "sections": [
            {"key": "alpha", "outcome": "succeeded"},
            {"key": "beta", "outcome": "succeeded"}
        ],
        "problems": [],
        "unavailable": [],
        "registry_effect": "updated",
        "audit_id": "audit-123"
    }));
}

#[test]
fn outcome_report_round_trips_plugin_rejected_partial_seed() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "plugin_rejected",
        "application": {"id": "app-1", "version": "2"},
        "partial": true,
        "sections": [
            {"key": "alpha", "outcome": "succeeded"},
            {"key": "beta", "outcome": "rejected", "detail": "bad", "location": "/x/y"}
        ],
        "problems": [],
        "unavailable": [],
        "registry_effect": "updated",
        "audit_id": "audit-123"
    }));
}

#[test]
fn outcome_report_round_trips_not_permitted_seed() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "not_permitted",
        "application": {"id": "app-1", "version": "2"},
        "partial": false,
        "sections": [
            {"key": "alpha", "outcome": "not_permitted", "detail": "no"},
            {"key": "beta", "outcome": "not_dispatched"}
        ],
        "problems": [],
        "unavailable": [],
        "registry_effect": "not_updated",
        "audit_id": "audit-123"
    }));
}

#[test]
fn outcome_report_round_trips_invalid_manifest_with_problems() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "invalid_manifest",
        "application": {"id": "app-1", "version": "2"},
        "partial": false,
        "sections": [],
        "problems": [
            {"location": "/sections/beta", "rule": "required", "message": "missing"}
        ],
        "unavailable": [],
        "registry_effect": "not_updated",
        "audit_id": "audit-123"
    }));
}

#[test]
fn outcome_report_round_trips_internal_failure_with_unknown_outcome() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "internal_failure",
        "application": {"id": "app-1", "version": "2"},
        "partial": true,
        "sections": [
            {"key": "alpha", "outcome": "succeeded"},
            {"key": "beta", "outcome": "timed_out"},
            {"key": "gamma", "outcome": "not_sent"}
        ],
        "problems": [],
        "unavailable": [],
        "registry_effect": "updated",
        "audit_id": "audit-123"
    }));
}

// The audit plugin refused the request hand-off: nothing was dispatched and no audit
// id exists.
#[test]
fn outcome_report_round_trips_unavailable_audit_plugin() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "temporarily_unavailable",
        "application": {"id": "app-1", "version": "2"},
        "partial": false,
        "sections": [],
        "problems": [],
        "unavailable": [{"dependency": "audit_plugin", "reason": "refused"}],
        "registry_effect": "not_updated"
    }));
}

// A section schema was unavailable after the request hand-off was accepted.
#[test]
fn outcome_report_round_trips_unavailable_section_schema() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "temporarily_unavailable",
        "application": {"id": "app-1", "version": "2"},
        "partial": false,
        "sections": [],
        "problems": [],
        "unavailable": [
            {"dependency": "section_schema", "key": "beta", "reason": "not_sent"},
            {"dependency": "section_schema", "key": "gamma", "reason": "timed_out"}
        ],
        "registry_effect": "not_updated",
        "audit_id": "audit-123"
    }));
}

#[test]
fn outcome_report_round_trips_not_found_uninstall_with_id_only() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "not_found",
        "application": {"id": "app-1"},
        "partial": false,
        "sections": [],
        "problems": [],
        "unavailable": [],
        "registry_effect": "unchanged",
        "audit_id": "audit-123"
    }));
}

#[test]
fn outcome_report_round_trips_succeeded_uninstall_with_last_version() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "succeeded",
        "application": {"id": "app-1", "version": "2"},
        "partial": false,
        "sections": [{"key": "alpha", "outcome": "succeeded"}],
        "problems": [],
        "unavailable": [],
        "registry_effect": "deleted",
        "audit_id": "audit-123"
    }));
}

#[test]
fn outcome_report_round_trips_uninstall_with_delete_failed() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "internal_failure",
        "application": {"id": "app-1", "version": "2"},
        "partial": true,
        "sections": [
            {"key": "alpha", "outcome": "succeeded"},
            {"key": "beta", "outcome": "internal_failure"}
        ],
        "problems": [],
        "unavailable": [],
        "registry_effect": "delete_failed",
        "audit_id": "audit-123"
    }));
}

// A never-storable uninstall id is omitted, so the report has no `application`.
#[test]
fn outcome_report_round_trips_never_storable_uninstall_without_application() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "not_found",
        "partial": false,
        "sections": [],
        "problems": [],
        "unavailable": [],
        "registry_effect": "unchanged",
        "audit_id": "audit-123"
    }));
}

// DESIGN §3.1: `application.id` and `.version` are each present only when extracted
// and within limits.
#[test]
fn outcome_report_round_trips_application_with_version_only() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "invalid_manifest",
        "application": {"version": "2"},
        "partial": false,
        "sections": [],
        "problems": [{"location": "/application/id", "rule": "required", "message": "missing"}],
        "unavailable": [],
        "registry_effect": "not_updated",
        "audit_id": "audit-123"
    }));
}

#[test]
fn outcome_report_round_trips_application_with_id_only() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "invalid_manifest",
        "application": {"id": "app-1"},
        "partial": false,
        "sections": [],
        "problems": [{"location": "/application/version", "rule": "required", "message": "missing"}],
        "unavailable": [],
        "registry_effect": "not_updated",
        "audit_id": "audit-123"
    }));
}

// Minimal report: no `application`, no `audit_id`, sections without detail or location.
// Equality with the input proves that absent members are omitted, never `null`.
#[test]
fn outcome_report_round_trips_minimal_report_without_optional_members() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "not_permitted",
        "partial": false,
        "sections": [{"key": "alpha", "outcome": "not_dispatched"}],
        "problems": [],
        "unavailable": [],
        "registry_effect": "not_updated"
    }));
}

#[test]
fn outcome_report_omits_absent_optional_members_when_serialized() {
    let report: OutcomeReport = serde_json::from_value(json!({
        "category": "not_permitted",
        "partial": false,
        "sections": [{"key": "alpha", "outcome": "not_dispatched"}],
        "problems": [],
        "unavailable": [{"dependency": "access_control", "reason": "unreachable"}],
        "registry_effect": "not_updated"
    }))
    .expect("deserialize");
    let back = serde_json::to_value(&report).expect("serialize");
    for absent in ["application", "audit_id"] {
        assert!(back.get(absent).is_none(), "{absent} must be omitted");
    }
    assert!(
        back["sections"][0].get("detail").is_none(),
        "detail omitted"
    );
    assert!(
        back["sections"][0].get("location").is_none(),
        "location omitted"
    );
    assert!(back["unavailable"][0].get("key").is_none(), "key omitted");
}

// Serde shape only: every optional member present at once. The combination is NOT a
// coherent report (see the per-category fixtures above).
#[test]
fn outcome_report_round_trips_every_member_serde_shape_only() {
    assert_lossless::<OutcomeReport>(&json!({
        "category": "temporarily_unavailable",
        "application": {"id": "app-1", "version": "2"},
        "partial": true,
        "sections": [
            {"key": "alpha", "outcome": "succeeded"},
            {"key": "beta", "outcome": "rejected", "detail": "bad", "location": "/x/y"},
            {"key": "gamma", "outcome": "not_permitted", "detail": "no"}
        ],
        "problems": [
            {"location": "/sections/beta", "rule": "required", "message": "missing"}
        ],
        "unavailable": [
            {"dependency": "section_schema", "key": "beta", "reason": "not_sent"},
            {"dependency": "audit_plugin", "reason": "timed_out"}
        ],
        "registry_effect": "not_updated",
        "audit_id": "audit-123"
    }));
}

// --- CallerIdentity (DESIGN §3.1 entity-caller-identity) --------------------------

fn caller_json() -> Value {
    json!({
        "subject_id": "00000000-0000-0000-0000-000000000001",
        "subject_type": "user",
        "subject_tenant_id": "00000000-0000-0000-0000-000000000002",
        "token_scopes": ["*"],
        "request_id": REQUEST_ID
    })
}

fn request_uuid() -> Uuid {
    Uuid::parse_str(REQUEST_ID).expect("request id")
}

fn security_context(scopes: Vec<String>, subject_type: Option<&str>) -> SecurityContext {
    let mut builder = SecurityContext::builder()
        .subject_id(Uuid::from_u128(1))
        .subject_tenant_id(Uuid::from_u128(2))
        .token_scopes(scopes)
        .bearer_token("super-secret-bearer".to_owned());
    if let Some(kind) = subject_type {
        builder = builder.subject_type(kind);
    }
    builder.build().expect("security context")
}

#[test]
fn caller_identity_round_trips_with_exact_field_names() {
    assert_lossless::<CallerIdentity>(&caller_json());
}

// DESIGN §3.1, §4.8 security: the serialized form has exactly the five members, so no
// bearer token can reach a plugin.
#[test]
fn caller_identity_serializes_exactly_five_members() {
    let identity: CallerIdentity = serde_json::from_value(caller_json()).expect("de");
    let value = serde_json::to_value(&identity).expect("ser");
    let mut keys: Vec<&str> = value
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "request_id",
            "subject_id",
            "subject_tenant_id",
            "subject_type",
            "token_scopes"
        ]
    );
}

#[test]
fn caller_identity_rejects_token_members() {
    for member in ["bearer_token", "token", "access_token"] {
        let mut value = caller_json();
        value[member] = json!("secret");
        assert_rejected::<CallerIdentity>(value, member);
    }
}

#[test]
fn caller_identity_round_trips_without_subject_type() {
    let mut value = caller_json();
    value
        .as_object_mut()
        .expect("object")
        .remove("subject_type");
    assert_lossless::<CallerIdentity>(&value);
}

// DESIGN §3.1: copies subject id, type, tenant and scopes; carries the request id.
#[test]
fn caller_identity_from_security_context_copies_identity_fields() {
    let ctx = security_context(vec!["*".to_owned()], Some("user"));
    let identity = CallerIdentity::from_security_context(&ctx, request_uuid());
    assert_eq!(
        serde_json::to_value(&identity).expect("ser"),
        json!({
            "subject_id": Uuid::from_u128(1).to_string(),
            "subject_type": "user",
            "subject_tenant_id": Uuid::from_u128(2).to_string(),
            "token_scopes": ["*"],
            "request_id": REQUEST_ID
        })
    );
}

// DESIGN §3.3: an empty scope list means no capability, so it must stay empty, and a
// list of several scopes is copied whole and in order.
#[test]
fn caller_identity_from_security_context_copies_scopes_exactly() {
    for scopes in [
        vec![],
        vec!["*".to_owned()],
        vec!["read:a".to_owned(), "write:b".to_owned(), "c".to_owned()],
    ] {
        let ctx = security_context(scopes.clone(), Some("user"));
        let identity = CallerIdentity::from_security_context(&ctx, request_uuid());
        let value = serde_json::to_value(&identity).expect("ser");
        assert_eq!(value["token_scopes"], json!(scopes), "scopes {scopes:?}");
    }
}

#[test]
fn caller_identity_from_security_context_omits_absent_subject_type() {
    let ctx = security_context(vec![], None);
    let identity = CallerIdentity::from_security_context(&ctx, request_uuid());
    let value = serde_json::to_value(&identity).expect("ser");
    assert!(value.get("subject_type").is_none(), "subject_type omitted");
}

// DESIGN §3.1, §4.8 security: the bearer token never reaches the identity.
#[test]
fn caller_identity_from_security_context_drops_bearer_token() {
    let ctx = security_context(vec!["*".to_owned()], Some("user"));
    assert!(ctx.bearer_token().is_some(), "fixture carries a token");
    let identity = CallerIdentity::from_security_context(&ctx, request_uuid());
    let text = serde_json::to_string(&identity).expect("ser");
    assert!(
        !text.contains("super-secret-bearer"),
        "token leaked: {text}"
    );
}

// DESIGN §3.1, §3.3: the SDK constructor rebuilds a SecurityContext with no token.
#[test]
fn caller_identity_to_security_context_copies_fields_and_has_no_token() {
    for (scopes, subject_type) in [
        (vec![], Some("user")),
        (vec!["*".to_owned()], Some("service")),
        (vec!["a".to_owned(), "b".to_owned()], None),
    ] {
        let mut value = caller_json();
        value["token_scopes"] = json!(scopes);
        match subject_type {
            Some(kind) => value["subject_type"] = json!(kind),
            None => {
                value
                    .as_object_mut()
                    .expect("object")
                    .remove("subject_type");
            }
        }
        let identity: CallerIdentity = serde_json::from_value(value).expect("de");
        let ctx = identity.to_security_context();
        assert_eq!(ctx.subject_id(), Uuid::from_u128(1));
        assert_eq!(
            ctx.subject_tenant_id(),
            Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("uuid")
        );
        assert_eq!(ctx.subject_type(), subject_type);
        assert_eq!(ctx.token_scopes(), scopes.as_slice(), "scopes copied");
        assert!(ctx.bearer_token().is_none(), "no bearer token");
    }
}

#[test]
fn caller_identity_security_context_round_trips_identity_fields() {
    let ctx = security_context(vec!["x".to_owned(), "y".to_owned()], Some("user"));
    let back = CallerIdentity::from_security_context(&ctx, request_uuid()).to_security_context();
    assert_eq!(back.subject_id(), ctx.subject_id());
    assert_eq!(back.subject_type(), ctx.subject_type());
    assert_eq!(back.subject_tenant_id(), ctx.subject_tenant_id());
    assert_eq!(back.token_scopes(), ctx.token_scopes());
    assert!(back.bearer_token().is_none(), "no bearer token");
}

// --- RequestHandoffContent (DESIGN §3.1 entity-audit-event-content) ----------------

fn seed_handoff(application: Option<Value>, manifest: Value) -> Value {
    let mut value = json!({
        "operation": "seed",
        "request_id": REQUEST_ID,
        "received_at": "2026-10-12T10:00:00Z",
        "manifest": manifest
    });
    if let Some(app) = application {
        value["application"] = app;
    }
    value
}

#[test]
fn request_handoff_round_trips_parsed_seed_with_decided_category() {
    let mut value = seed_handoff(Some(json!({"id": "app-1", "version": "1"})), json!({}));
    value["decided_category"] = json!("not_permitted");
    assert_lossless::<RequestHandoffContent>(&value);
}

#[test]
fn request_handoff_round_trips_parsed_seed_without_decided_category() {
    let value = seed_handoff(Some(json!({"id": "app-1", "version": "1"})), json!({}));
    assert_lossless::<RequestHandoffContent>(&value);
    let parsed: RequestHandoffContent = serde_json::from_value(value).expect("de");
    let back = serde_json::to_value(&parsed).expect("ser");
    assert!(back.get("decided_category").is_none(), "omitted, not null");
}

// An invalid seed body is still handed off and audited, with no `manifest`.
#[test]
fn request_handoff_round_trips_seed_whose_body_is_not_json() {
    let value = json!({
        "operation": "seed",
        "request_id": REQUEST_ID,
        "received_at": "2026-10-12T10:00:00Z"
    });
    assert_lossless::<RequestHandoffContent>(&value);
    let parsed: RequestHandoffContent = serde_json::from_value(value).expect("de");
    assert!(parsed.manifest.is_none(), "no manifest for a non-JSON body");
}

// A body that is the JSON value `null` parses; `manifest: null` is a value, not absence.
#[test]
fn request_handoff_round_trips_json_body_with_parsed_null() {
    let value = seed_handoff(None, Value::Null);
    assert_lossless::<RequestHandoffContent>(&value);
    let parsed: RequestHandoffContent = serde_json::from_value(value).expect("de");
    let back = serde_json::to_value(&parsed).expect("ser");
    assert_eq!(
        back.get("manifest"),
        Some(&Value::Null),
        "manifest null must not collapse to absent"
    );
}

#[test]
fn request_handoff_round_trips_seed_with_id_only() {
    let value = seed_handoff(Some(json!({"id": "app-1"})), json!({}));
    assert_lossless::<RequestHandoffContent>(&value);
}

#[test]
fn request_handoff_round_trips_seed_with_version_only() {
    let value = seed_handoff(Some(json!({"version": "1"})), json!({}));
    assert_lossless::<RequestHandoffContent>(&value);
}

// The caller identity is an argument of the hand-off, never part of its content.
#[test]
fn request_handoff_round_trips_seed_without_application() {
    let value = seed_handoff(None, json!({}));
    assert_lossless::<RequestHandoffContent>(&value);
    let parsed: RequestHandoffContent = serde_json::from_value(value).expect("de");
    let back = serde_json::to_value(&parsed).expect("ser");
    for absent in ["application", "caller", "caller_identity", "subject_id"] {
        assert!(back.get(absent).is_none(), "{absent} must be absent");
    }
}

#[test]
fn request_handoff_round_trips_uninstall_found_with_id_and_version() {
    assert_lossless::<RequestHandoffContent>(&json!({
        "operation": "uninstall",
        "request_id": REQUEST_ID,
        "received_at": "2026-10-12T10:00:00Z",
        "application": {"id": "app-1", "version": "3"}
    }));
}

#[test]
fn request_handoff_round_trips_uninstall_not_found_with_id_only() {
    assert_lossless::<RequestHandoffContent>(&json!({
        "operation": "uninstall",
        "request_id": REQUEST_ID,
        "received_at": "2026-10-12T10:00:00Z",
        "application": {"id": "app-1"}
    }));
}

// DESIGN §3.1: a never-storable uninstall id is carried as byte length plus the
// SHA-256 digest (`sha256:` + 64 lowercase hex) of its bytes, and has no `application`.
#[test]
fn request_handoff_round_trips_uninstall_never_storable_id_with_digest() {
    let value = json!({
        "operation": "uninstall",
        "request_id": REQUEST_ID,
        "received_at": "2026-10-12T10:00:00Z",
        "id_length_bytes": 3,
        "id_sha256": NEVER_STORABLE_ID_SHA256
    });
    assert_lossless::<RequestHandoffContent>(&value);
    let parsed: RequestHandoffContent = serde_json::from_value(value).expect("de");
    let back = serde_json::to_value(&parsed).expect("ser");
    assert!(back.get("application").is_none(), "no application member");
}

// --- FinalResultContent (DESIGN §3.1 entity-audit-event-content) -------------------

#[test]
fn final_result_round_trips_validated_seed_with_fingerprints() {
    assert_lossless::<FinalResultContent>(&json!({
        "audit_id": "audit-123",
        "request_id": REQUEST_ID,
        "validation": {"valid": true, "problems": []},
        "sections": [
            {"key": "alpha", "outcome": "succeeded", "schema_fingerprint": FINGERPRINT_OBJECT},
            {"key": "beta", "outcome": "timed_out", "schema_fingerprint": FINGERPRINT_STRING}
        ],
        "category": "internal_failure",
        "registry_effect": "not_updated",
        "completed_at": "2026-10-12T10:00:01Z"
    }));
}

#[test]
fn final_result_round_trips_failed_validation_with_problems() {
    assert_lossless::<FinalResultContent>(&json!({
        "audit_id": "audit-123",
        "request_id": REQUEST_ID,
        "validation": {
            "valid": false,
            "problems": [{"location": "/sections/alpha", "rule": "type", "message": "bad"}]
        },
        "sections": [
            {"key": "alpha", "outcome": "not_dispatched", "schema_fingerprint": FINGERPRINT_OBJECT}
        ],
        "category": "invalid_manifest",
        "registry_effect": "not_updated",
        "completed_at": "2026-10-12T10:00:01Z"
    }));
}

// An uninstall has no schema and no validation: no fingerprints, no `validation`.
#[test]
fn final_result_round_trips_uninstall_without_validation_or_fingerprints() {
    assert_lossless::<FinalResultContent>(&json!({
        "audit_id": "audit-123",
        "request_id": REQUEST_ID,
        "sections": [{"key": "alpha", "outcome": "succeeded"}],
        "category": "succeeded",
        "registry_effect": "deleted",
        "completed_at": "2026-10-12T10:00:01Z"
    }));
}

// A seed that ended before validation ran (for example, not permitted) has no
// `validation` member: "not run" is distinguishable from "valid".
#[test]
fn final_result_round_trips_seed_ended_before_validation() {
    let value = json!({
        "audit_id": "audit-123",
        "request_id": REQUEST_ID,
        "sections": [],
        "category": "not_permitted",
        "registry_effect": "not_updated",
        "completed_at": "2026-10-12T10:00:01Z"
    });
    assert_lossless::<FinalResultContent>(&value);
    let parsed: FinalResultContent = serde_json::from_value(value).expect("de");
    let back = serde_json::to_value(&parsed).expect("ser");
    assert!(
        back.get("validation").is_none(),
        "validation not run, omitted"
    );
}

// --- Debug redaction (DESIGN §2.2: manifest content never in diagnostics) ----------

#[test]
fn debug_of_request_handoff_redacts_manifest_content() {
    let secret = "s3cr3t-manifest-value";
    let value = seed_handoff(
        Some(json!({"id": "app-1", "version": "1"})),
        json!({"token": secret}),
    );
    let content: RequestHandoffContent = serde_json::from_value(value).expect("deserialize");

    let text = format!("{content:?}");

    assert!(
        !text.contains(secret),
        "Debug must not print the manifest: {text}"
    );
    assert!(
        text.contains("manifest: Some(\"<redacted>\")"),
        "Debug must mark the manifest as redacted: {text}"
    );
    assert!(text.contains("app-1"), "other members stay visible: {text}");
}

// --- Manifest depth inside the hand-off (DESIGN §3.1, §4.1) -------------------------

/// `depth` nested arrays: `[]` has depth 1.
fn nested_array_text(depth: usize) -> String {
    format!("{}{}", "[".repeat(depth), "]".repeat(depth))
}

fn handoff_round_trips_at_depth(depth: usize) -> bool {
    let text = nested_array_text(depth);
    let parsed: Value = serde_json::from_str(&text).expect("serde_json parses the manifest");
    let value = seed_handoff(None, parsed);
    let content: RequestHandoffContent = serde_json::from_value(value).expect("build");
    let wire = serde_json::to_string(&content).expect("serialize");
    serde_json::from_str::<RequestHandoffContent>(&wire).is_ok_and(|back| back == content)
}

// An audit plugin that stores the hand-off as JSON must be able to read it back for
// every manifest the gear accepts; the gear's parse depth must not exceed this bound.
#[test]
fn handoff_round_trips_manifest_at_max_depth() {
    assert!(
        handoff_round_trips_at_depth(HANDOFF_MANIFEST_MAX_DEPTH),
        "a manifest at the published depth bound must round-trip"
    );
}

// The bound is the largest depth that round-trips, so it is not needlessly low.
#[test]
fn handoff_manifest_max_depth_is_the_largest_that_round_trips() {
    assert!(
        !handoff_round_trips_at_depth(HANDOFF_MANIFEST_MAX_DEPTH + 1),
        "one level above the bound must fail the serde_json recursion limit"
    );
}
