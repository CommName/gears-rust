//! Shared builders and readers of the P7 request tests (the full request: dispatch, registry
//! effect, final-result hand-off, request task and supervisor).

use std::time::{Duration, UNIX_EPOCH};

use application_manager_sdk::models::{OutcomeReport, SectionOutcome};
use application_manager_sdk::{FinalResultContent, RequestHandoffContent};
use serde_json::{Value, json};

use crate::domain::registry::{LastResult, RegistryEntry};
use crate::test_support::{AuditCall, Gate, GateBuilder, seed_body};

/// A gate over three section plugins in priority order: `files`, `db`, `net`. Each serves its
/// own schema, so a fingerprint tells them apart.
pub fn three_section_gate() -> GateBuilder {
    GateBuilder::new()
        .section("files", files_schema())
        .section("db", db_schema())
        .section("net", net_schema())
}

/// The schema `files` serves.
pub fn files_schema() -> Value {
    json!({"type": "object", "properties": {"path": {"type": "string"}}})
}

/// The schema `db` serves.
pub fn db_schema() -> Value {
    json!({"type": "object", "properties": {"tables": {"type": "array"}}})
}

/// The schema `net` serves.
pub fn net_schema() -> Value {
    json!({"type": "object"})
}

/// The sections of [`three_section_body`].
pub fn three_sections() -> Value {
    json!({
        "files": {"path": "/srv/app", "nested": {"k": [1, 2.5, null, true]}},
        "db": {"tables": ["a", "b"]},
        "net": {},
    })
}

/// A valid seed of `app-1` version `1.0` for [`three_section_gate`].
pub fn three_section_body() -> Vec<u8> {
    seed_body("app-1", "1.0", &three_sections())
}

/// A valid seed of `app-1` version `1.0` for a gate with the one section `files`.
pub fn files_body() -> Vec<u8> {
    seed_body("app-1", "1.0", &json!({"files": {"path": "/srv/app"}}))
}

/// A stored entry of `id` at `version`, as an earlier seed left it.
pub fn stored(id: &str, version: &str) -> RegistryEntry {
    RegistryEntry::new(
        id,
        version,
        LastResult::Succeeded,
        UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        "audit-old",
    )
    .expect("a storable entry")
}

/// The section outcomes of a report, in order.
pub fn outcomes(report: &OutcomeReport) -> Vec<SectionOutcome> {
    report.sections.iter().map(|s| s.outcome).collect()
}

/// The keys of the sections of a report, in order.
pub fn keys(report: &OutcomeReport) -> Vec<&str> {
    report.sections.iter().map(|s| s.key.as_str()).collect()
}

/// The hand-off kinds the audit plugin received, in order.
pub fn audit_kinds(gate: &Gate) -> Vec<AuditCall> {
    gate.audit.calls().iter().map(|c| c.kind).collect()
}

/// The one request hand-off content.
///
/// # Panics
///
/// Panics unless exactly one request hand-off was made.
pub fn request_content(gate: &Gate) -> RequestHandoffContent {
    let all: Vec<RequestHandoffContent> = gate
        .audit
        .calls()
        .into_iter()
        .filter_map(|c| c.request)
        .collect();
    assert_eq!(all.len(), 1, "exactly one request hand-off: {all:?}");
    all[0].clone()
}

/// The one final-result hand-off: the audit id argument and the content.
///
/// # Panics
///
/// Panics unless exactly one final-result hand-off was made.
pub fn final_content(gate: &Gate) -> (String, FinalResultContent) {
    let all: Vec<(String, FinalResultContent)> = gate
        .audit
        .calls()
        .into_iter()
        .filter_map(|c| Some((c.audit_id?, c.result?)))
        .collect();
    assert_eq!(all.len(), 1, "exactly one final-result hand-off: {all:?}");
    all[0].clone()
}

/// Whether the audit plugin received a final-result hand-off.
pub fn has_final(gate: &Gate) -> bool {
    audit_kinds(gate).contains(&AuditCall::FinalResultHandoff)
}
