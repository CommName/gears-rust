//! Unit tests for the gate's value types and helpers (F-006, F-011): the redacting `Debug` of
//! `RequestInput`, `Proceed`, `GateOutcome` and `GateResult` (no request content in a debug
//! print), and the RFC 3339 rendering of the receive time (no panic outside the range).

#![allow(clippy::duration_suboptimal_units)]

use std::time::{Duration, UNIX_EPOCH};

use application_manager_sdk::models::{ApplicationRef, OutcomeCategory, Problem};
use application_manager_sdk::{ApplicationEnvelope, CallerIdentity};
use serde_json::json;
use uuid::Uuid;

use crate::domain::orchestrator::{GateOutcome, GateResult, Proceed, RequestInput, rfc3339};
use crate::domain::schema_validator::SectionSchema;
use crate::test_support::{GateBuilder, decided, seed_body};

const SENTINEL: &str = "SENTINEL-4f9c";

fn assert_redacted(text: &str) {
    assert!(!text.contains(SENTINEL), "request content in {text}");
}

#[test]
fn the_debug_of_a_request_input_shows_lengths_only() {
    let seed = RequestInput::Seed {
        body: format!("{{\"k\": \"{SENTINEL}\"}}").into_bytes(),
    };
    let uninstall = RequestInput::Uninstall {
        application_id: SENTINEL.to_owned(),
    };

    let (seed, uninstall) = (format!("{seed:?}"), format!("{uninstall:?}"));

    assert_redacted(&seed);
    assert_redacted(&uninstall);
    assert!(seed.contains("body_len"), "{seed}");
    assert!(uninstall.contains(&format!("application_id_len: {}", SENTINEL.len())));
}

#[test]
fn the_debug_of_a_proceed_and_a_gate_result_hides_the_manifest_and_the_envelope() {
    let proceed = Proceed {
        envelope: ApplicationEnvelope {
            id: SENTINEL.to_owned(),
            version: SENTINEL.to_owned(),
        },
        manifest: Some(json!({"sections": {"files": SENTINEL}})),
        schemas: vec![SectionSchema {
            key: "files".to_owned(),
            schema: json!({"description": SENTINEL}),
        }],
        fingerprints: vec![("files".to_owned(), SENTINEL.to_owned())],
    };
    let result = GateResult {
        request_id: Uuid::from_u128(1),
        caller: CallerIdentity {
            subject_id: Uuid::from_u128(2),
            subject_type: None,
            subject_tenant_id: Uuid::from_u128(3),
            token_scopes: vec![],
            request_id: Uuid::from_u128(1),
        },
        audit_id: Some("audit-1".to_owned()),
        outcome: GateOutcome::Proceed(proceed.clone()),
    };

    assert_redacted(&format!("{proceed:?}"));
    assert_redacted(&format!("{result:?}"));
    assert!(format!("{result:?}").contains("audit-1"));
}

#[tokio::test]
async fn the_debug_of_a_decided_result_hides_problems_and_the_application() {
    let gate = GateBuilder::new()
        .section("files", json!({"type": "object"}))
        .build();
    // An invalid manifest: the problems and the reported application quote request content.
    let result = gate
        .seed(seed_body(SENTINEL, "1", &json!({"files": SENTINEL})))
        .await;
    let report = decided(&result);
    assert_eq!(report.category, OutcomeCategory::InvalidManifest);
    assert!(
        report
            .problems
            .iter()
            .any(|p: &Problem| p.message.contains(SENTINEL))
            || report
                .application
                .as_ref()
                .is_some_and(|a: &ApplicationRef| a.id.as_deref() == Some(SENTINEL)),
        "the fixture carries the sentinel in the report: {report:?}"
    );

    assert_redacted(&format!("{result:?}"));
}

// F-011: `OffsetDateTime::from(SystemTime)` panics outside its range; the rendering is checked.
#[test]
fn rfc3339_renders_a_normal_time_and_nothing_outside_the_representable_range() {
    assert_eq!(
        rfc3339(UNIX_EPOCH + Duration::from_secs(1_800_000_000)),
        "2027-01-15T08:00:00Z"
    );
    assert_eq!(rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
    assert_eq!(
        rfc3339(UNIX_EPOCH - Duration::from_secs(86_400)),
        "1969-12-31T00:00:00Z"
    );
    for far in [
        UNIX_EPOCH + Duration::from_secs(u64::MAX >> 2),
        UNIX_EPOCH + Duration::from_secs(1 << 60),
        UNIX_EPOCH - Duration::from_secs(1 << 60),
        UNIX_EPOCH + Duration::from_secs(300_000_000_000),
    ] {
        assert_eq!(rfc3339(far), "", "{far:?}");
    }
}
