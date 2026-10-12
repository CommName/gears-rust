//! Tests for the plugin error kinds and the SDK-published bounds (DESIGN §3.3 plugin
//! contract, §4.8 unit tests).
//!
//! Planned API: `SectionPluginError::{rejected, not_permitted, internal}` and
//! `AuditPluginError::{refused, unavailable}` constructors on `#[non_exhaustive]`
//! variants, `AuditPluginError::reason`, `PLUGIN_DETAIL_MAX_BYTES`, and
//! `truncate_detail`.

use crate::errors::{
    AuditPluginError, PLUGIN_DETAIL_MAX_BYTES, SectionPluginError, truncate_detail,
};
use crate::models::{SectionOutcome, UnavailableReason};

// DESIGN §3.3: `Rejected` carries a detail and an optional JSON Pointer location.
#[test]
fn section_rejected_keeps_detail_and_location() {
    let err = SectionPluginError::rejected("bad value", Some("/limits/0"));
    match err {
        SectionPluginError::Rejected {
            ref detail,
            ref location,
            ..
        } => {
            assert_eq!(detail, "bad value");
            assert_eq!(location.as_deref(), Some("/limits/0"));
        }
        ref other => panic!("expected Rejected, got {other:?}"),
    }
    assert!(
        err.to_string().contains("bad value"),
        "Display names detail"
    );
}

// DESIGN §3.3: the location of `Rejected` is optional.
#[test]
fn section_rejected_without_location_has_none() {
    let err = SectionPluginError::rejected("bad", None);
    match err {
        SectionPluginError::Rejected { ref location, .. } => assert!(location.is_none()),
        ref other => panic!("expected Rejected, got {other:?}"),
    }
}

// DESIGN §3.3: `Rejected` gives section outcome `rejected`.
#[test]
fn section_rejected_maps_to_rejected_outcome() {
    let err = SectionPluginError::rejected("bad", None);
    assert_eq!(err.outcome(), SectionOutcome::Rejected);
}

// DESIGN §3.3: `NotPermitted` carries a detail, section outcome `not_permitted`.
#[test]
fn section_not_permitted_maps_to_not_permitted_outcome() {
    let err = SectionPluginError::not_permitted("needs extra permission");
    assert_eq!(err.outcome(), SectionOutcome::NotPermitted);
    assert!(
        err.to_string().contains("needs extra permission"),
        "Display names detail"
    );
}

// DESIGN §3.3: `Internal` carries a diagnostic detail, section outcome `internal_failure`.
#[test]
fn section_internal_maps_to_internal_failure_outcome() {
    let err = SectionPluginError::internal("db down");
    assert_eq!(err.outcome(), SectionOutcome::InternalFailure);
}

// DESIGN §3.3 audit error table: `Refused` gives reason `refused`.
#[test]
fn audit_refused_maps_to_refused_reason() {
    let err = AuditPluginError::refused("cannot record");
    assert_eq!(err.reason(), UnavailableReason::Refused);
    assert!(
        err.to_string().contains("cannot record"),
        "Display names detail"
    );
}

// DESIGN §3.3 audit error table: `Unavailable` gives reason `unreachable`.
#[test]
fn audit_unavailable_maps_to_unreachable_reason() {
    let err = AuditPluginError::unavailable("down");
    assert_eq!(err.reason(), UnavailableReason::Unreachable);
    assert!(err.to_string().contains("down"), "Display names detail");
}

// DESIGN §3.3: the detail bound is SDK-fixed (value not specified, so only its
// shape is pinned).
#[test]
fn plugin_detail_max_bytes_is_positive() {
    assert!(
        PLUGIN_DETAIL_MAX_BYTES > 0,
        "the bound must admit some text"
    );
}

// DESIGN §3.3: a detail at the bound is kept whole.
#[test]
fn truncate_detail_keeps_text_at_bound() {
    let text = "a".repeat(PLUGIN_DETAIL_MAX_BYTES);
    assert_eq!(truncate_detail(&text), text);
}

// DESIGN §3.3: a detail one byte over the bound is cut to the bound.
#[test]
fn truncate_detail_cuts_text_above_bound() {
    let text = "a".repeat(PLUGIN_DETAIL_MAX_BYTES + 1);
    assert_eq!(truncate_detail(&text).len(), PLUGIN_DETAIL_MAX_BYTES);
}

// DESIGN §3.3: a multi-byte character that straddles the cut is dropped whole, so
// the result is valid UTF-8 and never longer than the bound.
#[test]
fn truncate_detail_keeps_utf8_valid_when_char_straddles_cut() {
    // One ASCII byte short of the bound, then a 2-byte char crossing it.
    let text = format!("{}é", "a".repeat(PLUGIN_DETAIL_MAX_BYTES - 1));
    assert!(
        text.len() > PLUGIN_DETAIL_MAX_BYTES,
        "fixture must straddle"
    );
    let cut = truncate_detail(&text);
    assert!(cut.len() <= PLUGIN_DETAIL_MAX_BYTES, "within the bound");
    assert_eq!(
        cut,
        "a".repeat(PLUGIN_DETAIL_MAX_BYTES - 1),
        "char dropped whole"
    );
}

// DESIGN §3.3: empty text stays empty.
#[test]
fn truncate_detail_keeps_empty_text() {
    assert_eq!(truncate_detail(""), "");
}

// DESIGN §3.3: the SDK fixes the detail bound, so every constructor applies it and a
// plugin cannot push an unbounded string into caller-facing bodies or audit.
#[test]
fn constructors_bound_detail_and_location() {
    let long = "x".repeat(4 * PLUGIN_DETAIL_MAX_BYTES);
    let errors = [
        SectionPluginError::rejected(long.clone(), Some(&long)),
        SectionPluginError::not_permitted(long.clone()),
        SectionPluginError::internal(long.clone()),
    ];
    for err in errors {
        match err {
            SectionPluginError::Rejected {
                detail, location, ..
            } => {
                assert_eq!(detail.len(), PLUGIN_DETAIL_MAX_BYTES, "rejected detail");
                assert_eq!(
                    location.map(|l| l.len()),
                    Some(PLUGIN_DETAIL_MAX_BYTES),
                    "rejected location"
                );
            }
            SectionPluginError::NotPermitted { detail, .. }
            | SectionPluginError::Internal { detail, .. } => {
                assert_eq!(detail.len(), PLUGIN_DETAIL_MAX_BYTES, "detail");
            }
        }
    }
    for err in [
        AuditPluginError::refused(long.clone()),
        AuditPluginError::unavailable(long),
    ] {
        match err {
            AuditPluginError::Refused { detail, .. }
            | AuditPluginError::Unavailable { detail, .. } => {
                assert_eq!(detail.len(), PLUGIN_DETAIL_MAX_BYTES, "audit detail");
            }
        }
    }
}
