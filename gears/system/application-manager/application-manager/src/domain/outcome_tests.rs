#![allow(clippy::unwrap_used, clippy::expect_used)]

use uuid::Uuid;

use super::*;

/// One envelope problem.
fn problem() -> Problem {
    crate::domain::schema_validator::SchemaValidator::new(8)
        .unwrap()
        .check_envelope(&serde_json::json!({ "application": {}, "sections": {} }))
        .remove(0)
}

fn open(expired: bool) -> OpenOperation {
    OpenOperation {
        operation_id: Uuid::from_u128(7),
        expired,
    }
}

/// An allowed seed with nothing wrong.
fn clean() -> SyncFacts {
    SyncFacts {
        action: CallerAction::Seed,
        gate: Gate::Open,
        allowed: true,
        open: Some(Ok(None)),
        envelope_problems: vec![],
    }
}

#[test]
fn a_clean_request_has_no_synchronous_refusal() {
    assert_eq!(sync_refusal(&clean()), None);
}

#[test]
fn an_open_operation_past_its_deadline_is_not_a_conflict() {
    let facts = SyncFacts {
        open: Some(Ok(Some(open(true)))),
        ..clean()
    };
    assert_eq!(sync_refusal(&facts), None);
}

#[test]
fn the_synchronous_ranks_decide_in_design_order() {
    let all_wrong = |gate| SyncFacts {
        action: CallerAction::Uninstall,
        gate,
        allowed: true,
        open: Some(Err(StorageError::Unavailable)),
        envelope_problems: vec![problem()],
    };
    // Rank 1: the gate.
    assert_eq!(
        sync_refusal(&all_wrong(Gate::NotReady)),
        Some(DomainError::NotReady)
    );
    assert_eq!(
        sync_refusal(&all_wrong(Gate::AtCapacity)),
        Some(DomainError::AtCapacity)
    );
    // Rank 1: storage, for an allowed caller.
    assert_eq!(
        sync_refusal(&all_wrong(Gate::Open)),
        Some(DomainError::StorageUnavailable)
    );
    // Rank 2: a caller not allowed; storage is never consulted for it.
    let denied = SyncFacts {
        allowed: false,
        open: None,
        ..all_wrong(Gate::Open)
    };
    assert_eq!(
        sync_refusal(&denied),
        Some(DomainError::NotPermitted {
            action: CallerAction::Uninstall
        })
    );
    // Rank 3 beats rank 4.
    let busy = SyncFacts {
        open: Some(Ok(Some(open(false)))),
        ..all_wrong(Gate::Open)
    };
    assert_eq!(
        sync_refusal(&busy),
        Some(DomainError::Conflict {
            operation_id: Uuid::from_u128(7)
        })
    );
    // Rank 4.
    let invalid = SyncFacts {
        open: Some(Ok(None)),
        ..all_wrong(Gate::Open)
    };
    assert_eq!(
        sync_refusal(&invalid),
        Some(DomainError::InvalidManifest {
            problems: vec![problem()]
        })
    );
}

#[test]
fn every_refusal_records_its_category() {
    let cases = [
        (
            DomainError::NotReady,
            RequestCategory::TemporarilyUnavailable,
        ),
        (
            DomainError::AtCapacity,
            RequestCategory::TemporarilyUnavailable,
        ),
        (
            DomainError::StorageUnavailable,
            RequestCategory::TemporarilyUnavailable,
        ),
        (
            DomainError::NotPermitted {
                action: CallerAction::Seed,
            },
            RequestCategory::NotPermitted,
        ),
        (
            DomainError::Conflict {
                operation_id: Uuid::nil(),
            },
            RequestCategory::Conflict,
        ),
        (
            DomainError::InvalidManifest { problems: vec![] },
            RequestCategory::InvalidManifest,
        ),
        (DomainError::Defect, RequestCategory::InternalFailure),
    ];
    for (refusal, category) in cases {
        assert_eq!(category_of(&refusal), category, "{refusal:?}");
    }
}
