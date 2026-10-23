//! The DESIGN 3.3 outcome table, one row per rank, with its precedence.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use application_manager_sdk::{Dependency, SectionOutcome, UnavailableReasonCode};
use uuid::Uuid;

use super::*;

const BUSY: Uuid = Uuid::from_u128(7);

fn problem(rule: &str) -> Problem {
    Problem {
        location: String::new(),
        rule: rule.to_owned(),
        message: String::new(),
    }
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "the storage step of the facts is optional"
)]
fn open(expired: bool) -> Option<Result<Option<OpenOperation>, StorageError>> {
    let found = OpenOperation {
        operation_id: BUSY,
        expired,
    };
    Some(Ok(Some(found)))
}

/// Facts of a request that passes every rank.
fn clean() -> SyncFacts {
    SyncFacts {
        action: Action::Seed,
        gate: Gate::Open,
        access: Ok(()),
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
        open: open(true),
        ..clean()
    };
    assert_eq!(sync_refusal(&facts), None);
}

/// One row per synchronous rank and per precedence pair: the first matching
/// rank decides.
#[test]
fn the_synchronous_ranks_decide_in_design_order() {
    let invalid = vec![problem("type")];
    let cases: Vec<(&str, SyncFacts, DomainError)> = vec![
        (
            "rank 1: not ready",
            SyncFacts {
                gate: Gate::NotReady,
                ..clean()
            },
            DomainError::NotReady,
        ),
        (
            "rank 1: at capacity",
            SyncFacts {
                gate: Gate::AtCapacity,
                ..clean()
            },
            DomainError::AtCapacity,
        ),
        (
            "rank 1: access control cannot answer",
            SyncFacts {
                access: Err(AccessError::Unavailable),
                open: None,
                ..clean()
            },
            DomainError::AccessUnavailable,
        ),
        (
            "rank 1: storage cannot answer for a permitted caller",
            SyncFacts {
                open: Some(Err(StorageError::Unavailable)),
                ..clean()
            },
            DomainError::StorageUnavailable,
        ),
        (
            "rank 2: not permitted names the action",
            SyncFacts {
                action: Action::Uninstall,
                access: Err(AccessError::NotPermitted),
                open: None,
                ..clean()
            },
            DomainError::NotPermitted {
                action: Action::Uninstall,
            },
        ),
        (
            "rank 3: conflict names the open operation",
            SyncFacts {
                open: open(false),
                ..clean()
            },
            DomainError::Conflict { operation_id: BUSY },
        ),
        (
            "rank 4: envelope problems are listed",
            SyncFacts {
                envelope_problems: invalid.clone(),
                ..clean()
            },
            DomainError::InvalidManifest {
                problems: invalid.clone(),
            },
        ),
        (
            "1 over 2: not ready beats not permitted",
            SyncFacts {
                gate: Gate::NotReady,
                access: Err(AccessError::NotPermitted),
                open: None,
                ..clean()
            },
            DomainError::NotReady,
        ),
        (
            "1 over 3: at capacity beats a conflict",
            SyncFacts {
                gate: Gate::AtCapacity,
                open: open(false),
                ..clean()
            },
            DomainError::AtCapacity,
        ),
        (
            "1 over 4: access unavailable beats invalid",
            SyncFacts {
                access: Err(AccessError::Unavailable),
                open: None,
                envelope_problems: invalid.clone(),
                ..clean()
            },
            DomainError::AccessUnavailable,
        ),
        (
            "1 over 4: storage unavailable beats invalid",
            SyncFacts {
                open: Some(Err(StorageError::Unavailable)),
                envelope_problems: invalid.clone(),
                ..clean()
            },
            DomainError::StorageUnavailable,
        ),
        (
            "2 over 3: not permitted beats a conflict, storage unread",
            SyncFacts {
                access: Err(AccessError::NotPermitted),
                open: open(false),
                ..clean()
            },
            DomainError::NotPermitted {
                action: Action::Seed,
            },
        ),
        (
            "2 over 1 (storage): a denied caller never sees a storage failure",
            SyncFacts {
                access: Err(AccessError::NotPermitted),
                open: Some(Err(StorageError::Unavailable)),
                ..clean()
            },
            DomainError::NotPermitted {
                action: Action::Seed,
            },
        ),
        (
            "2 over 4: not permitted beats invalid",
            SyncFacts {
                access: Err(AccessError::NotPermitted),
                open: None,
                envelope_problems: invalid.clone(),
                ..clean()
            },
            DomainError::NotPermitted {
                action: Action::Seed,
            },
        ),
        (
            "3 over 4: a conflict beats invalid",
            SyncFacts {
                open: open(false),
                envelope_problems: invalid,
                ..clean()
            },
            DomainError::Conflict { operation_id: BUSY },
        ),
    ];
    for (name, facts, expected) in cases {
        assert_eq!(sync_refusal(&facts), Some(expected), "{name}");
    }
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
            DomainError::AccessUnavailable,
            RequestCategory::TemporarilyUnavailable,
        ),
        (
            DomainError::StorageUnavailable,
            RequestCategory::TemporarilyUnavailable,
        ),
        (
            DomainError::NotPermitted {
                action: Action::Seed,
            },
            RequestCategory::NotPermitted,
        ),
        (
            DomainError::Conflict { operation_id: BUSY },
            RequestCategory::Conflict,
        ),
        (
            DomainError::InvalidManifest { problems: vec![] },
            RequestCategory::InvalidManifest,
        ),
        (DomainError::Defect, RequestCategory::InternalFailure),
    ];
    for (err, category) in cases {
        assert_eq!(category_of(&err), category, "{err:?}");
    }
}

fn item(key: &str, reason: UnavailableReasonCode) -> UnavailableItem {
    UnavailableItem {
        dependency: Dependency::SectionSchema,
        key: Some(key.to_owned()),
        reason,
    }
}

#[test]
fn usable_schemas_go_on_to_the_section_checks() {
    assert_eq!(schema_category(&[]), None);
    assert_eq!(
        schema_category(&[("a", SchemaState::Usable), ("b", SchemaState::Usable)]),
        None
    );
}

#[test]
fn rank_5_lists_every_unavailable_schema_in_plugin_order() {
    let states = [
        (
            "a",
            SchemaState::Unavailable(UnavailableReason::Unreachable),
        ),
        ("b", SchemaState::Usable),
        ("c", SchemaState::Unavailable(UnavailableReason::TimedOut)),
        ("d", SchemaState::Unavailable(UnavailableReason::Unusable)),
        ("e", SchemaState::Unavailable(UnavailableReason::NotSent)),
    ];
    assert_eq!(
        schema_category(&states),
        Some(SchemaStage {
            category: RequestCategory::TemporarilyUnavailable,
            unavailable: vec![
                item("a", UnavailableReasonCode::Unreachable),
                item("c", UnavailableReasonCode::TimedOut),
                item("d", UnavailableReasonCode::Unusable),
                item("e", UnavailableReasonCode::NotSent),
            ],
        })
    );
}

#[test]
fn rank_6_is_a_plugin_that_refuses_the_caller() {
    let states = [("a", SchemaState::Usable), ("b", SchemaState::NotPermitted)];
    assert_eq!(
        schema_category(&states),
        Some(SchemaStage {
            category: RequestCategory::NotPermitted,
            unavailable: vec![],
        })
    );
}

#[test]
fn rank_5_beats_rank_6_in_either_order() {
    for states in [
        [
            ("a", SchemaState::NotPermitted),
            ("b", SchemaState::Unavailable(UnavailableReason::TimedOut)),
        ],
        [
            ("a", SchemaState::Unavailable(UnavailableReason::TimedOut)),
            ("b", SchemaState::NotPermitted),
        ],
    ] {
        let stage = schema_category(&states).unwrap();
        assert_eq!(stage.category, RequestCategory::TemporarilyUnavailable);
        assert_eq!(stage.unavailable.len(), 1);
        assert_eq!(stage.unavailable[0].reason, UnavailableReasonCode::TimedOut);
    }
}

fn entry(outcome: SectionOutcome) -> SectionOutcomeEntry {
    SectionOutcomeEntry {
        key: "k".to_owned(),
        outcome,
        schema_version: None,
        detail: None,
        location: None,
    }
}

fn category(outcomes: &[SectionOutcome]) -> RequestCategory {
    let entries: Vec<_> = outcomes.iter().copied().map(entry).collect();
    dispatch_category(&entries)
}

#[test]
fn after_dispatch_the_categories_rank_in_design_order() {
    use SectionOutcome as S;
    let cases = [
        (
            "all succeeded",
            vec![S::Succeeded, S::Succeeded],
            RequestCategory::Succeeded,
        ),
        ("no plugin at all", vec![], RequestCategory::Succeeded),
        (
            "not permitted",
            vec![S::Succeeded, S::NotPermitted],
            RequestCategory::NotPermitted,
        ),
        (
            "rejected",
            vec![S::Rejected, S::Succeeded],
            RequestCategory::PluginRejected,
        ),
        (
            "internal failure",
            vec![S::InternalFailure],
            RequestCategory::InternalFailure,
        ),
        (
            "timed out",
            vec![S::Succeeded, S::TimedOut],
            RequestCategory::InternalFailure,
        ),
        (
            "not sent",
            vec![S::NotSent],
            RequestCategory::InternalFailure,
        ),
        (
            "not dispatched",
            vec![S::NotDispatched],
            RequestCategory::InternalFailure,
        ),
        (
            "not permitted beats rejected and internal",
            vec![S::InternalFailure, S::Rejected, S::NotPermitted],
            RequestCategory::NotPermitted,
        ),
        (
            "rejected beats internal failure of every kind",
            vec![
                S::InternalFailure,
                S::TimedOut,
                S::NotSent,
                S::NotDispatched,
                S::Rejected,
            ],
            RequestCategory::PluginRejected,
        ),
        (
            "internal failure beats succeeded",
            vec![S::Succeeded, S::NotDispatched, S::Succeeded],
            RequestCategory::InternalFailure,
        ),
    ];
    for (name, outcomes, expected) in cases {
        assert_eq!(category(&outcomes), expected, "{name}");
    }
}
