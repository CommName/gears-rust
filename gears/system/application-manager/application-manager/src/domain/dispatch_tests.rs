//! Unit tests for the after-dispatch ranking and the last-result derivation (P7, DESIGN §3.1
//! "Application registry entry", §3.2 Request Orchestrator table "After dispatch", §4.8 "Unit":
//! "the precedence table and after-dispatch ranking as table-driven cases, ... the last-result
//! derivation").
//!
//! Planned API in `crate::domain::dispatch` (new module, pure functions over section outcomes;
//! the orchestrator calls them and owns no ranking of its own):
//!
//! ```text
//! /// The after-dispatch category: any `not_permitted` gives NotPermitted; else any `rejected`
//! /// gives PluginRejected; else any `internal_failure`, `timed_out` or `not_sent`, or a failed
//! /// registry write or delete (`registry_failed`), gives InternalFailure; else Succeeded.
//! /// `not_dispatched` ranks nowhere (a supervisor-reported section; the supervisor sets the
//! /// category itself).
//! pub fn after_dispatch_category(sections: &[SectionOutcome], registry_failed: bool)
//!     -> OutcomeCategory;
//! /// At least one `succeeded` and at least one other outcome (the report's `partial`).
//! pub fn is_partial(sections: &[SectionOutcome]) -> bool;
//! /// The registry entry's last result of a seed (DESIGN §3.1): all `succeeded`, an empty set
//! /// included: Succeeded; some `succeeded` and some other: PartiallyFailed; none `succeeded`:
//! /// the after-dispatch winner of the sections alone (NotPermitted, PluginRejected,
//! /// InternalFailure). The write result cannot be part of it: the entry is what is written.
//! pub fn derive_last_result(sections: &[SectionOutcome]) -> LastResult;
//! ```
#![allow(clippy::too_many_lines)]

use application_manager_sdk::models::{OutcomeCategory, SectionOutcome};

use crate::domain::dispatch::{after_dispatch_category, derive_last_result, is_partial};
use crate::domain::registry::LastResult;

use OutcomeCategory as C;
use SectionOutcome as S;

const OUTCOMES: [SectionOutcome; 6] = [
    S::Succeeded,
    S::Rejected,
    S::NotPermitted,
    S::InternalFailure,
    S::TimedOut,
    S::NotSent,
];

// DESIGN §3.2 "After dispatch" table, one row per case: rank 1 not_permitted, rank 2 rejected,
// rank 3 internal_failure / timed_out / not_sent / a failed registry write, none: succeeded.
#[test]
fn the_after_dispatch_table_decides_each_row() {
    let cases: [(&[SectionOutcome], bool, OutcomeCategory); 14] = [
        (&[], false, C::Succeeded),
        (&[S::Succeeded], false, C::Succeeded),
        (&[S::Succeeded, S::Succeeded], false, C::Succeeded),
        (&[S::NotPermitted], false, C::NotPermitted),
        (&[S::Rejected], false, C::PluginRejected),
        (&[S::InternalFailure], false, C::InternalFailure),
        (&[S::TimedOut], false, C::InternalFailure),
        (&[S::NotSent], false, C::InternalFailure),
        // The registry write or delete failing or timing out is an internal failure (rank 3).
        (&[], true, C::InternalFailure),
        (&[S::Succeeded], true, C::InternalFailure),
        // ... which a higher rank outranks.
        (&[S::Rejected], true, C::PluginRejected),
        (&[S::NotPermitted], true, C::NotPermitted),
        (&[S::Rejected, S::NotPermitted], true, C::NotPermitted),
        (
            &[S::TimedOut, S::NotSent, S::InternalFailure],
            false,
            C::InternalFailure,
        ),
    ];

    for (sections, registry_failed, expected) in cases {
        assert_eq!(
            after_dispatch_category(sections, registry_failed),
            expected,
            "{sections:?}, registry failed: {registry_failed}"
        );
    }
}

// The worst outcome wins whatever the order and whatever else is there: not_permitted over
// rejected over the internal failures over success.
#[test]
fn the_worst_outcome_wins_in_every_order_and_mixture() {
    let rank = |s: SectionOutcome| match s {
        S::NotPermitted => 3,
        S::Rejected => 2,
        S::InternalFailure | S::TimedOut | S::NotSent => 1,
        _ => 0,
    };
    for a in OUTCOMES {
        for b in OUTCOMES {
            for c in OUTCOMES {
                for registry_failed in [false, true] {
                    let worst = [a, b, c].into_iter().map(rank).max().unwrap_or(0);
                    let worst = if registry_failed { worst.max(1) } else { worst };
                    let expected = match worst {
                        3 => C::NotPermitted,
                        2 => C::PluginRejected,
                        1 => C::InternalFailure,
                        _ => C::Succeeded,
                    };
                    for order in [[a, b, c], [c, b, a], [b, c, a]] {
                        assert_eq!(
                            after_dispatch_category(&order, registry_failed),
                            expected,
                            "{order:?}, registry failed: {registry_failed}"
                        );
                    }
                }
            }
        }
    }
}

// `partial` is true when at least one section succeeded and at least one did not (DESIGN §3.1
// outcome report member `partial`; it matches the registry's `partially_failed`).
#[test]
fn partial_means_some_succeeded_and_some_did_not() {
    assert!(!is_partial(&[]));
    assert!(!is_partial(&[S::Succeeded]));
    assert!(!is_partial(&[S::Succeeded, S::Succeeded]));
    assert!(!is_partial(&[S::Rejected]));
    assert!(!is_partial(&[S::TimedOut, S::NotSent]));
    assert!(is_partial(&[S::Succeeded, S::Rejected]));
    assert!(is_partial(&[S::TimedOut, S::Succeeded]));
    assert!(is_partial(&[S::Succeeded, S::Succeeded, S::NotSent]));
    assert!(is_partial(&[S::NotPermitted, S::Succeeded, S::Rejected]));
}

// DESIGN §3.1: every section succeeded, an empty key set included: `succeeded`; at least one
// succeeded and one other: `partially_failed`; none succeeded: the after-dispatch winner.
#[test]
fn the_last_result_follows_the_three_derivation_rules() {
    let cases: [(&[SectionOutcome], LastResult); 12] = [
        (&[], LastResult::Succeeded),
        (&[S::Succeeded], LastResult::Succeeded),
        (&[S::Succeeded, S::Succeeded], LastResult::Succeeded),
        (&[S::Succeeded, S::Rejected], LastResult::PartiallyFailed),
        (&[S::TimedOut, S::Succeeded], LastResult::PartiallyFailed),
        (
            &[S::NotPermitted, S::Succeeded, S::Rejected],
            LastResult::PartiallyFailed,
        ),
        (&[S::Rejected], LastResult::PluginRejected),
        (&[S::NotPermitted], LastResult::NotPermitted),
        (&[S::InternalFailure], LastResult::InternalFailure),
        (&[S::TimedOut], LastResult::InternalFailure),
        (&[S::NotSent], LastResult::InternalFailure),
        // No success: the winner among the failures (not_permitted over rejected over internal).
        (
            &[S::Rejected, S::NotPermitted, S::TimedOut],
            LastResult::NotPermitted,
        ),
    ];

    for (sections, expected) in cases {
        assert_eq!(derive_last_result(sections), expected, "{sections:?}");
    }
    assert_eq!(
        derive_last_result(&[S::TimedOut, S::Rejected]),
        LastResult::PluginRejected,
        "rejected outranks the internal failures"
    );
}

// The last result is derived from the sections alone: the registry write that stores it is not
// part of it, so the same sections give the same result whatever the write does later. (The
// category is a different matter: `after_dispatch_category` takes the write into account.)
#[test]
fn the_last_result_and_the_category_agree_when_nothing_succeeded_and_the_write_is_good() {
    for a in OUTCOMES {
        for b in OUTCOMES {
            let sections = [a, b];
            if sections.contains(&S::Succeeded) {
                continue;
            }
            let expected = match after_dispatch_category(&sections, false) {
                C::NotPermitted => LastResult::NotPermitted,
                C::PluginRejected => LastResult::PluginRejected,
                C::InternalFailure => LastResult::InternalFailure,
                other => panic!("{sections:?}: unexpected category {other:?}"),
            };
            assert_eq!(derive_last_result(&sections), expected, "{sections:?}");
        }
    }
}

// `not_dispatched` is the supervisor's report of a section the panicked request task never
// reached: it ranks nowhere, so it never makes a category, and as a section that did not succeed
// it still makes a success `partial`.
#[test]
fn not_dispatched_ranks_nowhere_and_counts_as_not_succeeded_for_partial() {
    assert_eq!(
        after_dispatch_category(&[S::Succeeded, S::NotDispatched], false),
        C::Succeeded
    );
    assert_eq!(
        after_dispatch_category(&[S::NotDispatched; 3], false),
        C::Succeeded
    );
    assert_eq!(
        after_dispatch_category(&[S::NotDispatched, S::Rejected], false),
        C::PluginRejected
    );
    assert_eq!(
        after_dispatch_category(&[S::NotDispatched], true),
        C::InternalFailure
    );
    assert!(is_partial(&[S::Succeeded, S::NotDispatched]));
    assert!(is_partial(&[
        S::NotDispatched,
        S::Succeeded,
        S::NotDispatched
    ]));
    assert!(!is_partial(&[S::NotDispatched; 3]));
    assert!(!is_partial(&[S::NotDispatched, S::Rejected]));
}
