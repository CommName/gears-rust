//! After-dispatch ranking and the last-result derivation (DESIGN §3.1 "Application registry
//! entry", §3.2 Request Orchestrator "After dispatch" table).
//!
//! Pure functions over the section outcomes of one request; the request task calls them and
//! owns no ranking of its own.

use application_manager_sdk::models::{OutcomeCategory, SectionOutcome};

use crate::domain::registry::LastResult;

/// The after-dispatch category of a request.
///
/// In order: any `not_permitted` section gives not permitted; else any `rejected` gives plugin
/// rejected; else any `internal_failure`, `timed_out` or `not_sent` section, or a failed
/// registry write or delete (`registry_failed`), gives internal failure; else succeeded.
/// `not_dispatched` ranks nowhere: only the supervisor reports it, and it sets the category
/// itself.
#[must_use]
pub fn after_dispatch_category(
    sections: &[SectionOutcome],
    registry_failed: bool,
) -> OutcomeCategory {
    if sections.contains(&SectionOutcome::NotPermitted) {
        OutcomeCategory::NotPermitted
    } else if sections.contains(&SectionOutcome::Rejected) {
        OutcomeCategory::PluginRejected
    } else if registry_failed
        || sections.iter().any(|outcome| {
            matches!(
                outcome,
                SectionOutcome::InternalFailure
                    | SectionOutcome::TimedOut
                    | SectionOutcome::NotSent
            )
        })
    {
        OutcomeCategory::InternalFailure
    } else {
        OutcomeCategory::Succeeded
    }
}

/// Whether at least one section `succeeded` and at least one other outcome is not `succeeded`
/// (the report's `partial`).
#[must_use]
pub fn is_partial(sections: &[SectionOutcome]) -> bool {
    sections.contains(&SectionOutcome::Succeeded)
        && sections
            .iter()
            .any(|outcome| *outcome != SectionOutcome::Succeeded)
}

/// The registry entry's last result of a seed (DESIGN §3.1): every section `succeeded`, an empty
/// set included, gives succeeded; some `succeeded` and some other gives partially failed; none
/// `succeeded` gives the after-dispatch winner of the sections alone (not permitted, plugin
/// rejected or internal failure). The registry write is not part of it: the entry is what is
/// written.
#[must_use]
pub fn derive_last_result(sections: &[SectionOutcome]) -> LastResult {
    if sections
        .iter()
        .all(|outcome| *outcome == SectionOutcome::Succeeded)
    {
        LastResult::Succeeded
    } else if is_partial(sections) {
        LastResult::PartiallyFailed
    } else {
        match after_dispatch_category(sections, false) {
            OutcomeCategory::NotPermitted => LastResult::NotPermitted,
            OutcomeCategory::PluginRejected => LastResult::PluginRejected,
            _ => LastResult::InternalFailure,
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "dispatch_tests.rs"]
mod dispatch_tests;
