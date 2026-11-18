#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use application_manager_sdk::ReportApplication;

use super::*;

const SECS: fn(u64) -> Duration = Duration::from_secs;

fn entry(key: &str, outcome: Option<SectionOutcome>) -> SectionOutcomeEntry {
    SectionOutcomeEntry {
        key: key.to_owned(),
        section_type: None,
        outcome,
        detail: None,
        location: None,
    }
}

#[test]
fn the_window_covers_each_call_its_write_and_two_more_writes() {
    // 3 x (10s + 2s) + 2 x 2s + 30s
    assert_eq!(run_window(3, SECS(10), SECS(2), SECS(30)), Ok(SECS(70)));
}

#[test]
fn an_empty_plugin_set_still_owns_mark_running_and_the_final_write() {
    assert_eq!(run_window(0, SECS(10), SECS(2), SECS(30)), Ok(SECS(34)));
}

#[test]
fn an_overflowing_window_is_an_error() {
    assert_eq!(
        run_window(2, Duration::MAX, SECS(1), SECS(1)),
        Err(RunWindowOverflow)
    );
    assert_eq!(
        run_window(0, SECS(1), Duration::MAX, SECS(1)),
        Err(RunWindowOverflow)
    );
    assert_eq!(
        run_window(0, SECS(1), SECS(1), Duration::MAX),
        Err(RunWindowOverflow)
    );
    assert_eq!(
        run_window(usize::MAX, SECS(1), SECS(1), SECS(1)),
        Err(RunWindowOverflow)
    );
}

#[test]
fn the_category_follows_the_outcome_table_precedence() {
    use SectionOutcome::{
        Lost, NotDispatched, NotPermitted, NotSent, Rejected, Succeeded, TimedOut,
    };
    let cases: Vec<(Vec<Option<SectionOutcome>>, RequestCategory)> = vec![
        (vec![], RequestCategory::Succeeded),
        (
            vec![Some(Succeeded), Some(Succeeded)],
            RequestCategory::Succeeded,
        ),
        (
            vec![Some(TimedOut), Some(Rejected), Some(NotPermitted)],
            RequestCategory::NotPermitted,
        ),
        (
            vec![Some(Lost), Some(Rejected)],
            RequestCategory::PluginRejected,
        ),
        (
            vec![Some(Succeeded), Some(NotSent)],
            RequestCategory::InternalFailure,
        ),
        (
            vec![Some(Succeeded), Some(NotDispatched)],
            RequestCategory::InternalFailure,
        ),
        (
            vec![Some(Succeeded), None],
            RequestCategory::InternalFailure,
        ),
    ];
    for (outcomes, expected) in cases {
        let sections: Vec<_> = outcomes
            .iter()
            .enumerate()
            .map(|(i, o)| entry(&format!("k{i}"), *o))
            .collect();
        assert_eq!(completed_category(&sections), expected, "{outcomes:?}");
    }
}

#[test]
fn an_expiry_completion_is_never_succeeded() {
    assert_eq!(expiry_category(&[]), RequestCategory::InternalFailure);
    assert_eq!(
        expiry_category(&[entry("a", Some(SectionOutcome::Succeeded))]),
        RequestCategory::InternalFailure
    );
    assert_eq!(
        expiry_category(&[
            entry("a", Some(SectionOutcome::Rejected)),
            entry("b", Some(SectionOutcome::Lost))
        ]),
        RequestCategory::PluginRejected
    );
}

fn report_with(outcomes: &[Option<SectionOutcome>]) -> OutcomeReport {
    OutcomeReport {
        operation_id: Uuid::from_u128(1),
        action: OperationAction::Seed,
        category: None,
        application: ReportApplication {
            id: "app".to_owned(),
            version: Some("1".to_owned()),
        },
        sections: outcomes
            .iter()
            .enumerate()
            .map(|(i, o)| entry(&format!("k{i}"), *o))
            .collect(),
        problems: vec![],
        unavailable: vec![],
    }
}

fn outcomes(report: &OutcomeReport) -> Vec<Option<SectionOutcome>> {
    report.sections.iter().map(|s| s.outcome).collect()
}

#[test]
fn a_running_operation_loses_its_first_open_section() {
    let mut report = report_with(&[Some(SectionOutcome::Succeeded), None, None]);
    complete_open_sections(&mut report, true);
    assert_eq!(
        outcomes(&report),
        [
            Some(SectionOutcome::Succeeded),
            Some(SectionOutcome::Lost),
            Some(SectionOutcome::NotDispatched)
        ]
    );
}

#[test]
fn an_accepted_operation_dispatched_nothing() {
    let mut report = report_with(&[None, None]);
    complete_open_sections(&mut report, false);
    assert_eq!(
        outcomes(&report),
        [
            Some(SectionOutcome::NotDispatched),
            Some(SectionOutcome::NotDispatched)
        ]
    );
}
