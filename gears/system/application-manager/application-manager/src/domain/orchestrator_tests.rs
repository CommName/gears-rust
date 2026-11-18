#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::{RequestCategory, SectionOutcome};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use super::*;
use crate::domain::history::HistoryKind;
use crate::domain::operation::{NewOperation, Operation, OperationStatus};
use crate::domain::plugin_set::PluginSet;
use crate::infra::call_guard::CallGuard;
use crate::test_support::{
    Answer, FakePlugin, Hook, HookedRepo, RecordedSignals, allowed_callers, platform_ctx,
    section_plugin,
};

const APP: &str = "acme.app";

fn settings() -> OrchestratorSettings {
    OrchestratorSettings {
        plugin_call_timeout: Duration::from_secs(5),
        storage_call_timeout: Duration::from_secs(1),
        run_window_margin: Duration::from_secs(30),
        max_envelope_text_length: 64,
    }
}

struct Harness {
    orchestrator: Arc<RequestOrchestrator>,
    repo: Arc<HookedRepo>,
    plugins: Vec<Arc<FakePlugin>>,
    signals: Arc<RecordedSignals>,
}

/// A started orchestrator whose plugin set is one plugin per answer, keyed
/// `a`, `b`, … in that priority order.
async fn harness_with(answers: &[Answer], settings: OrchestratorSettings) -> Harness {
    let repo = HookedRepo::new().await;
    let plugins: Vec<_> = answers.iter().cloned().map(FakePlugin::answering).collect();
    let startup = Arc::new(StartupState::default());
    let keys = ["a", "b", "c", "d"];
    startup
        .publish(PluginSet::new(
            plugins
                .iter()
                .enumerate()
                .map(|(i, p)| section_plugin(keys[i], i16::try_from(i).unwrap(), p))
                .collect(),
        ))
        .unwrap();
    let guard = Arc::new(CallGuard::new(settings.plugin_call_timeout, 4));
    let signals = RecordedSignals::new();
    let orchestrator = Arc::new(
        RequestOrchestrator::new(
            Arc::new(allowed_callers(
                "app-seeder",
                vec![CallerAction::Seed, CallerAction::Uninstall],
            )),
            Arc::clone(&repo) as Arc<dyn OperationRepository>,
            Arc::new(SectionPluginRouter::new(guard)),
            Arc::new(SchemaValidator::new(settings.max_envelope_text_length).unwrap()),
            startup,
            settings,
        )
        .with_signals(Arc::clone(&signals) as Arc<dyn Signals>),
    );
    Harness {
        orchestrator,
        repo,
        plugins,
        signals,
    }
}

async fn harness(answers: &[Answer]) -> Harness {
    harness_with(answers, settings()).await
}

fn permit() -> tokio::sync::OwnedSemaphorePermit {
    Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap()
}

/// A seed or an uninstall, so one harness drives both entry points.
#[derive(Debug, Clone)]
enum Call {
    Seed(Value),
    Uninstall(String),
}

impl Call {
    async fn send(
        self,
        orchestrator: &Arc<RequestOrchestrator>,
        ctx: PlatformSecurityContext,
        receipt: Receipt,
        admission: Result<tokio::sync::OwnedSemaphorePermit, DomainError>,
    ) -> Result<Submitted, DomainError> {
        let orchestrator = Arc::clone(orchestrator);
        match self {
            Self::Seed(manifest) => orchestrator.seed(ctx, manifest, receipt, admission).await,
            Self::Uninstall(id) => orchestrator.uninstall(ctx, id, receipt, admission).await,
        }
    }
}

fn manifest(id: &str, sections: &Value) -> Call {
    Call::Seed(json!({ "application": { "id": id, "version": "1.0.0" }, "sections": sections }))
}

fn seed_all(h: &Harness) -> Call {
    let keys = ["a", "b", "c", "d"];
    let sections: serde_json::Map<String, Value> = (0..h.plugins.len())
        .map(|i| (keys[i].to_owned(), json!({ "n": i })))
        .collect();
    manifest(APP, &Value::Object(sections))
}

impl Harness {
    async fn submit_as(
        &self,
        caller: &str,
        input: Call,
    ) -> (Receipt, Result<Submitted, DomainError>) {
        let receipt = Receipt::new(OffsetDateTime::now_utc());
        let answer = input
            .send(
                &self.orchestrator,
                platform_ctx(caller),
                receipt,
                Ok(permit()),
            )
            .await;
        (receipt, answer)
    }

    async fn submit(&self, input: Call) -> (Receipt, Result<Submitted, DomainError>) {
        self.submit_as("app-seeder", input).await
    }

    /// Submit, expect acceptance, and return the completed operation.
    async fn run(&self, input: Call) -> Operation {
        let (receipt, answer) = self.submit(input).await;
        assert_eq!(
            answer,
            Ok(Submitted {
                operation_id: receipt.request_id
            })
        );
        self.repo.completed(receipt.request_id).await
    }

    fn calls(&self) -> Vec<usize> {
        self.plugins.iter().map(|p| p.calls()).collect()
    }

    async fn kinds(&self, id: Uuid) -> Vec<HistoryKind> {
        self.repo.events(id).await.iter().map(|e| e.kind).collect()
    }

    /// The single refused event of a refused request.
    async fn refused(&self, receipt: &Receipt) -> HistoryEvent {
        let events = self.repo.events(receipt.request_id).await;
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0].kind, HistoryKind::Refused);
        events.into_iter().next().unwrap()
    }
}

fn outcomes(op: &Operation) -> Vec<Option<SectionOutcome>> {
    op.report.sections.iter().map(|s| s.outcome).collect()
}

// ---- seed ----

#[tokio::test]
async fn a_seed_is_accepted_dispatched_in_priority_order_and_finished() {
    let h = harness(&[Answer::Ok, Answer::Ok]).await;
    let op = h.run(seed_all(&h)).await;
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert_eq!(
        outcomes(&op),
        [
            Some(SectionOutcome::Succeeded),
            Some(SectionOutcome::Succeeded)
        ]
    );
    assert_eq!(
        op.report.sections[0].section_type.as_deref(),
        Some("gts.cf.core.application_manager.section.v1~a.v1~")
    );
    assert_eq!(h.calls(), [1, 1]);
    let seen = h.plugins[0].seen.lock().unwrap().clone();
    assert_eq!(seen[0].0, "apply");
    assert_eq!(seen[0].1, op.operation_id);
    assert_eq!(
        h.kinds(op.operation_id).await,
        [HistoryKind::Accepted, HistoryKind::Finished],
        "exactly the accepted and finished events"
    );
    let records = h.repo.applications_page(None, None, 10).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].version, "1.0.0");
}

#[tokio::test]
async fn the_accepted_event_lists_the_keys_of_the_key_set_and_counts_the_others() {
    let h = harness(&[Answer::Ok]).await;
    let (receipt, answer) = h
        .submit(manifest(APP, &json!({ "a": {}, "zz": {}, "yy": {} })))
        .await;
    assert!(answer.is_ok());
    h.repo.completed(receipt.request_id).await;
    let events = h.repo.events(receipt.request_id).await;
    assert_eq!(
        events[0].content["section_keys"],
        json!({ "keys": ["a"], "other_keys": 2 })
    );
}

#[tokio::test]
async fn dispatch_continues_after_each_failure_kind_and_ranks_the_outcomes() {
    let h = harness(&[
        Answer::Rejected,
        Answer::Internal,
        Answer::NotPermitted,
        Answer::Ok,
    ])
    .await;
    // A valid seed carries every key; unknown keys are a section problem.
    let op = h.run(seed_all(&h)).await;
    assert_eq!(h.calls(), [1, 1, 1, 1]);
    assert_eq!(
        outcomes(&op),
        [
            Some(SectionOutcome::Rejected),
            Some(SectionOutcome::InternalFailure),
            Some(SectionOutcome::NotPermitted),
            Some(SectionOutcome::Succeeded)
        ]
    );
    assert_eq!(op.category, Some(RequestCategory::NotPermitted));
    assert_eq!(op.report.sections[0].detail.as_deref(), Some("bad"));
    assert_eq!(op.report.sections[0].location.as_deref(), Some("/a"));
}

#[tokio::test]
async fn a_section_problem_completes_from_accepted_without_a_call_or_a_record() {
    let h = harness(&[Answer::Ok, Answer::Ok]).await;
    let op = h
        .run(manifest(APP, &json!({ "a": {}, "unknown": {} })))
        .await;
    assert_eq!(op.category, Some(RequestCategory::InvalidManifest));
    assert_eq!(op.started_at, None, "never marked running");
    let rules: Vec<_> = op.report.problems.iter().map(|p| p.rule.as_str()).collect();
    assert_eq!(rules, ["missing_section", "unknown_section"]);
    assert_eq!(h.calls(), [0, 0]);
    assert_eq!(h.repo.applications_page(None, None, 10).await.unwrap(), []);
}

#[tokio::test]
async fn a_seed_with_an_empty_plugin_set_succeeds() {
    let h = harness(&[]).await;
    let op = h.run(manifest(APP, &json!({}))).await;
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
}

// ---- uninstall ----

#[tokio::test]
async fn an_uninstall_removes_in_priority_order_and_deletes_the_record() {
    let h = harness(&[Answer::Ok, Answer::Ok]).await;
    h.run(seed_all(&h)).await;
    let op = h.run(Call::Uninstall(APP.to_owned())).await;
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert_eq!(op.report.sections[0].section_type, None);
    for plugin in &h.plugins {
        let seen = plugin.seen.lock().unwrap().clone();
        assert_eq!(seen[1].0, "remove");
    }
    assert_eq!(h.repo.applications_page(None, None, 10).await.unwrap(), []);
}

#[tokio::test]
async fn bad_uninstall_ids_are_rank_4() {
    let h = harness(&[Answer::Ok]).await;
    let cases: Vec<(Call, (&str, &str))> = vec![
        (
            Call::Uninstall(String::new()),
            ("/application_id", "minLength"),
        ),
        (
            Call::Uninstall("a".repeat(65)),
            ("/application_id", "maxLength"),
        ),
        (
            Call::Uninstall("a\u{7}b".to_owned()),
            ("/application_id", "pattern"),
        ),
    ];
    for (input, (location, rule)) in cases {
        let (receipt, answer) = h.submit(input).await;
        let Err(DomainError::InvalidManifest { problems }) = answer else {
            panic!("{answer:?}");
        };
        assert_eq!(
            (problems[0].location.as_str(), problems[0].rule.as_str()),
            (location, rule)
        );
        let refused = h.refused(&receipt).await;
        assert_eq!(
            refused.content["result"]["category"],
            json!("invalid_manifest")
        );
    }
    assert_eq!(h.calls(), [0]);
}

// ---- synchronous refusals ----

#[tokio::test]
async fn an_envelope_problem_is_rank_4_with_a_refused_event_and_no_operation() {
    let h = harness(&[Answer::Ok]).await;
    let (receipt, answer) = h
        .submit(manifest(&"x".repeat(65), &json!({ "a": {} })))
        .await;
    let Err(DomainError::InvalidManifest { problems }) = answer else {
        panic!("{answer:?}");
    };
    assert_eq!(problems[0].rule, "maxLength");
    let refused = h.refused(&receipt).await;
    assert_eq!(refused.content["result"]["rules"], json!(["maxLength"]));
    assert_eq!(
        refused.content["application"]["id_bytes"],
        json!(65),
        "an id over the limits is recorded as length and digest"
    );
    assert_eq!(h.repo.read_operation(receipt.request_id).await, Ok(None));
    assert_eq!(h.calls(), [0]);
}

#[tokio::test]
async fn a_caller_not_allowed_is_rank_2_with_no_storage_read() {
    let h = harness(&[Answer::Ok]).await;
    let (receipt, answer) = h.submit_as("someone-else", seed_all(&h)).await;
    assert_eq!(
        answer,
        Err(DomainError::NotPermitted {
            action: CallerAction::Seed
        })
    );
    assert_eq!(h.repo.calls("open_operation"), 0);
    let refused = h.refused(&receipt).await;
    assert_eq!(
        refused.content["result"]["category"],
        json!("not_permitted")
    );
    assert_eq!(h.calls(), [0]);
}

#[tokio::test]
async fn rank_2_beats_rank_4() {
    let h = harness(&[Answer::Ok]).await;
    let (_, answer) = h
        .submit_as("someone-else", Call::Uninstall(String::new()))
        .await;
    assert_eq!(
        answer,
        Err(DomainError::NotPermitted {
            action: CallerAction::Uninstall
        })
    );
}

#[tokio::test]
async fn storage_that_cannot_answer_is_rank_1() {
    let h = harness(&[Answer::Ok]).await;
    h.repo.hook("open_operation", Hook::Fail);
    let (receipt, answer) = h.submit(seed_all(&h)).await;
    assert_eq!(answer, Err(DomainError::StorageUnavailable));
    let refused = h.refused(&receipt).await;
    assert_eq!(
        refused.content["result"]["unavailable"],
        json!([{ "dependency": "operation_storage", "reason": "unreachable" }])
    );
}

#[tokio::test]
async fn an_open_operation_is_a_conflict_naming_it() {
    let h = harness(&[Answer::Ok]).await;
    let busy = open_operation(&h, Duration::from_secs(3600)).await;
    let (receipt, answer) = h.submit(seed_all(&h)).await;
    assert_eq!(answer, Err(DomainError::Conflict { operation_id: busy }));
    let refused = h.refused(&receipt).await;
    assert_eq!(
        refused.content["result"]["busy_operation_id"],
        json!(busy.to_string())
    );
    // Another application is not blocked.
    let other = h.run(manifest("other.app", &json!({ "a": {} }))).await;
    assert_eq!(other.category, Some(RequestCategory::Succeeded));
}

/// An operation of `APP` accepted behind the orchestrator's back.
async fn open_operation(h: &Harness, window: Duration) -> Uuid {
    let id = Uuid::now_v7();
    h.repo
        .accept(
            NewOperation {
                operation_id: id,
                application_id: APP.to_owned(),
                action: OperationAction::Seed,
                run_window: window,
                finished_event_id: Uuid::now_v7(),
                report: initial_report(id, &h.orchestrator.parse_uninstall(APP.to_owned())),
            },
            HistoryEvent {
                event_id: Uuid::now_v7(),
                kind: HistoryKind::Accepted,
                correlation_id: id,
                action: OperationAction::Seed,
                written_at: OffsetDateTime::now_utc(),
                content: json!({}),
            },
        )
        .await
        .unwrap();
    id
}

#[tokio::test]
async fn an_expired_open_operation_is_completed_by_the_next_acceptance() {
    let h = harness(&[Answer::Ok]).await;
    let expired = open_operation(&h, Duration::ZERO).await;
    let op = h.run(seed_all(&h)).await;
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    let old = h.repo.read_operation(expired).await.unwrap().unwrap();
    assert_eq!(old.status, OperationStatus::Completed);
    assert_eq!(old.category, Some(RequestCategory::InternalFailure));
}

#[tokio::test]
async fn a_lost_race_is_a_conflict_naming_the_winner() {
    let h = harness(&[Answer::Ok]).await;
    let winner = open_operation(&h, Duration::from_secs(3600)).await;
    h.repo.hook("open_operation", Hook::NoneOpen);
    let (receipt, answer) = h.submit(seed_all(&h)).await;
    assert_eq!(
        answer,
        Err(DomainError::Conflict {
            operation_id: winner
        })
    );
    let refused = h.refused(&receipt).await;
    assert_eq!(
        refused.content["result"]["busy_operation_id"],
        json!(winner.to_string())
    );
}

#[tokio::test]
async fn a_lost_race_whose_winner_cannot_be_read_is_unavailable_and_records_nothing() {
    let h = harness(&[Answer::Ok]).await;
    open_operation(&h, Duration::from_secs(3600)).await;
    h.repo.hook("open_operation", Hook::NoneOpen);
    h.repo.hook("refuse_after_lost_race", Hook::Fail);
    let (receipt, answer) = h.submit(seed_all(&h)).await;
    assert_eq!(answer, Err(DomainError::StorageUnavailable));
    assert_eq!(h.repo.events(receipt.request_id).await, []);
}

#[tokio::test]
async fn an_acceptance_that_cannot_commit_is_unavailable_with_a_refused_event() {
    let h = harness(&[Answer::Ok]).await;
    h.repo.hook("accept", Hook::Fail);
    let (receipt, answer) = h.submit(seed_all(&h)).await;
    assert_eq!(answer, Err(DomainError::StorageUnavailable));
    let refused = h.refused(&receipt).await;
    assert_eq!(
        refused.content["result"]["category"],
        json!("temporarily_unavailable")
    );
    assert_eq!(h.calls(), [0]);
}

#[tokio::test]
async fn a_refusal_keeps_its_category_when_its_event_cannot_be_written() {
    let h = harness(&[Answer::Ok]).await;
    h.repo.hook("record_refused", Hook::Fail);
    let (_, answer) = h.submit_as("someone-else", seed_all(&h)).await;
    assert_eq!(
        answer,
        Err(DomainError::NotPermitted {
            action: CallerAction::Seed
        })
    );
}

#[tokio::test]
async fn a_gear_defect_before_the_answer_is_a_defect_with_a_refused_event() {
    let h = harness(&[Answer::Ok]).await;
    h.repo.hook("open_operation", Hook::Panic);
    let (receipt, answer) = h.submit(seed_all(&h)).await;
    assert_eq!(answer, Err(DomainError::Defect));
    // The supervisor writes the event after the answer.
    for _ in 0..200 {
        if !h.repo.events(receipt.request_id).await.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let refused = h.refused(&receipt).await;
    assert_eq!(
        refused.content["result"]["category"],
        json!("internal_failure")
    );
}

#[tokio::test]
async fn the_gate_refusal_event_names_the_gear_as_unavailable() {
    let h = harness(&[Answer::Ok]).await;
    let receipt = Receipt::new(OffsetDateTime::now_utc());
    let answer = seed_all(&h)
        .send(
            &h.orchestrator,
            platform_ctx("app-seeder"),
            receipt,
            Err(DomainError::AtCapacity),
        )
        .await;
    assert_eq!(answer, Err(DomainError::AtCapacity));
    assert_eq!(h.calls(), [0]);
    let refused = h.refused(&receipt).await;
    assert_eq!(
        refused.content["result"]["unavailable"],
        json!([{ "dependency": "application_manager", "reason": "at_capacity" }])
    );
}

// ---- the request task ----

#[tokio::test]
async fn the_request_task_completes_when_the_caller_drops_the_future() {
    let h = harness(&[Answer::Ok]).await;
    let receipt = Receipt::new(OffsetDateTime::now_utc());
    let submit = seed_all(&h).send(
        &h.orchestrator,
        platform_ctx("app-seeder"),
        receipt,
        Ok(permit()),
    );
    // Dropped before it can answer.
    assert!(tokio::time::timeout(Duration::ZERO, submit).await.is_err());
    let op = h.repo.completed(receipt.request_id).await;
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
}

#[tokio::test]
async fn a_call_that_no_longer_fits_is_not_dispatched() {
    let short = OrchestratorSettings {
        plugin_call_timeout: Duration::from_millis(100),
        storage_call_timeout: Duration::from_millis(100),
        run_window_margin: Duration::ZERO,
        ..settings()
    };
    let h = harness_with(&[Answer::Ok, Answer::Ok], short).await;
    // The first outcome write takes most of the window.
    h.repo.hook(
        "record_section_outcome",
        Hook::Sleep(Duration::from_millis(400)),
    );
    let op = h.run(seed_all(&h)).await;
    assert_eq!(
        outcomes(&op),
        [
            Some(SectionOutcome::Succeeded),
            Some(SectionOutcome::NotDispatched)
        ]
    );
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    assert_eq!(h.calls(), [1, 0]);
}

#[tokio::test]
async fn a_failed_outcome_write_stops_the_dispatch_and_keeps_the_outcome() {
    let h = harness(&[Answer::Ok, Answer::Ok]).await;
    h.repo.hook("record_section_outcome", Hook::Fail);
    let op = h.run(seed_all(&h)).await;
    assert_eq!(
        outcomes(&op),
        [
            Some(SectionOutcome::Succeeded),
            Some(SectionOutcome::NotDispatched)
        ],
        "the unwritten outcome reaches the final write"
    );
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    assert_eq!(h.calls(), [1, 0]);
}

#[tokio::test]
async fn a_request_task_failing_after_acceptance_gets_the_supervisor_completion() {
    let h = harness(&[Answer::Ok, Answer::Ok]).await;
    h.repo.hook("record_section_outcome", Hook::Panic);
    let op = h.run(seed_all(&h)).await;
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    assert_eq!(
        outcomes(&op),
        [
            Some(SectionOutcome::NotDispatched),
            Some(SectionOutcome::NotDispatched)
        ]
    );
    assert_eq!(h.repo.calls("complete_by_supervisor"), 1);
    assert_eq!(
        h.repo
            .applications_page(None, None, 10)
            .await
            .unwrap()
            .len(),
        1,
        "the seed was running, so it writes its record"
    );
}

// ---- answers to outcomes ----

#[test]
fn plugin_answers_map_to_outcomes_with_the_detail_cut() {
    let long = "d".repeat(PLUGIN_DETAIL_MAX_BYTES + 10);
    let (outcome, detail, location) = outcome_of(
        "a",
        &Ok(Err(SectionPluginError::Rejected {
            detail: long.clone(),
            location: Some("/x".to_owned()),
        })),
    );
    assert_eq!(outcome, SectionOutcome::Rejected);
    assert_eq!(detail.unwrap().len(), PLUGIN_DETAIL_MAX_BYTES);
    assert_eq!(location.as_deref(), Some("/x"));
    let (outcome, detail, _) = outcome_of(
        "a",
        &Ok(Err(SectionPluginError::NotPermitted { detail: long })),
    );
    assert_eq!(outcome, SectionOutcome::NotPermitted);
    assert_eq!(detail.unwrap().len(), PLUGIN_DETAIL_MAX_BYTES);
    let cases = [
        (Ok(Ok(())), SectionOutcome::Succeeded),
        (
            Ok(Err(SectionPluginError::Internal {
                message: "x".to_owned(),
            })),
            SectionOutcome::InternalFailure,
        ),
        (
            Err(CallFailure::Panicked("x".to_owned())),
            SectionOutcome::InternalFailure,
        ),
        (Err(CallFailure::TimedOut), SectionOutcome::TimedOut),
        (Err(CallFailure::NotSent), SectionOutcome::NotSent),
    ];
    for (answer, expected) in cases {
        let (outcome, detail, location) = outcome_of("a", &answer);
        assert_eq!(outcome, expected);
        assert_eq!((detail, location), (None, None), "{expected:?}");
    }
}

// ---- operational signals (DESIGN 4.1) ----

/// Wait until `line` was recorded `times` times.
async fn signalled(signals: &RecordedSignals, line: &str, times: usize) {
    for _ in 0..500 {
        if signals.count(line) >= times {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{line} not recorded {times} times: {:?}", signals.lines());
}

#[tokio::test]
async fn an_accepted_seed_raises_its_signals() {
    let h = harness(&[Answer::Ok, Answer::Rejected]).await;
    h.run(seed_all(&h)).await;
    signalled(&h.signals, "tasks:-1", 1).await;
    let lines = h.signals.lines();
    for line in [
        "tasks:1",
        "request:seed,accepted",
        "section:a,succeeded",
        "section:b,rejected",
        "completed:seed,plugin_rejected,run",
        "duration:seed",
        "tasks:-1",
    ] {
        assert_eq!(h.signals.count(line), 1, "{line}: {lines:?}");
    }
    assert_eq!(h.signals.count("history:true"), 2, "accepted and finished");
    assert_eq!(h.signals.count("history:false"), 0);
}

#[tokio::test]
async fn a_refusal_raises_its_category_and_the_caller_refusal() {
    let h = harness(&[Answer::Ok]).await;
    let (_, answer) = h.submit_as("someone-else", seed_all(&h)).await;
    assert!(answer.is_err());
    signalled(&h.signals, "tasks:-1", 1).await;
    assert_eq!(h.signals.count("request:seed,not_permitted"), 1);
    assert_eq!(h.signals.count("refused:seed"), 1);
    assert_eq!(h.signals.count("history:true"), 1);
}

#[tokio::test]
async fn an_unwritten_refused_event_and_storage_failure_are_signalled() {
    let h = harness(&[Answer::Ok]).await;
    h.repo.hook("open_operation", Hook::Fail);
    h.repo.hook("record_refused", Hook::Fail);
    let (_, answer) = h.submit(seed_all(&h)).await;
    assert_eq!(answer, Err(DomainError::StorageUnavailable));
    assert_eq!(h.signals.count("request:seed,temporarily_unavailable"), 1);
    assert_eq!(h.signals.count("unavailable:storage"), 1);
    assert_eq!(h.signals.count("history:false"), 1);
}

#[tokio::test]
async fn a_timed_out_section_is_counted_by_its_key() {
    let h = harness_with(
        &[Answer::Hang],
        OrchestratorSettings {
            plugin_call_timeout: Duration::from_millis(50),
            ..settings()
        },
    )
    .await;
    h.run(seed_all(&h)).await;
    assert_eq!(
        h.signals.count("section:a,timed_out"),
        1,
        "{:?}",
        h.signals.lines()
    );
    assert_eq!(h.signals.count("completed:seed,internal_failure,run"), 1);
}
