//! The request orchestrator over fakes, a real `SQLite` repository, and a fake
//! clock. Timeouts are real but short; no test depends on paused time because
//! the database runs on its own thread.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::too_many_lines)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use application_manager_sdk::{
    Dependency, Envelope, OperationAction, ReportApplication, RequestCategory, SectionOutcome,
    UnavailableItem, UnavailableReasonCode,
};
use futures::FutureExt;
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use super::*;
use crate::domain::actions::Action;
use crate::domain::audit::{AuditEvent, AuditKind};
use crate::domain::operation::{Operation, OperationStatus, run_deadline};
use crate::domain::ports::AccessError;
use crate::infra::storage::SeaOrmOperationRepository;
use crate::test_support::{
    ApplyAnswer, CallLog, FakeAccess, FakeClock, FakePlugin, Fault, HookedRepo, KeyAnswer, MARGIN,
    SchemaAnswer, T0, ctx, orchestrator, ready, section, sqlite_repo,
};

const APP: &str = "acme.app";
const TASKS: usize = 4;
const SHORT: Duration = Duration::from_millis(50);
const LONG: Duration = Duration::from_secs(60);

fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

/// A receipt with fixed ids: the accepted event id sorts before the finished
/// one, as time-ordered ids do.
fn receipt(n: u128) -> Receipt {
    Receipt {
        request_id: id(n * 16 + 1),
        accepted_event_id: id(n * 16 + 2),
        finished_event_id: id(n * 16 + 3),
        refused_event_id: id(n * 16 + 4),
        received_at: T0,
    }
}

fn manifest(app: &str, sections: &Value) -> Value {
    json!({ "application": { "id": app, "version": "1" }, "sections": sections })
}

fn seed_of(manifest: &Value) -> RequestInput {
    RequestInput::Seed(Bytes::from(serde_json::to_vec(manifest).unwrap()))
}

fn sections_of(keys: &[&str]) -> Value {
    Value::Object(keys.iter().map(|k| ((*k).to_owned(), json!({}))).collect())
}

/// What the first apply saw: the status and the event kinds.
type Seen = Option<(OperationStatus, Vec<AuditKind>)>;

struct H {
    orch: Arc<RequestOrchestrator>,
    access: Arc<FakeAccess>,
    repo: Arc<HookedRepo>,
    store: Arc<SeaOrmOperationRepository>,
    clock: Arc<FakeClock>,
    log: CallLog,
    plugins: Vec<Arc<FakePlugin>>,
    permits: Arc<Semaphore>,
    call_timeout: Duration,
}

async fn harness_with(
    keys: &[(&'static str, i16)],
    access: Result<(), AccessError>,
    call_timeout: Duration,
) -> H {
    let log = CallLog::default();
    let plugins: Vec<_> = keys
        .iter()
        .map(|(key, _)| {
            let plugin = FakePlugin::new(KeyAnswer::Key("unused"));
            plugin.log_to(key, &log);
            plugin
        })
        .collect();
    let set = keys
        .iter()
        .zip(&plugins)
        .map(|((key, priority), plugin)| section(key, *priority, plugin))
        .collect();
    let store = sqlite_repo().await;
    let repo = HookedRepo::over(Arc::clone(&store) as Arc<dyn OperationRepository>);
    let clock = FakeClock::new(T0);
    let access = FakeAccess::new(access);
    let orch = orchestrator(
        Arc::clone(&access),
        ready(set),
        Arc::clone(&repo),
        Arc::clone(&clock),
        call_timeout,
    );
    H {
        orch,
        access,
        repo,
        store,
        clock,
        log,
        plugins,
        permits: Arc::new(Semaphore::new(TASKS)),
        call_timeout,
    }
}

async fn harness(keys: &[(&'static str, i16)]) -> H {
    harness_with(keys, Ok(()), LONG).await
}

async fn wait_for(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

impl H {
    async fn submit(&self, input: RequestInput, receipt: Receipt) -> Result<Accepted, DomainError> {
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .expect("a free request-task permit");
        Arc::clone(&self.orch)
            .submit(ctx(), input, receipt, permit)
            .await
    }

    async fn seed(&self, receipt: Receipt, manifest: &Value) -> Result<Accepted, DomainError> {
        self.submit(seed_of(manifest), receipt).await
    }

    /// Wait until every request task has ended and returned its permit.
    async fn settle(&self) {
        wait_for("the request tasks to end", || {
            self.permits.available_permits() == TASKS
        })
        .await;
    }

    /// The record as read at `T0`, before any run deadline.
    async fn op(&self, operation_id: Uuid) -> Operation {
        self.store
            .read_operation(operation_id, T0)
            .await
            .unwrap()
            .expect("operation record")
    }

    async fn has_operation(&self, operation_id: Uuid) -> bool {
        self.store
            .read_operation(operation_id, T0)
            .await
            .unwrap()
            .is_some()
    }

    async fn events(&self, correlation_id: Uuid) -> Vec<AuditEvent> {
        self.store.audit_events(correlation_id).await.unwrap()
    }

    /// The plugins that were called with `method`, in call order.
    fn called(&self, method: &str) -> Vec<String> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, m)| *m == method)
            .map(|(key, _)| key.clone())
            .collect()
    }

    fn plugin_calls(&self) -> usize {
        self.log.lock().unwrap().len()
    }

    async fn wait_for_apply(&self, key: &str) {
        wait_for("an apply call", || {
            self.called("apply").iter().any(|k| k == key)
        })
        .await;
    }
}

fn kinds(events: &[AuditEvent]) -> Vec<AuditKind> {
    events.iter().map(|e| e.kind).collect()
}

fn category_of_event(event: &AuditEvent) -> &str {
    event.content["result"]["category"].as_str().unwrap()
}

fn outcomes(op: &Operation) -> Vec<(String, SectionOutcome)> {
    op.report
        .sections
        .iter()
        .map(|s| (s.key.clone(), s.outcome))
        .collect()
}

/// The one refused event of `request`, after checking there is exactly one.
async fn the_refused_event(h: &H, r: Receipt) -> AuditEvent {
    let events = h.events(r.request_id).await;
    assert_eq!(kinds(&events), [AuditKind::Refused], "one refused event");
    assert_eq!(events[0].event_id, r.refused_event_id);
    assert_eq!(events[0].correlation_id, r.request_id);
    events.into_iter().next().unwrap()
}

// ── Accepted operations ─────────────────────────────────────────────────────

#[tokio::test]
async fn a_seed_is_accepted_dispatched_and_finished() {
    let h = harness(&[("a", 1), ("b", 2)]).await;
    let r = receipt(1);
    let sections = json!({ "a": { "x": 1 }, "b": { "y": 2 } });
    let accepted = h.seed(r, &manifest(APP, &sections)).await.unwrap();
    assert_eq!(
        accepted,
        Accepted {
            operation_id: r.request_id
        }
    );
    h.settle().await;

    let op = h.op(r.request_id).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert_eq!(op.application_id, APP);
    assert_eq!(op.action, OperationAction::Seed);
    assert_eq!(op.accepted_at, T0);
    assert_eq!(op.finished_event_id, r.finished_event_id);
    assert_eq!(
        op.run_deadline,
        run_deadline(T0, OperationAction::Seed, 2, h.call_timeout, MARGIN).unwrap()
    );
    assert_eq!(
        op.report.application,
        ReportApplication {
            id: APP.to_owned(),
            version: Some("1".to_owned())
        }
    );
    assert_eq!(
        op.report
            .sections
            .iter()
            .map(|s| (s.key.as_str(), s.outcome, s.schema_version.as_deref()))
            .collect::<Vec<_>>(),
        [
            ("a", SectionOutcome::Succeeded, Some("1")),
            ("b", SectionOutcome::Succeeded, Some("1")),
        ]
    );

    let events = h.events(r.request_id).await;
    assert_eq!(kinds(&events), [AuditKind::Accepted, AuditKind::Finished]);
    assert_eq!(events[0].event_id, r.accepted_event_id);
    assert_eq!(
        events[1].event_id, r.finished_event_id,
        "finished reuses its id"
    );
    assert!(events.iter().all(|e| e.correlation_id == r.request_id));

    let applied = h.plugins[0].apply_args.lock().unwrap().clone();
    assert_eq!(
        applied,
        [(
            r.request_id,
            Envelope {
                id: APP.to_owned(),
                version: "1".to_owned()
            },
            json!({ "x": 1 })
        )],
        "the section unchanged, with the envelope and the operation id"
    );
    assert_eq!(
        h.plugins[1].apply_args.lock().unwrap()[0].2,
        json!({ "y": 2 })
    );
}

#[tokio::test]
async fn the_accepted_event_lists_the_keys_of_the_key_set() {
    let h = harness(&[("a", 1), ("b", 2)]).await;
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a", "b", "zzz"])))
        .await
        .unwrap();
    h.settle().await;
    let events = h.events(r.request_id).await;
    let content = &events[0].content;
    assert_eq!(content["application"], json!({ "id": APP, "version": "1" }));
    let mut keys: Vec<_> = content["section_keys"]["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| k.as_str().unwrap().to_owned())
        .collect();
    keys.sort();
    assert_eq!(keys, ["a", "b"]);
    assert_eq!(content["section_keys"]["other_keys"], 1);
    assert_eq!(
        content["caller"]["subject_id"],
        ctx().subject_id().to_string()
    );
}

#[tokio::test]
async fn dispatch_follows_priority_order() {
    let h = harness(&[("c", 5), ("a", 1), ("b", 3)]).await;
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a", "b", "c"])))
        .await
        .unwrap();
    h.settle().await;
    assert_eq!(h.called("apply"), ["a", "b", "c"]);
    let op = h.op(r.request_id).await;
    assert_eq!(
        outcomes(&op)
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "c"],
        "the report is in priority order"
    );
}

#[tokio::test]
async fn the_operation_and_accepted_event_are_committed_before_the_first_apply() {
    let h = harness(&[("a", 1)]).await;
    let seen: Arc<Mutex<Seen>> = Arc::default();
    let (store, sink) = (Arc::clone(&h.store), Arc::clone(&seen));
    h.plugins[0].before_call(Arc::new(move |operation_id| {
        let (store, sink) = (Arc::clone(&store), Arc::clone(&sink));
        async move {
            let op = store
                .read_operation(operation_id, T0)
                .await
                .unwrap()
                .unwrap();
            let events = store.audit_events(operation_id).await.unwrap();
            *sink.lock().unwrap() = Some((op.status, kinds(&events)));
        }
        .boxed()
    }));
    h.seed(receipt(1), &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    h.settle().await;
    assert_eq!(
        *seen.lock().unwrap(),
        Some((OperationStatus::Running, vec![AuditKind::Accepted])),
        "running, with the accepted event and no finished event, at the first apply"
    );
}

#[tokio::test]
async fn dispatch_continues_after_each_failure_kind() {
    let cases = [
        (
            ApplyAnswer::Rejected {
                detail: "bad".to_owned(),
                location: Some("/x".to_owned()),
            },
            SectionOutcome::Rejected,
            RequestCategory::PluginRejected,
        ),
        (
            ApplyAnswer::NotPermitted("no".to_owned()),
            SectionOutcome::NotPermitted,
            RequestCategory::NotPermitted,
        ),
        (
            ApplyAnswer::Internal,
            SectionOutcome::InternalFailure,
            RequestCategory::InternalFailure,
        ),
        (
            ApplyAnswer::Panic,
            SectionOutcome::InternalFailure,
            RequestCategory::InternalFailure,
        ),
        (
            ApplyAnswer::Hang,
            SectionOutcome::TimedOut,
            RequestCategory::InternalFailure,
        ),
    ];
    for (answer, outcome, category) in cases {
        let timeout = if matches!(answer, ApplyAnswer::Hang) {
            SHORT
        } else {
            LONG
        };
        let h = harness_with(&[("a", 1), ("b", 2)], Ok(()), timeout).await;
        h.plugins[0].set_apply(answer);
        let r = receipt(1);
        h.seed(r, &manifest(APP, &sections_of(&["a", "b"])))
            .await
            .unwrap();
        h.settle().await;
        assert_eq!(
            h.called("apply"),
            ["a", "b"],
            "{outcome:?}: b is still called"
        );
        let op = h.op(r.request_id).await;
        assert_eq!(
            outcomes(&op),
            [
                ("a".to_owned(), outcome),
                ("b".to_owned(), SectionOutcome::Succeeded)
            ]
        );
        assert_eq!(op.category, Some(category), "{outcome:?}");
        let events = h.events(r.request_id).await;
        assert_eq!(kinds(&events), [AuditKind::Accepted, AuditKind::Finished]);
    }
}

#[tokio::test]
async fn a_rejection_keeps_its_detail_and_location_in_the_report() {
    let h = harness(&[("a", 1)]).await;
    h.plugins[0].set_apply(ApplyAnswer::Rejected {
        detail: "bad port".to_owned(),
        location: Some("/port".to_owned()),
    });
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    h.settle().await;
    let entry = &h.op(r.request_id).await.report.sections[0];
    assert_eq!(entry.detail.as_deref(), Some("bad port"));
    assert_eq!(entry.location.as_deref(), Some("/port"));
}

#[tokio::test]
async fn not_permitted_beats_rejected_beats_internal_failure() {
    let h = harness(&[("a", 1), ("b", 2), ("c", 3)]).await;
    h.plugins[0].set_apply(ApplyAnswer::Internal);
    h.plugins[1].set_apply(ApplyAnswer::Rejected {
        detail: "bad".to_owned(),
        location: None,
    });
    h.plugins[2].set_apply(ApplyAnswer::NotPermitted("no".to_owned()));
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a", "b", "c"])))
        .await
        .unwrap();
    h.settle().await;
    assert_eq!(h.called("apply"), ["a", "b", "c"]);
    assert_eq!(
        h.op(r.request_id).await.category,
        Some(RequestCategory::NotPermitted)
    );
}

// ── Seed outcome ranks 5 to 7 ───────────────────────────────────────────────

#[tokio::test]
async fn rank_5_an_unavailable_schema_ends_the_operation_before_any_apply() {
    let h = harness(&[("a", 1), ("b", 2)]).await;
    h.plugins[0].set_schema(SchemaAnswer::Internal);
    h.plugins[1].set_schema(SchemaAnswer::NotPermitted("no".to_owned()));
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a", "b"])))
        .await
        .unwrap();
    h.settle().await;
    let op = h.op(r.request_id).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(
        op.category,
        Some(RequestCategory::TemporarilyUnavailable),
        "unavailable wins over not permitted"
    );
    assert_eq!(
        op.report.unavailable,
        [UnavailableItem {
            dependency: Dependency::SectionSchema,
            key: Some("a".to_owned()),
            reason: UnavailableReasonCode::Unreachable,
        }]
    );
    assert!(h.called("apply").is_empty());
    let events = h.events(r.request_id).await;
    assert_eq!(kinds(&events), [AuditKind::Accepted, AuditKind::Finished]);
}

#[tokio::test]
async fn rank_5_a_schema_that_times_out_is_unavailable() {
    let h = harness_with(&[("a", 1)], Ok(()), SHORT).await;
    h.plugins[0].set_schema(SchemaAnswer::Hang);
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    h.settle().await;
    let op = h.op(r.request_id).await;
    assert_eq!(op.category, Some(RequestCategory::TemporarilyUnavailable));
    assert_eq!(
        op.report.unavailable[0].reason,
        UnavailableReasonCode::TimedOut
    );
}

#[tokio::test]
async fn rank_6_a_plugin_that_refuses_the_caller_ends_the_operation() {
    let h = harness(&[("a", 1), ("b", 2)]).await;
    h.plugins[1].set_schema(SchemaAnswer::NotPermitted("no".to_owned()));
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a", "b"])))
        .await
        .unwrap();
    h.settle().await;
    let op = h.op(r.request_id).await;
    assert_eq!(op.category, Some(RequestCategory::NotPermitted));
    assert!(op.report.unavailable.is_empty());
    assert!(h.called("apply").is_empty());
}

#[tokio::test]
async fn rank_7_a_section_problem_ends_the_operation_before_any_apply() {
    let h = harness(&[("a", 1), ("b", 2)]).await;
    h.plugins[0].set_schema(SchemaAnswer::serve(
        json!({ "type": "object", "required": ["port"] }),
        "7",
    ));
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a", "b"])))
        .await
        .unwrap();
    h.settle().await;
    let op = h.op(r.request_id).await;
    assert_eq!(op.category, Some(RequestCategory::InvalidManifest));
    assert_eq!(op.report.problems.len(), 1);
    assert_eq!(op.report.problems[0].rule, "required");
    assert!(op.report.problems[0].location.starts_with("/sections/a"));
    assert!(h.called("apply").is_empty(), "no plugin is applied");
    let events = h.events(r.request_id).await;
    assert_eq!(kinds(&events), [AuditKind::Accepted, AuditKind::Finished]);
}

#[tokio::test]
async fn a_missing_and_an_unknown_section_are_section_problems() {
    let h = harness(&[("a", 1)]).await;
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["zzz"])))
        .await
        .unwrap();
    h.settle().await;
    let op = h.op(r.request_id).await;
    assert_eq!(op.category, Some(RequestCategory::InvalidManifest));
    let mut rules: Vec<_> = op.report.problems.iter().map(|p| p.rule.as_str()).collect();
    rules.sort_unstable();
    assert_eq!(rules, ["missing_section", "unknown_section"]);
    assert!(h.called("apply").is_empty());
}

#[tokio::test]
async fn an_empty_plugin_set_seeds_empty_sections_and_refuses_any_section() {
    let h = harness(&[]).await;
    let ok = receipt(1);
    h.seed(ok, &manifest(APP, &json!({}))).await.unwrap();
    let unknown = receipt(2);
    h.seed(unknown, &manifest("other", &sections_of(&["zzz"])))
        .await
        .unwrap();
    h.settle().await;
    let op = h.op(ok.request_id).await;
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert!(op.report.sections.is_empty());
    let op = h.op(unknown.request_id).await;
    assert_eq!(op.category, Some(RequestCategory::InvalidManifest));
    assert_eq!(op.report.problems[0].rule, "unknown_section");
}

#[tokio::test]
async fn envelope_and_section_problems_end_at_rank_4_with_the_envelope_problems_only() {
    let h = harness(&[("a", 1)]).await;
    let r = receipt(1);
    let bad = json!({ "application": { "id": "" }, "sections": {} });
    let err = h.submit(seed_of(&bad), r).await.unwrap_err();
    let DomainError::InvalidManifest { problems } = err else {
        panic!("rank 4 expected, got {err:?}");
    };
    assert!(!problems.is_empty());
    assert!(
        problems.iter().all(|p| p.rule != "missing_section"),
        "section problems are listed only once the envelope is valid: {problems:?}"
    );
    h.settle().await;
    assert_eq!(h.plugin_calls(), 0, "not even a schema call");
    assert!(!h.has_operation(r.request_id).await);
    the_refused_event(&h, r).await;
}

// ── Uninstall ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn an_uninstall_removes_in_priority_order_and_continues_after_a_failure() {
    let h = harness(&[("b", 2), ("a", 1)]).await;
    h.plugins[1].set_remove(ApplyAnswer::Internal);
    let r = receipt(1);
    h.submit(RequestInput::UninstallById(APP.to_owned()), r)
        .await
        .unwrap();
    h.settle().await;
    assert_eq!(h.called("remove"), ["a", "b"]);
    assert_eq!(h.plugin_calls(), 2, "no schema call, no apply");
    let op = h.op(r.request_id).await;
    assert_eq!(op.action, OperationAction::Uninstall);
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    assert_eq!(
        outcomes(&op),
        [
            ("a".to_owned(), SectionOutcome::InternalFailure),
            ("b".to_owned(), SectionOutcome::Succeeded)
        ]
    );
    assert_eq!(
        op.report.application,
        ReportApplication {
            id: APP.to_owned(),
            version: None
        }
    );
    assert_eq!(
        op.run_deadline,
        run_deadline(T0, OperationAction::Uninstall, 2, h.call_timeout, MARGIN).unwrap()
    );
    for plugin in &h.plugins {
        assert_eq!(
            *plugin.remove_args.lock().unwrap(),
            [(r.request_id, APP.to_owned())]
        );
    }
    let events = h.events(r.request_id).await;
    assert_eq!(kinds(&events), [AuditKind::Accepted, AuditKind::Finished]);
    assert!(events[0].content.get("section_keys").is_none());
}

#[tokio::test]
async fn an_uninstall_by_body_takes_the_id_from_the_body() {
    let h = harness(&[("a", 1)]).await;
    let r = receipt(1);
    let body = Bytes::from_static(br#"{"application_id":"from.body"}"#);
    h.submit(RequestInput::UninstallBody(body), r)
        .await
        .unwrap();
    h.settle().await;
    assert_eq!(h.op(r.request_id).await.application_id, "from.body");
    assert_eq!(
        *h.plugins[0].remove_args.lock().unwrap(),
        [(r.request_id, "from.body".to_owned())]
    );
}

#[tokio::test]
async fn an_uninstall_with_an_empty_plugin_set_succeeds() {
    let h = harness(&[]).await;
    let r = receipt(1);
    h.submit(RequestInput::UninstallById(APP.to_owned()), r)
        .await
        .unwrap();
    h.settle().await;
    let op = h.op(r.request_id).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert!(op.report.sections.is_empty());
}

// ── Refusals: every rank, with no plugin call and one refused event ─────────

#[tokio::test]
async fn refusals_make_no_plugin_call_and_record_one_refused_event() {
    // (name, access answer, input, expected error, expected category)
    let invalid_body = RequestInput::Seed(Bytes::from_static(b"{}"));
    let cases = [
        (
            "rank 2",
            Err(AccessError::NotPermitted),
            seed_of(&manifest(APP, &sections_of(&["a"]))),
            DomainError::NotPermitted {
                action: Action::Seed,
            },
            "not_permitted",
        ),
        (
            "rank 2 uninstall",
            Err(AccessError::NotPermitted),
            RequestInput::UninstallById(APP.to_owned()),
            DomainError::NotPermitted {
                action: Action::Uninstall,
            },
            "not_permitted",
        ),
        (
            "rank 1 access unavailable",
            Err(AccessError::Unavailable),
            seed_of(&manifest(APP, &sections_of(&["a"]))),
            DomainError::AccessUnavailable,
            "temporarily_unavailable",
        ),
    ];
    for (name, access, input, expected, category) in cases {
        let h = harness_with(&[("a", 1)], access, LONG).await;
        let r = receipt(1);
        assert_eq!(h.submit(input, r).await, Err(expected), "{name}");
        h.settle().await;
        assert_eq!(h.plugin_calls(), 0, "{name}");
        assert!(
            h.repo.open_operation_ids.lock().unwrap().is_empty(),
            "{name}: storage is consulted only for a permitted caller"
        );
        assert!(!h.has_operation(r.request_id).await, "{name}");
        let event = the_refused_event(&h, r).await;
        assert_eq!(category_of_event(&event), category, "{name}");
    }

    let h = harness(&[("a", 1)]).await;
    let r = receipt(1);
    let err = h.submit(invalid_body, r).await.unwrap_err();
    assert!(
        matches!(err, DomainError::InvalidManifest { .. }),
        "{err:?}"
    );
    h.settle().await;
    assert_eq!(h.plugin_calls(), 0, "rank 4");
    let event = the_refused_event(&h, r).await;
    assert_eq!(category_of_event(&event), "invalid_manifest");
}

#[tokio::test]
async fn rank_1_storage_cannot_answer_for_a_permitted_caller() {
    let h = harness(&[("a", 1)]).await;
    h.repo.inject("open_operation", Fault::Unavailable);
    let r = receipt(1);
    let err = h
        .seed(r, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap_err();
    assert_eq!(err, DomainError::StorageUnavailable);
    h.settle().await;
    assert_eq!(h.plugin_calls(), 0);
    let event = the_refused_event(&h, r).await;
    assert_eq!(category_of_event(&event), "temporarily_unavailable");
}

#[tokio::test]
async fn a_refusal_keeps_its_category_when_the_refused_event_cannot_be_recorded() {
    let h = harness_with(&[("a", 1)], Err(AccessError::NotPermitted), LONG).await;
    h.repo.inject("record_refused", Fault::Unavailable);
    let r = receipt(1);
    let err = h
        .seed(r, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap_err();
    assert_eq!(
        err,
        DomainError::NotPermitted {
            action: Action::Seed
        }
    );
    assert!(h.events(r.request_id).await.is_empty());
}

#[tokio::test]
async fn a_body_that_is_not_json_is_an_envelope_problem_with_a_refused_event() {
    for input in [
        RequestInput::Seed(Bytes::from_static(b"not json")),
        RequestInput::UninstallBody(Bytes::from_static(b"not json")),
    ] {
        let h = harness(&[("a", 1)]).await;
        let r = receipt(1);
        let err = h.submit(input, r).await.unwrap_err();
        let DomainError::InvalidManifest { problems } = err else {
            panic!("rank 4 expected, got {err:?}");
        };
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert_eq!(problems[0].location, "");
        assert_eq!(problems[0].rule, "type");
        h.settle().await;
        let event = the_refused_event(&h, r).await;
        assert_eq!(category_of_event(&event), "invalid_manifest");
        assert_eq!(event.content["result"]["problem_count"], 1);
        assert_eq!(event.content["result"]["rules"], json!(["type"]));
        assert_eq!(h.plugin_calls(), 0);
    }
}

#[tokio::test]
async fn an_uninstall_id_outside_the_limits_is_rank_4_and_runs_the_storage_step_with_the_empty_id()
{
    let too_long = "x".repeat(usize::try_from(crate::test_support::MAX_TEXT).unwrap() + 1);
    for bad in [
        String::new(),
        too_long,
        "a\u{7}b".to_owned(),
        "a\u{85}b".to_owned(),
    ] {
        let h = harness(&[("a", 1)]).await;
        let r = receipt(1);
        let err = h
            .submit(RequestInput::UninstallById(bad.clone()), r)
            .await
            .unwrap_err();
        assert!(
            matches!(err, DomainError::InvalidManifest { .. }),
            "{:?}: {err:?}",
            bad.len()
        );
        h.settle().await;
        assert_eq!(*h.repo.open_operation_ids.lock().unwrap(), [String::new()]);
        assert_eq!(*h.access.application_ids.lock().unwrap(), [None]);
        assert_eq!(h.plugin_calls(), 0);
        the_refused_event(&h, r).await;
    }
    let h = harness(&[("a", 1)]).await;
    let err = h
        .submit(
            RequestInput::UninstallBody(Bytes::from_static(b"{}")),
            receipt(1),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, DomainError::InvalidManifest { .. }),
        "{err:?}"
    );
}

#[tokio::test]
async fn rank_3_a_conflict_names_the_open_operation_and_calls_no_plugin() {
    let h = harness(&[("a", 1)]).await;
    h.plugins[0].set_apply(ApplyAnswer::Hang);
    let first = receipt(1);
    h.seed(first, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    h.wait_for_apply("a").await;
    let calls = h.plugin_calls();

    let second = receipt(2);
    let err = h
        .seed(second, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap_err();
    assert_eq!(
        err,
        DomainError::Conflict {
            operation_id: first.request_id
        }
    );
    let uninstall = receipt(3);
    let err = h
        .submit(RequestInput::UninstallById(APP.to_owned()), uninstall)
        .await
        .unwrap_err();
    assert_eq!(
        err,
        DomainError::Conflict {
            operation_id: first.request_id
        }
    );

    assert_eq!(h.plugin_calls(), calls, "a conflict calls no plugin");
    let event = the_refused_event(&h, second).await;
    assert_eq!(category_of_event(&event), "conflict");
    assert_eq!(
        event.content["result"]["busy_operation_id"],
        first.request_id.to_string()
    );
    assert!(!h.has_operation(second.request_id).await);
    the_refused_event(&h, uninstall).await;
}

#[tokio::test]
async fn another_application_does_not_conflict() {
    let h = harness(&[("a", 1)]).await;
    h.plugins[0].set_apply(ApplyAnswer::Hang);
    h.seed(receipt(1), &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    h.wait_for_apply("a").await;
    h.seed(receipt(2), &manifest("other.app", &sections_of(&["a"])))
        .await
        .unwrap();
}

#[tokio::test]
async fn an_expired_open_operation_is_interrupted_by_the_next_acceptance() {
    let h = harness(&[("a", 1)]).await;
    h.plugins[0].set_apply(ApplyAnswer::Hang);
    let first = receipt(1);
    h.seed(first, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    h.wait_for_apply("a").await;
    let deadline = h.op(first.request_id).await.run_deadline;

    h.clock.set(deadline + time::Duration::seconds(1));
    h.plugins[0].set_apply(ApplyAnswer::Ok);
    let second = receipt(2);
    h.seed(second, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    wait_for("the second operation", || {
        h.permits.available_permits() >= TASKS - 1
    })
    .await;

    let interrupted = h
        .store
        .read_operation(first.request_id, deadline + time::Duration::seconds(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(interrupted.status, OperationStatus::Interrupted);
    let events = h.events(first.request_id).await;
    assert_eq!(kinds(&events), [AuditKind::Accepted, AuditKind::Finished]);
}

#[tokio::test]
async fn a_lost_race_is_a_conflict_naming_the_winner() {
    let h = harness(&[("a", 1)]).await;
    h.plugins[0].set_apply(ApplyAnswer::Hang);
    let first = receipt(1);
    h.seed(first, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    h.wait_for_apply("a").await;
    let calls = h.plugin_calls();

    h.repo.hide_open_operations();
    let second = receipt(2);
    let err = h
        .seed(second, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap_err();
    assert_eq!(
        err,
        DomainError::Conflict {
            operation_id: first.request_id
        }
    );
    assert_eq!(h.plugin_calls(), calls);
    let event = the_refused_event(&h, second).await;
    assert_eq!(category_of_event(&event), "conflict");
    assert_eq!(
        event.content["result"]["busy_operation_id"],
        first.request_id.to_string()
    );
    assert!(!h.has_operation(second.request_id).await);
}

#[tokio::test]
async fn a_lost_race_whose_winner_cannot_be_read_is_unavailable_with_nothing_inserted() {
    let h = harness(&[("a", 1)]).await;
    h.plugins[0].set_apply(ApplyAnswer::Hang);
    h.seed(receipt(1), &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    h.wait_for_apply("a").await;
    h.repo.hide_open_operations();
    h.repo.inject("refuse_after_lost_race", Fault::Unavailable);
    let second = receipt(2);
    let err = h
        .seed(second, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap_err();
    assert_eq!(err, DomainError::StorageUnavailable);
    assert!(
        h.events(second.request_id).await.is_empty(),
        "nothing more is inserted"
    );
}

#[tokio::test]
async fn an_acceptance_that_cannot_commit_is_unavailable_with_a_refused_event() {
    let h = harness(&[("a", 1)]).await;
    h.repo.inject("accept", Fault::Unavailable);
    let r = receipt(1);
    let err = h
        .seed(r, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap_err();
    assert_eq!(err, DomainError::StorageUnavailable);
    h.settle().await;
    assert_eq!(h.plugin_calls(), 0);
    assert!(!h.has_operation(r.request_id).await);
    let event = the_refused_event(&h, r).await;
    assert_eq!(category_of_event(&event), "temporarily_unavailable");
}

#[tokio::test]
async fn a_gear_defect_before_the_answer_is_a_defect_with_an_internal_failure_event() {
    let h = harness(&[("a", 1)]).await;
    h.repo.inject("open_operation", Fault::Panic);
    let r = receipt(1);
    let err = h
        .seed(r, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap_err();
    assert_eq!(err, DomainError::Defect);
    h.settle().await;
    assert_eq!(h.plugin_calls(), 0);
    let event = the_refused_event(&h, r).await;
    assert_eq!(category_of_event(&event), "internal_failure");
}

#[tokio::test]
async fn the_gate_refusal_event_names_the_gear_as_unavailable() {
    for (refusal, reason) in [
        (DomainError::NotReady, "not_ready"),
        (DomainError::AtCapacity, "at_capacity"),
    ] {
        let h = harness(&[("a", 1)]).await;
        let r = receipt(1);
        let input = seed_of(&manifest(APP, &sections_of(&["a"])));
        h.orch.refuse_at_gate(&ctx(), &input, &r, &refusal).await;
        let event = the_refused_event(&h, r).await;
        assert_eq!(category_of_event(&event), "temporarily_unavailable");
        assert_eq!(event.content["application"]["id"], APP);
        assert_eq!(
            event.content["result"]["unavailable"],
            json!([{ "dependency": "application_manager", "reason": reason }])
        );
        assert_eq!(h.access.calls(), 0);
        assert_eq!(h.plugin_calls(), 0);
        assert!(h.repo.open_operation_ids.lock().unwrap().is_empty());
        assert!(!h.has_operation(r.request_id).await);
    }
}

// ── Access control ──────────────────────────────────────────────────────────

#[tokio::test]
async fn access_control_gets_the_action_and_the_application_id_within_the_limits() {
    let max = usize::try_from(crate::test_support::MAX_TEXT).unwrap();
    let cases: Vec<(String, Option<String>)> = vec![
        (APP.to_owned(), Some(APP.to_owned())),
        ("a".repeat(max), Some("a".repeat(max))),
        (String::new(), None),
        ("a".repeat(max + 1), None),
        ("a\u{7}b".to_owned(), None),
    ];
    for (app, expected) in cases {
        let h = harness(&[("a", 1)]).await;
        drop(
            h.seed(receipt(1), &manifest(&app, &sections_of(&["a"])))
                .await,
        );
        drop(
            h.submit(RequestInput::UninstallById(app.clone()), receipt(2))
                .await,
        );
        h.settle().await;
        assert_eq!(
            *h.access.application_ids.lock().unwrap(),
            [expected.clone(), expected],
            "{}",
            app.len()
        );
        assert_eq!(
            *h.access.actions.lock().unwrap(),
            [Action::Seed, Action::Uninstall]
        );
    }
    let h = harness(&[("a", 1)]).await;
    drop(
        h.submit(
            RequestInput::Seed(Bytes::from_static(b"not json")),
            receipt(1),
        )
        .await,
    );
    assert_eq!(*h.access.application_ids.lock().unwrap(), [None]);
}

// ── Supervision ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_request_task_completes_when_the_caller_drops_the_future() {
    let h = harness(&[("a", 1)]).await;
    let r = receipt(1);
    let permit = Arc::clone(&h.permits).try_acquire_owned().unwrap();
    let pending = Arc::clone(&h.orch).submit(
        ctx(),
        seed_of(&manifest(APP, &sections_of(&["a"]))),
        r,
        permit,
    );
    assert!(
        pending.now_or_never().is_none(),
        "dropped before the answer"
    );
    wait_for("the operation to be final", || {
        h.permits.available_permits() == TASKS
    })
    .await;
    let op = h.op(r.request_id).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
}

#[tokio::test]
async fn the_run_deadline_reached_mid_dispatch_leaves_the_record_open() {
    let h = harness(&[("a", 1), ("b", 2)]).await;
    let clock = Arc::clone(&h.clock);
    h.plugins[0].before_call(Arc::new(move |_| {
        clock.advance(Duration::from_hours(24));
        async {}.boxed()
    }));
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a", "b"])))
        .await
        .unwrap();
    h.settle().await;
    assert_eq!(
        h.called("apply"),
        ["a"],
        "b is not called past the deadline"
    );
    let op = h.op(r.request_id).await;
    assert_eq!(op.status, OperationStatus::Running, "left open");
    assert_eq!(op.category, None);
    assert_eq!(op.finished_at, None);
    let events = h.events(r.request_id).await;
    assert_eq!(kinds(&events), [AuditKind::Accepted], "no finished event");
}

#[tokio::test]
async fn supervisor_completion_follows_a_dispatch_task_panic() {
    let h = harness(&[("a", 1), ("b", 2), ("c", 3)]).await;
    h.repo.inject("record_report", Fault::Panic);
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a", "b", "c"])))
        .await
        .unwrap();
    h.settle().await;
    assert_eq!(
        h.called("apply"),
        ["a"],
        "the task died after the first section"
    );
    let op = h.op(r.request_id).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    assert_eq!(
        outcomes(&op),
        [
            ("a".to_owned(), SectionOutcome::Succeeded),
            ("b".to_owned(), SectionOutcome::NotDispatched),
            ("c".to_owned(), SectionOutcome::NotDispatched),
        ]
    );
    let events = h.events(r.request_id).await;
    assert_eq!(kinds(&events), [AuditKind::Accepted, AuditKind::Finished]);
    assert_eq!(events[1].event_id, r.finished_event_id);
}

#[tokio::test]
async fn a_failed_section_outcome_update_keeps_the_outcome_for_the_final_write() {
    let h = harness(&[("a", 1), ("b", 2)]).await;
    h.repo.inject("record_report", Fault::Unavailable);
    let r = receipt(1);
    h.seed(r, &manifest(APP, &sections_of(&["a", "b"])))
        .await
        .unwrap();
    h.settle().await;
    assert_eq!(h.called("apply"), ["a", "b"]);
    let op = h.op(r.request_id).await;
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert_eq!(
        outcomes(&op),
        [
            ("a".to_owned(), SectionOutcome::Succeeded),
            ("b".to_owned(), SectionOutcome::Succeeded)
        ]
    );
}

// ── Audit completeness ──────────────────────────────────────────────────────

#[tokio::test]
async fn every_request_leaves_exactly_its_events() {
    let h = harness(&[("a", 1)]).await;
    h.plugins[0].set_schema(SchemaAnswer::serve(
        json!({ "type": "object", "required": ["port"] }),
        "1",
    ));
    let completed = receipt(1);
    h.seed(completed, &manifest(APP, &sections_of(&["a"])))
        .await
        .unwrap();
    h.settle().await;
    let refused = receipt(2);
    h.submit(RequestInput::Seed(Bytes::from_static(b"[]")), refused)
        .await
        .unwrap_err();
    let uninstall = receipt(3);
    h.submit(RequestInput::UninstallById("other".to_owned()), uninstall)
        .await
        .unwrap();
    h.settle().await;

    assert_eq!(
        kinds(&h.events(completed.request_id).await),
        [AuditKind::Accepted, AuditKind::Finished]
    );
    assert_eq!(
        kinds(&h.events(refused.request_id).await),
        [AuditKind::Refused]
    );
    assert_eq!(
        kinds(&h.events(uninstall.request_id).await),
        [AuditKind::Accepted, AuditKind::Finished]
    );
}
