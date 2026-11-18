//! SQLite-backed tests of [`SeaOrmOperationRepository`]: the real `SeaORM`
//! paths against an in-memory database built from the gear's own migration.
//! Every call reads the database clock, so a test sets deadlines through the
//! run window: an operation accepted with [`OPEN`] stays open for the test,
//! one accepted with a zero window is expired at once. No test sleeps.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::{
    OperationAction, OutcomeReport, ReportApplication, RequestCategory, SectionOutcome,
};
use sea_orm::{ConnectionTrait, Database};
use sea_orm_migration::MigratorTrait;
use serde_json::json;
use time::OffsetDateTime;
use toolkit_db::migration_runner::run_migrations_for_testing;
use toolkit_db::{ConnectOpts, DBProvider, DbError, connect_db};
use tracing_test::traced_test;
use uuid::Uuid;

use super::*;
use crate::domain::history::HistoryKind;
use crate::domain::operation::OperationStatus;
use crate::infra::storage::Migrator;

const APP: &str = "acme.app";
/// A run window no test outlives.
const OPEN: Duration = Duration::from_secs(3600);
/// A run window that is over as soon as the operation is accepted.
const EXPIRED: Duration = Duration::ZERO;

struct Fixture {
    db: Arc<DBProvider<DbError>>,
    repo: Arc<SeaOrmOperationRepository>,
    dsn: String,
    /// Keeps a file-backed database alive; removed on drop.
    _file: Option<TempDb>,
}

/// A database file under the system temp directory, removed with its WAL and
/// shared-memory companions.
struct TempDb(std::path::PathBuf);

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let mut name = self.0.clone().into_os_string();
            name.push(suffix);
            std::fs::remove_file(name).ok();
        }
    }
}

async fn setup_with(timeout: Duration) -> Fixture {
    let dsn = format!(
        "sqlite:file:application_manager_repo_{}?mode=memory&cache=shared",
        Uuid::new_v4()
    );
    let db = connect_db(
        &dsn,
        ConnectOpts {
            max_conns: Some(1),
            min_conns: Some(1),
            ..Default::default()
        },
    )
    .await
    .expect("connect sqlite");
    run_migrations_for_testing(&db, Migrator::migrations())
        .await
        .expect("run migrations");
    let db = Arc::new(DBProvider::<DbError>::new(db));
    let repo = Arc::new(SeaOrmOperationRepository::new(Arc::clone(&db), timeout));
    Fixture {
        db,
        repo,
        dsn,
        _file: None,
    }
}

/// A file-backed `SQLite` database in WAL mode with a pool of `max_conns`, so
/// writers really contend.
async fn setup_file(max_conns: u32, timeout: Duration) -> Fixture {
    let path = std::env::temp_dir().join(format!("application_manager_repo_{}.db", Uuid::new_v4()));
    let file = TempDb(path.clone());
    let dsn = format!(
        "sqlite://{}?mode=rwc&wal=true&busy_timeout=5000",
        path.display()
    );
    let db = connect_db(
        &dsn,
        ConnectOpts {
            max_conns: Some(max_conns),
            min_conns: Some(1),
            ..Default::default()
        },
    )
    .await
    .expect("connect sqlite file");
    run_migrations_for_testing(&db, Migrator::migrations())
        .await
        .expect("run migrations");
    let db = Arc::new(DBProvider::<DbError>::new(db));
    let repo = Arc::new(SeaOrmOperationRepository::new(Arc::clone(&db), timeout));
    Fixture {
        db,
        repo,
        dsn,
        _file: Some(file),
    }
}

async fn setup() -> Fixture {
    setup_with(Duration::from_secs(10)).await
}

fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn report(op: u128, app: &str, action: OperationAction) -> OutcomeReport {
    OutcomeReport {
        operation_id: id(op),
        action,
        category: None,
        application: ReportApplication {
            id: app.to_owned(),
            version: (action == OperationAction::Seed).then(|| "1.0.0".to_owned()),
        },
        sections: vec![],
        problems: vec![],
        unavailable: vec![],
    }
}

/// Operation `n` of `app` with run window `window`. Its accepted event id is
/// `n + 0x1000`, its finished event id `n + 0x2000`: both are assigned at
/// receive, in that order.
fn new_op(n: u128, app: &str, action: OperationAction, window: Duration) -> NewOperation {
    NewOperation {
        operation_id: id(n),
        application_id: app.to_owned(),
        action,
        run_window: window,
        finished_event_id: id(n + 0x2000),
        report: report(n, app, action),
    }
}

fn event(event: u128, kind: HistoryKind, correlation: u128) -> HistoryEvent {
    HistoryEvent {
        event_id: id(event),
        kind,
        correlation_id: id(correlation),
        action: OperationAction::Seed,
        // Ignored on insert: the repository writes the database time.
        written_at: OffsetDateTime::UNIX_EPOCH,
        content: json!({ "marker": event }),
    }
}

fn accepted_event(n: u128) -> HistoryEvent {
    event(n + 0x1000, HistoryKind::Accepted, n)
}

async fn accept_as(
    f: &Fixture,
    n: u128,
    app: &str,
    action: OperationAction,
    window: Duration,
) -> Accepted {
    f.repo
        .accept(new_op(n, app, action, window), accepted_event(n))
        .await
        .expect("accept")
}

async fn accept(f: &Fixture, n: u128, app: &str, window: Duration) -> Accepted {
    accept_as(f, n, app, OperationAction::Seed, window).await
}

/// Accept seed `n` of `app` and mark it running with sections `keys`.
async fn running(f: &Fixture, n: u128, app: &str, keys: &[&str]) {
    accept(f, n, app, OPEN).await;
    let sections: Vec<_> = keys
        .iter()
        .map(|k| ((*k).to_owned(), Some(format!("gts.test.{k}.v1~"))))
        .collect();
    f.repo
        .mark_running(id(n), &sections)
        .await
        .expect("mark running");
}

async fn read(f: &Fixture, n: u128) -> Operation {
    f.repo
        .read_operation(id(n))
        .await
        .expect("read")
        .expect("operation exists")
}

async fn events(f: &Fixture, n: u128) -> Vec<HistoryEvent> {
    f.repo
        .history_page(Some(vec![id(n)]), None, HISTORY_PAGE_MAX_LIMIT)
        .await
        .expect("events")
}

async fn applications(f: &Fixture) -> Vec<ApplicationRecord> {
    f.repo
        .applications_page(None, None, APPLICATION_PAGE_MAX_LIMIT)
        .await
        .expect("applications")
}

fn outcomes(op: &Operation) -> Vec<Option<SectionOutcome>> {
    op.report.sections.iter().map(|s| s.outcome).collect()
}

fn kinds(events: &[HistoryEvent]) -> Vec<HistoryKind> {
    events.iter().map(|e| e.kind).collect()
}

/// `a` and `b` within `slack` of each other.
fn near(a: OffsetDateTime, b: OffsetDateTime, slack: time::Duration) -> bool {
    (a - b).abs() <= slack
}

// ---- the database clock ----

#[tokio::test]
async fn the_database_clock_is_read_as_utc_close_to_the_system_clock() {
    let f = setup().await;
    let conn = f.db.conn().unwrap();
    let now = db_now(&conn, DbBackend::Sqlite).await.unwrap();
    assert_eq!(now.offset(), time::UtcOffset::UTC);
    assert!(near(
        now,
        OffsetDateTime::now_utc(),
        time::Duration::seconds(5)
    ));
}

// ---- accept ----

#[tokio::test]
async fn accept_inserts_the_operation_and_the_accepted_event_at_database_time() {
    let f = setup().await;
    let accepted = accept(&f, 1, APP, Duration::from_secs(90)).await;
    assert_eq!(
        accepted.run_deadline,
        accepted.accepted_at + time::Duration::seconds(90)
    );
    assert!(near(
        accepted.accepted_at,
        OffsetDateTime::now_utc(),
        time::Duration::seconds(5)
    ));
    let op = read(&f, 1).await;
    assert_eq!(op.status, OperationStatus::Accepted);
    assert_eq!(op.category, None);
    assert_eq!(op.accepted_at, accepted.accepted_at);
    assert_eq!(op.run_deadline, accepted.run_deadline);
    assert_eq!(op.started_at, None);
    assert_eq!(op.finished_event_id, id(0x2001));
    assert_eq!(op.report, report(1, APP, OperationAction::Seed));
    let written = events(&f, 1).await;
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].event_id, id(0x1001));
    assert_eq!(written[0].written_at, accepted.accepted_at);
}

#[tokio::test]
async fn a_second_accept_while_one_is_open_is_index_violated_and_writes_nothing() {
    let f = setup().await;
    accept(&f, 1, APP, OPEN).await;
    assert_eq!(
        f.repo
            .accept(
                new_op(2, APP, OperationAction::Seed, OPEN),
                accepted_event(2)
            )
            .await,
        Err(StorageError::IndexViolated)
    );
    assert_eq!(f.repo.read_operation(id(2)).await, Ok(None));
    assert_eq!(events(&f, 2).await, []);
    // Another application is not blocked.
    accept(&f, 3, "other.app", OPEN).await;
}

#[tokio::test]
async fn accept_completes_an_expired_open_operation_first() {
    let f = setup().await;
    accept(&f, 1, APP, EXPIRED).await;
    accept(&f, 2, APP, OPEN).await;
    let first = read(&f, 1).await;
    assert_eq!(first.status, OperationStatus::Completed);
    assert_eq!(first.category, Some(RequestCategory::InternalFailure));
    let finished = events(&f, 1).await;
    assert_eq!(
        kinds(&finished),
        [HistoryKind::Accepted, HistoryKind::Finished]
    );
    assert_eq!(finished[1].content["result"]["reason"], json!("expired"));
    assert_eq!(
        f.repo
            .open_operation(APP)
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        id(2)
    );
}

#[tokio::test]
async fn a_failing_event_insert_rolls_the_operation_back() {
    let f = setup().await;
    accept(&f, 1, "other.app", OPEN).await;
    // The accepted event id of operation 1 is taken.
    let mut clash = accepted_event(2);
    clash.event_id = id(0x1001);
    assert_eq!(
        f.repo
            .accept(new_op(2, APP, OperationAction::Seed, OPEN), clash)
            .await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(f.repo.read_operation(id(2)).await, Ok(None));
    assert_eq!(f.repo.open_operation(APP).await, Ok(None));
}

// ---- open_operation ----

#[tokio::test]
async fn open_operation_reports_the_open_record_and_its_expiry() {
    let f = setup().await;
    assert_eq!(f.repo.open_operation(APP).await, Ok(None));
    accept(&f, 1, APP, OPEN).await;
    assert_eq!(
        f.repo.open_operation(APP).await,
        Ok(Some(OpenOperation {
            operation_id: id(1),
            expired: false
        }))
    );
    accept(&f, 2, "late.app", EXPIRED).await;
    assert_eq!(
        f.repo.open_operation("late.app").await,
        Ok(Some(OpenOperation {
            operation_id: id(2),
            expired: true
        }))
    );
}

#[tokio::test]
async fn open_operation_ignores_completed_records() {
    let f = setup().await;
    accept(&f, 1, APP, OPEN).await;
    f.repo.complete_by_supervisor(id(1)).await.unwrap();
    assert_eq!(f.repo.open_operation(APP).await, Ok(None));
}

#[tokio::test]
async fn application_ids_compare_exactly() {
    let f = setup().await;
    accept(&f, 1, "a", OPEN).await;
    accept(&f, 2, "A", OPEN).await;
    accept(&f, 3, "a ", OPEN).await;
    for (app, n) in [("a", 1), ("A", 2), ("a ", 3)] {
        assert_eq!(
            f.repo
                .open_operation(app)
                .await
                .unwrap()
                .unwrap()
                .operation_id,
            id(n),
            "{app:?}"
        );
    }
}

#[tokio::test]
async fn an_application_id_over_the_byte_limit_is_refused_by_the_schema() {
    let f = setup().await;
    // 128 two-byte characters: within 255 characters, over 255 bytes.
    let wide = "\u{e9}".repeat(128);
    assert_eq!(
        f.repo
            .accept(
                new_op(1, &wide, OperationAction::Seed, OPEN),
                accepted_event(1)
            )
            .await,
        Err(StorageError::Unavailable)
    );
    let at_limit = "\u{e9}".repeat(127);
    accept(&f, 2, &at_limit, OPEN).await;
}

// ---- lost race and refusal ----

#[tokio::test]
async fn a_lost_race_records_a_refused_event_naming_the_winner() {
    let f = setup().await;
    accept(&f, 1, APP, OPEN).await;
    let winner = f
        .repo
        .refuse_after_lost_race(
            APP,
            Box::new(|winner| {
                let mut refused = event(0x9001, HistoryKind::Refused, 0x9000);
                refused.content = json!({ "busy": winner.to_string() });
                refused
            }),
        )
        .await;
    assert_eq!(winner, Ok(id(1)));
    let refused = events(&f, 0x9000).await;
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].content, json!({ "busy": id(1).to_string() }));
}

#[tokio::test]
async fn a_lost_race_without_an_open_winner_is_unavailable_and_writes_nothing() {
    let f = setup().await;
    assert_eq!(
        f.repo
            .refuse_after_lost_race(
                APP,
                Box::new(|_| event(0x9001, HistoryKind::Refused, 0x9000))
            )
            .await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(events(&f, 0x9000).await, []);
}

#[tokio::test]
async fn record_refused_writes_one_event_at_database_time() {
    let f = setup().await;
    f.repo
        .record_refused(event(0x9001, HistoryKind::Refused, 0x9000))
        .await
        .unwrap();
    let refused = events(&f, 0x9000).await;
    assert_eq!(kinds(&refused), [HistoryKind::Refused]);
    assert!(near(
        refused[0].written_at,
        OffsetDateTime::now_utc(),
        time::Duration::seconds(5)
    ));
}

// ---- mark running and section outcomes ----

#[tokio::test]
async fn mark_running_writes_the_section_types_and_returns_the_time_left() {
    let f = setup().await;
    accept(&f, 1, APP, Duration::from_secs(600)).await;
    let left = f
        .repo
        .mark_running(
            id(1),
            &[
                ("billing".to_owned(), Some("gts.t.billing.v1~".to_owned())),
                ("crm".to_owned(), None),
            ],
        )
        .await
        .unwrap();
    assert!(
        left <= Duration::from_secs(600) && left > Duration::from_secs(590),
        "{left:?}"
    );
    let op = read(&f, 1).await;
    assert_eq!(op.status, OperationStatus::Running);
    assert!(op.started_at.is_some());
    let sections: Vec<_> = op
        .report
        .sections
        .iter()
        .map(|s| (s.key.as_str(), s.section_type.as_deref(), s.outcome))
        .collect();
    assert_eq!(
        sections,
        [
            ("billing", Some("gts.t.billing.v1~"), None),
            ("crm", None, None)
        ]
    );
}

#[tokio::test]
async fn mark_running_is_guarded() {
    let f = setup().await;
    assert_eq!(
        f.repo.mark_running(id(9), &[]).await,
        Err(StorageError::OperationFinal),
        "unknown"
    );
    accept(&f, 1, APP, EXPIRED).await;
    assert_eq!(
        f.repo.mark_running(id(1), &[]).await,
        Err(StorageError::OperationFinal),
        "expired"
    );
    running(&f, 2, "other.app", &[]).await;
    assert_eq!(
        f.repo.mark_running(id(2), &[]).await,
        Err(StorageError::OperationFinal),
        "already running"
    );
}

#[tokio::test]
async fn a_section_outcome_changes_only_its_own_entry() {
    let f = setup().await;
    running(&f, 1, APP, &["a", "b"]).await;
    f.repo
        .record_section_outcome(
            id(1),
            "b",
            SectionOutcome::Rejected,
            Some("too small".to_owned()),
            Some("/plan".to_owned()),
        )
        .await
        .unwrap();
    let op = read(&f, 1).await;
    assert_eq!(outcomes(&op), [None, Some(SectionOutcome::Rejected)]);
    assert_eq!(op.report.sections[1].detail.as_deref(), Some("too small"));
    assert_eq!(op.report.sections[1].location.as_deref(), Some("/plan"));
    assert_eq!(
        op.report.sections[0].section_type.as_deref(),
        Some("gts.test.a.v1~")
    );
    assert_eq!(
        f.repo
            .record_section_outcome(id(1), "nope", SectionOutcome::Succeeded, None, None)
            .await,
        Err(StorageError::OperationFinal),
        "no entry for the key"
    );
    f.repo.complete_by_supervisor(id(1)).await.unwrap();
    assert_eq!(
        f.repo
            .record_section_outcome(id(1), "a", SectionOutcome::Succeeded, None, None)
            .await,
        Err(StorageError::OperationFinal),
        "after a final write"
    );
}

// ---- finish ----

fn final_report(n: u128, app: &str, action: OperationAction) -> OutcomeReport {
    report(n, app, action)
}

#[tokio::test]
async fn finish_completes_a_running_seed_and_upserts_its_application() {
    let f = setup().await;
    running(&f, 1, APP, &["a"]).await;
    f.repo
        .record_section_outcome(id(1), "a", SectionOutcome::Succeeded, None, None)
        .await
        .unwrap();
    let report = read(&f, 1).await.report;
    f.repo
        .finish(id(1), &report, RequestCategory::Succeeded)
        .await
        .unwrap();
    let op = read(&f, 1).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert_eq!(op.report.category, Some(RequestCategory::Succeeded));
    let finished_at = op.finished_at.unwrap();
    let written = events(&f, 1).await;
    assert_eq!(
        kinds(&written),
        [HistoryKind::Accepted, HistoryKind::Finished]
    );
    assert_eq!(written[1].event_id, id(0x2001));
    assert_eq!(written[1].written_at, finished_at);
    assert_eq!(written[1].content["result"]["category"], json!("succeeded"));
    assert!(written[1].content["result"].get("reason").is_none());
    assert_eq!(
        applications(&f).await,
        [ApplicationRecord {
            application_id: APP.to_owned(),
            version: "1.0.0".to_owned(),
            operation_id: id(1),
            category: RequestCategory::Succeeded,
            finished_at,
        }]
    );
}

#[tokio::test]
async fn a_reseed_replaces_the_application_record() {
    let f = setup().await;
    running(&f, 1, APP, &[]).await;
    f.repo
        .finish(
            id(1),
            &final_report(1, APP, OperationAction::Seed),
            RequestCategory::Succeeded,
        )
        .await
        .unwrap();
    running(&f, 2, APP, &[]).await;
    let mut second = final_report(2, APP, OperationAction::Seed);
    second.application.version = Some("2.0.0".to_owned());
    f.repo
        .finish(id(2), &second, RequestCategory::PluginRejected)
        .await
        .unwrap();
    let records = applications(&f).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].version, "2.0.0");
    assert_eq!(records[0].operation_id, id(2));
    assert_eq!(records[0].category, RequestCategory::PluginRejected);
}

#[tokio::test]
async fn a_seed_completed_from_accepted_writes_no_application_record() {
    let f = setup().await;
    accept(&f, 1, APP, OPEN).await;
    f.repo
        .finish(
            id(1),
            &final_report(1, APP, OperationAction::Seed),
            RequestCategory::InvalidManifest,
        )
        .await
        .unwrap();
    assert_eq!(
        read(&f, 1).await.category,
        Some(RequestCategory::InvalidManifest)
    );
    assert_eq!(applications(&f).await, []);
}

#[tokio::test]
async fn a_succeeded_uninstall_deletes_the_application_record_and_a_failed_one_keeps_it() {
    let f = setup().await;
    running(&f, 1, APP, &[]).await;
    f.repo
        .finish(
            id(1),
            &final_report(1, APP, OperationAction::Seed),
            RequestCategory::Succeeded,
        )
        .await
        .unwrap();

    accept_as(&f, 2, APP, OperationAction::Uninstall, OPEN).await;
    f.repo
        .mark_running(id(2), &[("a".to_owned(), None)])
        .await
        .unwrap();
    f.repo
        .finish(
            id(2),
            &final_report(2, APP, OperationAction::Uninstall),
            RequestCategory::InternalFailure,
        )
        .await
        .unwrap();
    assert_eq!(
        applications(&f).await.len(),
        1,
        "a failed uninstall keeps it"
    );

    accept_as(&f, 3, APP, OperationAction::Uninstall, OPEN).await;
    f.repo
        .finish(
            id(3),
            &final_report(3, APP, OperationAction::Uninstall),
            RequestCategory::Succeeded,
        )
        .await
        .unwrap();
    assert_eq!(
        applications(&f).await,
        [],
        "a succeeded uninstall deletes it"
    );
}

#[tokio::test]
async fn the_first_final_write_wins() {
    let f = setup().await;
    running(&f, 1, APP, &[]).await;
    let report = final_report(1, APP, OperationAction::Seed);
    f.repo
        .finish(id(1), &report, RequestCategory::Succeeded)
        .await
        .unwrap();
    assert_eq!(
        f.repo
            .finish(id(1), &report, RequestCategory::InternalFailure)
            .await,
        Err(StorageError::OperationFinal)
    );
    assert_eq!(
        f.repo.complete_by_supervisor(id(1)).await,
        Err(StorageError::OperationFinal)
    );
    assert_eq!(f.repo.expire_if_expired(id(1)).await, Ok(false));
    assert_eq!(read(&f, 1).await.category, Some(RequestCategory::Succeeded));
    assert_eq!(events(&f, 1).await.len(), 2, "one finished event");
}

#[tokio::test]
async fn finish_after_the_run_deadline_is_operation_final_and_writes_nothing() {
    let f = setup().await;
    accept(&f, 1, APP, EXPIRED).await;
    assert_eq!(
        f.repo
            .finish(
                id(1),
                &final_report(1, APP, OperationAction::Seed),
                RequestCategory::Succeeded
            )
            .await,
        Err(StorageError::OperationFinal)
    );
    assert_eq!(events(&f, 1).await.len(), 1);
    assert_eq!(applications(&f).await, []);
}

// ---- supervisor completion ----

#[tokio::test]
async fn supervisor_completion_from_accepted_is_internal_failure_without_a_record() {
    let f = setup().await;
    accept(&f, 1, APP, OPEN).await;
    f.repo.complete_by_supervisor(id(1)).await.unwrap();
    let op = read(&f, 1).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    assert_eq!(op.started_at, None, "only mark running sets it");
    assert_eq!(applications(&f).await, []);
    assert_eq!(
        kinds(&events(&f, 1).await),
        [HistoryKind::Accepted, HistoryKind::Finished]
    );
}

#[tokio::test]
async fn supervisor_completion_from_running_marks_the_rest_not_dispatched() {
    let f = setup().await;
    running(&f, 1, APP, &["a", "b", "c"]).await;
    f.repo
        .record_section_outcome(id(1), "a", SectionOutcome::Succeeded, None, None)
        .await
        .unwrap();
    f.repo.complete_by_supervisor(id(1)).await.unwrap();
    let op = read(&f, 1).await;
    assert_eq!(
        outcomes(&op),
        [
            Some(SectionOutcome::Succeeded),
            Some(SectionOutcome::NotDispatched),
            Some(SectionOutcome::NotDispatched)
        ]
    );
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    assert_eq!(
        applications(&f).await.len(),
        1,
        "a running seed writes its record"
    );
}

// ---- expiry completion ----

#[tokio::test]
async fn expiry_leaves_an_unexpired_operation_alone() {
    let f = setup().await;
    accept(&f, 1, APP, OPEN).await;
    assert_eq!(f.repo.expire_if_expired(id(1)).await, Ok(false));
    assert_eq!(f.repo.expire_if_expired(id(9)).await, Ok(false), "unknown");
    assert_eq!(read(&f, 1).await.status, OperationStatus::Accepted);
}

/// A running seed with sections `a`, `b`, `c`, `a` succeeded, whose run
/// deadline is over.
async fn expired_running_seed(f: &Fixture, n: u128) {
    accept(f, n, APP, EXPIRED).await;
    // Mark running is guarded by the deadline, so the running state is
    // written behind the repository's back.
    let mut options = sea_orm::ConnectOptions::new(f.dsn.clone());
    options.max_connections(1);
    let conn = Database::connect(options).await.expect("second connection");
    let sections = json!([
        { "key": "a", "section_type": "gts.test.a.v1~", "outcome": "succeeded" },
        { "key": "b", "section_type": "gts.test.b.v1~" },
        { "key": "c", "section_type": "gts.test.c.v1~" }
    ]);
    let mut stored = serde_json::to_value(report(n, APP, OperationAction::Seed)).unwrap();
    stored["sections"] = sections;
    conn.execute_unprepared(&format!(
        "UPDATE operations SET status = 'running', report = '{stored}' WHERE operation_id = x'{n:032x}'"
    ))
    .await
    .expect("mark running by hand");
}

#[tokio::test]
async fn expiry_of_a_running_seed_loses_the_first_open_section_and_writes_the_record() {
    let f = setup().await;
    expired_running_seed(&f, 1).await;
    assert_eq!(f.repo.expire_if_expired(id(1)).await, Ok(true));
    let op = read(&f, 1).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(
        outcomes(&op),
        [
            Some(SectionOutcome::Succeeded),
            Some(SectionOutcome::Lost),
            Some(SectionOutcome::NotDispatched)
        ]
    );
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    let written = events(&f, 1).await;
    assert_eq!(written[1].content["result"]["reason"], json!("expired"));
    assert_eq!(
        written[1].content["result"]["sections"][1]["outcome"],
        json!("lost")
    );
    assert_eq!(applications(&f).await.len(), 1);
    assert_eq!(
        f.repo.expire_if_expired(id(1)).await,
        Ok(false),
        "runs once"
    );
    assert_eq!(events(&f, 1).await.len(), 2);
}

#[tokio::test]
async fn expiry_of_an_accepted_seed_is_internal_failure_without_a_record() {
    let f = setup().await;
    accept(&f, 1, APP, EXPIRED).await;
    assert_eq!(f.repo.expire_if_expired(id(1)).await, Ok(true));
    let op = read(&f, 1).await;
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    assert_eq!(applications(&f).await, []);
}

#[tokio::test]
async fn read_operation_runs_the_expiry_completion_first() {
    let f = setup().await;
    assert_eq!(f.repo.read_operation(id(9)).await, Ok(None));
    accept(&f, 1, APP, EXPIRED).await;
    assert_eq!(read(&f, 1).await.status, OperationStatus::Completed);
    accept(&f, 2, "other.app", OPEN).await;
    assert_eq!(read(&f, 2).await.status, OperationStatus::Accepted);
}

#[tokio::test]
async fn the_expiry_batch_goes_oldest_deadline_first_within_its_limit() {
    let f = setup().await;
    assert_eq!(f.repo.expire_batch(10).await, Ok(0));
    for n in 1..=3_u128 {
        accept(&f, n, &format!("app.{n}"), EXPIRED).await;
    }
    accept(&f, 4, "open.app", OPEN).await;
    assert_eq!(f.repo.expire_batch(2).await, Ok(2));
    assert_eq!(f.repo.expire_batch(10).await, Ok(1));
    assert_eq!(read(&f, 4).await.status, OperationStatus::Accepted);
}

/// Store a category token the migration's `CHECK` would refuse, on a separate
/// single-connection pool with `CHECK` enforcement off, so a row that does not
/// map back exists.
async fn corrupt_category(f: &Fixture, operation: u128) {
    let mut options = sea_orm::ConnectOptions::new(f.dsn.clone());
    options.max_connections(1);
    let conn = Database::connect(options).await.expect("second connection");
    for sql in [
        "PRAGMA ignore_check_constraints = ON".to_owned(),
        format!(
            "UPDATE operations SET category = 'bogus' WHERE operation_id = x'{operation:032x}'"
        ),
    ] {
        conn.execute_unprepared(&sql).await.expect("corrupt row");
    }
}

#[tokio::test]
async fn the_expiry_batch_goes_past_a_record_that_does_not_map() {
    let f = setup().await;
    accept(&f, 1, "bad.app", EXPIRED).await;
    accept(&f, 2, "good.app", EXPIRED).await;
    corrupt_category(&f, 1).await;
    assert_eq!(f.repo.expire_batch(10).await, Ok(1));
    assert_eq!(read(&f, 2).await.status, OperationStatus::Completed);
    assert_eq!(events(&f, 1).await.len(), 1, "the bad record is untouched");
}

#[tokio::test]
async fn the_expiry_batch_fails_when_every_attempted_record_fails() {
    let f = setup().await;
    accept(&f, 1, "bad.app", EXPIRED).await;
    corrupt_category(&f, 1).await;
    assert_eq!(
        f.repo.expire_batch(10).await,
        Err(StorageError::Unavailable)
    );
}

#[tokio::test]
async fn a_batch_never_handles_more_than_the_maintenance_batch_size() {
    let f = setup().await;
    let total = MAINTENANCE_BATCH_SIZE + 5;
    for n in 1..=u128::from(total) {
        accept(&f, n, &format!("app.{n}"), EXPIRED).await;
    }
    assert_eq!(
        f.repo.expire_batch(u64::MAX).await,
        Ok(MAINTENANCE_BATCH_SIZE)
    );
    assert_eq!(f.repo.expire_batch(u64::MAX).await, Ok(5));
    assert_eq!(
        f.repo.purge(Duration::ZERO, u64::MAX).await,
        Ok(MAINTENANCE_BATCH_SIZE)
    );
    assert_eq!(f.repo.purge(Duration::ZERO, u64::MAX).await, Ok(5));
}

// ---- purge ----

#[tokio::test]
async fn purge_removes_completed_records_past_retention_only() {
    let f = setup().await;
    running(&f, 1, APP, &[]).await;
    f.repo
        .finish(
            id(1),
            &final_report(1, APP, OperationAction::Seed),
            RequestCategory::Succeeded,
        )
        .await
        .unwrap();
    accept(&f, 2, "open.app", OPEN).await;

    assert_eq!(
        f.repo.purge(OPEN, 10).await,
        Ok(0),
        "not yet past retention"
    );
    assert_eq!(
        f.repo.purge(Duration::ZERO, 10).await,
        Ok(1),
        "at the retention boundary"
    );
    assert_eq!(f.repo.read_operation(id(1)).await, Ok(None));
    assert_eq!(
        read(&f, 2).await.status,
        OperationStatus::Accepted,
        "open is kept"
    );
    assert_eq!(events(&f, 1).await.len(), 2, "the history is kept");
    assert_eq!(
        applications(&f).await.len(),
        1,
        "the application record is kept"
    );
    assert_eq!(
        f.repo.purge(Duration::MAX, 10).await,
        Ok(0),
        "a cutoff before time"
    );
}

// ---- history and application pages ----

#[tokio::test]
async fn the_history_page_is_in_event_id_order_with_a_cursor_and_a_filter() {
    let f = setup().await;
    for (e, correlation) in [(0x30, 0xA), (0x10, 0xB), (0x20, 0xA)] {
        f.repo
            .record_refused(event(e, HistoryKind::Refused, correlation))
            .await
            .unwrap();
    }
    let ids = |page: Vec<HistoryEvent>| page.into_iter().map(|e| e.event_id).collect::<Vec<_>>();
    assert_eq!(
        ids(f.repo.history_page(None, None, 10).await.unwrap()),
        [id(0x10), id(0x20), id(0x30)]
    );
    assert_eq!(
        ids(f.repo.history_page(None, Some(id(0x10)), 1).await.unwrap()),
        [id(0x20)]
    );
    assert_eq!(
        ids(f
            .repo
            .history_page(Some(vec![id(0xA)]), None, 10)
            .await
            .unwrap()),
        [id(0x20), id(0x30)]
    );
    assert_eq!(
        ids(f
            .repo
            .history_page(Some(vec![id(0xA), id(0xB)]), None, 10)
            .await
            .unwrap()),
        [id(0x10), id(0x20), id(0x30)]
    );
    assert_eq!(
        f.repo.history_page(Some(vec![id(0xF)]), None, 10).await,
        Ok(vec![])
    );
}

#[tokio::test]
async fn the_application_page_is_in_id_order_with_a_cursor_and_a_filter() {
    let f = setup().await;
    for (n, app) in [(1, "c.app"), (2, "a.app"), (3, "b.app")] {
        running(&f, n, app, &[]).await;
        f.repo
            .finish(
                id(n),
                &final_report(n, app, OperationAction::Seed),
                RequestCategory::Succeeded,
            )
            .await
            .unwrap();
    }
    let names = |page: Vec<ApplicationRecord>| {
        page.into_iter()
            .map(|r| r.application_id)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        names(f.repo.applications_page(None, None, 10).await.unwrap()),
        ["a.app", "b.app", "c.app"]
    );
    assert_eq!(
        names(
            f.repo
                .applications_page(None, Some("a.app".to_owned()), 1)
                .await
                .unwrap()
        ),
        ["b.app"]
    );
    assert_eq!(
        names(
            f.repo
                .applications_page(Some(vec!["c.app".to_owned(), "a.app".to_owned()]), None, 10)
                .await
                .unwrap()
        ),
        ["a.app", "c.app"]
    );
}

#[tokio::test]
async fn pages_are_capped_at_their_maximum() {
    let f = setup().await;
    for e in 1..=u128::from(HISTORY_PAGE_MAX_LIMIT) + 1 {
        f.repo
            .record_refused(event(e, HistoryKind::Refused, 0xA))
            .await
            .unwrap();
    }
    assert_eq!(
        f.repo
            .history_page(None, None, u64::MAX)
            .await
            .unwrap()
            .len(),
        usize::try_from(HISTORY_PAGE_MAX_LIMIT).unwrap()
    );
}

// ---- storage timeout ----

/// Hold the only pooled connection until the returned sender fires.
async fn hold_connection(
    db: &Arc<DBProvider<DbError>>,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), DbError>>,
) {
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let (started, running) = tokio::sync::oneshot::channel::<()>();
    let db = Arc::clone(db);
    let holder = tokio::spawn(async move {
        db.transaction(move |_tx| {
            Box::pin(async move {
                started.send(()).ok();
                wait.await.ok();
                Ok::<(), DbError>(())
            })
        })
        .await
    });
    running.await.expect("the holder started");
    (release, holder)
}

#[tokio::test]
async fn a_slow_store_gives_unavailable_within_the_timeout() {
    let f = setup_with(Duration::from_millis(200)).await;
    let (release, holder) = hold_connection(&f.db).await;

    let started = std::time::Instant::now();
    assert_eq!(
        f.repo.open_operation(APP).await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(
        f.repo
            .accept(
                new_op(1, APP, OperationAction::Seed, OPEN),
                accepted_event(1)
            )
            .await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(
        f.repo
            .record_refused(event(0x9001, HistoryKind::Refused, 0x9000))
            .await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(
        f.repo.mark_running(id(1), &[]).await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(
        f.repo.history_page(None, None, 10).await,
        Err(StorageError::Unavailable)
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "each call is bounded by storage_call_timeout"
    );

    release.send(()).unwrap();
    holder.await.unwrap().unwrap();
    // Not sticky, and nothing from the timed-out accept was committed.
    assert_eq!(f.repo.open_operation(APP).await, Ok(None));
    assert_eq!(f.repo.read_operation(id(1)).await, Ok(None));
}

// ---- concurrency ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_accepts_for_one_application_have_exactly_one_winner() {
    let f = setup().await;
    let mut tasks = Vec::new();
    for n in 1..=8_u128 {
        let repo = Arc::clone(&f.repo);
        tasks.push(tokio::spawn(async move {
            repo.accept(
                new_op(n, APP, OperationAction::Seed, OPEN),
                accepted_event(n),
            )
            .await
        }));
    }
    let mut won = Vec::new();
    let mut violated = 0;
    for (n, task) in (1..=8_u128).zip(tasks) {
        match task.await.unwrap() {
            Ok(_) => won.push(n),
            Err(StorageError::IndexViolated) => violated += 1,
            Err(other) => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(won.len(), 1, "exactly one winner: {won:?}");
    assert_eq!(violated, 7);
    for n in 1..=8_u128 {
        assert_eq!(
            events(&f, n).await.len(),
            usize::from(n == won[0]),
            "events of {n}"
        );
    }
}

/// Run `n` accepts at once, released together by a barrier; `app` names the
/// application of accept `i`.
async fn burst_accepts(
    f: &Fixture,
    n: u128,
    app: impl Fn(u128) -> String,
) -> Vec<Result<Accepted, StorageError>> {
    let barrier = Arc::new(tokio::sync::Barrier::new(usize::try_from(n).unwrap()));
    let mut tasks = Vec::new();
    for i in 1..=n {
        let repo = Arc::clone(&f.repo);
        let barrier = Arc::clone(&barrier);
        let app = app(i);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repo.accept(
                new_op(i, &app, OperationAction::Seed, OPEN),
                accepted_event(i),
            )
            .await
        }));
    }
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap());
    }
    results
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_accepts_for_different_applications_all_succeed_on_a_file_database() {
    let f = setup_file(8, Duration::from_secs(30)).await;
    let results = burst_accepts(&f, 16, |i| format!("app.{i}")).await;
    assert!(
        results.iter().all(Result::is_ok),
        "contention is retried, never Unavailable: {results:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_accepts_for_one_application_on_a_file_database_have_one_winner() {
    let f = setup_file(4, Duration::from_secs(30)).await;
    let results = burst_accepts(&f, 16, |_| APP.to_owned()).await;
    let won = results.iter().filter(|r| r.is_ok()).count();
    let violated = results
        .iter()
        .filter(|r| **r == Err(StorageError::IndexViolated))
        .count();
    assert_eq!(won, 1, "{results:?}");
    assert_eq!(violated, 15, "no Unavailable among the losers: {results:?}");
}

// ---- stored timestamps ----

async fn stored_text(f: &Fixture, table: &str, column: &str) -> Vec<String> {
    let mut options = sea_orm::ConnectOptions::new(f.dsn.clone());
    options.max_connections(1);
    let conn = Database::connect(options).await.expect("second connection");
    let sql = format!("SELECT {column} AS t FROM {table} ORDER BY {column}");
    conn.query_all_raw(sea_orm::Statement::from_string(
        sea_orm::DbBackend::Sqlite,
        sql,
    ))
    .await
    .expect("read stored text")
    .iter()
    .map(|row| row.try_get::<String>("", "t").expect("text"))
    .collect()
}

#[tokio::test]
async fn sqlite_stores_timestamps_as_fixed_width_utc_text() {
    let f = setup().await;
    accept(&f, 1, APP, Duration::from_millis(1500)).await;
    let shape =
        |text: &str| text.len() == 23 && text.as_bytes()[10] == b' ' && text.as_bytes()[19] == b'.';
    for (table, column) in [
        ("operations", "accepted_at"),
        ("operations", "run_deadline"),
        ("operation_history", "written_at"),
    ] {
        let stored = stored_text(&f, table, column).await;
        assert!(
            stored.iter().all(|t| shape(t)),
            "{table}.{column}: {stored:?}"
        );
    }
}

// ---- a timeout in the middle of a transaction ----

#[tokio::test]
async fn a_timeout_after_the_first_statement_commits_nothing() {
    let f = setup_file(2, Duration::from_millis(300)).await;
    let (release, holder) = {
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let (started, running) = tokio::sync::oneshot::channel::<()>();
        let db = Arc::clone(&f.db);
        let holder = tokio::spawn(async move {
            // The second connection takes the write lock and keeps it.
            db.transaction(move |tx| {
                Box::pin(async move {
                    let held = event(0x9001, HistoryKind::Refused, 0x9000);
                    insert_event(tx, DbBackend::Sqlite, &held, OffsetDateTime::now_utc()).await?;
                    started.send(()).ok();
                    wait.await.ok();
                    Ok::<(), DbError>(())
                })
            })
            .await
        });
        running.await.expect("the holder holds the write lock");
        (release, holder)
    };

    let started = std::time::Instant::now();
    assert_eq!(
        f.repo
            .accept(
                new_op(1, APP, OperationAction::Seed, OPEN),
                accepted_event(1)
            )
            .await,
        Err(StorageError::Unavailable)
    );
    assert!(started.elapsed() < Duration::from_secs(4), "bounded");

    release.send(()).unwrap();
    holder.await.unwrap().unwrap();
    assert_eq!(f.repo.open_operation(APP).await, Ok(None));
    assert_eq!(events(&f, 1).await, []);
    assert_eq!(events(&f, 0x9000).await.len(), 1, "the holder's own write");
}

// ---- logs carry a class and the call, never the application id ----

const SECRET_APP: &str = "very-secret-application-id.example";

#[tokio::test]
#[traced_test]
async fn a_failed_step_logs_its_class_and_call_but_no_application_id() {
    let f = setup().await;
    accept(&f, 1, "other.app", OPEN).await;
    let mut clash = accepted_event(2);
    clash.event_id = id(0x1001);
    assert_eq!(
        f.repo
            .accept(new_op(2, SECRET_APP, OperationAction::Seed, OPEN), clash)
            .await,
        Err(StorageError::Unavailable)
    );
    assert!(logs_contain("class=\"constraint\""));
    assert!(logs_contain("call=\"accept\""));
    assert!(logs_contain("storage step failed"));
    assert!(!logs_contain(SECRET_APP));
}

#[tokio::test]
#[traced_test]
async fn a_timeout_logs_its_class_and_call_but_no_application_id() {
    let f = setup_with(Duration::from_millis(100)).await;
    let (release, holder) = hold_connection(&f.db).await;
    assert_eq!(
        f.repo.open_operation(SECRET_APP).await,
        Err(StorageError::Unavailable)
    );
    release.send(()).unwrap();
    holder.await.unwrap().unwrap();
    assert!(logs_contain("class=\"timeout\""));
    assert!(logs_contain("call=\"open_operation\""));
    assert!(!logs_contain(SECRET_APP));
}

#[tokio::test]
#[traced_test]
async fn a_row_that_does_not_map_logs_the_row_mapping_class() {
    let f = setup().await;
    accept(&f, 1, SECRET_APP, EXPIRED).await;
    corrupt_category(&f, 1).await;
    assert_eq!(
        f.repo.expire_if_expired(id(1)).await,
        Err(StorageError::Unavailable)
    );
    assert!(logs_contain("class=\"row_mapping\""));
    assert!(logs_contain("call=\"expire_if_expired\""));
    assert!(!logs_contain(SECRET_APP));
}

#[test]
fn error_classes_come_from_the_error_kind() {
    use sea_orm::{DbBackend, DbErr, RuntimeErr};
    let sea = |e: DbErr| DbError::Sea(e);
    let class = |e: &DbError| error_class(DbBackend::Sqlite, e);
    assert_eq!(
        class(&sea(DbErr::ConnectionAcquire(
            sea_orm::ConnAcquireErr::ConnectionClosed
        ))),
        "connection"
    );
    assert_eq!(
        class(&sea(DbErr::ConnectionAcquire(
            sea_orm::ConnAcquireErr::Timeout
        ))),
        "timeout"
    );
    assert_eq!(
        class(&sea(DbErr::Custom(
            "error returned from database: (code: 5) database is locked".to_owned()
        ))),
        "contention"
    );
    assert_eq!(
        class(&sea(DbErr::Conn(RuntimeErr::Internal("x".to_owned())))),
        "connection"
    );
    assert_eq!(class(&DbError::from(RowError)), "row_mapping");
    assert_eq!(class(&sea(DbErr::Custom("anything".to_owned()))), "other");
    assert_eq!(class(&DbError::InvalidParameter("x".to_owned())), "other");
}
