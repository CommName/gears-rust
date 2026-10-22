//! SQLite-backed tests of [`SeaOrmOperationRepository`]: the real `SeaORM`
//! paths against an in-memory database built from the gear's own migration.
//! Every call passes its own `now`, so deadlines are exact and no test sleeps.
#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::{
    OperationAction, OutcomeReport, ReportApplication, RequestCategory, SectionOutcome,
    SectionOutcomeEntry,
};
use sea_orm::{ConnectionTrait, Database};
use sea_orm_migration::MigratorTrait;
use serde_json::json;
use time::OffsetDateTime;
use time::macros::datetime;
use toolkit_db::migration_runner::run_migrations_for_testing;
use toolkit_db::{ConnectOpts, DBProvider, DbError, connect_db};
use tracing_test::traced_test;
use uuid::Uuid;

use super::*;
use crate::domain::audit::AuditKind;
use crate::domain::operation::OperationStatus;
use crate::infra::storage::Migrator;

const T0: OffsetDateTime = datetime!(2026-10-22 10:00:00 UTC);
const APP: &str = "acme.app";

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

fn secs(n: i64) -> time::Duration {
    time::Duration::seconds(n)
}

fn report(op: u128, app: &str) -> OutcomeReport {
    OutcomeReport {
        operation_id: id(op),
        action: OperationAction::Seed,
        category: None,
        interrupted: false,
        application: ReportApplication {
            id: app.to_owned(),
            version: Some("1.0.0".to_owned()),
        },
        sections: vec![],
        problems: vec![],
        unavailable: vec![],
    }
}

fn with_section(mut report: OutcomeReport, key: &str) -> OutcomeReport {
    report.sections.push(SectionOutcomeEntry {
        key: key.to_owned(),
        outcome: SectionOutcome::Succeeded,
        schema_version: Some("1".to_owned()),
        detail: None,
        location: None,
    });
    report
}

/// Operation `n` of `app`, accepted at `T0`, due `deadline_secs` after `T0`.
/// Its accepted event id is `n + 0x2000`, its finished event id `n + 0x1000`.
fn new_op(n: u128, app: &str, deadline_secs: i64) -> NewOperation {
    NewOperation {
        operation_id: id(n),
        application_id: app.to_owned(),
        action: OperationAction::Seed,
        accepted_at: T0,
        run_deadline: T0 + secs(deadline_secs),
        finished_event_id: id(n + 0x1000),
        report: report(n, app),
    }
}

fn accepted_event(n: u128) -> AuditEvent {
    AuditEvent {
        event_id: id(n + 0x2000),
        kind: AuditKind::Accepted,
        correlation_id: id(n),
        action: OperationAction::Seed,
        written_at: T0,
        content: json!({ "marker": "accepted" }),
    }
}

fn finished_event(n: u128) -> AuditEvent {
    AuditEvent {
        event_id: id(n + 0x1000),
        kind: AuditKind::Finished,
        correlation_id: id(n),
        action: OperationAction::Seed,
        written_at: T0 + secs(30),
        content: json!({ "marker": "finished" }),
    }
}

fn refused_event(event: u128, correlation: u128) -> AuditEvent {
    AuditEvent {
        event_id: id(event),
        kind: AuditKind::Refused,
        correlation_id: id(correlation),
        action: OperationAction::Seed,
        written_at: T0,
        content: json!({ "marker": "refused" }),
    }
}

async fn accept(f: &Fixture, n: u128, app: &str, deadline_secs: i64) {
    f.repo
        .accept(new_op(n, app, deadline_secs), accepted_event(n), T0)
        .await
        .expect("accept");
}

async fn read(f: &Fixture, n: u128, now: OffsetDateTime) -> Operation {
    f.repo
        .read_operation(id(n), now)
        .await
        .expect("read")
        .expect("operation exists")
}

async fn events(f: &Fixture, n: u128) -> Vec<AuditEvent> {
    f.repo.audit_events(id(n)).await.expect("events")
}

// ---- accept ----

#[tokio::test]
async fn accept_inserts_the_operation_and_the_accepted_event_together() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;

    let op = read(&f, 1, T0 + secs(1)).await;
    assert_eq!(op.operation_id, id(1));
    assert_eq!(op.application_id, APP);
    assert_eq!(op.action, OperationAction::Seed);
    assert_eq!(op.status, OperationStatus::Accepted);
    assert_eq!(op.category, None);
    assert_eq!(op.report, report(1, APP));
    assert_eq!(op.accepted_at, T0);
    assert_eq!(op.started_at, None);
    assert_eq!(op.finished_at, None);
    assert_eq!(op.run_deadline, T0 + secs(60));
    assert_eq!(op.finished_event_id, id(0x1001));
    assert_eq!(events(&f, 1).await, [accepted_event(1)]);
}

#[tokio::test]
async fn accept_stores_an_uninstall() {
    let f = setup().await;
    let mut new = new_op(1, APP, 60);
    new.action = OperationAction::Uninstall;
    let mut event = accepted_event(1);
    event.action = OperationAction::Uninstall;
    f.repo.accept(new, event.clone(), T0).await.unwrap();
    assert_eq!(read(&f, 1, T0).await.action, OperationAction::Uninstall);
    assert_eq!(events(&f, 1).await, [event]);
}

#[tokio::test]
async fn second_accept_for_the_same_application_while_open_is_index_violated() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let second = f
        .repo
        .accept(new_op(2, APP, 60), accepted_event(2), T0 + secs(1))
        .await;
    assert_eq!(second, Err(StorageError::IndexViolated));
}

#[tokio::test]
async fn a_failed_accept_inserts_nothing() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let second = f
        .repo
        .accept(new_op(2, APP, 60), accepted_event(2), T0 + secs(1))
        .await;
    assert!(second.is_err());
    assert_eq!(f.repo.read_operation(id(2), T0).await, Ok(None));
    assert_eq!(events(&f, 2).await, []);
    // The winner is untouched.
    assert_eq!(
        read(&f, 1, T0 + secs(1)).await.status,
        OperationStatus::Accepted
    );
    assert_eq!(events(&f, 1).await, [accepted_event(1)]);
}

#[tokio::test]
async fn a_failing_event_insert_rolls_the_operation_back() {
    let f = setup().await;
    accept(&f, 1, "other.app", 60).await;
    // The accepted event reuses an existing event id, so its insert fails
    // after the operation insert succeeded.
    let mut clash = accepted_event(2);
    clash.event_id = id(0x2001);
    let result = f.repo.accept(new_op(2, APP, 60), clash, T0).await;
    assert!(result.is_err(), "{result:?}");
    assert_eq!(f.repo.read_operation(id(2), T0).await, Ok(None));
    assert_eq!(f.repo.open_operation(APP, T0).await, Ok(None));
}

#[tokio::test]
async fn accept_for_another_application_succeeds_while_one_is_open() {
    let f = setup().await;
    accept(&f, 1, "one.app", 60).await;
    accept(&f, 2, "two.app", 60).await;
    assert_eq!(read(&f, 2, T0).await.application_id, "two.app");
}

#[tokio::test]
async fn accept_over_an_unexpired_open_operation_leaves_it_alone() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let now = T0 + secs(59);
    let second = f
        .repo
        .accept(new_op(2, APP, 120), accepted_event(2), now)
        .await;
    assert_eq!(second, Err(StorageError::IndexViolated));
    assert_eq!(read(&f, 1, now).await.status, OperationStatus::Accepted);
    assert_eq!(events(&f, 1).await.len(), 1, "no finished event");
}

#[tokio::test]
async fn accept_over_an_expired_open_operation_interrupts_it_then_succeeds() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let now = T0 + secs(61);
    f.repo
        .accept(new_op(2, APP, 120), accepted_event(2), now)
        .await
        .expect("an expired open operation does not block acceptance");

    let old = read(&f, 1, now).await;
    assert_eq!(old.status, OperationStatus::Interrupted);
    assert_eq!(old.category, None);
    assert_eq!(old.finished_at, Some(now));
    assert!(old.report.interrupted);
    assert_eq!(old.report.category, None);

    let old_events = events(&f, 1).await;
    assert_eq!(old_events.len(), 2);
    let finished = &old_events[1];
    assert_eq!(finished.kind, AuditKind::Finished);
    assert_eq!(
        finished.event_id,
        id(0x1001),
        "the record's finished event id"
    );
    assert_eq!(finished.correlation_id, id(1));
    assert_eq!(finished.written_at, now);
    assert_eq!(finished.content["result"]["interrupted"], json!(true));
    assert!(finished.content["result"].get("category").is_none());

    assert_eq!(read(&f, 2, now).await.status, OperationStatus::Accepted);
    assert_eq!(
        f.repo
            .open_operation(APP, now)
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        id(2)
    );
}

// ---- open_operation ----

#[tokio::test]
async fn open_operation_is_none_without_an_open_record() {
    let f = setup().await;
    assert_eq!(f.repo.open_operation(APP, T0).await, Ok(None));
}

#[tokio::test]
async fn open_operation_reports_expiry_at_the_deadline() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let open = |now| {
        let repo = Arc::clone(&f.repo);
        async move { repo.open_operation(APP, now).await.unwrap().unwrap() }
    };
    let before = open(T0 + secs(59)).await;
    assert_eq!(
        before,
        OpenOperation {
            operation_id: id(1),
            expired: false
        }
    );
    let at = open(T0 + secs(60)).await;
    assert!(at.expired, "run_deadline <= now");
    let after = open(T0 + secs(61)).await;
    assert!(after.expired);
}

#[tokio::test]
async fn open_operation_sees_a_running_record() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo.mark_running(id(1), T0 + secs(1)).await.unwrap();
    assert_eq!(
        f.repo.open_operation(APP, T0 + secs(2)).await,
        Ok(Some(OpenOperation {
            operation_id: id(1),
            expired: false
        }))
    );
}

#[tokio::test]
async fn open_operation_ignores_final_records() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            finished_event(1),
            T0 + secs(30),
        )
        .await
        .unwrap();
    assert_eq!(f.repo.open_operation(APP, T0 + secs(31)).await, Ok(None));
    // And acceptance for the application is free again.
    f.repo
        .accept(new_op(2, APP, 60), accepted_event(2), T0 + secs(31))
        .await
        .unwrap();
}

#[tokio::test]
async fn application_ids_compare_exactly() {
    let f = setup().await;
    accept(&f, 1, "a", 60).await;
    // Binary, no-pad collation: case and trailing space distinguish ids.
    accept(&f, 2, "A", 60).await;
    accept(&f, 3, "a ", 60).await;
    assert_eq!(
        f.repo
            .open_operation("a", T0)
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        id(1)
    );
    assert_eq!(
        f.repo
            .open_operation("A", T0)
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        id(2)
    );
    assert_eq!(
        f.repo
            .open_operation("a ", T0)
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        id(3)
    );
    assert_eq!(f.repo.open_operation("b", T0).await, Ok(None));
}

// ---- lost race and refusal ----

#[tokio::test]
async fn lost_race_reads_the_winner_and_records_a_refused_event_naming_it() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let winner = f
        .repo
        .refuse_after_lost_race(
            APP,
            Box::new(|winner| {
                let mut event = refused_event(0x9001, 0x9000);
                event.content = json!({ "busy_operation_id": winner.to_string() });
                event
            }),
        )
        .await
        .unwrap();
    assert_eq!(winner, id(1));
    let refused = events(&f, 0x9000).await;
    assert_eq!(refused.len(), 1);
    assert_eq!(refused[0].kind, AuditKind::Refused);
    assert_eq!(
        refused[0].content,
        json!({ "busy_operation_id": id(1).to_string() })
    );
}

#[tokio::test]
async fn lost_race_without_an_open_winner_is_unavailable_and_inserts_nothing() {
    let f = setup().await;
    let result = f
        .repo
        .refuse_after_lost_race(APP, Box::new(|_| refused_event(0x9001, 0x9000)))
        .await;
    assert_eq!(result, Err(StorageError::Unavailable));
    assert_eq!(events(&f, 0x9000).await, []);
}

#[tokio::test]
async fn record_refused_inserts_one_event_and_no_operation() {
    let f = setup().await;
    let event = refused_event(0x9001, 0x9000);
    f.repo.record_refused(event.clone()).await.unwrap();
    assert_eq!(events(&f, 0x9000).await, [event]);
    assert_eq!(f.repo.read_operation(id(0x9000), T0).await, Ok(None));
    assert_eq!(f.repo.open_operation(APP, T0).await, Ok(None));
}

// ---- guarded writes ----

#[tokio::test]
async fn mark_running_moves_accepted_to_running_and_sets_started_at() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let now = T0 + secs(5);
    f.repo.mark_running(id(1), now).await.unwrap();
    let op = read(&f, 1, now).await;
    assert_eq!(op.status, OperationStatus::Running);
    assert_eq!(op.started_at, Some(now));
    assert_eq!(op.finished_at, None);
}

#[tokio::test]
async fn mark_running_after_the_deadline_is_operation_final() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    assert_eq!(
        f.repo.mark_running(id(1), T0 + secs(60)).await,
        Err(StorageError::OperationFinal)
    );
    assert_eq!(
        read(&f, 1, T0 + secs(59)).await.status,
        OperationStatus::Accepted
    );
}

#[tokio::test]
async fn mark_running_on_an_unknown_operation_is_operation_final() {
    let f = setup().await;
    assert_eq!(
        f.repo.mark_running(id(404), T0).await,
        Err(StorageError::OperationFinal)
    );
}

#[tokio::test]
async fn record_report_replaces_the_report() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo.mark_running(id(1), T0 + secs(1)).await.unwrap();
    let updated = with_section(report(1, APP), "billing");
    f.repo
        .record_report(id(1), &updated, T0 + secs(2))
        .await
        .unwrap();
    let op = read(&f, 1, T0 + secs(3)).await;
    assert_eq!(op.report, updated);
    assert_eq!(op.status, OperationStatus::Running);
}

#[tokio::test]
async fn record_report_after_the_deadline_is_operation_final() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let updated = with_section(report(1, APP), "billing");
    assert_eq!(
        f.repo.record_report(id(1), &updated, T0 + secs(60)).await,
        Err(StorageError::OperationFinal)
    );
    assert_eq!(read(&f, 1, T0 + secs(59)).await.report, report(1, APP));
}

#[tokio::test]
async fn record_report_after_a_final_write_is_operation_final() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            finished_event(1),
            T0 + secs(10),
        )
        .await
        .unwrap();
    let late = with_section(report(1, APP), "billing");
    assert_eq!(
        f.repo.record_report(id(1), &late, T0 + secs(11)).await,
        Err(StorageError::OperationFinal)
    );
    assert_eq!(
        read(&f, 1, T0 + secs(12)).await.report,
        OutcomeReport {
            category: Some(RequestCategory::Succeeded),
            ..report(1, APP)
        }
    );
}

#[tokio::test]
async fn mark_running_after_a_final_write_is_operation_final() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            finished_event(1),
            T0 + secs(10),
        )
        .await
        .unwrap();
    assert_eq!(
        f.repo.mark_running(id(1), T0 + secs(11)).await,
        Err(StorageError::OperationFinal)
    );
}

// ---- finish ----

#[tokio::test]
async fn finish_completes_the_operation_and_inserts_the_finished_event() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo.mark_running(id(1), T0 + secs(1)).await.unwrap();
    let done = with_section(report(1, APP), "billing");
    let now = T0 + secs(30);
    f.repo
        .finish(
            id(1),
            &done,
            RequestCategory::Succeeded,
            finished_event(1),
            now,
        )
        .await
        .unwrap();

    let op = read(&f, 1, now).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert_eq!(op.finished_at, Some(now));
    assert_eq!(op.started_at, Some(T0 + secs(1)));
    // The stored report carries the derived category.
    assert_eq!(op.report.category, Some(RequestCategory::Succeeded));
    assert_eq!(
        op.report,
        OutcomeReport {
            category: Some(RequestCategory::Succeeded),
            ..done
        }
    );
    assert_eq!(events(&f, 1).await, [accepted_event(1), finished_event(1)]);
}

#[tokio::test]
async fn finish_from_accepted_is_allowed() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::TemporarilyUnavailable,
            finished_event(1),
            T0 + secs(2),
        )
        .await
        .unwrap();
    let op = read(&f, 1, T0 + secs(3)).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(op.category, Some(RequestCategory::TemporarilyUnavailable));
}

#[tokio::test]
async fn the_first_final_write_wins() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            finished_event(1),
            T0 + secs(10),
        )
        .await
        .unwrap();

    let mut second = finished_event(1);
    second.event_id = id(0x7777);
    let result = f
        .repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::InternalFailure,
            second,
            T0 + secs(11),
        )
        .await;
    assert_eq!(result, Err(StorageError::OperationFinal));

    let op = read(&f, 1, T0 + secs(12)).await;
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert_eq!(op.finished_at, Some(T0 + secs(10)));
    assert_eq!(events(&f, 1).await.len(), 2, "no extra event");
}

#[tokio::test]
async fn finish_after_the_deadline_is_operation_final_with_no_event() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let result = f
        .repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            finished_event(1),
            T0 + secs(60),
        )
        .await;
    assert_eq!(result, Err(StorageError::OperationFinal));
    assert_eq!(events(&f, 1).await, [accepted_event(1)]);
    assert_eq!(
        read(&f, 1, T0 + secs(59)).await.status,
        OperationStatus::Accepted
    );
}

#[tokio::test]
async fn finish_after_an_interruption_is_operation_final() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    assert_eq!(
        f.repo.interrupt_if_expired(id(1), T0 + secs(61)).await,
        Ok(true)
    );
    let result = f
        .repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            finished_event(1),
            T0 + secs(62),
        )
        .await;
    assert_eq!(result, Err(StorageError::OperationFinal));
    assert_eq!(
        events(&f, 1).await.len(),
        2,
        "accepted and the interruption's finished event"
    );
    assert_eq!(
        read(&f, 1, T0 + secs(63)).await.status,
        OperationStatus::Interrupted
    );
}

#[tokio::test]
async fn a_failing_finished_event_insert_rolls_the_update_back() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let mut clash = finished_event(1);
    clash.event_id = id(0x2001); // the accepted event's id
    let result = f
        .repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            clash,
            T0 + secs(10),
        )
        .await;
    assert!(result.is_err(), "{result:?}");
    let op = read(&f, 1, T0 + secs(11)).await;
    assert_eq!(op.status, OperationStatus::Accepted);
    assert_eq!(op.category, None);
    assert_eq!(op.finished_at, None);
}

// ---- supervisor completion ----

fn failed_report(n: u128) -> OutcomeReport {
    let mut report = report(n, APP);
    report.category = Some(RequestCategory::InternalFailure);
    report.sections.push(SectionOutcomeEntry {
        key: "billing".to_owned(),
        outcome: SectionOutcome::NotDispatched,
        schema_version: None,
        detail: None,
        location: None,
    });
    report
}

#[tokio::test]
async fn supervisor_completion_from_accepted_sets_started_at_and_internal_failure() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let now = T0 + secs(20);
    f.repo
        .complete_by_supervisor(id(1), &failed_report(1), finished_event(1), now)
        .await
        .unwrap();
    let op = read(&f, 1, now).await;
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    assert_eq!(op.started_at, Some(now), "set because it was null");
    assert_eq!(op.finished_at, Some(now));
    assert_eq!(op.report, failed_report(1));
    assert_eq!(events(&f, 1).await, [accepted_event(1), finished_event(1)]);
}

#[tokio::test]
async fn supervisor_completion_keeps_an_existing_started_at() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo.mark_running(id(1), T0 + secs(5)).await.unwrap();
    f.repo
        .complete_by_supervisor(id(1), &failed_report(1), finished_event(1), T0 + secs(20))
        .await
        .unwrap();
    let op = read(&f, 1, T0 + secs(21)).await;
    assert_eq!(op.started_at, Some(T0 + secs(5)));
    assert_eq!(op.finished_at, Some(T0 + secs(20)));
}

#[tokio::test]
async fn supervisor_completion_after_a_final_write_is_operation_final() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            finished_event(1),
            T0 + secs(10),
        )
        .await
        .unwrap();
    let mut second = finished_event(1);
    second.event_id = id(0x7777);
    let result = f
        .repo
        .complete_by_supervisor(id(1), &failed_report(1), second, T0 + secs(11))
        .await;
    assert_eq!(result, Err(StorageError::OperationFinal));
    assert_eq!(
        read(&f, 1, T0 + secs(12)).await.category,
        Some(RequestCategory::Succeeded)
    );
    assert_eq!(events(&f, 1).await.len(), 2);
}

// ---- interruption ----

#[tokio::test]
async fn interrupt_if_expired_does_not_touch_a_record_that_is_not_expired() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    assert_eq!(
        f.repo.interrupt_if_expired(id(1), T0 + secs(59)).await,
        Ok(false)
    );
    assert_eq!(
        read(&f, 1, T0 + secs(59)).await.status,
        OperationStatus::Accepted
    );
    assert_eq!(events(&f, 1).await.len(), 1);
}

#[tokio::test]
async fn interrupt_if_expired_keeps_the_section_outcomes_so_far() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo.mark_running(id(1), T0 + secs(1)).await.unwrap();
    let partial = with_section(report(1, APP), "billing");
    f.repo
        .record_report(id(1), &partial, T0 + secs(2))
        .await
        .unwrap();

    let now = T0 + secs(60);
    assert_eq!(f.repo.interrupt_if_expired(id(1), now).await, Ok(true));
    let op = read(&f, 1, now).await;
    assert_eq!(op.status, OperationStatus::Interrupted);
    assert_eq!(op.category, None);
    assert_eq!(op.finished_at, Some(now));
    assert_eq!(op.report.sections, partial.sections);
    assert!(op.report.interrupted);
    assert_eq!(op.report.category, None);

    let finished = events(&f, 1).await.pop().unwrap();
    assert_eq!(finished.kind, AuditKind::Finished);
    assert_eq!(finished.event_id, id(0x1001));
    assert_eq!(finished.content["result"]["interrupted"], json!(true));
    assert_eq!(
        finished.content["result"]["sections"],
        json!([{ "key": "billing", "outcome": "succeeded", "schema_version": "1" }])
    );
}

#[tokio::test]
async fn interrupt_if_expired_runs_once() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    assert_eq!(
        f.repo.interrupt_if_expired(id(1), T0 + secs(61)).await,
        Ok(true)
    );
    assert_eq!(
        f.repo.interrupt_if_expired(id(1), T0 + secs(62)).await,
        Ok(false)
    );
    assert_eq!(events(&f, 1).await.len(), 2, "one finished event");
    assert_eq!(
        read(&f, 1, T0 + secs(63)).await.finished_at,
        Some(T0 + secs(61))
    );
}

#[tokio::test]
async fn interrupt_if_expired_leaves_a_completed_record() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    f.repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            finished_event(1),
            T0 + secs(10),
        )
        .await
        .unwrap();
    assert_eq!(
        f.repo.interrupt_if_expired(id(1), T0 + secs(100)).await,
        Ok(false)
    );
    assert_eq!(
        read(&f, 1, T0 + secs(100)).await.status,
        OperationStatus::Completed
    );
}

#[tokio::test]
async fn interrupt_if_expired_on_an_unknown_operation_is_false() {
    let f = setup().await;
    assert_eq!(f.repo.interrupt_if_expired(id(404), T0).await, Ok(false));
}

#[tokio::test]
async fn batch_interruption_respects_the_limit_and_goes_oldest_deadline_first() {
    let f = setup().await;
    accept(&f, 1, "one.app", 10).await;
    accept(&f, 2, "two.app", 20).await;
    accept(&f, 3, "three.app", 30).await;
    accept(&f, 4, "four.app", 1000).await;
    let now = T0 + secs(100);

    assert_eq!(f.repo.interrupt_expired_batch(now, 2).await, Ok(2));
    assert_eq!(read(&f, 1, T0).await.status, OperationStatus::Interrupted);
    assert_eq!(read(&f, 2, T0).await.status, OperationStatus::Interrupted);
    assert_eq!(read(&f, 3, T0).await.status, OperationStatus::Accepted);

    assert_eq!(f.repo.interrupt_expired_batch(now, 2).await, Ok(1));
    assert_eq!(f.repo.interrupt_expired_batch(now, 2).await, Ok(0));
    assert_eq!(
        read(&f, 4, T0).await.status,
        OperationStatus::Accepted,
        "not expired"
    );
    for n in [1, 2, 3] {
        assert_eq!(events(&f, n).await.len(), 2, "accepted and finished of {n}");
    }
}

#[tokio::test]
async fn batch_interruption_covers_running_records() {
    let f = setup().await;
    accept(&f, 1, APP, 10).await;
    f.repo.mark_running(id(1), T0 + secs(1)).await.unwrap();
    assert_eq!(
        f.repo.interrupt_expired_batch(T0 + secs(10), 5).await,
        Ok(1)
    );
    assert_eq!(
        read(&f, 1, T0 + secs(11)).await.status,
        OperationStatus::Interrupted
    );
}

// ---- purge ----

#[tokio::test]
async fn purge_removes_only_final_records_older_than_the_cutoff_and_not_the_ledger() {
    let f = setup().await;
    accept(&f, 1, "a.app", 3600).await; // completed at +60s
    accept(&f, 2, "b.app", 3600).await; // completed at +120s
    accept(&f, 3, "c.app", 3600).await; // stays open
    accept(&f, 4, "d.app", 10).await; // interrupted at +180s
    for (n, at) in [(1, 60), (2, 120)] {
        f.repo
            .finish(
                id(n),
                &report(n, "x"),
                RequestCategory::Succeeded,
                finished_event(n),
                T0 + secs(at),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        f.repo.interrupt_if_expired(id(4), T0 + secs(180)).await,
        Ok(true)
    );

    let removed = f.repo.purge_finished(T0 + secs(150), 100).await.unwrap();
    assert_eq!(removed, 2);

    let now = T0 + secs(200);
    assert_eq!(f.repo.read_operation(id(1), now).await, Ok(None));
    assert_eq!(f.repo.read_operation(id(2), now).await, Ok(None));
    assert_eq!(
        read(&f, 3, now).await.status,
        OperationStatus::Accepted,
        "open: never purged"
    );
    assert_eq!(
        read(&f, 4, now).await.status,
        OperationStatus::Interrupted,
        "newer than the cutoff"
    );

    // The ledger is untouched.
    assert_eq!(events(&f, 1).await, [accepted_event(1), finished_event(1)]);
    assert_eq!(events(&f, 2).await, [accepted_event(2), finished_event(2)]);
    assert_eq!(events(&f, 3).await.len(), 1);
    assert_eq!(events(&f, 4).await.len(), 2);
}

#[tokio::test]
async fn purge_never_removes_an_open_record_however_old() {
    let f = setup().await;
    accept(&f, 1, APP, 10).await;
    assert_eq!(
        f.repo.purge_finished(T0 + secs(1_000_000), 100).await,
        Ok(0)
    );
    assert_eq!(read(&f, 1, T0).await.status, OperationStatus::Accepted);
}

#[tokio::test]
async fn purge_respects_the_limit_and_goes_oldest_first() {
    let f = setup().await;
    for (n, app) in [(1, "a.app"), (2, "b.app"), (3, "c.app")] {
        accept(&f, n, app, 3600).await;
    }
    for (n, at) in [(1, 10), (2, 20), (3, 30)] {
        f.repo
            .finish(
                id(n),
                &report(n, "x"),
                RequestCategory::Succeeded,
                finished_event(n),
                T0 + secs(at),
            )
            .await
            .unwrap();
    }
    let cutoff = T0 + secs(100);
    assert_eq!(f.repo.purge_finished(cutoff, 2).await, Ok(2));
    let now = T0 + secs(101);
    assert_eq!(f.repo.read_operation(id(1), now).await, Ok(None));
    assert_eq!(f.repo.read_operation(id(2), now).await, Ok(None));
    assert!(f.repo.read_operation(id(3), now).await.unwrap().is_some());
    assert_eq!(f.repo.purge_finished(cutoff, 2).await, Ok(1));
    assert_eq!(f.repo.purge_finished(cutoff, 2).await, Ok(0));
}

// ---- read ----

#[tokio::test]
async fn read_operation_is_none_for_an_unknown_id() {
    let f = setup().await;
    assert_eq!(f.repo.read_operation(id(404), T0).await, Ok(None));
}

#[tokio::test]
async fn read_operation_interrupts_an_expired_record_first() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let now = T0 + secs(61);
    let op = read(&f, 1, now).await;
    assert_eq!(op.status, OperationStatus::Interrupted);
    assert_eq!(op.finished_at, Some(now));
    assert!(op.report.interrupted);
    let recorded = events(&f, 1).await;
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[1].kind, AuditKind::Finished);
    // A second read changes nothing.
    assert_eq!(read(&f, 1, now + secs(10)).await, op);
    assert_eq!(events(&f, 1).await.len(), 2);
}

#[tokio::test]
async fn read_operation_leaves_an_unexpired_record_alone() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    assert_eq!(
        read(&f, 1, T0 + secs(59)).await.status,
        OperationStatus::Accepted
    );
    assert_eq!(events(&f, 1).await.len(), 1);
}

// ---- audit_events ----

#[tokio::test]
async fn audit_events_come_back_ordered_by_event_id_and_by_correlation() {
    let f = setup().await;
    for event in [3, 1, 2] {
        f.repo
            .record_refused(refused_event(0xA000 + event, 0xA))
            .await
            .unwrap();
    }
    f.repo
        .record_refused(refused_event(0xB001, 0xB))
        .await
        .unwrap();

    let ids: Vec<Uuid> = events(&f, 0xA).await.iter().map(|e| e.event_id).collect();
    assert_eq!(ids, [id(0xA001), id(0xA002), id(0xA003)]);
    assert_eq!(events(&f, 0xB).await.len(), 1);
    assert_eq!(events(&f, 0xC).await, []);
}

#[tokio::test]
async fn audit_events_round_trip_every_field() {
    let f = setup().await;
    let mut event = refused_event(0xA001, 0xA);
    event.action = OperationAction::Uninstall;
    event.written_at = datetime!(2026-10-22 10:00:00.123456 UTC);
    event.content = json!({ "application": { "id": "x" }, "result": { "problem_count": 0 } });
    f.repo.record_refused(event.clone()).await.unwrap();
    // `SQLite` stores whole milliseconds.
    event.written_at = datetime!(2026-10-22 10:00:00.123 UTC);
    assert_eq!(events(&f, 0xA).await, [event]);
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
        f.repo.open_operation(APP, T0).await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(
        f.repo
            .accept(new_op(1, APP, 60), accepted_event(1), T0)
            .await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(
        f.repo.record_refused(refused_event(0x9001, 0x9000)).await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(
        f.repo.mark_running(id(1), T0).await,
        Err(StorageError::Unavailable)
    );
    assert_eq!(
        f.repo.audit_events(id(1)).await,
        Err(StorageError::Unavailable)
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "each call is bounded by storage_call_timeout"
    );

    release.send(()).unwrap();
    holder.await.unwrap().unwrap();
    // Not sticky: the next call answers, and nothing from the timed-out accept
    // was committed.
    assert_eq!(f.repo.open_operation(APP, T0).await, Ok(None));
    assert_eq!(f.repo.read_operation(id(1), T0).await, Ok(None));
}

// ---- concurrency ----

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_accepts_for_one_application_have_exactly_one_winner() {
    let f = setup().await;
    let mut tasks = Vec::new();
    for n in 1..=8_u128 {
        let repo = Arc::clone(&f.repo);
        tasks.push(tokio::spawn(async move {
            repo.accept(new_op(n, APP, 60), accepted_event(n), T0).await
        }));
    }
    let mut won = Vec::new();
    let mut violated = 0;
    for (n, task) in (1..=8_u128).zip(tasks) {
        match task.await.unwrap() {
            Ok(()) => won.push(n),
            Err(StorageError::IndexViolated) => violated += 1,
            Err(other) => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(won.len(), 1, "exactly one winner: {won:?}");
    assert_eq!(violated, 7);
    let winner = won[0];
    assert_eq!(
        f.repo
            .open_operation(APP, T0)
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        id(winner)
    );
    for n in 1..=8_u128 {
        let expected = usize::from(n == winner);
        assert_eq!(events(&f, n).await.len(), expected, "events of {n}");
    }
}

// ---- contention (F-001) ----

/// Run `n` accepts at once, released together by a barrier; `app` names the
/// application of accept `i`.
async fn burst_accepts(
    f: &Fixture,
    n: u128,
    app: impl Fn(u128) -> String,
) -> Vec<Result<(), StorageError>> {
    let barrier = Arc::new(tokio::sync::Barrier::new(usize::try_from(n).unwrap()));
    let mut tasks = Vec::new();
    for i in 1..=n {
        let repo = Arc::clone(&f.repo);
        let barrier = Arc::clone(&barrier);
        let app = app(i);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repo.accept(new_op(i, &app, 60), accepted_event(i), T0)
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
    for i in 1..=16_u128 {
        assert_eq!(events(&f, i).await.len(), 1, "accepted event of {i}");
    }
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

// ---- batch interruption survives a failing record (F-005) ----

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
async fn batch_interruption_goes_past_a_record_that_does_not_map() {
    let f = setup().await;
    accept(&f, 1, "bad.app", 10).await;
    accept(&f, 2, "good.app", 20).await;
    corrupt_category(&f, 1).await;

    let now = T0 + secs(100);
    assert_eq!(f.repo.interrupt_expired_batch(now, 10).await, Ok(1));
    assert_eq!(read(&f, 2, now).await.status, OperationStatus::Interrupted);
    assert_eq!(events(&f, 2).await.len(), 2);
    // The bad record is untouched: still open, no finished event.
    assert_eq!(events(&f, 1).await, [accepted_event(1)]);
    assert_eq!(
        f.repo
            .open_operation("bad.app", T0)
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        id(1)
    );
}

#[tokio::test]
async fn batch_interruption_fails_when_every_attempted_record_fails() {
    let f = setup().await;
    accept(&f, 1, "bad.app", 10).await;
    corrupt_category(&f, 1).await;
    assert_eq!(
        f.repo.interrupt_expired_batch(T0 + secs(100), 10).await,
        Err(StorageError::Unavailable)
    );
}

#[tokio::test]
async fn batch_interruption_with_nothing_due_is_zero() {
    let f = setup().await;
    assert_eq!(f.repo.interrupt_expired_batch(T0, 10).await, Ok(0));
}

// ---- the finished event belongs to the record (F-006) ----

#[tokio::test]
async fn finish_derives_the_stored_category_and_clears_interrupted() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let mut given = report(1, APP);
    given.category = Some(RequestCategory::Conflict);
    given.interrupted = true;
    f.repo
        .finish(
            id(1),
            &given,
            RequestCategory::Succeeded,
            finished_event(1),
            T0 + secs(10),
        )
        .await
        .unwrap();
    let op = read(&f, 1, T0 + secs(11)).await;
    assert_eq!(op.category, Some(RequestCategory::Succeeded));
    assert_eq!(op.report.category, Some(RequestCategory::Succeeded));
    assert!(!op.report.interrupted);
}

#[tokio::test]
async fn supervisor_completion_stores_internal_failure_in_the_report() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    let mut given = report(1, APP);
    given.interrupted = true;
    f.repo
        .complete_by_supervisor(id(1), &given, finished_event(1), T0 + secs(10))
        .await
        .unwrap();
    let op = read(&f, 1, T0 + secs(11)).await;
    assert_eq!(op.report.category, Some(RequestCategory::InternalFailure));
    assert!(!op.report.interrupted);
}

/// A finished event that does not belong to operation 1, each in one way.
fn mismatched_events() -> Vec<(&'static str, AuditEvent)> {
    let mut other_id = finished_event(1);
    other_id.event_id = id(0x7777);
    let mut other_correlation = finished_event(1);
    other_correlation.correlation_id = id(2);
    let mut other_action = finished_event(1);
    other_action.action = OperationAction::Uninstall;
    let mut other_kind = finished_event(1);
    other_kind.kind = AuditKind::Refused;
    vec![
        ("event id", other_id),
        ("correlation id", other_correlation),
        ("action", other_action),
        ("kind", other_kind),
    ]
}

async fn assert_nothing_written(f: &Fixture) {
    let op = read(f, 1, T0 + secs(11)).await;
    assert_eq!(op.status, OperationStatus::Accepted);
    assert_eq!(op.category, None);
    assert_eq!(op.finished_at, None);
    assert_eq!(op.report, report(1, APP));
    assert_eq!(events(f, 1).await, [accepted_event(1)]);
    assert_eq!(events(f, 2).await, []);
    assert_eq!(events(f, 0x7777).await, []);
}

#[tokio::test]
async fn finish_refuses_a_finished_event_of_another_record_and_writes_nothing() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    for (what, event) in mismatched_events() {
        let result = f
            .repo
            .finish(
                id(1),
                &report(1, APP),
                RequestCategory::Succeeded,
                event,
                T0 + secs(10),
            )
            .await;
        assert_eq!(result, Err(StorageError::Inconsistent), "{what}");
        assert_nothing_written(&f).await;
    }
}

#[tokio::test]
async fn supervisor_completion_refuses_a_finished_event_of_another_record() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    for (what, event) in mismatched_events() {
        let result = f
            .repo
            .complete_by_supervisor(id(1), &failed_report(1), event, T0 + secs(10))
            .await;
        assert_eq!(result, Err(StorageError::Inconsistent), "{what}");
        assert_nothing_written(&f).await;
    }
}

#[tokio::test]
async fn a_final_record_is_operation_final_even_with_a_mismatched_event() {
    let f = setup().await;
    accept(&f, 1, APP, 60).await;
    assert_eq!(
        f.repo.interrupt_if_expired(id(1), T0 + secs(61)).await,
        Ok(true)
    );
    let (_, event) = mismatched_events().remove(0);
    let result = f
        .repo
        .finish(
            id(1),
            &report(1, APP),
            RequestCategory::Succeeded,
            event,
            T0 + secs(62),
        )
        .await;
    assert_eq!(result, Err(StorageError::OperationFinal));
}

// ---- audit order, offsets and sub-second guards (F-008) ----

fn written(event: u128, correlation: u128, at: OffsetDateTime) -> AuditEvent {
    let mut event = refused_event(event, correlation);
    event.written_at = at;
    event
}

fn micros(n: i64) -> time::Duration {
    time::Duration::microseconds(n)
}

#[tokio::test]
async fn audit_events_read_in_write_time_order_when_event_ids_run_the_other_way() {
    let f = setup().await;
    // Event ids ascend while write times descend, in whole seconds.
    for (event, at) in [(0xA001, 30), (0xA002, 20), (0xA003, 10)] {
        f.repo
            .record_refused(written(event, 0xA, T0 + secs(at)))
            .await
            .unwrap();
    }
    let ids: Vec<Uuid> = events(&f, 0xA).await.iter().map(|e| e.event_id).collect();
    assert_eq!(ids, [id(0xA003), id(0xA002), id(0xA001)]);
}

#[tokio::test]
async fn audit_events_with_equal_write_times_read_in_event_id_order() {
    let f = setup().await;
    for event in [0xA003, 0xA001, 0xA002] {
        f.repo
            .record_refused(written(event, 0xA, T0 + secs(5)))
            .await
            .unwrap();
    }
    let ids: Vec<Uuid> = events(&f, 0xA).await.iter().map(|e| e.event_id).collect();
    assert_eq!(ids, [id(0xA001), id(0xA002), id(0xA003)]);
}

#[tokio::test]
async fn audit_events_read_in_write_time_order_across_mixed_fraction_lengths() {
    let f = setup().await;
    for (event, at) in [
        (0xA001, secs(10) + micros(500_000)),
        (0xA002, secs(10)),
        (0xA003, secs(10) + micros(123_456)),
        (0xA004, secs(9) + micros(999_999)),
    ] {
        f.repo
            .record_refused(written(event, 0xA, T0 + at))
            .await
            .unwrap();
    }
    let ids: Vec<Uuid> = events(&f, 0xA).await.iter().map(|e| e.event_id).collect();
    assert_eq!(ids, [id(0xA004), id(0xA002), id(0xA003), id(0xA001)]);
}

#[tokio::test]
async fn audit_events_read_in_write_time_order_across_fractions_of_one_length() {
    let f = setup().await;
    for (event, at) in [
        (0xA001, micros(500_000)),
        (0xA002, micros(250_000)),
        (0xA003, micros(750_000)),
    ] {
        f.repo
            .record_refused(written(event, 0xA, T0 + secs(10) + at))
            .await
            .unwrap();
    }
    let ids: Vec<Uuid> = events(&f, 0xA).await.iter().map(|e| e.event_id).collect();
    assert_eq!(ids, [id(0xA002), id(0xA001), id(0xA003)]);
}

#[tokio::test]
async fn guards_compare_instants_given_with_an_offset() {
    let f = setup().await;
    let plus_two = time::UtcOffset::from_hms(2, 0, 0).unwrap();
    let local = |at: OffsetDateTime| at.to_offset(plus_two);
    let mut new = new_op(1, APP, 60);
    new.accepted_at = local(T0);
    new.run_deadline = local(T0 + secs(60));
    f.repo
        .accept(new, accepted_event(1), local(T0))
        .await
        .unwrap();
    let op = read(&f, 1, local(T0 + secs(1))).await;
    assert_eq!(op.run_deadline, T0 + secs(60));
    assert_eq!(op.accepted_at, T0);

    let open = f
        .repo
        .open_operation(APP, local(T0 + secs(59)))
        .await
        .unwrap()
        .unwrap();
    assert!(!open.expired);
    assert!(
        f.repo
            .open_operation(APP, local(T0 + secs(60)))
            .await
            .unwrap()
            .unwrap()
            .expired,
        "expired at the deadline"
    );
    f.repo
        .mark_running(id(1), local(T0 + secs(59)))
        .await
        .unwrap();
    assert_eq!(
        f.repo
            .record_report(id(1), &report(1, APP), local(T0 + secs(60)))
            .await,
        Err(StorageError::OperationFinal),
        "the deadline is exclusive for a write"
    );
    assert_eq!(
        f.repo
            .interrupt_if_expired(id(1), local(T0 + secs(59)))
            .await,
        Ok(false)
    );
    assert_eq!(
        f.repo
            .interrupt_if_expired(id(1), local(T0 + secs(60)))
            .await,
        Ok(true)
    );
}

#[tokio::test]
async fn guards_compare_sub_second_inputs_of_one_fraction_length() {
    let f = setup().await;
    let mut new = new_op(1, APP, 60);
    new.run_deadline = T0 + secs(60) + micros(500_000);
    f.repo.accept(new, accepted_event(1), T0).await.unwrap();

    f.repo
        .mark_running(id(1), T0 + secs(60) + micros(499_999))
        .await
        .expect("a microsecond before the deadline");
    assert_eq!(
        f.repo
            .record_report(id(1), &report(1, APP), T0 + secs(60) + micros(500_000))
            .await,
        Err(StorageError::OperationFinal),
        "at the deadline"
    );
    assert_eq!(
        f.repo
            .interrupt_if_expired(id(1), T0 + secs(60) + micros(499_999))
            .await,
        Ok(false)
    );
    assert_eq!(
        f.repo
            .interrupt_if_expired(id(1), T0 + secs(60) + micros(500_000))
            .await,
        Ok(true)
    );
}

#[tokio::test]
async fn guards_compare_sub_second_inputs_across_fraction_lengths() {
    let f = setup().await;
    let mut new = new_op(1, APP, 60);
    new.run_deadline = T0 + secs(60) + micros(500_000);
    f.repo.accept(new, accepted_event(1), T0).await.unwrap();
    f.repo
        .mark_running(id(1), T0 + secs(60))
        .await
        .expect("a whole second before a half-second deadline");
    assert_eq!(
        f.repo
            .interrupt_if_expired(id(1), T0 + secs(60) + micros(1))
            .await,
        Ok(false)
    );
    assert_eq!(
        f.repo.interrupt_if_expired(id(1), T0 + secs(61)).await,
        Ok(true)
    );
}

/// The stored text of `column` in `table`, read on a second connection.
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
    let plus_two = time::UtcOffset::from_hms(2, 0, 0).unwrap();
    let mut new = new_op(1, APP, 60);
    new.accepted_at = T0.to_offset(plus_two);
    new.run_deadline = T0 + secs(60) + micros(500_999);
    f.repo.accept(new, accepted_event(1), T0).await.unwrap();
    f.repo
        .record_refused(written(0xA001, 0xA, T0 + secs(10)))
        .await
        .unwrap();

    assert_eq!(
        stored_text(&f, "operations", "accepted_at").await,
        ["2026-10-22 10:00:00.000"]
    );
    assert_eq!(
        stored_text(&f, "operations", "run_deadline").await,
        ["2026-10-22 10:01:00.500"]
    );
    let written_at = stored_text(&f, "audit_ledger", "written_at").await;
    assert!(
        written_at.contains(&"2026-10-22 10:00:10.000".to_owned()),
        "{written_at:?}"
    );
    assert_eq!(
        read(&f, 1, T0).await.run_deadline,
        T0 + secs(60) + micros(500_000),
        "read back at millisecond precision"
    );
}

// ---- a timeout in the middle of a transaction (F-008) ----

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
                    insert_event(tx, DbBackend::Sqlite, &refused_event(0x9001, 0x9000)).await?;
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

    // The accept's first statement (a read) runs; its insert then waits on the
    // write lock until `storage_call_timeout` drops the call.
    let started = std::time::Instant::now();
    assert_eq!(
        f.repo
            .accept(new_op(1, APP, 60), accepted_event(1), T0)
            .await,
        Err(StorageError::Unavailable)
    );
    assert!(started.elapsed() < Duration::from_secs(4), "bounded");

    release.send(()).unwrap();
    holder.await.unwrap().unwrap();
    assert_eq!(f.repo.open_operation(APP, T0).await, Ok(None));
    assert_eq!(f.repo.read_operation(id(1), T0).await, Ok(None));
    assert_eq!(events(&f, 1).await, []);
    assert_eq!(events(&f, 0x9000).await.len(), 1, "the holder's own write");
}

// ---- logs carry a class and the call, never the application id (F-009) ----

const SECRET_APP: &str = "very-secret-application-id.example";

#[tokio::test]
#[traced_test]
async fn a_failed_step_logs_its_class_and_call_but_no_application_id() {
    let f = setup().await;
    accept(&f, 1, "other.app", 60).await;
    // The event id is taken, so the insert fails on a unique constraint.
    let mut clash = accepted_event(2);
    clash.event_id = id(0x2001);
    assert_eq!(
        f.repo.accept(new_op(2, SECRET_APP, 60), clash, T0).await,
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
        f.repo.open_operation(SECRET_APP, T0).await,
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
    accept(&f, 1, SECRET_APP, 10).await;
    corrupt_category(&f, 1).await;
    assert_eq!(
        f.repo.interrupt_if_expired(id(1), T0 + secs(20)).await,
        Err(StorageError::Unavailable)
    );
    assert!(logs_contain("class=\"row_mapping\""));
    assert!(logs_contain("call=\"interrupt_if_expired\""));
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

// ---- batch limits (F-010) ----

#[tokio::test]
async fn a_huge_limit_is_clamped_and_still_works() {
    let f = setup().await;
    for n in 1..=3_u128 {
        accept(&f, n, &format!("app.{n}"), 10).await;
    }
    assert_eq!(
        f.repo
            .interrupt_expired_batch(T0 + secs(20), u64::MAX)
            .await,
        Ok(3)
    );
    assert_eq!(
        f.repo.purge_finished(T0 + secs(3600), u64::MAX).await,
        Ok(3)
    );
}

#[tokio::test]
async fn a_batch_never_handles_more_than_the_maximum() {
    let f = setup().await;
    let total = MAX_BATCH_LIMIT + 5;
    for n in 1..=u128::from(total) {
        f.repo
            .accept(new_op(n, &format!("app.{n}"), 10), accepted_event(n), T0)
            .await
            .unwrap();
    }
    let now = T0 + secs(20);
    assert_eq!(
        f.repo.interrupt_expired_batch(now, u64::MAX).await,
        Ok(MAX_BATCH_LIMIT)
    );
    assert_eq!(f.repo.interrupt_expired_batch(now, u64::MAX).await, Ok(5));
    assert_eq!(
        f.repo.purge_finished(T0 + secs(3600), u64::MAX).await,
        Ok(MAX_BATCH_LIMIT)
    );
    assert_eq!(
        f.repo.purge_finished(T0 + secs(3600), u64::MAX).await,
        Ok(5)
    );
}
