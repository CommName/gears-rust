//! Real PostgreSQL and MySQL runs of the Application Manager storage: the
//! migration up/down/up, the open-operation index under a concurrent race,
//! binary no-pad collation of application ids, case-sensitive enum tokens, the
//! final-write guard, and an interruption racing a report write.
//!
//! SQLite runs in the default suite (`repo_tests.rs`). These need a Docker
//! daemon, so they sit behind `--features integration`. Without
//! `APPLICATION_MANAGER_DB_REQUIRE_DOCKER` a missing Docker skips these tests
//! and they pass vacuously. CI runs the `application-manager-db` lane with
//! `APPLICATION_MANAGER_DB_REQUIRE_DOCKER=1` (`gears-rust.mk`,
//! `.github/workflows/ci.yml`), where a missing Docker fails them.
//!
//! ```text
//! APPLICATION_MANAGER_DB_REQUIRE_DOCKER=1 make test-integration LANE=application-manager-db
//! ```

#![cfg(feature = "integration")]
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::doc_markdown
)]

use std::sync::Arc;
use std::time::Duration;

use application_manager::domain::audit::{AuditEvent, AuditKind};
use application_manager::domain::operation::{NewOperation, OperationStatus};
use application_manager::domain::ports::{OperationRepository, StorageError};
use application_manager::infra::storage::{Migrator, SeaOrmOperationRepository};
use application_manager::sdk::{
    OperationAction, OutcomeReport, ReportApplication, RequestCategory, SectionOutcome,
    SectionOutcomeEntry,
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement};
use sea_orm_migration::MigratorTrait;
use serde_json::json;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use time::OffsetDateTime;
use time::macros::datetime;
use toolkit_db::migration_runner::run_migrations_for_testing;
use toolkit_db::{ConnectOpts, DBProvider, DbError, connect_db};
use uuid::Uuid;

const T0: OffsetDateTime = datetime!(2026-10-22 10:00:00 UTC);
const TABLES: [&str; 2] = ["operations", "audit_ledger"];

/// Whether a missing or broken Docker must fail the run rather than skip it.
fn require_docker() -> bool {
    std::env::var_os("APPLICATION_MANAGER_DB_REQUIRE_DOCKER")
        .is_some_and(|v| v != "0" && !v.is_empty())
}

/// Skip, or fail when Docker is required, because a container did not start.
fn docker_unavailable(what: &str, detail: &str) {
    let msg = format!("{what}: {detail}. Install/start Docker to run these for real.");
    assert!(!require_docker(), "{msg}");
    eprintln!("skipping -- {msg}");
}

async fn wait_for_tcp(host: &str, port: u16, timeout: Duration) {
    use tokio::net::TcpStream;
    use tokio::time::{Instant, sleep};
    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect((host, port)).await.is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timeout waiting for {host}:{port}"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

async fn exec(db: &DatabaseConnection, sql: &str) -> Result<(), sea_orm::DbErr> {
    db.execute_raw(Statement::from_string(
        db.get_database_backend(),
        sql.to_owned(),
    ))
    .await
    .map(|_| ())
}

async fn assert_tables(db: &DatabaseConnection, present: bool, backend: &str) {
    for table in TABLES {
        let found = exec(db, &format!("SELECT COUNT(*) FROM {table}"))
            .await
            .is_ok();
        assert_eq!(found, present, "{table} on {backend}");
    }
}

/// A running backend: the DSN, and the container that must outlive the test.
struct Backend {
    name: &'static str,
    dsn: String,
    _container: Box<dyn std::any::Any + Send>,
}

async fn postgres() -> Option<Backend> {
    let request = test_containers::postgres()
        .with_env_var("POSTGRES_PASSWORD", "pass")
        .with_env_var("POSTGRES_USER", "user")
        .with_env_var("POSTGRES_DB", "app");
    let container = match request.start().await {
        Ok(container) => container,
        Err(e) => {
            docker_unavailable("PostgreSQL", &format!("could not start a container ({e})"));
            return None;
        }
    };
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let host = container
        .get_host()
        .await
        .expect("postgres host")
        .to_string();
    let host = host.trim_matches(['[', ']']).to_owned();
    wait_for_tcp(&host, port, Duration::from_mins(1)).await;
    Some(Backend {
        name: "postgres",
        dsn: format!("postgres://user:pass@{host}:{port}/app"),
        _container: Box::new(container),
    })
}

async fn mysql() -> Option<Backend> {
    let container = match test_containers::mysql().start().await {
        Ok(container) => container,
        Err(e) => {
            docker_unavailable("MySQL", &format!("could not start a container ({e})"));
            return None;
        }
    };
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let host = container.get_host().await.expect("mysql host").to_string();
    let host = host.trim_matches(['[', ']']).to_owned();
    wait_for_tcp(&host, port, Duration::from_mins(2)).await;
    Some(Backend {
        name: "mysql",
        dsn: format!("mysql://root@{host}:{port}/test"),
        _container: Box::new(container),
    })
}

/// The migration runs up, down and up again.
async fn assert_migration_round_trip(backend: &Backend) {
    let db = Database::connect(&backend.dsn).await.expect("connect");
    Migrator::up(&db, None).await.expect("apply");
    assert_tables(&db, true, backend.name).await;
    Migrator::down(&db, None).await.expect("roll back");
    assert_tables(&db, false, backend.name).await;
    Migrator::up(&db, None).await.expect("re-apply");
    assert_tables(&db, true, backend.name).await;
}

async fn repository(backend: &Backend) -> Arc<SeaOrmOperationRepository> {
    let db = connect_db(
        &backend.dsn,
        ConnectOpts {
            max_conns: Some(10),
            min_conns: Some(1),
            ..Default::default()
        },
    )
    .await
    .expect("connect pool");
    // Already applied by the round trip; applying again is a no-op.
    run_migrations_for_testing(&db, Migrator::migrations())
        .await
        .expect("migrations");
    Arc::new(SeaOrmOperationRepository::new(
        Arc::new(DBProvider::<DbError>::new(db)),
        Duration::from_secs(10),
    ))
}

fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn secs(n: i64) -> time::Duration {
    time::Duration::seconds(n)
}

fn report(n: u128, app: &str) -> OutcomeReport {
    OutcomeReport {
        operation_id: id(n),
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

fn new_op(n: u128, app: &str) -> NewOperation {
    NewOperation {
        operation_id: id(n),
        application_id: app.to_owned(),
        action: OperationAction::Seed,
        accepted_at: T0,
        run_deadline: T0 + secs(60),
        finished_event_id: id(n + 0x1000),
        report: report(n, app),
    }
}

fn event(event_id: u128, kind: AuditKind, correlation: u128) -> AuditEvent {
    AuditEvent {
        event_id: id(event_id),
        kind,
        correlation_id: id(correlation),
        action: OperationAction::Seed,
        written_at: T0,
        content: json!({ "marker": "x" }),
    }
}

async fn accept(repo: &SeaOrmOperationRepository, n: u128, app: &str) -> Result<(), StorageError> {
    repo.accept(
        new_op(n, app),
        event(n + 0x2000, AuditKind::Accepted, n),
        T0,
    )
    .await
}

/// Eight concurrent acceptances for one application: one winner, the rest
/// lose on the unique index, and only the winner's event is in the ledger.
async fn assert_index_race(repo: &Arc<SeaOrmOperationRepository>, base: u128, app: &str) {
    let mut tasks = Vec::new();
    for n in 1..=8_u128 {
        let repo = Arc::clone(repo);
        let app = app.to_owned();
        tasks.push(tokio::spawn(
            async move { accept(&repo, base + n, &app).await },
        ));
    }
    let mut won = Vec::new();
    for (n, task) in (1..=8_u128).zip(tasks) {
        match task.await.unwrap() {
            Ok(()) => won.push(base + n),
            Err(StorageError::IndexViolated) => {}
            Err(other) => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(won.len(), 1, "exactly one winner: {won:?}");
    let open = repo.open_operation(app, T0).await.unwrap().unwrap();
    assert_eq!(open.operation_id, id(won[0]));
    let mut ledger = 0;
    for n in 1..=8_u128 {
        ledger += repo.audit_events(id(base + n)).await.unwrap().len();
    }
    assert_eq!(ledger, 1, "a lost acceptance leaves no accepted event");
}

/// Application ids compare exactly: case and trailing space distinguish them,
/// in the unique index as well as in the lookup.
async fn assert_binary_collation(repo: &SeaOrmOperationRepository, base: u128) {
    accept(repo, base + 1, "a").await.expect("a");
    accept(repo, base + 2, "A").await.expect("`A` is not `a`");
    accept(repo, base + 3, "a ")
        .await
        .expect("`a ` is not `a` (no pad)");
    assert_eq!(
        accept(repo, base + 4, "a").await,
        Err(StorageError::IndexViolated)
    );
    for (n, app) in [(1, "a"), (2, "A"), (3, "a ")] {
        assert_eq!(
            repo.open_operation(app, T0)
                .await
                .unwrap()
                .unwrap()
                .operation_id,
            id(base + n),
            "{app:?}"
        );
    }
    assert_eq!(repo.open_operation("a  ", T0).await, Ok(None));
}

/// The first final write wins, sequentially and under a race, and a final
/// record frees its application.
async fn assert_final_write_guard(repo: &Arc<SeaOrmOperationRepository>, base: u128, app: &str) {
    let op = base + 1;
    accept(repo, op, app).await.unwrap();
    repo.mark_running(id(op), T0 + secs(1)).await.unwrap();

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let repo = Arc::clone(repo);
        let app = app.to_owned();
        tasks.push(tokio::spawn(async move {
            // Every racer carries the record's own finished event id.
            let mut finished = event(op + 0x1000, AuditKind::Finished, op);
            finished.written_at = T0 + secs(10);
            repo.finish(
                id(op),
                &report(op, &app),
                RequestCategory::Succeeded,
                finished,
                T0 + secs(10),
            )
            .await
        }));
    }
    let mut won = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(()) => won += 1,
            Err(StorageError::OperationFinal) => {}
            Err(other) => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(won, 1, "exactly one final write wins");

    let read = repo
        .read_operation(id(op), T0 + secs(11))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.status, OperationStatus::Completed);
    assert_eq!(read.category, Some(RequestCategory::Succeeded));
    assert_eq!(
        repo.audit_events(id(op)).await.unwrap().len(),
        2,
        "the accepted event and exactly one finished event"
    );
    // A late interruption and a late report are refused.
    assert_eq!(
        repo.interrupt_if_expired(id(op), T0 + secs(100)).await,
        Ok(false)
    );
    assert_eq!(
        repo.record_report(id(op), &report(op, app), T0 + secs(12))
            .await,
        Err(StorageError::OperationFinal)
    );
    // The final record frees the application.
    accept(repo, base + 2, app)
        .await
        .expect("a final record is not open");
}

/// An interruption and a report write race for one expired record. Either the
/// report write commits first and the interrupted record and its finished event
/// both carry the new section, or the interruption wins and the report write is
/// `OperationFinal`. The stored report and the event never disagree, and a
/// committed report write is never lost.
async fn assert_interruption_keeps_a_concurrent_report(
    repo: &Arc<SeaOrmOperationRepository>,
    base: u128,
) {
    let mut report_first = 0;
    for round in 0..24_u128 {
        let n = base + round;
        let app = format!("interrupt.race.{round}");
        accept(repo, n, &app).await.unwrap();
        repo.mark_running(id(n), T0 + secs(1)).await.unwrap();

        let mut late = report(n, &app);
        late.sections.push(SectionOutcomeEntry {
            key: "late".to_owned(),
            outcome: SectionOutcome::Succeeded,
            schema_version: Some("1".to_owned()),
            detail: None,
            location: None,
        });
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let writer = {
            let (repo, barrier) = (Arc::clone(repo), Arc::clone(&barrier));
            tokio::spawn(async move {
                barrier.wait().await;
                repo.record_report(id(n), &late, T0 + secs(10)).await
            })
        };
        let interrupter = {
            let (repo, barrier) = (Arc::clone(repo), barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                repo.interrupt_if_expired(id(n), T0 + secs(100)).await
            })
        };
        let written = writer.await.unwrap();
        assert_eq!(interrupter.await.unwrap(), Ok(true), "round {round}");
        let expected = match written {
            Ok(()) => {
                report_first += 1;
                1
            }
            Err(StorageError::OperationFinal) => 0,
            Err(other) => panic!("unexpected {other:?}"),
        };

        let op = repo
            .read_operation(id(n), T0 + secs(101))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(op.status, OperationStatus::Interrupted);
        assert!(op.report.interrupted);
        assert_eq!(op.report.sections.len(), expected, "round {round}: report");
        let events = repo.audit_events(id(n)).await.unwrap();
        let finished: Vec<_> = events
            .iter()
            .filter(|e| e.kind == AuditKind::Finished)
            .collect();
        assert_eq!(finished.len(), 1, "round {round}");
        let sections = finished[0].content["result"]["sections"]
            .as_array()
            .expect("sections array");
        assert_eq!(sections.len(), expected, "round {round}: finished event");
    }
    eprintln!("report committed before the interruption in {report_first} of 24 rounds");
}

/// The enum token columns compare as bytes: `'ACCEPTED'` is not `'accepted'`,
/// so the `CHECK` refuses it (it would pass under a case-insensitive default
/// collation).
async fn assert_enum_tokens_are_case_sensitive(backend: &Backend) {
    let db = Database::connect(&backend.dsn).await.expect("connect");
    let insert = |id: u128, status: &str| {
        format!(
            "INSERT INTO operations (operation_id, application_id, action, status, report, \
             accepted_at, run_deadline, finished_event_id) VALUES \
             (UNHEX('{id:032x}'), 'case.app.{id}', 'seed', '{status}', '{{}}', \
             '2026-10-22 10:00:00', '2026-10-22 10:01:00', UNHEX('{id:032x}'))"
        )
    };
    exec(&db, &insert(0x00CA_5E01, "accepted"))
        .await
        .expect("the lower-case token is accepted");
    assert!(
        exec(&db, &insert(0x00CA_5E02, "ACCEPTED")).await.is_err(),
        "`ACCEPTED` must be refused by the status CHECK"
    );
}

async fn run_suite(backend: Backend) {
    assert_migration_round_trip(&backend).await;
    let repo = repository(&backend).await;
    assert_index_race(&repo, 0x100, "race.app").await;
    assert_binary_collation(&repo, 0x200).await;
    assert_final_write_guard(&repo, 0x300, "guard.app").await;
    assert_interruption_keeps_a_concurrent_report(&repo, 0x400).await;
    if backend.name == "mysql" {
        assert_enum_tokens_are_case_sensitive(&backend).await;
    }
}

#[tokio::test]
async fn storage_behaves_on_postgres() {
    if let Some(backend) = postgres().await {
        run_suite(backend).await;
    }
}

#[tokio::test]
async fn storage_behaves_on_mysql() {
    if let Some(backend) = mysql().await {
        run_suite(backend).await;
    }
}
