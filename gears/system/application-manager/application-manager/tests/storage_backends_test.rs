//! Real PostgreSQL and MySQL runs of the Application Manager storage: the
//! migration up/down/up, the open-operation index under a concurrent race,
//! binary no-pad collation and the byte limit of application ids,
//! case-sensitive enum tokens, the final-write guard with the application
//! record, and concurrent expiry completions of one operation.
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

use application_manager::domain::history::{HistoryEvent, HistoryKind};
use application_manager::domain::operation::{NewOperation, OperationStatus};
use application_manager::domain::ports::{OperationRepository, StorageError};
use application_manager::infra::storage::{Migrator, SeaOrmOperationRepository};
use application_manager::sdk::{
    OperationAction, OutcomeReport, ReportApplication, RequestCategory, SectionOutcome,
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement};
use sea_orm_migration::MigratorTrait;
use serde_json::json;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use time::OffsetDateTime;
use toolkit_db::migration_runner::run_migrations_for_testing;
use toolkit_db::{ConnectOpts, DBProvider, DbError, connect_db};
use uuid::Uuid;

const TABLES: [&str; 3] = ["operations", "operation_history", "applications"];
/// A run window no test outlives.
const OPEN: Duration = Duration::from_secs(3600);

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

fn report(n: u128, app: &str) -> OutcomeReport {
    OutcomeReport {
        operation_id: id(n),
        action: OperationAction::Seed,
        category: None,
        application: ReportApplication {
            id: app.to_owned(),
            version: Some("1.0.0".to_owned()),
        },
        sections: vec![],
        problems: vec![],
        unavailable: vec![],
    }
}

fn new_op(n: u128, app: &str, window: Duration) -> NewOperation {
    NewOperation {
        operation_id: id(n),
        application_id: app.to_owned(),
        action: OperationAction::Seed,
        run_window: window,
        finished_event_id: id(n + 0x2000),
        report: report(n, app),
    }
}

fn event(event_id: u128, kind: HistoryKind, correlation: u128) -> HistoryEvent {
    HistoryEvent {
        event_id: id(event_id),
        kind,
        correlation_id: id(correlation),
        action: OperationAction::Seed,
        // Ignored on insert: the repository writes the database time.
        written_at: OffsetDateTime::UNIX_EPOCH,
        content: json!({ "marker": "x" }),
    }
}

async fn accept_within(
    repo: &SeaOrmOperationRepository,
    n: u128,
    app: &str,
    window: Duration,
) -> Result<(), StorageError> {
    repo.accept(
        new_op(n, app, window),
        event(n + 0x1000, HistoryKind::Accepted, n),
    )
    .await
    .map(|_| ())
}

async fn accept(repo: &SeaOrmOperationRepository, n: u128, app: &str) -> Result<(), StorageError> {
    accept_within(repo, n, app, OPEN).await
}

async fn history(repo: &SeaOrmOperationRepository, n: u128) -> Vec<HistoryEvent> {
    repo.history_page(Some(vec![id(n)]), None, 100)
        .await
        .unwrap()
}

/// Eight concurrent acceptances for one application: one winner, the rest
/// lose on the unique index, and only the winner's event is in the operation history.
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
    let open = repo.open_operation(app).await.unwrap().unwrap();
    assert_eq!(open.operation_id, id(won[0]));
    let mut events = 0;
    for n in 1..=8_u128 {
        events += history(repo, base + n).await.len();
    }
    assert_eq!(events, 1, "a lost acceptance leaves no accepted event");
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
            repo.open_operation(app)
                .await
                .unwrap()
                .unwrap()
                .operation_id,
            id(base + n),
            "{app:?}"
        );
    }
    assert_eq!(repo.open_operation("a  ").await, Ok(None));
    // 128 two-byte characters: within 255 characters, over 255 bytes.
    assert_eq!(
        accept(repo, base + 5, &"\u{e9}".repeat(128)).await,
        Err(StorageError::Unavailable),
        "the byte limit"
    );
    accept(repo, base + 6, &"\u{e9}".repeat(127))
        .await
        .expect("254 bytes fit");
}

/// Eight concurrent final writes of operation `op`; returns how many won.
async fn race_finish(repo: &Arc<SeaOrmOperationRepository>, op: u128, app: &str) -> usize {
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let repo = Arc::clone(repo);
        let app = app.to_owned();
        tasks.push(tokio::spawn(async move {
            repo.finish(id(op), &report(op, &app), RequestCategory::Succeeded)
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
    won
}

/// Operation `op` of `app` completed as succeeded with one finished event and
/// one application record, and no later write changes it.
async fn assert_completed_once(repo: &Arc<SeaOrmOperationRepository>, op: u128, app: &str) {
    let read = repo.read_operation(id(op)).await.unwrap().unwrap();
    assert_eq!(read.status, OperationStatus::Completed);
    assert_eq!(read.category, Some(RequestCategory::Succeeded));
    assert_eq!(
        history(repo, op).await.len(),
        2,
        "the accepted event and exactly one finished event"
    );
    let records = repo
        .applications_page(Some(vec![app.to_owned()]), None, 10)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].operation_id, id(op));
    assert_eq!(records[0].finished_at, read.finished_at.unwrap());
    assert_eq!(repo.expire_if_expired(id(op)).await, Ok(false));
    assert_eq!(
        repo.record_section_outcome(id(op), "a", SectionOutcome::Rejected, None, None)
            .await,
        Err(StorageError::OperationFinal)
    );
}

/// The first final write wins under a race, writes the application record
/// once, and frees the application.
async fn assert_final_write_guard(repo: &Arc<SeaOrmOperationRepository>, base: u128, app: &str) {
    let op = base + 1;
    accept(repo, op, app).await.unwrap();
    let left = repo
        .mark_running(id(op), &[("a".to_owned(), Some("gts.t.a.v1~".to_owned()))])
        .await
        .unwrap();
    assert!(left > Duration::from_secs(3500), "{left:?}");
    repo.record_section_outcome(id(op), "a", SectionOutcome::Succeeded, None, None)
        .await
        .unwrap();

    assert_eq!(
        race_finish(repo, op, app).await,
        1,
        "exactly one final write wins"
    );

    assert_completed_once(repo, op, app).await;
    // The final record frees the application.
    accept(repo, base + 2, app)
        .await
        .expect("a final record is not open");
}

/// Expiry completions race for one expired operation, from the sweeper and
/// the operation read: exactly one completes it, with one finished event.
async fn assert_one_expiry_completion(repo: &Arc<SeaOrmOperationRepository>, base: u128) {
    for round in 0..8_u128 {
        let n = base + round;
        accept_within(repo, n, &format!("expiry.race.{round}"), Duration::ZERO)
            .await
            .unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let (repo, barrier) = (Arc::clone(repo), Arc::clone(&barrier));
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                repo.expire_if_expired(id(n)).await
            }));
        }
        let mut completed = 0;
        for task in tasks {
            if task.await.unwrap() == Ok(true) {
                completed += 1;
            }
        }
        assert_eq!(completed, 1, "round {round}");
        let events = history(repo, n).await;
        let finished: Vec<_> = events
            .iter()
            .filter(|e| e.kind == HistoryKind::Finished)
            .collect();
        assert_eq!(finished.len(), 1, "round {round}");
        assert_eq!(finished[0].content["result"]["reason"], json!("expired"));
        let op = repo.read_operation(id(n)).await.unwrap().unwrap();
        assert_eq!(op.category, Some(RequestCategory::InternalFailure));
    }
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
    assert_one_expiry_completion(&repo, 0x400).await;
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
