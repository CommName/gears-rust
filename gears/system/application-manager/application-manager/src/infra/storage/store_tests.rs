//! Unit tests for the registry store adapter: timeouts and unreachable storage (DESIGN §3.2
//! "Registry Repository", §4.5 "Fault Tolerance" and "Timeouts outside the plugin calls",
//! §4.7 `registry_operation_timeout`, §3.3 `unavailable` reasons).
//!
//! Planned API in `crate::infra::storage::store` (new file, P4). The module ends with:
//!
//! ```text
//! #[cfg(test)]
//! #[cfg_attr(coverage_nightly, coverage(off))]
//! #[path = "store_tests.rs"]
//! mod store_tests;
//! ```
//!
//! ```text
//! /// `RegistryStore` over `RegistryRepo`. It supplies `AccessScope::allow_all()` (ADR 0007:
//! /// the permit's scope is always unscoped, so no scope crosses the port), runs each call on
//! /// `provider.conn()`, and bounds EVERY operation (get, upsert, delete, list) with
//! /// `tokio::time::timeout(operation_timeout, ..)`.
//! #[derive(Clone)]
//! pub struct SeaRegistryStore { /* provider, operation_timeout */ }
//! impl SeaRegistryStore {
//!     pub fn new(provider: DBProvider<DbError>, operation_timeout: Duration) -> Self;
//! }
//! #[async_trait] impl RegistryStore for SeaRegistryStore { /* get, upsert, delete, list */ }
//! ```
//!
//! Error mapping (DESIGN §3.2 "Reports 'storage cannot answer' as a distinct error kind"):
//!
//! - the `registry_operation_timeout` expired, on any backend (the future is dropped, the
//!   caller stops waiting): `RegistryError::TimedOut`. For `upsert` and `delete` that means
//!   NOT CONFIRMED: the statement may still commit;
//! - any storage failure, including the pool acquire timeout, a closed pool and a failed
//!   statement: `RegistryError::Unavailable`.
//!
//! Decision: P2 left a single `Unavailable` variant. DESIGN §3.2 speaks of one "distinct
//! error kind" and §3.3 gives the `unreachable` / `timed_out` reasons only for the access
//! control dependency; the DESIGN update that follows this phase records the two kinds for
//! `registry_storage` (`Unavailable` -> `unreachable`, `TimedOut` -> `timed_out`; either
//! kind on `upsert` or `delete` means not confirmed). P4 adds `TimedOut` next to
//! `Unavailable` (additive: the P2/P3 users of `Unavailable` keep compiling). The mapping to
//! the reason is `RegistryError::reason()` (`ports_tests`).
//!
//! These tests run on the REAL clock (a paused clock cannot coexist with real DB IO) with a
//! short operation timeout, and the blocker is a transaction that holds the only pooled
//! connection, so the store's call has nothing to run on until the blocker is released.
//! Every wait is bounded with `real_bounded`.
//!
//! - `every_operation_times_out_when_the_database_does_not_answer`: get (also of the empty
//!   id, which must reach storage, §3.2 step 3), upsert, delete, list each return `TimedOut`
//!   after at least `operation_timeout` (§4.5, §4.7).
//!
//! A timeout INSIDE a statement (a lock held by a second connection) is the shared case
//! `registry_cases::a_timeout_inside_the_statement_leaves_the_row_old_or_complete`, which
//! runs on every lane.
//! - `the_store_works_again_once_the_blocker_is_gone`: a timeout leaves no broken state (§4.5).
//! - `a_pool_acquire_timeout_is_unavailable_not_timed_out`: the pool's own error first
//!   (§4.7 "Operators set it below `registry_operation_timeout`").
//! - `storage_errors_are_unavailable_on_every_operation`: unmigrated database (§3.2).
//! - `the_store_is_usable_as_a_registry_store_trait_object`: `Arc<dyn RegistryStore>` (§3.2).
//! - `a_generous_timeout_does_not_disturb_normal_calls`: no spurious timeout (§4.7).

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use toolkit_db::{ConnectOpts, DbError};

use crate::domain::ports::{RegistryError, RegistryStore};
use crate::domain::registry::{LastResult, RegistryEntry};
use crate::test_support::registry_db::{self, RegistryTestDb, real_bounded};

const SHORT: Duration = Duration::from_millis(150);

fn sample(id: &str) -> RegistryEntry {
    RegistryEntry::new(
        id,
        "1",
        LastResult::Succeeded,
        std::time::UNIX_EPOCH,
        "audit",
    )
    .expect("a storable entry")
}

fn single_connection(acquire: Duration) -> ConnectOpts {
    ConnectOpts {
        max_conns: Some(1),
        acquire_timeout: Some(acquire),
        ..ConnectOpts::default()
    }
}

/// Holds the pool's only connection until the returned sender fires or drops.
async fn hold_the_only_connection(
    db: &RegistryTestDb,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let provider = db.provider.clone();
    let (held_tx, held_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        provider
            .transaction(|_tx| {
                Box::pin(async move {
                    held_tx.send(()).ok();
                    release_rx.await.ok();
                    Ok::<(), DbError>(())
                })
            })
            .await
            .expect("the blocking transaction");
    });
    real_bounded(held_rx)
        .await
        .expect("the blocker holds the connection");
    (release_tx, handle)
}

#[tokio::test]
async fn every_operation_times_out_when_the_database_does_not_answer() {
    let db = registry_db::sqlite_with(single_connection(Duration::from_secs(30)), true).await;
    let store = db.store(SHORT);
    let (release, blocker) = hold_the_only_connection(&db).await;
    let entry = sample("app");

    for name in ["get", "get_empty", "upsert", "delete", "list"] {
        let started = Instant::now();
        let result: Result<(), RegistryError> = real_bounded(async {
            match name {
                "get" => store.get("app").await.map(|_| ()),
                "get_empty" => store.get("").await.map(|_| ()),
                "upsert" => store.upsert(&entry).await,
                "delete" => store.delete("app").await,
                _ => store.list().await.map(|_| ()),
            }
        })
        .await;
        let elapsed = started.elapsed();

        assert_eq!(result, Err(RegistryError::TimedOut), "{name}");
        assert!(
            elapsed >= SHORT,
            "{name} returned after {elapsed:?}, before the operation timeout"
        );
        assert!(
            elapsed < Duration::from_secs(10),
            "{name} kept waiting for {elapsed:?}: the caller must stop at the timeout"
        );
    }

    release.send(()).ok();
    real_bounded(blocker).await.expect("blocker task");
}

#[tokio::test]
async fn the_store_works_again_once_the_blocker_is_gone() {
    let db = registry_db::sqlite_with(single_connection(Duration::from_secs(30)), true).await;
    let store = db.store(Duration::from_secs(30));
    let impatient = db.store(SHORT);
    let (release, blocker) = hold_the_only_connection(&db).await;

    assert_eq!(
        real_bounded(impatient.get("app")).await,
        Err(RegistryError::TimedOut)
    );
    release.send(()).ok();
    real_bounded(blocker).await.expect("blocker task");

    real_bounded(store.upsert(&sample("app")))
        .await
        .expect("upsert after the timeout");
    assert_eq!(
        real_bounded(store.get("app")).await.expect("get"),
        Some(sample("app")),
        "the database answers normally again"
    );
    assert_eq!(
        real_bounded(impatient.get("app")).await.expect("get"),
        Some(sample("app")),
        "the same impatient store answers normally when the database does"
    );
}

#[tokio::test]
async fn a_pool_acquire_timeout_is_unavailable_not_timed_out() {
    // The pool gives up after 5s, far before the 30s registry timeout. The fixture creates
    // and migrates the file before the short pool exists, so the acquire timeout cannot
    // expire while it connects.
    let db = registry_db::sqlite_with(single_connection(Duration::from_secs(5)), true).await;
    let store = db.store(Duration::from_secs(30));
    let (release, blocker) = hold_the_only_connection(&db).await;
    let started = Instant::now();

    let result = real_bounded(store.get("app")).await;

    assert_eq!(result, Err(RegistryError::Unavailable));
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(5),
        "the pool waited its own timeout: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(25),
        "the pool's acquire timeout came first, not the 30s registry timeout: {elapsed:?}"
    );
    release.send(()).ok();
    real_bounded(blocker).await.expect("blocker task");
}

#[tokio::test]
async fn storage_errors_are_unavailable_on_every_operation() {
    let db = registry_db::sqlite_with(ConnectOpts::default(), false).await;
    let store = db.store(Duration::from_secs(30));

    assert_eq!(
        real_bounded(store.get("app")).await,
        Err(RegistryError::Unavailable)
    );
    assert_eq!(
        real_bounded(store.upsert(&sample("app"))).await,
        Err(RegistryError::Unavailable)
    );
    assert_eq!(
        real_bounded(store.delete("app")).await,
        Err(RegistryError::Unavailable)
    );
    assert_eq!(
        real_bounded(store.list()).await,
        Err(RegistryError::Unavailable)
    );
}

#[tokio::test]
async fn the_store_is_usable_as_a_registry_store_trait_object() {
    let db = registry_db::sqlite().await;
    let port: Arc<dyn RegistryStore> = Arc::new(db.store(Duration::from_secs(30)));

    real_bounded(port.upsert(&sample("app")))
        .await
        .expect("upsert");

    assert_eq!(
        real_bounded(port.get("app")).await.expect("get"),
        Some(sample("app"))
    );
}

#[tokio::test]
async fn a_generous_timeout_does_not_disturb_normal_calls() {
    let db = registry_db::sqlite().await;
    let store = db.store(Duration::from_secs(30));
    for i in 0..20 {
        real_bounded(store.upsert(&sample(&format!("app-{i}"))))
            .await
            .expect("upsert");
    }
    assert_eq!(real_bounded(store.list()).await.expect("list").len(), 20);
}

const SENTINEL: &str = "SENTINEL-4f1c9e";

/// The `registry_storage_failed` lines captured so far, each asserted to carry only
/// `op` and `cause`.
fn failure_lines(logs: &crate::test_support::LogBuffer) -> Vec<String> {
    logs.lines_with("registry_storage_failed")
}

fn assert_one_failure(logs: &crate::test_support::LogBuffer, op: &str, cause: &str) {
    let lines = failure_lines(logs);
    assert_eq!(lines.len(), 1, "exactly one event for {op}: {lines:?}");
    assert!(
        crate::test_support::has_field(&lines[0], "op", op),
        "{op}: {}",
        lines[0]
    );
    assert!(
        crate::test_support::has_field(&lines[0], "cause", cause),
        "{cause}: {}",
        lines[0]
    );
}

async fn run_op(store: &impl RegistryStore, op: &str) -> RegistryError {
    let entry = sample("app");
    let result = match op {
        "get" => store.get("app").await.map(|_| ()),
        "upsert" => store.upsert(&entry).await,
        "delete" => store.delete("app").await,
        _ => store.list().await.map(|_| ()),
    };
    result.expect_err("the operation fails")
}

#[tokio::test]
async fn a_failed_statement_logs_one_event_per_operation() {
    let db = registry_db::sqlite_with(ConnectOpts::default(), false).await;
    let store = db.store(Duration::from_secs(30));
    let (logs, _guard) = crate::test_support::capture_logs();
    logs.assert_live();

    for op in ["get", "upsert", "delete", "list"] {
        let before = failure_lines(&logs).len();
        assert_eq!(
            real_bounded(run_op(&store, op)).await,
            RegistryError::Unavailable
        );
        let lines = failure_lines(&logs);
        assert_eq!(lines.len(), before + 1, "{op}: one event");
        let line = lines.last().expect("event");
        assert!(crate::test_support::has_field(line, "op", op), "{line}");
        assert!(
            crate::test_support::has_field(line, "cause", "statement"),
            "{line}"
        );
        assert!(!line.contains("no such table"), "{line}");
    }
}

#[tokio::test]
async fn a_timeout_logs_one_timed_out_event_per_operation() {
    let db = registry_db::sqlite_with(single_connection(Duration::from_secs(30)), true).await;
    let store = db.store(SHORT);
    let (release, blocker) = hold_the_only_connection(&db).await;
    let (logs, _guard) = crate::test_support::capture_logs();
    logs.assert_live();

    for op in ["get", "upsert", "delete", "list"] {
        let before = failure_lines(&logs).len();
        assert_eq!(
            real_bounded(run_op(&store, op)).await,
            RegistryError::TimedOut
        );
        let lines = failure_lines(&logs);
        assert_eq!(lines.len(), before + 1, "{op}: one event");
        let line = lines.last().expect("event");
        assert!(crate::test_support::has_field(line, "op", op), "{line}");
        assert!(
            crate::test_support::has_field(line, "cause", "timed_out"),
            "{line}"
        );
    }
    release.send(()).ok();
    real_bounded(blocker).await.expect("blocker task");
}

#[tokio::test]
async fn a_pool_acquire_failure_logs_a_connection_event() {
    let db = registry_db::sqlite_with(single_connection(Duration::from_millis(300)), true).await;
    let store = db.store(Duration::from_secs(30));
    let (release, blocker) = hold_the_only_connection(&db).await;
    let (logs, _guard) = crate::test_support::capture_logs();
    logs.assert_live();

    assert_eq!(
        real_bounded(run_op(&store, "get")).await,
        RegistryError::Unavailable
    );
    assert_one_failure(&logs, "get", "connection");

    release.send(()).ok();
    real_bounded(blocker).await.expect("blocker task");
}

#[tokio::test]
async fn a_corrupt_row_logs_row_unreadable_without_its_content() {
    use sea_orm::ConnectionTrait;
    let db = registry_db::sqlite().await;
    let store = db.store(Duration::from_secs(30));
    real_bounded(store.upsert(&sample(&format!("{SENTINEL}-id"))))
        .await
        .expect("seed a valid row");
    db.raw
        .execute_unprepared("PRAGMA ignore_check_constraints = ON")
        .await
        .expect("allow the corrupt row");
    db.raw
        .execute_unprepared(&format!(
            "UPDATE application_registry SET last_result = '{SENTINEL}-bogus', \
             version = '{SENTINEL}-ver', audit_id = '{SENTINEL}-audit'"
        ))
        .await
        .expect("corrupt the row");
    let (logs, _guard) = crate::test_support::capture_logs();
    logs.assert_live();

    for op in ["get", "list"] {
        let before = failure_lines(&logs).len();
        let result = match op {
            "get" => store.get(&format!("{SENTINEL}-id")).await.map(|_| ()),
            _ => store.list().await.map(|_| ()),
        };
        assert_eq!(result, Err(RegistryError::Unavailable), "{op}");
        let lines = failure_lines(&logs);
        assert_eq!(lines.len(), before + 1, "{op}: one event");
        let line = lines.last().expect("event");
        assert!(crate::test_support::has_field(line, "op", op), "{line}");
        assert!(
            crate::test_support::has_field(line, "cause", "row_unreadable"),
            "{line}"
        );
    }
    assert!(
        !logs.text().contains(SENTINEL),
        "row content must never be logged"
    );
}
