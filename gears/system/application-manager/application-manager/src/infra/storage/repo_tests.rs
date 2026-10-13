//! Unit tests for the registry repository (DESIGN §3.2 "Registry Repository", §3.7,
//! ADR 0007).
//!
//! Planned API in `crate::infra::storage::repo` (new file, P4). The module ends with:
//!
//! ```text
//! #[cfg(test)]
//! #[cfg_attr(coverage_nightly, coverage(off))]
//! #[path = "repo_tests.rs"]
//! mod repo_tests;
//! ```
//!
//! ```text
//! /// Result of a repository delete. The port `RegistryStore::delete` folds both into Ok
//! /// (DESIGN §3.2: deleting an entry that no longer exists counts as deleted). The type
//! /// exists so the tests can prove that a delete removed exactly the named id without a
//! /// second read (`registry_cases::repo_delete_reports_deleted_then_not_found`).
//! #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//! pub enum DeleteOutcome { Deleted, NotFound }
//!
//! /// Stateless, `DBRunner`-generic repository over the unrestricted entity. Every call takes
//! /// the permit's `AccessScope` (the allow-all scope, ADR 0007); the repository writes no
//! /// tenant value and issues no raw SQL. Built only from `secure()` wrappers.
//! pub struct RegistryRepo;
//! impl RegistryRepo {
//!     pub async fn get(runner: &impl DBRunner, scope: &AccessScope, application_id: &str)
//!         -> Result<Option<RegistryEntry>, RegistryError>;
//!     /// One `INSERT .. ON CONFLICT (application_id) DO UPDATE` over every other column:
//!     /// the last writer wins and a row is never a mix (DESIGN §4.2). Fails closed with
//!     /// `Unavailable`, writing nothing, when `scope` is not allow-all.
//!     pub async fn upsert(runner: &impl DBRunner, scope: &AccessScope, entry: &RegistryEntry)
//!         -> Result<(), RegistryError>;
//!     pub async fn delete(runner: &impl DBRunner, scope: &AccessScope, application_id: &str)
//!         -> Result<DeleteOutcome, RegistryError>;
//!     /// All entries; no ordering and no pagination (DESIGN §4.10).
//!     pub async fn list(runner: &impl DBRunner, scope: &AccessScope)
//!         -> Result<Vec<RegistryEntry>, RegistryError>;
//! }
//! ```
//!
//! Error mapping: a failed statement, a connection error, a pool acquire timeout, a
//! `ScopeError`, and a stored row that is not a valid `RegistryEntry` all map to
//! `RegistryError::Unavailable` ("storage cannot answer"). The repository never interprets
//! an id (DESIGN §3.2 "Registry Repository"): the one exception is an id holding NUL, which
//! no backend can bind in a text value, so `get` answers `None` and `delete` `NotFound`
//! WITHOUT touching the database. The empty id, control characters and an over-long id are
//! real queries that match nothing (the seed reads the empty id to prove storage answers).
//! The short-circuit for a never-storable uninstall id is the orchestrator's, through
//! `is_storable_text`, not the repository's.
//!
//! `upsert` fails closed: the unrestricted entity makes the platform skip its insert-scope
//! check, so the repository itself refuses (`Unavailable`, no write) a scope that is not
//! allow-all. The store always passes allow-all (ADR 0007).
//!
//! The backend-independent behaviour (upsert, overwrite, get, delete, list, bounds, unicode,
//! scopes, transactions) is asserted by `registry_cases::*` on `SQLite`, Postgres and `MySQL`.
//!
//! - `delete_outcome_values_differ_and_are_copy`: the distinct delete result (§3.2).
//! - `storage_errors_map_to_unavailable_on_every_operation`: no table -> `Unavailable` (§3.2).
//! - `ids_holding_nul_never_reach_the_database`: answered on a database with no table.
//! - `every_other_id_is_a_real_query`: `""`, control characters and an over-long id on a
//!   database with no table -> `Unavailable` (§3.2 step 3: storage must answer for `""`).
//! - `repo_never_issues_a_write_for_a_read`: a failed read leaves no table behind.

use toolkit_db::ConnectOpts;
use toolkit_db::secure::AccessScope;

use super::{DeleteOutcome, RegistryRepo};
use crate::domain::limits::REGISTRY_TEXT_MAX_BYTES;
use crate::domain::ports::RegistryError;
use crate::domain::registry::{LastResult, RegistryEntry};
use crate::test_support::registry_db::{self, REGISTRY_TABLE, real_bounded};

fn sample() -> RegistryEntry {
    RegistryEntry::new(
        "app",
        "1",
        LastResult::Succeeded,
        std::time::UNIX_EPOCH,
        "audit",
    )
    .expect("a storable entry")
}

#[test]
fn delete_outcome_values_differ_and_are_copy() {
    let a = DeleteOutcome::Deleted;
    let b = a;
    assert_eq!(a, b);
    assert_ne!(DeleteOutcome::Deleted, DeleteOutcome::NotFound);
}

#[tokio::test]
async fn storage_errors_map_to_unavailable_on_every_operation() {
    // No migration: the table does not exist, so every statement fails in the database.
    let db = registry_db::sqlite_with(ConnectOpts::default(), false).await;
    let conn = db.conn();
    let scope = AccessScope::allow_all();

    assert_eq!(
        real_bounded(RegistryRepo::get(&conn, &scope, "app")).await,
        Err(RegistryError::Unavailable)
    );
    assert_eq!(
        real_bounded(RegistryRepo::upsert(&conn, &scope, &sample())).await,
        Err(RegistryError::Unavailable)
    );
    assert_eq!(
        real_bounded(RegistryRepo::delete(&conn, &scope, "app")).await,
        Err(RegistryError::Unavailable)
    );
    assert_eq!(
        real_bounded(RegistryRepo::list(&conn, &scope)).await,
        Err(RegistryError::Unavailable)
    );
}

#[tokio::test]
async fn ids_holding_nul_never_reach_the_database() {
    let db = registry_db::sqlite_with(ConnectOpts::default(), false).await;
    let conn = db.conn();
    let scope = AccessScope::allow_all();

    for id in ["a\0b", "\0", "app\0"] {
        assert_eq!(
            real_bounded(RegistryRepo::get(&conn, &scope, id)).await,
            Ok(None),
            "get {id:?}"
        );
        assert_eq!(
            real_bounded(RegistryRepo::delete(&conn, &scope, id)).await,
            Ok(DeleteOutcome::NotFound),
            "delete {id:?}"
        );
    }
}

#[tokio::test]
async fn every_other_id_is_a_real_query() {
    // No table: a statement that reaches the database fails, so `Unavailable` proves the
    // repository asked.
    let db = registry_db::sqlite_with(ConnectOpts::default(), false).await;
    let conn = db.conn();
    let scope = AccessScope::allow_all();
    let too_long = "a".repeat(REGISTRY_TEXT_MAX_BYTES + 1);

    for id in ["", "a\nb", "\u{85}", "a\u{7f}", too_long.as_str()] {
        assert_eq!(
            real_bounded(RegistryRepo::get(&conn, &scope, id)).await,
            Err(RegistryError::Unavailable),
            "get {id:?}"
        );
        assert_eq!(
            real_bounded(RegistryRepo::delete(&conn, &scope, id)).await,
            Err(RegistryError::Unavailable),
            "delete {id:?}"
        );
    }
}

#[tokio::test]
async fn the_empty_id_matches_nothing_on_a_migrated_database() {
    let db = registry_db::sqlite().await;
    let conn = db.conn();
    let scope = AccessScope::allow_all();

    assert_eq!(
        real_bounded(RegistryRepo::get(&conn, &scope, "")).await,
        Ok(None)
    );
    assert_eq!(
        real_bounded(RegistryRepo::delete(&conn, &scope, "")).await,
        Ok(DeleteOutcome::NotFound)
    );
}

#[tokio::test]
async fn repo_never_issues_a_write_for_a_read() {
    let db = registry_db::sqlite_with(ConnectOpts::default(), false).await;
    let conn = db.conn();
    let scope = AccessScope::allow_all();

    real_bounded(RegistryRepo::get(&conn, &scope, "app"))
        .await
        .ok();
    real_bounded(RegistryRepo::list(&conn, &scope)).await.ok();

    assert!(
        !registry_db::table_exists(&db, REGISTRY_TABLE).await,
        "the repository never creates the table: only migrations do"
    );
}

#[tokio::test]
async fn a_refused_scope_logs_one_scope_refused_event() {
    let db = registry_db::sqlite().await;
    let conn = db.conn();
    let (logs, _guard) = crate::test_support::capture_logs();
    logs.assert_live();

    let scope = AccessScope::deny_all();
    assert_eq!(
        real_bounded(RegistryRepo::upsert(&conn, &scope, &sample())).await,
        Err(RegistryError::Unavailable)
    );

    let lines = logs.lines_with("registry_storage_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(crate::test_support::has_field(&lines[0], "op", "upsert"));
    assert!(crate::test_support::has_field(
        &lines[0],
        "cause",
        "scope_refused"
    ));
}
