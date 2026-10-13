//! Unit tests for the registry migration set (DESIGN §3.7 "Additional info" and
//! "Constraints", §3.2 "Startup and Registration": migrations come from
//! `DatabaseCapability::migrations()`, run by the platform before `init`).
//!
//! Planned API in `crate::infra::storage::migrations` (new file, P4). The module ends with:
//!
//! ```text
//! #[cfg(test)]
//! #[cfg_attr(coverage_nightly, coverage(off))]
//! #[path = "migrations_tests.rs"]
//! mod migrations_tests;
//! ```
//!
//! ```text
//! pub mod m20261013_000001_create_application_registry;   // file: migrations/<name>.rs
//! pub struct Migrator;                                     // impl MigratorTrait
//! // migrations() == [m20261013_000001_create_application_registry::Migration]
//! ```
//!
//! The one migration (`#[derive(DeriveMigrationName)] pub struct Migration;`):
//!
//! - `up`: `CREATE TABLE IF NOT EXISTS application_registry` with the five NOT NULL columns
//!   of §3.7, primary key `application_id`, no other index, no foreign key, no trigger,
//!   and CHECK constraints that the DATABASE enforces on every backend: `application_id`
//!   and `version` non-empty and at most `REGISTRY_TEXT_MAX_BYTES` BYTES of UTF-8 (a plain
//!   `VARCHAR(255)` counts characters on Postgres and not at all on `SQLite`, so a CHECK on
//!   the octet length is required), and `last_result` limited to the five listed tokens.
//!   Text columns compare binary and no-pad (`C` collation on Postgres, `BINARY` on
//!   `SQLite`, `utf8mb4_0900_bin` on `MySQL`, including `last_result`: the default `MySQL`
//!   collation is case-insensitive and would let `'Succeeded'` through the CHECK). `MySQL`
//!   uses `DATETIME(6)` for `last_seeded_at` (`TIMESTAMP` ends in 2038) and `LONGTEXT` for
//!   `audit_id` (`TEXT` caps at 64 KB). Raw SQL is allowed here and nowhere else (DESIGN §2.2).
//! - `down`: `DROP TABLE IF EXISTS application_registry`.
//!
//! The schema itself (columns, NOT NULL, primary key, no tenant column, no secondary index,
//! idempotent re-run, down and up again, bound / empty / token checks) is asserted by the
//! shared cases `registry_cases::{migration_*, database_*}` on `SQLite` and on Postgres.
//!
//! - `migrator_holds_exactly_one_initial_migration`: one migration (§3.7).
//! - `migration_name_follows_the_platform_convention`: `mYYYYMMDD_NNNNNN_<snake_case>` (§3.7
//!   "dated migration").
//! - `migration_applies_through_the_runner_on_an_empty_database`: applied 1, skipped 0.
//! - `migration_history_survives_a_restart`: a second run skips it by name (§3.7).

use sea_orm_migration::MigratorTrait;
use toolkit_db::ConnectOpts;
use toolkit_db::migration_runner::run_migrations_for_testing;

use super::Migrator;
use crate::test_support::registry_db::{self, real_bounded};

#[test]
fn migrator_holds_exactly_one_initial_migration() {
    assert_eq!(
        Migrator::migrations().len(),
        1,
        "one initial migration creates the table"
    );
}

#[test]
fn migration_name_follows_the_platform_convention() {
    let migrations = Migrator::migrations();
    let name = migrations[0].name().to_owned();

    let parts: Vec<&str> = name.splitn(3, '_').collect();
    assert_eq!(parts.len(), 3, "{name}");
    let date = parts[0]
        .strip_prefix('m')
        .expect("the name starts with `m`");
    assert_eq!(date.len(), 8, "{name}: YYYYMMDD");
    assert!(date.bytes().all(|b| b.is_ascii_digit()), "{name}");
    let (year, month, day) = (
        date[..4].parse::<u32>().expect("year"),
        date[4..6].parse::<u32>().expect("month"),
        date[6..].parse::<u32>().expect("day"),
    );
    assert!((2026..=2099).contains(&year), "{name}: a plausible year");
    assert!((1..=12).contains(&month), "{name}");
    assert!((1..=31).contains(&day), "{name}");
    assert_eq!(parts[1].len(), 6, "{name}: six-digit sequence");
    assert_eq!(
        parts[1], "000001",
        "{name}: the first migration of the gear"
    );
    assert!(
        parts[2]
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "{name}: snake_case description"
    );
    assert_eq!(parts[2], "create_application_registry");
}

#[tokio::test]
async fn migration_applies_through_the_runner_on_an_empty_database() {
    let db = registry_db::sqlite_with(ConnectOpts::default(), false).await;
    assert!(!registry_db::table_exists(&db, registry_db::REGISTRY_TABLE).await);

    let result = real_bounded(run_migrations_for_testing(
        &db.provider.db(),
        Migrator::migrations(),
    ))
    .await
    .expect("the migration applies on an empty database");

    assert_eq!(result.applied, 1);
    assert_eq!(result.skipped, 0);
    assert_eq!(
        result.applied_names,
        [Migrator::migrations()[0].name().to_owned()]
    );
    assert!(registry_db::table_exists(&db, registry_db::REGISTRY_TABLE).await);
}

#[tokio::test]
async fn migration_history_survives_a_restart() {
    let db = registry_db::sqlite_with(ConnectOpts::default(), false).await;
    let first = real_bounded(run_migrations_for_testing(
        &db.provider.db(),
        Migrator::migrations(),
    ))
    .await
    .expect("first run");
    let second = real_bounded(run_migrations_for_testing(
        &db.provider.db(),
        Migrator::migrations(),
    ))
    .await
    .expect("second run");

    assert_eq!(first.applied, 1);
    assert_eq!(second.applied, 0);
    assert_eq!(
        second.skipped, 1,
        "a restart skips the recorded migration by name"
    );
}

#[tokio::test]
async fn the_seeded_at_range_check_exists_on_sqlite() {
    let db = registry_db::sqlite().await;
    assert!(
        registry_db::check_constraint_exists(
            &db,
            registry_db::REGISTRY_TABLE,
            "ck_application_registry_seeded_at"
        )
        .await
    );
}
