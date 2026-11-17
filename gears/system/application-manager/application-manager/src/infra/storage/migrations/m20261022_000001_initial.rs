//! Initial schema: `operations`, the insert-only `operation_history`, and
//! `applications` (DESIGN 3.7), one statement list per backend.
//!
//! # Column mapping
//!
//! | Kind | `PostgreSQL` | `SQLite` | `MySQL` |
//! |---|---|---|---|
//! | UUID | `UUID` | `BLOB` + `length(x) = 16` | `BINARY(16)` |
//! | timestamp | `TIMESTAMPTZ` | `TEXT` `YYYY-MM-DD HH:MM:SS.SSS` UTC | `DATETIME(6)` |
//! | application id | `VARCHAR(255) COLLATE "C"` | `TEXT COLLATE BINARY` | `VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin` |
//! | JSON | `JSONB` | `TEXT` | `JSON` |
//! | enum token | `VARCHAR` `COLLATE "C"` + `CHECK` | `TEXT COLLATE BINARY` + `CHECK` | `VARCHAR` `utf8mb4_0900_bin` + `CHECK` |
//!
//! Decisions rather than mechanics:
//!
//! * **UUIDs are 16 raw bytes on `SQLite` and `MySQL`.** The driver encodes
//!   `Uuid` as its bytes, so a `CHAR(36)` column would receive binary (`MySQL`
//!   refuses it; `SQLite` silently stores a blob in a `TEXT` column). `SQLite`
//!   gets a `length(x) = 16` CHECK because a declared `BLOB` constrains
//!   nothing there. `PostgreSQL` keeps the native `UUID`.
//! * **The application id compares as bytes, with no padding.** `"a"`, `"A"`
//!   and `"a "` are three applications (PRD `fr-one-operation-per-application`).
//!   `SQLite`'s `TEXT` default is `BINARY`, stated anyway so a later
//!   `COLLATE NOCASE` cannot creep in. `utf8mb4_0900_bin` is a NO PAD
//!   collation; the application id may be any UTF-8 text, so the character set
//!   is `utf8mb4`, not ASCII.
//! * **`MySQL` uses `DATETIME(6)`, not `TIMESTAMP(6)`**: with
//!   `explicit_defaults_for_timestamp` off, the first `TIMESTAMP` column of a
//!   table silently gains `DEFAULT CURRENT_TIMESTAMP ON UPDATE
//!   CURRENT_TIMESTAMP`, which would rewrite `accepted_at` on every update.
//! * **Enums are text with a `CHECK`, compared as bytes.** `action`, `status`,
//!   `category` and `kind` hold the snake-case tokens of the SDK and domain
//!   types, with the same binary collation as the application id on every
//!   backend, so a `CHECK`, the partial index predicate and the `MySQL`
//!   generated column are case-sensitive: `'ACCEPTED'` is not `'accepted'`.
//! * **One open operation per application** is a unique index over the open
//!   rows (`accepted`, `running`) (DESIGN 3.7). `PostgreSQL` and `SQLite` have
//!   partial indexes. `MySQL` has none, so a stored generated column
//!   `open_application_id` holds the application id while the row is open and
//!   `NULL` otherwise, and a `UNIQUE KEY` over it constrains only open rows
//!   (`NULL`s never conflict).
//! * **`MySQL` declares its indexes inline** as `KEY` clauses. It has no
//!   `CREATE INDEX IF NOT EXISTS` and its DDL is not transactional, so an inline
//!   declaration is the only form covered by the table's own `IF NOT EXISTS`.
//! * **`operation_history` has no foreign key to `operations`**: purging an
//!   operation must not take its events with it. It carries no tenant column
//!   (ADR 0004), and the gear writes it with insert statements only.
//! * **`applications` has no foreign key to `operations` either**: an
//!   application record outlives the purge of its operation. Like the
//!   history, it carries no tenant column and is never purged.
//! * **Application ids and versions are at most `APPLICATION_ID_MAX_BYTES`
//!   bytes**, checked with `octet_length` (`PostgreSQL`, `MySQL`) and the
//!   blob length (`SQLite`); a `VARCHAR(255)` alone counts characters. A
//!   compile-time assertion keeps the literal in step with the constant.

use sea_orm::{ConnectionTrait, Statement};
use sea_orm_migration::prelude::*;

use crate::constants::APPLICATION_ID_MAX_BYTES;

// The DDL below spells the limit as the literal 255.
const _: () = assert!(APPLICATION_ID_MAX_BYTES == 255);

#[derive(DeriveMigrationName)]
pub struct Migration;

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------

const PG_UP_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS operations (
        operation_id       UUID          NOT NULL,
        application_id     VARCHAR(255) COLLATE \"C\" NOT NULL,
        action             VARCHAR(16) COLLATE \"C\" NOT NULL,
        status             VARCHAR(16) COLLATE \"C\" NOT NULL,
        category           VARCHAR(32) COLLATE \"C\" NULL,
        report             JSONB         NOT NULL,
        accepted_at        TIMESTAMPTZ   NOT NULL,
        started_at         TIMESTAMPTZ   NULL,
        finished_at        TIMESTAMPTZ   NULL,
        run_deadline       TIMESTAMPTZ   NOT NULL,
        finished_event_id  UUID          NOT NULL,

        CONSTRAINT pk_operations PRIMARY KEY (operation_id),
        CONSTRAINT ck_operations_action CHECK (action IN ('seed', 'uninstall')),
        CONSTRAINT ck_operations_application_id_bytes CHECK (octet_length(application_id) <= 255),
        CONSTRAINT ck_operations_status CHECK (
            status IN ('accepted', 'running', 'completed')
        ),
        CONSTRAINT ck_operations_category CHECK (
            category IS NULL OR category IN (
                'succeeded', 'invalid_manifest', 'plugin_rejected', 'not_permitted',
                'internal_failure'
            )
        )
    )",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_operations_open_application
        ON operations (application_id) WHERE status IN ('accepted', 'running')",
    "CREATE INDEX IF NOT EXISTS idx_operations_status_deadline
        ON operations (status, run_deadline)",
    "CREATE INDEX IF NOT EXISTS idx_operations_finished_at
        ON operations (finished_at)",
    "CREATE TABLE IF NOT EXISTS operation_history (
        event_id        UUID         NOT NULL,
        kind               VARCHAR(16) COLLATE \"C\" NOT NULL,
        correlation_id  UUID         NOT NULL,
        action             VARCHAR(16) COLLATE \"C\" NOT NULL,
        written_at      TIMESTAMPTZ  NOT NULL,
        content         JSONB        NOT NULL,

        CONSTRAINT pk_operation_history PRIMARY KEY (event_id),
        CONSTRAINT ck_operation_history_kind CHECK (kind IN ('accepted', 'finished', 'refused')),
        CONSTRAINT ck_operation_history_action CHECK (action IN ('seed', 'uninstall'))
    )",
    "CREATE INDEX IF NOT EXISTS idx_operation_history_correlation_id
        ON operation_history (correlation_id)",
    "CREATE TABLE IF NOT EXISTS applications (
        application_id  VARCHAR(255) COLLATE \"C\" NOT NULL,
        version         VARCHAR(255) COLLATE \"C\" NOT NULL,
        operation_id    UUID         NOT NULL,
        category        VARCHAR(32) COLLATE \"C\" NOT NULL,
        finished_at     TIMESTAMPTZ  NOT NULL,

        CONSTRAINT pk_applications PRIMARY KEY (application_id),
        CONSTRAINT ck_applications_application_id_bytes CHECK (octet_length(application_id) <= 255),
        CONSTRAINT ck_applications_version_bytes CHECK (octet_length(version) <= 255),
        CONSTRAINT ck_applications_category CHECK (
            category IN ('succeeded', 'invalid_manifest', 'plugin_rejected', 'not_permitted', 'internal_failure')
        )
    )",
];

// ---------------------------------------------------------------------------
// SQLite
// ---------------------------------------------------------------------------

const SQLITE_UP_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS operations (
        operation_id       BLOB  NOT NULL,
        application_id     TEXT COLLATE BINARY NOT NULL,
        action             TEXT COLLATE BINARY NOT NULL,
        status             TEXT COLLATE BINARY NOT NULL,
        category           TEXT COLLATE BINARY NULL,
        report             TEXT  NOT NULL,
        accepted_at        TEXT  NOT NULL,
        started_at         TEXT  NULL,
        finished_at        TEXT  NULL,
        run_deadline       TEXT  NOT NULL,
        finished_event_id  BLOB  NOT NULL,

        CONSTRAINT pk_operations PRIMARY KEY (operation_id),
        CONSTRAINT ck_operations_operation_id_len CHECK (length(operation_id) = 16),
        CONSTRAINT ck_operations_finished_event_id_len CHECK (length(finished_event_id) = 16),
        CONSTRAINT ck_operations_action CHECK (action IN ('seed', 'uninstall')),
        CONSTRAINT ck_operations_application_id_bytes CHECK (length(CAST(application_id AS BLOB)) <= 255),
        CONSTRAINT ck_operations_status CHECK (
            status IN ('accepted', 'running', 'completed')
        ),
        CONSTRAINT ck_operations_category CHECK (
            category IS NULL OR category IN (
                'succeeded', 'invalid_manifest', 'plugin_rejected', 'not_permitted',
                'internal_failure'
            )
        )
    )",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_operations_open_application
        ON operations (application_id) WHERE status IN ('accepted', 'running')",
    "CREATE INDEX IF NOT EXISTS idx_operations_status_deadline
        ON operations (status, run_deadline)",
    "CREATE INDEX IF NOT EXISTS idx_operations_finished_at
        ON operations (finished_at)",
    "CREATE TABLE IF NOT EXISTS operation_history (
        event_id        BLOB  NOT NULL,
        kind               TEXT COLLATE BINARY NOT NULL,
        correlation_id  BLOB  NOT NULL,
        action             TEXT COLLATE BINARY NOT NULL,
        written_at      TEXT  NOT NULL,
        content         TEXT  NOT NULL,

        CONSTRAINT pk_operation_history PRIMARY KEY (event_id),
        CONSTRAINT ck_operation_history_event_id_len CHECK (length(event_id) = 16),
        CONSTRAINT ck_operation_history_correlation_id_len CHECK (length(correlation_id) = 16),
        CONSTRAINT ck_operation_history_kind CHECK (kind IN ('accepted', 'finished', 'refused')),
        CONSTRAINT ck_operation_history_action CHECK (action IN ('seed', 'uninstall'))
    )",
    "CREATE INDEX IF NOT EXISTS idx_operation_history_correlation_id
        ON operation_history (correlation_id)",
    "CREATE TABLE IF NOT EXISTS applications (
        application_id  TEXT COLLATE BINARY NOT NULL,
        version         TEXT COLLATE BINARY NOT NULL,
        operation_id    BLOB  NOT NULL,
        category        TEXT COLLATE BINARY NOT NULL,
        finished_at     TEXT  NOT NULL,

        CONSTRAINT pk_applications PRIMARY KEY (application_id),
        CONSTRAINT ck_applications_operation_id_len CHECK (length(operation_id) = 16),
        CONSTRAINT ck_applications_application_id_bytes CHECK (length(CAST(application_id AS BLOB)) <= 255),
        CONSTRAINT ck_applications_version_bytes CHECK (length(CAST(version AS BLOB)) <= 255),
        CONSTRAINT ck_applications_category CHECK (
            category IN ('succeeded', 'invalid_manifest', 'plugin_rejected', 'not_permitted', 'internal_failure')
        )
    )",
];

// ---------------------------------------------------------------------------
// MySQL
// ---------------------------------------------------------------------------

const MYSQL_UP_STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS operations (
        operation_id       BINARY(16)   NOT NULL,
        application_id     VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
        action             VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
        status             VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
        category           VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NULL,
        report             JSON         NOT NULL,
        accepted_at        DATETIME(6)  NOT NULL,
        started_at         DATETIME(6)  NULL,
        finished_at        DATETIME(6)  NULL,
        run_deadline       DATETIME(6)  NOT NULL,
        finished_event_id  BINARY(16)   NOT NULL,
        open_application_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin
            GENERATED ALWAYS AS (CASE WHEN status IN ('accepted', 'running') THEN application_id END) STORED,

        PRIMARY KEY (operation_id),
        UNIQUE KEY ux_operations_open_application (open_application_id),
        KEY idx_operations_status_deadline (status, run_deadline),
        KEY idx_operations_finished_at (finished_at),
        CONSTRAINT ck_operations_action CHECK (action IN ('seed', 'uninstall')),
        CONSTRAINT ck_operations_application_id_bytes CHECK (octet_length(application_id) <= 255),
        CONSTRAINT ck_operations_status CHECK (
            status IN ('accepted', 'running', 'completed')
        ),
        CONSTRAINT ck_operations_category CHECK (
            category IS NULL OR category IN (
                'succeeded', 'invalid_manifest', 'plugin_rejected', 'not_permitted',
                'internal_failure'
            )
        )
    )",
    "CREATE TABLE IF NOT EXISTS operation_history (
        event_id        BINARY(16)   NOT NULL,
        kind               VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
        correlation_id  BINARY(16)   NOT NULL,
        action             VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
        written_at      DATETIME(6)  NOT NULL,
        content         JSON         NOT NULL,

        PRIMARY KEY (event_id),
        KEY idx_operation_history_correlation_id (correlation_id),
        CONSTRAINT ck_operation_history_kind CHECK (kind IN ('accepted', 'finished', 'refused')),
        CONSTRAINT ck_operation_history_action CHECK (action IN ('seed', 'uninstall'))
    )",
    "CREATE TABLE IF NOT EXISTS applications (
        application_id  VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
        version         VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
        operation_id    BINARY(16)   NOT NULL,
        category        VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL,
        finished_at     DATETIME(6)  NOT NULL,

        PRIMARY KEY (application_id),
        CONSTRAINT ck_applications_application_id_bytes CHECK (octet_length(application_id) <= 255),
        CONSTRAINT ck_applications_version_bytes CHECK (octet_length(version) <= 255),
        CONSTRAINT ck_applications_category CHECK (
            category IN ('succeeded', 'invalid_manifest', 'plugin_rejected', 'not_permitted', 'internal_failure')
        )
    )",
];

// ---------------------------------------------------------------------------
// Down: indexes go with their tables.
// ---------------------------------------------------------------------------

const DOWN_STATEMENTS: &[&str] = &[
    "DROP TABLE IF EXISTS applications",
    "DROP TABLE IF EXISTS operation_history",
    "DROP TABLE IF EXISTS operations",
];

// ---------------------------------------------------------------------------
// Migration dispatch.
// ---------------------------------------------------------------------------

/// `DatabaseBackend` is `#[non_exhaustive]`, so the dispatch below needs a
/// fallback arm. Returning an error rather than picking a dialect keeps the
/// schema from being created with DDL meant for a different engine.
fn unsupported_backend(backend: sea_orm::DatabaseBackend) -> DbErr {
    DbErr::Migration(format!(
        "application-manager migrations support Postgres, SQLite and MySQL only; \
         got unsupported database backend {backend:?}"
    ))
}

fn up_statements(backend: sea_orm::DatabaseBackend) -> Result<&'static [&'static str], DbErr> {
    match backend {
        sea_orm::DatabaseBackend::Postgres => Ok(PG_UP_STATEMENTS),
        sea_orm::DatabaseBackend::Sqlite => Ok(SQLITE_UP_STATEMENTS),
        sea_orm::DatabaseBackend::MySql => Ok(MYSQL_UP_STATEMENTS),
        other => Err(unsupported_backend(other)),
    }
}

// `MigrationTrait` is itself `#[async_trait]`-ified with an elided
// `SchemaManager<'_>` lifetime, so the impl must elide it too; spelling `<'_>`
// makes `async_trait` generate a different lifetime set and fails with E0195.
// The crate denies `rust_2018_idioms`, hence the targeted allow.
#[allow(elided_lifetimes_in_paths)]
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        let conn = manager.get_connection();
        for sql in up_statements(backend)? {
            conn.execute_raw(Statement::from_string(backend, (*sql).to_owned()))
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let backend = manager.get_database_backend();
        // Reject the unsupported backend on the way down too: a `down` that
        // silently succeeds where `up` refused to run would report a rollback
        // that never happened.
        _ = up_statements(backend)?;
        let conn = manager.get_connection();
        for sql in DOWN_STATEMENTS {
            conn.execute_raw(Statement::from_string(backend, (*sql).to_owned()))
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "m20261022_000001_initial_tests.rs"]
mod tests;
