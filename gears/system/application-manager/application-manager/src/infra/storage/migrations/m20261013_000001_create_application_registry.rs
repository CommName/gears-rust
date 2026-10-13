//! Creates `application_registry` (DESIGN §3.7): one arm per backend so that key columns
//! compare bytewise everywhere and the length CHECKs count bytes.
//!
//! Minimum backends: `MySQL` 8.0.17+ (collation `utf8mb4_0900_bin`, enforced CHECK
//! constraints, `octet_length` in CHECKs) or `MariaDB` 11.4.5+ (which provides that
//! collation); older servers reject the `MySQL` arm. `PostgreSQL` and `SQLite` need no
//! more than their supported releases.
//!
//! `CREATE TABLE IF NOT EXISTS` assumes the table is owned by this migration: a
//! pre-existing `application_registry` of another shape is kept as is and is not
//! repaired or checked.
//!
//! `last_seeded_at` is range-checked on every backend to `1970-01-01T00:00:00Z` up to
//! but excluding `10000-01-01T00:00:00Z`, so no out-of-range or infinite time is stored.
use sea_orm::DbBackend;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

const LAST_RESULTS: &str =
    "'succeeded', 'partially_failed', 'plugin_rejected', 'not_permitted', 'internal_failure'";

fn create_sql(backend: DbBackend) -> Result<String, DbErr> {
    Ok(match backend {
        DbBackend::Sqlite => format!(
            "CREATE TABLE IF NOT EXISTS application_registry (\
             application_id VARCHAR(255) NOT NULL COLLATE BINARY, \
             version VARCHAR(255) NOT NULL COLLATE BINARY, \
             last_result VARCHAR(32) NOT NULL, \
             last_seeded_at TIMESTAMP_WITH_TIMEZONE_TEXT NOT NULL, \
             audit_id TEXT NOT NULL, \
             PRIMARY KEY (application_id), \
             CONSTRAINT ck_application_registry_id CHECK (length(CAST(application_id AS BLOB)) BETWEEN 1 AND 255), \
             CONSTRAINT ck_application_registry_version CHECK (length(CAST(version AS BLOB)) BETWEEN 1 AND 255), \
             CONSTRAINT ck_application_registry_last_result CHECK (last_result IN ({LAST_RESULTS})), \
             CONSTRAINT ck_application_registry_seeded_at CHECK (last_seeded_at GLOB '[0-9][0-9][0-9][0-9]-*' AND last_seeded_at >= '1970-01-01'))"
        ),
        DbBackend::Postgres => format!(
            "CREATE TABLE IF NOT EXISTS application_registry (\
             application_id VARCHAR(255) COLLATE \"C\" NOT NULL, \
             version VARCHAR(255) COLLATE \"C\" NOT NULL, \
             last_result VARCHAR(32) NOT NULL, \
             last_seeded_at TIMESTAMPTZ(6) NOT NULL, \
             audit_id TEXT NOT NULL, \
             CONSTRAINT pk_application_registry PRIMARY KEY (application_id), \
             CONSTRAINT ck_application_registry_id CHECK (octet_length(application_id) BETWEEN 1 AND 255), \
             CONSTRAINT ck_application_registry_version CHECK (octet_length(version) BETWEEN 1 AND 255), \
             CONSTRAINT ck_application_registry_last_result CHECK (last_result IN ({LAST_RESULTS})), \
             CONSTRAINT ck_application_registry_seeded_at CHECK (last_seeded_at >= TIMESTAMPTZ '1970-01-01 00:00:00+00' AND last_seeded_at < TIMESTAMPTZ '10000-01-01 00:00:00+00'))"
        ),
        DbBackend::MySql => format!(
            "CREATE TABLE IF NOT EXISTS application_registry (\
             application_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL, \
             version VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL, \
             last_result VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL, \
             last_seeded_at DATETIME(6) NOT NULL, \
             audit_id LONGTEXT NOT NULL, \
             PRIMARY KEY (application_id), \
             CONSTRAINT ck_application_registry_id CHECK (octet_length(application_id) BETWEEN 1 AND 255), \
             CONSTRAINT ck_application_registry_version CHECK (octet_length(version) BETWEEN 1 AND 255), \
             CONSTRAINT ck_application_registry_last_result CHECK (last_result IN ({LAST_RESULTS})), \
             CONSTRAINT ck_application_registry_seeded_at CHECK (last_seeded_at >= '1970-01-01 00:00:00'))"
        ),
        _ => return Err(DbErr::Migration("unsupported database backend".to_owned())),
    })
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(&create_sql(manager.get_database_backend())?)
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP TABLE IF EXISTS application_registry")
            .await?;
        Ok(())
    }
}
