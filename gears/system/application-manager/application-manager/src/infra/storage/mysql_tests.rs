//! `MySQL` lane of the registry tests (DESIGN §4.8 "Integration": "`SQLite` always;
//! `PostgreSQL` and `MySQL` ... in the integration lane, through `libs/test-containers`").
//!
//! Needs Docker. Every test here is `#[ignore]`d, like the Postgres lane, so `cargo test`
//! and `make test` compile it and never start a container. Run the lane with
//!
//! ```text
//! make test-application-manager-mysql
//! # = cargo nextest run -p cf-gears-application-manager --run-ignored ignored-only \
//! #     -E 'test(/mysql_tests::/)' --no-fail-fast
//! # or, without nextest:
//! # cargo test -p cf-gears-application-manager --lib -- --ignored mysql_tests
//! ```
//!
//! The image comes from `libs/test-containers` (`GEARS_TEST_MYSQL_TAG` overrides the tag).
//!
//! - `mysql_lane_runs_every_case_and_the_catalog_checks`: starts ONE container, runs every
//!   shared case of `registry_cases::all()` (the same bodies as the `SQLite` lane) on its own
//!   fresh database, each in its own task (a case that hangs is aborted), then the catalog
//!   checks on one more database of the same container:
//!   - `application_id`, `version` and `last_result` use the binary, no-pad collation `utf8mb4_0900_bin`
//!     (§3.7: `utf8mb4_bin` would be PAD SPACE and equate `"app"` with `"app "`);
//!   - `last_seeded_at` is `DATETIME(6)` (a `TIMESTAMP(6)` ends in 2038), `audit_id` is
//!     `LONGTEXT` (a `TEXT` caps at 64 KB);
//!   - the id and version bounds are CHECK constraints over the octet length (a `VARCHAR(255)`
//!     alone counts characters);
//!   - no tenant column (ADR 0007).

use std::time::Duration;

use sea_orm::DbBackend;

use crate::test_support::registry_cases;
use crate::test_support::registry_db::{self, MySqlServer, REGISTRY_TABLE, RegistryTestDb};

/// Real-time bound of one case on the container.
const CASE_BOUND: Duration = Duration::from_mins(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker (testcontainers)"]
async fn mysql_lane_runs_every_case_and_the_catalog_checks() {
    let server = MySqlServer::start().await;

    let failures = registry_cases::run_lane(CASE_BOUND, || server.fresh_database()).await;
    assert!(
        failures.is_empty(),
        "cases failed on MySQL:\n{}",
        failures.join("\n")
    );

    let db = server.fresh_database().await;
    assert_eq!(db.backend, DbBackend::MySql);
    columns_have_the_design_types_and_collation(&db).await;
    the_bounds_are_byte_length_checks(&db).await;
    assert!(
        registry_db::check_constraint_exists(
            &db,
            REGISTRY_TABLE,
            "ck_application_registry_seeded_at"
        )
        .await,
        "seeded_at range CHECK"
    );
    assert_eq!(registry_db::columns_named_like(&db, "tenant").await, 0);
}

async fn columns_have_the_design_types_and_collation(db: &RegistryTestDb) {
    let rows = registry_db::query_rows(
        db,
        &format!(
            "SELECT CAST(column_name AS CHAR) AS name, CAST(column_type AS CHAR) AS column_type, \
             CAST(collation_name AS CHAR) AS collation \
             FROM information_schema.columns \
             WHERE table_schema = DATABASE() AND table_name = '{REGISTRY_TABLE}'"
        ),
    )
    .await;
    let column = |name: &str, field: &str| -> Option<String> {
        rows.iter()
            .find(|row| row.try_get::<String>("", "name").ok().as_deref() == Some(name))
            .unwrap_or_else(|| panic!("column {name} is missing"))
            .try_get::<Option<String>>("", field)
            .expect("catalog field")
    };

    assert_eq!(
        column("last_seeded_at", "column_type").as_deref(),
        Some("datetime(6)")
    );
    assert_eq!(
        column("audit_id", "column_type").as_deref(),
        Some("longtext")
    );
    // `last_result` too: under the default case-insensitive collation `'Succeeded'` would
    // pass the CHECK that lists `'succeeded'`.
    for text in ["application_id", "version", "last_result"] {
        assert_eq!(
            column(text, "collation").as_deref(),
            Some("utf8mb4_0900_bin"),
            "{text}: binary no-pad comparison on MySQL (DESIGN section 3.7)"
        );
    }
}

async fn the_bounds_are_byte_length_checks(db: &RegistryTestDb) {
    let rows = registry_db::query_rows(
        db,
        &format!(
            "SELECT CAST(tc.constraint_name AS CHAR) AS name, CAST(cc.check_clause AS CHAR) AS clause \
             FROM information_schema.table_constraints tc \
             JOIN information_schema.check_constraints cc \
               ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name \
             WHERE tc.table_schema = DATABASE() AND tc.table_name = '{REGISTRY_TABLE}' \
             AND tc.constraint_type = 'CHECK'"
        ),
    )
    .await;
    let clauses: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                row.try_get::<String>("", "name").expect("name"),
                row.try_get::<String>("", "clause")
                    .expect("clause")
                    .to_lowercase(),
            )
        })
        .collect();

    assert_eq!(
        clauses.len(),
        4,
        "id, version, last_result and seeded_at checks: {clauses:?}"
    );
    // MySQL stores `OCTET_LENGTH(x)` as its synonym `length(x)`, which counts bytes; the
    // character counting functions are the ones that must not appear.
    for column in ["application_id", "version"] {
        assert!(
            clauses.iter().any(|(_, clause)| {
                clause.contains(&format!("length(`{column}`)"))
                    && !clause.contains("char_length")
                    && !clause.contains("character_length")
            }),
            "{column} has a byte-length CHECK: {clauses:?}"
        );
    }
}
