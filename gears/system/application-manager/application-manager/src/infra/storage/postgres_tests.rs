//! Postgres lane of the registry tests (DESIGN §4.8 "Integration": "`SQLite` always;
//! `PostgreSQL` ... in the integration lane, through `libs/test-containers`").
//!
//! Needs Docker. Every test here is `#[ignore]`d, as in `bss-pricing` and `bss-coord`, so
//! `cargo test` and `make test` compile it and never start a container. Run the lane with
//!
//! ```text
//! make test-application-manager-pg
//! # = cargo nextest run -p cf-gears-application-manager --run-ignored ignored-only \
//! #     -E 'test(/postgres_tests::/)' --no-fail-fast
//! # or, without nextest:
//! # cargo test -p cf-gears-application-manager --lib -- --ignored postgres_tests
//! ```
//!
//! The image comes from `libs/test-containers` (`GEARS_TEST_PG_TAG` overrides the tag).
//!
//! - `postgres_lane_runs_every_case_and_the_catalog_checks`: starts ONE container, runs every
//!   shared case of `registry_cases::all()` (the same bodies as the `SQLite` lane) on its own
//!   fresh database, each in its own task so a failing case is named and the rest still run
//!   (a case that hangs is aborted), then the catalog checks on one more database of the
//!   same container:
//!   - `timestamp with time zone`, a text type for the bounded columns, the `C` collation on
//!     `application_id` and `version` (§3.7);
//!   - the table is created in the connection's schema, with no tenant column (ADR 0007).

use std::time::Duration;

use sea_orm::DbBackend;

use crate::test_support::registry_cases;
use crate::test_support::registry_db::{self, PgServer, REGISTRY_TABLE};

/// Real-time bound of one case on the container.
const CASE_BOUND: Duration = Duration::from_mins(3);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker (testcontainers)"]
async fn postgres_lane_runs_every_case_and_the_catalog_checks() {
    let server = PgServer::start().await;

    let failures = registry_cases::run_lane(CASE_BOUND, || server.fresh_database()).await;
    assert!(
        failures.is_empty(),
        "cases failed on Postgres:\n{}",
        failures.join("\n")
    );

    let db = server.fresh_database().await;
    assert_eq!(db.backend, DbBackend::Postgres);
    columns_have_the_design_types(&db).await;
    table_lives_in_the_current_schema(&db).await;
    assert!(
        registry_db::check_constraint_exists(
            &db,
            REGISTRY_TABLE,
            "ck_application_registry_seeded_at"
        )
        .await,
        "seeded_at range CHECK"
    );
}

async fn columns_have_the_design_types(db: &registry_db::RegistryTestDb) {
    let types = registry_db::query_rows(
        db,
        &format!(
            "SELECT column_name::text AS name, data_type::text AS data_type \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() AND table_name = '{REGISTRY_TABLE}'"
        ),
    )
    .await;
    let type_of = |column: &str| -> String {
        types
            .iter()
            .find(|row| row.try_get::<String>("", "name").ok().as_deref() == Some(column))
            .unwrap_or_else(|| panic!("column {column} is missing"))
            .try_get::<String>("", "data_type")
            .expect("data_type")
    };
    assert_eq!(type_of("last_seeded_at"), "timestamp with time zone");
    for text in ["application_id", "version", "last_result", "audit_id"] {
        let t = type_of(text);
        assert!(t == "character varying" || t == "text", "{text} is {t}");
    }
    assert_eq!(
        type_of("audit_id"),
        "text",
        "the audit id has no length bound"
    );

    let collations = registry_db::query_rows(
        db,
        &format!(
            "SELECT a.attname::text AS name, c.collname::text AS collation \
             FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation \
             WHERE a.attrelid = '{REGISTRY_TABLE}'::regclass \
             AND a.attname IN ('application_id', 'version')"
        ),
    )
    .await;
    assert_eq!(collations.len(), 2);
    for row in collations {
        assert_eq!(
            row.try_get::<String>("", "collation").expect("collation"),
            "C",
            "{}: binary no-pad comparison on PostgreSQL (DESIGN section 3.7)",
            row.try_get::<String>("", "name").expect("name")
        );
    }
}

async fn table_lives_in_the_current_schema(db: &registry_db::RegistryTestDb) {
    assert!(registry_db::table_exists(db, REGISTRY_TABLE).await);
    assert_eq!(registry_db::columns_named_like(db, "tenant").await, 0);
    let names: Vec<String> = registry_db::columns(db, REGISTRY_TABLE)
        .await
        .into_iter()
        .map(|c| c.name)
        .collect();
    assert_eq!(names.len(), 5, "{names:?}");
}
