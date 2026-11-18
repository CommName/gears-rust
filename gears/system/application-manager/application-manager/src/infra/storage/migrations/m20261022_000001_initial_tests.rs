//! Statement-list tests: what a container run cannot tell cheaply and what
//! silently rots otherwise. PostgreSQL and MySQL run for real in
//! `tests/storage_backends_test.rs`; SQLite runs for real in `repo_tests.rs`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::{DOWN_STATEMENTS, MYSQL_UP_STATEMENTS, PG_UP_STATEMENTS, SQLITE_UP_STATEMENTS};

/// PostgreSQL identifiers are cut at 63 bytes; the others allow more, so 63
/// is the portable limit.
const IDENTIFIER_MAX_BYTES: usize = 63;

const OPEN_INDEX: &str = "ux_operations_open_application";
const STATUS_DEADLINE_INDEX: &str = "idx_operations_status_deadline";
const FINISHED_AT_INDEX: &str = "idx_operations_finished_at";
const CORRELATION_INDEX: &str = "idx_operation_history_correlation_id";

fn lists() -> [(&'static str, &'static [&'static str]); 3] {
    [
        ("postgres", PG_UP_STATEMENTS),
        ("sqlite", SQLITE_UP_STATEMENTS),
        ("mysql", MYSQL_UP_STATEMENTS),
    ]
}

/// Whitespace-insensitive form, so a check does not depend on line breaks or
/// on the space before a parenthesis.
fn norm(sql: &str) -> String {
    let mut text = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    for (from, to) in [(" (", "("), ("( ", "("), (" )", ")"), (", ", ",")] {
        while text.contains(from) {
            text = text.replace(from, to);
        }
    }
    text
}

fn joined(statements: &[&str]) -> String {
    norm(&statements.join("\n"))
}

/// The statement that creates `table`.
fn create_table(statements: &[&'static str], table: &str) -> &'static str {
    let head = norm(&format!("CREATE TABLE IF NOT EXISTS {table} ("));
    statements
        .iter()
        .copied()
        .find(|s| norm(s).starts_with(&head))
        .unwrap_or_else(|| panic!("no statement creates {table}"))
}

#[test]
fn every_backend_creates_the_three_tables() {
    for (name, statements) in lists() {
        let sql = joined(statements);
        for table in ["operations", "operation_history", "applications"] {
            let create = norm(&format!("CREATE TABLE IF NOT EXISTS {table} ("));
            assert!(sql.contains(&create), "{name} does not create {table}");
        }
    }
}

#[test]
fn every_table_has_a_primary_key_on_its_id() {
    for (name, statements) in lists() {
        for (table, id) in [
            ("operations", "operation_id"),
            ("operation_history", "event_id"),
        ] {
            let create = norm(create_table(statements, table));
            assert!(create.contains(id), "{name} {table}: no {id}");
            assert!(
                create.contains("PRIMARY KEY"),
                "{name} {table}: no primary key"
            );
        }
    }
}

#[test]
fn every_backend_has_the_three_secondary_indexes() {
    let pg_like = [
        format!(
            "CREATE INDEX IF NOT EXISTS {STATUS_DEADLINE_INDEX} ON operations(status,run_deadline)"
        ),
        format!("CREATE INDEX IF NOT EXISTS {FINISHED_AT_INDEX} ON operations(finished_at)"),
        format!(
            "CREATE INDEX IF NOT EXISTS {CORRELATION_INDEX} ON operation_history(correlation_id)"
        ),
    ];
    for (name, statements) in [
        ("postgres", PG_UP_STATEMENTS),
        ("sqlite", SQLITE_UP_STATEMENTS),
    ] {
        let sql = joined(statements);
        for want in &pg_like {
            assert!(sql.contains(&norm(want)), "{name}: missing `{want}`");
        }
    }
    let operations = norm(create_table(MYSQL_UP_STATEMENTS, "operations"));
    let history = norm(create_table(MYSQL_UP_STATEMENTS, "operation_history"));
    for want in [
        format!("KEY {STATUS_DEADLINE_INDEX}(status,run_deadline)"),
        format!("KEY {FINISHED_AT_INDEX}(finished_at)"),
    ] {
        assert!(
            operations.contains(&norm(&want)),
            "mysql operations: missing `{want}`"
        );
    }
    let want = format!("KEY {CORRELATION_INDEX}(correlation_id)");
    assert!(
        history.contains(&norm(&want)),
        "mysql operation_history: missing `{want}`"
    );
}

#[test]
fn postgres_and_sqlite_open_index_is_a_partial_unique_index() {
    let want = norm(&format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {OPEN_INDEX} ON operations(application_id) \
         WHERE status IN ('accepted','running')"
    ));
    for (name, statements) in [
        ("postgres", PG_UP_STATEMENTS),
        ("sqlite", SQLITE_UP_STATEMENTS),
    ] {
        assert!(
            joined(statements).contains(&want),
            "{name}: missing the partial unique index"
        );
    }
}

#[test]
fn mysql_open_index_is_a_generated_column_with_a_unique_key() {
    let operations = norm(create_table(MYSQL_UP_STATEMENTS, "operations"));
    let column = norm(
        "open_application_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin \
         GENERATED ALWAYS AS (CASE WHEN status IN ('accepted','running') THEN application_id END) STORED",
    );
    assert!(
        operations.contains(&column),
        "generated column missing or different: {operations}"
    );
    let unique = norm(&format!("UNIQUE KEY {OPEN_INDEX}(open_application_id)"));
    assert!(
        operations.contains(&unique),
        "unique key missing: {operations}"
    );
    assert!(
        !joined(MYSQL_UP_STATEMENTS).contains("WHERE status IN"),
        "MySQL has no partial indexes"
    );
}

#[test]
fn mysql_declares_its_indexes_inline() {
    for statement in MYSQL_UP_STATEMENTS {
        let text = norm(statement).to_ascii_uppercase();
        assert!(
            !text.contains("CREATE INDEX") && !text.contains("CREATE UNIQUE INDEX"),
            "MySQL has no CREATE INDEX IF NOT EXISTS: {statement}"
        );
    }
    assert!(!MYSQL_UP_STATEMENTS.is_empty());
}

#[test]
fn application_id_is_binary_no_pad_on_every_backend() {
    let want = [
        (
            "postgres",
            PG_UP_STATEMENTS,
            "application_id VARCHAR(255) COLLATE \"C\"",
        ),
        (
            "sqlite",
            SQLITE_UP_STATEMENTS,
            "application_id TEXT COLLATE BINARY",
        ),
        (
            "mysql",
            MYSQL_UP_STATEMENTS,
            "application_id VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin",
        ),
    ];
    for (name, statements, column) in want {
        let operations = norm(create_table(statements, "operations"));
        assert!(operations.contains(&norm(column)), "{name}: {operations}");
    }
}

#[test]
fn enum_token_columns_use_the_same_binary_collation_as_the_application_id() {
    let collations = [
        (
            "postgres",
            PG_UP_STATEMENTS,
            "COLLATE \"C\"",
            ["VARCHAR(16)", "VARCHAR(32)"],
        ),
        (
            "sqlite",
            SQLITE_UP_STATEMENTS,
            "COLLATE BINARY",
            ["TEXT", "TEXT"],
        ),
        (
            "mysql",
            MYSQL_UP_STATEMENTS,
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin",
            ["VARCHAR(16)", "VARCHAR(32)"],
        ),
    ];
    for (name, statements, collation, [short, long]) in collations {
        let operations = norm(create_table(statements, "operations"));
        let history = norm(create_table(statements, "operation_history"));
        for (table, column, ty) in [
            (&operations, "action", short),
            (&operations, "status", short),
            (&operations, "category", long),
            (&history, "kind", short),
            (&history, "action", short),
        ] {
            let want = norm(&format!("{column} {ty} {collation}"));
            assert!(table.contains(&want), "{name}: missing `{want}`");
        }
    }
}

#[test]
fn uuid_timestamp_and_json_columns_use_the_d5_mapping() {
    let want = [
        (
            "postgres",
            PG_UP_STATEMENTS,
            [
                "operation_id UUID",
                "finished_event_id UUID NOT NULL",
                "run_deadline TIMESTAMPTZ",
                "report JSONB",
            ],
            [
                "event_id UUID",
                "correlation_id UUID",
                "written_at TIMESTAMPTZ",
                "content JSONB",
            ],
        ),
        (
            "sqlite",
            SQLITE_UP_STATEMENTS,
            [
                "operation_id BLOB",
                "finished_event_id BLOB NOT NULL",
                "run_deadline TEXT",
                "report TEXT",
            ],
            [
                "event_id BLOB",
                "correlation_id BLOB",
                "written_at TEXT",
                "content TEXT",
            ],
        ),
        (
            "mysql",
            MYSQL_UP_STATEMENTS,
            [
                "operation_id BINARY(16)",
                "finished_event_id BINARY(16) NOT NULL",
                "run_deadline DATETIME(6)",
                "report JSON",
            ],
            [
                "event_id BINARY(16)",
                "correlation_id BINARY(16)",
                "written_at DATETIME(6)",
                "content JSON",
            ],
        ),
    ];
    for (name, statements, operations_columns, history_columns) in want {
        let operations = norm(create_table(statements, "operations"));
        for column in operations_columns {
            assert!(
                operations.contains(&norm(column)),
                "{name} operations: `{column}`"
            );
        }
        let history = norm(create_table(statements, "operation_history"));
        for column in history_columns {
            assert!(
                history.contains(&norm(column)),
                "{name} operation_history: `{column}`"
            );
        }
    }
}

#[test]
fn sqlite_uuid_columns_check_for_16_bytes() {
    let operations = norm(create_table(SQLITE_UP_STATEMENTS, "operations"));
    let history = norm(create_table(SQLITE_UP_STATEMENTS, "operation_history"));
    for (table, column) in [
        (&operations, "operation_id"),
        (&operations, "finished_event_id"),
        (&history, "event_id"),
        (&history, "correlation_id"),
    ] {
        assert!(
            table.contains(&norm(&format!("length({column}) = 16"))),
            "{column}"
        );
    }
}

#[test]
fn enum_columns_are_text_with_check_constraints() {
    for (name, statements) in lists() {
        let operations = norm(create_table(statements, "operations"));
        let history = norm(create_table(statements, "operation_history"));
        let applications = norm(create_table(statements, "applications"));
        for (table, check) in [
            (&operations, "action IN ('seed','uninstall')"),
            (&operations, "status IN ('accepted','running','completed')"),
            (
                &operations,
                "category IS NULL OR category IN ('succeeded','invalid_manifest','plugin_rejected','not_permitted','internal_failure')",
            ),
            (
                &applications,
                "category IN ('succeeded','invalid_manifest','plugin_rejected','not_permitted','internal_failure')",
            ),
            (&history, "action IN ('seed','uninstall')"),
            (&history, "kind IN ('accepted','finished','refused')"),
        ] {
            assert!(
                table.contains(&norm(&format!("CHECK({check})"))),
                "{name}: CHECK({check})"
            );
        }
    }
}

#[test]
fn nullable_columns_are_exactly_the_design_ones() {
    for (name, statements) in lists() {
        let operations = norm(create_table(statements, "operations"));
        for column in [
            "application_id",
            "action",
            "status",
            "report",
            "accepted_at",
            "run_deadline",
            "finished_event_id",
        ] {
            let nullable = operations.split(',').any(|part| {
                part.trim_start_matches('(')
                    .starts_with(&format!("{column} "))
                    && !part.contains("NOT NULL")
            });
            assert!(!nullable, "{name}: operations.{column} must be NOT NULL");
        }
    }
}

#[test]
fn history_and_applications_have_no_foreign_key_to_operations() {
    // A purged operation must not take its events or its application record
    // with it.
    for (name, statements) in lists() {
        for table in ["operation_history", "applications"] {
            let sql = norm(create_table(statements, table)).to_ascii_uppercase();
            assert!(!sql.contains("REFERENCES"), "{name} {table}");
            assert!(!sql.contains("FOREIGN KEY"), "{name} {table}");
        }
    }
}

#[test]
fn application_ids_and_versions_are_limited_in_bytes() {
    for (name, statements, bytes) in [
        ("postgres", PG_UP_STATEMENTS, "octet_length"),
        ("mysql", MYSQL_UP_STATEMENTS, "octet_length"),
        ("sqlite", SQLITE_UP_STATEMENTS, "length(CAST"),
    ] {
        let operations = norm(create_table(statements, "operations"));
        let applications = norm(create_table(statements, "applications"));
        for (table, column) in [
            (&operations, "application_id"),
            (&applications, "application_id"),
            (&applications, "version"),
        ] {
            let check = norm(&format!("{bytes}({column}"));
            assert!(table.contains(&check), "{name}: {column} byte limit");
        }
        assert!(operations.contains("<= 255"), "{name}");
    }
}

#[test]
fn applications_columns_are_the_design_ones_and_not_null() {
    for (name, statements) in lists() {
        let applications = norm(create_table(statements, "applications"));
        for column in [
            "application_id",
            "version",
            "operation_id",
            "category",
            "finished_at",
        ] {
            // The column's declaration runs from its name to the next comma.
            let start = ["(", ","]
                .iter()
                .filter_map(|lead| applications.find(&format!("{lead}{column} ")))
                .min()
                .unwrap_or_else(|| panic!("{name}: applications.{column}"));
            let rest = &applications[start + 1..];
            let declared = &rest[..rest.find(',').unwrap_or(rest.len())];
            assert!(
                declared.contains("NOT NULL"),
                "{name}: applications.{column}"
            );
        }
    }
}

#[test]
fn identifiers_fit_63_bytes() {
    for (name, statements) in lists().into_iter().chain([("down", DOWN_STATEMENTS)]) {
        for statement in statements {
            for word in statement.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) {
                assert!(
                    word.len() <= IDENTIFIER_MAX_BYTES,
                    "{name}: `{word}` is {} bytes",
                    word.len()
                );
            }
        }
    }
    // The named objects exist and fit.
    for identifier in [
        OPEN_INDEX,
        STATUS_DEADLINE_INDEX,
        FINISHED_AT_INDEX,
        CORRELATION_INDEX,
    ] {
        assert!(identifier.len() <= IDENTIFIER_MAX_BYTES);
    }
}

#[test]
fn indexes_follow_their_tables() {
    for (name, statements) in [
        ("postgres", PG_UP_STATEMENTS),
        ("sqlite", SQLITE_UP_STATEMENTS),
    ] {
        let position = |needle: &str| {
            statements
                .iter()
                .position(|s| norm(s).contains(&norm(needle)))
                .unwrap_or_else(|| panic!("{name}: no statement with `{needle}`"))
        };
        assert!(
            position("CREATE TABLE IF NOT EXISTS operations(")
                < position(&format!("INDEX IF NOT EXISTS {OPEN_INDEX}"))
        );
        assert!(
            position("CREATE TABLE IF NOT EXISTS operation_history(")
                < position(&format!("INDEX IF NOT EXISTS {CORRELATION_INDEX}"))
        );
    }
}

#[test]
fn down_drops_the_three_tables() {
    let sql = joined(DOWN_STATEMENTS);
    assert!(sql.contains("DROP TABLE IF EXISTS operations"), "{sql}");
    assert!(sql.contains("DROP TABLE IF EXISTS applications"), "{sql}");
    assert!(
        sql.contains("DROP TABLE IF EXISTS operation_history"),
        "{sql}"
    );
}

#[test]
fn migration_is_registered_once() {
    use sea_orm_migration::MigratorTrait;
    let names: Vec<String> = super::super::Migrator::migrations()
        .iter()
        .map(|m| m.name().to_owned())
        .collect();
    assert_eq!(names, ["m20261022_000001_initial"]);
}
