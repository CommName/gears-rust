//! Test databases for the registry tests (DESIGN §3.7, §4.8 "Integration" and "Security").
//!
//! One `RegistryTestDb` is a migrated database plus a second, raw connection to it for
//! schema introspection (a `SQLite` in-memory database cannot be opened twice, so the
//! `SQLite` lane uses a file in a temporary directory). `PgServer` and `MySqlServer` each
//! own one container (`libs/test-containers`, Docker) and hand out one fresh database per
//! case.
//!
//! The introspection helpers hold the only raw SQL of the test tree that asks the catalog;
//! the production tree has none outside migrations (DESIGN §2.2, `mod_tests`).
//!
//! Database tests run on the real clock: a paused clock cannot coexist with real DB IO.
//! Every case is bounded with `real_bounded`.

use std::any::Any;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};
use sea_orm_migration::MigratorTrait;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use toolkit_db::migration_runner::run_migrations_for_testing;
use toolkit_db::secure::DbConn;
use toolkit_db::{ConnectOpts, DBProvider, DbError, connect_db};

use crate::infra::storage::migrations::Migrator;
use crate::infra::storage::store::SeaRegistryStore;

/// Real-time bound of one database case.
const CASE_BOUND: Duration = Duration::from_mins(2);

/// The registry table name (DESIGN §3.7).
pub const REGISTRY_TABLE: &str = "application_registry";

/// A migrated database and a raw connection to the same database.
pub struct RegistryTestDb {
    /// The platform provider the repository and the store run on.
    pub provider: DBProvider<DbError>,
    /// A second connection, for catalog queries only.
    pub raw: DatabaseConnection,
    /// The backend of both connections.
    pub backend: DbBackend,
    /// Keeps the temporary directory alive.
    _keep: Box<dyn Any + Send + Sync>,
}

impl RegistryTestDb {
    /// A non-transactional runner.
    #[must_use]
    pub fn conn(&self) -> DbConn<'_> {
        self.provider
            .conn()
            .expect("a connection outside any transaction")
    }

    /// The port adapter over this database with the given operation timeout.
    #[must_use]
    pub fn store(&self, operation_timeout: Duration) -> SeaRegistryStore {
        SeaRegistryStore::new(self.provider.clone(), operation_timeout)
    }
}

/// Bounds a database case with a REAL-time limit.
pub async fn real_bounded<F: Future>(fut: F) -> F::Output {
    tokio::time::timeout(CASE_BOUND, fut)
        .await
        .expect("the database case exceeded its real-time bound")
}

/// A migrated `SQLite` file database with the default pool.
pub async fn sqlite() -> RegistryTestDb {
    sqlite_with(ConnectOpts::default(), true).await
}

/// A `SQLite` file database with the given pool options, migrated or not.
pub async fn sqlite_with(opts: ConnectOpts, migrate: bool) -> RegistryTestDb {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let dsn = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("registry.db").display()
    );
    // Create (and migrate) the file with the default, generous pool first: creating a WAL
    // database fsyncs, which can take longer than a 1 s acquire timeout on a loaded machine.
    // Reopening the same file with the caller's options is then fast, so the fixture never
    // fails under a short `acquire_timeout` that a test wants to exercise later.
    {
        let first = connect_db(&dsn, ConnectOpts::default())
            .await
            .expect("create the SQLite file");
        if migrate {
            run_migrations_for_testing(&first, Migrator::migrations())
                .await
                .expect("the registry migration applies on an empty database");
        }
    }
    let db = connect_with_retry(&dsn, opts).await;
    let raw = Database::connect(&dsn)
        .await
        .expect("raw SQLite connection");
    RegistryTestDb {
        provider: DBProvider::<DbError>::new(db),
        raw,
        backend: DbBackend::Sqlite,
        _keep: Box::new(dir),
    }
}

/// Opens the pool with the caller's options, retrying. `connect_db` opens the pool's first
/// connection within the pool's own `acquire_timeout`, so a test that wants a short one (a 300 ms
/// pool whose only connection it then holds) could fail here, under load, before it starts. The
/// file exists and is migrated, so a retry is cheap, and an open pool has its connection ready:
/// the timeout then measures only the wait the test provokes.
async fn connect_with_retry(dsn: &str, opts: ConnectOpts) -> toolkit_db::Db {
    let mut last = None;
    for _ in 0..200 {
        match connect_db(dsn, opts.clone()).await {
            Ok(db) => return db,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("connect SQLite: {last:?}");
}

/// One Postgres container for a whole lane run.
pub struct PgServer {
    port: u16,
    admin: DatabaseConnection,
    next: AtomicU32,
    _container: ContainerAsync<Postgres>,
}

impl PgServer {
    /// Starts the pinned Postgres image (retrying the container start, whose port binding
    /// is sometimes readable a beat late).
    pub async fn start() -> PgServer {
        let mut last = None;
        for _ in 0..3 {
            match test_containers::postgres().start().await {
                Ok(container) => {
                    let port = container
                        .get_host_port_ipv4(5432)
                        .await
                        .expect("the mapped Postgres port");
                    let admin = Database::connect(format!(
                        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
                    ))
                    .await
                    .expect("admin connection to the Postgres container");
                    return PgServer {
                        port,
                        admin,
                        next: AtomicU32::new(0),
                        _container: container,
                    };
                }
                Err(e) => last = Some(e),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("the Postgres container did not start: {last:?}");
    }

    /// A fresh, migrated database on this server.
    pub async fn fresh_database(&self) -> RegistryTestDb {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let name = format!("registry_{}_{n}", std::process::id());
        self.admin
            .execute_unprepared(&format!("CREATE DATABASE \"{name}\""))
            .await
            .expect("create a database for the case");
        let dsn = format!(
            "postgres://postgres:postgres@127.0.0.1:{}/{name}",
            self.port
        );
        let db = connect_db(&dsn, ConnectOpts::default())
            .await
            .expect("connect Postgres");
        run_migrations_for_testing(&db, Migrator::migrations())
            .await
            .expect("the registry migration applies on an empty Postgres database");
        let raw = Database::connect(&dsn)
            .await
            .expect("raw Postgres connection");
        RegistryTestDb {
            provider: DBProvider::<DbError>::new(db),
            raw,
            backend: DbBackend::Postgres,
            _keep: Box::new(()),
        }
    }
}

/// One `MySQL` container for a whole lane run (`root` without a password, as in
/// `libs/test-containers`).
pub struct MySqlServer {
    port: u16,
    admin: DatabaseConnection,
    next: AtomicU32,
    _container: ContainerAsync<Mysql>,
}

impl MySqlServer {
    /// Starts the pinned `MySQL` image (retrying the container start) and waits until the
    /// server accepts connections: the readiness log line appears a little before it does.
    pub async fn start() -> MySqlServer {
        let mut last = None;
        for _ in 0..3 {
            match test_containers::mysql().start().await {
                Ok(container) => {
                    let port = container
                        .get_host_port_ipv4(3306)
                        .await
                        .expect("the mapped MySQL port");
                    let dsn = format!("mysql://root@127.0.0.1:{port}/mysql");
                    let admin = connect_when_ready(&dsn).await;
                    return MySqlServer {
                        port,
                        admin,
                        next: AtomicU32::new(0),
                        _container: container,
                    };
                }
                Err(e) => last = Some(e),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!("the MySQL container did not start: {last:?}");
    }

    /// A fresh, migrated database on this server.
    pub async fn fresh_database(&self) -> RegistryTestDb {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let name = format!("registry_{}_{n}", std::process::id());
        self.admin
            .execute_unprepared(&format!("CREATE DATABASE `{name}`"))
            .await
            .expect("create a database for the case");
        let dsn = format!("mysql://root@127.0.0.1:{}/{name}", self.port);
        let db = connect_db(&dsn, ConnectOpts::default())
            .await
            .expect("connect MySQL");
        run_migrations_for_testing(&db, Migrator::migrations())
            .await
            .expect("the registry migration applies on an empty MySQL database");
        let raw = Database::connect(&dsn).await.expect("raw MySQL connection");
        RegistryTestDb {
            provider: DBProvider::<DbError>::new(db),
            raw,
            backend: DbBackend::MySql,
            _keep: Box::new(()),
        }
    }
}

async fn connect_when_ready(dsn: &str) -> DatabaseConnection {
    let deadline = std::time::Instant::now() + Duration::from_mins(2);
    loop {
        match Database::connect(dsn).await {
            Ok(connection) => return connection,
            Err(e) if std::time::Instant::now() < deadline => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("the MySQL server never accepted a connection: {e}"),
        }
    }
}

/// Holds, on a second connection, the lock that blocks a write to `application_id`: a
/// `SELECT .. FOR UPDATE` on that row (Postgres, `MySQL`) or an open write transaction
/// (`SQLite`, which locks the whole database). The returned transaction holds the lock until
/// it is committed, rolled back or dropped.
pub async fn hold_write_lock(
    db: &RegistryTestDb,
    application_id: &str,
) -> sea_orm::DatabaseTransaction {
    use sea_orm::TransactionTrait;
    let tx = db
        .raw
        .begin()
        .await
        .expect("begin the blocking transaction");
    let sql = match db.backend {
        // A write statement takes SQLite's write lock at its start, even when no row matches.
        DbBackend::Sqlite => format!("DELETE FROM {REGISTRY_TABLE} WHERE 0 = 1"),
        DbBackend::Postgres | DbBackend::MySql => format!(
            "SELECT application_id FROM {REGISTRY_TABLE} WHERE application_id = '{application_id}' FOR UPDATE"
        ),
        other => unreachable!("unsupported backend {other:?}"),
    };
    tx.execute_unprepared(&sql).await.expect("take the lock");
    tx
}

/// One column of a table, as the catalog reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Whether the column is declared NOT NULL.
    pub not_null: bool,
}

/// One statement per backend, for the catalog helpers below.
struct PerBackend {
    sqlite: String,
    pg: String,
    mysql: String,
}

/// Runs a catalog query on the raw connection and returns its rows.
pub async fn query_rows(db: &RegistryTestDb, sql: &str) -> Vec<sea_orm::QueryResult> {
    db.raw
        .query_all_raw(Statement::from_string(db.backend, sql.to_owned()))
        .await
        .expect("catalog query")
}

async fn rows(db: &RegistryTestDb, sql: &PerBackend) -> Vec<sea_orm::QueryResult> {
    let sql = match db.backend {
        DbBackend::Sqlite => &sql.sqlite,
        DbBackend::Postgres => &sql.pg,
        DbBackend::MySql => &sql.mysql,
        other => unreachable!("unsupported backend {other:?}"),
    };
    query_rows(db, sql).await
}

async fn scalar(db: &RegistryTestDb, sql: &PerBackend) -> i64 {
    rows(db, sql)
        .await
        .first()
        .expect("one row")
        .try_get::<i64>("", "n")
        .expect("column n")
}

/// Columns of `table` in declaration order.
pub async fn columns(db: &RegistryTestDb, table: &str) -> Vec<ColumnInfo> {
    let sql = PerBackend {
        sqlite: format!(
            "SELECT name AS name, \"notnull\" AS not_null FROM pragma_table_info('{table}') ORDER BY cid"
        ),
        pg: format!(
            "SELECT column_name::text AS name, \
             (CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END)::bigint AS not_null \
             FROM information_schema.columns \
             WHERE table_schema = current_schema() AND table_name = '{table}' \
             ORDER BY ordinal_position"
        ),
        mysql: format!(
            "SELECT CAST(column_name AS CHAR) AS name, \
             CAST(CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS SIGNED) AS not_null \
             FROM information_schema.columns \
             WHERE table_schema = DATABASE() AND table_name = '{table}' \
             ORDER BY ordinal_position"
        ),
    };
    rows(db, &sql)
        .await
        .iter()
        .map(|row| ColumnInfo {
            name: row.try_get::<String>("", "name").expect("name"),
            not_null: row.try_get::<i64>("", "not_null").expect("not_null") != 0,
        })
        .collect()
}

/// Primary-key columns of `table`.
pub async fn primary_key(db: &RegistryTestDb, table: &str) -> Vec<String> {
    let sql = PerBackend {
        sqlite: format!(
            "SELECT name AS name FROM pragma_table_info('{table}') WHERE pk > 0 ORDER BY pk"
        ),
        pg: format!(
            "SELECT a.attname::text AS name FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = '{table}'::regclass AND i.indisprimary"
        ),
        mysql: format!(
            "SELECT CAST(column_name AS CHAR) AS name FROM information_schema.key_column_usage \
             WHERE table_schema = DATABASE() AND table_name = '{table}' \
             AND constraint_name = 'PRIMARY' ORDER BY ordinal_position"
        ),
    };
    rows(db, &sql)
        .await
        .iter()
        .map(|row| row.try_get::<String>("", "name").expect("name"))
        .collect()
}

/// Number of indexes of `table` other than the primary key.
pub async fn secondary_index_count(db: &RegistryTestDb, table: &str) -> i64 {
    scalar(
        db,
        &PerBackend {
            sqlite: format!(
                "SELECT count(*) AS n FROM pragma_index_list('{table}') WHERE origin <> 'pk'"
            ),
            pg: format!(
                "SELECT count(*)::bigint AS n FROM pg_index \
                 WHERE indrelid = '{table}'::regclass AND NOT indisprimary"
            ),
            mysql: format!(
                "SELECT count(DISTINCT index_name) AS n FROM information_schema.statistics \
                 WHERE table_schema = DATABASE() AND table_name = '{table}' \
                 AND index_name <> 'PRIMARY'"
            ),
        },
    )
    .await
}

/// Number of foreign keys declared on `table`.
pub async fn foreign_key_count(db: &RegistryTestDb, table: &str) -> i64 {
    scalar(
        db,
        &PerBackend {
            sqlite: format!("SELECT count(*) AS n FROM pragma_foreign_key_list('{table}')"),
            pg: format!(
                "SELECT count(*)::bigint AS n FROM pg_constraint \
                 WHERE conrelid = '{table}'::regclass AND contype = 'f'"
            ),
            mysql: format!(
                "SELECT count(*) AS n FROM information_schema.table_constraints \
                 WHERE table_schema = DATABASE() AND table_name = '{table}' \
                 AND constraint_type = 'FOREIGN KEY'"
            ),
        },
    )
    .await
}

/// Number of user triggers on `table`.
pub async fn trigger_count(db: &RegistryTestDb, table: &str) -> i64 {
    scalar(
        db,
        &PerBackend {
            sqlite: format!(
                "SELECT count(*) AS n FROM sqlite_master WHERE type = 'trigger' AND tbl_name = '{table}'"
            ),
            pg: format!(
                "SELECT count(*)::bigint AS n FROM pg_trigger \
                 WHERE tgrelid = '{table}'::regclass AND NOT tgisinternal"
            ),
            mysql: format!(
                "SELECT count(*) AS n FROM information_schema.triggers \
                 WHERE event_object_schema = DATABASE() AND event_object_table = '{table}'"
            ),
        },
    )
    .await
}

/// Whether the CHECK constraint `name` exists on `table`.
pub async fn check_constraint_exists(db: &RegistryTestDb, table: &str, name: &str) -> bool {
    scalar(
        db,
        &PerBackend {
            // SQLite keeps the constraint text in the table definition.
            sqlite: format!(
                "SELECT count(*) AS n FROM sqlite_master \
                 WHERE type = 'table' AND name = '{table}' AND sql LIKE '%CONSTRAINT {name} CHECK%'"
            ),
            pg: format!(
                "SELECT count(*)::bigint AS n FROM information_schema.table_constraints \
                 WHERE table_schema = current_schema() AND table_name = '{table}' \
                 AND constraint_name = '{name}' AND constraint_type = 'CHECK'"
            ),
            mysql: format!(
                "SELECT count(*) AS n FROM information_schema.table_constraints \
                 WHERE table_schema = DATABASE() AND table_name = '{table}' \
                 AND constraint_name = '{name}' AND constraint_type = 'CHECK'"
            ),
        },
    )
    .await
        == 1
}

/// Whether `table` exists.
pub async fn table_exists(db: &RegistryTestDb, table: &str) -> bool {
    scalar(
        db,
        &PerBackend {
            sqlite: format!(
                "SELECT count(*) AS n FROM sqlite_master WHERE type = 'table' AND name = '{table}'"
            ),
            pg: format!(
                "SELECT count(*)::bigint AS n FROM information_schema.tables \
                 WHERE table_schema = current_schema() AND table_name = '{table}'"
            ),
            mysql: format!(
                "SELECT count(*) AS n FROM information_schema.tables \
                 WHERE table_schema = DATABASE() AND table_name = '{table}'"
            ),
        },
    )
    .await
        == 1
}

/// Number of columns in the database whose name contains `needle`.
pub async fn columns_named_like(db: &RegistryTestDb, needle: &str) -> i64 {
    scalar(
        db,
        &PerBackend {
            sqlite: format!(
                "SELECT count(*) AS n FROM sqlite_master m, pragma_table_info(m.name) c \
                 WHERE m.type = 'table' AND c.name LIKE '%{needle}%'"
            ),
            pg: format!(
                "SELECT count(*)::bigint AS n FROM information_schema.columns \
                 WHERE table_schema = current_schema() AND column_name LIKE '%{needle}%'"
            ),
            mysql: format!(
                "SELECT count(*) AS n FROM information_schema.columns \
                 WHERE table_schema = DATABASE() AND column_name LIKE '%{needle}%'"
            ),
        },
    )
    .await
}
