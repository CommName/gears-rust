//! Operation repository over `SeaORM`: every step and transaction of DESIGN
//! 3.6, each bounded by `storage_call_timeout`.
//!
//! # Security boundary
//!
//! Both tables are internal bookkeeping with no tenant column (ADR 0005), so
//! their entities are `#[secure(unrestricted)]` and every statement goes
//! through `secure()` with `AccessScope::allow_all()`. Raw SQL appears only in
//! the migration.
//!
//! # Status guard (D9)
//!
//! A write to an operation matches only a record whose status is `accepted`
//! or `running` and whose `run_deadline` is after the call's `now`. The
//! interruption step matches the opposite deadline. A guarded update that
//! changes no row is [`StorageError::OperationFinal`]: the first final write
//! wins, and a transaction that fails its guard has written nothing.
//!
//! # Transactions and contention
//!
//! Every multi-statement transaction runs through `toolkit-db`'s retry
//! (`Db::transaction_with_retry_max`): a deadlock, a serialisation failure or a
//! `SQLite` `BUSY` restarts the transaction in a fresh one, up to
//! [`TX_ATTEMPTS`] attempts. The retry sits inside [`SeaOrmOperationRepository::bounded`],
//! so `storage_call_timeout` bounds the total. A step outcome
//! ([`StorageError::IndexViolated`], [`StorageError::OperationFinal`]) is never
//! a database error and is never retried.
//!
//! # Errors
//!
//! A timeout, a connection error and any other database error are
//! [`StorageError::Unavailable`]. A log line carries a fixed-vocabulary
//! `class` and the repository method as `call`, never database error text: a
//! driver message can echo the application id.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::{OutcomeReport, RequestCategory};
use async_trait::async_trait;
use sea_orm::sea_query::{Expr, Func, InsertStatement, SimpleExpr};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnAcquireErr, DbBackend, DbErr,
    EntityName, EntityTrait, Insert, Iterable, Order, QueryFilter, QueryTrait, SqlErr, Value,
};
use time::OffsetDateTime;
use toolkit_db::secure::{
    DBRunner, SecureDeleteExt, SecureEntityExt, SecureInsertExt, SecureUpdateExt,
};
use toolkit_db::secure::{ScopeError, TxConfig};
use toolkit_db::{DBProvider, DbError, DbTx, contention::is_retryable_contention};
use toolkit_security::AccessScope;
use uuid::Uuid;

use super::entity::{audit_event, operation};
use super::mapping::{
    RowError, enum_token, event_from_row, event_row, operation_from_row, operation_row,
    report_json, status_token, stored_time, stored_value,
};
use crate::domain::audit::{self, AuditEvent, AuditKind};
use crate::domain::operation::{NewOperation, OpenOperation, Operation, OperationStatus};
use crate::domain::ports::{BuildRefused, OperationRepository, StorageError};

/// Outcomes of a transaction step that are not database failures. They travel
/// through the transaction closure as an error so the transaction rolls back,
/// and [`classify`] turns them into their [`StorageError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum Step {
    #[error("open-operation index violated")]
    IndexViolated,
    #[error("operation is final")]
    OperationFinal,
    #[error("finished event does not belong to the operation")]
    Inconsistent,
}

fn step(outcome: Step) -> DbError {
    DbError::Other(anyhow::Error::new(outcome))
}

impl From<RowError> for DbError {
    fn from(error: RowError) -> Self {
        Self::Other(anyhow::Error::new(error))
    }
}

/// Attempts per write transaction, the first try included, on retryable
/// contention. An implementation choice: `toolkit-db`'s default of 3 is too few
/// for a burst of concurrent writers on one `SQLite` file, and the total stays
/// bounded by `storage_call_timeout` whatever this is.
const TX_ATTEMPTS: u32 = 10;

/// The most records one `interrupt_expired_batch` or `purge_finished` call
/// handles, whatever `limit` the caller passes. An implementation choice that
/// keeps the id list of the delete statement under every backend's
/// bind-parameter limit (`SQLite` allows 32766, `PostgreSQL` 65535).
pub const MAX_BATCH_LIMIT: u64 = 1000;

/// The [`StorageError`] of a failed storage step. A failure that is not a step
/// outcome logs its fixed-vocabulary class and the repository method, never the
/// driver's message.
fn classify(backend: DbBackend, call: &'static str, error: &DbError) -> StorageError {
    if let DbError::Other(inner) = error {
        match inner.downcast_ref::<Step>() {
            Some(Step::IndexViolated) => return StorageError::IndexViolated,
            Some(Step::OperationFinal) => return StorageError::OperationFinal,
            Some(Step::Inconsistent) => return StorageError::Inconsistent,
            None => {}
        }
    }
    tracing::warn!(
        class = error_class(backend, error),
        call,
        "application-manager: storage step failed"
    );
    StorageError::Unavailable
}

/// The log class of a database error: `connection`, `contention`, `constraint`,
/// `row_mapping` or `other` (`timeout` is logged where the timer fires). Derived
/// from the error's variant and kind, never from its message text.
fn error_class(backend: DbBackend, error: &DbError) -> &'static str {
    if let Some(db_err) = db_err(error) {
        return sea_class(backend, db_err);
    }
    match error {
        DbError::Other(inner) if inner.is::<RowError>() => "row_mapping",
        _ => "other",
    }
}

fn sea_class(backend: DbBackend, error: &DbErr) -> &'static str {
    if is_retryable_contention(backend, error) {
        return "contention";
    }
    match error {
        DbErr::ConnectionAcquire(ConnAcquireErr::Timeout) => "timeout",
        DbErr::ConnectionAcquire(_) | DbErr::Conn(_) => "connection",
        other => match other.sql_err() {
            Some(
                SqlErr::UniqueConstraintViolation(_) | SqlErr::ForeignKeyConstraintViolation(_),
            ) => "constraint",
            _ => "other",
        },
    }
}

/// The database error inside `error`, if any: a statement run through the
/// secure layer reports it wrapped in a `ScopeError`.
fn db_err(error: &DbError) -> Option<&DbErr> {
    match error {
        DbError::Sea(db_err) => Some(db_err),
        DbError::Other(inner) => match inner.downcast_ref::<ScopeError>() {
            Some(ScopeError::Db(db_err)) => Some(db_err),
            _ => None,
        },
        _ => None,
    }
}

/// The `SeaORM` implementation of [`OperationRepository`].
pub struct SeaOrmOperationRepository {
    db: Arc<DBProvider<DbError>>,
    backend: DbBackend,
    timeout: Duration,
}

impl SeaOrmOperationRepository {
    /// A repository over `db` whose every call is bounded by `timeout`.
    #[must_use]
    pub fn new(db: Arc<DBProvider<DbError>>, timeout: Duration) -> Self {
        let backend = db.db().backend();
        Self {
            db,
            backend,
            timeout,
        }
    }

    /// Run one storage call under `storage_call_timeout`. The single place
    /// the bound is applied (D8). A dropped call rolls its transaction back.
    async fn bounded<T>(
        &self,
        call: &'static str,
        work: impl Future<Output = Result<T, DbError>>,
    ) -> Result<T, StorageError> {
        match tokio::time::timeout(self.timeout, work).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(classify(self.backend, call, &error)),
            Err(_elapsed) => {
                tracing::warn!(
                    class = "timeout",
                    call,
                    "application-manager: storage call timed out"
                );
                Err(StorageError::Unavailable)
            }
        }
    }

    /// Run `body` as one transaction, restarted on retryable contention, all
    /// under `storage_call_timeout`. `body` runs once per attempt, so it
    /// clones what it moves.
    async fn transact<T, F>(&self, call: &'static str, body: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: for<'a> FnMut(
                &'a DbTx<'a>,
            )
                -> Pin<Box<dyn Future<Output = Result<T, DbError>> + Send + 'a>>
            + Send,
    {
        let db = self.db.db();
        self.bounded(
            call,
            db.transaction_with_retry_max(TxConfig::default(), TX_ATTEMPTS, db_err, body),
        )
        .await
    }
}

// ---- statements ----

const OPEN_STATUSES: [&str; 2] = ["accepted", "running"];

fn scope() -> AccessScope {
    AccessScope::allow_all()
}

fn is_open() -> Condition {
    Condition::all().add(operation::Column::Status.is_in(OPEN_STATUSES))
}

fn is_final() -> Condition {
    Condition::all().add(operation::Column::Status.is_in([
        status_token(OperationStatus::Completed),
        status_token(OperationStatus::Interrupted),
    ]))
}

/// The value `backend` binds for the timestamp `at`.
fn time_value(backend: DbBackend, at: OffsetDateTime) -> Value {
    stored_value(backend, at.into())
}

/// Open, with a run window still running at `now`: what a write must match.
fn writable(backend: DbBackend, operation_id: Uuid, now: OffsetDateTime) -> Condition {
    Condition::all()
        .add(operation::Column::OperationId.eq(operation_id))
        .add(is_open())
        .add(operation::Column::RunDeadline.gt(time_value(backend, now)))
}

/// Open and past its run window at `now`: what the interruption matches.
fn expired(backend: DbBackend, now: OffsetDateTime) -> Condition {
    Condition::all()
        .add(is_open())
        .add(operation::Column::RunDeadline.lte(time_value(backend, now)))
}

/// The insert of `row`, every value bound as [`stored_value`] gives it for
/// `backend`. Only `SQLite` changes anything, so only there is the statement
/// rebuilt, column by column as `Insert::one` builds it.
fn insert_of<A>(backend: DbBackend, mut row: A) -> Insert<A>
where
    A: ActiveModelTrait + Clone,
{
    let mut insert = Insert::one(row.clone());
    if backend != DbBackend::Sqlite {
        return insert;
    }
    let mut columns = Vec::new();
    let mut values = Vec::new();
    for column in <A::Entity as EntityTrait>::Column::iter() {
        if let ActiveValue::Set(value) | ActiveValue::Unchanged(value) = row.take(column) {
            columns.push(column);
            values.push(column.save_as(Expr::val(stored_value(backend, value))));
        }
    }
    let mut statement = InsertStatement::new();
    statement
        .into_table(A::Entity::default().table_ref())
        .columns(columns)
        .values_panic(values);
    *insert.query() = statement;
    insert
}

async fn insert_event(
    runner: &impl DBRunner,
    backend: DbBackend,
    event: &AuditEvent,
) -> Result<(), DbError> {
    insert_of(backend, event_row(backend, event)?)
        .secure()
        .scope_unchecked(&scope())?
        .exec(runner)
        .await?;
    Ok(())
}

async fn find_operation(
    runner: &impl DBRunner,
    condition: Condition,
) -> Result<Option<operation::Model>, DbError> {
    Ok(operation::Entity::find()
        .secure()
        .scope_with(&scope())
        .filter(condition)
        .one(runner)
        .await?)
}

/// A guarded update: changes the record `condition` matches, or is
/// [`Step::OperationFinal`] when it matches none.
async fn update_operation(
    runner: &impl DBRunner,
    condition: Condition,
    changes: Vec<(operation::Column, SimpleExpr)>,
) -> Result<(), DbError> {
    if update_if_matched(runner, condition, changes).await? {
        Ok(())
    } else {
        Err(step(Step::OperationFinal))
    }
}

async fn update_if_matched(
    runner: &impl DBRunner,
    condition: Condition,
    changes: Vec<(operation::Column, SimpleExpr)>,
) -> Result<bool, DbError> {
    let mut update = operation::Entity::update_many();
    for (column, value) in changes {
        update = update.col_expr(column, value);
    }
    let result = update
        .filter(condition)
        .secure()
        .scope_with(&scope())
        .exec(runner)
        .await?;
    Ok(result.rows_affected > 0)
}

/// The interruption step: when the record is open and past its deadline, mark
/// it `interrupted`, keep its report with `interrupted = true`, and insert the
/// finished event under the record's `finished_event_id`.
///
/// The guarded update of the status comes first: it claims the record, and on
/// `PostgreSQL` and `MySQL` it locks the row, so the report read next is the
/// last one committed and no concurrent report write can slip in before this
/// transaction ends (`SQLite` serialises writers).
async fn interrupt_step(
    runner: &impl DBRunner,
    backend: DbBackend,
    operation_id: Uuid,
    now: OffsetDateTime,
) -> Result<bool, DbError> {
    let guard = Condition::all()
        .add(operation::Column::OperationId.eq(operation_id))
        .add(expired(backend, now));
    let claim = vec![
        (
            operation::Column::Status,
            Expr::value(status_token(OperationStatus::Interrupted)),
        ),
        (
            operation::Column::FinishedAt,
            Expr::value(time_value(backend, now)),
        ),
    ];
    if !update_if_matched(runner, guard, claim).await? {
        return Ok(false);
    }
    let by_id = Condition::all().add(operation::Column::OperationId.eq(operation_id));
    let row = find_operation(runner, by_id.clone())
        .await?
        .ok_or_else(|| DbError::Sea(DbErr::RecordNotFound("operation".to_owned())))?;
    let mut record = operation_from_row(row)?;
    record.report.interrupted = true;
    record.report.category = None;
    let report = vec![(
        operation::Column::Report,
        Expr::value(report_json(&record.report)?),
    )];
    update_operation(runner, by_id, report).await?;

    let finished = AuditEvent {
        event_id: record.finished_event_id,
        kind: AuditKind::Finished,
        correlation_id: record.operation_id,
        action: record.action,
        written_at: now,
        content: audit::finished(&record.report),
    };
    insert_event(runner, backend, &finished).await?;
    Ok(true)
}

async fn accept_step(
    runner: &impl DBRunner,
    backend: DbBackend,
    new: &NewOperation,
    accepted: &AuditEvent,
    now: OffsetDateTime,
) -> Result<(), DbError> {
    // An expired open operation of the application does not block acceptance.
    let blocking = Condition::all()
        .add(operation::Column::ApplicationId.eq(new.application_id.as_str()))
        .add(expired(backend, now));
    if let Some(row) = find_operation(runner, blocking).await? {
        interrupt_step(runner, backend, row.operation_id, now).await?;
    }
    insert_of(backend, operation_row(backend, new)?)
        .secure()
        .scope_unchecked(&scope())?
        .exec(runner)
        .await
        .map_err(|error| {
            if error.is_unique_violation() {
                step(Step::IndexViolated)
            } else {
                DbError::from(error)
            }
        })?;
    insert_event(runner, backend, accepted).await
}

/// Record a completed operation: check the finished event against the record,
/// the guarded update, then the finished event.
///
/// The stored report carries `category` and `interrupted = false` whatever the
/// caller's copy says, so the column and the report never disagree. A finished
/// event that does not belong to the record (its id, correlation, action or
/// kind) is [`Step::Inconsistent`] and nothing is written.
#[expect(
    clippy::too_many_arguments,
    reason = "the backend travels with `now` to bind it as the backend stores it"
)]
async fn complete_step(
    runner: &impl DBRunner,
    backend: DbBackend,
    operation_id: Uuid,
    mut changes: Vec<(operation::Column, SimpleExpr)>,
    report: &OutcomeReport,
    category: RequestCategory,
    finished: &AuditEvent,
    now: OffsetDateTime,
) -> Result<(), DbError> {
    let Some(row) = find_operation(runner, writable(backend, operation_id, now)).await? else {
        return Err(step(Step::OperationFinal));
    };
    let belongs = finished.event_id == row.finished_event_id
        && finished.correlation_id == operation_id
        && finished.kind == AuditKind::Finished
        && enum_token(&finished.action)? == row.action;
    if !belongs {
        return Err(step(Step::Inconsistent));
    }
    let mut stored = report.clone();
    stored.category = Some(category);
    stored.interrupted = false;
    changes.extend([
        (
            operation::Column::Status,
            Expr::value(status_token(OperationStatus::Completed)),
        ),
        (
            operation::Column::Category,
            Expr::value(enum_token(&category)?),
        ),
        (
            operation::Column::FinishedAt,
            Expr::value(time_value(backend, now)),
        ),
        (
            operation::Column::Report,
            Expr::value(report_json(&stored)?),
        ),
    ]);
    update_operation(runner, writable(backend, operation_id, now), changes).await?;
    insert_event(runner, backend, finished).await
}

#[async_trait]
impl OperationRepository for SeaOrmOperationRepository {
    async fn open_operation(
        &self,
        application_id: &str,
        now: OffsetDateTime,
    ) -> Result<Option<OpenOperation>, StorageError> {
        let now = stored_time(self.backend, now);
        self.bounded("open_operation", async {
            let conn = self.db.conn()?;
            let open = Condition::all()
                .add(operation::Column::ApplicationId.eq(application_id))
                .add(is_open());
            Ok(find_operation(&conn, open).await?.map(|row| OpenOperation {
                operation_id: row.operation_id,
                expired: row.run_deadline <= now,
            }))
        })
        .await
    }

    async fn accept(
        &self,
        new: NewOperation,
        accepted: AuditEvent,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        let (backend, now) = (self.backend, stored_time(self.backend, now));
        self.transact("accept", move |tx| {
            let (new, accepted) = (new.clone(), accepted.clone());
            Box::pin(async move { accept_step(tx, backend, &new, &accepted, now).await })
        })
        .await
    }

    async fn refuse_after_lost_race(
        &self,
        application_id: &str,
        build_refused: BuildRefused,
    ) -> Result<Uuid, StorageError> {
        let application_id = application_id.to_owned();
        let build_refused: Arc<dyn Fn(Uuid) -> AuditEvent + Send + Sync> = Arc::from(build_refused);
        let backend = self.backend;
        let winner = self
            .transact("refuse_after_lost_race", move |tx| {
                let open = Condition::all()
                    .add(operation::Column::ApplicationId.eq(application_id.as_str()))
                    .add(is_open());
                let build_refused = Arc::clone(&build_refused);
                Box::pin(async move {
                    let Some(winner) = find_operation(tx, open).await? else {
                        return Ok(None);
                    };
                    let refused = build_refused(winner.operation_id);
                    insert_event(tx, backend, &refused).await?;
                    Ok(Some(winner.operation_id))
                })
            })
            .await?;
        // No open winner: the loser's refusal has nothing to name.
        winner.ok_or(StorageError::Unavailable)
    }

    async fn record_refused(&self, event: AuditEvent) -> Result<(), StorageError> {
        let backend = self.backend;
        self.transact("record_refused", move |tx| {
            let event = event.clone();
            Box::pin(async move { insert_event(tx, backend, &event).await })
        })
        .await
    }

    async fn mark_running(
        &self,
        operation_id: Uuid,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        let now = stored_time(self.backend, now);
        self.bounded("mark_running", async {
            let conn = self.db.conn()?;
            let guard = writable(self.backend, operation_id, now)
                .add(operation::Column::Status.eq(status_token(OperationStatus::Accepted)));
            let changes = vec![
                (
                    operation::Column::Status,
                    Expr::value(status_token(OperationStatus::Running)),
                ),
                (
                    operation::Column::StartedAt,
                    Expr::value(time_value(self.backend, now)),
                ),
            ];
            update_operation(&conn, guard, changes).await
        })
        .await
    }

    async fn record_report(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        let now = stored_time(self.backend, now);
        self.bounded("record_report", async {
            let conn = self.db.conn()?;
            let changes = vec![(operation::Column::Report, Expr::value(report_json(report)?))];
            update_operation(&conn, writable(self.backend, operation_id, now), changes).await
        })
        .await
    }

    async fn finish(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        category: RequestCategory,
        finished: AuditEvent,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        let (backend, now) = (self.backend, stored_time(self.backend, now));
        let report = report.clone();
        self.transact("finish", move |tx| {
            let (report, finished) = (report.clone(), finished.clone());
            Box::pin(async move {
                complete_step(
                    tx,
                    backend,
                    operation_id,
                    Vec::new(),
                    &report,
                    category,
                    &finished,
                    now,
                )
                .await
            })
        })
        .await
    }

    async fn complete_by_supervisor(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        finished: AuditEvent,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        let (backend, now) = (self.backend, stored_time(self.backend, now));
        let report = report.clone();
        self.transact("complete_by_supervisor", move |tx| {
            let (report, finished) = (report.clone(), finished.clone());
            Box::pin(async move {
                // `started_at` is set only when mark running never ran.
                let started_at = Func::coalesce([
                    Expr::col(operation::Column::StartedAt),
                    Expr::value(time_value(backend, now)),
                ]);
                complete_step(
                    tx,
                    backend,
                    operation_id,
                    vec![(operation::Column::StartedAt, started_at.into())],
                    &report,
                    RequestCategory::InternalFailure,
                    &finished,
                    now,
                )
                .await
            })
        })
        .await
    }

    async fn interrupt_if_expired(
        &self,
        operation_id: Uuid,
        now: OffsetDateTime,
    ) -> Result<bool, StorageError> {
        let (backend, now) = (self.backend, stored_time(self.backend, now));
        self.transact("interrupt_if_expired", move |tx| {
            Box::pin(async move { interrupt_step(tx, backend, operation_id, now).await })
        })
        .await
    }

    /// Each record goes through [`Self::interrupt_if_expired`], so each
    /// storage access has its own `storage_call_timeout`. A record that cannot
    /// be interrupted is logged by operation id and skipped; the rest go on.
    async fn interrupt_expired_batch(
        &self,
        now: OffsetDateTime,
        limit: u64,
    ) -> Result<u64, StorageError> {
        let now = stored_time(self.backend, now);
        let limit = limit.min(MAX_BATCH_LIMIT);
        let due: Vec<Uuid> = self
            .bounded("interrupt_expired_batch", async {
                let conn = self.db.conn()?;
                let rows = operation::Entity::find()
                    .secure()
                    .scope_with(&scope())
                    .filter(expired(self.backend, now))
                    .order_by(operation::Column::RunDeadline, Order::Asc)
                    .order_by(operation::Column::OperationId, Order::Asc)
                    .limit(limit)
                    .all(&conn)
                    .await?;
                Ok(rows.into_iter().map(|row| row.operation_id).collect())
            })
            .await?;
        let mut interrupted = 0_u64;
        let mut failed = 0_usize;
        for operation_id in &due {
            match self.interrupt_if_expired(*operation_id, now).await {
                Ok(true) => interrupted += 1,
                Ok(false) => {}
                Err(_) => {
                    failed += 1;
                    tracing::warn!(
                        %operation_id,
                        "application-manager: expired operation not interrupted"
                    );
                }
            }
        }
        if failed > 0 && failed == due.len() {
            return Err(StorageError::Unavailable);
        }
        Ok(interrupted)
    }

    async fn purge_finished(
        &self,
        before: OffsetDateTime,
        limit: u64,
    ) -> Result<u64, StorageError> {
        let before = stored_time(self.backend, before);
        let limit = limit.min(MAX_BATCH_LIMIT);
        self.bounded("purge_finished", async {
            let conn = self.db.conn()?;
            let old = Condition::all()
                .add(is_final())
                .add(operation::Column::FinishedAt.lt(time_value(self.backend, before)));
            let ids: Vec<Uuid> = operation::Entity::find()
                .secure()
                .scope_with(&scope())
                .filter(old)
                .order_by(operation::Column::FinishedAt, Order::Asc)
                .order_by(operation::Column::OperationId, Order::Asc)
                .limit(limit)
                .all(&conn)
                .await?
                .into_iter()
                .map(|row| row.operation_id)
                .collect();
            if ids.is_empty() {
                return Ok(0);
            }
            // The status filter repeats: an open record is never deleted.
            let deleted = operation::Entity::delete_many()
                .filter(
                    Condition::all()
                        .add(operation::Column::OperationId.is_in(ids))
                        .add(is_final()),
                )
                .secure()
                .scope_with(&scope())
                .exec(&conn)
                .await?;
            Ok(deleted.rows_affected)
        })
        .await
    }

    async fn read_operation(
        &self,
        operation_id: Uuid,
        now: OffsetDateTime,
    ) -> Result<Option<Operation>, StorageError> {
        let (backend, now) = (self.backend, stored_time(self.backend, now));
        self.transact("read_operation", move |tx| {
            Box::pin(async move {
                interrupt_step(tx, backend, operation_id, now).await?;
                let by_id = Condition::all().add(operation::Column::OperationId.eq(operation_id));
                Ok(find_operation(tx, by_id)
                    .await?
                    .map(operation_from_row)
                    .transpose()?)
            })
        })
        .await
    }

    async fn audit_events(&self, correlation_id: Uuid) -> Result<Vec<AuditEvent>, StorageError> {
        self.bounded("audit_events", async {
            let conn = self.db.conn()?;
            let rows = audit_event::Entity::find()
                .secure()
                .scope_with(&scope())
                .filter(Condition::all().add(audit_event::Column::CorrelationId.eq(correlation_id)))
                // Write time first: a request's events read in the order they
                // were written; the time-ordered event id breaks a tie.
                .order_by(audit_event::Column::WrittenAt, Order::Asc)
                .order_by(audit_event::Column::EventId, Order::Asc)
                .all(&conn)
                .await?;
            Ok(rows
                .into_iter()
                .map(event_from_row)
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "repo_tests.rs"]
mod tests;
