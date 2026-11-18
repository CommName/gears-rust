//! Operation repository over `SeaORM`: every step and transaction of DESIGN
//! 3.6 and 3.7, each bounded by `storage_call_timeout`.
//!
//! # Security boundary
//!
//! The three tables are internal bookkeeping with no tenant column (ADR
//! 0004), so their entities are `#[secure(unrestricted)]` and every statement
//! goes through `secure()` with `AccessScope::allow_all()`. Raw SQL appears
//! only in the migration; the database clock is read through a secure
//! projection ([`db_now`]).
//!
//! # Database time and the status guard
//!
//! Every call reads the database time once, first, and uses it for every
//! guard and timestamp it writes (DESIGN 3.1 "Status guard"). A write to an
//! operation matches only a record whose status is `accepted` or `running`
//! and whose `run_deadline` is after that time; the expiry completion matches
//! the opposite deadline. A guarded update that changes no row is
//! [`StorageError::OperationFinal`]: the first final write wins, and a
//! transaction that fails its guard has written nothing. A read-modify-write
//! of the report locks its row first on `PostgreSQL` and `MySQL`; `SQLite`
//! serialises writers.
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
use tracing::Instrument;

use application_manager_sdk::{
    OperationAction, OutcomeReport, RequestCategory, SectionOutcome, SectionOutcomeEntry,
};
use async_trait::async_trait;
use sea_orm::sea_query::{Expr, InsertStatement, SimpleExpr};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnAcquireErr, DbBackend, DbErr,
    EntityName, EntityTrait, FromQueryResult, Insert, Iterable, Order, QueryFilter, QuerySelect,
    QueryTrait, SqlErr, Value,
};
use time::{OffsetDateTime, PrimitiveDateTime};
use toolkit_db::secure::{
    DBRunner, SecureDeleteExt, SecureEntityExt, SecureInsertExt, SecureUpdateExt,
};
use toolkit_db::secure::{ScopeError, TxConfig};
use toolkit_db::{DBProvider, DbError, DbTx, contention::is_retryable_contention};
use toolkit_security::AccessScope;
use uuid::Uuid;

use super::entity::{application, history_event, operation};
use super::mapping::{
    RowError, application_from_row, application_row, enum_token, event_from_row, event_row,
    operation_from_row, operation_row, report_json, status_token, stored_time, stored_value,
};
use crate::constants::{
    APPLICATION_PAGE_MAX_LIMIT, HISTORY_PAGE_MAX_LIMIT, MAINTENANCE_BATCH_SIZE,
};
use crate::domain::history::{self, HistoryEvent, HistoryKind};
use crate::domain::operation::{
    Accepted, ApplicationRecord, CompletionReason, NewOperation, OpenOperation, Operation,
    OperationStatus, complete_open_sections, expiry_category,
};
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

/// The [`StorageError`] of a failed storage step. A failure that is not a step
/// outcome logs its fixed-vocabulary class and the repository method, never the
/// driver's message.
fn classify(backend: DbBackend, call: &'static str, error: &DbError) -> StorageError {
    if let DbError::Other(inner) = error {
        match inner.downcast_ref::<Step>() {
            Some(Step::IndexViolated) => return StorageError::IndexViolated,
            Some(Step::OperationFinal) => return StorageError::OperationFinal,
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
    /// the bound is applied. A dropped call rolls its transaction back.
    async fn bounded<T>(
        &self,
        call: &'static str,
        work: impl Future<Output = Result<T, DbError>>,
    ) -> Result<T, StorageError> {
        let span = tracing::debug_span!("application_manager.storage", call);
        match tokio::time::timeout(self.timeout, work)
            .instrument(span)
            .await
        {
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

fn is_completed() -> Condition {
    Condition::all().add(operation::Column::Status.eq(status_token(OperationStatus::Completed)))
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

/// Open and past its run window at `now`: what the expiry completion matches.
fn expired(backend: DbBackend, now: OffsetDateTime) -> Condition {
    Condition::all()
        .add(is_open())
        .add(operation::Column::RunDeadline.lte(time_value(backend, now)))
}

/// The database clock as text, `YYYY-MM-DD HH:MM:SS.fff[fff]` in UTC.
fn clock_sql(backend: DbBackend) -> Result<&'static str, DbError> {
    match backend {
        DbBackend::Postgres => Ok("to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')"),
        DbBackend::MySql => Ok("DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%d %H:%i:%s.%f')"),
        DbBackend::Sqlite => Ok("strftime('%Y-%m-%d %H:%M:%f', 'now')"),
        other => Err(DbError::Other(anyhow::anyhow!(
            "unsupported database backend {other:?}"
        ))),
    }
}

#[derive(Debug, FromQueryResult)]
struct ClockRow {
    now: String,
}

/// The database's current time, read once per storage call (DESIGN 3.1
/// "Status guard"). An aggregate over `operations` always returns one row,
/// so the clock is read through the secure layer with no raw statement. On
/// `PostgreSQL` it is the transaction's start time, the same for every
/// statement of the call.
async fn db_now(runner: &impl DBRunner, backend: DbBackend) -> Result<OffsetDateTime, DbError> {
    let clock = clock_sql(backend)?;
    let rows = operation::Entity::find()
        .secure()
        .scope_with(&scope())
        .project_all(runner, |select| {
            select
                .select_only()
                .column_as(Expr::cust("COUNT(*)"), "rows")
                .column_as(Expr::cust(clock), "now")
                .into_model::<ClockRow>()
        })
        .await?;
    let text = rows.into_iter().next().ok_or(RowError)?.now;
    let format = time::macros::format_description!(
        "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]"
    );
    let at = PrimitiveDateTime::parse(&text, format)
        .map_err(|_| RowError)?
        .assume_utc();
    Ok(stored_time(backend, at))
}

/// `at + window`, or a row error when it leaves the time range.
fn plus(at: OffsetDateTime, window: Duration) -> Result<OffsetDateTime, DbError> {
    time::Duration::try_from(window)
        .ok()
        .and_then(|window| at.checked_add(window))
        .ok_or_else(|| DbError::from(RowError))
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

/// Insert `event`, written at `now`.
async fn insert_event(
    runner: &impl DBRunner,
    backend: DbBackend,
    event: &HistoryEvent,
    now: OffsetDateTime,
) -> Result<(), DbError> {
    insert_of(backend, event_row(backend, event, now)?)
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

/// [`find_operation`] for a read-modify-write: the row is locked on
/// `PostgreSQL` and `MySQL` until the transaction ends.
async fn find_for_update(
    runner: &impl DBRunner,
    backend: DbBackend,
    condition: Condition,
) -> Result<Option<operation::Model>, DbError> {
    let select = operation::Entity::find()
        .secure()
        .scope_with(&scope())
        .filter(condition);
    let select = if backend == DbBackend::Sqlite {
        select
    } else {
        select.lock_exclusive()
    };
    Ok(select.one(runner).await?)
}

/// A guarded update: changes the record `condition` matches, or is
/// [`Step::OperationFinal`] when it matches none.
async fn update_operation(
    runner: &impl DBRunner,
    condition: Condition,
    changes: Vec<(operation::Column, SimpleExpr)>,
) -> Result<(), DbError> {
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
    if result.rows_affected > 0 {
        Ok(())
    } else {
        Err(step(Step::OperationFinal))
    }
}

/// Upsert the application record of a seed that finished while `running`.
async fn upsert_application(
    runner: &impl DBRunner,
    backend: DbBackend,
    record: &ApplicationRecord,
) -> Result<(), DbError> {
    let existing = application::Entity::find()
        .secure()
        .scope_with(&scope())
        .filter(
            Condition::all()
                .add(application::Column::ApplicationId.eq(record.application_id.as_str())),
        )
        .one(runner)
        .await?;
    if existing.is_none() {
        insert_of(backend, application_row(backend, record)?)
            .secure()
            .scope_unchecked(&scope())?
            .exec(runner)
            .await?;
        return Ok(());
    }
    application::Entity::update_many()
        .col_expr(
            application::Column::Version,
            Expr::value(record.version.clone()),
        )
        .col_expr(
            application::Column::OperationId,
            Expr::value(record.operation_id),
        )
        .col_expr(
            application::Column::Category,
            Expr::value(enum_token(&record.category)?),
        )
        .col_expr(
            application::Column::FinishedAt,
            Expr::value(time_value(backend, record.finished_at)),
        )
        .filter(
            Condition::all()
                .add(application::Column::ApplicationId.eq(record.application_id.as_str())),
        )
        .secure()
        .scope_with(&scope())
        .exec(runner)
        .await?;
    Ok(())
}

async fn delete_application(runner: &impl DBRunner, application_id: &str) -> Result<(), DbError> {
    application::Entity::delete_many()
        .filter(Condition::all().add(application::Column::ApplicationId.eq(application_id)))
        .secure()
        .scope_with(&scope())
        .exec(runner)
        .await?;
    Ok(())
}

/// Complete the open record `row`, already read under `guard` in this
/// transaction: the guarded update to `completed` with `report` (whose
/// category is `category`) at `now`, the finished event under the record's
/// `finished_event_id`, and the application record: upserted for a seed that
/// was `running`, deleted for an uninstall that succeeded.
#[expect(
    clippy::too_many_arguments,
    reason = "the backend travels with `now` to bind it as the backend stores it"
)]
async fn complete_step(
    runner: &impl DBRunner,
    backend: DbBackend,
    row: &operation::Model,
    guard: Condition,
    mut report: OutcomeReport,
    category: RequestCategory,
    reason: Option<CompletionReason>,
    now: OffsetDateTime,
) -> Result<(), DbError> {
    let record = operation_from_row(row.clone())?;
    report.category = Some(category);
    let changes = vec![
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
            Expr::value(report_json(&report)?),
        ),
    ];
    // The status read under the lock must still hold.
    let guard = guard.add(operation::Column::Status.eq(row.status.as_str()));
    update_operation(runner, guard, changes).await?;

    let finished = HistoryEvent {
        event_id: record.finished_event_id,
        kind: HistoryKind::Finished,
        correlation_id: record.operation_id,
        action: record.action,
        written_at: now,
        content: history::finished(&report, reason),
    };
    insert_event(runner, backend, &finished, now).await?;

    let was_running = record.status == OperationStatus::Running;
    match record.action {
        OperationAction::Seed if was_running => {
            let application = ApplicationRecord {
                application_id: record.application_id.clone(),
                version: report.application.version.clone().unwrap_or_default(),
                operation_id: record.operation_id,
                category,
                finished_at: now,
            };
            upsert_application(runner, backend, &application).await
        }
        OperationAction::Uninstall if category == RequestCategory::Succeeded => {
            delete_application(runner, &record.application_id).await
        }
        _ => Ok(()),
    }
}

/// The expiry completion of `operation_id` when it is open and its run
/// deadline is at or before `now`. Returns whether it completed it.
async fn expire_step(
    runner: &impl DBRunner,
    backend: DbBackend,
    operation_id: Uuid,
    now: OffsetDateTime,
) -> Result<bool, DbError> {
    let guard = Condition::all()
        .add(operation::Column::OperationId.eq(operation_id))
        .add(expired(backend, now));
    let Some(row) = find_for_update(runner, backend, guard.clone()).await? else {
        return Ok(false);
    };
    let record = operation_from_row(row.clone())?;
    let mut report = record.report;
    complete_open_sections(&mut report, record.status == OperationStatus::Running);
    let category = expiry_category(&report.sections);
    complete_step(
        runner,
        backend,
        &row,
        guard,
        report,
        category,
        Some(CompletionReason::Expired),
        now,
    )
    .await?;
    Ok(true)
}

/// The open record `operation_id`, locked, when a write may still change it at
/// `now`; [`Step::OperationFinal`] otherwise.
async fn writable_row(
    runner: &impl DBRunner,
    backend: DbBackend,
    operation_id: Uuid,
    now: OffsetDateTime,
) -> Result<operation::Model, DbError> {
    find_for_update(runner, backend, writable(backend, operation_id, now))
        .await?
        .ok_or_else(|| step(Step::OperationFinal))
}

async fn accept_step(
    runner: &impl DBRunner,
    backend: DbBackend,
    new: &NewOperation,
    accepted: &HistoryEvent,
) -> Result<Accepted, DbError> {
    let now = db_now(runner, backend).await?;
    // An expired open operation of the application does not block acceptance.
    let blocking = Condition::all()
        .add(operation::Column::ApplicationId.eq(new.application_id.as_str()))
        .add(expired(backend, now));
    if let Some(row) = find_operation(runner, blocking).await? {
        expire_step(runner, backend, row.operation_id, now).await?;
    }
    let run_deadline = stored_time(backend, plus(now, new.run_window)?);
    insert_of(backend, operation_row(backend, new, now, run_deadline)?)
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
    insert_event(runner, backend, accepted, now).await?;
    Ok(Accepted {
        accepted_at: now,
        run_deadline,
    })
}

#[async_trait]
impl OperationRepository for SeaOrmOperationRepository {
    async fn open_operation(
        &self,
        application_id: &str,
    ) -> Result<Option<OpenOperation>, StorageError> {
        self.bounded("open_operation", async {
            let conn = self.db.conn()?;
            let now = db_now(&conn, self.backend).await?;
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
        accepted: HistoryEvent,
    ) -> Result<Accepted, StorageError> {
        let backend = self.backend;
        self.transact("accept", move |tx| {
            let (new, accepted) = (new.clone(), accepted.clone());
            Box::pin(async move { accept_step(tx, backend, &new, &accepted).await })
        })
        .await
    }

    async fn refuse_after_lost_race(
        &self,
        application_id: &str,
        build_refused: BuildRefused,
    ) -> Result<Uuid, StorageError> {
        let application_id = application_id.to_owned();
        let build_refused: Arc<dyn Fn(Uuid) -> HistoryEvent + Send + Sync> =
            Arc::from(build_refused);
        let backend = self.backend;
        let winner = self
            .transact("refuse_after_lost_race", move |tx| {
                let open = Condition::all()
                    .add(operation::Column::ApplicationId.eq(application_id.as_str()))
                    .add(is_open());
                let build_refused = Arc::clone(&build_refused);
                Box::pin(async move {
                    let now = db_now(tx, backend).await?;
                    let Some(winner) = find_operation(tx, open).await? else {
                        return Ok(None);
                    };
                    let refused = build_refused(winner.operation_id);
                    insert_event(tx, backend, &refused, now).await?;
                    Ok(Some(winner.operation_id))
                })
            })
            .await?;
        // No open winner: the loser's refusal has nothing to name.
        winner.ok_or(StorageError::Unavailable)
    }

    async fn record_refused(&self, event: HistoryEvent) -> Result<(), StorageError> {
        let backend = self.backend;
        self.transact("record_refused", move |tx| {
            let event = event.clone();
            Box::pin(async move {
                let now = db_now(tx, backend).await?;
                insert_event(tx, backend, &event, now).await
            })
        })
        .await
    }

    async fn mark_running(
        &self,
        operation_id: Uuid,
        sections: &[(String, Option<String>)],
    ) -> Result<Duration, StorageError> {
        let backend = self.backend;
        let sections = sections.to_vec();
        self.transact("mark_running", move |tx| {
            let sections = sections.clone();
            Box::pin(async move {
                let now = db_now(tx, backend).await?;
                let guard = writable(backend, operation_id, now)
                    .add(operation::Column::Status.eq(status_token(OperationStatus::Accepted)));
                let row = find_for_update(tx, backend, guard.clone())
                    .await?
                    .ok_or_else(|| step(Step::OperationFinal))?;
                let mut report = operation_from_row(row.clone())?.report;
                report.sections = sections
                    .into_iter()
                    .map(|(key, section_type)| SectionOutcomeEntry {
                        key,
                        section_type,
                        outcome: None,
                        detail: None,
                        location: None,
                    })
                    .collect();
                let changes = vec![
                    (
                        operation::Column::Status,
                        Expr::value(status_token(OperationStatus::Running)),
                    ),
                    (
                        operation::Column::StartedAt,
                        Expr::value(time_value(backend, now)),
                    ),
                    (
                        operation::Column::Report,
                        Expr::value(report_json(&report)?),
                    ),
                ];
                update_operation(tx, guard, changes).await?;
                let left = row.run_deadline - now;
                Ok(Duration::try_from(left).unwrap_or(Duration::ZERO))
            })
        })
        .await
    }

    async fn record_section_outcome(
        &self,
        operation_id: Uuid,
        key: &str,
        outcome: SectionOutcome,
        detail: Option<String>,
        location: Option<String>,
    ) -> Result<(), StorageError> {
        let backend = self.backend;
        let key = key.to_owned();
        self.transact("record_section_outcome", move |tx| {
            let (key, detail, location) = (key.clone(), detail.clone(), location.clone());
            Box::pin(async move {
                let now = db_now(tx, backend).await?;
                let row = writable_row(tx, backend, operation_id, now).await?;
                let mut report = operation_from_row(row)?.report;
                let entry = report
                    .sections
                    .iter_mut()
                    .find(|entry| entry.key == key)
                    .ok_or_else(|| step(Step::OperationFinal))?;
                entry.outcome = Some(outcome);
                entry.detail = detail;
                entry.location = location;
                let changes = vec![(
                    operation::Column::Report,
                    Expr::value(report_json(&report)?),
                )];
                update_operation(tx, writable(backend, operation_id, now), changes).await
            })
        })
        .await
    }

    async fn finish(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        category: RequestCategory,
    ) -> Result<(), StorageError> {
        let backend = self.backend;
        let report = report.clone();
        self.transact("finish", move |tx| {
            let report = report.clone();
            Box::pin(async move {
                let now = db_now(tx, backend).await?;
                let row = writable_row(tx, backend, operation_id, now).await?;
                let guard = writable(backend, operation_id, now);
                complete_step(tx, backend, &row, guard, report, category, None, now).await
            })
        })
        .await
    }

    async fn complete_by_supervisor(&self, operation_id: Uuid) -> Result<(), StorageError> {
        let backend = self.backend;
        self.transact("complete_by_supervisor", move |tx| {
            Box::pin(async move {
                let now = db_now(tx, backend).await?;
                let row = writable_row(tx, backend, operation_id, now).await?;
                let mut report = operation_from_row(row.clone())?.report;
                complete_open_sections(&mut report, false);
                let guard = writable(backend, operation_id, now);
                complete_step(
                    tx,
                    backend,
                    &row,
                    guard,
                    report,
                    RequestCategory::InternalFailure,
                    None,
                    now,
                )
                .await
            })
        })
        .await
    }

    async fn expire_if_expired(&self, operation_id: Uuid) -> Result<bool, StorageError> {
        let backend = self.backend;
        self.transact("expire_if_expired", move |tx| {
            Box::pin(async move {
                let now = db_now(tx, backend).await?;
                expire_step(tx, backend, operation_id, now).await
            })
        })
        .await
    }

    /// Each record goes through [`Self::expire_if_expired`], so each storage
    /// access has its own `storage_call_timeout`. A record that cannot be
    /// completed is logged by operation id and skipped; the rest go on.
    async fn expire_batch(&self, limit: u64) -> Result<u64, StorageError> {
        let limit = limit.min(MAINTENANCE_BATCH_SIZE);
        let due: Vec<Uuid> = self
            .bounded("expire_batch", async {
                let conn = self.db.conn()?;
                let now = db_now(&conn, self.backend).await?;
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
        let mut completed = 0_u64;
        let mut failed = 0_usize;
        for operation_id in &due {
            match self.expire_if_expired(*operation_id).await {
                Ok(true) => completed += 1,
                Ok(false) => {}
                Err(_) => {
                    failed += 1;
                    tracing::warn!(
                        %operation_id,
                        "application-manager: expired operation not completed"
                    );
                }
            }
        }
        if failed > 0 && failed == due.len() {
            return Err(StorageError::Unavailable);
        }
        Ok(completed)
    }

    async fn purge(&self, retention: Duration, limit: u64) -> Result<u64, StorageError> {
        let limit = limit.min(MAINTENANCE_BATCH_SIZE);
        self.bounded("purge", async {
            let conn = self.db.conn()?;
            let now = db_now(&conn, self.backend).await?;
            // `finished_at + retention <= now`, as `finished_at <= now - retention`.
            let Some(cutoff) = time::Duration::try_from(retention)
                .ok()
                .and_then(|retention| now.checked_sub(retention))
            else {
                return Ok(0);
            };
            let cutoff = stored_time(self.backend, cutoff);
            let old = is_completed()
                .add(operation::Column::FinishedAt.lte(time_value(self.backend, cutoff)));
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
                .filter(is_completed().add(operation::Column::OperationId.is_in(ids)))
                .secure()
                .scope_with(&scope())
                .exec(&conn)
                .await?;
            Ok(deleted.rows_affected)
        })
        .await
    }

    async fn read_operation(&self, operation_id: Uuid) -> Result<Option<Operation>, StorageError> {
        let backend = self.backend;
        self.transact("read_operation", move |tx| {
            Box::pin(async move {
                let now = db_now(tx, backend).await?;
                expire_step(tx, backend, operation_id, now).await?;
                let by_id = Condition::all().add(operation::Column::OperationId.eq(operation_id));
                Ok(find_operation(tx, by_id)
                    .await?
                    .map(operation_from_row)
                    .transpose()?)
            })
        })
        .await
    }

    async fn history_page(
        &self,
        correlation_ids: Option<Vec<Uuid>>,
        after: Option<Uuid>,
        limit: u64,
    ) -> Result<Vec<HistoryEvent>, StorageError> {
        let limit = limit.min(HISTORY_PAGE_MAX_LIMIT);
        self.bounded("history_page", async {
            let conn = self.db.conn()?;
            let mut filter = Condition::all();
            if let Some(ids) = correlation_ids {
                filter = filter.add(history_event::Column::CorrelationId.is_in(ids));
            }
            if let Some(after) = after {
                filter = filter.add(history_event::Column::EventId.gt(after));
            }
            let rows = history_event::Entity::find()
                .secure()
                .scope_with(&scope())
                .filter(filter)
                .order_by(history_event::Column::EventId, Order::Asc)
                .limit(limit)
                .all(&conn)
                .await?;
            Ok(rows
                .into_iter()
                .map(event_from_row)
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }

    async fn applications_page(
        &self,
        application_ids: Option<Vec<String>>,
        after: Option<String>,
        limit: u64,
    ) -> Result<Vec<ApplicationRecord>, StorageError> {
        let limit = limit.min(APPLICATION_PAGE_MAX_LIMIT);
        self.bounded("applications_page", async {
            let conn = self.db.conn()?;
            let mut filter = Condition::all();
            if let Some(ids) = application_ids {
                filter = filter.add(application::Column::ApplicationId.is_in(ids));
            }
            if let Some(after) = after {
                filter = filter.add(application::Column::ApplicationId.gt(after));
            }
            let rows = application::Entity::find()
                .secure()
                .scope_with(&scope())
                .filter(filter)
                .order_by(application::Column::ApplicationId, Order::Asc)
                .limit(limit)
                .all(&conn)
                .await?;
            Ok(rows
                .into_iter()
                .map(application_from_row)
                .collect::<Result<Vec<_>, _>>()?)
        })
        .await
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "repo_tests.rs"]
mod tests;
