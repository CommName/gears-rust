//! Stateless registry repository over the secure ORM wrappers (DESIGN §3.2, §3.7).
//!
//! Every operation takes the `AccessScope` of the permit. The registry is platform-wide
//! (ADR 0007), so callers pass the allow-all scope; a scope that is not allow-all makes
//! the repository fails closed: `upsert` returns `Unavailable` and writes nothing, and
//! `get`, `list` and `delete` see and change nothing. Failures map to `RegistryError`.
//! Each failed operation emits one `registry_storage_failed` warning carrying only the
//! operation and a fixed cause; nothing from the row or the database error is logged or
//! returned.
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, DbErr, EntityTrait, QueryFilter, Set};
use time::OffsetDateTime;
use toolkit_db::secure::{
    AccessScope, DBRunner, ScopeError, SecureDeleteExt, SecureEntityExt, SecureInsertExt,
};

use crate::domain::ports::RegistryError;
use crate::domain::registry::{LastResult, RegistryEntry};
use crate::infra::storage::entity::{ActiveModel, Column, Entity, Model};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Result of a delete.
pub enum DeleteOutcome {
    /// A row was removed.
    Deleted,
    /// No row matched.
    NotFound,
}

/// Registry queries.
pub struct RegistryRepo;

/// Fixed cause of a failed registry operation, the only detail a failure log carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FailureCause {
    /// No connection could be acquired or the connection failed.
    Connection,
    /// The statement failed.
    Statement,
    /// A stored row is not a valid entry.
    RowUnreadable,
    /// The access scope was refused.
    ScopeRefused,
    /// The operation timeout expired.
    TimedOut,
}

impl FailureCause {
    fn as_str(self) -> &'static str {
        match self {
            Self::Connection => "connection",
            Self::Statement => "statement",
            Self::RowUnreadable => "row_unreadable",
            Self::ScopeRefused => "scope_refused",
            Self::TimedOut => "timed_out",
        }
    }
}

/// Emits the one `registry_storage_failed` event of a failed operation. `op` is one of
/// `get`, `list`, `upsert`, `delete`; never pass error text, ids or row content.
pub(crate) fn log_failure(op: &'static str, cause: FailureCause) {
    tracing::warn!(op, cause = cause.as_str(), "registry_storage_failed");
}

fn fail(op: &'static str, cause: FailureCause) -> RegistryError {
    log_failure(op, cause);
    RegistryError::Unavailable
}

fn scope_failure(op: &'static str, e: &ScopeError) -> RegistryError {
    let cause = match e {
        ScopeError::Db(DbErr::ConnectionAcquire(_) | DbErr::Conn(_)) => FailureCause::Connection,
        ScopeError::Db(_) => FailureCause::Statement,
        _ => FailureCause::ScopeRefused,
    };
    fail(op, cause)
}

fn to_entry(op: &'static str, row: Model) -> Result<RegistryEntry, RegistryError> {
    let result = LastResult::from_token(&row.last_result)
        .ok_or_else(|| fail(op, FailureCause::RowUnreadable))?;
    RegistryEntry::new(
        row.application_id,
        row.version,
        result,
        std::time::SystemTime::from(row.last_seeded_at),
        row.audit_id,
    )
    .map_err(|_| fail(op, FailureCause::RowUnreadable))
}

impl RegistryRepo {
    /// Returns the entry with the id, if any. An id containing NUL cannot be stored, so it
    /// is never found.
    ///
    /// # Errors
    ///
    /// `Unavailable` on a database failure or a stored row that is not a valid entry.
    pub async fn get(
        runner: &impl DBRunner,
        scope: &AccessScope,
        application_id: &str,
    ) -> Result<Option<RegistryEntry>, RegistryError> {
        if application_id.contains('\0') {
            return Ok(None);
        }
        let row = Entity::find_by_id(application_id.to_owned())
            .secure()
            .scope_with(scope)
            .one(runner)
            .await
            .map_err(|e| scope_failure("get", &e))?;
        row.map(|r| to_entry("get", r)).transpose()
    }

    /// Creates or replaces the entry with a single `INSERT .. ON CONFLICT DO UPDATE`.
    ///
    /// # Errors
    ///
    /// `Unavailable` on a database failure or when `scope` is not the allow-all scope.
    pub async fn upsert(
        runner: &impl DBRunner,
        scope: &AccessScope,
        entry: &RegistryEntry,
    ) -> Result<(), RegistryError> {
        if !scope.is_unconstrained() {
            return Err(fail("upsert", FailureCause::ScopeRefused));
        }
        let am = ActiveModel {
            application_id: Set(entry.application_id().to_owned()),
            version: Set(entry.version().to_owned()),
            last_result: Set(entry.last_result().as_token().to_owned()),
            last_seeded_at: Set(OffsetDateTime::from(entry.last_seeded_at())),
            audit_id: Set(entry.audit_id().to_owned()),
        };
        let on_conflict = OnConflict::column(Column::ApplicationId)
            .update_columns([
                Column::Version,
                Column::LastResult,
                Column::LastSeededAt,
                Column::AuditId,
            ])
            .to_owned();
        Entity::insert(am.clone())
            .secure()
            .scope_with_model(scope, &am)
            .map_err(|e| scope_failure("upsert", &e))?
            .on_conflict_raw(on_conflict)
            .exec(runner)
            .await
            .map_err(|e| scope_failure("upsert", &e))?;
        Ok(())
    }

    /// Deletes the entry with the id.
    ///
    /// # Errors
    ///
    /// `Unavailable` on a database failure.
    pub async fn delete(
        runner: &impl DBRunner,
        scope: &AccessScope,
        application_id: &str,
    ) -> Result<DeleteOutcome, RegistryError> {
        if application_id.contains('\0') {
            return Ok(DeleteOutcome::NotFound);
        }
        let result = Entity::delete_many()
            .filter(Column::ApplicationId.eq(application_id.to_owned()))
            .secure()
            .scope_with(scope)
            .exec(runner)
            .await
            .map_err(|e| scope_failure("delete", &e))?;
        Ok(if result.rows_affected > 0 {
            DeleteOutcome::Deleted
        } else {
            DeleteOutcome::NotFound
        })
    }

    /// Returns every entry, unordered and unpaginated.
    ///
    /// # Errors
    ///
    /// `Unavailable` on a database failure or a stored row that is not a valid entry.
    pub async fn list(
        runner: &impl DBRunner,
        scope: &AccessScope,
    ) -> Result<Vec<RegistryEntry>, RegistryError> {
        let rows = Entity::find()
            .secure()
            .scope_with(scope)
            .all(runner)
            .await
            .map_err(|e| scope_failure("list", &e))?;
        rows.into_iter().map(|r| to_entry("list", r)).collect()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "repo_tests.rs"]
mod repo_tests;
