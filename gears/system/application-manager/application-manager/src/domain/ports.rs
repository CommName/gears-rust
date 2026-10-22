//! Ports the domain calls; `infra` implements them.

use application_manager_sdk::{CallerIdentity, Envelope, SectionPluginError};
use application_manager_sdk::{
    CallerIdentity, OutcomeReport, RequestCategory, SectionPluginError, SectionSchema,
};
use async_trait::async_trait;
use time::OffsetDateTime;
use toolkit_macros::domain_model;
use uuid::Uuid;

use toolkit_security::SecurityContext;
use uuid::Uuid;

use crate::domain::actions::Action;
use crate::domain::audit::AuditEvent;
use crate::domain::operation::{NewOperation, OpenOperation, Operation};
use crate::domain::plugin_set::SectionPlugin;

/// Why a bounded plugin call produced no plugin answer
/// (`cpt-cf-application-manager-component-plugin-call-guard`).
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CallFailure {
    /// The plugin's in-flight cap was full; the plugin was not called.
    #[error("not_sent")]
    NotSent,
    /// The call was dropped at `plugin_call_timeout`; its effect is unknown.
    #[error("timed_out")]
    TimedOut,
    /// The call panicked.
    #[error("panicked: {0}")]
    Panicked(String),
}

/// What one bounded plugin call gave: the plugin's own answer, unchanged, or
/// why there was none.
pub type CallResult = Result<Result<(), SectionPluginError>, CallFailure>;

/// Section plugin calls, each bounded by the plugin call guard.
#[async_trait]
pub trait PluginCalls: Send + Sync {
    /// Apply `section` with `envelope` through `plugin` on behalf of
    /// `caller`, within operation `operation_id`.
    async fn apply(
        &self,
        plugin: &SectionPlugin,
        caller: &CallerIdentity,
        operation_id: Uuid,
        envelope: &Envelope,
        section: &serde_json::Value,
    ) -> CallResult;

    /// Remove the application `application_id` through `plugin` on behalf of
    /// `caller`, within operation `operation_id`.
    async fn remove(
        &self,
        plugin: &SectionPlugin,
        caller: &CallerIdentity,
        operation_id: Uuid,
        application_id: &str,
    ) -> CallResult;
}

/// Why a storage step produced no result.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    /// Storage cannot answer: a timeout at `storage_call_timeout`, a
    /// connection error, or any other database error.
    ///
    /// A write may still have committed: when the timeout fires during
    /// `COMMIT`, the outcome is unknown to the caller and the transaction may
    /// have taken effect.
    #[error("storage cannot answer")]
    Unavailable,
    /// The open-operation unique index rejected the insert.
    #[error("open-operation index violated")]
    IndexViolated,
    /// The guard matched no open, unexpired record: the operation is final
    /// (or past its deadline), and the first final write won.
    #[error("operation is final")]
    OperationFinal,
    /// The finished event passed to a completing write does not belong to the
    /// operation: its id is not the record's `finished_event_id`, its
    /// correlation id is not the operation id, its action is not the record's,
    /// or it is not a finished event. A caller bug; nothing was written.
    #[error("finished event does not belong to the operation")]
    Inconsistent,
}

/// Builds the refused event once the winner of a lost index race is known.
///
/// `Fn`, not `FnOnce`: a transaction restarted on contention builds the event
/// again.
pub type BuildRefused = Box<dyn Fn(Uuid) -> AuditEvent + Send + Sync>;

/// The only writer of operation records and of the audit ledger
/// (`cpt-cf-application-manager-component-operation-repository`). Every
/// method is bounded by `storage_call_timeout`. The ledger has insert methods
/// only.
#[async_trait]
pub trait OperationRepository: Send + Sync {
    /// The open (`accepted` or `running`) operation of `application_id`;
    /// `expired` is `run_deadline <= now`.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn open_operation(
        &self,
        application_id: &str,
        now: OffsetDateTime,
    ) -> Result<Option<OpenOperation>, StorageError>;

    /// One transaction: interrupt an expired open operation of the same
    /// application, insert the operation, insert the accepted event.
    ///
    /// # Errors
    /// [`StorageError::IndexViolated`] when another open operation of the
    /// application exists; [`StorageError::Unavailable`].
    async fn accept(
        &self,
        new: NewOperation,
        accepted: AuditEvent,
        now: OffsetDateTime,
    ) -> Result<(), StorageError>;

    /// One transaction: read the open winner of `application_id`, then insert
    /// the refused event `build_refused` makes for it. Returns the winner.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`], including when no open winner exists.
    async fn refuse_after_lost_race(
        &self,
        application_id: &str,
        build_refused: BuildRefused,
    ) -> Result<Uuid, StorageError>;

    /// Insert a refused event in its own transaction.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn record_refused(&self, event: AuditEvent) -> Result<(), StorageError>;

    /// Guarded update to `running`, setting `started_at`.
    ///
    /// # Errors
    /// [`StorageError::OperationFinal`]; [`StorageError::Unavailable`].
    async fn mark_running(
        &self,
        operation_id: Uuid,
        now: OffsetDateTime,
    ) -> Result<(), StorageError>;

    /// Guarded update of the report.
    ///
    /// # Errors
    /// [`StorageError::OperationFinal`]; [`StorageError::Unavailable`].
    async fn record_report(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        now: OffsetDateTime,
    ) -> Result<(), StorageError>;

    /// One transaction: guarded update to `completed` with `category` and
    /// `finished_at`, then the finished event. A guard miss inserts no event.
    /// The stored report carries `category` and `interrupted = false`.
    ///
    /// # Errors
    /// [`StorageError::OperationFinal`]; [`StorageError::Inconsistent`] when
    /// `finished` does not belong to the record; [`StorageError::Unavailable`].
    async fn finish(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        category: RequestCategory,
        finished: AuditEvent,
        now: OffsetDateTime,
    ) -> Result<(), StorageError>;

    /// One transaction: guarded update straight to `completed` as internal
    /// failure, setting `started_at` if null and `finished_at`, then the
    /// finished event. The stored report carries the `internal_failure`
    /// category and `interrupted = false`.
    ///
    /// # Errors
    /// [`StorageError::OperationFinal`]; [`StorageError::Inconsistent`] when
    /// `finished` does not belong to the record; [`StorageError::Unavailable`].
    async fn complete_by_supervisor(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        finished: AuditEvent,
        now: OffsetDateTime,
    ) -> Result<(), StorageError>;

    /// The interruption step: when the record is open and expired, set
    /// `interrupted` and `finished_at`, keep the report with `interrupted =
    /// true`, and insert the finished event under the record's
    /// `finished_event_id`. Returns whether the record was interrupted.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn interrupt_if_expired(
        &self,
        operation_id: Uuid,
        now: OffsetDateTime,
    ) -> Result<bool, StorageError>;

    /// The interruption step for up to `limit` expired open records, oldest
    /// deadline first; the implementation caps `limit` at a bind-safe maximum.
    /// A record that fails is skipped and the rest go on. Returns how many were
    /// interrupted.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`] when the listing fails or every attempted
    /// record failed.
    async fn interrupt_expired_batch(
        &self,
        now: OffsetDateTime,
        limit: u64,
    ) -> Result<u64, StorageError>;

    /// Delete up to `limit` final records that finished before `before`,
    /// oldest first; the implementation caps `limit` at a bind-safe maximum.
    /// Never touches an open record or the ledger. Returns how
    /// many were deleted.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn purge_finished(&self, before: OffsetDateTime, limit: u64)
    -> Result<u64, StorageError>;

    /// Run the interruption step on an expired record, then read it.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn read_operation(
        &self,
        operation_id: Uuid,
        now: OffsetDateTime,
    ) -> Result<Option<Operation>, StorageError>;

    /// The events of one request in the order they were written: by write time,
    /// then by event id.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn audit_events(&self, correlation_id: Uuid) -> Result<Vec<AuditEvent>, StorageError>;
}
