//! Ports the domain calls; `infra` implements them.

use std::time::Duration;

use application_manager_sdk::{
    CallerIdentity, Envelope, OutcomeReport, RequestCategory, SectionOutcome, SectionPluginError,
};
use async_trait::async_trait;
use toolkit_macros::domain_model;
use uuid::Uuid;

use crate::domain::history::HistoryEvent;
use crate::domain::operation::{
    Accepted, ApplicationRecord, NewOperation, OpenOperation, Operation,
};
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
    /// connection error, any other database error, or a stored row that does
    /// not map back (logged as a defect).
    ///
    /// A write may still have committed: when the timeout fires during
    /// `COMMIT`, the outcome is unknown to the caller and the transaction may
    /// have taken effect.
    #[error("storage cannot answer")]
    Unavailable,
    /// The open-operation unique index rejected the insert.
    #[error("open-operation index violated")]
    IndexViolated,
    /// The guard matched no open record before its run deadline: the
    /// operation is final or expired, and the first final write won.
    #[error("operation is final")]
    OperationFinal,
}

/// Builds the refused event once the winner of a lost index race is known.
///
/// `Fn`, not `FnOnce`: a transaction restarted on contention builds the event
/// again.
pub type BuildRefused = Box<dyn Fn(Uuid) -> HistoryEvent + Send + Sync>;

/// The only writer of operation records, the operation history, and
/// application records (`cpt-cf-application-manager-component-operation-repository`).
///
/// Every method is bounded by `storage_call_timeout`. Every transaction reads
/// the database time first and uses it for its guards and timestamps (DESIGN
/// 3.1 "Status guard"). The history has insert methods only.
#[async_trait]
pub trait OperationRepository: Send + Sync {
    /// The open (`accepted` or `running`) operation of `application_id`;
    /// `expired` is `run_deadline` at or before the database time.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn open_operation(
        &self,
        application_id: &str,
    ) -> Result<Option<OpenOperation>, StorageError>;

    /// The acceptance transaction: run the expiry completion on an expired
    /// open operation of the same application, insert the operation with
    /// `accepted_at` the database time and `run_deadline` that time plus
    /// `new.run_window`, and insert the accepted event.
    ///
    /// # Errors
    /// [`StorageError::IndexViolated`] when another open operation of the
    /// application exists; [`StorageError::Unavailable`].
    async fn accept(
        &self,
        new: NewOperation,
        accepted: HistoryEvent,
    ) -> Result<Accepted, StorageError>;

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
    async fn record_refused(&self, event: HistoryEvent) -> Result<(), StorageError>;

    /// Mark running: the guarded update of an `accepted` operation to
    /// `running`, setting `started_at`, and writing one report entry per
    /// plugin in priority order with its key and, for a seed section, its
    /// section type. Returns the time left before the run deadline.
    ///
    /// # Errors
    /// [`StorageError::OperationFinal`]; [`StorageError::Unavailable`].
    async fn mark_running(
        &self,
        operation_id: Uuid,
        sections: &[(String, Option<String>)],
    ) -> Result<Duration, StorageError>;

    /// The guarded write of one section outcome into the report entry of
    /// `key`, leaving every other entry as it is.
    ///
    /// # Errors
    /// [`StorageError::OperationFinal`], also when the report has no entry for
    /// `key`; [`StorageError::Unavailable`].
    async fn record_section_outcome(
        &self,
        operation_id: Uuid,
        key: &str,
        outcome: SectionOutcome,
        detail: Option<String>,
        location: Option<String>,
    ) -> Result<(), StorageError>;

    /// The final transaction: the guarded update to `completed` with
    /// `category`, `finished_at` and `report` (whose category is set to
    /// `category`), the finished event, and the application record: upserted
    /// for a seed that was `running`, deleted for an uninstall that succeeded.
    ///
    /// # Errors
    /// [`StorageError::OperationFinal`]; [`StorageError::Unavailable`].
    async fn finish(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        category: RequestCategory,
    ) -> Result<(), StorageError>;

    /// The supervisor completion: the guarded update to `completed` as
    /// internal failure with `not_dispatched` for every plugin without an
    /// outcome, the finished event, and, for a seed that was `running`, the
    /// application record.
    ///
    /// # Errors
    /// [`StorageError::OperationFinal`]; [`StorageError::Unavailable`].
    async fn complete_by_supervisor(&self, operation_id: Uuid) -> Result<(), StorageError>;

    /// The expiry completion, when the operation is open and its run deadline
    /// is at or before the database time: `completed` with the category of
    /// its section outcomes, `lost` for the first plugin without an outcome of
    /// a `running` operation and `not_dispatched` for the rest, the finished
    /// event with the reason `expired`, and, for a seed that was `running`,
    /// the application record. Returns whether it completed the operation.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn expire_if_expired(&self, operation_id: Uuid) -> Result<bool, StorageError>;

    /// The expiry completion of up to `limit` expired open operations, oldest
    /// deadline first, each in its own bounded transaction; `limit` is capped
    /// at `MAINTENANCE_BATCH_SIZE`. A record that fails is skipped and the
    /// rest go on. Returns how many were completed.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`] when the listing fails or every attempted
    /// record failed.
    async fn expire_batch(&self, limit: u64) -> Result<u64, StorageError>;

    /// Delete up to `limit` completed operations whose `finished_at` plus
    /// `retention` is at or before the database time, oldest first; `limit` is
    /// capped at `MAINTENANCE_BATCH_SIZE`. Never touches an open record, the
    /// history, or the application records. Returns how many were deleted.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn purge(&self, retention: Duration, limit: u64) -> Result<u64, StorageError>;

    /// Run the expiry completion on an expired record, then read it.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn read_operation(&self, operation_id: Uuid) -> Result<Option<Operation>, StorageError>;

    /// One page of history events in `event_id` order, after `after`, at most
    /// `limit` (capped at `HISTORY_PAGE_MAX_LIMIT`), optionally only those
    /// whose `correlation_id` is in `correlation_ids`.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn history_page(
        &self,
        correlation_ids: Option<Vec<Uuid>>,
        after: Option<Uuid>,
        limit: u64,
    ) -> Result<Vec<HistoryEvent>, StorageError>;

    /// One page of application records in `application_id` order, after
    /// `after`, at most `limit` (capped at `APPLICATION_PAGE_MAX_LIMIT`),
    /// optionally only those whose id is in `application_ids`.
    ///
    /// # Errors
    /// [`StorageError::Unavailable`].
    async fn applications_page(
        &self,
        application_ids: Option<Vec<String>>,
        after: Option<String>,
        limit: u64,
    ) -> Result<Vec<ApplicationRecord>, StorageError>;
}
