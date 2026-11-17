//! Domain errors; `api::rest::error` maps them to canonical errors.

use toolkit_macros::domain_model;
use uuid::Uuid;

use crate::domain::actions::CallerAction;
use crate::domain::schema_validator::Problem;

/// A refused request, by outcome rank (DESIGN 3.3), or a refused read.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    /// Startup has not finished, or `stop` has begun (rank 1).
    #[error("not ready")]
    NotReady,
    /// No request-task permit is free (rank 1).
    #[error("at capacity")]
    AtCapacity,
    /// Operation storage cannot answer, for an allowed caller or at
    /// acceptance (rank 1 or the acceptance rank).
    #[error("operation storage cannot answer")]
    StorageUnavailable,
    /// The allowed callers do not grant the action (rank 2).
    #[error("not permitted: {action}")]
    NotPermitted {
        /// The refused action.
        action: CallerAction,
    },
    /// Another operation of the application is open before its run deadline
    /// (rank 3).
    #[error("application busy: operation {operation_id} is open")]
    Conflict {
        /// The open operation.
        operation_id: Uuid,
    },
    /// An envelope problem, or an uninstall id outside the limits (rank 4).
    #[error("invalid manifest")]
    InvalidManifest {
        /// Every problem, with its location and rule.
        problems: Vec<Problem>,
    },
    /// No section plugin owns the requested section key (read: 404).
    #[error("unknown section key")]
    UnknownSectionKey,
    /// The operation id is unknown, malformed, or purged (read: 404).
    #[error("operation not found")]
    OperationNotFound,
    /// The request task failed before it answered: a gear defect (500).
    #[error("internal failure")]
    Defect,
}
