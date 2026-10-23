//! Domain errors; `api::rest::error` maps them to canonical errors.

use toolkit_macros::domain_model;
use uuid::Uuid;

use crate::domain::actions::CallerAction;
use crate::domain::actions::Action;
use crate::domain::router::UnavailableReason;
use crate::domain::schema_validator::Problem;

/// A refused request, by outcome rank (DESIGN 3.3).
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    /// Startup has not finished (rank 1).
    #[error("not ready")]
    NotReady,
    /// The allowed callers do not grant the action (rank 2).
    #[error("not permitted: {action}")]
    NotPermitted {
        /// The refused action.
        action: CallerAction,
    },
    /// No section plugin owns the requested section key (read: 404).
    #[error("unknown section key")]
    UnknownSectionKey,
    /// Another operation of the application is open before its run deadline
    /// (rank 3).
    #[error("application busy: operation {operation_id} is open")]
    Conflict {
        /// The open operation.
        operation_id: Uuid,
    },
    /// An envelope problem, an uninstall id outside the limits, or (inside
    /// the operation report only) a section problem (rank 4).
    #[error("invalid manifest")]
    InvalidManifest {
        /// Every problem found.
        problems: Vec<Problem>,
    },
    /// The operation id is unknown, malformed or purged (read: 404).
    #[error("operation not found")]
    OperationNotFound,
    /// No free request-task permit (rank 1).
    #[error("at capacity")]
    AtCapacity,
    /// Storage cannot answer (rank 1 for a permitted caller, or the
    /// acceptance rank; read: 503).
    #[error("storage cannot answer")]
    StorageUnavailable,
    /// The request task failed before it answered (gear defect, 500).
    #[error("gear defect")]
    Defect,
}
