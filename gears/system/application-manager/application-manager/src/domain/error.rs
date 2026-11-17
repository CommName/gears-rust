//! Domain errors; `api::rest::error` maps them to canonical errors.

use toolkit_macros::domain_model;

use crate::domain::actions::CallerAction;

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
}
