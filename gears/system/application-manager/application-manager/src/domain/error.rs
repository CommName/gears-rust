//! Domain errors; `api::rest::error` maps them to canonical errors.

use toolkit_macros::domain_model;

use crate::domain::actions::Action;

/// A refused request, by outcome rank (DESIGN 3.3).
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainError {
    /// Startup has not finished (rank 1).
    #[error("not ready")]
    NotReady,
    /// Access control cannot answer (rank 1).
    #[error("access control cannot answer")]
    AccessUnavailable,
    /// Platform access control did not permit the action (rank 2).
    #[error("not permitted: {action}")]
    NotPermitted {
        /// The refused action.
        action: Action,
    },
}
