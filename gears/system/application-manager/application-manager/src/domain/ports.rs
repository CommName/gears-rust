//! Ports the domain calls; `infra` implements them.

use async_trait::async_trait;
use toolkit_macros::domain_model;
use toolkit_security::SecurityContext;

use crate::domain::actions::Action;

/// Why platform access control did not permit an action.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AccessError {
    /// Access control cannot answer: an evaluation failure or no answer
    /// within `pdp_call_timeout` (outcome rank 1).
    #[error("access control cannot answer")]
    Unavailable,
    /// Denied, a constraint compile failure, or a permit with any constraint
    /// (outcome rank 2).
    #[error("not permitted")]
    NotPermitted,
}

/// Platform access control for the gear-level actions of
/// [`crate::domain::actions`].
#[async_trait]
pub trait AccessControl: Send + Sync {
    /// Decide whether the caller may perform `action`, as a platform-level
    /// permission with no context tenant (ADR 0005).
    ///
    /// # Errors
    /// [`AccessError`] when the action is not permitted or cannot be decided.
    async fn authorize(&self, ctx: &SecurityContext, action: Action) -> Result<(), AccessError>;
}
