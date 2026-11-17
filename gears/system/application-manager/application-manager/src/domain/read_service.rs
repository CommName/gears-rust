//! Read service (`cpt-cf-application-manager-component-read-service`): reads
//! record no history event and decide no outcome category.

use std::fmt;
use std::sync::Arc;

use toolkit_macros::domain_model;
use toolkit_security::PlatformSecurityContext;

use crate::domain::actions::CallerAction;
use crate::domain::allowed_callers::{AllowedCallers, identity_fields};
use crate::domain::error::DomainError;
use crate::domain::plugin_set::StartupState;

/// Serves the discovery reads.
#[domain_model]
pub struct ReadService {
    allowed: Arc<AllowedCallers>,
    startup: Arc<StartupState>,
}

impl fmt::Debug for ReadService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadService")
            .field("startup", &self.startup)
            .finish_non_exhaustive()
    }
}

impl ReadService {
    /// A read service over the startup result.
    #[must_use]
    pub fn new(allowed: Arc<AllowedCallers>, startup: Arc<StartupState>) -> Self {
        Self { allowed, startup }
    }

    /// The section keys in priority order, from memory, when the allowed
    /// callers grant `read_schema`. Calls no section plugin.
    ///
    /// # Errors
    /// [`DomainError::NotReady`] before startup ends, with no caller check;
    /// [`DomainError::NotPermitted`] when the caller is not allowed.
    pub fn list_section_keys(
        &self,
        ctx: &PlatformSecurityContext,
    ) -> Result<Vec<String>, DomainError> {
        let set = self.startup.plugin_set().ok_or(DomainError::NotReady)?;
        self.check(ctx, CallerAction::ReadSchema)?;
        Ok(set.keys())
    }

    fn check(
        &self,
        ctx: &PlatformSecurityContext,
        action: CallerAction,
    ) -> Result<(), DomainError> {
        if self.allowed.allows(ctx.identity(), action) {
            return Ok(());
        }
        let (kind, domain, name) = identity_fields(ctx.identity());
        tracing::info!(
            action = action.as_str(),
            identity_kind = kind,
            identity_domain = domain,
            identity_name = name,
            "application-manager: caller not allowed"
        );
        Err(DomainError::NotPermitted { action })
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "read_service_tests.rs"]
mod tests;
