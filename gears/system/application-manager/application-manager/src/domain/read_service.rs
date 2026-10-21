//! Read service (`cpt-cf-application-manager-component-read-service`): reads
//! record no audit event and decide no outcome category.

use std::fmt;
use std::sync::Arc;

use toolkit_macros::domain_model;
use toolkit_security::SecurityContext;

use crate::domain::actions::Action;
use crate::domain::error::DomainError;
use crate::domain::plugin_set::StartupState;
use crate::domain::ports::{AccessControl, AccessError};

/// Serves the discovery reads.
#[domain_model]
pub struct ReadService {
    access: Arc<dyn AccessControl>,
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
    pub fn new(access: Arc<dyn AccessControl>, startup: Arc<StartupState>) -> Self {
        Self { access, startup }
    }

    /// The section keys in priority order, from memory, after a
    /// `read_schema` permit. Calls no section plugin.
    ///
    /// # Errors
    /// [`DomainError::NotReady`] before startup ends, with no access check;
    /// [`DomainError::AccessUnavailable`] or [`DomainError::NotPermitted`]
    /// from access control.
    pub async fn list_section_keys(
        &self,
        ctx: &SecurityContext,
    ) -> Result<Vec<String>, DomainError> {
        let set = self.startup.plugin_set().ok_or(DomainError::NotReady)?;
        authorize(self.access.as_ref(), ctx, Action::ReadSchema).await?;
        Ok(set.keys())
    }
}

async fn authorize(
    access: &dyn AccessControl,
    ctx: &SecurityContext,
    action: Action,
) -> Result<(), DomainError> {
    access.authorize(ctx, action).await.map_err(|e| match e {
        AccessError::Unavailable => DomainError::AccessUnavailable,
        AccessError::NotPermitted => DomainError::NotPermitted { action },
    })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "read_service_tests.rs"]
mod tests;
