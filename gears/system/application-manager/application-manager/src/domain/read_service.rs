//! Read service (`cpt-cf-application-manager-component-read-service`): reads
//! record no history event and decide no outcome category.

use std::fmt;
use std::sync::Arc;

use toolkit_macros::domain_model;
use toolkit_security::PlatformSecurityContext;

use crate::domain::actions::CallerAction;
use crate::domain::allowed_callers::{AllowedCallers, identity_fields};
use crate::domain::error::DomainError;
use crate::domain::operation::Operation;
use crate::domain::plugin_set::StartupState;
use crate::domain::ports::OperationRepository;
use crate::domain::schema_validator::{CompiledSection, SchemaValidator};

/// The combined schema of a whole manifest with every section type.
#[domain_model]
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestSchema {
    /// The envelope and one required section per key (Draft-07).
    pub schema: serde_json::Value,
    /// Each section key with its exact section type, in priority order.
    pub sections: Vec<(String, String)>,
}

/// Serves the discovery reads, from memory.
#[domain_model]
pub struct ReadService {
    allowed: Arc<AllowedCallers>,
    startup: Arc<StartupState>,
    validator: Arc<SchemaValidator>,
    operations: Arc<dyn OperationRepository>,
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
    pub fn new(
        allowed: Arc<AllowedCallers>,
        startup: Arc<StartupState>,
        validator: Arc<SchemaValidator>,
        operations: Arc<dyn OperationRepository>,
    ) -> Self {
        Self {
            allowed,
            startup,
            validator,
            operations,
        }
    }

    /// One operation record, after the expiry completion of an expired open
    /// one, when the allowed callers grant `read_operation` (DESIGN 3.6
    /// "Operation read"). Calls no section plugin and records no history
    /// event.
    ///
    /// # Errors
    /// [`DomainError::NotPermitted`] before any storage read;
    /// [`DomainError::OperationNotFound`] for a malformed, unknown, or purged
    /// id; [`DomainError::StorageUnavailable`] when storage cannot answer.
    pub async fn read_operation(
        &self,
        ctx: &PlatformSecurityContext,
        operation_id: &str,
    ) -> Result<Operation, DomainError> {
        self.check(ctx, CallerAction::ReadOperation)?;
        let Ok(operation_id) = uuid::Uuid::parse_str(operation_id) else {
            return Err(DomainError::OperationNotFound);
        };
        match self.operations.read_operation(operation_id).await {
            Ok(Some(operation)) => Ok(operation),
            Ok(None) => Err(DomainError::OperationNotFound),
            Err(_) => Err(DomainError::StorageUnavailable),
        }
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

    /// One section schema, composed under its key, with its section type,
    /// when the allowed callers grant `read_schema`. Calls no section plugin.
    ///
    /// # Errors
    /// [`DomainError::NotReady`] before startup ends, with no caller check;
    /// [`DomainError::NotPermitted`] when the caller is not allowed;
    /// [`DomainError::UnknownSectionKey`] when no plugin owns `key`.
    pub fn section_schema(
        &self,
        ctx: &PlatformSecurityContext,
        key: &str,
    ) -> Result<Arc<CompiledSection>, DomainError> {
        let set = self.startup.plugin_set().ok_or(DomainError::NotReady)?;
        self.check(ctx, CallerAction::ReadSchema)?;
        set.plugin(key)
            .map(|p| Arc::clone(&p.section))
            .ok_or(DomainError::UnknownSectionKey)
    }

    /// The combined schema with every section type, when the allowed callers
    /// grant `read_schema`. Calls no section plugin.
    ///
    /// # Errors
    /// As [`Self::list_section_keys`].
    pub fn manifest_schema(
        &self,
        ctx: &PlatformSecurityContext,
    ) -> Result<ManifestSchema, DomainError> {
        let set = self.startup.plugin_set().ok_or(DomainError::NotReady)?;
        self.check(ctx, CallerAction::ReadSchema)?;
        let sections = set.sections();
        Ok(ManifestSchema {
            schema: self.validator.combined_schema(&sections),
            sections: sections
                .iter()
                .map(|s| (s.key().to_owned(), s.section_type().to_owned()))
                .collect(),
        })
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
