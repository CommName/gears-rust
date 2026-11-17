//! Read service (`cpt-cf-application-manager-component-read-service`): reads
//! record no history event and decide no outcome category. The
//! platform-plane reads are decided by the allowed callers, the tenant-plane
//! reads by the PDP (ADR 0004); no read calls a section plugin.

use std::fmt;
use std::sync::Arc;

use toolkit_macros::domain_model;
use toolkit_odata::{ODataQuery, Page, PageInfo};
use toolkit_security::{PlatformSecurityContext, SecurityContext};

use crate::constants::{APPLICATION_PAGE_MAX_LIMIT, HISTORY_PAGE_MAX_LIMIT};
use crate::domain::actions::{CallerAction, TenantAction};
use crate::domain::allowed_callers::{AllowedCallers, identity_fields};
use crate::domain::error::DomainError;
use crate::domain::history::HistoryEvent;
use crate::domain::listing::{ApplicationFilterField, HistoryFilterField, ListQuery};
use crate::domain::operation::{ApplicationRecord, Operation};
use crate::domain::plugin_set::StartupState;
use crate::domain::ports::{AccessControl, AccessError, OperationRepository};
use crate::domain::schema_validator::{CompiledSection, SchemaValidator};
use crate::domain::signals::{NoSignals, Signals};

/// The combined schema of a whole manifest with every section type.
#[domain_model]
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestSchema {
    /// The envelope and one required section per key (Draft-07).
    pub schema: serde_json::Value,
    /// Each section key with its exact section type, in priority order.
    pub sections: Vec<(String, String)>,
}

/// Serves the discovery reads from memory, and the operation, history and
/// application reads from storage.
#[domain_model]
pub struct ReadService {
    allowed: Arc<AllowedCallers>,
    startup: Arc<StartupState>,
    validator: Arc<SchemaValidator>,
    operations: Arc<dyn OperationRepository>,
    access: Arc<dyn AccessControl>,
    signals: Arc<dyn Signals>,
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
        access: Arc<dyn AccessControl>,
    ) -> Self {
        Self {
            allowed,
            startup,
            validator,
            operations,
            access,
            signals: Arc::new(NoSignals),
        }
    }

    /// The same service, raising its signals on `signals`.
    #[must_use]
    pub fn with_signals(mut self, signals: Arc<dyn Signals>) -> Self {
        self.signals = signals;
        self
    }

    /// One page of history events in `event_id` order, when the PDP permits
    /// `read_history` (DESIGN 3.3). The `$filter` names operation ids, matched
    /// against the correlation id, so a refusal's request id matches too; an
    /// unknown id gives an empty page.
    ///
    /// # Errors
    /// [`DomainError::AccessDenied`] or [`DomainError::AccessUnavailable`]
    /// before any storage read; [`DomainError::InvalidQuery`] for an option
    /// the read does not serve; [`DomainError::StorageUnavailable`].
    pub async fn read_history(
        &self,
        ctx: &SecurityContext,
        query: &ODataQuery,
    ) -> Result<Page<HistoryEvent>, DomainError> {
        self.decide(ctx, TenantAction::ReadHistory).await?;
        let list = ListQuery::<uuid::Uuid>::from_odata::<HistoryFilterField>(
            query,
            "event_id",
            HISTORY_PAGE_MAX_LIMIT,
        )?;
        let items = self
            .operations
            .history_page(list.ids.clone(), list.after, list.limit)
            .await
            .map_err(|_| self.storage_unavailable())?;
        let next_cursor = list.next_cursor(&items, |event| &event.event_id)?;
        Ok(page(items, next_cursor, list.limit))
    }

    /// One page of application records in `application_id` order, when the
    /// PDP permits `read_applications` (DESIGN 3.3).
    ///
    /// # Errors
    /// As [`Self::read_history`].
    pub async fn list_applications(
        &self,
        ctx: &SecurityContext,
        query: &ODataQuery,
    ) -> Result<Page<ApplicationRecord>, DomainError> {
        self.decide(ctx, TenantAction::ReadApplications).await?;
        let list = ListQuery::<String>::from_odata::<ApplicationFilterField>(
            query,
            "application_id",
            APPLICATION_PAGE_MAX_LIMIT,
        )?;
        let items = self
            .operations
            .applications_page(list.ids.clone(), list.after.clone(), list.limit)
            .await
            .map_err(|_| self.storage_unavailable())?;
        let next_cursor = list.next_cursor(&items, |record| &record.application_id)?;
        Ok(page(items, next_cursor, list.limit))
    }

    /// The PDP decision of a tenant-plane read.
    async fn decide(&self, ctx: &SecurityContext, action: TenantAction) -> Result<(), DomainError> {
        match self.access.authorize(ctx, action).await {
            Ok(()) => Ok(()),
            Err(AccessError::NotPermitted) => {
                self.signals.caller_refused(action.as_str());
                Err(DomainError::AccessDenied { action })
            }
            Err(AccessError::Unavailable) => {
                self.signals.dependency_unavailable("pdp");
                Err(DomainError::AccessUnavailable)
            }
        }
    }

    fn storage_unavailable(&self) -> DomainError {
        self.signals.dependency_unavailable("storage");
        DomainError::StorageUnavailable
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
            Err(_) => Err(self.storage_unavailable()),
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
        self.signals.caller_refused(action.as_str());
        Err(DomainError::NotPermitted { action })
    }
}

fn page<T>(items: Vec<T>, next_cursor: Option<String>, limit: u64) -> Page<T> {
    Page::new(
        items,
        PageInfo {
            next_cursor,
            prev_cursor: None,
            limit,
        },
    )
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "read_service_tests.rs"]
mod tests;
