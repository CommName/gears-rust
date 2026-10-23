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
use crate::domain::schema_validator::{CompiledSection, SchemaValidator};
use toolkit_security::SecurityContext;
use uuid::Uuid;

use crate::constants::cut;
use crate::domain::actions::Action;
use crate::domain::error::{DomainError, UnavailableSchema};
use crate::domain::operation::Operation;
use crate::domain::plugin_set::{SectionPlugin, StartupState};
use crate::domain::ports::{AccessControl, AccessError, Clock, OperationRepository};
use crate::domain::router::{SchemaFetch, SectionPluginRouter, UnavailableReason};
use crate::domain::schema_validator::{PreparedSection, SchemaValidator, prepare_section};

/// One live section schema with its schema version.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionSchemaRead {
    /// The section key.
    pub key: String,
    /// The served schema composed under the key.
    pub schema: Value,
    /// The schema version, as served.
    pub schema_version: String,
}

/// The schema version of one section.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionVersion {
    /// The section key.
    pub key: String,
    /// The schema version, as served.
    pub schema_version: String,
}

/// The live combined schema with every schema version.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSchemaRead {
    /// The combined schema of a whole manifest.
    pub schema: Value,
    /// Every section's schema version, in priority order.
    pub sections: Vec<SectionVersion>,
}

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
    router: SectionPluginRouter,
    validator: Arc<SchemaValidator>,
    #[allow(dead_code, reason = "read_operation is implemented in PR 5")]
    operations: Arc<dyn OperationRepository>,
    #[allow(dead_code, reason = "read_operation is implemented in PR 5")]
    clock: Arc<dyn Clock>,
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
    ) -> Self {
        Self {
            allowed,
            startup,
            validator,
        }
    }

    /// The section keys in priority order, from memory, when the allowed
    /// callers grant `read_schema`. Calls no section plugin.
    pub fn new(
        access: Arc<dyn AccessControl>,
        startup: Arc<StartupState>,
        router: SectionPluginRouter,
        validator: Arc<SchemaValidator>,
        operations: Arc<dyn OperationRepository>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            access,
            startup,
            router,
            validator,
            operations,
            clock,
        }
    }

    /// The operation record, after a `read_operation` permit; an expired open
    /// record is interrupted first.
    ///
    /// # Errors
    /// [`DomainError::AccessUnavailable`] or [`DomainError::NotPermitted`]
    /// from access control, with no storage call; then
    /// [`DomainError::OperationNotFound`] for an unknown id and
    /// [`DomainError::StorageUnavailable`].
    pub async fn read_operation(
        &self,
        _ctx: &SecurityContext,
        _operation_id: Uuid,
    ) -> Result<Operation, DomainError> {
        todo!("PR 5: operation read")
    }

    /// The section keys in priority order, from memory, after a
    /// `read_schema` permit. Calls no section plugin.
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

fn not_permitted(key: &str, detail: &str) -> DomainError {
    DomainError::SectionNotPermitted {
        key: key.to_owned(),
        detail: cut(detail, PLUGIN_DETAIL_MAX_BYTES).to_owned(),
    }
}

/// The usable schema `plugin` answered with, checked and compiled.
async fn usable(plugin: &SectionPlugin, fetch: SchemaFetch) -> Result<PreparedSection, NoSchema> {
    match fetch {
        SchemaFetch::Served(served) => {
            prepare_section(&plugin.key, served).await.map_err(|cause| {
                tracing::warn!(
                    key = %plugin.key,
                    cause = cause.as_str(),
                    "application-manager: section schema unusable"
                );
                NoSchema::Unavailable(UnavailableReason::Unusable)
            })
        }
        SchemaFetch::NotPermitted { detail } => Err(NoSchema::NotPermitted(detail)),
        SchemaFetch::Unavailable(reason) => Err(NoSchema::Unavailable(reason)),
    }
}

/// Every schema, in plugin order, or the refusal: unavailable wins over not
/// permitted (DESIGN 3.3).
fn every_usable(
    results: Vec<(&SectionPlugin, Result<PreparedSection, NoSchema>)>,
) -> Result<Vec<PreparedSection>, DomainError> {
    let mut prepared = Vec::with_capacity(results.len());
    let mut unavailable = Vec::new();
    let mut refusal = None;
    for (plugin, result) in results {
        match result {
            Ok(section) => prepared.push(section),
            Err(NoSchema::Unavailable(reason)) => unavailable.push(UnavailableSchema {
                key: plugin.key.clone(),
                reason,
            }),
            Err(NoSchema::NotPermitted(detail)) => {
                refusal.get_or_insert_with(|| not_permitted(&plugin.key, &detail));
            }
        }
    }
    if !unavailable.is_empty() {
        return Err(DomainError::SchemaUnavailable { items: unavailable });
    }
    refusal.map_or(Ok(prepared), Err)
}

async fn authorize(
    access: &dyn AccessControl,
    ctx: &SecurityContext,
    action: Action,
) -> Result<(), DomainError> {
    access
        .authorize(ctx, action, None)
        .await
        .map_err(|e| match e {
            AccessError::Unavailable => DomainError::AccessUnavailable,
            AccessError::NotPermitted => DomainError::NotPermitted { action },
        })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "read_service_tests.rs"]
mod tests;
