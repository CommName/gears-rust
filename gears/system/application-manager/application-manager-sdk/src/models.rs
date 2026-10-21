//! Models passed to section plugins.

use serde::{Deserialize, Serialize};
use toolkit_macros::domain_model;
use toolkit_security::{SecurityContext, SecurityContextBuildError};
use uuid::Uuid;

/// Why a [`CallerIdentity`] cannot become a security context.
///
/// `#[non_exhaustive]` allows additive changes within v1.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CallerIdentityError {
    /// The subject id or the subject tenant id is nil, as for an anonymous
    /// caller. Every route is authenticated, so the identity is refused.
    #[error("anonymous caller identity")]
    Anonymous,
    /// The security context builder refused the identity.
    #[error("security context build failed: {0}")]
    Build(#[from] SecurityContextBuildError),
}

/// The caller as a plugin sees it. It has no token field, so it cannot carry
/// a token.
///
/// `#[non_exhaustive]` allows additive changes within v1; outside this crate
/// it is built with [`CallerIdentity::from_security_context`].
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CallerIdentity {
    /// Subject id.
    pub subject_id: Uuid,
    /// Subject type, when known.
    pub subject_type: Option<String>,
    /// Tenant of the subject.
    pub subject_tenant_id: Uuid,
    /// Scopes of the caller's token.
    pub token_scopes: Vec<String>,
}

impl CallerIdentity {
    /// Copy the identity from a security context, dropping the bearer token.
    #[must_use]
    pub fn from_security_context(ctx: &SecurityContext) -> Self {
        Self {
            subject_id: ctx.subject_id(),
            subject_type: ctx.subject_type().map(str::to_owned),
            subject_tenant_id: ctx.subject_tenant_id(),
            token_scopes: ctx.token_scopes().to_vec(),
        }
    }

    /// Build a token-less security context for the plugin's own enforcer check.
    ///
    /// Fails closed: an identity with a nil subject id or nil subject tenant
    /// id (what an anonymous context copies to) is refused, so the result
    /// never looks authenticated when the caller was not.
    ///
    /// # Errors
    /// [`CallerIdentityError::Anonymous`] for a nil subject id or subject
    /// tenant id; [`CallerIdentityError::Build`] propagates the builder error.
    pub fn to_security_context(&self) -> Result<SecurityContext, CallerIdentityError> {
        if self.subject_id.is_nil() || self.subject_tenant_id.is_nil() {
            return Err(CallerIdentityError::Anonymous);
        }
        let mut builder = SecurityContext::builder()
            .subject_id(self.subject_id)
            .subject_tenant_id(self.subject_tenant_id)
            .token_scopes(self.token_scopes.clone());
        if let Some(subject_type) = &self.subject_type {
            builder = builder.subject_type(subject_type);
        }
        Ok(builder.build()?)
    }
}

/// The manifest's `application` object; the content is opaque to the gear.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// Application id.
    pub id: String,
    /// Application version.
    pub version: String,
}

/// A section schema and its version.
///
/// The schema is the raw JSON document, not a compiled validator, on purpose.
/// The gear does not trust what a plugin serves: on every seed and schema
/// read it checks the raw document itself (Draft 2020-12 dialect, no `$ref`
/// outside the document, at most [`SERVED_SCHEMA_MAX_BYTES`]), then compiles
/// it with `jsonschema` under its own limits and validates the section. A
/// schema that fails any of these is reported as `unusable`. Plugins
/// therefore never need `jsonschema`, and the type stays plain data that can
/// be cloned and serialized.
///
/// `#[non_exhaustive]` allows additive changes within v1; plugins build it
/// with [`SectionSchema::new`].
///
/// [`SERVED_SCHEMA_MAX_BYTES`]: crate::limits::SERVED_SCHEMA_MAX_BYTES
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SectionSchema {
    /// Self-contained JSON Schema Draft 2020-12 document, as served. It is
    /// checked and compiled by the gear, not here.
    pub schema: serde_json::Value,
    /// Changes whenever the schema content changes.
    pub schema_version: String,
}

impl SectionSchema {
    /// A schema with its version.
    #[must_use]
    pub fn new(schema: serde_json::Value, schema_version: impl Into<String>) -> Self {
        Self {
            schema,
            schema_version: schema_version.into(),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "models_tests.rs"]
mod tests;
