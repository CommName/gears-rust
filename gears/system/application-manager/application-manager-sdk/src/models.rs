//! Models passed to section plugins.

use serde::{Deserialize, Serialize};
use toolkit_macros::domain_model;
use toolkit_security::{PlatformIdentity, PlatformSecurityContext};

/// The caller as a plugin sees it: the validated platform identity. It holds
/// no credential.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallerIdentity(PlatformIdentity);

impl CallerIdentity {
    /// Copy the validated identity from a platform security context.
    #[must_use]
    pub fn from_platform_context(ctx: &PlatformSecurityContext) -> Self {
        Self(ctx.identity().clone())
    }

    /// The validated platform identity.
    #[must_use]
    pub fn identity(&self) -> &PlatformIdentity {
        &self.0
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

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "models_tests.rs"]
mod tests;
