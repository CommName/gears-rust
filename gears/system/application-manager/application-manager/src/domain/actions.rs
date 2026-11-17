//! The four platform-plane actions the allowed callers grant, and the two
//! tenant-plane reads the PDP decides (DESIGN 3.3, ADR 0004).
//!
//! The platform-plane names are the `allowed_callers` setting values, and the
//! tenant-plane names are the permission catalog's actions, so neither the
//! configuration nor the catalog can drift from the checks.

use std::fmt;

use serde::Deserialize;
use toolkit_macros::domain_model;

/// A platform-plane action on the application resource.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallerAction {
    /// Seed: submit a manifest.
    Seed,
    /// Uninstall, in both forms.
    Uninstall,
    /// The three discovery reads.
    ReadSchema,
    /// Read one operation.
    ReadOperation,
}

impl CallerAction {
    /// The action name, as configured and as reported in a refusal.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Seed => "seed",
            Self::Uninstall => "uninstall",
            Self::ReadSchema => "read_schema",
            Self::ReadOperation => "read_operation",
        }
    }
}

impl fmt::Display for CallerAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A tenant-plane read on the application resource, decided by the PDP as a
/// platform-level permission (ADR 0004).
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TenantAction {
    /// Read the operation history.
    ReadHistory,
    /// List the installed applications.
    ReadApplications,
}

impl TenantAction {
    /// Both actions, in catalog order.
    pub const ALL: [Self; 2] = [Self::ReadHistory, Self::ReadApplications];

    /// The action name, as sent to the PDP and declared in the catalog.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadHistory => "read_history",
            Self::ReadApplications => "read_applications",
        }
    }
}

impl fmt::Display for TenantAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
