//! The four platform-plane actions the allowed callers grant (DESIGN 3.3).
//!
//! The same names are the `allowed_callers` setting values, so the
//! configuration and the checks cannot drift.

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
