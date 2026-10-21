//! The five gear-level actions platform access control decides (DESIGN 3.3).
//!
//! The same names feed the PDP request and the permission catalog in
//! [`crate::gts::permissions`], so the two cannot drift.

use std::fmt;

use toolkit_macros::domain_model;

/// A gear-level action on the application resource.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Seed: submit a manifest.
    Seed,
    /// Uninstall, in both forms.
    Uninstall,
    /// Read one operation.
    ReadOperation,
    /// The three discovery reads.
    ReadSchema,
    /// Read the audit events of one request.
    ReadAudit,
}

impl Action {
    /// Every action, in DESIGN 3.3 table order.
    pub const ALL: [Self; 5] = [
        Self::Seed,
        Self::Uninstall,
        Self::ReadOperation,
        Self::ReadSchema,
        Self::ReadAudit,
    ];

    /// The action name sent to the PDP and declared in the catalog.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Seed => "seed",
            Self::Uninstall => "uninstall",
            Self::ReadOperation => "read_operation",
            Self::ReadSchema => "read_schema",
            Self::ReadAudit => "read_audit",
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
