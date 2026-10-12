//! SDK error kinds (DESIGN §3.3).

use thiserror::Error;

use crate::models::{SectionOutcome, UnavailableReason};

/// Largest diagnostic detail, in bytes, that the gear keeps from a plugin.
pub const PLUGIN_DETAIL_MAX_BYTES: usize = 1024;

/// Cuts `text` to at most [`PLUGIN_DETAIL_MAX_BYTES`] bytes at a UTF-8 char boundary.
#[must_use]
pub fn truncate_detail(text: &str) -> String {
    if text.len() <= PLUGIN_DETAIL_MAX_BYTES {
        return text.to_owned();
    }
    let mut end = PLUGIN_DETAIL_MAX_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

/// Error returned by a section plugin.
///
/// The constructors cut `detail` and `location` to [`PLUGIN_DETAIL_MAX_BYTES`], so a
/// plugin cannot push an unbounded string into caller-facing bodies or audit.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SectionPluginError {
    /// The plugin rejected the section. The detail is caller-facing: it must not
    /// contain manifest values or secrets.
    #[non_exhaustive]
    #[error("section rejected: {detail}")]
    Rejected {
        /// Why the section was rejected.
        detail: String,
        /// Optional JSON Pointer to the offending value.
        location: Option<String>,
    },
    /// The caller lacks a permission the plugin requires. The detail is
    /// caller-facing: it must not contain manifest values or secrets.
    #[non_exhaustive]
    #[error("not permitted: {detail}")]
    NotPermitted {
        /// Which permission is missing.
        detail: String,
    },
    /// The plugin failed internally. The detail is logged only; it must not contain
    /// manifest values or secrets.
    #[non_exhaustive]
    #[error("internal plugin failure: {detail}")]
    Internal {
        /// Diagnostic detail.
        detail: String,
    },
}

impl SectionPluginError {
    /// A rejection with a detail and an optional JSON Pointer location.
    #[must_use]
    pub fn rejected(detail: impl Into<String>, location: Option<&str>) -> Self {
        Self::Rejected {
            detail: truncate_detail(&detail.into()),
            location: location.map(truncate_detail),
        }
    }

    /// A missing-permission error.
    #[must_use]
    pub fn not_permitted(detail: impl Into<String>) -> Self {
        Self::NotPermitted {
            detail: truncate_detail(&detail.into()),
        }
    }

    /// An internal failure.
    #[must_use]
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal {
            detail: truncate_detail(&detail.into()),
        }
    }

    /// The section outcome this error maps to.
    #[must_use]
    pub fn outcome(&self) -> SectionOutcome {
        match self {
            Self::Rejected { .. } => SectionOutcome::Rejected,
            Self::NotPermitted { .. } => SectionOutcome::NotPermitted,
            Self::Internal { .. } => SectionOutcome::InternalFailure,
        }
    }
}

/// Error returned by the audit plugin.
///
/// The constructors cut `detail` to [`PLUGIN_DETAIL_MAX_BYTES`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuditPluginError {
    /// The plugin refused to record the hand-off.
    #[non_exhaustive]
    #[error("audit plugin refused: {detail}")]
    Refused {
        /// Diagnostic detail.
        detail: String,
    },
    /// The plugin is unavailable.
    #[non_exhaustive]
    #[error("audit plugin unavailable: {detail}")]
    Unavailable {
        /// Diagnostic detail.
        detail: String,
    },
}

impl AuditPluginError {
    /// A refusal.
    #[must_use]
    pub fn refused(detail: impl Into<String>) -> Self {
        Self::Refused {
            detail: truncate_detail(&detail.into()),
        }
    }

    /// An unavailability.
    #[must_use]
    pub fn unavailable(detail: impl Into<String>) -> Self {
        Self::Unavailable {
            detail: truncate_detail(&detail.into()),
        }
    }

    /// The report reason this error maps to.
    #[must_use]
    pub fn reason(&self) -> UnavailableReason {
        match self {
            Self::Refused { .. } => UnavailableReason::Refused,
            Self::Unavailable { .. } => UnavailableReason::Unreachable,
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(
    clippy::assertions_on_constants,
    clippy::non_ascii_literal,
    clippy::trivially_copy_pass_by_ref,
    clippy::needless_pass_by_value
)]
#[path = "errors_tests.rs"]
mod errors_tests;
