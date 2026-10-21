//! Errors a section plugin returns to the Application Manager.

/// Outcome of a failed section plugin call (DESIGN section 3.3).
///
/// From `apply` and `remove`, `Rejected` gives `rejected`, `NotPermitted` gives
/// `not_permitted`, and `Internal` gives `internal_failure` and is only
/// logged. The `detail` of `Rejected` and `NotPermitted` is returned cut at
/// [`crate::PLUGIN_DETAIL_MAX_BYTES`]. From `schema`, see
/// [`crate::SectionPluginClientV1::schema`] for the mapping.
///
/// `#[non_exhaustive]` allows additive changes within v1.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum SectionPluginError {
    /// The plugin refuses the section content. The gear reports a rejection
    /// of that section; `location` is a JSON Pointer into the section.
    #[error("section rejected: {detail}")]
    Rejected {
        /// Why the section was refused.
        detail: String,
        /// JSON Pointer into the section, when known.
        location: Option<String>,
    },
    /// The plugin refuses the caller. The gear reports a permission refusal.
    #[error("not permitted: {detail}")]
    NotPermitted {
        /// Why the caller was refused.
        detail: String,
    },
    /// The plugin failed for a reason the caller cannot act on. The gear only
    /// logs `message` and reports a generic failure.
    #[error("internal plugin error: {message}")]
    Internal {
        /// Diagnostic text, logged only.
        message: String,
    },
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "error_tests.rs"]
mod tests;
