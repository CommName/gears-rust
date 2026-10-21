//! Plugin-facing limits (DESIGN sections 3.1 to 3.3).

/// Longest accepted schema version, in bytes (DESIGN 3.1). A schema version is
/// opaque text of 1 to `SCHEMA_VERSION_MAX_BYTES` bytes.
pub const SCHEMA_VERSION_MAX_BYTES: usize = 256;

/// Longest plugin error `detail` returned to the caller, in bytes (DESIGN 3.3).
/// A longer `detail` is cut at `PLUGIN_DETAIL_MAX_BYTES`.
pub const PLUGIN_DETAIL_MAX_BYTES: usize = 1024;

/// Largest served section schema, in bytes (DESIGN 3.2). A schema above
/// `SERVED_SCHEMA_MAX_BYTES` is `unusable`.
pub const SERVED_SCHEMA_MAX_BYTES: usize = 1_048_576;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "limits_tests.rs"]
mod tests;
