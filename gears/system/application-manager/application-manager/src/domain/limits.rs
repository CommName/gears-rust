//! Domain-level storage and platform limits (DESIGN §3.7, §4.7).

/// Byte bound of the registry text columns (application id and version), fixed by the
/// initial migration (DESIGN §3.7). Changing it is a schema change.
pub const REGISTRY_TEXT_MAX_BYTES: usize = 255;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(clippy::non_ascii_literal)]
#[path = "limits_tests.rs"]
mod limits_tests;
