//! Domain-level storage and platform limits (DESIGN §3.7, §4.7).

/// Byte bound of the registry text columns (application id and version), fixed by the
/// initial migration (DESIGN §3.7). Changing it is a schema change.
pub const REGISTRY_TEXT_MAX_BYTES: usize = 255;

/// Byte bound of one free-text value in a log event (escaped, cut at a character
/// boundary), so a plugin- or registry-provided string cannot flood the log.
pub const LOG_TEXT_MAX_BYTES: usize = 1024;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(clippy::non_ascii_literal)]
#[path = "limits_tests.rs"]
mod limits_tests;
