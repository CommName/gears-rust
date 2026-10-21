//! Constants of the gear.

use std::time::Duration;

/// The gateway request deadline the synchronous budget must fit in (DESIGN 4.3).
pub const GATEWAY_REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// Upper bound of `max_envelope_text_length`, and so of the application id and
/// version, in bytes (DESIGN 4.2). DESIGN names the constant without a value;
/// 255 is the implementation's choice.
pub const APPLICATION_ID_MAX_BYTES: u32 = 255;

/// The most UTF-8 bytes one character can take, and so the lower bound of
/// `max_envelope_text_length`: the published envelope schema counts
/// characters, so it allows `max_envelope_text_length / 4` of them to stay
/// within the limit in bytes, and that must be at least one.
pub const UTF8_MAX_CHAR_BYTES: u32 = 4;

/// `Retry-After` of every temporarily-unavailable refusal, in seconds
/// (DESIGN 3.3 "Refusal bodies"). DESIGN names the constant without a value;
/// 5 is the implementation's choice.
pub const REFUSAL_RETRY_AFTER_SECONDS: u64 = 5;

/// Longest plugin-supplied text written to a log, in bytes (DESIGN 4.2).
/// DESIGN names the constant without a value; 1024 is the implementation's
/// choice.
pub const LOG_TEXT_MAX_BYTES: usize = 1024;

/// Most evaluation paths a served section schema may allow: the number of
/// subschema evaluations one instance can reach, counting a subschema once for
/// every way of reaching it through `$ref` and `$dynamicRef`. It bounds the
/// work of validating one section. DESIGN does not name a value; 1,000,000 is
/// the implementation's choice, above what a schema of
/// `SERVED_SCHEMA_MAX_BYTES` without shared references can reach.
pub const SCHEMA_EVALUATION_PATHS_MAX: u64 = 1_000_000;

/// Longest chain of `$ref` and `$dynamicRef` hops in a served section schema.
/// It bounds the recursion depth of compiling and validating. DESIGN does not
/// name a value; 64 is the implementation's choice.
pub const SCHEMA_REF_CHAIN_MAX: usize = 64;

/// Most problems one validation reports. DESIGN does not name a value; 100 is
/// the implementation's choice.
pub const PROBLEMS_MAX: usize = 100;

/// Longest problem location, in bytes; a longer one is cut. DESIGN does not
/// name a value; 1024 is the implementation's choice.
pub const PROBLEM_LOCATION_MAX_BYTES: usize = 1024;

/// `text` cut to at most `max_bytes` bytes, on a character boundary.
#[must_use]
pub fn cut(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Whether `key` follows the section key grammar: one or more lower-case
/// ASCII letters, digits, `-`, `_`, or `.` (DESIGN 3.3).
#[must_use]
pub fn is_valid_section_key(key: &str) -> bool {
    !key.is_empty()
        && key.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'-' | b'_' | b'.')
        })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "constants_tests.rs"]
mod tests;
