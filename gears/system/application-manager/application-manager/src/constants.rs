//! Constants of the gear.

use std::time::Duration;

/// The gateway request deadline the synchronous budget must fit in (DESIGN 4.3).
pub const GATEWAY_REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// Upper bound of `max_envelope_text_length`, and so of the application id and
/// version, in bytes (DESIGN 4.2). DESIGN names the constant without a value;
/// 255 is the implementation's choice.
pub const APPLICATION_ID_MAX_BYTES: u32 = 255;

/// `Retry-After` of every temporarily-unavailable refusal, in seconds
/// (DESIGN 3.3 "Refusal bodies"). DESIGN names the constant without a value;
/// 5 is the implementation's choice.
pub const REFUSAL_RETRY_AFTER_SECONDS: u64 = 5;

/// Longest plugin-supplied text written to a log, in bytes (DESIGN 4.2).
/// DESIGN names the constant without a value; 1024 is the implementation's
/// choice.
pub const LOG_TEXT_MAX_BYTES: usize = 1024;

/// Compiled size limit of one regular expression in a section schema, in
/// bytes; a larger one makes the schema `unusable` (DESIGN 4.2). DESIGN names
/// the constant without a value; 1 MiB is the implementation's choice.
pub const SCHEMA_REGEX_SIZE_LIMIT: usize = 1 << 20;

/// Lazy DFA cache limit of one regular expression in a section schema, in
/// bytes (DESIGN 4.2). DESIGN names the constant without a value; 1 MiB is
/// the implementation's choice.
pub const SCHEMA_REGEX_DFA_SIZE_LIMIT: usize = 1 << 20;

/// Most evaluation paths a registered section schema may allow: the number of
/// subschema evaluations one instance can reach, counting a subschema once for
/// every way of reaching it through `$ref`. It bounds the work of validating
/// one section. DESIGN does not name a value; 1,000,000 is the
/// implementation's choice.
pub const SCHEMA_EVALUATION_PATHS_MAX: u64 = 1_000_000;

/// Longest chain of `$ref` hops in a registered section schema. It bounds the
/// recursion depth of compiling and validating. DESIGN does not name a value;
/// 64 is the implementation's choice.
pub const SCHEMA_REF_CHAIN_MAX: usize = 64;

/// Most problems one validation reports. DESIGN does not name a value; 100 is
/// the implementation's choice.
pub const PROBLEMS_MAX: usize = 100;

/// Longest problem location, in bytes; a longer one is cut. DESIGN does not
/// name a value; 1024 is the implementation's choice.
pub const PROBLEM_LOCATION_MAX_BYTES: usize = 1024;

/// Most expired operations one sweep completes, and most completed records
/// one purge deletes (DESIGN 4.2). DESIGN names the constant without a value;
/// 100 is the implementation's choice, well under every backend's
/// bind-parameter limit.
pub const MAINTENANCE_BATCH_SIZE: u64 = 100;

/// Largest and default page of the history read (DESIGN 3.3, 4.2). DESIGN
/// names the constant without a value; 100 is the implementation's choice.
pub const HISTORY_PAGE_MAX_LIMIT: u64 = 100;

/// Largest and default page of the application list (DESIGN 3.3, 4.2). DESIGN
/// names the constant without a value; 100 is the implementation's choice.
pub const APPLICATION_PAGE_MAX_LIMIT: u64 = 100;

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
