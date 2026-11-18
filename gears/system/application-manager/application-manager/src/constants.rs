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
