//! Schema fingerprint (DESIGN §3.1 "Schema fingerprint"): the SHA-256 digest of the RFC 8785
//! canonical JSON of a schema, through the FIPS-validated `aws-lc-rs` provider.
//!
//! The workspace has no RFC 8785 implementation, so the canonical form is written here:
//! members sorted by UTF-16 code units (section 3.2.3), no insignificant whitespace, strings
//! escaped as section 3.2.2.2 requires, and numbers read as IEEE-754 doubles and written with
//! the ECMAScript number-to-string serialization (section 3.2.2.3).
//!
//! Precondition: the schema passed `check_schemas`, so it holds no integer beyond 2^53 (a
//! double cannot hold one; the schema validator reports it as cause `number`).

use aws_lc_rs::digest::{SHA256, digest};
use serde_json::Value;

/// `sha256:` and the lower-case hex SHA-256 digest of `text`.
pub(crate) fn sha256_text(text: &str) -> String {
    let mut out = String::from("sha256:");
    for byte in digest(&SHA256, text.as_bytes()).as_ref() {
        for nibble in [byte >> 4, byte & 0x0f] {
            out.push(char::from_digit(u32::from(nibble), 16).unwrap_or('0'));
        }
    }
    out
}

/// `sha256:` and the lower-case hex digest of the RFC 8785 canonical JSON of `schema`.
///
/// Formatting and member order never move the digest; any change of content does, except a
/// change between numbers that read as the same double (`1.0` and `1`).
#[must_use]
pub fn schema_fingerprint(schema: &Value) -> String {
    let mut canonical = String::new();
    write_canonical(schema, &mut canonical);
    sha256_text(&canonical)
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(n) => match n.as_f64() {
            Some(double) => out.push_str(&es_number(double)),
            // Only a number type beyond a double (a build with `arbitrary_precision`); the
            // precondition keeps it from here.
            None => out.push_str(&n.to_string()),
        },
        Value::String(s) => write_string(s, out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(members) => {
            let mut members: Vec<(&String, &Value)> = members.iter().collect();
            members.sort_by(|a, b| a.0.encode_utf16().cmp(b.0.encode_utf16()));
            out.push('{');
            for (i, (name, member)) in members.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(name, out);
                out.push(':');
                write_canonical(member, out);
            }
            out.push('}');
        }
    }
}

/// RFC 8785 section 3.2.2.2: `\b \t \n \f \r \" \\` as two characters, every other control
/// character as a lower-case `\u00xx`, everything else (`/`, DEL, non-ASCII) raw.
fn write_string(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '\u{08}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{0c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c < '\u{20}' => {
                out.push_str("\\u00");
                let code = u32::from(c);
                for nibble in [code >> 4, code & 0x0f] {
                    out.push(char::from_digit(nibble, 16).unwrap_or('0'));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// ECMAScript `Number::toString` (RFC 8785 section 3.2.2.3) of a finite double: the shortest
/// round-trip digits, laid out as a plain decimal for a decimal exponent in `-6..21` and in
/// exponent form outside it.
fn es_number(value: f64) -> String {
    if value == 0.0 {
        return "0".to_owned();
    }
    let sign = if value < 0.0 { "-" } else { "" };
    // Rust's `{:e}` prints the shortest round-trip digits as `d.ddde<exp>` (always with an `e`);
    // it fixes the digit count. At an exact tie between two `k`-digit strings it picks the
    // upper one, ECMAScript the even one, so the digits are written again with `k` digits by
    // the exactly rounded (half to even) `{:.*e}`.
    let shortest = format!("{:e}", value.abs());
    let shortest_digits = shortest.split_once('e').map_or(&*shortest, |(m, _)| m);
    let digit_count = shortest_digits.chars().filter(char::is_ascii_digit).count();
    let scientific = format!("{:.*e}", digit_count.saturating_sub(1), value.abs());
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    // ECMAScript names: `k` digits, and `n` such that the value is 0.digits * 10^n.
    let k = i64::try_from(digits.len()).unwrap_or(i64::MAX);
    let n = exponent.parse::<i64>().unwrap_or(0).saturating_add(1);
    let zeros = |count: i64| "0".repeat(usize::try_from(count).unwrap_or(0));
    let leading = |count: i64| {
        digits
            .chars()
            .take(usize::try_from(count).unwrap_or(0))
            .collect::<String>()
    };
    let trailing = |count: i64| {
        digits
            .chars()
            .skip(usize::try_from(count).unwrap_or(0))
            .collect::<String>()
    };
    let body = if k <= n && n <= 21 {
        format!("{digits}{}", zeros(n - k))
    } else if 0 < n && n <= 21 {
        format!("{}.{}", leading(n), trailing(n))
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", zeros(-n))
    } else {
        let power = n - 1;
        let power = format!("{}{}", if power < 0 { '-' } else { '+' }, power.abs());
        if k == 1 {
            format!("{digits}e{power}")
        } else {
            format!("{}.{}e{power}", leading(1), trailing(1))
        }
    };
    format!("{sign}{body}")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "fingerprint_tests.rs"]
mod fingerprint_tests;
