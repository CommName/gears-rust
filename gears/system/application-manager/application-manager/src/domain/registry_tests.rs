//! Unit tests for the registry entry domain type (DESIGN §3.1 "Application registry
//! entry", §3.7 "Table: `application_registry`", §3.2 "Registry Repository", ADR 0005).
//!
//! Planned API in `crate::domain::registry` (new file, P4). The module ends with:
//!
//! ```text
//! #[cfg(test)]
//! #[cfg_attr(coverage_nightly, coverage(off))]
//! #[path = "registry_tests.rs"]
//! mod registry_tests;
//! ```
//!
//! ```text
//! /// The last result of a seed, as stored in `application_registry.last_result`
//! /// (DESIGN §3.1, §3.7). This is NOT `OutcomeCategory`: `partially_failed` is a registry
//! /// value only, and `not_found`, `invalid_manifest` and `temporarily_unavailable` never
//! /// reach the registry (no write happens before dispatch).
//! #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
//! pub enum LastResult { Succeeded, PartiallyFailed, PluginRejected, NotPermitted, InternalFailure }
//! impl LastResult {
//!     pub const ALL: [LastResult; 5];
//!     /// The stored token: `succeeded`, `partially_failed`, `plugin_rejected`,
//!     /// `not_permitted`, `internal_failure`.
//!     pub fn as_token(self) -> &'static str;
//!     /// Exact, case-sensitive inverse of `as_token`; anything else is `None`.
//!     pub fn from_token(token: &str) -> Option<LastResult>;
//! }
//!
//! /// Why a registry text value can never be stored (DESIGN §3.7 constraints, §4.1 "Never-
//! /// storable id" and "Control character").
//! #[derive(Debug, Clone, Copy, PartialEq, Eq)]
//! pub enum RegistryEntryError {
//!     EmptyApplicationId, ApplicationIdTooLong, ApplicationIdControlCharacter,
//!     EmptyVersion, VersionTooLong, VersionControlCharacter,
//!     /// The audit id holds a NUL, which Postgres cannot store in a text value.
//!     AuditIdNotStorable,
//!     /// The seed time is before `UNIX_EPOCH` or after 9999-12-31T23:59:59.999999Z (the
//!     /// range every backend stores exactly).
//!     SeededAtOutOfRange,
//! }
//!
//! /// Non-empty, at most `REGISTRY_TEXT_MAX_BYTES` UTF-8 bytes, no Unicode Cc character.
//! /// P5 reuses it for the never-storable id rule (its negation).
//! pub fn is_storable_text(text: &str) -> bool;
//!
//! /// One registry entry (DESIGN §3.7). Fields are private; `new` is the only constructor,
//! /// so every value in memory satisfies the table constraints.
//! #[derive(Debug, Clone, PartialEq, Eq)]
//! pub struct RegistryEntry { /* application_id, version, last_result, last_seeded_at, audit_id */ }
//! impl RegistryEntry {
//!     /// `last_seeded_at` is truncated to whole microseconds (the precision every backend
//!     /// keeps), so an entry read back from any backend equals the entry written.
//!     /// `audit_id` is opaque, unbounded and not interpreted (DESIGN §3.7 states no bound
//!     /// for it), except that a NUL is rejected (`AuditIdNotStorable`). A time outside
//!     /// `UNIX_EPOCH ..= 9999-12-31T23:59:59.999999Z` (after truncation) is rejected
//!     /// (`SeededAtOutOfRange`): a pre-epoch value would keep its nanoseconds and a
//!     /// far-future one would overflow the timestamp column.
//!     pub fn new(application_id: impl Into<String>, version: impl Into<String>,
//!                last_result: LastResult, last_seeded_at: std::time::SystemTime,
//!                audit_id: impl Into<String>) -> Result<RegistryEntry, RegistryEntryError>;
//!     pub fn application_id(&self) -> &str;
//!     pub fn version(&self) -> &str;
//!     pub fn last_result(&self) -> LastResult;
//!     pub fn last_seeded_at(&self) -> std::time::SystemTime;
//!     pub fn audit_id(&self) -> &str;
//! }
//! ```
//!
//! Decisions recorded here: `last_seeded_at` is `SystemTime` (the domain `Clock` port
//! returns it and DESIGN open item 9 concerns only the SDK hand-off times); the storage
//! layer converts to `time::OffsetDateTime` for the `timestamptz` column. The id and the
//! version are checked in the order id then version, and a control character is checked
//! after emptiness and length, then the audit id, then the seed time (the first failing
//! rule is reported; tests below never combine two failing rules).
//!
//! - `last_result_tokens_are_the_design_values`: the five stored tokens (§3.7).
//! - `last_result_all_lists_five_distinct_values`: `ALL` (§3.7).
//! - `last_result_token_round_trips_and_rejects_anything_else`: exact match only (§3.7).
//! - `new_keeps_every_field`: getters return what was given (§3.7).
//! - `new_accepts_text_at_exactly_the_byte_bound`: 255 bytes pass (§3.7).
//! - `new_rejects_text_one_byte_over_the_bound`: 256 bytes fail, id and version (§3.7).
//! - `the_bound_counts_utf8_bytes_not_characters`: multi-byte text (§3.7 "in bytes of UTF-8").
//! - `new_rejects_empty_id_and_version`: non-empty (§3.7).
//! - `new_rejects_every_control_character_of_category_cc`: U+0000..U+001F, U+007F..U+009F (§4.1).
//! - `new_accepts_characters_next_to_the_control_ranges`: not Cc, so stored (§4.1).
//! - `new_never_normalizes_or_trims_text`: whitespace and normalization forms kept (§3.7).
//! - `new_leaves_the_audit_id_uninterpreted`: any text, any length, but no NUL (§3.7).
//! - `new_truncates_the_seed_time_to_microseconds`: backend-independent equality.
//! - `new_accepts_the_epoch_and_truncates_just_after_it`, `new_rejects_a_time_before_the_epoch`,
//!   `new_accepts_the_last_storable_instant_and_rejects_one_microsecond_more`: the seed time
//!   range.
//! - `entries_are_equal_only_when_every_field_is_equal`: field-wise equality.
//! - `is_storable_text_table`: the shared predicate (§4.1).
//! - `is_storable_text_rejects_every_cc_code_point_and_only_those`: all 65 Cc code points.
//! - `is_storable_text_counts_bytes_at_multi_byte_bounds`: 127/128 x U+00E9, 85/86 x U+20AC,
//!   63/64 x U+1F600.
//! - `is_storable_text_agrees_with_entry_validation`: one corpus, id and version.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::domain::limits::REGISTRY_TEXT_MAX_BYTES;
use crate::domain::registry::{LastResult, RegistryEntry, RegistryEntryError, is_storable_text};

fn at(secs: u64, nanos: u32) -> SystemTime {
    UNIX_EPOCH + Duration::new(secs, nanos)
}

fn valid(id: &str, version: &str) -> Result<RegistryEntry, RegistryEntryError> {
    RegistryEntry::new(id, version, LastResult::Succeeded, at(1, 0), "audit-1")
}

#[test]
fn last_result_tokens_are_the_design_values() {
    assert_eq!(LastResult::Succeeded.as_token(), "succeeded");
    assert_eq!(LastResult::PartiallyFailed.as_token(), "partially_failed");
    assert_eq!(LastResult::PluginRejected.as_token(), "plugin_rejected");
    assert_eq!(LastResult::NotPermitted.as_token(), "not_permitted");
    assert_eq!(LastResult::InternalFailure.as_token(), "internal_failure");
}

#[test]
fn last_result_all_lists_five_distinct_values() {
    let set: HashSet<LastResult> = LastResult::ALL.into_iter().collect();
    assert_eq!(LastResult::ALL.len(), 5);
    assert_eq!(set.len(), 5, "ALL must not repeat a value");
    let tokens: HashSet<&str> = LastResult::ALL
        .into_iter()
        .map(LastResult::as_token)
        .collect();
    assert_eq!(tokens.len(), 5, "every value has its own token");
}

#[test]
fn last_result_token_round_trips_and_rejects_anything_else() {
    for result in LastResult::ALL {
        assert_eq!(LastResult::from_token(result.as_token()), Some(result));
    }
    for bad in [
        "",
        "Succeeded",
        "SUCCEEDED",
        " succeeded",
        "succeeded ",
        "partial",
        "not_found",
        "invalid_manifest",
        "temporarily_unavailable",
        "succeeded\u{0}",
    ] {
        assert_eq!(LastResult::from_token(bad), None, "token {bad:?}");
    }
}

#[test]
fn new_keeps_every_field() {
    let entry = RegistryEntry::new(
        "app-1",
        "2.3.4",
        LastResult::PartiallyFailed,
        at(1_800_000_000, 123_456_000),
        "audit-xyz",
    )
    .expect("a valid entry");

    assert_eq!(entry.application_id(), "app-1");
    assert_eq!(entry.version(), "2.3.4");
    assert_eq!(entry.last_result(), LastResult::PartiallyFailed);
    assert_eq!(entry.last_seeded_at(), at(1_800_000_000, 123_456_000));
    assert_eq!(entry.audit_id(), "audit-xyz");
}

#[test]
fn new_accepts_text_at_exactly_the_byte_bound() {
    assert_eq!(REGISTRY_TEXT_MAX_BYTES, 255, "pinned by limits_tests too");
    let at_bound = "a".repeat(REGISTRY_TEXT_MAX_BYTES);

    let entry = valid(&at_bound, &at_bound).expect("255 bytes are storable");

    assert_eq!(entry.application_id(), at_bound);
    assert_eq!(entry.version(), at_bound);
}

#[test]
fn new_rejects_text_one_byte_over_the_bound() {
    let over = "a".repeat(REGISTRY_TEXT_MAX_BYTES + 1);

    assert_eq!(
        valid(&over, "1").expect_err("id over the bound"),
        RegistryEntryError::ApplicationIdTooLong
    );
    assert_eq!(
        valid("app", &over).expect_err("version over the bound"),
        RegistryEntryError::VersionTooLong
    );
}

#[test]
fn the_bound_counts_utf8_bytes_not_characters() {
    // U+00E9 is 2 bytes: 127 of them are 254 bytes, 128 are 256.
    let two_byte_ok = "\u{e9}".repeat(127);
    let two_byte_over = "\u{e9}".repeat(128);
    // U+20AC is 3 bytes: 85 of them are exactly 255 bytes, 86 are 258.
    let three_byte_exact = "\u{20ac}".repeat(85);
    let three_byte_over = "\u{20ac}".repeat(86);
    // U+1F600 is 4 bytes: 63 are 252, 64 are 256.
    let four_byte_ok = "\u{1f600}".repeat(63);
    let four_byte_over = "\u{1f600}".repeat(64);
    // 255 characters of 2 bytes would pass a character count and must not pass here.
    let many_chars = "\u{e9}".repeat(255);

    for ok in [&two_byte_ok, &three_byte_exact, &four_byte_ok] {
        assert!(valid(ok, "1").is_ok(), "{} bytes must pass", ok.len());
        assert!(valid("app", ok).is_ok(), "{} bytes must pass", ok.len());
    }
    for over in [
        &two_byte_over,
        &three_byte_over,
        &four_byte_over,
        &many_chars,
    ] {
        assert_eq!(
            valid(over, "1").expect_err("over"),
            RegistryEntryError::ApplicationIdTooLong,
            "{} bytes",
            over.len()
        );
        assert_eq!(
            valid("app", over).expect_err("over"),
            RegistryEntryError::VersionTooLong,
            "{} bytes",
            over.len()
        );
    }
}

#[test]
fn new_rejects_empty_id_and_version() {
    assert_eq!(
        valid("", "1").expect_err("empty id"),
        RegistryEntryError::EmptyApplicationId
    );
    assert_eq!(
        valid("app", "").expect_err("empty version"),
        RegistryEntryError::EmptyVersion
    );
}

#[test]
fn new_rejects_every_control_character_of_category_cc() {
    let control: Vec<char> = ('\u{0}'..='\u{1f}').chain('\u{7f}'..='\u{9f}').collect();
    assert_eq!(control.len(), 32 + 33, "the Cc category has 65 code points");

    for c in control {
        let at_start = format!("{c}app");
        let in_middle = format!("a{c}b");
        let at_end = format!("app{c}");
        for id in [&at_start, &in_middle, &at_end] {
            assert_eq!(
                valid(id, "1").expect_err("control character in id"),
                RegistryEntryError::ApplicationIdControlCharacter,
                "U+{:04X} in {id:?}",
                u32::from(c)
            );
            assert_eq!(
                valid("app", id).expect_err("control character in version"),
                RegistryEntryError::VersionControlCharacter,
                "U+{:04X} in {id:?}",
                u32::from(c)
            );
        }
    }
}

#[test]
fn new_accepts_characters_next_to_the_control_ranges() {
    // Space (U+0020), U+00A0 (no-break space), U+00A1 and format / separator characters
    // are not Cc: they are stored as given.
    for c in [
        '\u{20}', '\u{a0}', '\u{a1}', '\u{ad}', '\u{200b}', '\u{2028}', '\u{2029}', '\u{feff}',
        '\u{7e}',
    ] {
        let id = format!("a{c}b");
        let entry = valid(&id, &id).unwrap_or_else(|e| panic!("U+{:04X}: {e:?}", u32::from(c)));
        assert_eq!(entry.application_id(), id);
    }
}

#[test]
fn new_never_normalizes_or_trims_text() {
    let nfc = "caf\u{e9}";
    let nfd = "cafe\u{301}";
    let a = valid(nfc, "1").expect("nfc");
    let b = valid(nfd, "1").expect("nfd");
    assert_ne!(a, b, "normalization forms are distinct ids");
    assert_eq!(a.application_id(), nfc);
    assert_eq!(b.application_id(), nfd);

    for text in [" ", "  lead", "trail  ", "UPPER", "MiXed"] {
        let entry = valid(text, text).expect("kept as given");
        assert_eq!(entry.application_id(), text);
        assert_eq!(entry.version(), text);
    }
}

#[test]
fn new_leaves_the_audit_id_uninterpreted() {
    let long = "\u{e9}".repeat(4000);
    let huge = "\u{e9}\u{20ac}\u{1f600}".repeat(11_500);
    for audit in [
        "",
        "a",
        "tab\there",
        "line\nbreak",
        "\u{1f600}",
        long.as_str(),
        huge.as_str(),
        "  spaced  ",
    ] {
        let entry = RegistryEntry::new("app", "1", LastResult::Succeeded, at(1, 0), audit)
            .unwrap_or_else(|e| panic!("audit id of {} bytes: {e:?}", audit.len()));
        assert_eq!(entry.audit_id(), audit);
    }
}

#[test]
fn new_rejects_a_nul_in_the_audit_id() {
    // Postgres rejects a NUL in a text value, so the entry never accepts one.
    for audit in ["\0", "a\0b", "audit\0"] {
        assert_eq!(
            RegistryEntry::new("app", "1", LastResult::Succeeded, at(1, 0), audit)
                .expect_err("a NUL in the audit id"),
            RegistryEntryError::AuditIdNotStorable,
            "{audit:?}"
        );
    }
}

#[test]
fn new_truncates_the_seed_time_to_microseconds() {
    let whole = at(1_800_000_000, 123_456_000);
    let ragged = at(1_800_000_000, 123_456_789);

    let kept = RegistryEntry::new("app", "1", LastResult::Succeeded, whole, "a").expect("ok");
    let cut = RegistryEntry::new("app", "1", LastResult::Succeeded, ragged, "a").expect("ok");

    assert_eq!(kept.last_seeded_at(), whole, "whole microseconds are kept");
    assert_eq!(
        cut.last_seeded_at(),
        whole,
        "sub-microsecond digits are cut, not rounded"
    );
    assert_eq!(kept, cut, "so the two entries are the same entry");
}

const LAST_SECS: u64 = 253_402_300_799; // 9999-12-31T23:59:59Z

fn seeded(time: SystemTime) -> Result<RegistryEntry, RegistryEntryError> {
    RegistryEntry::new("app", "1", LastResult::Succeeded, time, "a")
}

#[test]
fn new_accepts_the_epoch_and_truncates_just_after_it() {
    assert_eq!(
        seeded(UNIX_EPOCH).expect("the epoch").last_seeded_at(),
        UNIX_EPOCH
    );
    assert_eq!(
        seeded(UNIX_EPOCH + Duration::from_nanos(999))
            .expect("999 ns after the epoch")
            .last_seeded_at(),
        UNIX_EPOCH,
        "sub-microsecond digits are cut, so the entry is the epoch"
    );
    assert_eq!(
        seeded(UNIX_EPOCH + Duration::from_micros(1))
            .expect("1 us")
            .last_seeded_at(),
        UNIX_EPOCH + Duration::from_micros(1)
    );
}

#[test]
fn new_rejects_a_time_before_the_epoch() {
    // A pre-epoch value would keep its nanoseconds and differ after a round trip.
    for before in [
        UNIX_EPOCH - Duration::from_nanos(1),
        UNIX_EPOCH - Duration::from_micros(1),
        UNIX_EPOCH - Duration::from_secs(1),
        UNIX_EPOCH - Duration::from_hours(24 * 365 * 100),
    ] {
        assert_eq!(
            seeded(before).expect_err("before the epoch"),
            RegistryEntryError::SeededAtOutOfRange,
            "{before:?}"
        );
    }
}

#[test]
fn new_accepts_the_last_storable_instant_and_rejects_one_microsecond_more() {
    let last = UNIX_EPOCH + Duration::new(LAST_SECS, 999_999_000);

    assert_eq!(
        seeded(last)
            .expect("9999-12-31T23:59:59.999999Z")
            .last_seeded_at(),
        last
    );
    assert_eq!(
        seeded(last + Duration::from_nanos(999))
            .expect("truncates to the last instant")
            .last_seeded_at(),
        last
    );
    assert_eq!(
        seeded(last + Duration::from_micros(1)).expect_err("one microsecond past the range"),
        RegistryEntryError::SeededAtOutOfRange
    );
    assert_eq!(
        seeded(UNIX_EPOCH + Duration::from_mins(5_000_000_000)).expect_err("year 11 000+"),
        RegistryEntryError::SeededAtOutOfRange,
        "far-future values are rejected, not a panic later in the database layer"
    );
}

#[test]
fn entries_are_equal_only_when_every_field_is_equal() {
    let base = || RegistryEntry::new("app", "1", LastResult::Succeeded, at(5, 0), "audit");
    let same = base().expect("ok");
    assert_eq!(same, base().expect("ok"));

    let variants = [
        RegistryEntry::new("app2", "1", LastResult::Succeeded, at(5, 0), "audit"),
        RegistryEntry::new("app", "2", LastResult::Succeeded, at(5, 0), "audit"),
        RegistryEntry::new("app", "1", LastResult::PartiallyFailed, at(5, 0), "audit"),
        RegistryEntry::new("app", "1", LastResult::Succeeded, at(6, 0), "audit"),
        RegistryEntry::new("app", "1", LastResult::Succeeded, at(5, 0), "audit2"),
    ];
    for other in variants {
        assert_ne!(same, other.expect("ok"));
    }
}

#[test]
fn is_storable_text_table() {
    let bound = "a".repeat(REGISTRY_TEXT_MAX_BYTES);
    let over = "a".repeat(REGISTRY_TEXT_MAX_BYTES + 1);
    let cases: [(&str, bool); 12] = [
        ("app", true),
        (" ", true),
        (".", true),
        ("..", true),
        ("a/b", true),
        (bound.as_str(), true),
        ("", false),
        (over.as_str(), false),
        ("a\0b", false),
        ("a\nb", false),
        ("a\u{7f}b", false),
        ("a\u{85}b", false),
    ];
    for (text, expected) in cases {
        assert_eq!(
            is_storable_text(text),
            expected,
            "{:?}",
            text.escape_debug().to_string()
        );
    }
}

#[test]
fn is_storable_text_rejects_every_cc_code_point_and_only_those() {
    let mut rejected = 0;
    for code in 0..=0x2_ffff_u32 {
        let Some(c) = char::from_u32(code) else {
            continue;
        };
        let text = format!("a{c}b");
        let storable = is_storable_text(&text);
        assert_eq!(storable, !c.is_control(), "U+{code:04X}");
        if !storable {
            rejected += 1;
        }
    }
    assert_eq!(
        rejected,
        32 + 33,
        "exactly the 65 Cc code points are refused"
    );
    for c in ('\u{0}'..='\u{1f}').chain('\u{7f}'..='\u{9f}') {
        assert!(
            !is_storable_text(&c.to_string()),
            "U+{:04X} alone",
            u32::from(c)
        );
        assert!(
            !is_storable_text(&format!("{c}app")),
            "U+{:04X} first",
            u32::from(c)
        );
        assert!(
            !is_storable_text(&format!("app{c}")),
            "U+{:04X} last",
            u32::from(c)
        );
    }
}

#[test]
fn is_storable_text_counts_bytes_at_multi_byte_bounds() {
    for (unit, fits, over) in [
        ("\u{e9}", 127, 128),
        ("\u{20ac}", 85, 86),
        ("\u{1f600}", 63, 64),
    ] {
        assert!(is_storable_text(&unit.repeat(fits)), "{fits} x {unit}");
        assert!(!is_storable_text(&unit.repeat(over)), "{over} x {unit}");
    }
    assert!(
        !is_storable_text(&"\u{e9}".repeat(255)),
        "255 characters of 2 bytes"
    );
    assert!(is_storable_text(&"a".repeat(REGISTRY_TEXT_MAX_BYTES)));
    assert!(!is_storable_text(&"a".repeat(REGISTRY_TEXT_MAX_BYTES + 1)));
}

#[test]
fn is_storable_text_agrees_with_entry_validation() {
    let bound = "a".repeat(REGISTRY_TEXT_MAX_BYTES);
    let over = "a".repeat(REGISTRY_TEXT_MAX_BYTES + 1);
    let mut corpus: Vec<String> = vec![
        String::new(),
        " ".into(),
        "app".into(),
        "a/b".into(),
        "a\0b".into(),
        "a\nb".into(),
        "a\u{7f}".into(),
        "a\u{9f}".into(),
        "a\u{a0}".into(),
        "caf\u{e9}".into(),
        "\u{e9}".repeat(127),
        "\u{e9}".repeat(128),
        "\u{20ac}".repeat(85),
        "\u{20ac}".repeat(86),
        "\u{1f600}".repeat(63),
        "\u{1f600}".repeat(64),
        bound,
        over,
    ];
    corpus.extend(('\u{0}'..='\u{9f}').map(|c| format!("x{c}")));

    for text in &corpus {
        let storable = is_storable_text(text);
        assert_eq!(
            RegistryEntry::new(text.as_str(), "1", LastResult::Succeeded, at(1, 0), "a").is_ok(),
            storable,
            "id {:?}",
            text.escape_debug().to_string()
        );
        assert_eq!(
            RegistryEntry::new("app", text.as_str(), LastResult::Succeeded, at(1, 0), "a").is_ok(),
            storable,
            "version {:?}",
            text.escape_debug().to_string()
        );
    }
}

// F-013: the registry rule is the shared envelope text rule: `is_storable_text` is exactly
// "`text_limit_violations` at the column bound is empty", and the first violation is the
// registry fault, over a corpus at the edges (empty, bound, bound + 1, control characters).
#[test]
fn the_registry_text_rule_is_the_shared_text_limit_rule() {
    use crate::domain::manifest::text_limit_violations;
    let max = REGISTRY_TEXT_MAX_BYTES;
    let ctl = char::from(7).to_string();
    let corpus = [
        String::new(),
        "a".to_owned(),
        "a".repeat(max),
        "a".repeat(max + 1),
        "\u{e9}".repeat(max.div_euclid(2)),
        "\u{e9}".repeat(max.div_euclid(2) + 1),
        ctl.clone(),
        format!("a{ctl}b"),
        format!("{ctl}{}", "a".repeat(max)),
        char::from(0x7f).to_string(),
        "\u{85}".to_owned(),
    ];
    for text in &corpus {
        assert_eq!(
            is_storable_text(text),
            text_limit_violations(text, max).is_empty(),
            "{text:?}"
        );
    }
}
