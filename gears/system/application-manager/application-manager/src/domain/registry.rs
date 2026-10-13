//! Registry entry domain types (DESIGN §3.3, §4.1).
//!
//! A [`RegistryEntry`] is the stored record of one seeded application: its id, the
//! version last seeded, the outcome token of the last seeding, when it was seeded and
//! the audit id of that seeding. Construction validates every field so an entry that
//! exists is always storable.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use toolkit_macros::domain_model;

use crate::domain::limits::REGISTRY_TEXT_MAX_BYTES;

/// Outcome of the last seeding of an application.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LastResult {
    /// Every section was seeded.
    Succeeded,
    /// At least one section succeeded and at least one did not.
    PartiallyFailed,
    /// A section plugin rejected the seeding.
    PluginRejected,
    /// The caller was not permitted.
    NotPermitted,
    /// An internal failure occurred.
    InternalFailure,
}

impl LastResult {
    /// Every variant, in token order of the storage CHECK constraint.
    pub const ALL: [LastResult; 5] = [
        Self::Succeeded,
        Self::PartiallyFailed,
        Self::PluginRejected,
        Self::NotPermitted,
        Self::InternalFailure,
    ];

    /// The stable storage token of this result.
    #[must_use]
    pub fn as_token(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::PartiallyFailed => "partially_failed",
            Self::PluginRejected => "plugin_rejected",
            Self::NotPermitted => "not_permitted",
            Self::InternalFailure => "internal_failure",
        }
    }

    /// Parses a storage token; `None` for an unknown token.
    #[must_use]
    pub fn from_token(token: &str) -> Option<LastResult> {
        Self::ALL.into_iter().find(|r| r.as_token() == token)
    }
}

/// Why a [`RegistryEntry`] could not be constructed.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryEntryError {
    /// The application id is empty.
    EmptyApplicationId,
    /// The application id exceeds the byte limit.
    ApplicationIdTooLong,
    /// The application id contains a control character.
    ApplicationIdControlCharacter,
    /// The version is empty.
    EmptyVersion,
    /// The version exceeds the byte limit.
    VersionTooLong,
    /// The version contains a control character.
    VersionControlCharacter,
    /// The audit id contains a NUL character and cannot be stored.
    AuditIdNotStorable,
    /// The seeding time is outside the storable range.
    SeededAtOutOfRange,
}

/// First rule an application id or version text breaks. The one source of the bound and
/// control-character rules for [`is_storable_text`] and [`RegistryEntry::new`].
enum TextFault {
    Empty,
    TooLong,
    Control,
}

fn text_fault(text: &str) -> Option<TextFault> {
    if text.is_empty() {
        Some(TextFault::Empty)
    } else if text.len() > REGISTRY_TEXT_MAX_BYTES {
        Some(TextFault::TooLong)
    } else if text.chars().any(char::is_control) {
        Some(TextFault::Control)
    } else {
        None
    }
}

/// Whether `text` is a storable application id or version: non-empty, within the byte
/// limit and free of control characters.
#[must_use]
pub fn is_storable_text(text: &str) -> bool {
    text_fault(text).is_none()
}

/// One stored registry record.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryEntry {
    application_id: String,
    version: String,
    last_result: LastResult,
    last_seeded_at: SystemTime,
    audit_id: String,
}

impl RegistryEntry {
    /// Builds a validated entry. The seeding time is truncated to microseconds.
    ///
    /// # Errors
    ///
    /// Returns the [`RegistryEntryError`] naming the first invalid field: the id or
    /// version is empty, too long or has a control character, the audit id contains NUL,
    /// or the seeding time is outside `UNIX_EPOCH..=9999-12-31T23:59:59.999999Z`.
    pub fn new(
        application_id: impl Into<String>,
        version: impl Into<String>,
        last_result: LastResult,
        last_seeded_at: SystemTime,
        audit_id: impl Into<String>,
    ) -> Result<RegistryEntry, RegistryEntryError> {
        let application_id = application_id.into();
        let version = version.into();
        match text_fault(&application_id) {
            Some(TextFault::Empty) => return Err(RegistryEntryError::EmptyApplicationId),
            Some(TextFault::TooLong) => return Err(RegistryEntryError::ApplicationIdTooLong),
            Some(TextFault::Control) => {
                return Err(RegistryEntryError::ApplicationIdControlCharacter);
            }
            None => {}
        }
        match text_fault(&version) {
            Some(TextFault::Empty) => return Err(RegistryEntryError::EmptyVersion),
            Some(TextFault::TooLong) => return Err(RegistryEntryError::VersionTooLong),
            Some(TextFault::Control) => return Err(RegistryEntryError::VersionControlCharacter),
            None => {}
        }
        let audit_id = audit_id.into();
        if audit_id.contains('\0') {
            return Err(RegistryEntryError::AuditIdNotStorable);
        }
        let last_seeded_at = checked_seed_time(last_seeded_at)?;
        Ok(RegistryEntry {
            application_id,
            version,
            last_result,
            last_seeded_at,
            audit_id,
        })
    }

    /// The application id.
    #[must_use]
    pub fn application_id(&self) -> &str {
        &self.application_id
    }

    /// The version last seeded.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// The outcome of the last seeding.
    #[must_use]
    pub fn last_result(&self) -> LastResult {
        self.last_result
    }

    /// When the application was last seeded, at microsecond precision.
    #[must_use]
    pub fn last_seeded_at(&self) -> SystemTime {
        self.last_seeded_at
    }

    /// The audit id of the last seeding.
    #[must_use]
    pub fn audit_id(&self) -> &str {
        &self.audit_id
    }
}

/// 9999-12-31T23:59:59.999999Z as seconds and microseconds since the epoch.
const MAX_SECS: u64 = 253_402_300_799;
const MAX_MICROS: u32 = 999_999;

fn checked_seed_time(t: SystemTime) -> Result<SystemTime, RegistryEntryError> {
    let d = t
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RegistryEntryError::SeededAtOutOfRange)?;
    let micros = d.subsec_micros();
    if d.as_secs() > MAX_SECS || (d.as_secs() == MAX_SECS && micros > MAX_MICROS) {
        return Err(RegistryEntryError::SeededAtOutOfRange);
    }
    Ok(UNIX_EPOCH + Duration::new(d.as_secs(), micros * 1_000))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "registry_tests.rs"]
mod registry_tests;
