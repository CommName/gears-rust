//! `ClickHouse` row structs mirroring `usage_records` and `usage_type_catalog`
//! (see `migrations/0001_init.sql`).
//!
//! These carry the raw storage-typed columns; [`super::mapper`] turns a row into
//! the validated SDK model (and back where needed). Column types match the DDL:
//! - `UUID` → [`uuid::Uuid`] (serialised as two `u64` halves in `RowBinary`;
//!   see [`ch_uuid`]).
//! - `DateTime64(6)` → `i64` epoch-microseconds.
//! - `Decimal128(9)` → [`rust_decimal::Decimal`] (serialised as `i128` in
//!   `RowBinary` with scale 9; see [`ch_decimal128_9`]).
//! - `Map(String, String)` → [`std::collections::HashMap`]`<String, String>`.
//! - `Enum8(…)` → a local `#[repr(i8)]` enum via `serde_repr`
//!   ([`UsageRecordStatusCode`] / [`UsageTypeKindCode`]). The `clickhouse`
//!   crate's `RowBinaryWithNamesAndTypes` schema validation requires an
//!   `Enum8` column to (de)serialize as its underlying `i8` discriminant —
//!   a plain `String`/`&str` field is rejected with a `SchemaMismatch`
//!   error ("attempting to (de)serialize `ClickHouse` type Enum8(…) as
//!   `&str` which is not compatible"). [`super::mapper`] converts to/from
//!   the SDK's `UsageKind` / `UsageRecordStatus`, whose own serde shape is a
//!   lowercase string tuned for the REST API, not this storage wire format.
//! - `Array(String)` → `Vec<String>`.
//! - `Nullable(T)` → `Option<T>`.
//! - `UInt64` → `u64`.

use std::collections::HashMap;

use rust_decimal::Decimal;
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

/// `RowBinary`-compatible serde helpers for `UUID` columns.
///
/// `ClickHouse` stores `UUID` as two `u64` halves in `RowBinary` format.
/// The standard `uuid::Uuid` serde implementation serialises as raw bytes
/// in non-human-readable contexts, which does not match; this module
/// bridges the gap by serialising as `(u64, u64)` in binary mode and as
/// a hyphenated string in human-readable mode (e.g. JSON tests).
///
/// Apply with `#[serde(with = "ch_uuid")]`.
pub(crate) mod ch_uuid {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use uuid::Uuid;

    pub fn serialize<S: Serializer>(u: &Uuid, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            u.to_string().serialize(s)
        } else {
            u.as_u64_pair().serialize(s)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Uuid, D::Error> {
        if d.is_human_readable() {
            let s: String = Deserialize::deserialize(d)?;
            Uuid::parse_str(&s).map_err(serde::de::Error::custom)
        } else {
            let (hi, lo): (u64, u64) = Deserialize::deserialize(d)?;
            Ok(Uuid::from_u64_pair(hi, lo))
        }
    }
}

/// `RowBinary`-compatible serde helper for `Nullable(UUID)` columns.
///
/// Apply with `#[serde(with = "ch_uuid_opt")]`.
pub(crate) mod ch_uuid_opt {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use uuid::Uuid;

    // serde's #[serde(with = "...")] requires `&Option<T>` for serialize; the
    // clippy::ref_option lint does not apply to this fixed API contract.
    #[allow(
        clippy::ref_option,
        reason = "required signature for serde with-module"
    )]
    pub fn serialize<S: Serializer>(opt: &Option<Uuid>, s: S) -> Result<S::Ok, S::Error> {
        match opt {
            Some(u) => {
                if s.is_human_readable() {
                    Some(u.to_string()).serialize(s)
                } else {
                    Some(u.as_u64_pair()).serialize(s)
                }
            }
            None => {
                if s.is_human_readable() {
                    None::<String>.serialize(s)
                } else {
                    None::<(u64, u64)>.serialize(s)
                }
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Uuid>, D::Error> {
        if d.is_human_readable() {
            let opt: Option<String> = Deserialize::deserialize(d)?;
            opt.map(|s| Uuid::parse_str(&s).map_err(serde::de::Error::custom))
                .transpose()
        } else {
            let opt: Option<(u64, u64)> = Deserialize::deserialize(d)?;
            Ok(opt.map(|(hi, lo)| Uuid::from_u64_pair(hi, lo)))
        }
    }
}

/// `RowBinary`-compatible serde helper for `Decimal128(9)` columns.
///
/// `ClickHouse` stores `Decimal128(9)` as a 16-byte little-endian signed
/// integer (the value × 10^9). This module serialises `rust_decimal::Decimal`
/// as `i128` in binary mode and as a decimal string in human-readable mode
/// (e.g. JSON tests).
///
/// Apply with `#[serde(with = "ch_decimal128_9")]`.
pub(crate) mod ch_decimal128_9 {
    use rust_decimal::Decimal;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    const SCALE: u32 = 9;

    pub fn serialize<S: Serializer>(d: &Decimal, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            d.to_string().serialize(s)
        } else {
            let mut rescaled = *d;
            rescaled.rescale(SCALE);
            rescaled.mantissa().serialize(s)
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Decimal, D::Error> {
        if d.is_human_readable() {
            let s: String = Deserialize::deserialize(d)?;
            s.parse::<Decimal>().map_err(serde::de::Error::custom)
        } else {
            let raw: i128 = Deserialize::deserialize(d)?;
            let negative = raw < 0;
            let abs_val = raw.unsigned_abs();
            // Safety: the earlier `abs_val >> 96 != 0` guard ensures that only
            // the lower 96 bits are non-zero, so the casts to u32 are lossless.
            #[allow(
                clippy::cast_possible_truncation,
                reason = "abs_val >> 96 guard ensures lower 96 bits are the only non-zero bits"
            )]
            let lo = abs_val as u32;
            #[allow(
                clippy::cast_possible_truncation,
                reason = "abs_val >> 96 guard ensures lower 96 bits are the only non-zero bits"
            )]
            let mid = (abs_val >> 32) as u32;
            #[allow(
                clippy::cast_possible_truncation,
                reason = "abs_val >> 96 guard ensures lower 96 bits are the only non-zero bits"
            )]
            let hi = (abs_val >> 64) as u32;
            if abs_val >> 96 != 0 {
                return Err(serde::de::Error::custom(
                    "Decimal128(9) value overflows `rust_decimal::Decimal`",
                ));
            }
            Ok(Decimal::from_parts(lo, mid, hi, negative, SCALE))
        }
    }
}

/// Wire representation of the `usage_records.status` column
/// (`Enum8('active' = 1, 'inactive' = 2)`).
///
/// Discriminants must match the DDL exactly (see `migrations/0001_init.sql`).
/// Use [`super::mapper::parse_status`] / [`super::mapper::status_to_code`] to
/// convert to/from the SDK's `usage_collector_sdk::UsageRecordStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum UsageRecordStatusCode {
    Active = 1,
    Inactive = 2,
}

/// One row of the `usage_records` table.
///
/// Field types match `ClickHouse` `RowBinary` encoding:
/// - `DateTime64(6)` → `i64` epoch-microseconds.
/// - `UUID` → [`Uuid`] via [`ch_uuid`] / [`ch_uuid_opt`].
/// - `Decimal128(9)` → [`Decimal`] via [`ch_decimal128_9`].
/// - `Map(String, String)` → `HashMap<String, String>`.
/// - `Enum8(…)` → [`UsageRecordStatusCode`] (`#[repr(i8)]` via `serde_repr`).
/// - `UInt64` → `u64`.
#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize, serde::Serialize)]
pub struct UsageRecordRow {
    /// `id` — deterministic gateway-derived record id.
    #[serde(with = "ch_uuid")]
    pub id: Uuid,
    /// `tenant_id` — owning tenant.
    #[serde(with = "ch_uuid")]
    pub tenant_id: Uuid,
    /// `gts_id` — usage-type identifier (application-enforced FK).
    pub gts_id: String,
    /// `value` — signed `Decimal128(9)` measurement.
    #[serde(with = "ch_decimal128_9")]
    pub value: Decimal,
    /// `created_at` — event time as epoch-microseconds (`DateTime64(6)`).
    pub created_at: i64,
    /// `resource_id` — resource attribution leaf.
    pub resource_id: String,
    /// `resource_type` — resource type discriminator.
    pub resource_type: String,
    /// `subject_id` — optional subject identifier.
    pub subject_id: Option<String>,
    /// `subject_type` — optional subject type discriminator.
    pub subject_type: Option<String>,
    /// `idempotency_key` — caller-supplied dedup key.
    pub idempotency_key: String,
    /// `corrects_id` — set on a compensation row; references the corrected row.
    #[serde(with = "ch_uuid_opt")]
    pub corrects_id: Option<Uuid>,
    /// `status` — `'active'` / `'inactive'` (`Enum8` wire value as its `i8` discriminant).
    pub status: UsageRecordStatusCode,
    /// `metadata` — caller metadata as `Map(String, String)`.
    pub metadata: HashMap<String, String>,
    /// `ingested_at` — server insert timestamp as epoch-microseconds.
    pub ingested_at: i64,
    /// `version` — `ReplacingMergeTree` version column; higher value wins.
    pub version: u64,
}

/// Wire representation of the `usage_type_catalog.kind` column
/// (`Enum8('counter' = 1, 'gauge' = 2)`).
///
/// Discriminants must match the DDL exactly (see `migrations/0001_init.sql`).
/// Use [`super::mapper::parse_kind`] / [`super::mapper::kind_to_code`] to
/// convert to/from the SDK's `usage_collector_sdk::UsageKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(i8)]
pub enum UsageTypeKindCode {
    Counter = 1,
    Gauge = 2,
}

/// One row of the `usage_type_catalog` table.
///
/// Field types match `ClickHouse` `RowBinary` encoding.
#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize, serde::Serialize)]
pub struct UsageTypeRow {
    /// `gts_id` — catalog sort key.
    pub gts_id: String,
    /// `kind` — `'counter'` / `'gauge'` (`Enum8` wire value as its `i8` discriminant).
    pub kind: UsageTypeKindCode,
    /// `metadata_fields` — declared metadata key names (`Array(String)`).
    pub metadata_fields: Vec<String>,
    /// `version` — `ReplacingMergeTree` version column; higher value wins.
    pub version: u64,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "entity_tests.rs"]
mod entity_tests;
