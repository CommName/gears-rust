//! Row → SDK-model mapping plus the small pure helpers the stores share.
//!
//! Every conversion that can fail on malformed stored data surfaces as
//! [`UsageCollectorPluginError::Internal`] — a row already in the database that
//! cannot be reconstituted is a plugin invariant break, not a caller error.
//!
//! `UsageTypeGtsId` is the SDK newtype over `gts::GtsInstanceId`; it is built
//! from a stored `&str` via [`gts_id_from_str`] (validating `UsageTypeGtsId::new`)
//! and read back via [`gts_id_str`] (`AsRef<str>`).
//!
//! Unlike the reference plugin (which stores metadata as `jsonb`), this plugin
//! stores metadata as `Map(String, String)` and maps it directly to
//! `BTreeMap<MetadataKey, String>` — no JSON encode/decode is required.
//!
//! `DateTime64(6)` columns are stored as `i64` epoch-microseconds. This module
//! converts to/from [`time::OffsetDateTime`] using nanosecond arithmetic.

use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;

use time::OffsetDateTime;

use usage_collector_sdk::{
    IdempotencyKey, MetadataKey, ResourceRef, SubjectRef, UsageCollectorPluginError, UsageKind,
    UsageRecord, UsageRecordStatus, UsageType, UsageTypeGtsId,
};

use super::entity::{UsageRecordRow, UsageRecordStatusCode, UsageTypeKindCode, UsageTypeRow};

/// Borrow the raw GTS instance id string out of a [`UsageTypeGtsId`] (for binding).
#[must_use]
pub fn gts_id_str(gts_id: &UsageTypeGtsId) -> &str {
    gts_id.as_ref()
}

/// Reconstruct a validated [`UsageTypeGtsId`] from a stored string.
///
/// # Errors
///
/// Returns [`UsageCollectorPluginError::Internal`] when the stored value is not
/// a valid usage-type GTS id (a stored-data invariant break).
pub fn gts_id_from_str(raw: &str) -> Result<UsageTypeGtsId, UsageCollectorPluginError> {
    UsageTypeGtsId::new(raw).map_err(|e| {
        UsageCollectorPluginError::internal(format!("stored gts_id `{raw}` invalid: {e}"))
    })
}

/// Convert a stored [`UsageRecordStatusCode`] into [`UsageRecordStatus`].
///
/// Infallible: [`UsageRecordStatusCode`] is a closed `#[repr(i8)]` enum that
/// only ever decodes to one of its two variants (see `entity.rs`) — any
/// other `Enum8` discriminant is already rejected by the `clickhouse` crate's
/// `RowBinaryWithNamesAndTypes` schema validation before this is called.
#[must_use]
pub fn parse_status(code: UsageRecordStatusCode) -> UsageRecordStatus {
    match code {
        UsageRecordStatusCode::Active => UsageRecordStatus::Active,
        UsageRecordStatusCode::Inactive => UsageRecordStatus::Inactive,
    }
}

/// [`UsageRecordStatusCode`] form of a [`UsageRecordStatus`] for storage.
#[must_use]
pub fn status_to_code(status: UsageRecordStatus) -> UsageRecordStatusCode {
    match status {
        UsageRecordStatus::Active => UsageRecordStatusCode::Active,
        UsageRecordStatus::Inactive => UsageRecordStatusCode::Inactive,
    }
}

/// String form of a [`UsageRecordStatus`], matching the `Enum8` value names
/// (used for query fragments / keyset cursor keys, not row (de)serialization).
#[must_use]
pub fn status_to_str(status: UsageRecordStatus) -> &'static str {
    match status {
        UsageRecordStatus::Active => "active",
        UsageRecordStatus::Inactive => "inactive",
    }
}

/// Convert a stored [`UsageTypeKindCode`] into [`UsageKind`].
///
/// Infallible: [`UsageTypeKindCode`] is a closed `#[repr(i8)]` enum that only
/// ever decodes to one of its two variants (see `entity.rs`) — any other
/// `Enum8` discriminant is already rejected by the `clickhouse` crate's
/// `RowBinaryWithNamesAndTypes` schema validation before this is called.
#[must_use]
pub fn parse_kind(code: UsageTypeKindCode) -> UsageKind {
    match code {
        UsageTypeKindCode::Counter => UsageKind::Counter,
        UsageTypeKindCode::Gauge => UsageKind::Gauge,
    }
}

/// [`UsageTypeKindCode`] form of a [`UsageKind`] for storage.
#[must_use]
pub fn kind_to_code(kind: UsageKind) -> UsageTypeKindCode {
    match kind {
        UsageKind::Counter => UsageTypeKindCode::Counter,
        UsageKind::Gauge => UsageTypeKindCode::Gauge,
    }
}

/// Convert a `Map(String, String)` into a typed metadata map.
///
/// # Errors
///
/// Returns [`UsageCollectorPluginError::Internal`] when a key fails
/// [`MetadataKey::new`] validation (a stored-data invariant break).
pub fn metadata_hashmap_to_btree<S: BuildHasher>(
    map: HashMap<String, String, S>,
) -> Result<BTreeMap<MetadataKey, String>, UsageCollectorPluginError> {
    let mut out = BTreeMap::new();
    for (key, val) in map {
        let metadata_key = MetadataKey::new(key).map_err(|e| {
            UsageCollectorPluginError::internal(format!("stored metadata key invalid: {e}"))
        })?;
        out.insert(metadata_key, val);
    }
    Ok(out)
}

/// Convert a typed metadata map into a `Map(String, String)`.
#[must_use]
pub fn metadata_btree_to_hashmap(map: &BTreeMap<MetadataKey, String>) -> HashMap<String, String> {
    map.iter()
        .map(|(k, v)| (k.as_str().to_owned(), v.clone()))
        .collect()
}

/// Convert epoch-microseconds (`i64`) to [`OffsetDateTime`].
///
/// # Errors
///
/// Returns [`UsageCollectorPluginError::Internal`] when the timestamp is
/// out of range for [`OffsetDateTime`].
pub fn micros_to_datetime(micros: i64) -> Result<OffsetDateTime, UsageCollectorPluginError> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000).map_err(|e| {
        UsageCollectorPluginError::internal(format!(
            "stored timestamp {micros} µs out of range: {e}"
        ))
    })
}

/// Convert [`OffsetDateTime`] to epoch-microseconds (`i64`).
///
/// Uses [`usage_collector_sdk::created_at_micros`] so the µs projection stays
/// in sync with the deterministic id derivation in [`usage_collector_sdk`].
#[must_use]
#[allow(
    clippy::cast_possible_truncation,
    reason = "practical timestamps fit in i64"
)]
pub fn datetime_to_micros(dt: OffsetDateTime) -> i64 {
    usage_collector_sdk::created_at_micros(dt) as i64
}

/// Map a [`UsageRecordRow`] into a validated [`UsageRecord`].
///
/// # Errors
///
/// Returns [`UsageCollectorPluginError::Internal`] when any stored component
/// fails its SDK newtype validation.
pub fn record_row_to_model(row: UsageRecordRow) -> Result<UsageRecord, UsageCollectorPluginError> {
    let gts_id = gts_id_from_str(&row.gts_id)?;

    let resource_ref = ResourceRef::new(row.resource_id, row.resource_type).map_err(|e| {
        UsageCollectorPluginError::internal(format!("stored resource_ref invalid: {e}"))
    })?;

    let subject_ref = match row.subject_id {
        Some(subject_id) => Some(SubjectRef::new(subject_id, row.subject_type).map_err(|e| {
            UsageCollectorPluginError::internal(format!("stored subject_ref invalid: {e}"))
        })?),
        None => None,
    };

    let idempotency_key = IdempotencyKey::new(row.idempotency_key).map_err(|e| {
        UsageCollectorPluginError::internal(format!("stored idempotency_key invalid: {e}"))
    })?;

    let metadata = metadata_hashmap_to_btree(row.metadata)?;
    let status = parse_status(row.status);
    let created_at = micros_to_datetime(row.created_at)?;

    Ok(UsageRecord {
        id: row.id,
        gts_id,
        tenant_id: row.tenant_id,
        resource_ref,
        subject_ref,
        metadata,
        value: row.value,
        idempotency_key,
        corrects_id: row.corrects_id,
        status,
        created_at,
    })
}

/// Map a [`UsageTypeRow`] into a validated [`UsageType`].
///
/// # Errors
///
/// Returns [`UsageCollectorPluginError::Internal`] when the stored `gts_id`,
/// `kind`, or any `metadata_fields` entry fails its SDK newtype validation.
pub fn type_row_to_model(row: UsageTypeRow) -> Result<UsageType, UsageCollectorPluginError> {
    let gts_id = gts_id_from_str(&row.gts_id)?;
    let kind = parse_kind(row.kind);

    let mut metadata_fields = std::collections::BTreeSet::new();
    for field in row.metadata_fields {
        let key = MetadataKey::new(field).map_err(|e| {
            UsageCollectorPluginError::internal(format!(
                "stored metadata_fields entry invalid: {e}"
            ))
        })?;
        metadata_fields.insert(key);
    }

    Ok(UsageType {
        gts_id,
        kind,
        metadata_fields,
    })
}

/// Build a [`UsageRecordRow`] ready for `INSERT` from an SDK [`UsageRecord`].
///
/// `version` must be supplied by the caller (current Unix-timestamp microseconds
/// cast to `u64`).
#[must_use]
pub fn record_to_row(record: &UsageRecord, version: u64) -> UsageRecordRow {
    let now_micros = datetime_to_micros(OffsetDateTime::now_utc());
    UsageRecordRow {
        id: record.id,
        tenant_id: record.tenant_id,
        gts_id: gts_id_str(&record.gts_id).to_owned(),
        value: record.value,
        created_at: datetime_to_micros(record.created_at),
        resource_id: record.resource_ref.resource_id().to_owned(),
        resource_type: record.resource_ref.resource_type().to_owned(),
        subject_id: record
            .subject_ref
            .as_ref()
            .map(|s| s.subject_id().to_owned()),
        subject_type: record
            .subject_ref
            .as_ref()
            .and_then(|s| s.subject_type())
            .map(str::to_owned),
        idempotency_key: record.idempotency_key.as_str().to_owned(),
        corrects_id: record.corrects_id,
        status: status_to_code(UsageRecordStatus::Active),
        metadata: metadata_btree_to_hashmap(&record.metadata),
        ingested_at: now_micros,
        version,
    }
}

/// Build a deactivation-marker [`UsageRecordRow`] for `id` with `status = 'inactive'`
/// and a `version` strictly higher than `prev_version`.
#[must_use]
pub fn make_inactive_marker(
    source: &UsageRecordRow,
    base_version: u64,
    offset: u64,
) -> UsageRecordRow {
    let mut marker = source.clone();
    marker.status = UsageRecordStatusCode::Inactive;
    marker.version = base_version.saturating_add(offset);
    marker
}

/// Extract the cursor-key string for a given `field` name from a row.
///
/// Returns `None` for unknown fields or `NULL` optional columns (a `NULL` value
/// cannot seed a stable keyset boundary).
#[must_use]
pub fn record_row_key(row: &UsageRecordRow, field: &str) -> Option<String> {
    use time::format_description::well_known::Rfc3339;
    match field {
        "id" => Some(row.id.to_string()),
        "corrects_id" => row.corrects_id.map(|id| id.to_string()),
        "created_at" => {
            let dt = OffsetDateTime::from_unix_timestamp_nanos(i128::from(row.created_at) * 1_000)
                .ok()?;
            dt.format(&Rfc3339).ok()
        }
        "tenant_id" => Some(row.tenant_id.to_string()),
        "resource_id" => Some(row.resource_id.clone()),
        "resource_type" => Some(row.resource_type.clone()),
        "subject_id" => row.subject_id.clone(),
        "subject_type" => row.subject_type.clone(),
        "status" => Some(status_to_str(parse_status(row.status)).to_owned()),
        _ => None,
    }
}

/// Compare the canonical fields of a stored row against an incoming record for
/// dedup absorption vs [`UsageCollectorPluginError::IdempotencyConflict`].
///
/// The canonical set compared here is `id`, `value`, `resource_ref`,
/// `subject_ref`, `corrects_id`, and `metadata`. Excluded are the dedup-key
/// fields (`tenant_id` / `gts_id` / `created_at` / `id`) — the lookup
/// key, already matched — and server-managed `status` / `ingested_at` /
/// `version`.
///
/// # Errors
///
/// Returns [`UsageCollectorPluginError::Internal`] when the stored metadata
/// cannot be decoded (a stored-data invariant break, distinct from a field
/// mismatch which returns `Ok(false)`).
pub fn canonical_equal(
    row: &UsageRecordRow,
    incoming: &UsageRecord,
) -> Result<bool, UsageCollectorPluginError> {
    let stored_metadata = metadata_hashmap_to_btree(row.metadata.clone())?;
    Ok(row.id == incoming.id
        && row.value == incoming.value
        && row.resource_id == incoming.resource_ref.resource_id()
        && row.resource_type == incoming.resource_ref.resource_type()
        && row.subject_id.as_deref()
            == incoming
                .subject_ref
                .as_ref()
                .map(usage_collector_sdk::SubjectRef::subject_id)
        && row.subject_type.as_deref()
            == incoming.subject_ref.as_ref().and_then(|s| s.subject_type())
        && row.corrects_id == incoming.corrects_id
        && stored_metadata == incoming.metadata)
}

/// Mint a `ReplacingMergeTree` merge-resolution version: current
/// Unix-timestamp microseconds as `u64`.
///
/// This is the value written to the `version` column, not a row revision
/// counter: on merge / `FINAL` resolution the physical copy carrying the
/// highest `version` for a sort key wins, so a freshly minted value always
/// supersedes an earlier one.
#[must_use]
#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "micros since epoch is always positive and fits in u64 for practical timestamps"
)]
pub fn current_merge_version() -> u64 {
    let micros = usage_collector_sdk::created_at_micros(OffsetDateTime::now_utc());
    micros as u64
}

/// Compute a `version` that is strictly higher than `existing_version` by at
/// least `offset + 1`.  Guards the deactivation cascade against a deactivation
/// marker that resolves to a lower version than the row it must supersede.
#[must_use]
pub fn version_higher_than(existing_version: u64, offset: u64) -> u64 {
    let now = current_merge_version();
    now.max(existing_version.saturating_add(offset).saturating_add(1))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "mapper_tests.rs"]
mod mapper_tests;
