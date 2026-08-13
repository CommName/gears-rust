use std::collections::{BTreeMap, HashMap};

use rust_decimal::Decimal;
use uuid::Uuid;

use usage_collector_sdk::{
    MetadataKey, UsageCollectorPluginError, UsageKind, UsageRecordStatus, UsageTypeGtsId,
};

use super::super::entity::{
    UsageRecordRow, UsageRecordStatusCode, UsageTypeKindCode, UsageTypeRow,
};
use super::{
    canonical_equal, gts_id_from_str, gts_id_str, kind_to_code, make_inactive_marker,
    metadata_btree_to_hashmap, metadata_hashmap_to_btree, micros_to_datetime, parse_kind,
    parse_status, record_row_key, record_row_to_model, record_to_row, status_to_code,
    status_to_str, type_row_to_model, version_higher_than,
};

// ── status round-trip ────────────────────────────────────────────────────────
//
// `UsageRecordStatusCode` is a closed `#[repr(i8)]` enum (see `entity.rs`
// doc comment); an "unknown" wire discriminant can no longer be represented
// in a `UsageRecordRow`, so there is no `parse_status`-rejects-unknown case
// left to test here — that class of malformed-data error is now caught by
// the `clickhouse` crate's own schema validation before a row is decoded.

#[test]
fn parse_status_round_trips_through_code_form() {
    for status in [UsageRecordStatus::Active, UsageRecordStatus::Inactive] {
        let code = status_to_code(status);
        assert_eq!(parse_status(code), status);
    }
}

#[test]
fn status_to_str_emits_lowercase_wire_tokens() {
    assert_eq!(status_to_str(UsageRecordStatus::Active), "active");
    assert_eq!(status_to_str(UsageRecordStatus::Inactive), "inactive");
}

// ── kind round-trip ──────────────────────────────────────────────────────────
//
// Same rationale as status above: `UsageTypeKindCode` is a closed
// `#[repr(i8)]` enum, so there is no "rejects unknown" case to test.

#[test]
fn parse_kind_round_trips_through_code_form() {
    for kind in [UsageKind::Counter, UsageKind::Gauge] {
        let code = kind_to_code(kind);
        assert_eq!(parse_kind(code), kind);
    }
}

// ── metadata HashMap <-> BTreeMap round-trip ─────────────────────────────────

#[test]
fn metadata_round_trips_hashmap_btree() {
    let mut btree = BTreeMap::new();
    btree.insert(MetadataKey::new("region").unwrap(), "eu-west".to_owned());
    btree.insert(MetadataKey::new("tier").unwrap(), "gold".to_owned());

    let hmap = metadata_btree_to_hashmap(&btree);
    let back = metadata_hashmap_to_btree(hmap).unwrap();
    assert_eq!(back, btree);
}

#[test]
fn empty_metadata_round_trips() {
    let btree: BTreeMap<MetadataKey, String> = BTreeMap::new();
    let hmap = metadata_btree_to_hashmap(&btree);
    assert!(hmap.is_empty());
    assert!(metadata_hashmap_to_btree(hmap).unwrap().is_empty());
}

#[test]
fn metadata_hashmap_invalid_key_is_internal() {
    let mut hmap = HashMap::new();
    hmap.insert(String::new(), "value".to_owned()); // empty key is invalid
    assert!(matches!(
        metadata_hashmap_to_btree(hmap),
        Err(UsageCollectorPluginError::Internal(_))
    ));
}

// ── gts_id primitive ─────────────────────────────────────────────────────────

const VALID_GTS_ID: &str = "gts.cf.core.uc.usage_record.v1~cf.compute._.vcpu_hours.v1";

#[test]
fn gts_id_from_str_accepts_valid_and_rejects_invalid_as_internal() {
    assert!(gts_id_from_str(VALID_GTS_ID).is_ok());
    assert!(matches!(
        gts_id_from_str("not-a-valid-gts-id"),
        Err(UsageCollectorPluginError::Internal(_))
    ));
}

#[test]
fn gts_id_str_returns_raw_string() {
    let gts_id = UsageTypeGtsId::new(VALID_GTS_ID).unwrap();
    assert_eq!(gts_id_str(&gts_id), VALID_GTS_ID);
}

// ── record row -> model ──────────────────────────────────────────────────────

fn valid_metadata_hmap() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("region".to_owned(), "eu-west".to_owned());
    m
}

fn valid_record_row() -> UsageRecordRow {
    UsageRecordRow {
        id: Uuid::from_u128(1),
        tenant_id: Uuid::from_u128(2),
        gts_id: VALID_GTS_ID.to_owned(),
        value: Decimal::new(425, 1),               // 42.5
        created_at: 1_700_000_000 * 1_000_000_i64, // 2023-11-14 in microseconds
        resource_id: "res-1".to_owned(),
        resource_type: "compute.vm".to_owned(),
        subject_id: Some("subj-1".to_owned()),
        subject_type: Some("user".to_owned()),
        idempotency_key: "idem-1".to_owned(),
        corrects_id: None,
        status: UsageRecordStatusCode::Active,
        metadata: valid_metadata_hmap(),
        ingested_at: 1_700_000_100 * 1_000_000_i64,
        version: 1_700_000_000_000_000_u64,
    }
}

#[test]
fn record_row_to_model_maps_valid_row() {
    let row = valid_record_row();
    let model = record_row_to_model(row).expect("a fully valid row must map");

    assert_eq!(model.id, Uuid::from_u128(1));
    assert_eq!(model.tenant_id, Uuid::from_u128(2));
    assert_eq!(model.gts_id, UsageTypeGtsId::new(VALID_GTS_ID).unwrap());
    assert_eq!(model.value, Decimal::new(425, 1));
    assert_eq!(model.resource_ref.resource_id(), "res-1");
    assert_eq!(model.resource_ref.resource_type(), "compute.vm");
    let subject = model.subject_ref.as_ref().expect("subject present");
    assert_eq!(subject.subject_id(), "subj-1");
    assert_eq!(subject.subject_type(), Some("user"));
    assert_eq!(model.idempotency_key.as_str(), "idem-1");
    assert_eq!(model.corrects_id, None);
    assert_eq!(model.status, UsageRecordStatus::Active);
    assert_eq!(
        model.metadata.get(&MetadataKey::new("region").unwrap()),
        Some(&"eu-west".to_owned())
    );
}

#[test]
fn record_row_absent_subject_maps_to_none() {
    let mut row = valid_record_row();
    row.subject_id = None;
    row.subject_type = None;
    let model = record_row_to_model(row).expect("a row without a subject maps");
    assert!(model.subject_ref.is_none());
}

#[test]
fn record_row_invalid_gts_id_is_internal() {
    let mut row = valid_record_row();
    row.gts_id = "not-a-valid-gts-id".to_owned();
    assert!(matches!(
        record_row_to_model(row),
        Err(UsageCollectorPluginError::Internal(_))
    ));
}

#[test]
fn record_row_invalid_resource_ref_is_internal() {
    let mut row = valid_record_row();
    row.resource_id = String::new(); // empty resource_id fails ResourceRef::new
    assert!(matches!(
        record_row_to_model(row),
        Err(UsageCollectorPluginError::Internal(_))
    ));
}

#[test]
fn record_row_invalid_subject_ref_is_internal() {
    let mut row = valid_record_row();
    row.subject_id = Some(String::new()); // present-but-empty subject_id is rejected
    assert!(matches!(
        record_row_to_model(row),
        Err(UsageCollectorPluginError::Internal(_))
    ));
}

#[test]
fn record_row_invalid_idempotency_key_is_internal() {
    let mut row = valid_record_row();
    row.idempotency_key = String::new();
    assert!(matches!(
        record_row_to_model(row),
        Err(UsageCollectorPluginError::Internal(_))
    ));
}

// A malformed `status` can no longer be represented in a `UsageRecordRow` --
// see the "status round-trip" section above for the rationale.

// ── usage-type row -> model ──────────────────────────────────────────────────

fn valid_type_row() -> UsageTypeRow {
    UsageTypeRow {
        gts_id: VALID_GTS_ID.to_owned(),
        kind: UsageTypeKindCode::Counter,
        metadata_fields: vec!["region".to_owned(), "tier".to_owned()],
        version: 1,
    }
}

#[test]
fn type_row_to_model_maps_valid_row() {
    let model = type_row_to_model(valid_type_row()).expect("a fully valid type row must map");
    assert_eq!(model.gts_id, UsageTypeGtsId::new(VALID_GTS_ID).unwrap());
    assert_eq!(model.kind, UsageKind::Counter);
    assert_eq!(model.metadata_fields.len(), 2);
    assert!(
        model
            .metadata_fields
            .contains(&MetadataKey::new("region").unwrap())
    );
}

#[test]
fn type_row_invalid_gts_id_is_internal() {
    let mut row = valid_type_row();
    row.gts_id = "not-a-valid-gts-id".to_owned();
    assert!(matches!(
        type_row_to_model(row),
        Err(UsageCollectorPluginError::Internal(_))
    ));
}

// A malformed `kind` can no longer be represented in a `UsageTypeRow` --
// see the "kind round-trip" section above for the rationale.

#[test]
fn type_row_invalid_metadata_field_is_internal() {
    let mut row = valid_type_row();
    row.metadata_fields = vec![String::new()]; // empty key fails MetadataKey::new
    assert!(matches!(
        type_row_to_model(row),
        Err(UsageCollectorPluginError::Internal(_))
    ));
}

// ── canonical_equal ──────────────────────────────────────────────────────────

#[test]
fn canonical_equal_returns_true_for_identical_fields() {
    use time::OffsetDateTime;
    use usage_collector_sdk::{IdempotencyKey, ResourceRef, UsageRecord, UsageRecordStatus};

    let row = valid_record_row();
    let created_at =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(row.created_at) * 1_000).unwrap();

    let record = UsageRecord {
        id: row.id,
        tenant_id: row.tenant_id,
        gts_id: UsageTypeGtsId::new(&row.gts_id).unwrap(),
        value: row.value,
        created_at,
        resource_ref: ResourceRef::new(row.resource_id.clone(), row.resource_type.clone()).unwrap(),
        subject_ref: None,
        idempotency_key: IdempotencyKey::new(row.idempotency_key.clone()).unwrap(),
        corrects_id: None,
        status: UsageRecordStatus::Active,
        metadata: BTreeMap::new(),
    };

    // Row has a subject and non-empty metadata, record does not — must be false.
    assert!(!canonical_equal(&row, &record).unwrap());
}

#[test]
fn version_higher_than_is_always_strictly_greater() {
    let existing = 1_000_u64;
    let result = version_higher_than(existing, 0);
    assert!(result > existing);
}

#[test]
fn version_higher_than_with_offset_adds_headroom() {
    let existing = 1_000_u64;
    let result = version_higher_than(existing, 5);
    assert!(result > existing.saturating_add(5));
}

// ── model → row / write path ─────────────────────────────────────────────────

#[test]
fn record_to_row_maps_active_fields_and_optional_subject() {
    use std::collections::BTreeMap;

    use time::OffsetDateTime;
    use usage_collector_sdk::{
        IdempotencyKey, MetadataKey, ResourceRef, SubjectRef, UsageRecord, UsageRecordStatus,
    };

    let created_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let mut metadata = BTreeMap::new();
    metadata.insert(MetadataKey::new("region").unwrap(), "eu-west".to_owned());

    let record = UsageRecord {
        id: Uuid::from_u128(11),
        tenant_id: Uuid::from_u128(22),
        gts_id: UsageTypeGtsId::new(VALID_GTS_ID).unwrap(),
        value: Decimal::new(100, 0),
        created_at,
        resource_ref: ResourceRef::new("res-9".to_owned(), "compute.vm".to_owned()).unwrap(),
        subject_ref: Some(SubjectRef::new("subj-9".to_owned(), Some("user".to_owned())).unwrap()),
        idempotency_key: IdempotencyKey::new("idem-9".to_owned()).unwrap(),
        corrects_id: Some(Uuid::from_u128(33)),
        status: UsageRecordStatus::Inactive, // write path always persists Active
        metadata,
    };

    let row = record_to_row(&record, 42);
    assert_eq!(row.id, record.id);
    assert_eq!(row.tenant_id, record.tenant_id);
    assert_eq!(row.gts_id, VALID_GTS_ID);
    assert_eq!(row.value, record.value);
    assert_eq!(row.resource_id, "res-9");
    assert_eq!(row.resource_type, "compute.vm");
    assert_eq!(row.subject_id.as_deref(), Some("subj-9"));
    assert_eq!(row.subject_type.as_deref(), Some("user"));
    assert_eq!(row.idempotency_key, "idem-9");
    assert_eq!(row.corrects_id, Some(Uuid::from_u128(33)));
    assert_eq!(row.status, UsageRecordStatusCode::Active);
    assert_eq!(row.version, 42);
    assert_eq!(
        row.metadata.get("region").map(String::as_str),
        Some("eu-west")
    );
}

#[test]
fn make_inactive_marker_flips_status_and_bumps_version() {
    let source = valid_record_row();
    let marker = make_inactive_marker(&source, 100, 3);
    assert_eq!(marker.status, UsageRecordStatusCode::Inactive);
    assert_eq!(marker.version, 103);
    assert_eq!(marker.id, source.id);
}

#[test]
fn record_row_key_extracts_known_fields() {
    let row = valid_record_row();
    assert_eq!(record_row_key(&row, "id"), Some(row.id.to_string()));
    assert_eq!(
        record_row_key(&row, "tenant_id"),
        Some(row.tenant_id.to_string())
    );
    assert_eq!(
        record_row_key(&row, "resource_id"),
        Some("res-1".to_owned())
    );
    assert_eq!(
        record_row_key(&row, "resource_type"),
        Some("compute.vm".to_owned())
    );
    assert_eq!(
        record_row_key(&row, "subject_id"),
        Some("subj-1".to_owned())
    );
    assert_eq!(
        record_row_key(&row, "subject_type"),
        Some("user".to_owned())
    );
    assert_eq!(record_row_key(&row, "status"), Some("active".to_owned()));
    assert!(record_row_key(&row, "created_at").is_some());
    assert_eq!(record_row_key(&row, "corrects_id"), None);
    assert_eq!(record_row_key(&row, "unknown"), None);

    let mut with_corrects = row;
    with_corrects.corrects_id = Some(Uuid::from_u128(99));
    assert_eq!(
        record_row_key(&with_corrects, "corrects_id"),
        Some(Uuid::from_u128(99).to_string())
    );
}

// ── micros_to_datetime ───────────────────────────────────────────────────────

#[test]
fn micros_to_datetime_round_trips_a_stored_timestamp() {
    let micros = 1_700_000_000_123_456_i64;
    let dt = micros_to_datetime(micros).expect("an in-range timestamp must convert");
    assert_eq!(super::datetime_to_micros(dt), micros);
}

/// A stored value outside `OffsetDateTime`'s range surfaces as `Internal`
/// instead of panicking the read path.
#[test]
fn micros_to_datetime_rejects_out_of_range_value() {
    let err =
        micros_to_datetime(i64::MAX).expect_err("i64::MAX microseconds is far past year 9999");
    assert!(
        matches!(&err, UsageCollectorPluginError::Internal(msg) if msg.contains("out of range")),
        "unexpected error: {err:?}"
    );
}
