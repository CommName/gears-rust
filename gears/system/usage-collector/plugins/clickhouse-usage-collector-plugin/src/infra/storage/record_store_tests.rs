// Test modules using bare `panic!` opt in explicitly.
#![allow(clippy::panic)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rust_decimal::Decimal;
use uuid::Uuid;

use toolkit_odata::{CursorV1, ODataOrderBy, ODataQuery, OrderKey, SortDir};
use usage_collector_sdk::{UsageCollectorPluginError, UsageRecord, UsageTypeGtsId};

use super::{
    AggregateNdjsonParser, ChRecordStore, InsertKind, catalog_lookup_sql, err_for_slot,
    insert_dedup_token, parse_aggregate_response, prefer_dedup_row, record_dedup_key,
    row_dedup_key, split_by_catalog, split_by_representable,
};
use crate::domain::ports::RecordStore;
use crate::infra::metrics::Metrics;
use crate::infra::storage::entity::{UsageRecordRow, UsageRecordStatusCode};
use crate::infra::storage::mapper::{canonical_equal, version_higher_than};

const VCPU_GTS: &str = "gts.cf.core.uc.usage_record.v1~cf.compute._.vcpu_hours.v1";

fn make_row(id: Uuid, tenant_id: Uuid, created_at_micros: i64, version: u64) -> UsageRecordRow {
    UsageRecordRow {
        id,
        tenant_id,
        gts_id: VCPU_GTS.to_owned(),
        value: Decimal::new(100, 0),
        created_at: created_at_micros,
        resource_id: "res-1".to_owned(),
        resource_type: "vm".to_owned(),
        subject_id: None,
        subject_type: None,
        idempotency_key: "idem-1".to_owned(),
        corrects_id: None,
        status: UsageRecordStatusCode::Active,
        metadata: HashMap::new(),
        ingested_at: created_at_micros,
        version,
    }
}

// ── dedup key helpers ─────────────────────────────────────────────────────────

#[test]
fn row_dedup_key_matches_record_dedup_key_for_same_tuple() {
    use time::OffsetDateTime;
    use usage_collector_sdk::{IdempotencyKey, ResourceRef, UsageRecord, UsageRecordStatus};

    let id = Uuid::from_u128(1);
    let tenant_id = Uuid::from_u128(2);
    let created_at_micros = 1_700_000_000_000_000_i64;

    let row = make_row(id, tenant_id, created_at_micros, 1);

    let gts_id = UsageTypeGtsId::new(VCPU_GTS).unwrap();
    let created_at =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(created_at_micros) * 1_000).unwrap();
    let record = UsageRecord {
        id,
        tenant_id,
        gts_id,
        value: Decimal::new(100, 0),
        created_at,
        resource_ref: ResourceRef::new("res-1".to_owned(), "vm".to_owned()).unwrap(),
        subject_ref: None,
        idempotency_key: IdempotencyKey::new("idem-1".to_owned()).unwrap(),
        corrects_id: None,
        status: UsageRecordStatus::Active,
        metadata: std::collections::BTreeMap::default(),
    };

    assert_eq!(row_dedup_key(&row), record_dedup_key(&record));
}

// ── canonical_equal ───────────────────────────────────────────────────────────

#[test]
fn canonical_equal_returns_true_for_exact_match() {
    use time::OffsetDateTime;
    use usage_collector_sdk::{IdempotencyKey, ResourceRef, UsageRecord, UsageRecordStatus};

    let id = Uuid::from_u128(42);
    let tenant_id = Uuid::from_u128(99);
    let created_at_micros = 1_700_000_000_000_000_i64;
    let row = make_row(id, tenant_id, created_at_micros, 1);

    let gts_id = UsageTypeGtsId::new(VCPU_GTS).unwrap();
    let created_at =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(created_at_micros) * 1_000).unwrap();
    let record = UsageRecord {
        id,
        tenant_id,
        gts_id,
        value: Decimal::new(100, 0),
        created_at,
        resource_ref: ResourceRef::new("res-1".to_owned(), "vm".to_owned()).unwrap(),
        subject_ref: None,
        idempotency_key: IdempotencyKey::new("idem-1".to_owned()).unwrap(),
        corrects_id: None,
        status: UsageRecordStatus::Active,
        metadata: std::collections::BTreeMap::default(),
    };

    assert!(canonical_equal(&row, &record).unwrap());
}

#[test]
fn canonical_equal_returns_false_when_value_differs() {
    use time::OffsetDateTime;
    use usage_collector_sdk::{IdempotencyKey, ResourceRef, UsageRecord, UsageRecordStatus};

    let id = Uuid::from_u128(42);
    let tenant_id = Uuid::from_u128(99);
    let created_at_micros = 1_700_000_000_000_000_i64;
    let row = make_row(id, tenant_id, created_at_micros, 1);

    let gts_id = UsageTypeGtsId::new(VCPU_GTS).unwrap();
    let created_at =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(created_at_micros) * 1_000).unwrap();
    let record = UsageRecord {
        id,
        tenant_id,
        gts_id,
        value: Decimal::new(999, 0), // differs
        created_at,
        resource_ref: ResourceRef::new("res-1".to_owned(), "vm".to_owned()).unwrap(),
        subject_ref: None,
        idempotency_key: IdempotencyKey::new("idem-1".to_owned()).unwrap(),
        corrects_id: None,
        status: UsageRecordStatus::Active,
        metadata: std::collections::BTreeMap::default(),
    };

    assert!(!canonical_equal(&row, &record).unwrap());
}

// ── version_higher_than ───────────────────────────────────────────────────────

#[test]
fn version_higher_than_exceeds_existing() {
    let existing = 100_u64;
    assert!(version_higher_than(existing, 0) > existing);
}

#[test]
fn version_higher_than_with_offset_provides_headroom() {
    let existing = 100_u64;
    let offset = 5_u64;
    assert!(version_higher_than(existing, offset) > existing.saturating_add(offset));
}

// ── UsageRecord::try_from(UsageRecordRow) ─────────────────────────────────────
//
// A malformed `status` can no longer be represented in a `UsageRecordRow` --
// `UsageRecordStatusCode` is a closed `#[repr(i8)]` enum, so any Enum8
// discriminant outside {Active = 1, Inactive = 2} is rejected by the
// `clickhouse` crate's own schema validation before a row is ever
// constructed here (see `entity.rs` / `From<UsageRecordStatusCode>`).

#[test]
fn row_with_invalid_gts_id_maps_to_internal() {
    let mut row = make_row(Uuid::new_v4(), Uuid::new_v4(), 1_700_000_000_000_000, 1);
    row.gts_id = "not-valid".to_owned();
    assert!(matches!(
        UsageRecord::try_from(row),
        Err(UsageCollectorPluginError::Internal(_))
    ));
}

// ── batch dedup key uniqueness ────────────────────────────────────────────────

#[test]
fn two_distinct_rows_produce_distinct_dedup_keys() {
    let row1 = make_row(Uuid::from_u128(1), Uuid::from_u128(10), 1_000_000, 1);
    let mut row2 = make_row(Uuid::from_u128(2), Uuid::from_u128(10), 1_000_000, 1);
    row2.idempotency_key = "idem-2".to_owned();
    assert_ne!(row_dedup_key(&row1), row_dedup_key(&row2));
}

#[test]
fn same_tuple_produces_same_dedup_key() {
    let row1 = make_row(Uuid::from_u128(5), Uuid::from_u128(10), 2_000_000, 1);
    let row2 = make_row(Uuid::from_u128(5), Uuid::from_u128(10), 2_000_000, 2); // version differs
    assert_eq!(row_dedup_key(&row1), row_dedup_key(&row2));
}

/// The dedup key is the SPI's canonical tuple, not the record `id`. Keying on
/// `id` is what let a row whose `id` disagreed with its own canonical tuple slip
/// past the lookup and be re-inserted under an idempotency key already in use.
#[test]
fn dedup_key_excludes_id_and_includes_idempotency_key() {
    let tenant_id = Uuid::from_u128(10);
    let created_at_micros = 3_000_000_i64;

    let row = make_row(Uuid::from_u128(1), tenant_id, created_at_micros, 1);

    let mut mismatched_id = make_row(Uuid::from_u128(2), tenant_id, created_at_micros, 1);
    assert_eq!(
        row_dedup_key(&row),
        row_dedup_key(&mismatched_id),
        "a differing `id` must not move the dedup key, or the lookup misses the stored row"
    );

    mismatched_id.idempotency_key = "idem-2".to_owned();
    assert_ne!(
        row_dedup_key(&row),
        row_dedup_key(&mismatched_id),
        "a differing `idempotency_key` is a different dedup identity"
    );

    // The incoming-record projection agrees with the stored-row one.
    let record = make_record(Uuid::from_u128(3), tenant_id, created_at_micros);
    assert_eq!(
        row_dedup_key(&row),
        record_dedup_key(&record),
        "`record_dedup_key` must project the same tuple, independent of `id`"
    );
}

// ── prefer_dedup_row ──────────────────────────────────────────────────────────
//
// `ClickHouse` has no UNIQUE constraint, so rows written while the lookup was
// keyed on `id` can leave two rows sharing a dedup key with different `id`s.
// Both survive version resolution (distinct `id`s are distinct sort keys), so
// the lookup must choose between them deterministically and in favour of the
// caller's own record.

#[test]
fn prefer_dedup_row_takes_the_only_candidate() {
    let row = make_row(Uuid::from_u128(9), Uuid::from_u128(10), 1_000_000, 1);
    let chosen = prefer_dedup_row(None, row, Uuid::from_u128(9));
    assert_eq!(chosen.id, Uuid::from_u128(9));
}

#[test]
fn prefer_dedup_row_prefers_the_expected_id_over_a_lower_id() {
    let tenant_id = Uuid::from_u128(10);
    let expected = Uuid::from_u128(5);
    let twin = make_row(Uuid::from_u128(1), tenant_id, 1_000_000, 1);
    let honest = make_row(expected, tenant_id, 1_000_000, 1);

    // Whichever order the rows arrive in, the expected `id` wins — even though
    // the twin sorts lower.
    assert_eq!(
        prefer_dedup_row(Some(twin.clone()), honest.clone(), expected).id,
        expected
    );
    assert_eq!(
        prefer_dedup_row(Some(honest), twin, expected).id,
        expected,
        "an already-chosen exact match must not be displaced by a lower-`id` twin"
    );
}

#[test]
fn prefer_dedup_row_falls_back_to_the_lowest_id() {
    let tenant_id = Uuid::from_u128(10);
    let absent = Uuid::from_u128(99);
    let low = make_row(Uuid::from_u128(1), tenant_id, 1_000_000, 1);
    let high = make_row(Uuid::from_u128(2), tenant_id, 1_000_000, 1);

    // Order-independent when neither candidate is the expected `id`, so the
    // outcome never depends on which part ClickHouse read first.
    assert_eq!(
        prefer_dedup_row(Some(low.clone()), high.clone(), absent).id,
        Uuid::from_u128(1)
    );
    assert_eq!(
        prefer_dedup_row(Some(high), low, absent).id,
        Uuid::from_u128(1)
    );
}

// ── err_for_slot ──────────────────────────────────────────────────────────────
//
// `UsageCollectorPluginError` is deliberately not `Clone`, so `create_batch`
// rebuilds an equivalent value per variant to place in every slot an error
// covers. A variant that loses its payload here would downgrade a
// caller-visible outcome (e.g. a retryable `Transient` becoming an opaque
// `Internal`), so each arm is pinned.

#[test]
fn err_for_slot_preserves_transient_payload() {
    let src = UsageCollectorPluginError::Transient {
        detail: "backend unreachable".to_owned(),
        retry_after_seconds: Some(7),
    };
    match err_for_slot(&src) {
        UsageCollectorPluginError::Transient {
            detail,
            retry_after_seconds,
        } => {
            assert_eq!(detail, "backend unreachable");
            assert_eq!(
                retry_after_seconds,
                Some(7),
                "retry_after_seconds must survive so the host can honour the backoff"
            );
        }
        other => panic!("expected Transient, got {other:?}"),
    }
}

#[test]
fn err_for_slot_preserves_usage_type_not_found_gts_id() {
    let gts_id = UsageTypeGtsId::new(VCPU_GTS).unwrap();
    let src = UsageCollectorPluginError::UsageTypeNotFound {
        gts_id: gts_id.clone(),
    };
    match err_for_slot(&src) {
        UsageCollectorPluginError::UsageTypeNotFound { gts_id: got } => assert_eq!(got, gts_id),
        other => panic!("expected UsageTypeNotFound, got {other:?}"),
    }
}

#[test]
fn err_for_slot_preserves_internal_message() {
    let src = UsageCollectorPluginError::Internal("dedup lookup exploded".to_owned());
    match err_for_slot(&src) {
        UsageCollectorPluginError::Internal(msg) => assert_eq!(msg, "dedup lookup exploded"),
        other => panic!("expected Internal, got {other:?}"),
    }
}

/// The enum is `#[non_exhaustive]`, so an unmodelled variant must still degrade
/// to an `Internal` carrying the original text rather than being dropped.
#[test]
fn err_for_slot_falls_back_to_internal_for_other_variants() {
    let src = UsageCollectorPluginError::UsageRecordNotFound {
        id: Uuid::from_u128(7),
    };
    let rendered = src.to_string();
    match err_for_slot(&src) {
        UsageCollectorPluginError::Internal(msg) => assert_eq!(
            msg, rendered,
            "the fallback arm must carry the original error text"
        ),
        other => panic!("expected Internal fallback, got {other:?}"),
    }
}

/// An `IdempotencyConflict` — what the ingest queue reports for a row refused
/// against a differing pending row — keeps its variant and its payload, so
/// the gateway still maps it to the conflict response.
#[test]
fn err_for_slot_preserves_idempotency_conflict() {
    let src = UsageCollectorPluginError::IdempotencyConflict {
        idempotency_key: "idem-1".to_owned(),
        existing_id: Uuid::from_u128(7),
    };
    match err_for_slot(&src) {
        UsageCollectorPluginError::IdempotencyConflict {
            idempotency_key,
            existing_id,
        } => {
            assert_eq!(idempotency_key, "idem-1");
            assert_eq!(existing_id, Uuid::from_u128(7));
        }
        other => panic!("expected the conflict to be preserved, got {other:?}"),
    }
}

// ── push_metadata_filters ─────────────────────────────────────────────────────

/// Both the key and every value are bound, so no caller-supplied metadata text
/// reaches the SQL string.
///
/// The `values.is_empty()` -> `FALSE` arm of `push_metadata_filters` is not
/// covered: `MetadataFilter`'s fields are private and both `new` and its
/// `Deserialize` impl reject an empty value set, so an empty filter cannot be
/// constructed to pass in.
#[test]
fn metadata_filter_binds_key_and_every_value() {
    use usage_collector_sdk::MetadataFilter;

    use crate::infra::storage::query::translate::{SqlBind, SqlCtx};

    let filter = MetadataFilter::new("region", ["eu-west", "us-east"]).unwrap();
    let mut ctx = SqlCtx::new();
    let mut clauses = Vec::new();
    ChRecordStore::push_metadata_filters(std::slice::from_ref(&filter), &mut ctx, &mut clauses);

    assert_eq!(clauses, vec!["metadata[?] IN (?, ?)".to_owned()]);
    assert_eq!(ctx.binds.len(), 3, "one key bind plus one bind per value");
    assert!(matches!(&ctx.binds[0], SqlBind::Str(s) if s == "region"));
}

/// The gateway's `$filter` shape — tenant, `[from, …)` window, status — lands
/// in the one `WHERE` as separate conjuncts, in text order, with the binds in
/// the same order. Translating any conjunct out of order would still produce
/// valid SQL — with the tenant UUID bound to the status placeholder.
#[test]
fn push_filter_translates_every_conjunct_into_the_one_where() {
    use toolkit_odata::filter::{FilterField, FilterNode, FilterOp, ODataValue};
    use usage_collector_sdk::UsageRecordFilterField;

    use crate::infra::storage::query::translate::{SqlBind, SqlCtx};

    let field =
        |name: &str| UsageRecordFilterField::from_name(name).expect("field is on the schema");
    let tenant = Uuid::from_u128(7);
    let from =
        chrono::DateTime::from_timestamp_micros(1_767_225_600_000_000).expect("timestamp in range");

    let node = FilterNode::and(vec![
        FilterNode::binary(field("tenant_id"), FilterOp::Eq, ODataValue::Uuid(tenant)),
        FilterNode::binary(
            field("created_at"),
            FilterOp::Ge,
            ODataValue::DateTime(from),
        ),
        FilterNode::binary(
            field("status"),
            FilterOp::Eq,
            ODataValue::String("active".to_owned()),
        ),
    ]);

    // `gts_id` already occupies the clause list, exactly as at the call sites.
    let mut ctx = SqlCtx::new();
    let mut clauses = vec!["gts_id = ?".to_owned()];

    ChRecordStore::push_filter(&node, &mut ctx, &mut clauses).expect("filter translates");

    assert_eq!(
        clauses,
        vec![
            "gts_id = ?".to_owned(),
            "tenant_id = ?".to_owned(),
            "created_at >= fromUnixTimestamp64Micro(?)".to_owned(),
            "status = ?".to_owned(),
        ],
        "every conjunct joins the one WHERE in text order"
    );
    assert_eq!(ctx.binds.len(), 3);
    assert!(matches!(&ctx.binds[0], SqlBind::Uuid(u) if *u == tenant));
    assert!(matches!(
        &ctx.binds[1],
        SqlBind::DateTime64Micros(1_767_225_600_000_000)
    ));
    assert!(matches!(&ctx.binds[2], SqlBind::Str(s) if s == "active"));
}

// ── Offline store ─────────────────────────────────────────────────────────────

/// Build a store over an offline client.
///
/// Port 1 is reserved and never bound, so any query that is actually issued
/// fails fast (connection refused) instead of blocking. The `clickhouse` crate's
/// default address (`http://localhost:8123`) would let a real local server
/// answer these "offline" tests.
fn offline_store() -> ChRecordStore {
    ChRecordStore::new(
        clickhouse::Client::default().with_url("http://127.0.0.1:1"),
        Arc::new(Metrics::new()),
        // Generous: every assertion here either short-circuits before I/O or
        // fails fast on connection refused, so the deadline is never what a
        // test observes.
        std::time::Duration::from_secs(30),
        // The direct path: the offline tier exercises every statement a store
        // issues itself, which group commit would hand to the queue instead.
        true,
        None,
    )
}

/// Build a `UsageRecord` matching [`make_row`]'s canonical fields.
fn make_record(id: Uuid, tenant_id: Uuid, created_at_micros: i64) -> UsageRecord {
    use time::OffsetDateTime;
    use usage_collector_sdk::{IdempotencyKey, ResourceRef, UsageRecordStatus};

    UsageRecord {
        id,
        tenant_id,
        gts_id: UsageTypeGtsId::new(VCPU_GTS).unwrap(),
        value: Decimal::new(100, 0),
        created_at: OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(created_at_micros) * 1_000,
        )
        .unwrap(),
        resource_ref: ResourceRef::new("res-1".to_owned(), "vm".to_owned()).unwrap(),
        subject_ref: None,
        idempotency_key: IdempotencyKey::new("idem-1".to_owned()).unwrap(),
        corrects_id: None,
        status: UsageRecordStatus::Active,
        metadata: std::collections::BTreeMap::default(),
    }
}

/// [`make_record`] under a caller-chosen usage type, for multi-`gts_id` batches.
fn make_record_for(gts_id: &str, id: Uuid, tenant_id: Uuid, created_at_micros: i64) -> UsageRecord {
    UsageRecord {
        gts_id: UsageTypeGtsId::new(gts_id).unwrap(),
        ..make_record(id, tenant_id, created_at_micros)
    }
}

// ── Empty-input short circuits ────────────────────────────────────────────────

/// An all-absorbed / all-rejected batch leaves nothing to write; the insert must
/// then be skipped rather than sending an empty INSERT to `ClickHouse`.
#[tokio::test]
async fn insert_records_with_no_rows_is_a_no_op() {
    offline_store()
        .insert_records(&[], std::time::Instant::now(), InsertKind::Record)
        .await
        .expect("an empty row set must not touch the backend");
}

// ── insert_dedup_token ────────────────────────────────────────────────────────

/// Two statements writing the same rows must dedup against each other whatever
/// order the rows arrive in — a retried batch is not guaranteed to be composed
/// in the same order — and a row repeated inside one statement must not change
/// the token either.
#[test]
fn insert_dedup_token_is_order_insensitive_and_ignores_repeated_rows() {
    let tenant = Uuid::from_u128(2);
    let a = make_row(Uuid::from_u128(10), tenant, 1_700_000_000_000_000, 1);
    let b = make_row(Uuid::from_u128(11), tenant, 1_700_000_000_000_001, 1);
    let c = make_row(Uuid::from_u128(12), tenant, 1_700_000_000_000_002, 1);

    let forward = insert_dedup_token(&[a.clone(), b.clone(), c.clone()], InsertKind::Record);
    let reversed = insert_dedup_token(&[c.clone(), b.clone(), a.clone()], InsertKind::Record);
    let repeated = insert_dedup_token(&[a.clone(), a.clone(), b.clone(), c], InsertKind::Record);
    assert_eq!(forward, reversed, "row order must not enter the token");
    assert_eq!(forward, repeated, "a repeated row must not enter the token");

    let other = insert_dedup_token(&[a, b], InsertKind::Record);
    assert_ne!(
        forward, other,
        "a different row set must produce a different token"
    );
}

/// A single create and a batch containing only that record are the same write
/// and must dedup against each other at the engine.
#[test]
fn insert_dedup_token_single_row_equals_batch_of_one() {
    let row = make_row(
        Uuid::from_u128(10),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
        1,
    );
    assert_eq!(
        insert_dedup_token(std::slice::from_ref(&row), InsertKind::Record),
        insert_dedup_token(&[row], InsertKind::Record),
    );
}

/// A deactivation marker shares its source row's `id`, so without the kind
/// discriminator a cascade issued inside the dedup window of the create that
/// wrote those ids would be dropped as a retry of it.
#[test]
fn insert_dedup_token_discriminates_markers_from_records() {
    let row = make_row(
        Uuid::from_u128(10),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
        1,
    );
    let mut marker = row.clone();
    marker.status = UsageRecordStatusCode::Inactive;
    marker.version = row.version + 1;
    assert_ne!(
        insert_dedup_token(std::slice::from_ref(&row), InsertKind::Record),
        insert_dedup_token(std::slice::from_ref(&marker), InsertKind::Marker),
        "the same id under the two kinds must not share a token"
    );
    // Only `id` and `kind` enter the token: the payload, `status` and
    // `version` do not, so a retried marker set with fresh versions still
    // matches the one already written.
    let mut retried = marker.clone();
    retried.version += 5;
    assert_eq!(
        insert_dedup_token(std::slice::from_ref(&marker), InsertKind::Marker),
        insert_dedup_token(std::slice::from_ref(&retried), InsertKind::Marker),
    );
}

/// The token is a setting value on the wire; `ClickHouse` accepts any string,
/// but a UUID keeps it fixed-width and free of characters that would need
/// quoting in `system.query_log` queries.
#[test]
fn insert_dedup_token_is_a_uuid_string() {
    let row = make_row(
        Uuid::from_u128(10),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
        1,
    );
    let token = insert_dedup_token(&[row], InsertKind::Record);
    assert!(
        Uuid::parse_str(&token).is_ok(),
        "token must be a UUID: {token}"
    );
}

#[tokio::test]
async fn batch_dedup_lookup_with_no_records_returns_empty_map() {
    let found = offline_store()
        .batch_dedup_lookup(&[])
        .await
        .expect("an empty record set must not touch the backend");
    assert!(found.is_empty());
}

// ── Dedup-hit resolution ──────────────────────────────────────────────────────
//
// `canonical_equal` deliberately excludes `status`, so absorbing purely on it
// would let a create -> deactivate -> re-create of the same dedup key return
// `Ok` carrying the *inactive* stored row. The key is already bound to a
// record the caller cannot have back, which is an idempotency conflict.

#[test]
fn dedup_hit_on_an_inactive_stored_row_is_an_idempotency_conflict() {
    let id = Uuid::from_u128(7);
    let tenant_id = Uuid::from_u128(8);
    let created_at_micros = 1_700_000_000_000_000_i64;

    let mut row = make_row(id, tenant_id, created_at_micros, 1);
    row.status = UsageRecordStatusCode::Inactive;
    let record = make_record(id, tenant_id, created_at_micros);

    assert!(
        canonical_equal(&row, &record).unwrap(),
        "the canonical fields must match, so only `status` can drive the rejection"
    );

    match offline_store().resolve_dedup_hit(&row, &record) {
        Err(UsageCollectorPluginError::IdempotencyConflict { existing_id, .. }) => {
            assert_eq!(existing_id, id);
        }
        other => panic!("expected IdempotencyConflict, got {other:?}"),
    }
}

/// A stored row sharing the canonical dedup tuple but carrying a different `id`
/// is a corrupted identity: the key is already bound to a record the caller
/// cannot address, so it must fail closed rather than absorb.
///
/// This test is only meaningful because the lookup keys on the canonical tuple
/// ([`super::DedupKey`]) rather than on `id` — an `id`-keyed lookup would never
/// surface this row at all, and the create would silently insert a duplicate.
#[test]
fn dedup_hit_on_a_mismatched_id_row_is_an_idempotency_conflict() {
    let stored_id = Uuid::from_u128(7);
    let incoming_id = Uuid::from_u128(8);
    let tenant_id = Uuid::from_u128(9);
    let created_at_micros = 1_700_000_000_000_000_i64;

    let row = make_row(stored_id, tenant_id, created_at_micros, 1);
    let record = make_record(incoming_id, tenant_id, created_at_micros);

    assert_eq!(
        row_dedup_key(&row),
        record_dedup_key(&record),
        "the rows must share a dedup key, so only the mismatched `id` can drive the rejection"
    );
    assert_eq!(
        row.status,
        UsageRecordStatusCode::Active,
        "the stored row must be active, so `status` cannot drive the rejection either"
    );

    match offline_store().resolve_dedup_hit(&row, &record) {
        Err(UsageCollectorPluginError::IdempotencyConflict {
            existing_id,
            idempotency_key,
        }) => {
            assert_eq!(existing_id, stored_id, "the conflict names the stored row");
            assert_eq!(idempotency_key, "idem-1");
        }
        other => panic!("expected IdempotencyConflict, got {other:?}"),
    }
}

#[test]
fn dedup_hit_on_an_identical_active_row_is_absorbed() {
    let id = Uuid::from_u128(7);
    let tenant_id = Uuid::from_u128(8);
    let created_at_micros = 1_700_000_000_000_000_i64;

    let row = make_row(id, tenant_id, created_at_micros, 1);
    let record = make_record(id, tenant_id, created_at_micros);

    let absorbed = offline_store()
        .resolve_dedup_hit(&row, &record)
        .expect("an identical active row must be absorbed");
    assert_eq!(absorbed.id, id);
}

/// A second row for a dedup key already composed *within the same batch* goes
/// through the same comparison as one already stored, so a conflicting
/// in-batch duplicate is reported rather than silently swallowed.
#[test]
fn conflicting_in_batch_duplicate_is_an_idempotency_conflict() {
    let id = Uuid::from_u128(7);
    let tenant_id = Uuid::from_u128(8);
    let created_at_micros = 1_700_000_000_000_000_i64;

    let composed = make_row(id, tenant_id, created_at_micros, 1);
    let mut conflicting = make_record(id, tenant_id, created_at_micros);
    conflicting.value = Decimal::new(999, 0);

    assert_eq!(
        record_dedup_key(&conflicting),
        row_dedup_key(&composed),
        "the two rows must share a dedup key for this to be an in-batch duplicate"
    );

    match offline_store().resolve_dedup_hit(&composed, &conflicting) {
        Err(UsageCollectorPluginError::IdempotencyConflict { existing_id, .. }) => {
            assert_eq!(existing_id, id);
        }
        other => panic!("expected IdempotencyConflict, got {other:?}"),
    }
}

// ── Batch pipeline (no I/O) ───────────────────────────────────────────────────
//
// `create_batch` is three statements around pure composition. The pure parts
// are exercised here directly; the statements themselves are covered by the
// feature-gated live suite.

const RAM_GTS: &str = "gts.cf.core.uc.usage_record.v1~cf.compute._.ram_gb.v1";

/// One bound parameter per distinct usage type, in a deterministic order, and
/// no caller-supplied identifier in the SQL text itself.
#[test]
fn catalog_lookup_sql_binds_one_str_per_distinct_gts_id() {
    use std::collections::BTreeSet;

    use crate::infra::storage::query::translate::SqlBind;

    let gts_ids: BTreeSet<&str> = [VCPU_GTS, RAM_GTS, VCPU_GTS].into_iter().collect();
    let (sql, ctx) = catalog_lookup_sql(&gts_ids);

    assert_eq!(
        sql,
        "SELECT gts_id FROM usage_type_catalog WHERE gts_id IN (?, ?)"
    );
    assert_eq!(ctx.binds.len(), 2, "one bind per distinct gts_id");
    // `BTreeSet` order: RAM_GTS sorts before VCPU_GTS.
    assert!(matches!(&ctx.binds[0], SqlBind::Str(s) if s == RAM_GTS));
    assert!(matches!(&ctx.binds[1], SqlBind::Str(s) if s == VCPU_GTS));
    assert!(
        !sql.contains(VCPU_GTS),
        "identifiers are bound, never inlined into the SQL text"
    );
}

/// A missing usage type marks exactly the slots that reference it, at their
/// own input positions, and hands every other record on to the dedup step.
#[test]
fn catalog_miss_marks_only_that_gts_ids_slots() {
    let tenant_id = Uuid::from_u128(2);
    let records = vec![
        make_record_for(
            RAM_GTS,
            Uuid::from_u128(1),
            tenant_id,
            1_700_000_000_000_000,
        ),
        make_record_for(
            VCPU_GTS,
            Uuid::from_u128(3),
            tenant_id,
            1_700_000_000_000_001,
        ),
        make_record_for(
            RAM_GTS,
            Uuid::from_u128(4),
            tenant_id,
            1_700_000_000_000_002,
        ),
    ];
    let known: HashSet<String> = [VCPU_GTS.to_owned()].into_iter().collect();
    let mut outcomes = vec![None, None, None];

    let passed = split_by_catalog(&records, &known, &mut outcomes);

    assert_eq!(passed, vec![1], "only the registered type's record passes");
    for idx in [0, 2] {
        match &outcomes[idx] {
            Some(Err(UsageCollectorPluginError::UsageTypeNotFound { gts_id })) => {
                assert_eq!(gts_id.as_ref(), RAM_GTS, "slot {idx} names its own gts_id");
            }
            other => panic!("slot {idx} must be UsageTypeNotFound, got {other:?}"),
        }
    }
    assert!(
        outcomes[1].is_none(),
        "a passed slot is left for the dedup step to decide"
    );
}

/// Composed rows get contiguous versions from the batch base; a record absorbed
/// from storage composes no row and consumes no version; an identical in-batch
/// duplicate shares its twin's row and slot list.
#[test]
fn compose_batch_assigns_contiguous_versions_and_skips_absorbed_rows() {
    let tenant_id = Uuid::from_u128(8);
    let base_micros = 1_700_000_000_000_000_i64;
    let stored_id = Uuid::from_u128(7);
    let records = vec![
        make_record(Uuid::from_u128(1), tenant_id, base_micros + 1),
        // Already stored: absorbed, composes nothing.
        make_record(stored_id, tenant_id, base_micros),
        make_record(Uuid::from_u128(3), tenant_id, base_micros + 3),
        // Identical in-batch twin of the row above.
        make_record(Uuid::from_u128(3), tenant_id, base_micros + 3),
    ];
    let stored = make_row(stored_id, tenant_id, base_micros, 1);
    let existing: HashMap<_, _> = [(row_dedup_key(&stored), stored)].into_iter().collect();
    let passed = vec![0, 1, 2, 3];
    let mut outcomes = vec![None, None, None, None];

    let (to_insert, row_slots) =
        offline_store().compose_batch(&records, &passed, &existing, 1_000, &mut outcomes);

    let versions: Vec<u64> = to_insert.iter().map(|r| r.version).collect();
    assert_eq!(
        versions,
        vec![1_000, 1_001],
        "one version per composed row, contiguous from the base"
    );
    assert_eq!(
        row_slots,
        vec![vec![0], vec![2, 3]],
        "the twin hangs off its sibling's row rather than composing its own"
    );
    match &outcomes[1] {
        Some(Ok(absorbed)) => assert_eq!(absorbed.id, stored_id, "absorbed from storage"),
        other => panic!("slot 1 must be absorbed, got {other:?}"),
    }
    for idx in [0, 2, 3] {
        assert!(
            matches!(outcomes[idx], Some(Ok(_))),
            "slot {idx} must be Ok, got {:?}",
            outcomes[idx]
        );
    }
}

/// A backend failure on the batch's first read is still a per-record outcome,
/// never a batch-level `Err` that discards the whole submission.
///
/// The failure here comes from the unreachable server, so its classification
/// is whatever the client reports for a refused connection; the contract under
/// test is the per-record shape of the result, not the variant.
#[tokio::test]
async fn create_batch_reports_backend_failures_per_record() {
    let store = offline_store();
    let records = vec![make_record(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
    )];

    let outcomes = store
        .create_batch(records)
        .await
        .expect("a backend failure inside the batch is a per-record outcome");

    assert_eq!(outcomes.len(), 1);
    match outcomes.into_iter().next() {
        Some(Err(
            UsageCollectorPluginError::Transient { .. }
            | UsageCollectorPluginError::Internal { .. },
        )) => {}
        other => panic!("expected a per-record backend failure, got {other:?}"),
    }
}

/// The batch INSERT runs after the dedup SELECTs have already decided every
/// record's outcome. A failed write must therefore rewrite only the slots that
/// were waiting on it — the rows absorbed from storage keep their `Ok`, since
/// a write that never landed cannot invalidate a row that was already there.
///
/// (Driving a real SELECT-ok / INSERT-fail sequence end to end needs a live
/// `ClickHouse` that accepts the reads and rejects the write, which the
/// feature-gated suite covers; the mapping itself is asserted here.)
#[test]
fn a_failed_insert_rewrites_only_the_slots_it_backed() {
    use super::apply_insert_failure;

    let absorbed = make_record(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
    );
    let mut outcomes: Vec<Option<Result<UsageRecord, UsageCollectorPluginError>>> = vec![
        Some(Ok(absorbed)),
        Some(Ok(make_record(
            Uuid::from_u128(3),
            Uuid::from_u128(2),
            1_700_000_000_000_001,
        ))),
        Some(Ok(make_record(
            Uuid::from_u128(4),
            Uuid::from_u128(2),
            1_700_000_000_000_002,
        ))),
    ];
    // Slot 0 was absorbed from storage; slots 1 and 2 share one composed row
    // (an identical in-batch duplicate), so both hang off the same write.
    let row_slots = vec![vec![1_usize, 2_usize]];

    apply_insert_failure(
        &UsageCollectorPluginError::transient("insert failed (test stub)"),
        &row_slots,
        &mut outcomes,
    );

    assert!(
        matches!(outcomes[0], Some(Ok(_))),
        "an absorbed row must keep the outcome the dedup read decided"
    );
    for idx in [1, 2] {
        match &outcomes[idx] {
            Some(Err(UsageCollectorPluginError::Transient { .. })) => {}
            other => panic!("slot {idx} must carry the insert failure, got {other:?}"),
        }
    }
}

// ── parse_aggregate_response ──────────────────────────────────────────────────

#[test]
fn aggregate_response_parses_dimensions_and_decimal_values() {
    let dim_names = vec!["d0".to_owned(), "d1".to_owned()];
    let body = b"{\"d0\":\"tenant-a\",\"d1\":\"vm\",\"agg\":\"12.5\"}\n\
                 {\"d0\":\"tenant-b\",\"d1\":\"disk\",\"agg\":7}\n";

    let buckets = parse_aggregate_response(body, &dim_names).expect("well-formed NDJSON");

    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0].key, vec!["tenant-a", "vm"]);
    assert_eq!(
        buckets[0].value.as_ref().map(ToString::to_string),
        Some("12.5".to_owned())
    );
    assert_eq!(buckets[1].key, vec!["tenant-b", "disk"]);
    assert_eq!(
        buckets[1].value.as_ref().map(ToString::to_string),
        Some("7".to_owned())
    );
}

/// The quoted form is exact; the unquoted form is not — which is precisely why
/// `aggregate` reads its result with `output_format_json_quote_decimals = 1`.
///
/// `serde_json` is built without `arbitrary_precision`, so an unquoted
/// *fractional* number is stored as an `f64` and a `Decimal128(9)` wider than
/// ~15 significant digits comes back carrying digits that were never in the
/// data. Both cases above this test use whole numbers (`7`, `2`), which survive
/// the `f64` round-trip intact and so cannot detect the difference.
///
/// The parser deliberately keeps accepting both shapes; the query-side setting
/// is what guarantees the exact branch is the one taken in production.
#[test]
fn aggregate_response_is_exact_only_for_a_quoted_decimal() {
    let dim_names: Vec<String> = Vec::new();
    let exact = "1234567890.123456789";

    let quoted =
        parse_aggregate_response(format!("{{\"agg\":\"{exact}\"}}\n").as_bytes(), &dim_names)
            .expect("quoted decimal");
    assert_eq!(
        quoted[0].value.as_ref().map(ToString::to_string),
        Some(exact.to_owned()),
        "the quoted branch is exact end-to-end"
    );

    let unquoted =
        parse_aggregate_response(format!("{{\"agg\":{exact}}}\n").as_bytes(), &dim_names)
            .expect("an unquoted decimal still parses");
    assert_ne!(
        unquoted[0].value, quoted[0].value,
        "an unquoted fractional number has already lost precision through f64"
    );
}

/// An empty `MIN`/`MAX`/`AVG` group comes back as JSON `null`, which is a
/// valid absent value rather than a parse failure. Blank lines between rows
/// are skipped, and a missing dimension key decodes as an empty component.
#[test]
fn aggregate_response_handles_null_values_blank_lines_and_missing_dimensions() {
    let dim_names = vec!["d0".to_owned()];
    let body = b"{\"d0\":\"tenant-a\",\"agg\":null}\n\n{\"agg\":\"3\"}\n";

    let buckets = parse_aggregate_response(body, &dim_names).expect("well-formed NDJSON");

    assert_eq!(buckets.len(), 2, "the blank line must be skipped");
    assert_eq!(buckets[0].key, vec!["tenant-a"]);
    assert!(buckets[0].value.is_none());
    assert_eq!(buckets[1].key, vec![String::new()]);
}

#[test]
fn aggregate_response_with_an_ungrouped_query_yields_one_keyless_bucket() {
    let buckets = parse_aggregate_response(b"{\"agg\":\"42\"}\n", &[]).expect("well-formed NDJSON");
    assert_eq!(buckets.len(), 1);
    assert!(buckets[0].key.is_empty());
}

#[test]
fn aggregate_response_rejects_malformed_json() {
    let err = parse_aggregate_response(b"{not json}\n", &[])
        .expect_err("a malformed line must not be silently dropped");
    assert!(matches!(err, UsageCollectorPluginError::Internal(_)));
}

#[test]
fn aggregate_response_rejects_an_unexpected_value_type() {
    let err = parse_aggregate_response(b"{\"agg\":[1,2]}\n", &[])
        .expect_err("an array aggregate value is not decodable as a decimal");
    assert!(matches!(err, UsageCollectorPluginError::Internal(_)));
}

/// Chunk boundaries that split a JSON line mid-object must still decode once
/// the newline arrives — this is the streaming path `aggregate` uses.
#[test]
fn aggregate_response_stream_parses_across_chunk_boundaries() {
    let dim_names = vec!["d0".to_owned()];
    let mut parser = AggregateNdjsonParser::new(dim_names);
    parser
        .push_chunk(br#"{"d0":"tenan"#)
        .expect("partial first chunk is not yet a line");
    parser
        .push_chunk(
            br#"t-a","agg":"1"}
{"d0":"ten"#,
        )
        .expect("first complete line + partial second");
    parser
        .push_chunk(
            br#"ant-b","agg":2}
"#,
        )
        .expect("second line completes");

    let buckets = parser.finish().expect("streamed NDJSON");
    assert_eq!(buckets.len(), 2);
    assert_eq!(buckets[0].key, vec!["tenant-a"]);
    assert_eq!(
        buckets[0].value.as_ref().map(ToString::to_string),
        Some("1".to_owned())
    );
    assert_eq!(buckets[1].key, vec!["tenant-b"]);
    assert_eq!(
        buckets[1].value.as_ref().map(ToString::to_string),
        Some("2".to_owned())
    );
}

// ── build_aggregate_sql / build_list_sql ──────────────────────────────────────

/// The realistic clause set: `gts_id` is always pinned, `status = 'active'`
/// always follows it, and non-`SUM` ops add the `corrects_id` partition.
fn clauses() -> Vec<String> {
    vec![
        "gts_id = ?".to_owned(),
        "status = 'active'".to_owned(),
        "corrects_id IS NULL".to_owned(),
    ]
}

/// The aggregate is one `FINAL` read with one `WHERE`: no `GROUP BY` on the
/// sort key, no `HAVING`, no `LIMIT 1 BY`, no `argMax`, no subquery.
/// Deactivation and duplicate creates are both collapsed by `FINAL` before
/// the `status = 'active'` clause applies.
#[test]
fn aggregate_sql_reads_final_with_one_where() {
    let sql = ChRecordStore::build_aggregate_sql("SUM(value) AS agg", &clauses(), "", "");
    assert_eq!(
        sql,
        "SELECT SUM(value) AS agg FROM usage_records FINAL \
         WHERE gts_id = ? AND status = 'active' AND corrects_id IS NULL"
    );
    for forbidden in [
        "HAVING",
        "LIMIT 1 BY",
        "argMax",
        "GROUP BY gts_id",
        "version",
        "NOT IN",
    ] {
        assert!(
            !sql.contains(forbidden),
            "no resolution construct may remain (`{forbidden}`): {sql}"
        );
    }
    assert_eq!(sql.matches("SELECT").count(), 1, "one level: {sql}");
    assert_eq!(sql.matches("WHERE").count(), 1, "one WHERE: {sql}");
    assert!(
        sql.contains(" FINAL WHERE "),
        "FINAL qualifies the table: {sql}"
    );
}

/// Every placeholder in the text is one bind: clauses appear exactly once, so
/// the caller applies each clause bind exactly once.
#[test]
fn aggregate_sql_binds_each_placeholder_once() {
    let mut scan = clauses();
    scan.push("created_at >= fromUnixTimestamp64Micro(?)".to_owned());
    scan.push("metadata[?] IN (?, ?)".to_owned());
    let placeholders: usize = scan.iter().map(|c| c.matches('?').count()).sum();
    let sql = ChRecordStore::build_aggregate_sql("SUM(value) AS agg", &scan, "", "");
    assert_eq!(sql.matches('?').count(), placeholders, "{sql}");
    let text = scan.join(" AND ");
    assert_eq!(sql.matches(text.as_str()).count(), 1, "{sql}");
}

/// Grouping and the bucket cap trail the `WHERE`.
#[test]
fn aggregate_sql_appends_grouping_and_limit_after_the_where() {
    let sql = ChRecordStore::build_aggregate_sql(
        "SUM(value) AS agg",
        &clauses(),
        " GROUP BY 1",
        " LIMIT 100001",
    );
    assert!(
        sql.ends_with("AND corrects_id IS NULL GROUP BY 1 LIMIT 100001"),
        "{sql}"
    );
}

/// The syntax-error guard stated as a property rather than as literal strings,
/// across every combination of the branches that vary.
#[test]
fn aggregate_sql_never_emits_an_empty_clause_or_a_double_space() {
    let extra_variants: [Vec<String>; 2] = [vec![], vec!["status = ?".to_owned()]];
    let grouping_variants = [("", ""), (" GROUP BY 1", " LIMIT 100001")];

    for extra in &extra_variants {
        for (group_by, limit_clause) in grouping_variants {
            let mut all = clauses();
            all.extend(extra.iter().cloned());
            let sql = ChRecordStore::build_aggregate_sql(
                "SUM(value) AS agg",
                &all,
                group_by,
                limit_clause,
            );
            let ctx = format!("extra={extra:?} group_by={group_by:?} sql={sql}");
            assert!(!sql.contains("  "), "double space: {ctx}");
            assert!(!sql.contains("WHERE )"), "empty WHERE before close: {ctx}");
            assert!(!sql.contains("WHERE GROUP BY"), "empty WHERE: {ctx}");
            assert!(!sql.contains("WHERE LIMIT"), "empty WHERE: {ctx}");
            assert!(!sql.contains("AND AND"), "empty conjunct: {ctx}");
            assert!(!sql.contains("AND GROUP BY"), "dangling AND: {ctx}");
            assert!(!sql.ends_with("WHERE"), "trailing WHERE: {ctx}");
            assert!(!sql.ends_with("AND"), "trailing AND: {ctx}");
        }
    }
}

/// The ungrouped shape (`dim_count == 0`) appends nothing after the last
/// clause. The gateway depends on this call producing exactly one bucket, so
/// the text must not gain a stray clause that could filter it away.
#[test]
fn aggregate_sql_ungrouped_ends_at_its_last_clause() {
    let sql = ChRecordStore::build_aggregate_sql("SUM(value) AS agg", &clauses(), "", "");
    assert!(sql.ends_with("AND corrects_id IS NULL"), "{sql}");
}

/// The key-prefix predicates — `tenant_id` and `created_at` reach `aggregate`
/// only through `$filter`, and with `gts_id` they are the whole sort-key
/// prefix — are conjuncts of the one `WHERE`, where they prune the read.
#[test]
fn aggregate_sql_keeps_key_prefix_predicates_in_the_where() {
    let mut scan = clauses();
    scan.push("tenant_id = ?".to_owned());
    scan.push("created_at >= fromUnixTimestamp64Micro(?)".to_owned());
    let sql = ChRecordStore::build_aggregate_sql("SUM(value) AS agg", &scan, "", "");
    assert_eq!(
        sql,
        "SELECT SUM(value) AS agg FROM usage_records FINAL WHERE gts_id = ? AND \
         status = 'active' AND corrects_id IS NULL AND tenant_id = ? AND \
         created_at >= fromUnixTimestamp64Micro(?)"
    );
}

/// `list` is likewise one `FINAL` read: no version sort, no `LIMIT 1 BY`, no
/// subquery, the caller's `ORDER BY` and look-ahead `LIMIT` applied once after
/// every predicate. There is no `status` clause of its own — `list` returns
/// inactive rows at their resolved status.
#[test]
fn list_sql_reads_final_then_orders_and_limits() {
    let scan = vec!["gts_id = ?".to_owned()];
    let sql = ChRecordStore::build_list_sql(&scan, "created_at ASC, id ASC", 101);
    assert_eq!(
        sql,
        format!(
            "SELECT {} FROM usage_records FINAL WHERE gts_id = ? \
             ORDER BY created_at ASC, id ASC LIMIT 101",
            super::RECORD_COLUMNS
        )
    );
    for forbidden in ["LIMIT 1 BY", "version DESC", "argMax", "GROUP BY", "NOT IN"] {
        assert!(
            !sql.contains(forbidden),
            "no resolution construct may remain (`{forbidden}`): {sql}"
        );
    }
    assert!(
        !sql.contains("status ="),
        "list carries no status predicate of its own: {sql}"
    );
}

/// Bind contract for `list`: every clause once, in text order, the keyset
/// tuple last before `ORDER BY`.
#[test]
fn list_sql_places_the_keyset_predicate_last_before_order_by() {
    let scan = vec![
        "gts_id = ?".to_owned(),
        "tenant_id = ?".to_owned(),
        "metadata[?] IN (?)".to_owned(),
        "status = ?".to_owned(),
        "(created_at, id) > (fromUnixTimestamp64Micro(?), ?)".to_owned(),
    ];
    let sql = ChRecordStore::build_list_sql(&scan, "created_at ASC, id ASC", 51);
    let placeholders: usize = scan.iter().map(|c| c.matches('?').count()).sum();
    assert_eq!(
        sql.matches('?').count(),
        placeholders,
        "each bind once: {sql}"
    );
    assert!(
        sql.ends_with(
            "AND status = ? AND (created_at, id) > (fromUnixTimestamp64Micro(?), ?) \
             ORDER BY created_at ASC, id ASC LIMIT 51"
        ),
        "status, then keyset, then ORDER BY / LIMIT: {sql}"
    );
}

// ── finalize_outcomes ────────────────────────────────────────────────────────

/// The SPI result vector is positional: result `i` belongs to input `i`. This
/// must hold whatever mix of successes and failures the slots carry.
#[test]
fn finalize_outcomes_preserves_positional_order() {
    let tenant = Uuid::from_u128(11);
    let base = 1_700_000_000_000_000_i64;
    let records = vec![
        make_record(Uuid::from_u128(1), tenant, base),
        make_record(Uuid::from_u128(2), tenant, base + 1),
    ];
    let outcomes = vec![
        Some(Ok(records[0].clone())),
        Some(Err(UsageCollectorPluginError::internal("slot 1 failed"))),
    ];

    let finalized = super::finalize_outcomes(outcomes, &records);

    assert_eq!(finalized.len(), 2);
    match &finalized[0] {
        Ok(r) => assert_eq!(r.id, records[0].id, "slot 0 keeps its own record"),
        other => panic!("slot 0 must stay Ok, got {other:?}"),
    }
    assert!(finalized[1].is_err(), "slot 1 keeps its own failure");
}

/// An unfilled slot means a record was neither written, absorbed, nor rejected
/// — the batch loop lost it. Dropping it would shorten the result vector and
/// silently re-index every later record's outcome onto the wrong input; that is
/// far worse than reporting a failure, so it is reported as an invariant break.
#[test]
fn finalize_outcomes_reports_an_unresolved_slot_as_an_invariant_break() {
    let tenant = Uuid::from_u128(12);
    let base = 1_700_000_000_000_000_i64;
    let records = vec![
        make_record(Uuid::from_u128(1), tenant, base),
        make_record(Uuid::from_u128(2), tenant, base + 1),
    ];
    // Slot 0 never resolved.
    let outcomes = vec![None, Some(Ok(records[1].clone()))];

    let finalized = super::finalize_outcomes(outcomes, &records);

    assert_eq!(
        finalized.len(),
        2,
        "the result vector must stay the same length as the input, so positions still line up"
    );
    match &finalized[0] {
        Err(UsageCollectorPluginError::Internal(msg)) => assert!(
            msg.contains("invariant break"),
            "the unresolved slot must say so, got: {msg}"
        ),
        other => panic!("an unresolved slot must be an Internal error, got {other:?}"),
    }
    assert!(
        finalized[1].is_ok(),
        "a resolved neighbour must be untouched"
    );
}

// ── compose_batch: compensation rows ─────────────────────────────────────────

/// A composed row carrying `corrects_id` is a compensation and is counted as
/// one. The count is per *composed* row, so an in-batch twin of a compensation
/// must not double-count it.
#[test]
fn compose_batch_composes_compensation_rows_once_per_row() {
    let tenant = Uuid::from_u128(13);
    let base = 1_700_000_000_000_000_i64;
    let corrected = Uuid::from_u128(99);

    let mut compensation = make_record(Uuid::from_u128(1), tenant, base);
    compensation.corrects_id = Some(corrected);
    let records = vec![compensation.clone(), compensation];

    let existing = HashMap::new();
    let passed = vec![0, 1];
    let mut outcomes = vec![None, None];

    let (to_insert, row_slots) =
        offline_store().compose_batch(&records, &passed, &existing, 500, &mut outcomes);

    assert_eq!(
        to_insert.len(),
        1,
        "the identical twin shares the composed row"
    );
    assert_eq!(
        to_insert[0].corrects_id,
        Some(corrected),
        "the composed row must carry the corrected id through to storage"
    );
    assert_eq!(
        row_slots,
        vec![vec![0, 1]],
        "both input slots depend on the one composed row landing"
    );
    for idx in [0, 1] {
        assert!(
            matches!(outcomes[idx], Some(Ok(_))),
            "slot {idx} must be Ok, got {:?}",
            outcomes[idx]
        );
    }
}

// ── Aggregate NDJSON decoding: rejection paths ───────────────────────────────

/// An `agg` value that is a well-typed JSON string but not a decimal is a
/// protocol disagreement, not a value — reporting it as `None` would silently
/// turn a broken response into an empty bucket.
#[test]
fn aggregate_response_rejects_an_unparseable_decimal() {
    let err = parse_aggregate_response(b"{\"agg\":\"not-a-decimal\"}\n", &[])
        .expect_err("a non-decimal agg string must be refused");

    let msg = err.to_string();
    assert!(
        msg.contains("aggregate value parse error"),
        "expected a decimal parse failure, got: {msg}"
    );
}

/// The response is decoded as UTF-8 per line; invalid bytes in a *complete*
/// line are refused rather than lossily replaced, which would corrupt a
/// dimension key.
#[test]
fn aggregate_response_rejects_invalid_utf8_in_a_complete_line() {
    let mut body = b"{\"agg\":\"1\",\"d0\":\"".to_vec();
    body.push(0xFF);
    body.extend_from_slice(b"\"}\n");

    let err = parse_aggregate_response(&body, &["d0".to_owned()])
        .expect_err("invalid UTF-8 in a complete line must be refused");

    let msg = err.to_string();
    assert!(
        msg.contains("utf-8"),
        "expected a UTF-8 decode failure, got: {msg}"
    );
}

/// Same guard on the unterminated final line, which `finish` decodes on its own
/// path rather than through the newline scan.
#[test]
fn aggregate_response_rejects_invalid_utf8_in_an_unterminated_final_line() {
    let mut body = b"{\"agg\":\"1\",\"d0\":\"".to_vec();
    body.push(0xFF);
    body.extend_from_slice(b"\"}"); // no trailing newline

    let err = parse_aggregate_response(&body, &["d0".to_owned()])
        .expect_err("invalid UTF-8 in the trailing line must be refused");

    let msg = err.to_string();
    assert!(
        msg.contains("utf-8"),
        "expected a UTF-8 decode failure, got: {msg}"
    );
}

/// `finish` must also parse a well-formed final line that never got a newline —
/// `ClickHouse` does terminate `JSONEachRow` rows, but a body truncated at the
/// transport would otherwise lose its last bucket silently.
#[test]
fn aggregate_response_parses_an_unterminated_final_line() {
    let buckets = parse_aggregate_response(
        b"{\"d0\":\"a\",\"agg\":\"1\"}\n{\"d0\":\"b\",\"agg\":\"2\"}",
        &["d0".to_owned()],
    )
    .expect("both lines decode");

    assert_eq!(
        buckets.len(),
        2,
        "the unterminated final line must still yield its bucket"
    );
    assert_eq!(buckets[1].key, vec!["b".to_owned()]);
}

// ── Offline store variants ───────────────────────────────────────────────────

/// A store whose client skips `Client::insert`'s table-metadata round trip.
///
/// Insert validation is on by default and makes acquiring the handle itself a
/// request, which against an unreachable endpoint fails before `write` / `end`
/// are ever reached. Disabling it is what lets the offline tier exercise the
/// rest of the insert path.
fn offline_store_without_insert_validation() -> ChRecordStore {
    ChRecordStore::new(
        clickhouse::Client::default()
            .with_url("http://127.0.0.1:1")
            .with_validation(false),
        Arc::new(Metrics::new()),
        std::time::Duration::from_secs(30),
        true,
        None,
    )
}

fn assert_backend_failure(err: &UsageCollectorPluginError, what: &str) {
    match err {
        UsageCollectorPluginError::Transient { .. } | UsageCollectorPluginError::Internal(_) => {}
        other => panic!("{what} must surface as a backend error, got {other:?}"),
    }
}

// ── Write paths reach the backend ────────────────────────────────────────────

/// The single-row insert must report the write it could not make. Reaching the
/// failure through `write`/`end` rather than through handle acquisition is what
/// the validation-free client buys.
#[tokio::test]
async fn insert_record_surfaces_a_backend_failure() {
    let store = offline_store_without_insert_validation();
    let row = make_row(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
        1,
    );

    let err = store
        .insert_record(&row, std::time::Instant::now())
        .await
        .expect_err("a single-row insert cannot succeed against an unreachable backend");

    assert_backend_failure(&err, "a failed single-row insert");
}

/// The batch insert path, including the per-row `write` loop that an empty
/// batch short-circuits past.
#[tokio::test]
async fn insert_records_surfaces_a_backend_failure_for_a_non_empty_batch() {
    let store = offline_store_without_insert_validation();
    let tenant = Uuid::from_u128(2);
    let base = 1_700_000_000_000_000_i64;
    let rows = vec![
        make_row(Uuid::from_u128(1), tenant, base, 1),
        make_row(Uuid::from_u128(2), tenant, base + 1, 2),
    ];

    let err = store
        .insert_records(&rows, std::time::Instant::now(), InsertKind::Record)
        .await
        .expect_err("a batch insert cannot succeed against an unreachable backend");

    assert_backend_failure(&err, "a failed batch insert");
}

/// `create`'s first statement is the catalog existence check, so an unreachable
/// backend must not read as "the type is missing" — the caller would treat that
/// as an authoritative rejection of a perfectly valid record.
#[tokio::test]
async fn create_does_not_report_usage_type_not_found_when_the_catalog_read_never_completed() {
    let store = offline_store();
    let record = make_record(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
    );

    let err = store
        .create(record)
        .await
        .expect_err("create cannot succeed against an unreachable backend");

    assert!(
        !matches!(err, UsageCollectorPluginError::UsageTypeNotFound { .. }),
        "an unreachable backend must never be reported as a missing usage type, got {err:?}"
    );
    assert_backend_failure(&err, "a failed catalog existence check");
}

/// The dedup pre-read, reached directly. A failure here must propagate rather
/// than read as `None` ("no earlier row"), which would turn a retry into a
/// duplicate write.
#[tokio::test]
async fn dedup_point_lookup_surfaces_a_backend_failure() {
    let store = offline_store();
    let record = make_record(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
    );

    let err = store
        .dedup_point_lookup(&record)
        .await
        .expect_err("the dedup pre-read cannot succeed against an unreachable backend");

    assert_backend_failure(&err, "a failed dedup pre-read");
}

/// `deactivate` reads the target before composing any marker, so an unreachable
/// backend must not read as `UsageRecordNotFound` — the gateway distinguishes
/// that from a retryable failure.
#[tokio::test]
async fn deactivate_does_not_report_not_found_when_the_target_read_never_completed() {
    let store = offline_store();

    let err = store
        .deactivate(Uuid::from_u128(1))
        .await
        .expect_err("deactivate cannot succeed against an unreachable backend");

    assert!(
        !matches!(err, UsageCollectorPluginError::UsageRecordNotFound { .. }),
        "an unreachable backend must never be reported as an absent record, got {err:?}"
    );
    assert_backend_failure(&err, "a failed deactivation target read");
}

// ── list / aggregate: guards that run before any I/O ─────────────────────────
//
// Each rejection below returns before a statement is issued, so it is
// assertable offline — and asserting the message rather than just "some error"
// is what distinguishes a guard that fired from a connection that failed.

fn record_cursor(keys: &[&str], signed_order: &str, filter_hash: Option<&str>) -> CursorV1 {
    CursorV1 {
        k: keys.iter().map(|k| (*k).to_owned()).collect(),
        o: SortDir::Asc,
        s: signed_order.to_owned(),
        f: filter_hash.map(str::to_owned),
        d: "fwd".to_owned(),
    }
}

fn created_at_id_asc() -> ODataOrderBy {
    ODataOrderBy(vec![
        OrderKey {
            field: "created_at".to_owned(),
            dir: SortDir::Asc,
        },
        OrderKey {
            field: "id".to_owned(),
            dir: SortDir::Asc,
        },
    ])
}

fn vcpu_gts() -> UsageTypeGtsId {
    UsageTypeGtsId::new(VCPU_GTS).expect("valid gts_id")
}

/// Only forward paging is minted in v1; a `"bwd"` token would be walked forward
/// because the keyset operator comes from the sort direction, not `cursor.d`.
#[tokio::test]
async fn list_rejects_a_backward_cursor() {
    let store = offline_store();
    let mut cursor = record_cursor(
        &[
            "2026-08-10T11:00:00Z",
            "00000000-0000-4000-8000-000000000001",
        ],
        "+created_at,+id",
        None,
    );
    cursor.d = "bwd".to_owned();
    let query = ODataQuery::new()
        .with_order(created_at_id_asc())
        .with_cursor(cursor);

    let err = store
        .list(vcpu_gts(), &query, &[])
        .await
        .expect_err("a backward cursor must be refused");

    let msg = err.to_string();
    assert!(
        msg.contains("only forward paging is supported"),
        "expected a direction rejection, got: {msg}"
    );
}

/// A token minted under a different `$filter` describes a different row set.
#[tokio::test]
async fn list_rejects_a_cursor_whose_filter_hash_does_not_match() {
    let store = offline_store();
    let query = ODataQuery::new()
        .with_order(created_at_id_asc())
        .with_cursor(record_cursor(
            &[
                "2026-08-10T11:00:00Z",
                "00000000-0000-4000-8000-000000000001",
            ],
            "+created_at,+id",
            Some("hash-from-another-query"),
        ));

    let err = store
        .list(vcpu_gts(), &query, &[])
        .await
        .expect_err("a cursor from a differently-filtered query must be refused");

    let msg = err.to_string();
    assert!(
        msg.contains("cursor filter hash mismatch"),
        "expected a filter-hash rejection, got: {msg}"
    );
}

/// Unlike the catalog list, `list` honours `query.order` — so the cursor is
/// checked against the caller's order, and a token minted under another one
/// cannot be walked forward.
#[tokio::test]
async fn list_rejects_a_cursor_minted_under_a_different_sort_order() {
    let store = offline_store();
    let query = ODataQuery::new()
        .with_order(created_at_id_asc())
        .with_cursor(record_cursor(
            &[
                "2026-08-10T11:00:00Z",
                "00000000-0000-4000-8000-000000000001",
            ],
            "-created_at,-id",
            None,
        ));

    let err = store
        .list(vcpu_gts(), &query, &[])
        .await
        .expect_err("a cursor minted under a different order must be refused");

    let msg = err.to_string();
    assert!(
        msg.contains("cursor sort order mismatch"),
        "expected a sort-order rejection, got: {msg}"
    );
}

/// The complement of the three rejections above: a cursor that validates builds
/// its keyset predicate and the call goes on to issue a statement, which is
/// what reaching a backend error proves.
#[tokio::test]
async fn list_accepts_a_matching_cursor_and_reaches_the_backend() {
    let store = offline_store();
    let query = ODataQuery::new()
        .with_order(created_at_id_asc())
        .with_cursor(record_cursor(
            &[
                "2026-08-10T11:00:00Z",
                "00000000-0000-4000-8000-000000000001",
            ],
            "+created_at,+id",
            None,
        ));

    let err = store
        .list(vcpu_gts(), &query, &[])
        .await
        .expect_err("the statement cannot succeed against an unreachable backend");

    assert!(
        matches!(err, UsageCollectorPluginError::Transient { .. }),
        "a cursor that validates must fail at the backend, not as a cursor rejection: {err:?}"
    );
}

/// `$filter` is translated through the `UsageRecordFilterField` allowlist, so a
/// name that is not on it is refused before any SQL is built and no unvetted
/// identifier can reach the statement text.
#[tokio::test]
async fn list_rejects_a_filter_naming_a_field_outside_the_allowlist() {
    use toolkit_odata::ast::{CompareOperator, Expr, Value};

    let store = offline_store();
    let query = ODataQuery::new()
        .with_order(created_at_id_asc())
        .with_filter(Expr::Compare(
            Box::new(Expr::Identifier(
                "definitely_not_a_record_column".to_owned(),
            )),
            CompareOperator::Eq,
            Box::new(Expr::Value(Value::String("x".to_owned()))),
        ));

    let err = store
        .list(vcpu_gts(), &query, &[])
        .await
        .expect_err("an unknown filter field must be refused");

    let msg = err.to_string();
    assert!(
        msg.contains("invalid filter"),
        "expected a filter-translation rejection, got: {msg}"
    );
}

/// The same allowlist guard on the aggregate path.
#[tokio::test]
async fn aggregate_rejects_a_filter_naming_a_field_outside_the_allowlist() {
    use toolkit_odata::ast::{CompareOperator, Expr, Value};
    use usage_collector_sdk::{AggregationOp, AggregationSpec};

    let store = offline_store();
    let query = ODataQuery::new().with_filter(Expr::Compare(
        Box::new(Expr::Identifier(
            "definitely_not_a_record_column".to_owned(),
        )),
        CompareOperator::Eq,
        Box::new(Expr::Value(Value::String("x".to_owned()))),
    ));

    let err = store
        .aggregate(
            vcpu_gts(),
            &query,
            &[],
            AggregationSpec {
                op: AggregationOp::Sum,
                group_by: Vec::new(),
            },
        )
        .await
        .expect_err("an unknown filter field must be refused");

    let msg = err.to_string();
    assert!(
        msg.contains("invalid filter"),
        "expected a filter-translation rejection, got: {msg}"
    );
}

/// A grouped aggregate builds its dimension SELECT list, the subject-not-null
/// guards, the `GROUP BY` ordinals and the bucket-cap `LIMIT`, then issues the
/// statement — so reaching a backend error proves the whole assembly ran.
///
/// The dimensions are chosen to hit every shape: a plain column, both
/// subject guards, and a metadata key (which contributes a SELECT-list bind
/// that must be applied before any `WHERE` placeholder).
#[tokio::test]
async fn a_grouped_aggregate_assembles_its_query_and_reaches_the_backend() {
    use usage_collector_sdk::{
        AggregationDimension, AggregationOp, AggregationSpec, MetadataFilter, MetadataKey,
    };

    let store = offline_store();
    let metadata_filter =
        MetadataFilter::new("region", ["eu-west"]).expect("a valid metadata filter");

    let err = store
        .aggregate(
            vcpu_gts(),
            &ODataQuery::new(),
            std::slice::from_ref(&metadata_filter),
            AggregationSpec {
                op: AggregationOp::Sum,
                group_by: vec![
                    AggregationDimension::TenantId,
                    AggregationDimension::SubjectId,
                    AggregationDimension::SubjectType,
                    AggregationDimension::Metadata(
                        MetadataKey::new("region").expect("a valid metadata key"),
                    ),
                ],
            },
        )
        .await
        .expect_err("the statement cannot succeed against an unreachable backend");

    assert!(
        matches!(err, UsageCollectorPluginError::Transient { .. }),
        "a fully-assembled aggregate must fail at the backend, not while building: {err:?}"
    );
}

/// The ungrouped aggregate emits no `GROUP BY` and no dimension aliases — the
/// other side of the `dim_count == 0` branch.
#[tokio::test]
async fn an_ungrouped_aggregate_assembles_its_query_and_reaches_the_backend() {
    use usage_collector_sdk::{AggregationOp, AggregationSpec};

    let store = offline_store();

    let err = store
        .aggregate(
            vcpu_gts(),
            &ODataQuery::new(),
            &[],
            AggregationSpec {
                op: AggregationOp::Count,
                group_by: Vec::new(),
            },
        )
        .await
        .expect_err("the statement cannot succeed against an unreachable backend");

    assert!(
        matches!(err, UsageCollectorPluginError::Transient { .. }),
        "an ungrouped aggregate must fail at the backend, not while building: {err:?}"
    );
}

/// Mirrors the reference plugin's `acquire_failure_clears_ready_gauge`: a
/// real `ChError::Network` from a refused connection, flowing through the
/// production `get` path, must clear `uc_clickhouse_ready` to 0. No Docker —
/// port 1 is reserved and never bound.
#[tokio::test]
async fn get_against_an_unreachable_backend_clears_readiness() {
    use opentelemetry::metrics::MeterProvider as _;

    use crate::infra::metrics::SCOPE_NAME;
    use crate::infra::metrics::metrics_tests::{gauge_last_u64, local_provider};

    let (provider, exporter) = local_provider();
    let metrics = Arc::new(Metrics::with_meter(&provider.meter(SCOPE_NAME)));
    metrics.set_ready(true);

    let store = ChRecordStore::new(
        clickhouse::Client::default().with_url("http://127.0.0.1:1"),
        Arc::clone(&metrics),
        std::time::Duration::from_secs(5),
        true,
        None,
    );

    let err = store
        .get(Uuid::from_u128(0xdead))
        .await
        .expect_err("a get against a dead port must fail");
    assert!(
        matches!(err, UsageCollectorPluginError::Transient { .. }),
        "a refused connection is retryable, got {err:?}"
    );

    provider.force_flush().expect("flush in-memory metrics");
    assert_eq!(
        gauge_last_u64(&exporter, "uc_clickhouse_ready"),
        Some(0),
        "a connection failure on the request path must clear the readiness gauge"
    );
}

// ── create / create_batch: the Decimal128(9) value check runs before any I/O ─
//
// The column holds nine fractional digits. A value with non-zero digits beyond
// that would be rounded by the RowBinary encoder, so it is refused locally.
// The rejection message names `Decimal128(9)`, which is what distinguishes the
// guard from the refused connection (`"ClickHouse error"` / `Transient`).

/// `1.0000000005`: ten fractional digits, the last one non-zero.
fn lossy_scale_10_value() -> Decimal {
    Decimal::new(10_000_000_005, 10)
}

/// `1.0000000000`: ten fractional digits, all trailing zeros — exactly `1`
/// at scale 9.
fn exact_scale_10_value() -> Decimal {
    Decimal::new(10_000_000_000, 10)
}

fn assert_decimal_rejection(err: &UsageCollectorPluginError, what: &str) {
    match err {
        UsageCollectorPluginError::Internal(msg) => assert!(
            msg.contains("Decimal128(9)"),
            "{what} must name the Decimal128(9) column, got: {msg}"
        ),
        other => panic!("{what} must be Internal, got {other:?}"),
    }
}

fn assert_not_decimal_rejection(err: &UsageCollectorPluginError, what: &str) {
    assert_backend_failure(err, what);
    assert!(
        !err.to_string().contains("Decimal128(9)"),
        "{what} must not be a value rejection, got {err:?}"
    );
}

/// The value pre-pass decides exactly the slots whose value would be rounded,
/// at their own input positions, and hands every other record on to the
/// catalog step. Trailing zeros beyond scale 9 lose nothing and pass.
#[test]
fn split_by_representable_marks_only_the_unrepresentable_slots() {
    let tenant_id = Uuid::from_u128(2);
    let mut records = vec![
        make_record(Uuid::from_u128(1), tenant_id, 1_700_000_000_000_000),
        make_record(Uuid::from_u128(3), tenant_id, 1_700_000_000_000_001),
        make_record(Uuid::from_u128(4), tenant_id, 1_700_000_000_000_002),
    ];
    records[1].value = lossy_scale_10_value();
    records[2].value = exact_scale_10_value();
    let mut outcomes = vec![None, None, None];

    let passed = split_by_representable(&records, &mut outcomes);

    assert_eq!(
        passed,
        vec![0, 2],
        "trailing zeros beyond scale 9 pass; lost digits do not"
    );
    match &outcomes[1] {
        Some(Err(err)) => assert_decimal_rejection(err, "slot 1"),
        other => panic!("slot 1 must be rejected, got {other:?}"),
    }
    assert!(
        outcomes[0].is_none() && outcomes[2].is_none(),
        "passed slots are left for the catalog step to decide"
    );
}

/// A slot an earlier pass already decided is neither overwritten with
/// `UsageTypeNotFound` nor handed on to the dedup step.
#[test]
fn split_by_catalog_leaves_a_decided_slot_alone() {
    let tenant_id = Uuid::from_u128(2);
    let records = vec![
        // Unknown type AND already decided: the earlier decision must win.
        make_record_for(
            RAM_GTS,
            Uuid::from_u128(1),
            tenant_id,
            1_700_000_000_000_000,
        ),
        make_record(Uuid::from_u128(3), tenant_id, 1_700_000_000_000_001),
    ];
    let known: HashSet<String> = [VCPU_GTS.to_owned()].into_iter().collect();
    let mut outcomes = vec![
        Some(Err(UsageCollectorPluginError::internal("decided earlier"))),
        None,
    ];

    let passed = split_by_catalog(&records, &known, &mut outcomes);

    assert_eq!(passed, vec![1], "a decided slot is not passed on");
    match &outcomes[0] {
        Some(Err(UsageCollectorPluginError::Internal(msg))) => assert_eq!(
            msg, "decided earlier",
            "an earlier decision is not overwritten by UsageTypeNotFound"
        ),
        other => panic!("slot 0 must keep its earlier decision, got {other:?}"),
    }
}

/// `create` refuses a value the column cannot hold exactly before its first
/// statement — the message, not merely "some error", proves the guard fired
/// rather than the connection failing.
#[tokio::test]
async fn create_rejects_an_unrepresentable_value_before_any_statement() {
    let store = offline_store();
    let mut record = make_record(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
    );
    record.value = lossy_scale_10_value();

    let err = store
        .create(record)
        .await
        .expect_err("a value the column cannot hold exactly must be refused");

    assert_decimal_rejection(
        &err,
        "create of a value with ten significant fractional digits",
    );
}

/// A scale-10 value that is exactly representable at scale 9 is not a value
/// rejection: `create` proceeds to the (unreachable) backend.
#[tokio::test]
async fn create_accepts_trailing_zeros_beyond_scale_9_and_reaches_the_backend() {
    let store = offline_store();
    let mut record = make_record(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
    );
    record.value = exact_scale_10_value();

    let err = store
        .create(record)
        .await
        .expect_err("create cannot succeed against an unreachable backend");

    assert_not_decimal_rejection(&err, "create of a trailing-zero scale-10 value");
}

/// One unrepresentable value is decided in its own slot and does not stop the
/// rest of the batch from reaching the backend.
#[tokio::test]
async fn create_batch_decides_an_unrepresentable_slot_locally_and_isolates_it() {
    let store = offline_store();
    let tenant_id = Uuid::from_u128(2);
    let good = make_record(Uuid::from_u128(1), tenant_id, 1_700_000_000_000_000);
    let mut bad = make_record(Uuid::from_u128(3), tenant_id, 1_700_000_000_000_001);
    bad.value = lossy_scale_10_value();

    let outcomes = store
        .create_batch(vec![good, bad])
        .await
        .expect("a value rejection inside the batch is a per-record outcome");

    assert_eq!(outcomes.len(), 2, "one outcome per input row, in order");
    let good_err = outcomes[0]
        .as_ref()
        .expect_err("the representable record still reaches the unreachable backend");
    assert_not_decimal_rejection(good_err, "slot 0");
    let bad_err = outcomes[1]
        .as_ref()
        .expect_err("the unrepresentable record is refused");
    assert_decimal_rejection(bad_err, "slot 1");
}

/// A batch with nothing representable never issues a statement: its only
/// slot carries the value rejection, not a backend failure.
#[tokio::test]
async fn create_batch_of_only_unrepresentable_values_issues_no_statement() {
    let store = offline_store();
    let mut bad = make_record(
        Uuid::from_u128(1),
        Uuid::from_u128(2),
        1_700_000_000_000_000,
    );
    bad.value = lossy_scale_10_value();

    let outcomes = store
        .create_batch(vec![bad])
        .await
        .expect("per-record outcomes");

    assert_eq!(outcomes.len(), 1);
    let err = outcomes[0]
        .as_ref()
        .expect_err("the only record is refused");
    assert_decimal_rejection(err, "the only slot");
}

// ── group commit through the store ───────────────────────────────────────────
//
// The pre-checks (catalog, dedup) run against the in-process mock server; the
// write goes through a real `IngestQueue` whose batcher flushes into a scripted
// fake sink, so what reaches the "INSERT" and how each slot is decided are both
// observable without Docker. Real time (not the paused clock): the mock server
// is real I/O, which a paused clock would race against the client deadline.

mod group_commit {
    use std::sync::Arc;
    use std::time::Duration;

    use clickhouse::test::Mock;
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;
    use usage_collector_sdk::{UsageCollectorPluginError, UsageRecord};

    use super::{ChRecordStore, InsertKind, VCPU_GTS, insert_dedup_token};
    use crate::domain::ports::RecordStore;
    use crate::infra::metrics::Metrics;
    use crate::infra::storage::entity::UsageRecordRow;
    use crate::infra::storage::ingest::sink::FlushSink;
    use crate::infra::storage::ingest::sink::test_support::{FakeSink, Step};
    use crate::infra::storage::ingest::test_rows::{row, row_with_value};
    use crate::infra::storage::ingest::{IngestLimits, IngestQueue, PendingCall};
    use crate::infra::storage::test_mock::{client, ok, rows};

    fn limits() -> IngestLimits {
        IngestLimits {
            max_rows: 100,
            max_bytes: 1 << 20,
            max_wait: Duration::from_millis(10),
            queue_capacity: 8,
            max_concurrent_flushes: 2,
            max_outstanding_rows: 300,
            max_outstanding_bytes: 3 << 20,
            drain_deadline: Duration::from_secs(5),
        }
    }

    /// A store over the mock server with (or without) a running queue.
    struct Rig {
        /// Kept alive: the client points at it.
        _mock: Mock,
        store: ChRecordStore,
        queue: Arc<IngestQueue>,
        sink: Arc<FakeSink>,
        _cancel: CancellationToken,
    }

    fn rig(mock: Mock, batching: bool, queue_capacity: usize) -> Rig {
        let mut l = limits();
        l.queue_capacity = queue_capacity;
        let queue = IngestQueue::new(l, Arc::new(Metrics::new()));
        let sink = FakeSink::new();
        let cancel = CancellationToken::new();
        let dyn_sink: Arc<dyn FlushSink> = Arc::clone(&sink) as Arc<dyn FlushSink>;
        drop(tokio::spawn(
            Arc::clone(&queue).run(dyn_sink, cancel.clone()),
        ));
        let store = ChRecordStore::new(
            client(&mock),
            Arc::new(Metrics::new()),
            Duration::from_secs(5),
            false,
            batching.then(|| Arc::clone(&queue)),
        );
        Rig {
            _mock: mock,
            store,
            queue,
            sink,
            _cancel: cancel,
        }
    }

    fn record(n: u128) -> UsageRecord {
        UsageRecord::try_from(row(n)).expect("fixture row maps to a record")
    }

    /// Queue the catalog hit and an empty dedup pre-read for one `create`.
    fn prechecks_for_new_record(mock: &Mock) {
        rows(mock, [VCPU_GTS.to_owned()]);
        rows(mock, Vec::<UsageRecordRow>::new());
    }

    #[tokio::test]
    async fn create_through_the_queue_returns_the_record_after_its_flush() {
        let mock = Mock::new();
        prechecks_for_new_record(&mock);
        let r = rig(mock, true, 8);
        let rec = record(1);

        let stored = r.store.create(rec.clone()).await.expect("create");
        assert_eq!(stored.id, rec.id);
        let calls = r.sink.calls();
        assert_eq!(calls.len(), 1, "one flush carried the row");
        assert_eq!(calls[0].rows.len(), 1);
        assert_eq!(calls[0].rows[0].id, rec.id);
        assert_eq!(
            calls[0].token,
            insert_dedup_token(&calls[0].rows, InsertKind::Record)
        );
        assert_eq!(r.queue.snapshot().coalesce_entries, 0);
    }

    /// Per-row outcomes map back onto the slots each composed row backs: a
    /// row refused against a differing pending twin is that slot's
    /// `IdempotencyConflict`, the rest succeed, and a locally rejected slot is
    /// untouched by the write.
    #[tokio::test]
    async fn create_batch_through_the_queue_maps_row_outcomes_onto_slots() {
        let mock = Mock::new();
        rows(&mock, [VCPU_GTS.to_owned()]);
        rows(&mock, Vec::<UsageRecordRow>::new());
        let r = rig(mock, true, 8);

        // A differing payload for record 2's key is pending in flight.
        let gate = Arc::new(Notify::new());
        r.sink.script([Step::Gate(Arc::clone(&gate))]);
        let pending = r
            .queue
            .submit(PendingCall {
                rows: vec![row_with_value(2, 999)],
            })
            .expect("admitted");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(r.sink.call_count(), 1, "the twin is in flight");

        let mut unrepresentable = record(3);
        unrepresentable.value = rust_decimal::Decimal::new(1, 12);
        let batch = vec![record(1), record(2), unrepresentable];
        let outcomes = r.store.create_batch(batch).await.expect("batch");
        assert_eq!(outcomes.len(), 3);
        assert!(outcomes[0].is_ok(), "{:?}", outcomes[0]);
        match &outcomes[1] {
            Err(UsageCollectorPluginError::IdempotencyConflict { existing_id, .. }) => {
                assert_eq!(*existing_id, record(2).id);
            }
            other => panic!("expected a conflict against the pending twin, got {other:?}"),
        }
        assert!(
            matches!(outcomes[2], Err(UsageCollectorPluginError::Internal(_))),
            "the value check decided slot 2 before any statement"
        );
        gate.notify_one();
        assert!(pending.wait(Duration::from_secs(5)).await.is_ok());
        let calls = r.sink.calls();
        assert_eq!(calls.len(), 2, "the twin's flush and the batch's flush");
        assert_eq!(
            calls[1].rows.len(),
            1,
            "only record 1 was written by the batch"
        );
    }

    /// An admission rejection reaches only the slots backed by a composed row;
    /// a slot absorbed from storage keeps the outcome the pre-read decided.
    #[tokio::test]
    async fn create_batch_admission_rejection_fans_out_only_to_backed_slots() {
        let mock = Mock::new();
        rows(&mock, [VCPU_GTS.to_owned()]);
        // Record 1 is already stored (identical payload) → absorbed.
        rows(&mock, vec![row(1)]);
        let r = rig(mock, true, 1);

        // Fill the one admission slot with a gated call.
        let gate = Arc::new(Notify::new());
        r.sink.script([Step::Gate(Arc::clone(&gate))]);
        let filler = r
            .queue
            .submit(PendingCall { rows: vec![row(9)] })
            .expect("admitted");

        let outcomes = r
            .store
            .create_batch(vec![record(1), record(2)])
            .await
            .expect("batch");
        assert!(outcomes[0].is_ok(), "absorbed slot keeps its outcome");
        assert!(
            matches!(
                outcomes[1],
                Err(UsageCollectorPluginError::Transient { .. })
            ),
            "the composed slot carries the admission rejection: {:?}",
            outcomes[1]
        );
        gate.notify_one();
        assert!(filler.wait(Duration::from_secs(5)).await.is_ok());
    }

    /// Deactivation markers are written synchronously by the store itself,
    /// never through the queue.
    #[tokio::test]
    async fn deactivate_bypasses_the_queue() {
        let mock = Mock::new();
        rows(&mock, vec![row(4)]);
        ok(&mock); // the marker INSERT, straight to the client
        let r = rig(mock, true, 8);
        r.store.deactivate(row(4).id).await.expect("deactivate");
        assert_eq!(r.sink.call_count(), 0, "no flush carried the marker");
    }

    /// With group commit disabled the store issues its own INSERT.
    #[tokio::test]
    async fn batching_disabled_uses_the_direct_insert() {
        let mock = Mock::new();
        prechecks_for_new_record(&mock);
        ok(&mock); // the direct INSERT
        let r = rig(mock, false, 8);
        r.store.create(record(5)).await.expect("create");
        assert_eq!(r.sink.call_count(), 0);
    }
}
