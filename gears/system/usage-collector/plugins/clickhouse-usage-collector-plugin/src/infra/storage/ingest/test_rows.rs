//! Row fixtures shared by the ingest-queue tests.

use std::collections::HashMap;

use rust_decimal::Decimal;
use uuid::Uuid;

use crate::infra::storage::entity::{UsageRecordRow, UsageRecordStatusCode};

pub const VCPU_GTS: &str = "gts.cf.core.uc.usage_record.v1~cf.compute._.vcpu_hours.v1";

/// A row whose dedup tuple is unique to `n` (distinct `created_at` and
/// idempotency key), with a fixed tenant and payload.
pub fn row(n: u128) -> UsageRecordRow {
    row_with_value(n, 100)
}

/// [`row`] with a chosen `value`, so two rows can share a dedup tuple and
/// differ in payload.
pub fn row_with_value(n: u128, value: i64) -> UsageRecordRow {
    let micros = 1_700_000_000_000_000_i64 + i64::try_from(n).unwrap_or(0);
    UsageRecordRow {
        id: Uuid::from_u128(n + 1),
        tenant_id: Uuid::from_u128(0xA),
        gts_id: VCPU_GTS.to_owned(),
        value: Decimal::new(value, 0),
        created_at: micros,
        resource_id: "res-1".to_owned(),
        resource_type: "vm".to_owned(),
        subject_id: None,
        subject_type: None,
        idempotency_key: format!("idem-{n}"),
        corrects_id: None,
        status: UsageRecordStatusCode::Active,
        metadata: HashMap::new(),
        ingested_at: micros,
        version: 1,
    }
}

/// `count` distinct rows starting at `from`.
pub fn rows(from: u128, count: u128) -> Vec<UsageRecordRow> {
    (from..from + count).map(row).collect()
}
