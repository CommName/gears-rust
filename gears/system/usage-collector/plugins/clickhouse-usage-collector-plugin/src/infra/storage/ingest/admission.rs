//! Admission control for the ingest queue: the outstanding-work bounds and
//! the byte estimate they are measured in.

use usage_collector_sdk::UsageCollectorPluginError;

use super::IngestLimits;
use super::state::Phase;
use crate::infra::metrics::AdmissionReason;
use crate::infra::storage::entity::UsageRecordRow;

/// Retry hint handed back with a bound rejection, in seconds. One flush cycle
/// (`max_wait` plus an `INSERT`) is well inside it.
const RETRY_AFTER_SECS: u64 = 1;

/// Fixed-width part of a `usage_records` row as `RowBinary` carries it: two
/// `UUID`s, a `Decimal128`, two `DateTime64`, a `UInt64`, the `Enum8`, and the
/// `Nullable(UUID)` with its null flag.
const ROW_FIXED_BYTES: usize = 16 + 16 + 16 + 8 + 8 + 8 + 1 + 17;

/// Estimated wire size of one row: the fixed-width columns plus every string
/// and map entry. An estimate rather than the encoded length — it is the unit
/// `max_bytes` and the outstanding byte bound are measured in, and it only has
/// to be proportional and cheap.
#[must_use]
pub(crate) fn estimated_row_bytes(row: &UsageRecordRow) -> usize {
    let strings = row.gts_id.len()
        + row.resource_id.len()
        + row.resource_type.len()
        + row.subject_id.as_ref().map_or(0, String::len)
        + row.subject_type.as_ref().map_or(0, String::len)
        + row.idempotency_key.len();
    let metadata: usize = row
        .metadata
        .iter()
        .map(|(key, value)| key.len() + value.len() + 2)
        .sum();
    ROW_FIXED_BYTES + strings + metadata
}

/// Outstanding work (queued + in flight) as the bounds see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Outstanding {
    pub calls: usize,
    pub rows: usize,
    pub bytes: usize,
}

/// Decide whether a call of `rows` rows / `bytes` estimated bytes may enter.
///
/// Checked in this order: admission closed; the call alone exceeds an
/// outstanding bound (permanent); the call bound; the row bound; the byte
/// bound. A call larger than a *flush* target but inside the outstanding
/// bounds is admitted — it is flushed alone.
///
/// # Errors
///
/// The first bound the call violates.
pub(crate) fn check(
    limits: &IngestLimits,
    outstanding: Outstanding,
    rows: usize,
    bytes: usize,
    phase: Phase,
) -> Result<(), AdmissionReason> {
    if phase != Phase::Open {
        return Err(AdmissionReason::Closed);
    }
    if rows > limits.max_outstanding_rows || bytes > limits.max_outstanding_bytes {
        return Err(AdmissionReason::Oversize);
    }
    if outstanding.calls >= limits.queue_capacity {
        return Err(AdmissionReason::Calls);
    }
    if outstanding.rows.saturating_add(rows) > limits.max_outstanding_rows {
        return Err(AdmissionReason::Rows);
    }
    if outstanding.bytes.saturating_add(bytes) > limits.max_outstanding_bytes {
        return Err(AdmissionReason::Bytes);
    }
    Ok(())
}

/// The error a refused call receives.
///
/// Every bound and the closed queue are `Transient` with a retry hint: the
/// condition clears as flushes complete (or the caller is routed to another
/// instance). `Oversize` is `Internal`: the same call can never be admitted,
/// so a retry would loop.
#[must_use]
pub(crate) fn rejection(reason: AdmissionReason) -> UsageCollectorPluginError {
    match reason {
        AdmissionReason::Closed => UsageCollectorPluginError::transient_with_retry(
            "ingest queue is not accepting writes (shutting down)",
            Some(RETRY_AFTER_SECS),
        ),
        AdmissionReason::Calls => UsageCollectorPluginError::transient_with_retry(
            "ingest queue is full (insert_batch.queue_capacity outstanding calls)",
            Some(RETRY_AFTER_SECS),
        ),
        AdmissionReason::Rows => UsageCollectorPluginError::transient_with_retry(
            "ingest queue is full (outstanding row bound)",
            Some(RETRY_AFTER_SECS),
        ),
        AdmissionReason::Bytes => UsageCollectorPluginError::transient_with_retry(
            "ingest queue is full (outstanding byte bound)",
            Some(RETRY_AFTER_SECS),
        ),
        AdmissionReason::Oversize => UsageCollectorPluginError::internal(
            "call exceeds the ingest queue's outstanding row/byte bound on its own; raise \
             insert_batch.max_rows / insert_batch.max_bytes or send a smaller batch",
        ),
    }
}
