//! `OpenTelemetry` metric inventory for the `ClickHouse` storage plugin.
//!
//! Realizes design ID `cpt-cf-uc-ch-plugin-design-metric-inventory`: every
//! backend-internal series this plugin owns under the `uc_clickhouse_*`
//! sub-namespace. Instrument names are the **full literal** Prometheus names
//! (`snake_case`, `_total` on counters, `_seconds` on duration histograms) with
//! **no** `.with_unit(...)` hint — matching the parent gateway and the
//! `uc_timescaledb_*` reference plugin. Histogram bucket layouts bracket the
//! NFR p95 budgets in `DESIGN.md` §1.2.
//!
//! All labels are bounded to enumerated value sets (see the `label` module):
//! unbounded identifiers (`tenant_id`, `gts_id`, record `id`, `idempotency_key`,
//! `corrects_id`, `request_id`, `trace_id`) MUST NOT appear as metric dimensions
//! — they belong in logs and traces.

use std::sync::Arc;
use std::time::Instant;

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::{InstrumentationScope, KeyValue, global};
use tokio_util::sync::CancellationToken;

/// `OpenTelemetry` instrumentation scope (meter name) for every plugin series.
pub(crate) const SCOPE_NAME: &str = "uc.clickhouse";

/// Seconds-valued duration histogram bucket boundaries for backend operations.
/// Brackets the §1.2 p95 budgets with finer low-end resolution so client-side
/// percentiles are comparable.
const DURATION_BOUNDARIES_SECS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Row-count histogram bucket boundaries for the per-write batch size.
/// Integer-ish boundaries spanning a single row up to a large bulk write,
/// so write amortization is observable (`uc_clickhouse_batch_rows`).
const BATCH_ROW_BOUNDARIES: &[f64] = &[1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0];

/// Seconds-valued bucket boundaries for the ingest queue's wait and age
/// histograms: finer at the low end than [`DURATION_BOUNDARIES_SECS`], since
/// `insert_batch.max_wait_ms` defaults to tens of milliseconds.
const QUEUE_WAIT_BOUNDARIES_SECS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Row-count boundaries for one group-commit flush, spanning a lone call up
/// to the default `insert_batch.max_rows` and beyond.
const FLUSH_ROW_BOUNDARIES: &[f64] = &[
    1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10_000.0, 50_000.0,
];

/// Estimated-byte boundaries for one group-commit flush (1 KiB … 64 MiB).
const FLUSH_BYTE_BOUNDARIES: &[f64] = &[
    1024.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
];

/// Bounded metric label keys and values.
///
/// Centralizing the `&'static str` constants keeps every call site on the
/// enumerated value sets from `DESIGN.md` §4 and prevents accidental
/// high-cardinality labels from leaking in.
pub mod label {
    /// Label key for the insert mode dimension.
    pub const MODE: &str = "mode";
    /// `mode` value: a single-row ingest.
    pub const MODE_SINGLE: &str = "single";
    /// `mode` value: a batch (multi-row) ingest.
    pub const MODE_BATCH: &str = "batch";
    /// `mode` value: one group-commit flush `INSERT` (several calls' rows).
    pub const MODE_FLUSH: &str = "flush";

    /// Label key for the outstanding-work state dimension.
    pub const STATE: &str = "state";
    /// `state` value: admitted, not yet cut into a flush.
    pub const STATE_QUEUED: &str = "queued";
    /// `state` value: cut into a flush whose `INSERT` has not resolved.
    pub const STATE_IN_FLIGHT: &str = "in_flight";

    /// Label key for the flush / drain outcome dimension.
    pub const OUTCOME: &str = "outcome";
    /// `outcome` value: the flush `INSERT` succeeded.
    pub const OUTCOME_OK: &str = "ok";
    /// `outcome` value: a transient failure after bounded retries.
    pub const OUTCOME_TRANSIENT: &str = "transient";
    /// `outcome` value: a non-retryable failure fanned out to every waiter.
    pub const OUTCOME_INTERNAL: &str = "internal";
    /// `outcome` value: a row-attributable failure, retried per call.
    pub const OUTCOME_ISOLATED: &str = "isolated";
    /// `outcome` value: a transient failure with no waiter left to retry for.
    pub const OUTCOME_ABANDONED: &str = "abandoned";
    /// `outcome` value: the flush task ended before it resolved its rows.
    pub const OUTCOME_ABORTED: &str = "aborted";
    /// `outcome` value (drain): every flush completed within the deadline.
    pub const OUTCOME_DRAINED: &str = "drained";
    /// `outcome` value (drain): in-flight flushes were aborted at the deadline.
    pub const OUTCOME_DEADLINE: &str = "deadline";

    /// Label key for the admission-rejection reason dimension.
    pub const REASON: &str = "reason";
    /// `reason` value: the outstanding-call bound.
    pub const REASON_CALLS: &str = "calls";
    /// `reason` value: the outstanding-row bound.
    pub const REASON_ROWS: &str = "rows";
    /// `reason` value: the outstanding-byte bound.
    pub const REASON_BYTES: &str = "bytes";
    /// `reason` value: the call alone exceeds an outstanding bound.
    pub const REASON_OVERSIZE: &str = "oversize";
    /// `reason` value: admission is closed (shutting down).
    pub const REASON_CLOSED: &str = "closed";

    /// Label key for the query-kind dimension.
    pub const QUERY_KIND: &str = "query_kind";
    /// `query_kind` value: a raw (keyset) record listing.
    pub const QUERY_KIND_RAW: &str = "raw";
    /// `query_kind` value: a server-side aggregated query.
    pub const QUERY_KIND_AGGREGATED: &str = "aggregated";

    /// Label key for the backend-error classification dimension.
    pub const ERROR_CATEGORY: &str = "error_category";
    /// `error_category` value: a retryable transient backend failure.
    pub const ERROR_CATEGORY_TRANSIENT: &str = "transient";
    /// `error_category` value: a non-retryable internal backend failure.
    pub const ERROR_CATEGORY_INTERNAL: &str = "internal";
}

/// Insert-mode dimension behind the `mode` label of
/// `uc_clickhouse_insert_duration_seconds`. Closed enum so a call site cannot
/// pass an arbitrary string into the bounded label set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertMode {
    /// A single-record create as the caller observes it (`mode = "single"`):
    /// pre-checks, queue wait and flush included when group commit is on.
    Single,
    /// A batch create as the caller observes it (`mode = "batch"`).
    Batch,
    /// One group-commit flush `INSERT` (`mode = "flush"`): the statement
    /// alone, which may carry several calls' rows.
    Flush,
}

impl InsertMode {
    /// The bounded `mode` label value for this mode.
    pub(crate) const fn as_label(self) -> &'static str {
        match self {
            Self::Single => label::MODE_SINGLE,
            Self::Batch => label::MODE_BATCH,
            Self::Flush => label::MODE_FLUSH,
        }
    }
}

/// Outstanding-work state behind the `state` label of the
/// `uc_clickhouse_ingest_outstanding_*` gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutstandingState {
    /// Admitted, not yet cut into a flush.
    Queued,
    /// Cut into a flush whose `INSERT` has not resolved.
    InFlight,
}

impl OutstandingState {
    pub(crate) const fn as_label(self) -> &'static str {
        match self {
            Self::Queued => label::STATE_QUEUED,
            Self::InFlight => label::STATE_IN_FLIGHT,
        }
    }
}

/// Flush outcome behind the `outcome` label of `uc_clickhouse_flushes_total`
/// and `uc_clickhouse_flush_duration_seconds`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushOutcome {
    /// The `INSERT` succeeded; every row is committed.
    Ok,
    /// A transient failure survived the bounded retries.
    Transient,
    /// A non-retryable failure, fanned out to every waiter.
    Internal,
    /// A row-attributable failure; each call was retried once on its own.
    Isolated,
    /// A transient failure with no waiter left; not retried.
    Abandoned,
    /// The flush task was aborted (or panicked) before it resolved its rows.
    Aborted,
}

impl FlushOutcome {
    pub(crate) const fn as_label(self) -> &'static str {
        match self {
            Self::Ok => label::OUTCOME_OK,
            Self::Transient => label::OUTCOME_TRANSIENT,
            Self::Internal => label::OUTCOME_INTERNAL,
            Self::Isolated => label::OUTCOME_ISOLATED,
            Self::Abandoned => label::OUTCOME_ABANDONED,
            Self::Aborted => label::OUTCOME_ABORTED,
        }
    }
}

/// Shutdown-drain outcome behind the `outcome` label of
/// `uc_clickhouse_shutdown_drain_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Every queued call was flushed and every flush completed.
    Drained,
    /// In-flight flushes were aborted at the drain deadline; their waiters
    /// received `Transient`.
    Deadline,
    /// The batcher stopped without draining (hard abort or panic); every
    /// unresolved waiter received `Transient`.
    Aborted,
}

impl DrainOutcome {
    pub(crate) const fn as_label(self) -> &'static str {
        match self {
            Self::Drained => label::OUTCOME_DRAINED,
            Self::Deadline => label::OUTCOME_DEADLINE,
            Self::Aborted => label::OUTCOME_ABORTED,
        }
    }
}

/// Admission-rejection reason behind the `reason` label of
/// `uc_clickhouse_admission_rejected_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionReason {
    /// `insert_batch.queue_capacity` outstanding calls reached.
    Calls,
    /// The outstanding-row bound would be exceeded.
    Rows,
    /// The outstanding-byte bound would be exceeded.
    Bytes,
    /// The call alone exceeds an outstanding bound (permanent).
    Oversize,
    /// Admission is closed: the plugin is shutting down.
    Closed,
}

impl AdmissionReason {
    pub(crate) const fn as_label(self) -> &'static str {
        match self {
            Self::Calls => label::REASON_CALLS,
            Self::Rows => label::REASON_ROWS,
            Self::Bytes => label::REASON_BYTES,
            Self::Oversize => label::REASON_OVERSIZE,
            Self::Closed => label::REASON_CLOSED,
        }
    }
}

/// The ingest queue's instruments (`storage::ingest`), kept apart from the
/// request-path inventory so each builder stays readable.
#[derive(Debug)]
struct IngestInstruments {
    /// `uc_clickhouse_ingest_outstanding_calls` — labelled by `state`.
    outstanding_calls: Gauge<u64>,
    /// `uc_clickhouse_ingest_outstanding_rows` — labelled by `state`.
    outstanding_rows: Gauge<u64>,
    /// `uc_clickhouse_ingest_outstanding_bytes` — labelled by `state`
    /// (estimated row bytes, the unit the byte bounds are measured in).
    outstanding_bytes: Gauge<u64>,
    /// `uc_clickhouse_ingest_queue_wait_seconds` — per call, from admission
    /// to its cut.
    queue_wait: Histogram<f64>,
    /// `uc_clickhouse_ingest_queue_age_seconds` — per flush, the age of its
    /// oldest call at the cut (what `max_wait_ms` bounds).
    queue_age: Histogram<f64>,
    /// `uc_clickhouse_flush_rows` — rows per flush.
    flush_rows: Histogram<f64>,
    /// `uc_clickhouse_flush_bytes` — estimated bytes per flush.
    flush_bytes: Histogram<f64>,
    /// `uc_clickhouse_flush_duration_seconds` — labelled by `outcome`;
    /// retries and isolation attempts included.
    flush_duration: Histogram<f64>,
    /// `uc_clickhouse_flushes_total` — labelled by `outcome`.
    flushes: Counter<u64>,
    /// `uc_clickhouse_admission_rejected_total` — labelled by `reason`.
    admission_rejected: Counter<u64>,
    /// `uc_clickhouse_coalesced_rows_total` — rows that joined an identical
    /// row already queued or in flight **in this instance**. Not a measure of
    /// cross-instance duplicates: another instance's identical write is never
    /// seen here and is resolved at read time instead.
    coalesced_rows: Counter<u64>,
    /// `uc_clickhouse_coalesce_conflicts_total` — rows refused as
    /// `IdempotencyConflict` against a differing pending row in this instance.
    coalesce_conflicts: Counter<u64>,
    /// `uc_clickhouse_flush_retries_total` — whole-flush retries after a
    /// transient failure.
    flush_retries: Counter<u64>,
    /// `uc_clickhouse_isolation_attempts_total` — per-call isolated `INSERT`s
    /// after a row-attributable failure of a combined flush.
    isolation_attempts: Counter<u64>,
    /// `uc_clickhouse_ingest_wait_timeouts_total` — calls whose request budget
    /// ran out while waiting on their flush.
    wait_timeouts: Counter<u64>,
    /// `uc_clickhouse_shutdown_drain_total` — labelled by `outcome`; one per
    /// batcher stop.
    shutdown_drain: Counter<u64>,
}

/// Build the ingest-queue instruments against `meter`.
fn ingest_instruments(meter: &Meter) -> IngestInstruments {
    IngestInstruments {
        outstanding_calls: meter
            .u64_gauge("uc_clickhouse_ingest_outstanding_calls")
            .with_description("Ingest calls admitted to the group-commit queue, by state")
            .build(),
        outstanding_rows: meter
            .u64_gauge("uc_clickhouse_ingest_outstanding_rows")
            .with_description("Rows admitted to the group-commit queue, by state")
            .build(),
        outstanding_bytes: meter
            .u64_gauge("uc_clickhouse_ingest_outstanding_bytes")
            .with_description("Estimated row bytes admitted to the group-commit queue, by state")
            .build(),
        queue_wait: meter
            .f64_histogram("uc_clickhouse_ingest_queue_wait_seconds")
            .with_description("Time an ingest call waited in the queue before its flush was cut")
            .with_boundaries(QUEUE_WAIT_BOUNDARIES_SECS.to_vec())
            .build(),
        queue_age: meter
            .f64_histogram("uc_clickhouse_ingest_queue_age_seconds")
            .with_description("Age of the oldest queued call when a flush was cut")
            .with_boundaries(QUEUE_WAIT_BOUNDARIES_SECS.to_vec())
            .build(),
        flush_rows: meter
            .f64_histogram("uc_clickhouse_flush_rows")
            .with_description("Rows per group-commit flush INSERT")
            .with_boundaries(FLUSH_ROW_BOUNDARIES.to_vec())
            .build(),
        flush_bytes: meter
            .f64_histogram("uc_clickhouse_flush_bytes")
            .with_description("Estimated row bytes per group-commit flush INSERT")
            .with_boundaries(FLUSH_BYTE_BOUNDARIES.to_vec())
            .build(),
        flush_duration: meter
            .f64_histogram("uc_clickhouse_flush_duration_seconds")
            .with_description("Duration of a group-commit flush, retries included, by outcome")
            .with_boundaries(DURATION_BOUNDARIES_SECS.to_vec())
            .build(),
        flushes: meter
            .u64_counter("uc_clickhouse_flushes_total")
            .with_description("Group-commit flushes, by outcome")
            .build(),
        admission_rejected: meter
            .u64_counter("uc_clickhouse_admission_rejected_total")
            .with_description("Ingest calls refused at admission, by reason")
            .build(),
        coalesced_rows: meter
            .u64_counter("uc_clickhouse_coalesced_rows_total")
            .with_description(
                "Rows coalesced onto an identical row pending in this instance's queue \
                 (instance-local; not a cross-instance duplicate count)",
            )
            .build(),
        coalesce_conflicts: meter
            .u64_counter("uc_clickhouse_coalesce_conflicts_total")
            .with_description(
                "Rows refused as an idempotency conflict against a differing pending row",
            )
            .build(),
        flush_retries: meter
            .u64_counter("uc_clickhouse_flush_retries_total")
            .with_description("Whole-flush retries after a transient `ClickHouse` failure")
            .build(),
        isolation_attempts: meter
            .u64_counter("uc_clickhouse_isolation_attempts_total")
            .with_description("Per-call isolated INSERTs after a row-attributable flush failure")
            .build(),
        wait_timeouts: meter
            .u64_counter("uc_clickhouse_ingest_wait_timeouts_total")
            .with_description("Ingest calls whose request budget expired while waiting on a flush")
            .build(),
        shutdown_drain: meter
            .u64_counter("uc_clickhouse_shutdown_drain_total")
            .with_description("Group-commit batcher stops, by drain outcome")
            .build(),
    }
}

/// Query-kind dimension behind the `query_kind` label of
/// `uc_clickhouse_query_duration_seconds` and `uc_clickhouse_query_requests_total`.
/// Closed enum so the bounded label set is enforced by the type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    /// A raw (keyset) record listing (`query_kind = "raw"`).
    Raw,
    /// A server-side aggregated query (`query_kind = "aggregated"`).
    Aggregated,
}

impl QueryKind {
    /// The bounded `query_kind` label value for this kind.
    pub(crate) const fn as_label(self) -> &'static str {
        match self {
            Self::Raw => label::QUERY_KIND_RAW,
            Self::Aggregated => label::QUERY_KIND_AGGREGATED,
        }
    }
}

/// Backend-error classification behind the `error_category` label of
/// `uc_clickhouse_backend_errors_total`, mirroring the SPI transient-vs-internal
/// split. Closed enum so an out-of-set class is unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// A retryable transient backend failure (`error_category = "transient"`).
    Transient,
    /// A non-retryable internal backend failure (`error_category = "internal"`).
    Internal,
}

impl ErrorClass {
    /// The bounded `error_category` label value for this classification.
    pub(crate) const fn as_label(self) -> &'static str {
        match self {
            Self::Transient => label::ERROR_CATEGORY_TRANSIENT,
            Self::Internal => label::ERROR_CATEGORY_INTERNAL,
        }
    }
}

/// The full `OpenTelemetry` metric inventory for the `ClickHouse` plugin.
///
/// Built once via [`Metrics::new`] and shared through an `Arc<Metrics>`; the
/// `OTel` instrument handles are themselves cheap `Arc`-backed clones, so the
/// struct is intentionally not `Clone` (share the `Arc`, not the struct).
#[derive(Debug)]
pub struct Metrics {
    // --- Histograms (seconds, unless noted) ---
    /// `uc_clickhouse_insert_duration_seconds` — labelled by `mode`.
    insert_duration: Histogram<f64>,
    /// `uc_clickhouse_query_duration_seconds` — labelled by `query_kind`.
    query_duration: Histogram<f64>,
    /// `uc_clickhouse_deactivate_duration_seconds`.
    deactivate_duration: Histogram<f64>,
    /// `uc_clickhouse_pool_acquire_duration_seconds` — time to acquire an HTTP
    /// connection from the `ClickHouse` client's underlying pool.
    pool_acquire_duration: Histogram<f64>,
    /// `uc_clickhouse_batch_rows` — row count per batch write.
    batch_rows: Histogram<f64>,

    // --- Counters ---
    /// `uc_clickhouse_dedup_absorbed_total`.
    dedup_absorbed: Counter<u64>,
    /// `uc_clickhouse_idempotency_conflicts_total`.
    idempotency_conflict: Counter<u64>,
    /// `uc_clickhouse_compensations_total`.
    compensation: Counter<u64>,
    /// `uc_clickhouse_backend_errors_total` — labelled by `error_category`.
    backend_error: Counter<u64>,
    /// `uc_clickhouse_migration_failures_total`.
    migration_failure: Counter<u64>,
    /// `uc_clickhouse_query_requests_total` — labelled by `query_kind`.
    query_requests: Counter<u64>,
    /// `uc_clickhouse_usage_type_referenced_total` — `delete_usage_type` calls
    /// refused because the pre-delete reference probe found referencing rows.
    usage_type_referenced: Counter<u64>,
    /// `uc_clickhouse_orphaned_reference_detected_total` — usage records found
    /// to reference a `gts_id` whose catalog row is already gone.
    ///
    /// Incremented once per `delete_usage_type` whose post-delete sweep found
    /// rows that landed inside the probe→delete window, i.e. exactly the
    /// residual race this backend accepts. A non-zero rate is the signal that
    /// deletes are being run against types with live ingest.
    orphaned_reference_detected: Counter<u64>,

    // --- Synchronous gauges ---
    /// `uc_clickhouse_usage_type_catalog_size` — live catalog row count.
    ///
    /// Not monotone: `delete_usage_type` removes the catalog row, so the gauge
    /// can decrease as well as increase.
    usage_type_catalog_size: Gauge<u64>,
    /// `uc_clickhouse_ready` — 1 = healthy, 0 = degraded.
    ///
    /// Lifecycle: 0 at the start of `init()`; 1 once the full init sequence
    /// has succeeded; 0 via [`Metrics::clear_ready`] when a request-path call
    /// hits a connectivity-class `ClickHouse` error or the client-side
    /// deadline expires; 1 again via [`Metrics::rearm_ready`] on the next
    /// successful round-trip; and 0 for good once the gear's cancellation
    /// token fires (the shutdown watcher records it and `rearm_ready` becomes
    /// a no-op). A missing series (never started) is therefore
    /// distinguishable from a published 0 (starting up, failed to start,
    /// backend unreachable, or shut down). The catalog-size refresh worker
    /// never touches it.
    ready: Gauge<u64>,
    /// Gear shutdown token; gates [`Metrics::rearm_ready`] so the request path
    /// cannot flip `uc_clickhouse_ready` back to 1 once the shutdown watcher
    /// owns the gauge. [`Metrics::with_meter`] / [`Metrics::new`] install a
    /// fresh token that never fires; production wiring swaps in the gear's
    /// token via [`Metrics::with_shutdown`].
    shutdown: CancellationToken,
    /// The group-commit ingest queue's instruments.
    ingest: IngestInstruments,
    // Intentionally omitted instruments (each has a reason):
    //
    // - No `uc_clickhouse_dedup_stale_total`: `ClickHouse` has no server-side
    //   dedup; the plugin performs its own SELECT-before-INSERT dedup. There is no
    //   "stale dedup hit whose record had aged out" scenario analogous to the
    //   `TimescaleDB` plugin's MVCC-window race.
    //
    // - No `uc_clickhouse_batch_retries_total`: `ClickHouse` has no deadlock
    //   victim / row-level locking. The one bounded in-process retry — a
    //   group-commit flush after a transient failure — is counted by
    //   `uc_clickhouse_flush_retries_total`.
    //
    // - No `uc_clickhouse_tls_handshake_failures_total`: the `ClickHouse` HTTP
    //   client surfaces TLS errors as generic `Network` errors; there is no
    //   separate TLS-handshake error variant to distinguish.
    //
    // - No observable pool gauges (`pool_connections_active`,
    //   `pool_connections_idle`): the `clickhouse` 0.15.x crate runs on a
    //   `hyper-util` legacy client's internal connection pool and exposes no
    //   pool-size counters.
    //
    // `uc_clickhouse_orphaned_reference_detected_total` IS registered above.
    // It was previously omitted because its only intended incrementer was the
    // deferred periodic orphan-reconciliation worker; the `delete_usage_type`
    // post-delete sweep is now a second, shipped incrementer, and the deferred
    // worker reuses the same instrument when it lands.
}

impl Metrics {
    /// Build the complete metric inventory against the global meter provider.
    ///
    /// Production entry point; resolves the meter from the process-global
    /// provider and delegates to [`Metrics::with_meter`].
    #[must_use]
    pub fn new() -> Self {
        let scope = InstrumentationScope::builder(SCOPE_NAME).build();
        let meter = global::meter_with_scope(scope);
        Self::with_meter(&meter)
    }

    /// Build the inventory against an explicit [`Meter`] instead of the global
    /// provider.
    ///
    /// [`Metrics::new`] resolves the meter from the process-global provider;
    /// this seam lets a test install a local meter provider backed by an
    /// in-memory reader and assert the recorded series without mutating global
    /// state (so the assertions stay parallel-safe).
    #[must_use]
    pub fn with_meter(meter: &Meter) -> Self {
        let insert_duration = meter
            .f64_histogram("uc_clickhouse_insert_duration_seconds")
            .with_description("Duration of usage-record inserts into `ClickHouse`, by mode")
            .with_boundaries(DURATION_BOUNDARIES_SECS.to_vec())
            .build();
        let query_duration = meter
            .f64_histogram("uc_clickhouse_query_duration_seconds")
            .with_description("Duration of usage-record queries against `ClickHouse`, by kind")
            .with_boundaries(DURATION_BOUNDARIES_SECS.to_vec())
            .build();
        let deactivate_duration = meter
            .f64_histogram("uc_clickhouse_deactivate_duration_seconds")
            .with_description("Duration of the event-deactivation cascade in `ClickHouse`")
            .with_boundaries(DURATION_BOUNDARIES_SECS.to_vec())
            .build();
        let pool_acquire_duration = meter
            .f64_histogram("uc_clickhouse_pool_acquire_duration_seconds")
            .with_description(
                "Time to acquire an HTTP connection from the `ClickHouse` client pool",
            )
            .with_boundaries(DURATION_BOUNDARIES_SECS.to_vec())
            .build();
        let batch_rows = meter
            .f64_histogram("uc_clickhouse_batch_rows")
            .with_description("Row count per batch write to `ClickHouse`")
            .with_boundaries(BATCH_ROW_BOUNDARIES.to_vec())
            .build();

        let dedup_absorbed = meter
            .u64_counter("uc_clickhouse_dedup_absorbed_total")
            .with_description("Exact-equality retries silently absorbed on the dedup-key conflict")
            .build();
        let idempotency_conflict = meter
            .u64_counter("uc_clickhouse_idempotency_conflicts_total")
            .with_description("Canonical-field-mismatch idempotency conflicts")
            .build();
        let compensation = meter
            .u64_counter("uc_clickhouse_compensations_total")
            .with_description("Inserts carrying a `corrects_id` (compensating records)")
            .build();
        let backend_error = meter
            .u64_counter("uc_clickhouse_backend_errors_total")
            .with_description("`ClickHouse` errors, by SPI transient/internal classification")
            .build();
        let migration_failure = meter
            .u64_counter("uc_clickhouse_migration_failures_total")
            .with_description("Schema-migration failures at plugin startup")
            .build();
        let query_requests = meter
            .u64_counter("uc_clickhouse_query_requests_total")
            .with_description(
                "Query requests dispatched to `ClickHouse`, by kind (workload mix observable)",
            )
            .build();
        let usage_type_referenced = meter
            .u64_counter("uc_clickhouse_usage_type_referenced_total")
            .with_description(
                "`delete_usage_type` calls refused because the type is still referenced",
            )
            .build();
        let orphaned_reference_detected = meter
            .u64_counter("uc_clickhouse_orphaned_reference_detected_total")
            .with_description(
                "Usage records found referencing a `gts_id` whose catalog row is already gone",
            )
            .build();
        let usage_type_catalog_size = meter
            .u64_gauge("uc_clickhouse_usage_type_catalog_size")
            .with_description("Current live usage-type catalog row count in `ClickHouse`")
            .build();
        let ready = meter
            .u64_gauge("uc_clickhouse_ready")
            .with_description("Plugin-local backend readiness (1 = migration ok, 0 = degraded)")
            .build();

        Self {
            insert_duration,
            query_duration,
            deactivate_duration,
            pool_acquire_duration,
            batch_rows,
            dedup_absorbed,
            idempotency_conflict,
            compensation,
            backend_error,
            migration_failure,
            query_requests,
            usage_type_referenced,
            orphaned_reference_detected,
            usage_type_catalog_size,
            ready,
            shutdown: CancellationToken::new(),
            ingest: ingest_instruments(meter),
        }
    }

    /// Bind the gear's cancellation token so [`Metrics::rearm_ready`] stops
    /// re-arming `uc_clickhouse_ready` once shutdown begins.
    ///
    /// Production wiring calls this once in `Gear::init` with the gear token.
    /// Tests that do not exercise shutdown keep the default never-firing
    /// token installed by [`Metrics::with_meter`].
    #[must_use]
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    // --- Histogram recording helpers ---

    /// Record an insert duration (seconds) for the given [`InsertMode`].
    pub(crate) fn record_insert(&self, mode: InsertMode, secs: f64) {
        self.insert_duration
            .record(secs, &[KeyValue::new(label::MODE, mode.as_label())]);
    }

    /// Record a query duration (seconds) for the given [`QueryKind`].
    pub(crate) fn record_query(&self, kind: QueryKind, secs: f64) {
        self.query_duration
            .record(secs, &[KeyValue::new(label::QUERY_KIND, kind.as_label())]);
    }

    /// Record a deactivation-cascade duration (seconds).
    pub(crate) fn record_deactivate(&self, secs: f64) {
        self.deactivate_duration.record(secs, &[]);
    }

    /// Record a `ClickHouse` HTTP connection-pool acquire duration (seconds).
    pub(crate) fn record_pool_acquire(&self, secs: f64) {
        self.pool_acquire_duration.record(secs, &[]);
    }

    /// Record the row count of a batch write.
    pub(crate) fn record_batch_rows(&self, n: f64) {
        self.batch_rows.record(n, &[]);
    }

    // --- Counter helpers ---

    /// Increment the silently-absorbed dedup retry counter.
    pub(crate) fn inc_dedup_absorbed(&self) {
        self.dedup_absorbed.add(1, &[]);
    }

    /// Increment the idempotency-conflict counter.
    pub(crate) fn inc_idempotency_conflict(&self) {
        self.idempotency_conflict.add(1, &[]);
    }

    /// Increment the compensation (`corrects_id` insert) counter.
    pub(crate) fn inc_compensation(&self) {
        self.compensation.add(1, &[]);
    }

    /// Increment the backend-error counter for the given [`ErrorClass`].
    pub(crate) fn inc_backend_error(&self, class: ErrorClass) {
        self.backend_error
            .add(1, &[KeyValue::new(label::ERROR_CATEGORY, class.as_label())]);
    }

    /// Increment the migration-failure counter.
    pub(crate) fn inc_migration_failure(&self) {
        self.migration_failure.add(1, &[]);
    }

    /// Increment the query-requests counter for the given [`QueryKind`].
    pub(crate) fn inc_query_request(&self, kind: QueryKind) {
        self.query_requests
            .add(1, &[KeyValue::new(label::QUERY_KIND, kind.as_label())]);
    }

    /// Increment the referenced-usage-type counter (a refused delete).
    pub(crate) fn inc_usage_type_referenced(&self) {
        self.usage_type_referenced.add(1, &[]);
    }

    /// Increment the orphaned-reference counter.
    ///
    /// Called once per `delete_usage_type` whose post-delete sweep found rows
    /// that landed inside the probe→delete window.
    pub(crate) fn inc_orphaned_reference_detected(&self) {
        self.orphaned_reference_detected.add(1, &[]);
    }

    // --- Ingest queue (group commit) ---

    /// Set the three outstanding-work gauges for one `state`.
    pub(crate) fn set_ingest_outstanding(
        &self,
        state: OutstandingState,
        calls: u64,
        rows: u64,
        bytes: u64,
    ) {
        let attrs = [KeyValue::new(label::STATE, state.as_label())];
        self.ingest.outstanding_calls.record(calls, &attrs);
        self.ingest.outstanding_rows.record(rows, &attrs);
        self.ingest.outstanding_bytes.record(bytes, &attrs);
    }

    /// Record one call's queue wait (seconds), admission to cut.
    pub(crate) fn record_queue_wait(&self, secs: f64) {
        self.ingest.queue_wait.record(secs, &[]);
    }

    /// Record the oldest call's age (seconds) at a flush cut.
    pub(crate) fn record_queue_age(&self, secs: f64) {
        self.ingest.queue_age.record(secs, &[]);
    }

    /// Record one completed flush: its outcome, duration, rows and bytes.
    pub(crate) fn record_flush(&self, outcome: FlushOutcome, secs: f64, rows: f64, bytes: f64) {
        let attrs = [KeyValue::new(label::OUTCOME, outcome.as_label())];
        self.ingest.flush_duration.record(secs, &attrs);
        self.ingest.flushes.add(1, &attrs);
        self.ingest.flush_rows.record(rows, &[]);
        self.ingest.flush_bytes.record(bytes, &[]);
    }

    /// Count one admission rejection under its reason.
    pub(crate) fn inc_admission_rejected(&self, reason: AdmissionReason) {
        self.ingest
            .admission_rejected
            .add(1, &[KeyValue::new(label::REASON, reason.as_label())]);
    }

    /// Count rows coalesced onto a pending identical row (instance-local).
    pub(crate) fn inc_coalesced_rows(&self, n: u64) {
        self.ingest.coalesced_rows.add(n, &[]);
    }

    /// Count rows refused as a conflict against a differing pending row.
    pub(crate) fn inc_coalesce_conflicts(&self, n: u64) {
        self.ingest.coalesce_conflicts.add(n, &[]);
    }

    /// Count one whole-flush retry.
    pub(crate) fn inc_flush_retry(&self) {
        self.ingest.flush_retries.add(1, &[]);
    }

    /// Count one per-call isolation attempt.
    pub(crate) fn inc_isolation_attempt(&self) {
        self.ingest.isolation_attempts.add(1, &[]);
    }

    /// Count one call whose request budget expired while waiting.
    pub(crate) fn inc_wait_timeout(&self) {
        self.ingest.wait_timeouts.add(1, &[]);
    }

    /// Count one batcher stop under its drain outcome.
    pub(crate) fn inc_shutdown_drain(&self, outcome: DrainOutcome) {
        self.ingest
            .shutdown_drain
            .add(1, &[KeyValue::new(label::OUTCOME, outcome.as_label())]);
    }

    // --- Synchronous gauge setters ---

    /// Set the live usage-type catalog row count.
    pub(crate) fn set_catalog_size(&self, n: u64) {
        self.usage_type_catalog_size.record(n, &[]);
    }

    /// Set the plugin-local readiness gauge (1 when ready, else 0).
    ///
    /// The lifecycle entry points: `false` at the start of `init()`, `true`
    /// after a successful `init()`, and `false` again from the shutdown
    /// watcher. Must NOT be called with `true` after the cancellation token
    /// fires — the request path goes through [`Metrics::rearm_ready`], which
    /// enforces that, rather than calling this directly.
    pub(crate) fn set_ready(&self, ready: bool) {
        self.ready.record(u64::from(ready), &[]);
    }

    /// Re-arm `uc_clickhouse_ready` to 1 after a successful backend
    /// round-trip, unless the shutdown token has fired.
    ///
    /// Once `cancel` fires the shutdown watcher owns the gauge, so a
    /// drain-time success must not flip it back to 1. The check-then-set
    /// window against the watcher is a sub-tick blip on a best-effort gauge
    /// during one-way shutdown — accepted rather than serialized, as in the
    /// reference plugin.
    pub(crate) fn rearm_ready(&self) {
        if !self.shutdown.is_cancelled() {
            self.set_ready(true);
        }
    }

    /// Clear `uc_clickhouse_ready` to 0 on a connectivity-class backend
    /// error or client-side deadline expiry on the request path.
    pub(crate) fn clear_ready(&self) {
        self.set_ready(false);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Which duration histogram an [`OpDurationGuard`] records on drop.
#[derive(Debug, Clone, Copy)]
pub enum TimedOp {
    /// `uc_clickhouse_query_duration_seconds`, labelled by [`QueryKind`].
    Query(QueryKind),
    /// `uc_clickhouse_deactivate_duration_seconds`.
    Deactivate,
}

/// Records an operation-duration histogram on drop, so the duration is captured
/// on **every** return path — including error arms that `?` out before a
/// success-only `record_*` call would run. Construct it at the top of an
/// operation and let it fall out of scope on return.
///
/// Holds an `Arc<Metrics>` (the inventory is shared via `Arc`, never deep
/// cloned); the target series is fixed at construction.
#[derive(Debug)]
pub struct OpDurationGuard {
    metrics: Arc<Metrics>,
    op: TimedOp,
    start: Instant,
}

impl OpDurationGuard {
    /// Start timing `op` against `metrics`; records on drop.
    #[must_use]
    pub(crate) fn start(metrics: Arc<Metrics>, op: TimedOp) -> Self {
        Self {
            metrics,
            op,
            start: Instant::now(),
        }
    }
}

impl Drop for OpDurationGuard {
    fn drop(&mut self) {
        let secs = self.start.elapsed().as_secs_f64();
        match self.op {
            TimedOp::Query(kind) => self.metrics.record_query(kind, secs),
            TimedOp::Deactivate => self.metrics.record_deactivate(secs),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "metrics_tests.rs"]
pub(crate) mod metrics_tests;
