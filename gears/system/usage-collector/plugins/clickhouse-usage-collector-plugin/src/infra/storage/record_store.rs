//! `ClickHouse`-backed [`RecordStore`] over the `usage_records` table.
//!
//! All operations — `create` / `create_batch` / `get` / `list` / `aggregate` /
//! `deactivate` — are implemented against `ClickHouse` using the `clickhouse`
//! 0.15.x crate.
//!
//! ## Key design differences from the `TimescaleDB` reference plugin
//!
//! - **No `ON CONFLICT DO NOTHING`**: `ClickHouse` has no unique constraints.
//!   Dedup is performed explicitly: SELECT then INSERT.
//! - **No `FOR UPDATE`**: `ClickHouse` has no row-level locks. Coordination is
//!   provided by the cluster-backed
//!   [`LockManager`](crate::infra::coordination::lock_manager::LockManager)
//!   instead, reached through the [`CatalogLockPort`] seam.
//! - **No `UPDATE`**: deactivation uses versioned marker rows (INSERT with
//!   `status = 'inactive'` and a higher `version`) rather than `ALTER TABLE …
//!   UPDATE` (an async mutation unsuitable for the request path).
//! - **`FINAL` keyword**: every SELECT appends `FINAL` to the table name so
//!   `ReplacingMergeTree` version resolution is applied before the result is
//!   returned — un-qualified reads may return stale pre-deactivation or
//!   duplicate rows.
//! - **`?` placeholders**: `ClickHouse` uses positional `?` (not `$N`).
//! - **`arrayElement(metadata, ?)`**: map access (not `metadata ->> key`); avoid
//!   `metadata[?]` in sea-query because `[…]` is tokenized as a quoted span.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
#[cfg(test)]
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use futures::future::join_all;
use tracing::instrument;
use uuid::Uuid;

use toolkit_odata::filter::{FilterField, convert_expr_to_filter_node};
use toolkit_odata::{ODataQuery, Page as ODataPage, PageInfo, SortDir};

use usage_collector_sdk::{
    AggregationBucket, AggregationDimension, AggregationResult, AggregationSpec, MetadataFilter,
    UsageCollectorPluginError, UsageRecord, UsageRecordFilterField, UsageTypeGtsId,
    is_keyset_safe_record_field,
};

use crate::domain::ports::RecordStore;
use crate::infra::coordination::lock_manager::LockGuardPort;
use crate::infra::metrics::{InsertMode, LockMode, Metrics, OpDurationGuard, QueryKind, TimedOp};
use crate::infra::storage::catalog_store::CatalogLockPort;
use crate::infra::storage::entity::{UsageRecordRow, UsageRecordStatusCode};
use crate::infra::storage::error::tracked_ch_err;
use crate::infra::storage::mapper::{
    canonical_equal, current_merge_version, datetime_to_micros, gts_id_str, make_inactive_marker,
    record_row_key, record_row_to_model, record_to_row, version_higher_than,
};
use crate::infra::storage::query::aggregate::{
    agg_select_expr, aggregate_limit, corrects_id_partition_clause, dimension_select_expr,
};
use crate::infra::storage::query::build::{
    apply_condition, build_select_final, catalog_exists_gts_id, prepared_select, prepared_sql,
    record_get_by_id,
};
use crate::infra::storage::query::effective_page_size;
use crate::infra::storage::query::expr::from_unix_timestamp64_micro;
use crate::infra::storage::query::keyset::{
    encode_next_cursor, ensure_forward_cursor, keyset_condition, order_by_clauses,
};
use crate::infra::storage::query::schema::{RECORD_SELECT_COLUMNS, UsageRecords};
use crate::infra::storage::query::translate::{
    SqlBind, SqlCtx, bind_one, record_column, translate_record_filter,
};

use sea_query::{Alias, Condition, Expr, ExprTrait, Query, SimpleExpr, Value as SeaValue};

/// `ClickHouse`-backed implementation of [`RecordStore`] over `usage_records`.
#[derive(Clone)]
pub struct ChRecordStore {
    client: clickhouse::Client,
    lock_manager: Arc<dyn CatalogLockPort>,
    metrics: Arc<Metrics>,
}

impl ChRecordStore {
    /// Build a store from an existing `ClickHouse` client, exclusive-lock port,
    /// and metric inventory.
    ///
    /// The lock port is the erased [`CatalogLockPort`] the catalog store also
    /// depends on — both stores contend on the same exclusive per-`gts_id`
    /// cluster mutex — so create-path lock failures are exercisable offline
    /// with a stub implementation.
    #[must_use]
    pub fn new(
        client: clickhouse::Client,
        lock_manager: Arc<dyn CatalogLockPort>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            client,
            lock_manager,
            metrics,
        }
    }

    /// Execute a `SELECT … FROM usage_type_catalog FINAL WHERE gts_id = ?`
    /// catalog existence check while the caller holds the exclusive create lock.
    ///
    /// Returns `Ok(())` if the usage type exists; `UsageTypeNotFound`
    /// otherwise. A deleted usage type is a real row removal (lightweight
    /// `DELETE FROM`, see `catalog_store::ChCatalogStore::delete`), so its
    /// absence from this query is immediate and unconditional — no tombstone
    /// flag is consulted here.
    ///
    /// # Errors
    ///
    /// Returns `Transient` on connectivity errors, `Internal` on protocol
    /// errors, `UsageTypeNotFound` when absent.
    ///
    /// (`ClickHouse` errors are mapped via [`tracked_ch_err`].)
    #[instrument(skip_all, fields(gts_id = %gts_id_str(gts_id)))]
    async fn check_catalog_existence(
        &self,
        gts_id: &UsageTypeGtsId,
    ) -> Result<(), UsageCollectorPluginError> {
        let found: Option<String> = prepared_sql(
            &self.client,
            catalog_exists_gts_id(gts_id_str(gts_id)),
        )
        .map_err(UsageCollectorPluginError::internal)?
        .fetch_optional::<String>()
        .await
        .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        if found.is_none() {
            return Err(UsageCollectorPluginError::UsageTypeNotFound {
                gts_id: gts_id.clone(),
            });
        }
        Ok(())
    }

    /// Dedup point-lookup using the full `ORDER BY` key prefix:
    /// `WHERE tenant_id = ? AND gts_id = ? AND created_at = ? AND id = ?`
    ///
    /// Returns the stored row if found, `None` if no row exists for this key.
    ///
    /// # Errors
    ///
    /// Returns `Transient` or `Internal` on `ClickHouse` errors.
    #[instrument(skip_all, fields(gts_id = %gts_id_str(&record.gts_id)))]
    async fn dedup_point_lookup(
        &self,
        record: &UsageRecord,
    ) -> Result<Option<UsageRecordRow>, UsageCollectorPluginError> {
        let created_at_micros = datetime_to_micros(record.created_at);
        let stmt = Query::select()
            .columns(RECORD_SELECT_COLUMNS)
            .from(UsageRecords::Table)
            .and_where(Expr::col(UsageRecords::TenantId).eq(record.tenant_id.to_string()))
            .and_where(Expr::col(UsageRecords::GtsId).eq(gts_id_str(&record.gts_id)))
            .and_where(
                Expr::col(UsageRecords::CreatedAt).eq(from_unix_timestamp64_micro(created_at_micros)),
            )
            .and_where(Expr::col(UsageRecords::Id).eq(record.id.to_string()))
            .to_owned();
        prepared_select(&self.client, stmt)
            .map_err(UsageCollectorPluginError::internal)?
            .fetch_optional::<UsageRecordRow>()
            .await
            .map_err(|e| tracked_ch_err(&self.metrics, &e))
    }

    /// Insert a single row into `usage_records`.
    ///
    /// Tracks pool-acquire duration (time to get the `Insert` handle) via the
    /// metric inventory, and reports the single-row create latency measured
    /// from `op_start` — the caller's critical-section entry, so lock
    /// acquisition and the catalog check are inside the observed window.
    ///
    /// # Errors
    ///
    /// Returns `Transient` or `Internal` on `ClickHouse` errors.
    async fn insert_record(
        &self,
        row: &UsageRecordRow,
        op_start: Instant,
    ) -> Result<(), UsageCollectorPluginError> {
        let pool_start = Instant::now();
        let mut insert: clickhouse::insert::Insert<UsageRecordRow> = self
            .client
            .insert("usage_records")
            .await
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        self.metrics
            .record_pool_acquire(pool_start.elapsed().as_secs_f64());

        insert
            .write(row)
            .await
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        insert
            .end()
            .await
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        self.metrics
            .record_insert(InsertMode::Single, op_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Insert multiple rows into `usage_records` in a single INSERT statement.
    ///
    /// A single `ClickHouse` INSERT is applied as one atomic part write, so a
    /// `FINAL`-qualified reader either sees all rows or none.
    ///
    /// Tracks pool-acquire duration via the metric inventory and reports the
    /// batch-write latency measured from `op_start`, which the caller sets to
    /// the point its observed window should begin.
    ///
    /// # Errors
    ///
    /// Returns `Transient` or `Internal` on `ClickHouse` errors.
    async fn insert_records(
        &self,
        rows: &[UsageRecordRow],
        op_start: Instant,
    ) -> Result<(), UsageCollectorPluginError> {
        if rows.is_empty() {
            return Ok(());
        }
        let pool_start = Instant::now();
        let mut insert: clickhouse::insert::Insert<UsageRecordRow> = self
            .client
            .insert("usage_records")
            .await
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        self.metrics
            .record_pool_acquire(pool_start.elapsed().as_secs_f64());
        // Row counts up to the batch cap (≤1000) fit exactly in f64's 52-bit mantissa.
        #[allow(
            clippy::cast_precision_loss,
            reason = "batch size is bounded by REF_COUNT_CAP"
        )]
        let batch_len = rows.len() as f64;
        self.metrics.record_batch_rows(batch_len);

        for row in rows {
            insert
                .write(row)
                .await
                .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        }
        insert
            .end()
            .await
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        self.metrics
            .record_insert(InsertMode::Batch, op_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Batch dedup pre-check: SELECT all rows whose `(tenant_id, gts_id,
    /// created_at, id)` 4-tuple appears in the input list.
    ///
    /// Returns a map from the 4-tuple key to the stored row.
    ///
    /// # Errors
    ///
    /// Returns `Transient` or `Internal` on `ClickHouse` errors.
    #[instrument(skip_all, fields(record_count = records.len()))]
    async fn batch_dedup_lookup(
        &self,
        records: &[&UsageRecord],
    ) -> Result<HashMap<DedupKey, UsageRecordRow>, UsageCollectorPluginError> {
        if records.is_empty() {
            return Ok(HashMap::new());
        }
        // Build `(t, g, c, i) IN ((?, ?, fromUnixTimestamp64Micro(?), ?), ...)`.
        let mut ctx = SqlCtx::new();
        let mut tuples = Vec::with_capacity(records.len());
        for r in records {
            ctx.push(SqlBind::Uuid(r.tenant_id));
            ctx.push(SqlBind::Str(gts_id_str(&r.gts_id).to_owned()));
            ctx.push(SqlBind::DateTime64Micros(datetime_to_micros(r.created_at)));
            ctx.push(SqlBind::Uuid(r.id));
            tuples.push("(?, ?, fromUnixTimestamp64Micro(?), ?)");
        }
        let in_clause = tuples.join(", ");
        let stmt = Query::select()
            .columns(RECORD_SELECT_COLUMNS)
            .from(UsageRecords::Table)
            .and_where(Expr::cust(format!(
                "(tenant_id, gts_id, created_at, id) IN ({in_clause})"
            )))
            .to_owned();
        let (sql, _) = build_select_final(stmt);
        // Custom IN tuple binds are not in sea-query Values — apply SqlCtx binds.
        let mut q = self.client.query(&sql);
        for b in &ctx.binds {
            q = bind_one(q, b);
        }
        let rows: Vec<UsageRecordRow> = q
            .fetch_all()
            .await
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        Ok(rows.into_iter().map(|r| (row_dedup_key(&r), r)).collect())
    }

    /// Append metadata side-channel filters as [`Condition`]s.
    fn metadata_conditions(metadata_filter: &[MetadataFilter]) -> Condition {
        let mut all = Condition::all();
        for mf in metadata_filter {
            if mf.values().is_empty() {
                all = all.add(Expr::cust("FALSE"));
                continue;
            }
            let mut vals: Vec<sea_query::Value> =
                vec![sea_query::Value::String(Some(mf.key().as_str().to_owned()))];
            let mut ph = Vec::with_capacity(mf.values().len());
            for v in mf.values() {
                vals.push(sea_query::Value::String(Some(v.clone())));
                ph.push("?");
            }
            let template = format!("arrayElement(metadata, ?) IN ({})", ph.join(", "));
            all = all.add(Expr::cust_with_values(template, vals));
        }
        all
    }
}

/// The 4-tuple dedup identity for `usage_records`: `(tenant_id, gts_id,
/// created_at_micros, id)`.
///
/// `created_at` is stored as `i64` epoch-microseconds, so the dedup key is
/// already µs-normalised; no truncation is needed.
type DedupKey = (Uuid, String, i64, Uuid);

fn record_dedup_key(r: &UsageRecord) -> DedupKey {
    (
        r.tenant_id,
        gts_id_str(&r.gts_id).to_owned(),
        datetime_to_micros(r.created_at),
        r.id,
    )
}

fn row_dedup_key(r: &UsageRecordRow) -> DedupKey {
    (r.tenant_id, r.gts_id.clone(), r.created_at, r.id)
}

/// Build an `Internal` error noting a dedup invariant break and log it at
/// `error` level so it is observable without exposing identifiers to callers.
fn dedup_invariant_break(record: &UsageRecord, msg: &'static str) -> UsageCollectorPluginError {
    tracing::error!(
        tenant_id = %record.tenant_id,
        gts_id = %gts_id_str(&record.gts_id),
        idempotency_key = %record.idempotency_key.as_str(),
        "{msg}"
    );
    UsageCollectorPluginError::internal(msg)
}

/// Build a fresh, independently-owned copy of a partition-level failure for
/// embedding into every record's outcome slot in that `gts_id` partition.
///
/// [`UsageCollectorPluginError`] is intentionally not `Clone` (it is a
/// foundation-owned SPI contract type — `cpt-cf-usage-collector-dod-*
/// -plugin-contract-stability`), so [`ChRecordStore::create_batch`]
/// reconstructs an equivalent value per variant instead of cloning a shared
/// instance. Only [`CatalogLockPort::acquire_exclusive_for_create`],
/// [`ChRecordStore::check_catalog_existence`], [`ChRecordStore::batch_dedup_lookup`],
/// and [`ClusterLockGuard::ensure_still_held`](crate::infra::coordination::lock_manager::ClusterLockGuard::ensure_still_held)
/// can actually produce (`Transient` / `UsageTypeNotFound` / `Internal`) are
/// reachable here; the fallback arm exists only because the enum is
/// `#[non_exhaustive]`.
fn err_for_partition(err: &UsageCollectorPluginError) -> UsageCollectorPluginError {
    match err {
        UsageCollectorPluginError::Transient {
            detail,
            retry_after_seconds,
        } => UsageCollectorPluginError::Transient {
            detail: detail.clone(),
            retry_after_seconds: *retry_after_seconds,
        },
        UsageCollectorPluginError::UsageTypeNotFound { gts_id } => {
            UsageCollectorPluginError::UsageTypeNotFound {
                gts_id: gts_id.clone(),
            }
        }
        UsageCollectorPluginError::Internal(msg) => {
            UsageCollectorPluginError::Internal(msg.clone())
        }
        other => UsageCollectorPluginError::internal(other.to_string()),
    }
}

/// Fan a failed batch write out across the outcome slots that depended on it.
///
/// Rows absorbed from storage keep whatever the dedup read decided for them —
/// a write that never landed cannot invalidate a row that was already there —
/// so only the slots backed by a composed row are rewritten.
fn apply_insert_failure(
    err: &UsageCollectorPluginError,
    row_slots: &[Vec<usize>],
    outcomes: &mut [Option<Result<UsageRecord, UsageCollectorPluginError>>],
) {
    for slots in row_slots {
        for &idx in slots {
            outcomes[idx] = Some(Err(err_for_partition(err)));
        }
    }
}

impl ChRecordStore {
    /// Resolve a dedup-key hit against an already-materialised row.
    ///
    /// A stored row is absorbed only when it is still `active` and every
    /// canonical field matches. An `inactive` stored row means the dedup key
    /// was created and then deactivated, so re-creating it must not resurrect
    /// the deactivated row as a silent absorb — the key is already bound to a
    /// record the caller cannot have back, which is exactly
    /// [`UsageCollectorPluginError::IdempotencyConflict`].
    fn resolve_dedup_hit(
        &self,
        row: &UsageRecordRow,
        record: &UsageRecord,
    ) -> Result<UsageRecord, UsageCollectorPluginError> {
        if row.status == UsageRecordStatusCode::Inactive || !canonical_equal(row, record)? {
            self.metrics.inc_idempotency_conflict();
            return Err(UsageCollectorPluginError::IdempotencyConflict {
                idempotency_key: record.idempotency_key.as_str().to_owned(),
                existing_id: row.id,
            });
        }
        self.metrics.inc_dedup_absorbed();
        record_row_to_model(row.clone())
    }

    /// Critical section of create while holding `guard`.
    ///
    /// `op_start` is the caller's critical-section entry instant, forwarded to
    /// the insert-latency histogram so lock and catalog contention are inside
    /// the observed window.
    async fn create_under_lock(
        &self,
        record: UsageRecord,
        guard: &dyn LockGuardPort,
        op_start: Instant,
    ) -> Result<UsageRecord, UsageCollectorPluginError> {
        self.check_catalog_existence(&record.gts_id).await?;

        let stored = self.dedup_point_lookup(&record).await?;

        if let Some(row) = stored {
            return self.resolve_dedup_hit(&row, &record);
        }

        if let Err(e) = guard.ensure_still_held().await {
            self.metrics.inc_lock_manager_unavailable(LockMode::Create);
            return Err(e);
        }

        if record.corrects_id.is_some() {
            self.metrics.inc_compensation();
        }

        let version = current_merge_version();
        let row = record_to_row(&record, version);
        self.insert_record(&row, op_start).await?;
        record_row_to_model(row)
    }

    /// Critical section of one `gts_id` partition of [`Self::create_batch`],
    /// run while its exclusive partition lock is held.
    ///
    /// Mirrors [`Self::create_under_lock`]'s read phase: catalog existence
    /// check, batch dedup pre-read, then a lease renewal so the caller knows
    /// the partition is still owned before any row is composed. Returns the
    /// stored rows keyed by their dedup identity.
    ///
    /// # Errors
    ///
    /// Returns `UsageTypeNotFound` when the partition's usage type is absent,
    /// and `Transient` / `Internal` on `ClickHouse` or lease failures. Every
    /// error is a whole-partition failure — the caller fans it out across that
    /// partition's outcome slots.
    async fn create_partition_under_lock(
        &self,
        records: &[UsageRecord],
        idxs: &[usize],
        guard: &dyn LockGuardPort,
    ) -> Result<HashMap<DedupKey, UsageRecordRow>, UsageCollectorPluginError> {
        let first = *idxs.first().ok_or_else(|| {
            UsageCollectorPluginError::internal("empty gts_id partition (invariant break)")
        })?;
        self.check_catalog_existence(&records[first].gts_id).await?;

        let record_refs: Vec<&UsageRecord> = idxs.iter().map(|&i| &records[i]).collect();
        let existing = self.batch_dedup_lookup(&record_refs).await?;

        if let Err(e) = guard.ensure_still_held().await {
            self.metrics.inc_lock_manager_unavailable(LockMode::Create);
            return Err(e);
        }

        Ok(existing)
    }
}

#[async_trait]
impl RecordStore for ChRecordStore {
    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-ingest-dedup
    #[instrument(skip(self, record), fields(gts_id = %gts_id_str(&record.gts_id)))]
    async fn create(&self, record: UsageRecord) -> Result<UsageRecord, UsageCollectorPluginError> {
        let op_start = Instant::now();
        let guard = self
            .lock_manager
            .acquire_exclusive_for_create(gts_id_str(&record.gts_id))
            .await?;

        let result = self
            .create_under_lock(record, guard.as_ref(), op_start)
            .await;

        if let Err(e) = guard.release().await {
            tracing::warn!(error = %e, "failed to release create cluster lock");
            if result.is_ok() {
                return Err(e);
            }
        }

        result
    }

    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-ingest-batch
    #[instrument(skip(self, records), fields(batch_size = records.len()))]
    async fn create_batch(
        &self,
        records: Vec<UsageRecord>,
    ) -> Result<Vec<Result<UsageRecord, UsageCollectorPluginError>>, UsageCollectorPluginError>
    {
        if records.is_empty() {
            tracing::warn!(
                "create_usage_records called with an empty batch (host-contract breach)"
            );
            return Err(UsageCollectorPluginError::internal(
                "create_usage_records called with an empty batch (host-contract breach)",
            ));
        }

        let op_start = Instant::now();

        let mut grouped: HashMap<&str, Vec<usize>> = HashMap::new();
        for (idx, record) in records.iter().enumerate() {
            grouped
                .entry(gts_id_str(&record.gts_id))
                .or_default()
                .push(idx);
        }

        // Acquiring per-`gts_id` locks in `HashMap` iteration order lets two
        // concurrent multi-type batches take the same pair of locks in opposite
        // orders and deadlock until both leases lapse. Sorting the partition
        // keys gives every caller one global acquisition order.
        let mut partitions: Vec<(&str, Vec<usize>)> = grouped.into_iter().collect();
        partitions.sort_unstable_by(|a, b| a.0.cmp(b.0));

        let mut outcomes: Vec<Option<Result<UsageRecord, UsageCollectorPluginError>>> =
            (0..records.len()).map(|_| None).collect();

        // Phase 1: take every partition lock up front, sequentially, in the
        // sorted order above.
        let mut locked: Vec<(usize, Box<dyn LockGuardPort>)> = Vec::with_capacity(partitions.len());
        for (p_idx, (partition_gts_id, idxs)) in partitions.iter().enumerate() {
            match self
                .lock_manager
                .acquire_exclusive_for_create(partition_gts_id)
                .await
            {
                Ok(guard) => locked.push((p_idx, guard)),
                Err(e) => {
                    for &idx in idxs {
                        outcomes[idx] = Some(Err(err_for_partition(&e)));
                    }
                }
            }
        }

        // Phase 2: every partition now holds its lock, so their read phases are
        // independent and run concurrently.
        let prepared = join_all(locked.iter().map(|(p_idx, guard)| {
            self.create_partition_under_lock(&records, &partitions[*p_idx].1, guard.as_ref())
        }))
        .await;

        // Phase 3: compose rows sequentially so version offsets and
        // within-batch dedup stay deterministic.
        let version_base = current_merge_version();
        let mut next_offset: u64 = 0;
        let mut to_insert: Vec<UsageRecordRow> = Vec::new();
        // Row position in `to_insert` rather than a clone of the row itself.
        let mut insert_map: HashMap<DedupKey, usize> = HashMap::new();
        // Parallel to `to_insert`: the `held` position that composed each row,
        // and the outcome slots whose success depends on that row landing.
        let mut row_partition: Vec<usize> = Vec::new();
        let mut row_slots: Vec<Vec<usize>> = Vec::new();
        let mut held: Vec<(usize, Box<dyn LockGuardPort>)> = Vec::with_capacity(locked.len());

        for ((p_idx, guard), partition_result) in locked.into_iter().zip(prepared) {
            let idxs = &partitions[p_idx].1;
            let existing = match partition_result {
                Ok(existing) => existing,
                Err(e) => {
                    for &idx in idxs {
                        outcomes[idx] = Some(Err(err_for_partition(&e)));
                    }
                    if let Err(rel) = guard.release().await {
                        tracing::warn!(error = %rel, "failed to release create-batch cluster lock");
                    }
                    continue;
                }
            };

            let held_idx = held.len();
            for &idx in idxs {
                let record = &records[idx];
                let key = record_dedup_key(record);
                let outcome = if let Some(stored_row) = existing.get(&key) {
                    self.resolve_dedup_hit(stored_row, record)
                } else if let Some(&row_idx) = insert_map.get(&key) {
                    // A second row for the same dedup key inside this batch is
                    // only absorbed when it is canonically identical to the one
                    // already composed; otherwise it is a conflict just as it
                    // would be against a stored row.
                    let resolved = self.resolve_dedup_hit(&to_insert[row_idx], record);
                    if resolved.is_ok() {
                        row_slots[row_idx].push(idx);
                    }
                    resolved
                } else {
                    let version = version_base.saturating_add(next_offset);
                    next_offset += 1;
                    let row = record_to_row(record, version);
                    let row_idx = to_insert.len();
                    to_insert.push(row);
                    insert_map.insert(key, row_idx);
                    row_partition.push(held_idx);
                    row_slots.push(vec![idx]);
                    if record.corrects_id.is_some() {
                        self.metrics.inc_compensation();
                    }
                    record_row_to_model(to_insert[row_idx].clone())
                };
                outcomes[idx] = Some(outcome);
            }

            held.push((p_idx, guard));
        }

        // Phase 4: renew every lease concurrently immediately before the
        // combined write, so a lease that expired while the other partitions
        // were being prepared cannot let a concurrent `delete_usage_type`
        // orphan these rows.  Concurrent renewal prevents the sequential
        // Nth-partition expiry hazard under tight lock_ttl_secs (each
        // ensure_still_held is a cluster round-trip).
        let mut expired: HashSet<usize> = HashSet::new();
        let renew_results = join_all(
            held.iter().enumerate().map(|(held_idx, (p_idx, guard))| {
                async move {
                    let result = guard.ensure_still_held().await;
                    (held_idx, *p_idx, result)
                }
            }),
        )
        .await;
        for (held_idx, p_idx, result) in &renew_results {
            if let Err(e) = result {
                self.metrics.inc_lock_manager_unavailable(LockMode::Create);
                for &idx in &partitions[*p_idx].1 {
                    outcomes[idx] = Some(Err(err_for_partition(e)));
                }
                expired.insert(*held_idx);
            }
        }
        if !expired.is_empty() {
            let mut kept_rows = Vec::with_capacity(to_insert.len());
            let mut kept_slots = Vec::with_capacity(row_slots.len());
            for ((row, slots), partition) in
                to_insert.into_iter().zip(row_slots).zip(&row_partition)
            {
                if !expired.contains(partition) {
                    kept_rows.push(row);
                    kept_slots.push(slots);
                }
            }
            to_insert = kept_rows;
            row_slots = kept_slots;
        }

        let insert_result = self.insert_records(&to_insert, op_start).await;

        // The cluster guard's `Drop` is a no-op, so every guard is released
        // explicitly — including on the insert-failure path, which would
        // otherwise hold each `gts_id` until its lease lapsed.
        for (_, guard) in held {
            if let Err(e) = guard.release().await {
                tracing::warn!(error = %e, "failed to release create-batch cluster lock");
            }
        }

        // A failed write does not invalidate the outcomes already decided for
        // absorbed rows, so it is reported per slot rather than as a top-level
        // error that would discard the whole batch's per-record contract.
        if let Err(e) = insert_result {
            tracing::warn!(error = %e, "create-batch insert failed; reporting per-record outcomes");
            apply_insert_failure(&e, &row_slots, &mut outcomes);
        }

        let results = outcomes
            .into_iter()
            .enumerate()
            .map(|(idx, outcome)| {
                outcome.unwrap_or_else(|| {
                    Err(dedup_invariant_break(
                        &records[idx],
                        "batch index unresolved after partition processing (invariant break)",
                    ))
                })
            })
            .collect();

        Ok(results)
    }

    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-get
    #[instrument(skip_all, fields(record_id = %id))]
    async fn get(&self, id: Uuid) -> Result<UsageRecord, UsageCollectorPluginError> {
        let row: Option<UsageRecordRow> = prepared_sql(
            &self.client,
            record_get_by_id(&id.to_string()),
        )
        .map_err(UsageCollectorPluginError::internal)?
        .fetch_optional()
        .await
        .map_err(|e| tracked_ch_err(&self.metrics, &e))?;

        match row {
            Some(row) => record_row_to_model(row),
            None => Err(UsageCollectorPluginError::UsageRecordNotFound { id }),
        }
    }

    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-list-keyset
    #[instrument(skip_all, fields(gts_id = %gts_id_str(&gts_id)))]
    async fn list(
        &self,
        gts_id: UsageTypeGtsId,
        query: &ODataQuery,
        metadata_filter: &[MetadataFilter],
    ) -> Result<ODataPage<UsageRecord>, UsageCollectorPluginError> {
        let _timer =
            OpDurationGuard::start(Arc::clone(&self.metrics), TimedOp::Query(QueryKind::Raw));
        self.metrics.inc_query_request(QueryKind::Raw);

        let limit =
            effective_page_size(query.limit, crate::infra::storage::query::DEFAULT_PAGE_SIZE);

        let q = {
            let mut stmt = Query::select()
                .columns(RECORD_SELECT_COLUMNS)
                .from(UsageRecords::Table)
                .and_where(Expr::col(UsageRecords::GtsId).eq(gts_id_str(&gts_id)))
                .limit(limit.saturating_add(1))
                .to_owned();

            if let Some(expr) = query.filter() {
                let node = convert_expr_to_filter_node::<UsageRecordFilterField>(expr)
                    .map_err(|e| UsageCollectorPluginError::internal(format!("invalid filter: {e}")))?;
                let cond =
                    translate_record_filter(&node).map_err(UsageCollectorPluginError::internal)?;
                apply_condition(&mut stmt, cond);
            }

            apply_condition(&mut stmt, Self::metadata_conditions(metadata_filter));

            if let Some(cursor) = query.cursor.as_ref() {
                ensure_forward_cursor(cursor).map_err(UsageCollectorPluginError::internal)?;
                if cursor.f.as_deref() != query.filter_hash.as_deref() {
                    return Err(UsageCollectorPluginError::internal(
                        "cursor filter hash mismatch",
                    ));
                }
                if !query.order.equals_signed_tokens(&cursor.s) {
                    return Err(UsageCollectorPluginError::internal(
                        "cursor sort order mismatch",
                    ));
                }
                let order_pairs: Vec<(&str, bool)> = query
                    .order
                    .0
                    .iter()
                    .map(|key| (key.field.as_str(), matches!(key.dir, SortDir::Asc)))
                    .collect();
                let cond = keyset_condition(
                    &order_pairs,
                    &cursor.k,
                    record_column,
                    |name| UsageRecordFilterField::from_name(name).map(|f| f.kind()),
                    is_keyset_safe_record_field,
                )
                .map_err(UsageCollectorPluginError::internal)?;
                apply_condition(&mut stmt, cond);
            }

            let order_clauses = order_by_clauses(&query.order, record_column)
                .map_err(UsageCollectorPluginError::internal)?;
            for (col, dir) in order_clauses {
                stmt.order_by_expr(Expr::cust(col), dir);
            }

            prepared_select(&self.client, stmt).map_err(UsageCollectorPluginError::internal)?
        };
        let mut rows: Vec<UsageRecordRow> = q
            .fetch_all()
            .await
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?;

        let has_next = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
        if has_next {
            rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        }

        let next_cursor = if has_next {
            let last = rows.last().ok_or_else(|| {
                UsageCollectorPluginError::internal("non-empty page lost its tail")
            })?;
            let keys = query
                .order
                .0
                .iter()
                .map(|key| {
                    record_row_key(last, &key.field).ok_or_else(|| {
                        UsageCollectorPluginError::internal(format!(
                            "order field `{}` has no cursor key on the row",
                            key.field
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let token = encode_next_cursor(&query.order, &keys, query.filter_hash.as_deref())
                .map_err(UsageCollectorPluginError::internal)?;
            Some(token)
        } else {
            None
        };

        let items = rows
            .into_iter()
            .map(record_row_to_model)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ODataPage::new(
            items,
            PageInfo {
                next_cursor,
                prev_cursor: None,
                limit,
            },
        ))
    }

    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-query-aggregated
    #[instrument(skip_all, fields(gts_id = %gts_id_str(&gts_id)))]
    async fn aggregate(
        &self,
        gts_id: UsageTypeGtsId,
        query: &ODataQuery,
        metadata_filter: &[MetadataFilter],
        spec: AggregationSpec,
    ) -> Result<AggregationResult, UsageCollectorPluginError> {
        let _timer = OpDurationGuard::start(
            Arc::clone(&self.metrics),
            TimedOp::Query(QueryKind::Aggregated),
        );
        self.metrics.inc_query_request(QueryKind::Aggregated);

        let dim_count = spec.group_by.len();
        let q = {
            let mut stmt = Query::select().from(UsageRecords::Table).to_owned();
            apply_condition(
                &mut stmt,
                Condition::all()
                    .add(Expr::col(UsageRecords::GtsId).eq(gts_id_str(&gts_id)))
                    .add(Expr::cust("status = 'active'")),
            );
            if let Some(clause) = corrects_id_partition_clause(spec.op) {
                apply_condition(&mut stmt, Condition::all().add(Expr::cust(clause)));
            }
            if let Some(expr) = query.filter() {
                let node = convert_expr_to_filter_node::<UsageRecordFilterField>(expr)
                    .map_err(|e| UsageCollectorPluginError::internal(format!("invalid filter: {e}")))?;
                let cond =
                    translate_record_filter(&node).map_err(UsageCollectorPluginError::internal)?;
                apply_condition(&mut stmt, cond);
            }
            apply_condition(&mut stmt, Self::metadata_conditions(metadata_filter));
            for dim in &spec.group_by {
                match dim {
                    AggregationDimension::SubjectId => {
                        apply_condition(
                            &mut stmt,
                            Condition::all().add(Expr::cust("subject_id IS NOT NULL")),
                        );
                    }
                    AggregationDimension::SubjectType => {
                        apply_condition(
                            &mut stmt,
                            Condition::all().add(Expr::cust("subject_type IS NOT NULL")),
                        );
                    }
                    _ => {}
                }
            }

            for (i, dim) in spec.group_by.iter().enumerate() {
                stmt.expr_as(dimension_select_expr(dim), Alias::new(format!("d{i}")));
            }
            stmt.expr_as(agg_select_expr(spec.op), Alias::new("agg"));

            if dim_count > 0 {
                let ordinals: Vec<SimpleExpr> = (1..=dim_count)
                    .map(|n| Expr::cust(n.to_string()))
                    .collect();
                stmt.add_group_by(ordinals);
            }
            if let Some(lim) = aggregate_limit(dim_count) {
                stmt.limit(lim);
            }

            prepared_select(&self.client, stmt).map_err(UsageCollectorPluginError::internal)?
        };
        let mut cursor = q
            .fetch_rows()
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        let mut buckets = Vec::new();
        while let Some(row) = cursor
            .next()
            .await
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?
        {
            buckets.push(bucket_from_data_row(&row, dim_count)?);
        }

        Ok(AggregationResult { buckets })
    }

    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-deactivate-cascade
    #[instrument(skip_all, fields(record_id = %id))]
    async fn deactivate(&self, id: Uuid) -> Result<(), UsageCollectorPluginError> {
        let _timer = OpDurationGuard::start(Arc::clone(&self.metrics), TimedOp::Deactivate);

        // No coordination lock required for deactivation (DESIGN.md §3.6): the host
        // prevents a concurrent compensation from reaching create_usage_record while
        // a deactivation is in flight (plugin-spi.md Method 5 caller-side rule).

        // Step 1: Read the target + active depth-1 compensations.
        let rows: Vec<UsageRecordRow> = {
            let id_s = id.to_string();
            let stmt = Query::select()
                .columns(RECORD_SELECT_COLUMNS)
                .from(UsageRecords::Table)
                .cond_where(
                    Condition::any()
                        .add(Expr::col(UsageRecords::Id).eq(id_s.clone()))
                        .add(
                            Condition::all()
                                .add(Expr::col(UsageRecords::CorrectsId).eq(id_s))
                                .add(Expr::cust("status = 'active'")),
                        ),
                )
                .to_owned();
            prepared_select(&self.client, stmt)
                .map_err(UsageCollectorPluginError::internal)?
                .fetch_all()
                .await
                .map_err(|e| tracked_ch_err(&self.metrics, &e))?
        };

        // Step 2: Identify target and compensation rows.
        let target_row = rows.iter().find(|r| r.id == id);
        match target_row {
            None => return Err(UsageCollectorPluginError::UsageRecordNotFound { id }),
            Some(r) if r.status == UsageRecordStatusCode::Inactive => {
                return Err(UsageCollectorPluginError::UsageRecordAlreadyInactive { id });
            }
            Some(_) => {}
        }

        // Step 3: Compose one versioned marker row per affected id (target +
        // active compensations). `version_higher_than` mints each marker's
        // version off the row it supersedes, so no batch-wide base version is
        // needed — the per-row offset only spaces markers whose source rows
        // already share a version.
        let markers: Vec<UsageRecordRow> = rows
            .iter()
            .enumerate()
            .map(|(i, r)| make_inactive_marker(r, version_higher_than(r.version, i as u64), 0))
            .collect();

        // Step 4: One multi-row INSERT for all marker rows.
        // ATOMICITY NOTE (DESIGN.md §3.6): A single ClickHouse INSERT is applied as
        // one atomic part write. A FINAL-qualified reader either sees the pre-cascade
        // state or the fully-flipped state — never a partially-flipped cascade.
        self.insert_records(&markers, Instant::now()).await?;

        Ok(())
    }
}


/// Decode one aggregate [`clickhouse::DataRow`] into an [`AggregationBucket`].
///
/// Dimension columns are aliased `d0`…`dN` and the measure column is `agg`,
/// matching the SELECT built by [`RecordStore::aggregate`].
fn bucket_from_data_row(
    row: &clickhouse::DataRow,
    dim_count: usize,
) -> Result<AggregationBucket, UsageCollectorPluginError> {
    let mut key = Vec::with_capacity(dim_count);
    for i in 0..dim_count {
        let alias = format!("d{i}");
        let idx = row
            .column_names
            .iter()
            .position(|c| c.as_ref() == alias)
            .ok_or_else(|| {
                UsageCollectorPluginError::internal(format!(
                    "aggregate response missing dimension column {alias}"
                ))
            })?;
        key.push(sea_value_as_string(&row.values[idx]));
    }

    let agg_idx = row
        .column_names
        .iter()
        .position(|c| c.as_ref() == "agg")
        .ok_or_else(|| {
            UsageCollectorPluginError::internal("aggregate response missing agg column")
        })?;
    let value = sea_value_as_optional_bigdecimal(&row.values[agg_idx])?;
    Ok(AggregationBucket { key, value })
}

fn sea_value_as_string(v: &SeaValue) -> String {
    match v {
        SeaValue::String(Some(s)) => s.clone(),
        SeaValue::String(None) => String::new(),
        SeaValue::Uuid(Some(u)) => u.to_string(),
        SeaValue::Uuid(None) => String::new(),
        SeaValue::BigInt(Some(n)) => n.to_string(),
        SeaValue::BigUnsigned(Some(n)) => n.to_string(),
        SeaValue::Int(Some(n)) => n.to_string(),
        SeaValue::Unsigned(Some(n)) => n.to_string(),
        SeaValue::SmallInt(Some(n)) => n.to_string(),
        SeaValue::SmallUnsigned(Some(n)) => n.to_string(),
        SeaValue::TinyInt(Some(n)) => n.to_string(),
        SeaValue::TinyUnsigned(Some(n)) => n.to_string(),
        SeaValue::Double(Some(n)) => n.to_string(),
        SeaValue::Float(Some(n)) => n.to_string(),
        SeaValue::Bool(Some(b)) => b.to_string(),
        SeaValue::Decimal(Some(d)) => d.to_string(),
        _ => String::new(),
    }
}

fn sea_value_as_optional_bigdecimal(
    v: &SeaValue,
) -> Result<Option<BigDecimal>, UsageCollectorPluginError> {
    let s = match v {
        SeaValue::Decimal(None)
        | SeaValue::Double(None)
        | SeaValue::Float(None)
        | SeaValue::BigInt(None)
        | SeaValue::BigUnsigned(None)
        | SeaValue::Int(None)
        | SeaValue::Unsigned(None)
        | SeaValue::String(None) => return Ok(None),
        SeaValue::Decimal(Some(d)) => d.to_string(),
        SeaValue::Double(Some(n)) => n.to_string(),
        SeaValue::Float(Some(n)) => n.to_string(),
        SeaValue::BigInt(Some(n)) => n.to_string(),
        SeaValue::BigUnsigned(Some(n)) => n.to_string(),
        SeaValue::Int(Some(n)) => n.to_string(),
        SeaValue::Unsigned(Some(n)) => n.to_string(),
        SeaValue::SmallInt(Some(n)) => n.to_string(),
        SeaValue::SmallUnsigned(Some(n)) => n.to_string(),
        SeaValue::TinyInt(Some(n)) => n.to_string(),
        SeaValue::TinyUnsigned(Some(n)) => n.to_string(),
        SeaValue::String(Some(s)) => s.clone(),
        other => {
            return Err(UsageCollectorPluginError::internal(format!(
                "unexpected aggregate value type: {other:?}"
            )));
        }
    };
    Ok(Some(BigDecimal::from_str(&s).map_err(|e| {
        UsageCollectorPluginError::internal(format!("aggregate value parse error: {e}"))
    })?))
}

/// Decode a `JSONEachRow` aggregate response body into [`AggregationBucket`]s.
///
/// `dim_names` are the `d0`…`dN` column aliases the SELECT emitted, in
/// `group_by` order; a missing or non-string dimension decodes as an empty
/// key component. The `agg` column is accepted as a JSON string or number and
/// parsed into a [`BigDecimal`]; a JSON `null` (an empty `MIN`/`MAX`/`AVG`
/// group) becomes `None`.
///
/// # Errors
///
/// Returns [`UsageCollectorPluginError::Internal`] when a line is not valid
/// JSON, carries an `agg` value of an unexpected JSON type, or holds an
/// unparseable decimal.
#[cfg(test)]
fn parse_aggregate_response(
    bytes: &[u8],
    dim_names: &[String],
) -> Result<Vec<AggregationBucket>, UsageCollectorPluginError> {
    let mut buckets = Vec::new();
    for line in BufReader::new(bytes).lines() {
        let line = line.map_err(|e| {
            UsageCollectorPluginError::internal(format!("aggregate response read error: {e}"))
        })?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let obj: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            UsageCollectorPluginError::internal(format!("aggregate JSON parse error: {e}"))
        })?;
        let key = dim_names
            .iter()
            .map(|dim_name| {
                obj.get(dim_name)
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
                    .unwrap_or_default()
            })
            .collect();
        let value = match obj.get("agg") {
            None | Some(serde_json::Value::Null) => None,
            Some(v) => {
                let s = match v {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    other => {
                        return Err(UsageCollectorPluginError::internal(format!(
                            "unexpected aggregate value type: {other}"
                        )));
                    }
                };
                Some(s.parse::<BigDecimal>().map_err(|e| {
                    UsageCollectorPluginError::internal(format!("aggregate value parse error: {e}"))
                })?)
            }
        };
        buckets.push(AggregationBucket { key, value });
    }
    Ok(buckets)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "record_store_tests.rs"]
mod record_store_tests;
