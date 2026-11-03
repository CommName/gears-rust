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
//! - **No `FOR UPDATE`, no coordination lock**: `ClickHouse` has no row-level
//!   locks and this plugin uses no external mutex. Creates are not serialised
//!   against each other. Two concurrent creates for one dedup key can both
//!   pass the pre-read and both write. Inside one plugin instance the ingest
//!   queue coalesces them onto one row ([`crate::infra::storage::ingest`]);
//!   across instances both rows are stored — each flush carries its own
//!   `insert_deduplication_token` ([`insert_dedup_token`]), which the engine
//!   only matches against an identical flush within the table's finite dedup
//!   window — and every read resolves them to the highest `version`
//!   ([`query::dedup`]). Consequences: (a) the dedup pre-read is best-effort;
//!   (b) `IdempotencyConflict` is raised when the earlier row is already
//!   visible at pre-read time, or is pending in this instance's queue;
//!   differing payloads written by two instances inside each other's window
//!   converge highest-version-wins with both callers answered `Ok` — there is
//!   no cross-instance first-writer-wins; (c) `UsageTypeNotFound` comes from a
//!   catalog read with no ordering guarantee against a concurrent
//!   `create_usage_type`; (d) that same catalog read has no ordering guarantee
//!   against a concurrent `delete_usage_type` either — the delete's own
//!   post-delete sweep (see `catalog_store`) removes rows that land inside its
//!   window, but an insert that passed the check before the delete and commits
//!   after the sweep orphans a row. That residual is accepted by design; the
//!   insert-time catalog check is what bounds it, since a deleted type is
//!   refused here from the moment its catalog row is gone.
//! - **Group commit**: with `insert_batch` enabled (the default) neither
//!   `create` nor `create_batch` issues its own `INSERT`. Each runs its checks
//!   and pre-read, composes its rows, and submits them to the plugin's one
//!   [`IngestQueue`]; the batcher writes queued calls as shared synchronous
//!   `INSERT`s through [`ChFlushSink`] and the call awaits the outcome of the
//!   flush that carried its rows. A call's rows stay together in one flush,
//!   so its visibility is that of one statement: one part per
//!   `toYYYYMM(created_at)` partition and, on a cluster, per shard. The
//!   deactivation cascade and every catalog write bypass the queue and stay
//!   synchronous.
//! - **No `UPDATE`**: deactivation uses versioned marker rows (INSERT with
//!   `status = 'inactive'` and a higher `version`, same `id`) rather than
//!   `ALTER TABLE … UPDATE` (an async mutation unsuitable for the request path).
//! - **Every read resolves versions itself**: `get`, the deactivation cascade
//!   and the create-path dedup lookups resolve `ReplacingMergeTree` versions
//!   over a tiny candidate set — `ORDER BY version DESC LIMIT 1 BY <sort key>`,
//!   see [`query::dedup`]. `list` and `aggregate` read `FROM usage_records
//!   FINAL` (`do_not_merge_across_partitions_select_final = 1`), so a
//!   deactivation marker and a duplicate create are both collapsed before the
//!   caller's predicates, ordering, limit or aggregation see a row. No read
//!   waits for or depends on a background merge.
//!
//!   [`query::dedup`]: crate::infra::storage::query::dedup
//! - **`?` placeholders**: `ClickHouse` uses positional `?` (not `$N`).
//! - **`metadata['key']`**: map subscript (not `metadata ->> key`).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use tracing::instrument;
use uuid::Uuid;

use toolkit_odata::filter::{FilterField, FilterNode, convert_expr_to_filter_node};
use toolkit_odata::{ODataQuery, Page as ODataPage, PageInfo, SortDir};

use usage_collector_sdk::{
    AggregationBucket, AggregationDimension, AggregationResult, AggregationSpec, MetadataFilter,
    UsageCollectorPluginError, UsageRecord, UsageRecordFilterField, UsageTypeGtsId,
    is_keyset_safe_record_field,
};

use crate::domain::ports::RecordStore;
use crate::infra::metrics::{InsertMode, Metrics, OpDurationGuard, QueryKind, TimedOp};
use crate::infra::storage::entity::{
    EpochMicros, UsageRecordRow, UsageRecordStatusCode, ch_decimal128_9,
};
use crate::infra::storage::error::{classify, kind_of, tracked_ch_err, with_deadline};
use crate::infra::storage::ingest::sink::{FlushError, FlushSink};
use crate::infra::storage::ingest::{CallOutcome, IngestQueue, PendingCall, remaining_budget};
use crate::infra::storage::mapper::{
    canonical_equal, current_merge_version, make_inactive_marker, record_row_key,
    version_higher_than,
};
use crate::infra::storage::pool::configure_insert;
use crate::infra::storage::query::aggregate::{
    agg_select_expr, aggregate_limit_clause, corrects_id_partition_clause, dimension_select_expr,
};
use crate::infra::storage::query::dedup::{RECORD_DEDUP_KEY, latest_by};
use crate::infra::storage::query::effective_page_size;
use crate::infra::storage::query::keyset::{
    encode_next_cursor, ensure_forward_cursor, keyset_predicate, render_order_by,
};
use crate::infra::storage::query::translate::{
    SqlBind, SqlCtx, bind_one, record_column, translate_record_conjuncts,
};

/// Static column list for every `usage_records` SELECT, in [`UsageRecordRow`]
/// field order. A `'static` constant (never caller input), so decoding is
/// positional without SQL injection risk.
const RECORD_COLUMNS: &str = "id, tenant_id, gts_id, value, created_at, resource_id, \
     resource_type, subject_id, subject_type, idempotency_key, corrects_id, status, metadata, \
     ingested_at, version";

/// What a `usage_records` `INSERT` writes, for [`insert_dedup_token`].
///
/// A deactivation marker carries the *same* `id` as the row it supersedes, so
/// the token must tell the two apart: otherwise a marker set written inside the
/// engine's dedup window of the create that wrote those very ids would be
/// dropped as a retry of it, and the deactivation would silently not happen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertKind {
    /// Ordinary or compensation usage rows (single create or batch).
    Record,
    /// Deactivation markers written by the cascade.
    Marker,
}

impl InsertKind {
    /// Fixed `UUIDv5` namespace per kind — the discriminator described above.
    fn namespace(self) -> Uuid {
        match self {
            // Arbitrary fixed values; they only need to be distinct and stable.
            Self::Record => Uuid::from_u128(0x7a4c_1f0e_2b9d_4c63_8e5a_0d1b_9f2e_3c4d),
            Self::Marker => Uuid::from_u128(0x2e9b_6d3a_8f10_4b7c_a1d5_6e2f_0c8b_9a7e),
        }
    }
}

/// `insert_deduplication_token` for one `usage_records` `INSERT`.
///
/// `ClickHouse` deduplicates an inserted block against the last
/// `non_replicated_deduplication_window` blocks by this token (per partition
/// the block touches), so two racing statements writing the same rows must
/// produce the same token and any other pair must not. The token is the `UUIDv5`
/// of the rows' `id`s — sorted and deduplicated, so it is insensitive to row
/// order and to a repeated row — under a per-[`InsertKind`] namespace. A single
/// create and a batch containing only that record therefore produce the same
/// token, which is the intended behaviour: they are the same write.
///
/// `id` is itself the `UUIDv5` of the record's dedup tuple (plugin-spi.md), so
/// two statements share a token exactly when they carry the same set of
/// logical rows of the same kind.
pub(crate) fn insert_dedup_token(rows: &[UsageRecordRow], kind: InsertKind) -> String {
    insert_dedup_token_for(rows.iter(), kind)
}

/// [`insert_dedup_token`] over any row iterator (the ingest queue holds its
/// rows behind `Arc`).
pub(crate) fn insert_dedup_token_for<'a>(
    rows: impl IntoIterator<Item = &'a UsageRecordRow>,
    kind: InsertKind,
) -> String {
    let mut ids: Vec<Uuid> = rows.into_iter().map(|r| r.id).collect();
    ids.sort_unstable();
    ids.dedup();
    let mut buf = Vec::with_capacity(ids.len() * 16);
    for id in &ids {
        buf.extend_from_slice(id.as_bytes());
    }
    Uuid::new_v5(&kind.namespace(), &buf).to_string()
}

/// Settings every `FINAL` range read (`list`, `aggregate`) carries.
///
/// `do_not_merge_across_partitions_select_final = 1` lets `ClickHouse`
/// collapse each partition on its own instead of one merge across all of
/// them. It is only correct when every version of a sort key lives in one
/// partition, which holds here: the partition key `toYYYYMM(created_at)` is a
/// function of the sort-key column `created_at`, and a marker or duplicate
/// shares its source row's `created_at`. `ClickHouse` applies the optimisation
/// automatically only when the partition key is a *prefix* of the sorting key,
/// so it has to be stated.
const FINAL_READ_SETTINGS: &[(&str, &str)] =
    &[("do_not_merge_across_partitions_select_final", "1")];

/// `ClickHouse`-backed implementation of [`RecordStore`] over `usage_records`.
#[derive(Clone)]
pub struct ChRecordStore {
    client: clickhouse::Client,
    metrics: Arc<Metrics>,
    request_timeout: Duration,
    /// Whether direct single-row `usage_records` `INSERT`s (group commit off)
    /// carry `async_insert = 1` / `wait_for_async_insert = 1`
    /// ([`crate::config::ClickHousePluginConfig::async_insert`]).
    async_insert: bool,
    /// The group-commit queue every record write is submitted to; `None`
    /// keeps the direct path (one `INSERT` per call).
    ingest: Option<Arc<IngestQueue>>,
}

impl std::fmt::Debug for ChRecordStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChRecordStore")
            .field("request_timeout", &self.request_timeout)
            .field("async_insert", &self.async_insert)
            .field("group_commit", &self.ingest.is_some())
            .finish_non_exhaustive()
    }
}

impl ChRecordStore {
    /// Build a store from an existing `ClickHouse` client, metric inventory,
    /// per-request client-side deadline, and the ingest queue to write through.
    ///
    /// There is no coordination primitive to inject: every write path is a
    /// plain read-then-insert whose concurrency semantics are described in the
    /// module docs.
    ///
    /// `request_timeout` bounds every individual `ClickHouse` await and, with
    /// group commit, the whole wait for a call's flush; production wiring
    /// passes `ClickHousePluginConfig::client_deadline()`. It is an explicit
    /// parameter rather than a defaulted builder step so a future wiring
    /// change cannot silently leave a store on a default that disagrees with
    /// the configured budget.
    ///
    /// `async_insert` selects server-side asynchronous inserts for the direct
    /// single-row path; production wiring passes
    /// `ClickHousePluginConfig::async_insert`. `ingest` is the plugin's
    /// group-commit queue (`Some` when `insert_batch.enabled`); both are
    /// explicit for the same reason as `request_timeout` — a store silently
    /// defaulting to the opposite of the configured value would change
    /// durability and part-formation behaviour without anything failing.
    #[must_use]
    pub fn new(
        client: clickhouse::Client,
        metrics: Arc<Metrics>,
        request_timeout: Duration,
        async_insert: bool,
        ingest: Option<Arc<IngestQueue>>,
    ) -> Self {
        Self {
            client,
            metrics,
            request_timeout,
            async_insert,
            ingest,
        }
    }

    /// The write side of the group-commit queue over this store's client:
    /// what `Gear::init` hands to `IngestQueue::run`.
    #[must_use]
    pub fn flush_sink(&self) -> Arc<dyn FlushSink> {
        Arc::new(ChFlushSink {
            client: self.client.clone(),
            metrics: Arc::clone(&self.metrics),
            request_timeout: self.request_timeout,
        })
    }

    /// Execute a `SELECT … FROM usage_type_catalog WHERE gts_id = ?` catalog
    /// existence check for a single record.
    ///
    /// Returns `Ok(())` if the usage type exists; `UsageTypeNotFound`
    /// otherwise.
    ///
    /// A type seen here is **not** guaranteed to still exist when the `INSERT`
    /// that follows commits: `delete_usage_type` removes the catalog row with
    /// no mutual exclusion against this path. That is the accepted residual
    /// documented on
    /// [`ChCatalogStore::delete`](crate::infra::storage::catalog_store) — the
    /// delete's post-delete sweep removes rows that land inside its own
    /// window, but a check that passed *before* the delete and an `INSERT`
    /// that commits *after* the sweep still orphan a row. This check is what
    /// keeps that window narrow rather than unbounded: once the catalog row is
    /// gone, every subsequent insert for the `gts_id` is refused here.
    ///
    /// No version resolution: the question is only whether *any* row carries
    /// this `gts_id`. Existence is invariant across versions — every physical
    /// copy shares the `gts_id` that is the whole sort key, and no copy is a
    /// tombstone — so `LIMIT 1` on the first match is both correct and the
    /// cheapest form of the query.
    ///
    /// # Errors
    ///
    /// Returns `Transient` on connectivity errors, `Internal` on protocol
    /// errors, `UsageTypeNotFound` when absent.
    ///
    /// (`ClickHouse` errors are mapped via [`tracked_ch_err`].)
    #[instrument(skip_all, fields(gts_id = %gts_id.as_ref()))]
    async fn check_catalog_existence(
        &self,
        gts_id: &UsageTypeGtsId,
    ) -> Result<(), UsageCollectorPluginError> {
        let sql = "SELECT gts_id FROM usage_type_catalog WHERE gts_id = ? LIMIT 1";
        let found: Option<String> = {
            with_deadline(
                &self.metrics,
                self.request_timeout,
                self.client
                    .query(sql)
                    .bind(gts_id.as_ref())
                    .fetch_optional::<String>(),
            )
            .await?
        };
        if found.is_none() {
            return Err(UsageCollectorPluginError::UsageTypeNotFound {
                gts_id: gts_id.clone(),
            });
        }
        Ok(())
    }

    /// Catalog existence check for a whole batch: one
    /// `SELECT gts_id FROM usage_type_catalog WHERE gts_id IN (…)` over the
    /// batch's distinct `gts_id`s, returning the subset that exists.
    ///
    /// The batch analogue of [`Self::check_catalog_existence`]; the caller
    /// maps every record whose `gts_id` is absent from the result to
    /// `UsageTypeNotFound`.
    ///
    /// # Errors
    ///
    /// Returns `Transient` or `Internal` on `ClickHouse` errors.
    #[instrument(skip_all, fields(type_count = gts_ids.len()))]
    async fn existing_usage_types(
        &self,
        gts_ids: &BTreeSet<&str>,
    ) -> Result<HashSet<String>, UsageCollectorPluginError> {
        if gts_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let (sql, ctx) = catalog_lookup_sql(gts_ids);
        let mut q = self.client.query(&sql);
        for b in &ctx.binds {
            q = bind_one(q, b);
        }
        let found: Vec<String> =
            { with_deadline(&self.metrics, self.request_timeout, q.fetch_all::<String>()).await? };
        Ok(found.into_iter().collect())
    }

    /// Dedup lookup on the canonical dedup tuple [`DedupKey`]:
    /// `WHERE tenant_id = ? AND gts_id = ? AND created_at = ? AND
    /// idempotency_key = ?`
    ///
    /// The three leading columns are the `ORDER BY` prefix, so this still
    /// prunes down to a primary-key point; `idempotency_key` is a residual
    /// filter over the handful of rows sharing that exact microsecond.
    ///
    /// Returns the stored row if found, `None` if no row exists for this key.
    /// A well-formed table holds at most one matching row, but `ClickHouse`
    /// cannot enforce that — see [`prefer_dedup_row`] for how a legacy
    /// mismatched-`id` twin is resolved. `ORDER BY id` only makes the candidate
    /// order stable; the choice itself is made by `prefer_dedup_row`.
    ///
    /// `LIMIT 1 BY id` — not the full sort key — is the version-resolution
    /// step: the `WHERE` already pins `gts_id`, `tenant_id` and `created_at`,
    /// so `id` is the only part of the sort key still free. Each surviving `id`
    /// is one logical row at its highest `version`, and `prefer_dedup_row` then
    /// chooses between distinct `id`s.
    ///
    /// NOTE — filtering on `idempotency_key` (not a sort-key column) *below*
    /// the resolution step is safe. `idempotency_key` is invariant across every
    /// version of a given sort key: `id` is the `UUIDv5` of the canonical
    /// tuple, and [`make_inactive_marker`] clones the source row wholesale, so a
    /// deactivation marker carries the same key. Filtering before and after
    /// resolution therefore selects the same sort keys — which is what keeps
    /// the "inactive stored row ⇒ conflict" path in
    /// [`Self::resolve_dedup_hit`] working: a deactivated key's surviving row
    /// is its marker, so the `status` the caller sees is `inactive`.
    ///
    /// # Errors
    ///
    /// Returns `Transient` or `Internal` on `ClickHouse` errors.
    #[instrument(skip_all, fields(gts_id = %record.gts_id.as_ref()))]
    async fn dedup_point_lookup(
        &self,
        record: &UsageRecord,
    ) -> Result<Option<UsageRecordRow>, UsageCollectorPluginError> {
        // Spelled out rather than built from `dedup::latest_by`: the candidate
        // order `prefer_dedup_row` relies on (`id` ascending) has to lead the
        // `ORDER BY`, with `version DESC` as the tie-break that makes
        // `LIMIT 1 BY id` keep each id's newest row.
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM usage_records \
             WHERE gts_id = ? AND tenant_id = ? \
             AND created_at = fromUnixTimestamp64Micro(?) AND idempotency_key = ? \
             ORDER BY id ASC, version DESC LIMIT 1 BY id"
        );
        let created_at_micros = EpochMicros::from(record.created_at).0;
        let rows: Vec<UsageRecordRow> = {
            with_deadline(
                &self.metrics,
                self.request_timeout,
                self.client
                    .query(&sql)
                    .bind(record.gts_id.as_ref())
                    .bind(record.tenant_id.to_string())
                    .bind(created_at_micros)
                    .bind(record.idempotency_key.as_str())
                    .fetch_all(),
            )
            .await?
        };
        Ok(rows.into_iter().fold(None, |current, candidate| {
            Some(prefer_dedup_row(current, candidate, record.id))
        }))
    }

    /// Insert a single row into `usage_records`.
    ///
    /// Tracks pool-acquire duration (time to get the `Insert` handle) via the
    /// metric inventory, and reports the single-row create latency measured
    /// from `op_start` — the caller's entry instant, so the catalog check and
    /// the dedup read are inside the observed window.
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
        // Insert-time timeouts and settings both come from `configure_insert`,
        // so this site cannot drift from the batch and catalog ones.
        //
        // The direct path, taken only with group commit disabled. The dedup
        // token makes a racing retry of this very row a no-op at the engine —
        // on a synchronous insert. With `async_insert = 1` on the shipped
        // non-replicated table the token is carried but not enforced (async
        // dedup is a `Replicated*` feature); twins are then resolved at read
        // time like any other duplicate. See
        // `config::ClickHousePluginConfig::async_insert`.
        let token = insert_dedup_token(std::slice::from_ref(row), InsertKind::Record);
        let mut insert: clickhouse::insert::Insert<UsageRecordRow> = configure_insert(
            {
                with_deadline(
                    &self.metrics,
                    self.request_timeout,
                    self.client.insert("usage_records"),
                )
                .await?
            },
            self.request_timeout,
            self.async_insert,
            Some(&token),
        );
        self.metrics
            .record_pool_acquire(pool_start.elapsed().as_secs_f64());

        {
            insert
                .write(row)
                .await
                .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        }
        {
            insert
                .end()
                .await
                .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        }
        self.metrics
            .record_insert(InsertMode::Single, op_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Insert multiple rows into `usage_records` in a single INSERT statement.
    ///
    /// Always synchronous, regardless of the store's `async_insert` setting:
    /// both callers depend on the statement's rows becoming visible together,
    /// which the server-side async-insert buffer does not guarantee. The body
    /// carries the full argument.
    ///
    /// Atomicity is **per partition**, not per statement. `usage_records` is
    /// `PARTITION BY toYYYYMM(created_at)` and a `ClickHouse` INSERT commits one
    /// part per partition it touches, so rows landing in one month are visible
    /// all-or-nothing while a statement spanning two months is two commits — a
    /// version-resolving reader can observe one part without the other. The one
    /// caller that depends on the all-or-nothing reading, the deactivation
    /// cascade, states the resulting window at its own call site.
    ///
    /// A statement whose block spans more than `max_partitions_per_insert_block`
    /// partitions (server default 100, i.e. 100 distinct months) is rejected by
    /// `ClickHouse` outright; that surfaces as `Transient`/`Internal` like any
    /// other insert failure.
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
        kind: InsertKind,
    ) -> Result<(), UsageCollectorPluginError> {
        if rows.is_empty() {
            return Ok(());
        }
        let pool_start = Instant::now();
        // Synchronous, so the engine enforces this token: an identical racing
        // batch, or a repeated cascade for the same ids, is dropped as a block.
        // `kind` keeps a marker set from being mistaken for the create that
        // wrote the same ids (see `InsertKind`).
        let token = insert_dedup_token(rows, kind);
        // Deliberately **synchronous**, whatever `self.async_insert` says — the
        // one place this store departs from the configured setting, and the
        // reason is the per-statement atomicity both callers of this method
        // rely on.
        //
        // `async_insert = 1` hands the rows to a server-side buffer that
        // coalesces across statements and flushes on its own schedule, and one
        // statement's rows are not guaranteed to land in a single commit. That
        // is harmless for independent rows but not here:
        //
        // * the deactivation cascade writes one marker row per affected id and
        //   requires them to flip together (see the ATOMICITY NOTE on
        //   `deactivate`); async inserts made a concurrent resolved read
        //   observe a flipped compensation under a still-active target, which
        //   `ch_deactivation_cascade_is_atomic` catches; and
        // * `create_batch` is documented as a whole-batch write (DESIGN.md
        //   §3.6 Batch Ingest).
        //
        // Nothing is given up by staying synchronous: with group commit on,
        // this method only writes the deactivation cascade's markers; with it
        // off, a batch statement already carries its call's rows in one part.
        // `insert_record` is the one site the server-side buffer can help, and
        // it has no cross-row invariant to lose.
        let mut insert: clickhouse::insert::Insert<UsageRecordRow> = configure_insert(
            {
                with_deadline(
                    &self.metrics,
                    self.request_timeout,
                    self.client.insert("usage_records"),
                )
                .await?
            },
            self.request_timeout,
            false,
            Some(&token),
        );
        self.metrics
            .record_pool_acquire(pool_start.elapsed().as_secs_f64());
        self.metrics.record_batch_rows(as_f64(rows.len()));

        {
            for row in rows {
                insert
                    .write(row)
                    .await
                    .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
            }
        }
        {
            insert
                .end()
                .await
                .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        }
        self.metrics
            .record_insert(InsertMode::Batch, op_start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Batch dedup pre-check: SELECT all rows whose `(tenant_id, gts_id,
    /// created_at, idempotency_key)` canonical dedup tuple appears in the input
    /// list — written in sort-key order (`gts_id` first) so the tuple's first
    /// three components are the table's key prefix — the batch analogue of [`Self::dedup_point_lookup`], including its
    /// argument for filtering on `idempotency_key` below the resolution step.
    ///
    /// Returns a map from the 4-tuple key to the stored row. Two stored rows can
    /// share a key only through a legacy mismatched-`id` twin; the collision is
    /// resolved by [`prefer_dedup_row`] against the incoming record's `id`.
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
        // Build `(g, t, c, k) IN ((?, ?, fromUnixTimestamp64Micro(?), ?), ...)`
        // — tuple element order follows `ORDER BY (gts_id, tenant_id,
        // created_at, id)`, so the leading three elements pin the sort-key
        // prefix; `idempotency_key` is not in the key and is a residual filter.
        // A bare epoch-microsecond integer in a tuple comparison is coerced
        // through Decimal arithmetic by ClickHouse and can raise
        // DECIMAL_OVERFLOW before the query starts.
        let mut ctx = SqlCtx::new();
        let mut tuples = Vec::with_capacity(records.len());
        // The `id` each incoming record expects to find, so a collision between
        // two stored rows sharing a dedup key is resolved toward the caller's
        // own record rather than by part-read order.
        let mut expected_ids: HashMap<DedupKey, Uuid> = HashMap::with_capacity(records.len());
        for r in records {
            ctx.push(SqlBind::Str(r.gts_id.as_ref().to_owned()));
            ctx.push(SqlBind::Uuid(r.tenant_id));
            ctx.push(SqlBind::DateTime64Micros(EpochMicros::from(r.created_at).0));
            ctx.push(SqlBind::Str(r.idempotency_key.as_str().to_owned()));
            tuples.push("(?, ?, fromUnixTimestamp64Micro(?), ?)");
            expected_ids.insert(record_dedup_key(r), r.id);
        }
        let in_clause = tuples.join(", ");
        // Every column in the `IN` tuple is version-invariant, so the filter
        // may sit below the resolution step and still prune on the sort-key
        // prefix. Unlike the single-record path this cannot pin `created_at`
        // to one value, so the resolution keys on the whole sort key.
        let resolve_latest = latest_by(RECORD_DEDUP_KEY);
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM usage_records \
             WHERE (gts_id, tenant_id, created_at, idempotency_key) IN ({in_clause})\
             {resolve_latest}"
        );
        let mut q = self.client.query(&sql);
        for b in &ctx.binds {
            q = bind_one(q, b);
        }
        let rows: Vec<UsageRecordRow> =
            { with_deadline(&self.metrics, self.request_timeout, q.fetch_all()).await? };
        let mut out: HashMap<DedupKey, UsageRecordRow> = HashMap::with_capacity(rows.len());
        for row in rows {
            let key = row_dedup_key(&row);
            // A row the batch did not ask for cannot come back from the `IN`
            // filter; `Uuid::nil` is an unreachable fallback that simply makes
            // the lowest-`id` rule the tie-break.
            let expected_id = expected_ids.get(&key).copied().unwrap_or_else(Uuid::nil);
            let chosen = prefer_dedup_row(out.remove(&key), row, expected_id);
            out.insert(key, chosen);
        }
        Ok(out)
    }

    /// Append metadata side-channel filters as parameterised `WHERE` clauses.
    ///
    /// `metadata['?'] IN (?, ?)` — both key and each value are bound via `ctx`.
    fn push_metadata_filters(
        metadata_filter: &[MetadataFilter],
        ctx: &mut SqlCtx,
        clauses: &mut Vec<String>,
    ) {
        for mf in metadata_filter {
            if mf.values().is_empty() {
                clauses.push("FALSE".to_owned());
                continue;
            }
            ctx.push(SqlBind::Str(mf.key().as_str().to_owned()));
            let placeholders = mf
                .values()
                .iter()
                .map(|v| {
                    ctx.push(SqlBind::Str(v.clone()));
                    "?"
                })
                .collect::<Vec<_>>();
            clauses.push(format!("metadata[?] IN ({})", placeholders.join(", ")));
        }
    }

    /// Translate a caller `$filter` into conjuncts of the one `WHERE`, binding
    /// in text order.
    ///
    /// The read paths carry a single `WHERE` over a `FINAL` read, so every
    /// conjunct — key-prefix predicates, metadata, `status`, the keyset tuple —
    /// lands in the same clause list in the order it is pushed, and `ctx`
    /// accumulates the binds in exactly that order ([`translate_record_conjuncts`]
    /// flattens the top-level `AND` spine so the key-prefix conjuncts stay
    /// visible to the primary-key analysis).
    fn push_filter(
        node: &FilterNode<UsageRecordFilterField>,
        ctx: &mut SqlCtx,
        clauses: &mut Vec<String>,
    ) -> Result<(), UsageCollectorPluginError> {
        let conjuncts =
            translate_record_conjuncts(node, ctx).map_err(UsageCollectorPluginError::internal)?;
        clauses.extend(conjuncts);
        Ok(())
    }

    /// Assemble the aggregate query text.
    ///
    /// Split out of [`RecordStore::aggregate`] so the text is unit-testable
    /// without a live `ClickHouse`.
    ///
    /// ```text
    /// SELECT <aliased_select> FROM usage_records FINAL
    /// WHERE <clauses joined by AND>
    /// [GROUP BY 1, …] [LIMIT …]
    /// ```
    ///
    /// One level, one `WHERE`. `FINAL` collapses every sort key to its highest
    /// `version` before the predicates apply, so `status = 'active'` (always
    /// among `clauses`) drops deactivated rows — their surviving version is the
    /// marker — and a duplicate create is one row. The only `GROUP BY` is the
    /// caller's dimension grouping. The `WHERE` is never empty: `gts_id = ?` is
    /// always the first clause.
    fn build_aggregate_sql(
        aliased_select: &str,
        clauses: &[String],
        group_by: &str,
        limit_clause: &str,
    ) -> String {
        format!(
            "SELECT {aliased_select} FROM usage_records FINAL WHERE {}{group_by}{limit_clause}",
            clauses.join(" AND ")
        )
    }

    /// Assemble the keyset-list query text.
    ///
    /// Split out of [`RecordStore::list`] for the same reason as
    /// [`Self::build_aggregate_sql`].
    ///
    /// ```text
    /// SELECT <RECORD_COLUMNS> FROM usage_records FINAL
    /// WHERE <clauses joined by AND>
    /// ORDER BY <order_sql> LIMIT <limit>
    /// ```
    ///
    /// `list` returns inactive rows too, so there is no `status` clause of its
    /// own: `FINAL` makes each logical row visible once, at its resolved
    /// status, and the caller's `status` filter, the keyset tuple and the
    /// `ORDER BY` all read that resolved row. `limit` is the caller's
    /// look-ahead `LIMIT` (page size plus one) and applies after every
    /// predicate, so a filtered page is never short.
    fn build_list_sql(clauses: &[String], order_sql: &str, limit: u64) -> String {
        format!(
            "SELECT {RECORD_COLUMNS} FROM usage_records FINAL WHERE {} ORDER BY {order_sql} LIMIT {limit}",
            clauses.join(" AND ")
        )
    }

    /// Submit a call's composed rows to the group-commit queue and await the
    /// flush that carries them, within what is left of the request budget.
    ///
    /// # Errors
    ///
    /// Admission errors (`Transient` on a bound, `Internal` for an oversize
    /// call) and the wait's own `Transient` (budget exhausted, queue stopped);
    /// each applies to every row of the call.
    async fn write_through_queue(
        queue: &IngestQueue,
        rows: Vec<UsageRecordRow>,
        budget: Duration,
    ) -> Result<CallOutcome, UsageCollectorPluginError> {
        queue.submit(PendingCall { rows })?.wait(budget).await
    }
}

/// The group-commit queue's write side over the store's `ClickHouse` client:
/// one synchronous `INSERT` per flush, classified for the batcher's policy.
pub(crate) struct ChFlushSink {
    client: clickhouse::Client,
    metrics: Arc<Metrics>,
    request_timeout: Duration,
}

#[async_trait]
impl FlushSink for ChFlushSink {
    /// One `INSERT` of exactly `rows` under `token`, always synchronous
    /// (`async_insert = 0`): the flush *is* the batching layer, and the
    /// durability ack and the engine-side token check both depend on the
    /// statement committing before it returns. Classification happens here,
    /// where the `ClickHouse` error is still at hand (`error::classify`).
    async fn insert(&self, rows: &[Arc<UsageRecordRow>], token: &str) -> Result<(), FlushError> {
        let started = Instant::now();
        let acquired = with_deadline(
            &self.metrics,
            self.request_timeout,
            self.client.insert("usage_records"),
        )
        .await
        .map_err(|error| FlushError {
            kind: kind_of(&error),
            error,
        })?;
        self.metrics
            .record_pool_acquire(started.elapsed().as_secs_f64());
        let mut insert: clickhouse::insert::Insert<UsageRecordRow> =
            configure_insert(acquired, self.request_timeout, false, Some(token));
        let classified = |e: &clickhouse::error::Error| FlushError {
            kind: classify(e),
            error: tracked_ch_err(&self.metrics, e),
        };
        for row in rows {
            insert
                .write(row.as_ref())
                .await
                .map_err(|e| classified(&e))?;
        }
        insert.end().await.map_err(|e| classified(&e))?;
        self.metrics
            .record_insert(InsertMode::Flush, started.elapsed().as_secs_f64());
        Ok(())
    }
}

/// The 4-tuple dedup identity for `usage_records`: `(tenant_id, gts_id,
/// created_at_micros, idempotency_key)` — the SPI's canonical dedup tuple
/// (plugin-spi.md §"Plugin-specific outputs"; ADR-0014).
///
/// The record `id` is deliberately **not** part of this key. `id` is a
/// deterministic `UUIDv5` projection *of* this tuple, so for well-formed data
/// keying on either is equivalent — but a stored row whose `id` disagrees with
/// its own canonical tuple would be missed by an `id`-keyed lookup and inserted
/// as new, silently binding an idempotency key that is already taken. Keying on
/// the tuple instead lets [`canonical_equal`] compare stored vs incoming `id`
/// and surface the disagreement as `IdempotencyConflict`.
///
/// `created_at` is stored as `i64` epoch-microseconds, so the dedup key is
/// already µs-normalised; no truncation is needed.
///
/// NOTE — the field order differs from the `TimescaleDB` sibling plugin's
/// `(tenant_id, gts_id, idempotency_key, created_at)`. That is deliberate:
/// `ClickHouse` can only prune an `IN`-tuple against the primary key when the
/// tuple's *leading* elements match the `ORDER BY` prefix, so `created_at` (a
/// key column) must stay ahead of `idempotency_key` (which is not in the sort
/// key). Postgres enforces its own order through a real `UNIQUE` index and has
/// no such constraint.
pub(crate) type DedupKey = (Uuid, String, i64, String);

/// Build the catalog existence query for a batch's distinct `gts_id`s:
/// `SELECT gts_id FROM usage_type_catalog WHERE gts_id IN (?, ?, …)`.
///
/// One `SqlBind::Str` per id, in `BTreeSet` order, so the placeholder and bind
/// sequences are deterministic and every caller-supplied identifier reaches the
/// server as a bound parameter rather than SQL text.
///
/// Like [`ChRecordStore::check_catalog_existence`] this needs no version
/// resolution — it asks only which ids are present, and unmerged duplicate
/// copies of one type are collapsed by the caller's `HashSet`.
fn catalog_lookup_sql(gts_ids: &BTreeSet<&str>) -> (String, SqlCtx) {
    let mut ctx = SqlCtx::new();
    let placeholders = gts_ids
        .iter()
        .map(|gts_id| {
            ctx.push(SqlBind::Str((*gts_id).to_owned()));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    (
        format!("SELECT gts_id FROM usage_type_catalog WHERE gts_id IN ({placeholders})"),
        ctx,
    )
}

/// Decide locally whether `record.value` can be stored in the
/// `Decimal128(9)` column exactly.
///
/// The column holds nine fractional digits; `rust_decimal::Decimal` carries
/// up to twenty-eight. A value with non-zero digits beyond the ninth place
/// would be rounded by the `RowBinary` encoder, persisting a usage amount
/// other than the one supplied, so it is refused before any statement is
/// issued (`ch_decimal128_9::scaled_mantissa`). Trailing zeros beyond scale 9
/// are exactly representable and pass. The plugin error contract has no
/// validation variant, so the rejection is reported as `Internal` carrying the
/// reason; it is not a backend error and does not touch the backend metrics.
fn unrepresentable_value(record: &UsageRecord) -> Option<UsageCollectorPluginError> {
    ch_decimal128_9::scaled_mantissa(&record.value)
        .err()
        .map(|msg| {
            tracing::warn!(
                record_id = %record.id,
                gts_id = %record.gts_id.as_ref(),
                value_scale = record.value.scale(),
                "usage record value cannot be stored exactly in Decimal128(9); rejecting without insert"
            );
            UsageCollectorPluginError::internal(format!("usage record {}: {msg}", record.id))
        })
}

/// Apply the batch value check ([`unrepresentable_value`]): every record
/// whose `value` cannot be stored exactly gets its rejection in its own slot;
/// the input indices of the rest are returned in input order for the catalog
/// step. Performs no I/O.
fn split_by_representable(
    records: &[UsageRecord],
    outcomes: &mut [Option<Result<UsageRecord, UsageCollectorPluginError>>],
) -> Vec<usize> {
    let mut passed = Vec::with_capacity(records.len());
    for (idx, record) in records.iter().enumerate() {
        match unrepresentable_value(record) {
            Some(err) => outcomes[idx] = Some(Err(err)),
            None => passed.push(idx),
        }
    }
    passed
}

/// Apply the batch catalog check: every undecided record whose `gts_id` is not
/// in `known` gets `UsageTypeNotFound` in its own slot; the input indices of
/// the rest are returned in input order for the dedup step. A slot an earlier
/// pass already decided (the value check) is left alone and is not returned.
fn split_by_catalog(
    records: &[UsageRecord],
    known: &HashSet<String>,
    outcomes: &mut [Option<Result<UsageRecord, UsageCollectorPluginError>>],
) -> Vec<usize> {
    let mut passed = Vec::with_capacity(records.len());
    for (idx, record) in records.iter().enumerate() {
        if outcomes[idx].is_some() {
            continue;
        }
        if known.contains(record.gts_id.as_ref()) {
            passed.push(idx);
        } else {
            outcomes[idx] = Some(Err(UsageCollectorPluginError::UsageTypeNotFound {
                gts_id: record.gts_id.clone(),
            }));
        }
    }
    passed
}

/// Turn the per-slot outcome table into the SPI's positional result vector.
///
/// Every slot is filled by the time this runs; an empty one is an invariant
/// break and is reported as such rather than dropped or reordered.
fn finalize_outcomes(
    outcomes: Vec<Option<Result<UsageRecord, UsageCollectorPluginError>>>,
    records: &[UsageRecord],
) -> Vec<Result<UsageRecord, UsageCollectorPluginError>> {
    outcomes
        .into_iter()
        .enumerate()
        .map(|(idx, outcome)| {
            outcome.unwrap_or_else(|| {
                Err(dedup_invariant_break(
                    &records[idx],
                    "batch index unresolved after batch processing (invariant break)",
                ))
            })
        })
        .collect()
}

fn record_dedup_key(r: &UsageRecord) -> DedupKey {
    (
        r.tenant_id,
        r.gts_id.as_ref().to_owned(),
        EpochMicros::from(r.created_at).0,
        r.idempotency_key.as_str().to_owned(),
    )
}

pub(crate) fn row_dedup_key(r: &UsageRecordRow) -> DedupKey {
    (
        r.tenant_id,
        r.gts_id.clone(),
        r.created_at,
        r.idempotency_key.clone(),
    )
}

/// Pick between two stored rows that share a [`DedupKey`], preferring the one
/// whose `id` matches `expected_id`.
///
/// `ClickHouse` has no `UNIQUE` constraint, so rows written before the dedup
/// lookup was keyed on the canonical tuple can leave two rows sharing a tuple
/// with different `id`s. Both survive version resolution — distinct `id`s are
/// distinct sort keys, so neither `ReplacingMergeTree` nor a `LIMIT 1 BY` over
/// the sort key ever collapses them together.
///
/// The exact-`id` match wins so an honest retry is still absorbed when a
/// corrupt twin exists; otherwise the lowest `id` wins, which makes the choice
/// deterministic rather than dependent on which part `ClickHouse` reads first.
fn prefer_dedup_row(
    current: Option<UsageRecordRow>,
    candidate: UsageRecordRow,
    expected_id: Uuid,
) -> UsageRecordRow {
    let Some(current) = current else {
        return candidate;
    };
    if current.id == expected_id {
        return current;
    }
    if candidate.id == expected_id || candidate.id < current.id {
        return candidate;
    }
    current
}

/// Build an `Internal` error noting a dedup invariant break and log it at
/// `error` level so it is observable without exposing identifiers to callers.
fn dedup_invariant_break(record: &UsageRecord, msg: &'static str) -> UsageCollectorPluginError {
    tracing::error!(
        tenant_id = %record.tenant_id,
        gts_id = %record.gts_id.as_ref(),
        idempotency_key = %record.idempotency_key.as_str(),
        "{msg}"
    );
    UsageCollectorPluginError::internal(msg)
}

/// Build a fresh, independently-owned copy of an error for embedding into
/// every outcome slot it applies to.
///
/// [`UsageCollectorPluginError`] is intentionally not `Clone` (it is a
/// foundation-owned SPI contract type — `cpt-cf-usage-collector-dod-*
/// -plugin-contract-stability`), so [`ChRecordStore::create_batch`] and the
/// ingest queue reconstruct an equivalent value per variant instead of
/// cloning a shared instance. The read producers — [`ChRecordStore::existing_usage_types`],
/// [`ChRecordStore::batch_dedup_lookup`] and [`ChRecordStore::insert_records`]
/// — only yield `Transient` / `Internal`; the queue adds `IdempotencyConflict`
/// (a row refused against a differing pending row). The `UsageTypeNotFound`
/// arm is kept so a caller-visible variant is never downgraded should a
/// producer change, and the fallback arm exists only because the enum is
/// `#[non_exhaustive]`.
pub(crate) fn err_for_slot(err: &UsageCollectorPluginError) -> UsageCollectorPluginError {
    match err {
        UsageCollectorPluginError::Transient {
            detail,
            retry_after_seconds,
        } => UsageCollectorPluginError::Transient {
            detail: detail.clone(),
            retry_after_seconds: *retry_after_seconds,
        },
        UsageCollectorPluginError::IdempotencyConflict {
            idempotency_key,
            existing_id,
        } => UsageCollectorPluginError::IdempotencyConflict {
            idempotency_key: idempotency_key.clone(),
            existing_id: *existing_id,
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

/// Fan the group-commit queue's per-row outcomes out across the slots each
/// composed row backs. A row's `Ok` leaves its slots as the composition
/// decided them (the composed record); a row's error — the flush failure, or
/// an `IdempotencyConflict` against a differing pending row — is written to
/// every slot behind it.
fn apply_row_outcomes(
    outcome: CallOutcome,
    row_slots: &[Vec<usize>],
    outcomes: &mut [Option<Result<UsageRecord, UsageCollectorPluginError>>],
) {
    for (row_idx, result) in outcome.rows.into_iter().enumerate() {
        let Err(err) = result else {
            continue;
        };
        for &idx in row_slots.get(row_idx).map_or(&[][..], Vec::as_slice) {
            if let Some(slot) = outcomes.get_mut(idx) {
                *slot = Some(Err(err_for_slot(&err)));
            }
        }
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "a batch is bounded by the gear's batch cap, far below f64's mantissa"
)]
fn as_f64(n: usize) -> f64 {
    n as f64
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
            outcomes[idx] = Some(Err(err_for_slot(err)));
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
    ///
    /// The canonical fields include the record `id`. Because the lookup keys on
    /// the canonical tuple and not on `id` ([`DedupKey`]), that comparison is a
    /// load-bearing fail-closed guard rather than a tautology: a stored row
    /// whose `id` disagrees with its own dedup tuple is reported as a conflict
    /// instead of being missed and re-inserted under a key already in use.
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
        UsageRecord::try_from(row.clone())
    }

    /// Compose the rows a batch has to write, deciding every passed record's
    /// outcome in input order.
    ///
    /// `passed` holds the input indices that survived the value and catalog
    /// checks, so every record composed here is known to be exactly
    /// representable in the `Decimal128(9)` column; `existing` is the dedup
    /// pre-read over exactly those records. A stored
    /// row for a record's dedup key decides its outcome via
    /// [`Self::resolve_dedup_hit`]. A row already composed in this batch for
    /// the same key is treated identically: since [`DedupKey`] is the
    /// canonical tuple rather than the `id`, two in-batch records sharing a
    /// tuple but carrying different payloads collide here and the second is a
    /// conflict instead of both being written under one idempotency key.
    /// Anything else becomes a new row versioned `base + <rows composed so
    /// far>`, which keeps the batch's versions distinct and increasing.
    ///
    /// Returns the rows to insert and, parallel to them, the input indices
    /// whose success depends on each row landing. Every `passed` slot in
    /// `outcomes` is filled on return. Performs no I/O.
    fn compose_batch(
        &self,
        records: &[UsageRecord],
        passed: &[usize],
        existing: &HashMap<DedupKey, UsageRecordRow>,
        base: u64,
        outcomes: &mut [Option<Result<UsageRecord, UsageCollectorPluginError>>],
    ) -> (Vec<UsageRecordRow>, Vec<Vec<usize>>) {
        let mut to_insert: Vec<UsageRecordRow> = Vec::new();
        // Row position in `to_insert` rather than a clone of the row itself.
        let mut insert_map: HashMap<DedupKey, usize> = HashMap::new();
        // Parallel to `to_insert`: the input positions whose success depends
        // on that row landing.
        let mut row_slots: Vec<Vec<usize>> = Vec::new();

        for &idx in passed {
            let record = &records[idx];
            let key = record_dedup_key(record);
            let outcome = if let Some(stored_row) = existing.get(&key) {
                self.resolve_dedup_hit(stored_row, record)
            } else if let Some(&row_idx) = insert_map.get(&key) {
                let resolved = self.resolve_dedup_hit(&to_insert[row_idx], record);
                if resolved.is_ok() {
                    row_slots[row_idx].push(idx);
                }
                resolved
            } else {
                let offset = u64::try_from(to_insert.len()).unwrap_or(u64::MAX);
                let row = UsageRecordRow::from((record, base.saturating_add(offset)));
                let row_idx = to_insert.len();
                to_insert.push(row);
                insert_map.insert(key, row_idx);
                row_slots.push(vec![idx]);
                if record.corrects_id.is_some() {
                    self.metrics.inc_compensation();
                }
                UsageRecord::try_from(to_insert[row_idx].clone())
            };
            outcomes[idx] = Some(outcome);
        }

        (to_insert, row_slots)
    }
}

#[async_trait]
impl RecordStore for ChRecordStore {
    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-ingest-dedup
    /// Create one record: local value check, catalog existence check, dedup
    /// pre-read, then the write — through the group-commit queue when it is
    /// enabled, as its own `INSERT` otherwise.
    ///
    /// The value check ([`unrepresentable_value`]) runs before any statement:
    /// a `value` the `Decimal128(9)` column cannot hold exactly is rejected
    /// rather than rounded, and never reaches the backend.
    ///
    /// The statements are independent — nothing serialises two creates for
    /// the same dedup key, so both can pass the pre-read. In this instance the
    /// queue then coalesces the second onto the first (identical payload) or
    /// refuses it as `IdempotencyConflict` (differing payload); across
    /// instances both rows are written and every read resolves them to the
    /// higher `version`, so a differing payload converges
    /// highest-version-wins rather than being reported. Once the earlier row
    /// is visible the pre-read sees it and the usual absorb / conflict rules
    /// apply.
    ///
    /// The whole call — pre-checks, queue wait and flush — must fit the
    /// client-side deadline: what the pre-checks used is subtracted from the
    /// budget the queue wait gets.
    #[instrument(skip(self, record), fields(gts_id = %record.gts_id.as_ref()))]
    async fn create(&self, record: UsageRecord) -> Result<UsageRecord, UsageCollectorPluginError> {
        let op_start = Instant::now();
        let entered = tokio::time::Instant::now();
        if let Some(err) = unrepresentable_value(&record) {
            return Err(err);
        }
        self.check_catalog_existence(&record.gts_id).await?;

        if let Some(row) = self.dedup_point_lookup(&record).await? {
            return self.resolve_dedup_hit(&row, &record);
        }

        if record.corrects_id.is_some() {
            self.metrics.inc_compensation();
        }

        let row = UsageRecordRow::from((&record, current_merge_version()));
        match &self.ingest {
            Some(queue) => {
                let budget = remaining_budget(entered, self.request_timeout);
                let outcome = Self::write_through_queue(queue, vec![row.clone()], budget).await?;
                outcome.rows.into_iter().next().unwrap_or_else(|| {
                    Err(UsageCollectorPluginError::internal(
                        "ingest outcome carried no row (invariant break)",
                    ))
                })?;
                self.metrics
                    .record_insert(InsertMode::Single, op_start.elapsed().as_secs_f64());
            }
            None => self.insert_record(&row, op_start).await?,
        }
        UsageRecord::try_from(row)
    }

    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-ingest-batch
    /// Create a batch with two read statements regardless of how many usage
    /// types it spans — one catalog existence query over the distinct
    /// `gts_id`s, one dedup pre-read over every record that passed it — and
    /// one write of the composed rows: a submission to the group-commit queue
    /// (the rows travel together in one flush `INSERT`, possibly alongside
    /// other calls' rows), or a multi-row `INSERT` of their own when group
    /// commit is off.
    ///
    /// Before the first statement, a local value check
    /// ([`split_by_representable`]) decides every record whose `value` the
    /// `Decimal128(9)` column cannot hold exactly; those slots are rejected
    /// rather than rounded and take no part in the statements that follow.
    ///
    /// Outcomes are positional. A failed catalog or dedup read is a failure
    /// for every slot that depended on it (every slot that passed the value
    /// check for the catalog read); a failed write is reported only in the
    /// slots backed by a composed row, so rows absorbed from storage keep the
    /// outcome the pre-read decided, and with group commit each composed row
    /// carries its own outcome (a row coalesced onto a pending twin succeeds
    /// or fails with that twin; one refused against a differing pending row is
    /// an `IdempotencyConflict`). The write commits one part per
    /// `toYYYYMM(created_at)` partition it touches (and per shard on a
    /// cluster), so a reader sees all of the batch's rows for a given month or
    /// none of them; a batch spanning several months can be observed part-way
    /// through (see [`Self::insert_records`]).
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
        let entered = tokio::time::Instant::now();
        let mut outcomes: Vec<Option<Result<UsageRecord, UsageCollectorPluginError>>> =
            (0..records.len()).map(|_| None).collect();

        // 0. Local value check, no I/O. A record the column cannot hold
        //    exactly is decided here and shapes none of the statements below.
        let representable = split_by_representable(&records, &mut outcomes);

        // 1. One catalog existence query over the distinct usage types of the
        //    records still undecided. A failed read decides nothing for any of
        //    them, so every such slot carries it; value rejections stay put.
        let distinct: BTreeSet<&str> = representable
            .iter()
            .map(|&i| records[i].gts_id.as_ref())
            .collect();
        let known = match self.existing_usage_types(&distinct).await {
            Ok(known) => known,
            Err(e) => {
                for &idx in &representable {
                    outcomes[idx] = Some(Err(err_for_slot(&e)));
                }
                return Ok(finalize_outcomes(outcomes, &records));
            }
        };
        let passed = split_by_catalog(&records, &known, &mut outcomes);

        // 2. One dedup pre-read over everything that passed the catalog check.
        let record_refs: Vec<&UsageRecord> = passed.iter().map(|&i| &records[i]).collect();
        let existing = match self.batch_dedup_lookup(&record_refs).await {
            Ok(existing) => existing,
            Err(e) => {
                for &idx in &passed {
                    outcomes[idx] = Some(Err(err_for_slot(&e)));
                }
                return Ok(finalize_outcomes(outcomes, &records));
            }
        };

        // 3. Resolve every passed record and compose the rows to write.
        let base = current_merge_version();
        let (to_insert, row_slots) =
            self.compose_batch(&records, &passed, &existing, base, &mut outcomes);

        // 4. One write of the composed rows (a no-op when nothing was
        //    composed). A failed write does not invalidate the outcomes
        //    already decided for absorbed rows, so it is reported per backed
        //    slot rather than as a batch-level error that would discard them.
        match &self.ingest {
            Some(queue) if !to_insert.is_empty() => {
                self.metrics.record_batch_rows(as_f64(to_insert.len()));
                let budget = remaining_budget(entered, self.request_timeout);
                match Self::write_through_queue(queue, to_insert, budget).await {
                    Ok(outcome) => {
                        apply_row_outcomes(outcome, &row_slots, &mut outcomes);
                        self.metrics
                            .record_insert(InsertMode::Batch, op_start.elapsed().as_secs_f64());
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "create-batch write failed; reporting per-record outcomes");
                        apply_insert_failure(&e, &row_slots, &mut outcomes);
                    }
                }
            }
            Some(_) => {}
            None => {
                if let Err(e) = self
                    .insert_records(&to_insert, op_start, InsertKind::Record)
                    .await
                {
                    tracing::warn!(error = %e, "create-batch insert failed; reporting per-record outcomes");
                    apply_insert_failure(&e, &row_slots, &mut outcomes);
                }
            }
        }

        Ok(finalize_outcomes(outcomes, &records))
    }

    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-get
    #[instrument(skip_all, fields(record_id = %id))]
    async fn get(&self, id: Uuid) -> Result<UsageRecord, UsageCollectorPluginError> {
        // `LIMIT 1 BY` the whole sort key rather than a bare `LIMIT 1`: `id` is
        // normally the `UUIDv5` of the other three key columns, but a legacy
        // mismatched-`id` twin (the case `prefer_dedup_row` exists for) lets
        // one `id` span two sort keys. A global highest-`version` pick could
        // then return a row that is not its own key's resolved version.
        // Resolving per key and taking the first row keeps this a bloom-filter
        // point read (`idx_records_id` prunes the non-sort-key-prefix `id`
        // predicate) over a candidate set of a few rows — cheaper than a
        // `FINAL` merge for a single key.
        let resolve_latest = latest_by(RECORD_DEDUP_KEY);
        let sql =
            format!("SELECT {RECORD_COLUMNS} FROM usage_records WHERE id = ?{resolve_latest}");
        let row: Option<UsageRecordRow> = with_deadline(
            &self.metrics,
            self.request_timeout,
            self.client
                .query(&sql)
                .bind(id.to_string())
                .fetch_optional(),
        )
        .await?;

        match row {
            Some(row) => UsageRecord::try_from(row),
            None => Err(UsageCollectorPluginError::UsageRecordNotFound { id }),
        }
    }

    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-list-keyset
    #[instrument(skip_all, fields(gts_id = %gts_id.as_ref()))]
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

        // One `WHERE` over a `FINAL` read (see `Self::build_list_sql`): every
        // clause is pushed into `clauses` in text order and its binds into
        // `ctx` in the same order, so applying `ctx.binds` after the leading
        // `gts_id` bind matches the `?`s exactly.
        //
        // The key-prefix predicates — `tenant_id` and `created_at`, which
        // reach `list` only through `$filter` — are conjuncts of that one
        // `WHERE`, where the primary-key analysis prunes the read on them; the
        // `status`-naming conjuncts and the keyset tuple sit beside them and
        // read the resolved row `FINAL` produced.
        let mut ctx = SqlCtx::new();
        let mut clauses: Vec<String> = vec!["gts_id = ?".to_owned()];

        // Metadata side-channel.
        Self::push_metadata_filters(metadata_filter, &mut ctx, &mut clauses);

        // `$filter` (validated AST → typed node → parameterised conjuncts).
        if let Some(expr) = query.filter() {
            let node = convert_expr_to_filter_node::<UsageRecordFilterField>(expr)
                .map_err(|e| UsageCollectorPluginError::internal(format!("invalid filter: {e}")))?;
            Self::push_filter(&node, &mut ctx, &mut clauses)?;
        }

        // Keyset continuation (forward only). The predicate stays whole — it
        // is a lexicographic tuple over the sort order, not a conjunction.
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
            let predicate = keyset_predicate(
                &order_pairs,
                &cursor.k,
                record_column,
                |name| UsageRecordFilterField::from_name(name).map(|f| f.kind()),
                is_keyset_safe_record_field,
                &mut ctx,
            )
            .map_err(UsageCollectorPluginError::internal)?;
            clauses.push(predicate);
        }

        let order_sql = render_order_by(&query.order, record_column)
            .map_err(UsageCollectorPluginError::internal)?;

        let sql = Self::build_list_sql(&clauses, &order_sql, limit.saturating_add(1));

        // `gts_id` first, then every clause bind in text order.
        let mut q = self.client.query(&sql).bind(gts_id.as_ref());
        for b in &ctx.binds {
            q = bind_one(q, b);
        }
        for (name, value) in FINAL_READ_SETTINGS {
            q = q.with_setting(*name, *value);
        }

        let mut rows: Vec<UsageRecordRow> =
            with_deadline(&self.metrics, self.request_timeout, q.fetch_all()).await?;

        // Look-ahead row present → a next page exists.
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
            .map(UsageRecord::try_from)
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
    #[instrument(skip_all, fields(gts_id = %gts_id.as_ref()))]
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

        // One `WHERE` over a `FINAL` read, as in `list` (see
        // `Self::build_aggregate_sql`): `FINAL` collapses every sort key to its
        // highest version, so `status = 'active'` on the resolved row drops a
        // deactivated record (its surviving version is the marker) and a
        // duplicate create is one row; no further resolution step is needed
        // and the only `GROUP BY` is the caller's dimension grouping.
        //
        // Pushing the whole `$filter` into that `WHERE` matters most here:
        // `tenant_id` and `created_at` reach this method only through
        // `$filter`, and with `gts_id` they are the whole key prefix, so an
        // aggregate over one tenant's time window is a key-range read.
        //
        // Binds are applied in text order: the SELECT-list metadata keys first
        // (see below), then `gts_id`, then every clause bind.
        let mut ctx = SqlCtx::new();
        let mut clauses: Vec<String> =
            vec!["gts_id = ?".to_owned(), "status = 'active'".to_owned()];

        // `corrects_id` partition (plugin-spi.md §Method 3).
        if let Some(clause) = corrects_id_partition_clause(spec.op) {
            clauses.push(clause.to_owned());
        }

        // Metadata side-channel.
        Self::push_metadata_filters(metadata_filter, &mut ctx, &mut clauses);

        // `$filter`.
        if let Some(expr) = query.filter() {
            let node = convert_expr_to_filter_node::<UsageRecordFilterField>(expr)
                .map_err(|e| UsageCollectorPluginError::internal(format!("invalid filter: {e}")))?;
            Self::push_filter(&node, &mut ctx, &mut clauses)?;
        }

        // Dimension SELECT exprs + subject-not-null guards.
        // SELECT-list binds (metadata keys) are collected separately from the
        // WHERE context: they appear first in the SQL text and must be applied
        // before `gts_id` and every WHERE `?`.
        let mut select_dims: Vec<String> = Vec::with_capacity(spec.group_by.len());
        let mut select_binds: Vec<SqlBind> = Vec::new();
        for dim in &spec.group_by {
            match dim {
                AggregationDimension::SubjectId => {
                    clauses.push("subject_id IS NOT NULL".to_owned());
                }
                AggregationDimension::SubjectType => {
                    clauses.push("subject_type IS NOT NULL".to_owned());
                }
                _ => {}
            }
            let (expr, bind) = dimension_select_expr(dim);
            select_dims.push(expr);
            if let Some(b) = bind {
                select_binds.push(b);
            }
        }

        let dim_count = select_dims.len();
        let mut select_parts = select_dims;
        select_parts.push(agg_select_expr(spec.op).to_owned());
        let group_by = if dim_count == 0 {
            String::new()
        } else {
            let ordinals = (1..=dim_count)
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!(" GROUP BY {ordinals}")
        };

        let limit_clause = aggregate_limit_clause(dim_count);

        // `JSONEachRow` lets us decode a result whose column count varies per call
        // without a fixed `Row` struct. Every column is aliased (`d0`…`dN`, `agg`)
        // so the JSON keys stay predictable whatever the dimension exprs are.
        let aliased_select = select_parts
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                if i == dim_count {
                    format!("{expr} AS agg")
                } else {
                    format!("{expr} AS d{i}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let sql = Self::build_aggregate_sql(&aliased_select, &clauses, &group_by, &limit_clause);

        // Bind order matches left-to-right `?` in `sql`: SELECT metadata keys,
        // then `gts_id`, then the clause binds.
        let mut q = self
            .client
            .query(&sql)
            .with_setting("output_format_json_quote_decimals", "1");
        for (name, value) in FINAL_READ_SETTINGS {
            q = q.with_setting(*name, *value);
        }
        for b in &select_binds {
            q = bind_one(q, b);
        }
        q = q.bind(gts_id.as_ref());
        for b in &ctx.binds {
            q = bind_one(q, b);
        }

        // Column names for the JSON key lookup below, matching the aliases above.
        let dim_names: Vec<String> = (0..dim_count).map(|i| format!("d{i}")).collect();

        // Stream-parse `JSONEachRow` as chunks arrive. The server-side `LIMIT`
        // from `aggregate_limit_clause` still caps row count; streaming avoids
        // holding the full encoded body in memory in addition to the buckets.
        let mut parser = AggregateNdjsonParser::new(dim_names);
        let mut cursor = q
            .fetch_bytes("JSONEachRow")
            .map_err(|e| tracked_ch_err(&self.metrics, &e))?;
        // The deadline bounds each chunk read rather than the whole stream: a
        // large aggregate legitimately takes longer than one request budget to
        // drain, but a stall between chunks is the failure this guards.
        while let Some(chunk) =
            with_deadline(&self.metrics, self.request_timeout, cursor.next()).await?
        {
            parser.push_chunk(&chunk)?;
        }

        Ok(AggregationResult {
            buckets: parser.finish()?,
        })
    }

    // @cpt-flow:cpt-cf-uc-ch-plugin-seq-deactivate-cascade
    #[instrument(skip_all, fields(record_id = %id))]
    async fn deactivate(&self, id: Uuid) -> Result<(), UsageCollectorPluginError> {
        let _timer = OpDurationGuard::start(Arc::clone(&self.metrics), TimedOp::Deactivate);

        // No coordination anywhere in this plugin (DESIGN.md §3.6). For the
        // cascade specifically, the host prevents a concurrent compensation from
        // reaching create_usage_record while a deactivation is in flight
        // (plugin-spi.md Method 5 caller-side rule); a plain retry of the target
        // record racing this cascade is the documented resurrection window.

        // Step 1: Read the target + active depth-1 compensations.
        //
        // `status` is the one version-dependent column, so it must be filtered
        // *above* the resolution step. Pushing `status = 'active'` below it
        // would drop a compensation's own inactive marker while keeping the
        // superseded active row, and the resolution would then hand back that
        // stale row — an already-deactivated compensation would be cascaded a
        // second time, and the target's `AlreadyInactive` check below would
        // never fire.
        //
        // The inner query keeps `id` / `corrects_id` (both version-invariant),
        // so `idx_records_id` and `idx_records_corrects_id` still prune it.
        // Each `id` bind therefore appears twice in the text — inner then
        // outer — hence four binds for two placeholders' worth of values.
        let resolve_latest = latest_by(RECORD_DEDUP_KEY);
        let sql = format!(
            "SELECT {RECORD_COLUMNS} FROM \
             (SELECT {RECORD_COLUMNS} FROM usage_records \
             WHERE id = ? OR corrects_id = ?{resolve_latest}) \
             WHERE id = ? OR (corrects_id = ? AND status = 'active')"
        );
        let rows: Vec<UsageRecordRow> = {
            with_deadline(
                &self.metrics,
                self.request_timeout,
                self.client
                    .query(&sql)
                    .bind(id.to_string())
                    .bind(id.to_string())
                    .bind(id.to_string())
                    .bind(id.to_string())
                    .fetch_all(),
            )
            .await?
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
        //
        // ATOMICITY NOTE (DESIGN.md §3.6): a ClickHouse INSERT commits one part
        // per partition it touches, and `usage_records` is PARTITION BY
        // toYYYYMM(created_at). A marker clones its source row's `created_at` —
        // it must, because `created_at` is in the sort key and sharing it is
        // what makes the marker supersede the row — so all markers for one
        // month land in one part and flip together.
        //
        // The cascade is therefore atomic exactly when the target and its
        // depth-1 compensations fall in the same `toYYYYMM(created_at)`
        // partition, which is the common case: a compensation's event time
        // normally sits close after the record it corrects. When it does not —
        // correcting the 31st on the 1st — the markers commit as two parts and
        // a resolved read can see the target flipped while a compensation is
        // still active. The window is the gap between the two commits. It is
        // not retried or compensated for; it joins the races DESIGN.md §3.6
        // enumerates rather than being closed here, and it is the price of the
        // partition pruning and whole-partition TTL drops the partition key
        // buys (`migrator::m0001_init::RECORDS_PARTITION_KEY`).
        self.insert_records(&markers, Instant::now(), InsertKind::Marker)
            .await?;

        Ok(())
    }
}

/// Incremental `JSONEachRow` decoder for aggregate responses.
///
/// Keeps only an incomplete trailing line between chunks so peak memory is
/// dominated by the already-parsed [`AggregationBucket`]s (bounded by the
/// server-side bucket-cap `LIMIT`), not a second full copy of the body.
struct AggregateNdjsonParser {
    leftover: Vec<u8>,
    dim_names: Vec<String>,
    buckets: Vec<AggregationBucket>,
}

impl AggregateNdjsonParser {
    fn new(dim_names: Vec<String>) -> Self {
        Self {
            leftover: Vec::new(),
            dim_names,
            buckets: Vec::new(),
        }
    }

    fn push_chunk(&mut self, chunk: &[u8]) -> Result<(), UsageCollectorPluginError> {
        self.leftover.extend_from_slice(chunk);
        self.consume_complete_lines()
    }

    fn finish(mut self) -> Result<Vec<AggregationBucket>, UsageCollectorPluginError> {
        self.consume_complete_lines()?;
        if !self.leftover.is_empty() {
            let line = std::str::from_utf8(&self.leftover).map_err(|e| {
                UsageCollectorPluginError::internal(format!("aggregate response utf-8 error: {e}"))
            })?;
            if let Some(bucket) = parse_aggregate_line(line, &self.dim_names)? {
                self.buckets.push(bucket);
            }
            self.leftover.clear();
        }
        Ok(self.buckets)
    }

    fn consume_complete_lines(&mut self) -> Result<(), UsageCollectorPluginError> {
        let mut start = 0;
        while let Some(rel) = self.leftover[start..].iter().position(|&b| b == b'\n') {
            let end = start + rel;
            let line = std::str::from_utf8(&self.leftover[start..end]).map_err(|e| {
                UsageCollectorPluginError::internal(format!("aggregate response utf-8 error: {e}"))
            })?;
            if let Some(bucket) = parse_aggregate_line(line, &self.dim_names)? {
                self.buckets.push(bucket);
            }
            start = end + 1;
        }
        if start > 0 {
            self.leftover.drain(..start);
        }
        Ok(())
    }
}

/// Decode a complete `JSONEachRow` aggregate response body into
/// [`AggregationBucket`]s.
///
/// Test helper over [`AggregateNdjsonParser`]; production aggregate path
/// pushes chunks into the parser directly.
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
    let mut parser = AggregateNdjsonParser::new(dim_names.to_vec());
    parser.push_chunk(bytes)?;
    parser.finish()
}

/// Parse one NDJSON line into an optional bucket (`None` for blank lines).
fn parse_aggregate_line(
    line: &str,
    dim_names: &[String],
) -> Result<Option<AggregationBucket>, UsageCollectorPluginError> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
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
    Ok(Some(AggregationBucket { key, value }))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "record_store_tests.rs"]
mod record_store_tests;
