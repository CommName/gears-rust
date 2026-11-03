//! Version resolution for `ReplacingMergeTree` reads.
//!
//! Both plugin tables are `ReplacingMergeTree(version)`, so several physical
//! rows can share one sort key until a background merge collapses them: a
//! deactivation marker beside the row it supersedes, or a duplicate create —
//! the same logical row written by two collector instances, each under its own
//! `insert_deduplication_token`, or by two racing calls of one instance. Every
//! read resolves those rows itself and never relies on a merge having run.
//!
//! - **Point reads** — `get`, the deactivation cascade's read, the create-path
//!   dedup lookups, the catalog — resolve explicitly with [`latest_by`] /
//!   [`LATEST_ONE`]: `ORDER BY version DESC LIMIT 1 BY <key>` over a candidate
//!   set the key or a bloom filter has already pruned to a handful of rows.
//! - **Range reads** — `list` and `aggregate` — read `FROM usage_records FINAL`
//!   with `do_not_merge_across_partitions_select_final = 1` (every version of a
//!   key shares its `created_at`, hence its `toYYYYMM(created_at)` partition, so
//!   partitions collapse independently). `FINAL` merges only the part ranges
//!   whose primary-key ranges overlap and streams the rest, so its cost grows
//!   with the number of unmerged twins and markers, not with the scan. On a
//!   cluster it runs per shard against the local table; `usage_records` is
//!   sharded by `tenant_id`, which every version of a row shares, so the
//!   per-shard collapse is exact.
//!
//! ## The rule the point reads rest on
//!
//! A deactivation marker (`mapper::make_inactive_marker`) clones its source row
//! and changes only `status` (→ `inactive`) and `version` (→ higher); a
//! duplicate create carries the same payload under another `version`. So on a
//! point read a predicate on `status` **must not** sit below the resolution
//! step: it would retain a superseded active row while discarding that row's
//! own marker, and the resolution would hand back the stale row. Every other
//! column is identical across the versions of a key, so a predicate on it
//! selects the same keys above or below the step and may prune the scan. The
//! deactivation cascade applies its `status = 'active'` above the step for
//! exactly this reason.
//!
//! On the `FINAL` range reads the question does not arise: `ClickHouse`
//! evaluates the `WHERE` on the collapsed rows (only conditions on the sorting
//! key alone are moved below the merge, and those are version-invariant by the
//! rule above), so `status` predicates, the keyset tuple and the caller's
//! `$filter` share one `WHERE`.
//!
//! ## What resolution does and does not promise
//!
//! The row that survives is the one with the highest `version` — a per-instance
//! epoch-microsecond stamp. For identical twins that is the only row there is.
//! For *differing* payloads under one dedup key, written by two instances
//! inside each other's pre-read window, the higher version wins: there is no
//! cross-instance first-writer-wins, and both callers were answered `Ok`. The
//! engine's token window (`non_replicated_deduplication_window` /
//! `replicated_deduplication_window`) drops only a retry of an *identical*
//! flush, and is finite.

/// Suffix resolving each sort key to its highest-`version` row: `ORDER BY
/// version DESC LIMIT 1 BY <keys>`.
///
/// `keys` must name the columns identifying one logical row — the table's
/// `ORDER BY` tuple, or the subset of it left unpinned by the query's `WHERE`.
/// Rendered with a leading space so it appends directly to a query body.
///
/// Callers that also need a presentation order put it *outside* this suffix
/// (in an enclosing query), because `ORDER BY version DESC` is what makes
/// `LIMIT 1 BY` pick the resolved row.
#[must_use]
pub fn latest_by(keys: &[&str]) -> String {
    format!(" ORDER BY version DESC LIMIT 1 BY {}", keys.join(", "))
}

/// Suffix resolving a single expected sort key: `ORDER BY version DESC LIMIT 1`.
///
/// Only for reads whose `WHERE` pins the whole sort key (so at most one logical
/// row can match) — `usage_type_catalog` point reads, whose sort key is
/// `(gts_id)` alone. A read that can span sort keys must use [`latest_by`]
/// instead, or it may return a row that is not its own key's resolved version.
pub const LATEST_ONE: &str = " ORDER BY version DESC LIMIT 1";

/// The `usage_records` sort key — the tuple `ReplacingMergeTree` collapses on.
///
/// The schema's `ORDER BY` is this constant (`migrator::m0001_init::RECORDS_ORDER_BY`), so
/// the helpers here and the DDL cannot drift apart. The *set* of columns is
/// the row identity; their order is not (`LIMIT 1 BY` is order-insensitive).
/// The order is chosen for the primary index; see `migrator::m0001_init::RECORDS_ORDER_BY`.
pub const RECORD_DEDUP_KEY: &[&str] = &["gts_id", "tenant_id", "created_at", "id"];

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "dedup_tests.rs"]
mod dedup_tests;
