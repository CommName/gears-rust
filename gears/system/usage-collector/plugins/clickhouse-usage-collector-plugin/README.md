# ClickHouse Usage Collector Plugin

ClickHouse storage-backend plugin that implements the Usage Collector `UsageCollectorPluginV1` SPI. It is the durable system of record for usage records and the usage-type catalog: the Usage Collector gateway gear discovers it via the types registry and dispatches all persistence to it. The plugin owns nothing of the host's domain logic — it is pure persistence over a ClickHouse columnar OLAP database, with no coordination backend: every write is a plain read-then-insert, coalesced with other calls' writes by an in-process group commit before it reaches ClickHouse, and every read resolves `ReplacingMergeTree(version)` versions itself (`FINAL` on the `list`/`aggregate` range reads, `LIMIT 1 BY` on the point reads), so neither a deactivation marker nor a duplicate row written by another instance depends on a background merge to be read correctly.

## Configuration

Config maps to `ClickHousePluginConfig` (`src/config.rs`). Durations are whole seconds (repo convention).

| Key | Default | Description |
| --- | --- | --- |
| `database_url` | _(required)_ | ClickHouse HTTP endpoint URL including credentials, e.g. `https://user:${CH_PASSWORD}@host:8443/db`. Held as a `secrecy::SecretString` (Debug-redacted, no Display/Serialize, zeroized on drop); `${VAR}` placeholders are expanded at startup. Only the `http` and `https` schemes are accepted; a plaintext `http://` URL is additionally rejected unless `allow_insecure_http = true` (see [TLS enforcement](#tls-enforcement) below). Credentials containing URL-reserved characters must be percent-encoded, and the encoded bytes must decode to valid UTF-8; a credential that does not is rejected by config validation at startup rather than silently altered. |
| `allow_insecure_http` | `false` | Explicit development/test opt-out that permits a plaintext `http://` `database_url`. Has no effect on a `https://` URL, and does not admit a non-HTTP scheme. **MUST NOT** be set in production. |
| `request_timeout_secs` | `30` | Per-request timeout budget in seconds (reads and writes). Drives three mechanisms: the ClickHouse *server* settings `send_timeout`/`receive_timeout`; a *client-side* deadline 5s later on every individual ClickHouse await — and, for a record write, on the whole call, so the group-commit queue wait and the flush `INSERT` share it (startup validation requires `insert_batch.max_wait_ms` to leave at least half of it for the flush); and, while `async_insert` is on, the budget an `INSERT` has to absorb the server-side buffer flush (startup validation requires ≥ 2s in that case). The client-side one is the backstop for a connection that is accepted and then never answered (or held open by an intermediary), which the server settings cannot bound because they never reach a server. Sized 5s apart so a responsive server's own timeout fires first and callers get its descriptive error. |
| `insert_batch` | _(enabled, defaults below)_ | The in-process group commit every `usage_records` write goes through — see [Group commit](#group-commit). Sub-keys: `enabled` (default `true`; `false` keeps the direct path, one synchronous `INSERT` per call), `max_rows` (default `10000`; rows per flush), `max_bytes` (default `16777216`; estimated row bytes per flush), `max_wait_ms` (default `50`; the longest the oldest queued call waits before a flush is cut), `queue_capacity` (default `1024`; calls outstanding at once, queued or in flight), `max_concurrent_flushes` (default `4`; flush `INSERT`s in flight at once). Every value must be ≥ 1 and `max_wait_ms` at most half of `request_timeout_secs`. Required on a cluster. A config written before this block existed gets these defaults. |
| `async_insert` | `false` | Server-side alternative to group commit for a single-node deployment that sets `insert_batch.enabled = false`: single-row `usage_records` `INSERT`s then carry `async_insert = 1` and `wait_for_async_insert = 1`, applied **per statement** so no `SELECT` is affected, and the server coalesces concurrent inserts into shared parts. The two must not stack — group-commit flushes carry an explicit `async_insert = 0`, and startup validation refuses `async_insert = true` together with `insert_batch.enabled = true` (and on any cluster). `wait_for_async_insert = 1` is pinned: without it an acknowledged record can be lost on a server restart, and the dedup pre-read would stop seeing its own prior insert. The flush wait is bounded server-side by `async_insert_busy_timeout_ms` and charged against `request_timeout_secs`, which must be ≥ 2s while this is on. Multi-row `INSERT`s and every `usage_type_catalog` write stay synchronous regardless. ClickHouse enforces the `insert_deduplication_token` on asynchronous inserts only for `Replicated*` tables; on the non-replicated default, twins are resolved at read time like any other duplicate. |
| `retention_period_secs` | `31536000` (365d) | `usage_records` retention window; rows older than this are dropped via ClickHouse TTL. Must be in `(0, 100 years]`. Migration DDL creates the table with this value; on every startup `ensure_retention_ttl` issues `ALTER TABLE … MODIFY TTL` when the live interval differs from this value — see [Retention window management](#retention-window-management). |
| `vendor` | `constructorfabric` | Vendor name for GTS instance registration. Must not be empty or blank; an empty value fails startup validation. |
| `priority` | `11` | Plugin priority (lower = higher precedence when multiple plugins are registered). Defaults one below the TimescaleDB plugin's `10`, so with both registered under default config the TimescaleDB backend is selected deterministically; the host's selector breaks equal priorities by registration order, so give each instance a distinct value when overriding. |
| `cluster` | _(unset)_ | Opt-in sharded cluster topology; unset means single node. When set, the plugin reads and writes through `Distributed` tables, which the operator provisions unless `manage_schema` is on: see [Sharded cluster deployment](#sharded-cluster-deployment). Sub-keys: `name` (required; the `remote_servers` cluster name), `records_local_table` (default `usage_records_local`), `catalog_local_table` (default `usage_type_catalog_local`), `insert_quorum` (optional; `auto` or a replica count), `manage_schema` (default `false`; run the cluster migrations at startup), `migration_statement_timeout_secs` (default `3600`; budget per cluster migration statement). All names must be plain identifiers, and the local tables must not be named `usage_records`, `usage_type_catalog` or `usage_collector_migrations`. Requires `insert_batch.enabled = true` (and therefore `async_insert = false`). |

```yaml
gears:
  clickhouse-usage-collector-plugin:
    config:
      database_url: "https://user:${CH_PASSWORD}@host:8443/usage"
      request_timeout_secs: 30
      retention_period_secs: 31536000
      vendor: "constructorfabric"
      priority: 11
      insert_batch:            # group commit (defaults shown)
        enabled: true
        max_rows: 10000
        max_bytes: 16777216
        max_wait_ms: 50
        queue_capacity: 1024
        max_concurrent_flushes: 4
```

The plugin's only gear dependency is `types-registry` (for the registration handshake).

## Operational requirements

### TLS enforcement

- `database_url` embeds ClickHouse credentials and is the transport for every usage record, so a plaintext `http://` connection is a credential- and data-exposure risk, not just a style choice.
- On startup, `ClickHousePluginConfig::validate` (called before any connection is made) rejects a `http://` `database_url` **unless** `allow_insecure_http = true` is set explicitly. A `https://` URL never needs the override.
- The check reads the scheme of the *parsed* URL, which is normalized to lowercase, so `HTTP://` is gated exactly like `http://` rather than slipping past a raw string comparison.
- `validate` also rejects any scheme other than `http`/`https` — including the native-protocol `clickhouse://` and `tcp://` forms — since this plugin talks to ClickHouse's HTTP interface only. That is a separate failure from the TLS gate: `allow_insecure_http` is consent to skip TLS, not consent to an unusable scheme. Only the offending scheme appears in the error; the DSN never does.
- `allow_insecure_http` exists for local development/test against an unencrypted ClickHouse instance (e.g. a Docker test container) — it **MUST NOT** be set in production.
- Even with the override, `build_client` still emits a `tracing::warn!` on every plaintext connection so operators have a durable, per-startup signal that TLS is off.
- The HTTPS transport is built from the rustls `CryptoProvider` the host process installed, not from the `clickhouse` crate's own connector (which hardcodes a non-FIPS `aws-lc-rs` provider). A server started with `--features fips` therefore gets FIPS-validated crypto on the ClickHouse path too, and this plugin needs no `fips` feature of its own. If no provider has been installed — i.e. `toolkit::bootstrap::init_crypto_provider` has not run — `build_client` fails rather than falling back.

### Schema migrations

The schema is managed by a versioned migration runner (`src/infra/storage/migrator.rs`), modelled on `sea-orm-migration`. sea-orm cannot run it, since neither `sea-query` nor `sea-orm-migration` has a ClickHouse backend. Each migration is a named step (`m0001_init`, `m0002_…`) with an `up`. The runner records the steps it applied in a tracking table and, on every start, runs only the steps that are not recorded yet.

The DDL is built in Rust, with no SQL files, and one step list serves both topologies. Each step is a file under `src/infra/storage/migrator/` (`m0001_init.rs`, …) that declares logical tables once, with the design rationale for each column, key and setting. The migrator fills in the topology: plain `ReplacingMergeTree` tables on a single node; on a cluster, `ReplicatedReplacingMergeTree` local tables behind `Distributed` tables, all `ON CLUSTER`; plus the configured retention in the TTL. `src/infra/storage/ddl.rs` is the small `CREATE TABLE` builder behind it. sea-query cannot express `ENGINE`, `PARTITION BY`, `TTL`, `SETTINGS`, `ON CLUSTER` or skip indexes.

| Topology | Steps | Tracking table |
|---|---|---|
| Single node | `m0001_init`, `m0002_insert_dedup_window` | `usage_collector_migrations` |
| Cluster, with `cluster.manage_schema` | the same steps (`m0002_insert_dedup_window` is a no-op: replicated tables deduplicate on their own) | `usage_collector_migrations`, replicated to every host |

- **Existing deployments.** A deployment from before the runner has its tables but no tracking table. Its first start runs every step as a no-op and records them.
- **No lock.** ClickHouse has no advisory lock, so replicas that start together can run the same step concurrently. Every step must therefore be idempotent: `IF NOT EXISTS` DDL, read before alter, and guarded data movement. A duplicate record in the tracking table is harmless.
- **Adding a step.** Add the next numbered file under `src/infra/storage/migrator/` (`m0003_….rs`), declare it in `migrator.rs`, and append it to the end of `migrations()` there. The same list serves both topologies. Never rename, reorder or remove a step that has shipped, because steps are matched by name. A one-time schema fix that a restart cannot apply today, such as the indexes below, belongs in a new step.
- **Every start.** `ensure_retention_ttl` is not a step. It follows config, so it runs on every start, after the migrations.

### Retention window management

- `m0001_init` creates `usage_records` with a TTL of `retention_period_secs`.
- On every plugin `init`, after the [schema migrations](#schema-migrations), `ensure_retention_ttl` reads the live TTL from `system.tables.create_table_query` and compares it to `retention_period_secs`. When missing or different, it runs:

      ALTER TABLE usage_records MODIFY TTL created_at + INTERVAL <n> SECOND DELETE

- Changing `retention_period_secs` in config and restarting therefore updates the table TTL automatically; no manual operator `ALTER` is required for retention changes.
- Verify the effective clause with `SHOW CREATE TABLE usage_records` (ClickHouse may rewrite `INTERVAL <n> SECOND` as `toIntervalSecond(<n>)`).
- `usage_type_catalog` carries no TTL clause and is never retention-bounded.
- ClickHouse applies TTL eviction asynchronously during background merges, so rows can outlive the threshold for a while; expiry is not a synchronous delete.

### Data-skipping index management

Same class of gotcha for indexes (not TTL): everything in `CREATE TABLE IF NOT EXISTS` other than what `ensure_retention_ttl` and `ensure_insert_dedup_window` reconcile applies **only at first provisioning**.

- The schema declares two `bloom_filter` data-skipping indexes on `usage_records` — `idx_records_id` on `id` and `idx_records_corrects_id` on `corrects_id` — so that `get_usage_record` (`WHERE id = ?`) and the deactivation cascade (`WHERE id = ? OR corrects_id = ?`) prune granules instead of scanning the table. The `ORDER BY` key is deliberately unchanged, since it is the dedup identity `ReplacingMergeTree` collapses on.
- A deployment provisioned **before** these indexes existed does not get them from a restart: the DDL re-runs as a no-op. Add them manually, then materialize them over the existing parts:

      ALTER TABLE usage_records ADD INDEX idx_records_id id TYPE bloom_filter GRANULARITY 1
      ALTER TABLE usage_records ADD INDEX idx_records_corrects_id corrects_id TYPE bloom_filter GRANULARITY 1
      ALTER TABLE usage_records MATERIALIZE INDEX idx_records_id
      ALTER TABLE usage_records MATERIALIZE INDEX idx_records_corrects_id

- `MATERIALIZE INDEX` is a background mutation over existing parts; new parts are indexed on write, so read latency improves gradually until it finishes. Verify with `SHOW CREATE TABLE usage_records` and watch `system.mutations`.

### Insert dedup window

- The single-node schema creates `usage_records` with `SETTINGS non_replicated_deduplication_window = 10000`. Every `usage_records` `INSERT` carries an `insert_deduplication_token` derived from its row ids, and the window is what lets ClickHouse drop a retry of the *same* statement — the same group-commit flush, or the same marker write — instead of storing it again. The window is finite (10,000 blocks here; `replicated_deduplication_window`, at its server default, on a cluster) and matches only an identical flush: a duplicate written by another collector instance carries another token, is stored, and is resolved by the reads' version resolution and by the next merge (see [Storage semantics](#storage-semantics)).
- The single-node migration step `m0002_insert_dedup_window` (`ensure_insert_dedup_window`) reads the live setting from `system.tables.create_table_query` and, when it is missing or differs, runs:

      ALTER TABLE usage_records MODIFY SETTING non_replicated_deduplication_window = 10000

  `MODIFY SETTING` is metadata-only, so a deployment provisioned before the setting existed picks it up on its next restart with no manual step. Like any step, it runs once and is then recorded, so a setting changed by hand later is not reverted.
- On a table provisioned as `ReplicatedReplacingMergeTree` the setting is accepted but inert: `replicated_deduplication_window` governs, and asynchronous inserts are deduplicated by token too (`async_insert_deduplicate = 1` is already sent).
- Verify the token is on the wire:

      SYSTEM FLUSH LOGS;
      SELECT event_time, Settings['insert_deduplication_token'], written_rows
      FROM system.query_log
      WHERE type = 'QueryFinish' AND query_kind = 'Insert'
        AND positionCaseInsensitive(query, 'usage_records') > 0
      ORDER BY event_time DESC LIMIT 5

  A deduplicated block shows `written_rows = 0` for the losing insert.

### Group commit

With `insert_batch.enabled` (the default) no `create_usage_record` or `create_usage_records` call issues its own `INSERT`. Each call runs its own checks — the `Decimal128(9)` value check, the catalog existence check, the dedup pre-read — composes the rows it has to write, and submits them to one in-process queue; a background flusher (the gear's lifecycle task) cuts the queued calls into flushes and writes each flush as **one synchronous `INSERT`**, then completes every call the flush carried. The `INSERT` size is thereby decoupled from the API-call size: a request-shaped ingest stream becomes a stream of shared parts.

- **When a flush is cut.** On reaching `max_rows` or `max_bytes` (estimated row bytes), or once the *oldest* queued call has waited `max_wait_ms` — continuous arrivals cannot postpone it. Up to `max_concurrent_flushes` `INSERT`s are in flight at once; the flusher never blocks behind one of them.
- **A call's rows travel together**, in input order, in one flush; a flush combines as many whole calls as fit. A call larger than `max_rows`/`max_bytes` is flushed alone, never split. That is a batching guarantee, not transactional atomicity: a flush commits one part per `toYYYYMM(created_at)` partition it touches and, on a cluster, per shard, so a reader can observe part of a call that spans months or shards (the same bound a direct multi-row `INSERT` has).
- **No acknowledgement before the `INSERT` succeeded.** A call returns `Ok` only once its flush is committed, so a record is durable and visible to its own retry's dedup pre-read when the call returns. Deactivation markers and every `usage_type_catalog` write bypass the queue and stay synchronous.
- **Settings on the wire.** Every flush carries `async_insert = 0` explicitly, so a server or profile default of `1` cannot add a second buffering layer, plus one `insert_deduplication_token` computed over the flush's rows; on a cluster `distributed_foreground_insert = 1` and the configured `insert_quorum` apply as for every insert.
- **Bounds.** At most `queue_capacity` calls are outstanding (queued or in flight), and at most `max_rows × (max_concurrent_flushes + 1)` rows / `max_bytes × (max_concurrent_flushes + 1)` estimated bytes. A call over a bound is refused with a retryable `Transient` error (`Retry-After` hint 1 s), counted in `uc_clickhouse_admission_rejected_total{reason}`; a call that alone exceeds the row or byte bound is refused as `Internal`, since a retry could never admit it. The queue wait and the flush share the call's `request_timeout_secs` budget: a call whose budget runs out while it waits gets `Transient` (its rows are still flushed if another call waits on them, otherwise dropped at the next cut).
- **Coalescing inside one instance.** A row whose dedup tuple is already queued or in flight is not written twice: an identical payload joins the pending row and receives its outcome (`uc_clickhouse_coalesced_rows_total` — an instance-local count, **not** a measure of cross-instance duplicates); a differing payload is refused at once as `IdempotencyConflict` naming the pending row's id (`uc_clickhouse_coalesce_conflicts_total`). The coalescing entry lives exactly as long as the row is unresolved.
- **Failures.** A flush is immutable — the same rows, order, versions and token on every attempt. A transient failure (connectivity, the overload codes listed under [Storage semantics](#storage-semantics)) is retried up to three attempts with jittered backoff (50 ms, 100 ms), never while draining and never once every waiter is gone; after the last attempt every affected call gets `Transient`. A failure ClickHouse attributes to row content (`CANNOT_PARSE_*`, `ARGUMENT_OUT_OF_BOUND`, `VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE`, `DECIMAL_OVERFLOW`, `INCORRECT_DATA`) on a flush that combined several calls is **isolated**: each call is written once more on its own, directly, under the token a non-batched write of it would have carried, so the bad call fails and the rest succeed (`uc_clickhouse_isolation_attempts_total`; bounded to one `INSERT` per call, never re-queued, never recursive). Schema, permission and every other error fan out to every waiter as they are. A `Distributed` `INSERT` can commit on some shards before failing, so a `Transient` outcome means "unknown": the caller's retry pre-read absorbs the rows that did land, and the reads resolve any twin.
- **Shutdown.** When the gear's cancellation token fires, admission closes (a late call gets `Transient`), everything queued is flushed without waiting for `max_wait_ms`, and in-flight flushes are awaited up to the drain deadline (`max_wait_ms` + the client-side deadline, capped at 29 s so the lifecycle stop budget stays under the host's 35 s per-gear deadline). Whatever is still unresolved at the deadline, or when the batcher is aborted, is completed with `Transient` — no caller hangs. `uc_clickhouse_shutdown_drain_total{outcome}` records how each stop ended.
- **Watching it.** `uc_clickhouse_flush_rows` / `uc_clickhouse_flush_bytes` show the amortisation; `uc_clickhouse_ingest_queue_wait_seconds` and `uc_clickhouse_ingest_queue_age_seconds` the latency it costs (bounded by `max_wait_ms`); `uc_clickhouse_insert_duration_seconds{mode="flush"}` the `INSERT` itself, next to `mode="single"` / `mode="batch"` for the end-to-end call latency. Confirm the part-count improvement under concurrent ingest with:

      SELECT partition, count() AS parts, sum(rows) AS rows,
             round(sum(rows) / count()) AS avg_rows_per_part
      FROM system.parts
      WHERE database = currentDatabase() AND table = 'usage_records' AND active
      GROUP BY partition ORDER BY partition

  and the `INSERT` count against the call count in `system.query_log` (`query_kind = 'Insert'`, `written_rows`). Corroborate merge pressure with `system.merges` and `system.part_log` (`event_type = 'MergeParts'`).

**Server-side asynchronous inserts** (`async_insert`, single node only, `insert_batch.enabled = false`) are the alternative for a deployment that cannot run the lifecycle task: the server buffers single-row `INSERT`s into shared parts, `wait_for_async_insert = 1` keeps the durability ack and read-your-writes, and the busy timeout is the server's (`async_insert_busy_timeout_ms`, deliberately not exposed). The direct path otherwise writes one part per request. Multi-row `INSERT`s and catalog writes are synchronous on that path too.

### Workload isolation and pool contention

- One `clickhouse::Client` (and therefore one underlying HTTP connection pool) serves **both** the ingestion and the query paths. This is a deliberate v1 choice, not a solved isolation guarantee.
- **The pool is not tunable from config.** There is no `pool_max_connections`-style setting: `clickhouse` 0.15.1 exposes no pool-bound builder (`with_setting`/`with_option` set ClickHouse *server* settings, and the `with_http_client` seam cannot be used from outside the crate), so no config field could drive it. Everything below is therefore an operational mitigation, not a knob.
- Risk to the ingestion-throughput NFR: a burst of aggregation or list queries competes with ingest writes for the same pool and the same ClickHouse server resources, so query bursts can delay pool acquisition on the write path and push ingest below its throughput budget. The plugin has no internal reservation, priority, or queueing that protects ingest from read traffic.
- Operator guidance:
  - Watch `uc_clickhouse_pool_acquire_duration_seconds` together with `uc_clickhouse_insert_duration_seconds{mode="flush"}` and `uc_clickhouse_query_requests_total{query_kind="aggregated"}`: rising pool-acquire time correlated with query volume is this contention, not a ClickHouse slowdown.
  - Bound read cost server-side with ClickHouse's own controls (settings profiles / quotas per query user, `max_concurrent_queries_for_user`) so heavy analytical queries cannot consume the whole server.
  - `request_timeout_secs` bounds how long any single starved request waits; sizing it too high lets a query burst hold pool capacity longer.
  - For hard separation, run **two plugin instances** with distinct GTS priorities pointed at different endpoints (write primary vs. read replica). This is an operational workaround; the plugin does not split pools internally.
- A future revision may split ingestion and query onto independently-pooled clients (additive, non-breaking to the SPI); until then this contention point is accepted and documented rather than assumed away.

### Sharded cluster deployment

Setting `cluster` switches the plugin from a self-provisioned single node to a sharded, replicated ClickHouse cluster. By default the schema belongs to the operator: the plugin creates and alters nothing and only checks the schema at startup. With `manage_schema: true` the plugin runs the cluster [schema migrations](#schema-migrations) itself and then applies the same check.

```yaml
gears:
  clickhouse-usage-collector-plugin:
    config:
      database_url: "https://user:${CH_PASSWORD}@ch-lb:8443/usage"  # a load balancer over the nodes
      # insert_batch stays enabled (the default): a cluster requires group commit
      cluster:
        name: "usage_cluster"      # from remote_servers
        # insert_quorum: "auto"    # optional, see below
        # manage_schema: true      # optional: the plugin runs the cluster DDL
```

**Layout.** `usage_records` and `usage_type_catalog` are `Distributed` tables. Behind them are shard-local `ReplicatedReplacingMergeTree` tables, `usage_records_local` and `usage_type_catalog_local`. `usage_records` is sharded by `cityHash64(tenant_id)` and `usage_type_catalog` by `cityHash64(gts_id)`. The tenant sharding key is required, not a tuning choice: a deactivation marker clones the row it marks, a duplicate create carries the same tenant, and a compensation belongs to the tenant it corrects, so every version of a row lands on the same shard. That is what makes the per-shard `FINAL` of `list` and `aggregate` exact — each shard collapses every version of its own keys, and the initiator never receives two rows for one key.

**Prerequisites (both modes).** On every node, configure the cluster in `remote_servers`, ClickHouse Keeper (`zookeeper`), and the `{shard}` / `{replica}` `macros`, then create the target database `ON CLUSTER`. In addition:
- point `database_url` at a load balancer or proxy over the nodes, not at a single node: the plugin tolerates an unreachable replica only if it can still reach some node;
- set `internal_replication = true` for every shard in `remote_servers`, so that a `Distributed` insert goes to one replica of the shard and replication copies it to the others;
- configure `insert_quorum: auto` only for three or more replicas per shard. `auto` means a majority of the shard's replicas, so with two replicas it needs both, and every insert fails with `TOO_FEW_LIVE_REPLICAS` while one of them is down.

**Provisioning by the operator (default).**
1. Configure `cluster` and start the plugin. With the tables missing, startup fails, and the error ends with the cluster DDL for your configured cluster name, local table names and retention (`migrator::render_ddl`: every migration step's DDL, rendered for the cluster).
2. Run that DDL once on any node in the target database (`clickhouse-client --database <db> --multiquery`). Its statements are `ON CLUSTER`, so this creates all four tables on every node.
3. Start the plugin again.

**Provisioning by the plugin (`manage_schema: true`).** Start the plugin. It runs the migration steps, which execute the same DDL, then reconciles the TTL `ON CLUSTER`. Each statement is bounded by `migration_statement_timeout_secs`, which also sets how long the server waits for every host to run an `ON CLUSTER` statement (`distributed_ddl_task_timeout`). Once the schema is up to date, startup issues no `ON CLUSTER` statement, so an unreachable host does not delay it. A pending statement waits only for the hosts ClickHouse Keeper reports active (`distributed_ddl_output_mode = 'throw_only_active'`): a host that has just gone down holds it until Keeper expires that host's session (`session_timeout_ms`, 30 s by default), and a failure on an active host still fails startup. A host that was down runs the statement from the DDL queue when it returns. Replicated tables also catch up on `ALTER`s through Keeper replication, but `CREATE TABLE` depends on the DDL queue alone, whose entries expire after `task_max_lifetime` (7 days by default). A host that comes back after that lacks the new tables: startup validation then names it, and the operator applies the DDL the error prints. The plugin's user then needs `CREATE TABLE`, `ALTER`, `INSERT` and `SELECT` on the plugin's tables and the `CLUSTER` privilege.

The integration suite (`tests/cluster_integration_ch.rs`) covers both paths, and both use this same DDL.

**Topology is fixed at first start.** A deployment runs on a single node or on a cluster from the start; the plugin does not migrate a single-node deployment's data to a cluster. If single-node `usage_records` / `usage_type_catalog` tables are found on a cluster, `CREATE TABLE IF NOT EXISTS` leaves them in place, and startup validation refuses to start because they are not `Distributed` tables.

**Startup validation.** The plugin refuses to start, listing every problem, when:
- the cluster is not declared in `system.clusters`;
- any of the four tables is missing on a reachable replica (checked through `clusterAllReplicas`);
- a local table is not `ReplicatedReplacingMergeTree` or has a different `ORDER BY` or `PARTITION BY`;
- a `Distributed` table targets another cluster, database or local table, or uses another sharding key.

Unreachable replicas do not prevent startup, so a plugin pod can restart during a rolling ClickHouse upgrade. The plugin logs a warning and checks those replicas on the next start that reaches them. If every replica of a shard is unreachable, it logs a second warning and starts anyway: requests that need that shard fail with a retryable `Transient` error (`ALL_CONNECTION_TRIES_FAILED`) until a replica is back, and tenants on the other shards keep working.

With an operator-provisioned schema, a `usage_records` TTL that differs from `retention_period_secs` only produces a warning; change it with `ALTER TABLE usage_records_local ON CLUSTER … MODIFY TTL`. With `manage_schema`, the plugin runs that `ALTER` itself. The single-node migration steps never run in cluster mode.

**What the plugin sends.** Every request carries:
- `distributed_product_mode = 'local'` (a guard: the request path issues no subquery today, and a future one is shard-local by the colocation invariant instead of being refused);
- `optimize_skip_unused_shards = 1`, so tenant-pinned reads touch one shard;
- `skip_unavailable_shards = 0`, pinned so that a request needing an unreachable shard fails as a retryable `Transient` error instead of returning a partial result, even if a server settings profile enables the setting;
- `distributed_foreground_insert = 1`, so an `INSERT` returns only after the owning shard committed it and the `insert_deduplication_token` is enforced by the replicated table's `replicated_deduplication_window`;
- `insert_quorum`, when configured.

Group commit is required (`insert_batch.enabled = true`): through a `Distributed` table every `INSERT` is synchronous per shard, so the plugin's flusher is the only layer that turns a request-shaped stream into shared parts. `async_insert` must be `false`: an async insert is buffered on the initiator rather than on the shard that commits it, which bypasses that dedup and the durability ack.

**Partial commit.** A flush `INSERT` through the `Distributed` table is one block per shard, each committed by its owning shard. When one shard fails, the others may already have committed their blocks; the plugin reports `Transient` to every call of the flush and does not know which rows landed. Retrying is safe: the retry's dedup pre-read absorbs the rows that did land, an identical regrouped flush is dropped by the shard's token window, and `FINAL` resolves anything stored twice. The pre-retry state is deliberately not pinned by the integration tests.

**Mutations.** `delete_usage_type` and its orphan sweep run `ALTER TABLE <local> ON CLUSTER <name> DELETE WHERE gts_id = ?` with `mutations_sync = 2`, waiting for every replica, because ClickHouse refuses mutations on a `Distributed` table. The plugin's database user therefore needs `ALTER DELETE` on the local tables and the `CLUSTER` privilege, besides `SELECT` and `INSERT` on all four tables.

**Consistency.** A read through a `Distributed` table may be served by a replica that has not yet fetched a just-committed part, so read-your-writes across replicas is bounded by replication lag (ADR-0006 accepts this). That includes the dedup pre-read of a retried create. The insert dedup token still drops an exact retry of the same flush, because dedup is coordinated through Keeper; a retry regrouped with other calls carries another token, is stored, and is resolved by `FINAL`. Visibility is per shard: a flush spanning shards is committed shard by shard. Set `insert_quorum` (for example `auto`, with three or more replicas per shard; see the prerequisites above) to acknowledge an insert only once a quorum of the shard's replicas has it. This narrows the window at a write-latency cost.

## Storage semantics

- **Deduplication** — three layers. (1) An application-level read-before-insert: a stored row with an identical payload is absorbed, a differing one is `IdempotencyConflict`. (2) Inside one instance, the group-commit queue coalesces a row that is already queued or in flight: identical → joins it, differing → `IdempotencyConflict` naming the pending row. (3) At the engine, every flush carries an `insert_deduplication_token`, so a retry of the *same* flush within the table's finite dedup window is dropped. Nothing serializes creates across instances: two instances with the same dedup key can both pass their pre-reads and both write, under different tokens, and both get `Ok`. Both rows are stored under one sort key with different `version`s; every read resolves them to the **highest version** — `FINAL` on `list`/`aggregate`, `LIMIT 1 BY` on `get` and the pre-reads — and `ReplacingMergeTree(version)` collapses them at the next merge. For identical payloads that is invisible; for differing payloads the higher version wins and no conflict is reported — there is **no cross-instance first-writer-wins**. Once the earlier row is visible, a later create sees it and the usual absorb / conflict rules apply. See DESIGN.md §3.6 and §3.8.
- **Batch ingest** — two read statements per `create_usage_records` call regardless of how many usage types it spans — one catalog existence query over the batch's distinct `gts_id`s, one dedup pre-read over every record that passed it — then one submission of the composed rows to the group-commit queue, where they travel together in one flush `INSERT` (alone when group commit is off; the gear caps a batch at 100 records). A flush commits **one part per partition it touches**, and `usage_records` is `PARTITION BY toYYYYMM(created_at)` — so a reader sees all of a batch's new rows or none of them only when the batch falls in a single calendar month (and, on a cluster, one shard); a batch spanning two months is two commits and can be observed part-way through. See [Group commit](#group-commit).
- **Consistency profile** — on a single-node deployment: effectively immediate read-after-write for any reader. On a replicated deployment: bounded by ClickHouse's own replication lag. Every read shows each logical row once, at its highest version, without waiting for a merge: point reads (`get`, the deactivation cascade, the dedup pre-reads) resolve `ReplacingMergeTree` versions in SQL (`ORDER BY version DESC LIMIT 1 BY <sort key>`) over a bloom-filter-pruned candidate set; `list` and `aggregate` read `FROM usage_records FINAL` with `do_not_merge_across_partitions_select_final = 1` (every version of a key shares its `created_at`, hence its monthly partition), which merges only the part ranges whose keys overlap and streams the rest, so its cost grows with the number of unmerged twins and markers rather than with the scan. Visibility is per partition and per shard, not per call.
  For strict cross-replica read-your-writes, configure ClickHouse's native `insert_quorum` write-quorum setting on the server; this plugin does not enable it by default and enabling it incurs a proportional write-latency cost — see DESIGN.md §3.8.
- **Workload isolation** — the ClickHouse client/pool is shared by both the ingestion and query paths (v1 design), so query bursts can degrade ingestion throughput; see [Workload isolation and pool contention](#workload-isolation-and-pool-contention).
- **Referential integrity** — application-emulated on both sides, with a known gap on the delete side. Create side: `create_usage_record(s)` checks that every referenced `gts_id` exists in `usage_type_catalog` (an existence read, which needs no version resolution) and rejects absent ones with `UsageTypeNotFound`. Delete side: `delete_usage_type` runs an existence read, then a capped reference probe over `usage_records` (refusing with `UsageTypeReferenced` → HTTP 409 if any row references the type), then removes the catalog row with `ALTER TABLE … DELETE` under `mutations_sync = 1`, then re-probes and sweeps any record that landed in between. **This is not race-free.** ClickHouse has no foreign key, so an insert whose own catalog check passed before the catalog row was removed can commit after the sweep and orphan a row. The reference TimescaleDB plugin closes this window with a native `FOREIGN KEY … ON DELETE RESTRICT` on `usage_records.gts_id`, admitting no window in which a concurrent `create_usage_record` can reference a type being deleted; this plugin narrows that window instead of closing it. The window is bounded, not closed: the catalog row is deleted **before** the sweep on purpose, so from that moment the insert-time check refuses new records on its own. A record already admitted to the group-commit queue when the delete runs widens the window to the flush interval (`max_wait_ms`). Watch `uc_clickhouse_orphaned_reference_detected_total` — a non-zero rate means deletes are being run against types with live ingest. Prefer deleting a type while no ingest for it is in flight. See DESIGN.md §3.6.
- **Retention** — ClickHouse TTL clause on `usage_records`: `retention_period_secs` in `CREATE TABLE`, then reconciled to `retention_period_secs` on every startup via `ensure_retention_ttl` (`ALTER TABLE … MODIFY TTL` when needed) — see [Retention window management](#retention-window-management).
- **Error classification** — a failure is reported as retryable `Transient` when the client could not reach ClickHouse at all (network, timeout, compression), when the client-side deadline expires, or when ClickHouse itself answers with one of a fixed allowlist of overload/backpressure codes: `159` `TIMEOUT_EXCEEDED`, `202` `TOO_MANY_SIMULTANEOUS_QUERIES`, `203` `NO_FREE_CONNECTION`, `209` `SOCKET_TIMEOUT`, `210` `NETWORK_ERROR`, `252` `TOO_MANY_PARTS`, `279` `ALL_CONNECTION_TRIES_FAILED`, `285` `TOO_FEW_LIVE_REPLICAS`, `999` `KEEPER_EXCEPTION` — plus HTTP `502`/`503`/`504` when ClickHouse returned no readable body (typically an intermediary). Anything else, including `241` `MEMORY_LIMIT_EXCEEDED` (permanent for an over-large batch, so retrying it would loop) and `319` `UNKNOWN_STATUS_OF_INSERT`, is `Internal`. Only the unreachable-backend cases (and a client-side deadline expiry, which is classified the same way) clear the `uc_clickhouse_ready` gauge: a server that answers with backpressure is degraded, not down. The gauge is re-armed by the next successful request, so it recovers without a restart; alert on `uc_clickhouse_ready == 0` with a `for:` window so a single slow request does not page. It is never re-armed after the gear's cancellation token fires.
- **Deactivation** — applied as a single multi-row `INSERT` of versioned marker rows (depth-1 only); no `UPDATE` or `ALTER TABLE … DELETE` is issued on the request path. A plain retry of a record that races its own deactivation and misses the original row at pre-read can re-insert it as active with a higher version (the gateway's caller-side active-target check protects compensations, not retries); see DESIGN.md §3.6.
- **Schema** — single node: versioned, idempotent migration steps at startup (see [Schema migrations](#schema-migrations)); cluster: operator-provisioned, or plugin-managed with `manage_schema`, and validated either way (see [Sharded cluster deployment](#sharded-cluster-deployment)); indexes still apply only at first provisioning (see [Data-skipping index management](#data-skipping-index-management)), while TTL is updated on startup when config differs.

## SPI conformance

The crate implements `usage_collector_sdk::UsageCollectorPluginV1` (via `StorageAdapter` over the record and catalog stores). Conformance is enforced at compile time. All ten SPI methods are implemented. One does not meet its contract in full: `delete_usage_type` emulates `ON DELETE RESTRICT` without a foreign key and admits a race the SPI forbids — see [Storage semantics](#storage-semantics) and the deviations table in `docs/PRD.md`.

## Running integration tests

The real-DB suites are gated behind the `clickhouse` feature, marked `#[ignore]`, and require Docker for a ClickHouse image:

    cargo nextest run -p cf-gears-clickhouse-usage-collector-plugin --features clickhouse --run-ignored all

(or `make test-integration LANE=usage-collector-ch`, which also sets `CH_REQUIRE_DOCKER=1` so a missing Docker fails instead of skipping).

This includes `tests/cluster_integration_ch.rs`, which boots a two-shard cluster (two nodes plus embedded Keeper on a private Docker network) per test. Each test either provisions the cluster from the operator DDL (`migrator::render_ddl`) or leaves it bare for the managed migrations.

Without the feature, only unit tests run (no Docker needed):

    cargo test -p cf-gears-clickhouse-usage-collector-plugin

## Running E2E tests

The suites above exercise this crate directly. To exercise it as the bound
storage plugin behind the usage-collector gear's HTTP API — over a real server,
through a real ClickHouse container:

    make e2e-usage-collector

That target runs the usage-collector E2E suite twice, once per storage backend
(TimescaleDB, then this one), building a dedicated binary for each: every gear
linked into the server is initialized, and both plugins fail `init` without
their own live database, so they cannot share one process. The test bodies are
shared and speak HTTP only — anything they assert is a contract of the gear's
API that both plugins must satisfy, `delete_usage_type` included: the
referenced-type delete seam
(`test_delete_usage_type_referenced_by_record_is_rejected`) runs unmodified on
both backends, since the uncontended contract — 409 for a referenced type, 204
for an unreferenced one — is identical either way. The race this backend admits
between probe and delete is not reachable from a single-threaded HTTP test and
is covered by documentation rather than by an E2E assertion.

Config lives in `testing/e2e/suites/usage_collector/config-clickhouse.yaml`;
the container is managed by `ClickHouseSidecar` in
`testing/e2e/lib/sidecars.py`. The ClickHouse image and tag are pinned once,
as `CLICKHOUSE_IMAGE`/`CLICKHOUSE_TAG` in
`libs/test-containers/src/lib.rs`, and consumed from there by both
`tests/common/mod.rs` (via `test_containers::clickhouse()`) and by
`sidecars.py`; the Rust test `e2e_sidecar_pins_the_same_clickhouse_image`
checks the two stay in agreement.
