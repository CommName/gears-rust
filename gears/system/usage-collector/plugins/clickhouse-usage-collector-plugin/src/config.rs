use std::borrow::Cow;

use percent_encoding::percent_decode_str;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use crate::infra::storage::ingest::{DRAIN_DEADLINE_CAP, IngestLimits};
use crate::infra::storage::migrator::MIGRATIONS_TRACKER;
use crate::infra::storage::topology::{CATALOG_TABLE, RECORDS_TABLE};

/// Return `true` when `url` uses a plaintext `http` connection (no TLS).
///
/// Used by [`ClickHousePluginConfig::validate`] to fail closed on an
/// unencrypted `database_url` by default, and by
/// [`crate::infra::storage::pool::build_client`] for the runtime TLS-posture
/// warning. `pub(crate)` so both call sites — and their unit tests — share
/// one definition instead of duplicating the scheme check.
///
/// The scheme is compared after URL parsing, not as a string prefix: URL
/// schemes are case-insensitive and `Url::parse` normalizes them to lowercase,
/// so `HTTP://host/db` is a cleartext connection that a `starts_with("http://")`
/// test would wave through — bypassing the gate and the warning both.
///
/// Input that does not parse as an absolute URL is reported as plaintext: it
/// cannot be shown to be encrypted, and this is the fail-closed direction.
/// `validate` rejects unparseable input earlier, so in practice this only
/// affects `build_client`'s defense-in-depth warning, where a spurious warning
/// is harmless and a missed one is not.
pub(crate) fn is_plaintext_url(url: &str) -> bool {
    match url::Url::parse(url) {
        Ok(parsed) => parsed.scheme() == "http",
        Err(_) => true,
    }
}

/// Percent-decode a `database_url` userinfo component (username or password)
/// into the literal credential expected by `Client::with_user` /
/// `with_password`.
///
/// `Url::username` / `Url::password` return the encoded form, so an operator
/// who embeds a secret containing URL-reserved characters must percent-encode
/// it, and this is the one place it is decoded back. Shared by
/// [`ClickHousePluginConfig::validate`] (fail-closed at startup) and
/// [`crate::infra::storage::pool::build_client`] (the actual decode) so both
/// agree on what is acceptable.
///
/// # Errors
///
/// Returns [`std::str::Utf8Error`] when the decoded bytes are not valid UTF-8.
/// This is deliberately *not* a lossy decode: the `clickhouse` client takes
/// `&str`, and replacing an invalid sequence with U+FFFD would silently turn a
/// corrupted or truncated secret into a *different* credential, so the gear
/// would start and then loop on authentication failures with no hint that the
/// config was wrong. The error carries only a byte offset, never the value.
pub(crate) fn decode_userinfo(encoded: &str) -> Result<String, std::str::Utf8Error> {
    percent_decode_str(encoded)
        .decode_utf8()
        .map(Cow::into_owned)
}

/// Configuration for the `ClickHouse` Usage Collector storage backend.
/// Durations are whole seconds (repo convention).
#[derive(Debug, Clone, Deserialize, toolkit_macros::ExpandVars)]
#[serde(default, deny_unknown_fields)]
pub struct ClickHousePluginConfig {
    /// `ClickHouse` HTTP endpoint URL including credentials, e.g.
    /// `https://user:${CH_PASSWORD}@host:8443/db`. Held as a
    /// [`SecretString`]: `Debug` emits `[REDACTED]` (so `tracing::debug!(?cfg)`
    /// and panic-formatter dumps never print the resolved URL), there is no
    /// `Display`, `Serialize`, or `PartialEq` to leak the bytes through a
    /// config-snapshot path or an assertion message, and the buffer is zeroized
    /// on drop. The only read accessor is
    /// [`ExposeSecret::expose_secret`](secrecy::ExposeSecret::expose_secret),
    /// so every read site is grep-able; `${VAR}` templating is expanded via the
    /// `#[expand_vars]` derive before any consumer sees the value.
    /// [`Self::validate`] admits only the `http` and `https` schemes, and
    /// rejects a plaintext `http` URL unless [`Self::allow_insecure_http`] is
    /// set. Both checks read the parsed (lowercase-normalized) scheme, so
    /// `HTTP://` is treated as `http://`.
    #[expand_vars]
    pub database_url: SecretString,
    /// Explicit development/test opt-out for a plaintext (`http://`)
    /// `database_url`. `database_url` embeds credentials
    /// ([`Self::database_url`]), so an unencrypted connection sends them —
    /// and every usage record — over the network in cleartext. Defaults to
    /// `false`: [`Self::validate`] rejects a `http://` `database_url` unless
    /// this is explicitly set to `true`. Has no effect on a `https://` URL, and
    /// does not admit a scheme outside `http`/`https`: it is consent to skip
    /// TLS, not consent to a scheme the `ClickHouse` client cannot speak.
    pub allow_insecure_http: bool,
    /// HTTP request timeout in seconds (applies to reads and writes).
    ///
    /// Drives three distinct mechanisms: the `ClickHouse` server settings
    /// `send_timeout` / `receive_timeout`; the client-side deadline from
    /// `Self::client_deadline`, `CLIENT_DEADLINE_GRACE_SECS` later; and — while
    /// [`Self::async_insert`] is enabled — the budget an `INSERT` has to absorb
    /// the server-side async-insert buffer flush, which is why [`Self::validate`]
    /// enforces a floor of `MIN_ASYNC_INSERT_TIMEOUT_SECS` in that case.
    pub request_timeout_secs: u64,
    /// Server-side alternative to the plugin's own group commit for a
    /// deployment that keeps [`Self::insert_batch`] disabled: send
    /// **single-row** `usage_records` `INSERT`s with the `ClickHouse` settings
    /// `async_insert = 1` and `wait_for_async_insert = 1`, applied
    /// per-statement via `clickhouse::insert::Insert::with_setting` rather than
    /// on the shared `Client`, so no `SELECT` is affected — not even the
    /// table-metadata read the `clickhouse` crate performs inside
    /// `Client::insert` itself, which runs before this setting is applied.
    ///
    /// Off by default: with group commit on, the plugin already coalesces
    /// every call's rows into shared `INSERT`s, and every flush carries an
    /// explicit `async_insert = 0` so a server or profile default cannot add
    /// a second buffering layer underneath it. [`Self::validate`] refuses this
    /// together with `insert_batch.enabled` for that reason.
    ///
    /// `async_insert = 1` moves part formation from the request into a
    /// server-side buffer that coalesces concurrent inserts sharing one
    /// (query, settings, format) triple into shared parts. Without group
    /// commit, `create_usage_record` issues one `INSERT` per record, so a
    /// request-shaped ingest stream otherwise writes one small part per request
    /// and drives `ReplacingMergeTree` part count and merge pressure up. Every
    /// such `INSERT` this plugin emits is identical in SQL text, settings, and
    /// format, so the whole ingest stream lands in one queue.
    ///
    /// **Scope.** This setting governs the direct single-row path only.
    /// Multi-row `usage_records` `INSERT`s — `create_usage_records` and the
    /// deactivation cascade — stay synchronous whatever this says, because the
    /// async buffer does not guarantee that one statement's rows become visible
    /// in a single commit and both depend on that (the cascade's marker rows
    /// must flip together). `usage_type_catalog` writes are likewise always
    /// synchronous: that table is control-plane (written only by
    /// `create_usage_type`, unpartitioned, tens of rows), so there is no
    /// concurrent insert stream for the buffer to coalesce and no part count to
    /// reduce.
    ///
    /// `wait_for_async_insert = 1` is pinned, not separately configurable.
    /// Two properties depend on it:
    ///
    /// * **Durability ack.** With `0` the `INSERT` returns as soon as the row
    ///   is buffered, so a server restart loses acknowledged usage records.
    ///   This plugin is the system of record; it cannot answer `Ok` for a row
    ///   that is not committed.
    /// * **Read-your-writes.** `create_usage_record` reads (the dedup
    ///   point-lookup) and then writes, and a retry of the same request must
    ///   observe the earlier insert or it inserts a second time under an
    ///   idempotency key already in use. Only `1` makes the row queryable by
    ///   the time the call returns.
    ///
    /// The cost is latency: the `INSERT` blocks until the buffer flushes,
    /// bounded by the server-side `async_insert_busy_timeout_ms` (adaptive on
    /// `ClickHouse` 24.x+: `async_insert_busy_timeout_min_ms` 50ms →
    /// `async_insert_busy_timeout_max_ms` 200ms) plus the part commit. That
    /// budget is deliberately left to the server rather than mirrored into a
    /// config field — it is a property of the cluster's whole insert workload,
    /// not of this plugin, the server adapts it automatically, and a
    /// plugin-pinned value would defeat that adaptation. An operator who must
    /// pin it should do so in a `ClickHouse` settings profile for the plugin's
    /// DB user, the same mechanism the README already recommends for bounding
    /// read cost.
    ///
    /// Because the flush wait is charged against the request budget,
    /// [`Self::validate`] requires
    /// `request_timeout_secs >= MIN_ASYNC_INSERT_TIMEOUT_SECS` while this is
    /// enabled.
    ///
    /// **Insert dedup token.** Every `usage_records` `INSERT` carries an
    /// `insert_deduplication_token` (see `record_store::insert_dedup_token`),
    /// and `configure_insert` adds `async_insert_deduplicate = 1` when this is
    /// on. `ClickHouse` enforces the token on synchronous inserts against the
    /// table's `non_replicated_deduplication_window`; for *asynchronous*
    /// inserts it enforces it only on `Replicated*` engines. On the shipped
    /// non-replicated table, therefore, a racing duplicate single-record
    /// create is collapsed by `optimize_on_insert` when both land in one
    /// flush and by the background merge otherwise — visible twice to
    /// `list`/`aggregate` (which no longer resolve versions at read time) until
    /// then, while `get` resolves it immediately. Batches and deactivation
    /// markers are always synchronous and always engine-deduplicated.
    ///
    /// Enabling it requires `insert_batch.enabled = false`.
    pub async_insert: bool,
    /// The in-process group commit every `usage_records` write goes through
    /// (`infra::storage::ingest`). See [`InsertBatchConfig`] for the knobs;
    /// enabled by default, and required on a cluster.
    pub insert_batch: InsertBatchConfig,
    /// `usage_records` retention window in seconds; rows older are deleted via
    /// `ClickHouse` TTL. Must be in `(0, MAX_RETENTION_SECS]`.
    pub retention_period_secs: u64,
    /// Vendor name for GTS instance registration.
    pub vendor: String,
    /// Plugin priority (lower = higher priority).
    pub priority: i16,
    /// Opt-in sharded cluster topology. `None` (the default) is the
    /// single-node deployment: the plugin provisions and reconciles its own
    /// schema. `Some` switches to the cluster schema — `Distributed` tables
    /// named `usage_records` / `usage_type_catalog` over
    /// `ReplicatedReplacingMergeTree` local tables — which the plugin
    /// validates at startup. By default the operator provisions it (the DDL
    /// is `migrator::render_ddl`, printed by the startup check when missing); with
    /// [`ClickHouseClusterConfig::manage_schema`] the plugin runs the cluster
    /// migrations itself.
    pub cluster: Option<ClickHouseClusterConfig>,
}

/// Group-commit settings for [`ClickHousePluginConfig::insert_batch`].
///
/// Every `create_usage_record` / `create_usage_records` call runs its own
/// checks and dedup pre-read, then hands its new rows to one in-process queue;
/// a background flusher writes queued calls as shared synchronous `INSERT`s,
/// so the `INSERT` size is decoupled from the API call size. A flush is cut
/// when it reaches `max_rows` or `max_bytes`, or when the *oldest* queued call
/// has waited `max_wait_ms` — continuous arrivals cannot postpone it. Calls
/// are never split across flushes; a call larger than a flush target is
/// flushed alone.
///
/// Bounds: at most `queue_capacity` calls are outstanding (queued or in
/// flight), and at most `max_rows × (max_concurrent_flushes + 1)` rows /
/// `max_bytes × (max_concurrent_flushes + 1)` estimated bytes. A call over a
/// bound is refused with a retryable `Transient` error; a call that alone
/// exceeds the row or byte bound is refused as `Internal`, since a retry could
/// never admit it. Up to `max_concurrent_flushes` `INSERT`s are in flight at
/// once.
///
/// Durations are milliseconds here (the one sub-second knob in this config);
/// `max_bytes` counts an estimate of the `RowBinary` row size, not the encoded
/// statement.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InsertBatchConfig {
    /// Route record writes through the group-commit queue. `false` keeps the
    /// direct path: one synchronous `INSERT` per call (per record for single
    /// creates, per batch for `create_usage_records`), optionally with
    /// server-side [`ClickHousePluginConfig::async_insert`]. Must be `true`
    /// on a cluster.
    pub enabled: bool,
    /// Rows per flush; a flush is cut once the queue holds at least this many.
    pub max_rows: u64,
    /// Estimated row bytes per flush; same trigger semantics as `max_rows`.
    pub max_bytes: u64,
    /// Longest the oldest queued call waits before a flush is cut, in
    /// milliseconds. Charged against `request_timeout_secs`, so
    /// [`ClickHousePluginConfig::validate`] requires it to leave at least half
    /// of that budget for the `INSERT` itself.
    pub max_wait_ms: u64,
    /// Outstanding calls (queued + in flight) admitted at once.
    pub queue_capacity: u64,
    /// Flush `INSERT`s in flight at once.
    pub max_concurrent_flushes: u64,
}

impl Default for InsertBatchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_rows: 10_000,
            max_bytes: 16 * 1024 * 1024,
            max_wait_ms: 50,
            queue_capacity: 1024,
            max_concurrent_flushes: 4,
        }
    }
}

impl InsertBatchConfig {
    /// Every field must be at least 1.
    fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("insert_batch.max_rows", self.max_rows),
            ("insert_batch.max_bytes", self.max_bytes),
            ("insert_batch.max_wait_ms", self.max_wait_ms),
            ("insert_batch.queue_capacity", self.queue_capacity),
            (
                "insert_batch.max_concurrent_flushes",
                self.max_concurrent_flushes,
            ),
        ] {
            if value == 0 {
                return Err(format!("{name} must be greater than 0"));
            }
        }
        Ok(())
    }

    /// The queue limits for these settings and the client-side deadline.
    ///
    /// The drain deadline — how long the shutdown drain may await in-flight
    /// flushes — is `max_wait + client_deadline`, capped at
    /// `DRAIN_DEADLINE_CAP` so the lifecycle stop budget stays under the
    /// host's per-gear shutdown deadline.
    #[must_use]
    pub fn limits(&self, client_deadline: std::time::Duration) -> IngestLimits {
        let to_usize = |v: u64| usize::try_from(v).unwrap_or(usize::MAX);
        let max_rows = to_usize(self.max_rows);
        let max_bytes = to_usize(self.max_bytes);
        let flushes = to_usize(self.max_concurrent_flushes);
        let max_wait = std::time::Duration::from_millis(self.max_wait_ms);
        IngestLimits {
            max_rows,
            max_bytes,
            max_wait,
            queue_capacity: to_usize(self.queue_capacity),
            max_concurrent_flushes: flushes,
            max_outstanding_rows: max_rows.saturating_mul(flushes.saturating_add(1)),
            max_outstanding_bytes: max_bytes.saturating_mul(flushes.saturating_add(1)),
            drain_deadline: max_wait
                .saturating_add(client_deadline)
                .min(DRAIN_DEADLINE_CAP),
        }
    }
}

/// Sharded-cluster topology for [`ClickHousePluginConfig::cluster`].
///
/// Every identifier here is interpolated into SQL (`ON CLUSTER <name>`,
/// `ALTER TABLE <local> …`), so [`ClickHousePluginConfig::validate`] admits
/// only plain `ClickHouse` identifiers.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClickHouseClusterConfig {
    /// Cluster name as declared in the server's `remote_servers` config.
    pub name: String,
    /// Shard-local `ReplicatedReplacingMergeTree` table behind the
    /// `usage_records` `Distributed` table. Must not be a name the plugin
    /// creates itself (`usage_records`, `usage_type_catalog`,
    /// `usage_collector_migrations`).
    pub records_local_table: String,
    /// Shard-local `ReplicatedReplacingMergeTree` table behind the
    /// `usage_type_catalog` `Distributed` table. Same reserved names as
    /// [`Self::records_local_table`].
    pub catalog_local_table: String,
    /// Optional `insert_quorum` sent with every insert: a replica count, or
    /// `auto` (majority). Unset leaves the server default (no quorum), so an
    /// insert is acknowledged once one replica of the target shard has it.
    pub insert_quorum: Option<String>,
    /// Run the cluster migrations (`ON CLUSTER` DDL) at startup instead of
    /// expecting an operator-provisioned schema. Off by default: the plugin's
    /// user then needs `CREATE` / `ALTER` and the `CLUSTER` privilege.
    pub manage_schema: bool,
    /// Budget for one cluster migration statement (the migrations and the TTL
    /// reconciliation), in seconds. `ON CLUSTER` DDL waits for every host, so
    /// it does not fit the request-path deadline. Sets the server-side
    /// `send_timeout`, `receive_timeout` and `distributed_ddl_task_timeout`
    /// (`migrator::migration_client`); the client-side deadline adds a grace.
    pub migration_statement_timeout_secs: u64,
}

impl Default for ClickHouseClusterConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            records_local_table: "usage_records_local".to_owned(),
            catalog_local_table: "usage_type_catalog_local".to_owned(),
            insert_quorum: None,
            manage_schema: false,
            migration_statement_timeout_secs: 3600,
        }
    }
}

/// Tables the plugin creates under fixed names, which a configured local
/// table must not reuse.
const RESERVED_TABLE_NAMES: [&str; 3] = [RECORDS_TABLE, CATALOG_TABLE, MIGRATIONS_TRACKER];

/// Whether `s` is a plain `ClickHouse` identifier (`[A-Za-z_][A-Za-z0-9_]*`),
/// safe to interpolate unquoted into SQL.
pub(crate) fn is_plain_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl ClickHouseClusterConfig {
    /// Client-side deadline for one cluster migration statement:
    /// [`Self::migration_statement_timeout_secs`] plus the same grace
    /// [`ClickHousePluginConfig::client_deadline`] adds, so the server's own
    /// timeout fires first.
    #[must_use]
    pub fn migration_deadline(&self) -> std::time::Duration {
        std::time::Duration::from_secs(
            self.migration_statement_timeout_secs
                .saturating_add(CLIENT_DEADLINE_GRACE_SECS),
        )
    }

    /// # Errors
    ///
    /// Returns an error string for an identifier that is not a plain
    /// `ClickHouse` identifier, two local tables sharing a name, a local table
    /// named after a table the plugin creates itself, an `insert_quorum` that is neither `auto` nor a positive integer, or a
    /// zero migration statement timeout.
    fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("cluster.name", &self.name),
            ("cluster.records_local_table", &self.records_local_table),
            ("cluster.catalog_local_table", &self.catalog_local_table),
        ] {
            if !is_plain_identifier(value) {
                return Err(format!(
                    "{field} = `{value}` is not a plain ClickHouse identifier \
                     ([A-Za-z_][A-Za-z0-9_]*); it is interpolated into SQL"
                ));
            }
        }
        if self.records_local_table == self.catalog_local_table {
            return Err(
                "cluster.records_local_table and cluster.catalog_local_table must differ"
                    .to_owned(),
            );
        }
        // Exact match: `ClickHouse` table names are case-sensitive.
        for (field, value) in [
            ("cluster.records_local_table", &self.records_local_table),
            ("cluster.catalog_local_table", &self.catalog_local_table),
        ] {
            if RESERVED_TABLE_NAMES.contains(&value.as_str()) {
                return Err(format!(
                    "{field} = `{value}` is reserved: the plugin creates a table of that name itself"
                ));
            }
        }
        if let Some(quorum) = &self.insert_quorum
            && quorum != "auto"
            && !quorum.parse::<u64>().is_ok_and(|n| n > 0)
        {
            return Err(format!(
                "cluster.insert_quorum = `{quorum}` must be `auto` or a positive replica count"
            ));
        }
        if self.migration_statement_timeout_secs == 0 {
            return Err(
                "cluster.migration_statement_timeout_secs must be greater than 0".to_owned(),
            );
        }
        Ok(())
    }
}

impl Default for ClickHousePluginConfig {
    fn default() -> Self {
        Self {
            database_url: SecretString::default(),
            allow_insecure_http: false,
            request_timeout_secs: 30,
            // Off by default: the plugin's own group commit (`insert_batch`,
            // on by default) is what coalesces a request-shaped ingest stream
            // into shared parts, and the two must not stack.
            async_insert: false,
            insert_batch: InsertBatchConfig::default(),
            // Same window the migration DDL bakes in, so a config-less start
            // needs no `MODIFY TTL` reconciliation at startup.
            retention_period_secs: crate::infra::storage::pool::DEFAULT_RETENTION_SECS,
            vendor: "constructorfabric".to_owned(),
            // One below the TimescaleDB plugin's default of 10, so with both
            // registered under default config the choice is deterministic
            // rather than decided by types-registry iteration order.
            priority: 11,
            cluster: None,
        }
    }
}

/// Upper bound on `retention_period_secs` (100 years in seconds).
///
/// `ClickHouse`'s TTL interval expression is evaluated as a `DateTime` offset;
/// a pathological retention window would overflow the `DateTime` type and
/// surface as a confusing DDL failure at schema-provisioning time. 100 years
/// is far beyond any realistic usage-data retention while staying safely inside
/// `ClickHouse`'s `DateTime64` range.
const MAX_RETENTION_SECS: u64 = 100 * 365 * 86_400;

/// Grace added to the server-side timeout to form the client-side deadline
/// (see [`ClickHousePluginConfig::client_deadline`]).
pub(crate) const CLIENT_DEADLINE_GRACE_SECS: u64 = 5;

/// Floor on `request_timeout_secs` while
/// [`ClickHousePluginConfig::async_insert`] is enabled.
///
/// `wait_for_async_insert = 1` blocks the `INSERT` until the server flushes
/// its async-insert buffer — up to `async_insert_busy_timeout_ms` (server
/// default; adaptive 50-200ms on `ClickHouse` 24.x+) plus the part commit. A 1s
/// server-side budget leaves under 800ms of headroom for the commit and
/// surfaces under load as intermittent `Transient` insert timeouts, which read
/// as backend flakiness rather than as the misconfiguration they are. Failing
/// at startup with both values named is strictly better.
///
/// The default `request_timeout_secs` of 30 clears this by more than an order
/// of magnitude; only a deliberately tiny configured budget trips it.
pub(crate) const MIN_ASYNC_INSERT_TIMEOUT_SECS: u64 = 2;

impl ClickHousePluginConfig {
    /// Client-side deadline for a single `ClickHouse` request.
    ///
    /// [`Self::request_timeout_secs`] is forwarded to `ClickHouse` as the
    /// server settings `send_timeout` / `receive_timeout`, which the *server*
    /// applies to its own socket handling. They therefore do nothing when a
    /// connection is accepted and then never answered, or when an intermediary
    /// holds the socket open — cases where a request would otherwise hang
    /// indefinitely. This deadline is the client-side backstop for exactly
    /// those cases.
    ///
    /// It sits [`CLIENT_DEADLINE_GRACE_SECS`] *past* the server-side budget so
    /// that when the server is answering, its own timeout fires first and the
    /// caller gets the server's descriptive error rather than a bare
    /// client-side timeout.
    pub(crate) fn client_deadline(&self) -> std::time::Duration {
        // `validate` bounds `request_timeout_secs` only from below (> 0), so a
        // pathologically large configured value must not overflow the add.
        std::time::Duration::from_secs(
            self.request_timeout_secs
                .saturating_add(CLIENT_DEADLINE_GRACE_SECS),
        )
    }
    /// The lifecycle stop budget: the drain deadline plus a second for the
    /// batcher to complete its waiters and return. Evaluated at stop by the
    /// gear's `stop_timeout_fn`.
    #[must_use]
    pub(crate) fn drain_stop_timeout(&self) -> std::time::Duration {
        self.insert_batch
            .limits(self.client_deadline())
            .drain_deadline
            .saturating_add(std::time::Duration::from_secs(1))
    }

    /// Validate invariants not expressible in the type system.
    ///
    /// # Errors
    ///
    /// Returns an error string for an empty `database_url`, one whose scheme is
    /// neither `http` nor `https`, one whose percent-encoded username or
    /// password does not decode to valid UTF-8 (see [`decode_userinfo`]), a
    /// plaintext `http` `database_url` without
    /// [`Self::allow_insecure_http`], a zero timeout, a
    /// `request_timeout_secs` below `MIN_ASYNC_INSERT_TIMEOUT_SECS` while
    /// [`Self::async_insert`] is enabled, a retention window outside
    /// `(0, MAX_RETENTION_SECS]`, a blank `vendor`, a cluster without
    /// `insert_batch.enabled`, `async_insert` together with
    /// `insert_batch.enabled` (two buffering layers), a zero `insert_batch`
    /// field, or an `insert_batch.max_wait_ms` above half of
    /// `request_timeout_secs`.
    pub fn validate(&self) -> Result<(), String> {
        if self.database_url.expose_secret().trim().is_empty() {
            return Err("database_url must not be empty".to_owned());
        }
        let parsed = match url::Url::parse(self.database_url.expose_secret()) {
            Ok(parsed) => parsed,
            Err(e) => return Err(format!("database_url must be a valid absolute URL: {e}")),
        };
        // The `clickhouse` crate speaks only ClickHouse's HTTP interface, so a
        // native-protocol or non-network scheme is a misconfiguration that would
        // otherwise surface as an opaque request failure on the first query.
        // Only the scheme is interpolated: `database_url` embeds credentials.
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(format!(
                "database_url scheme `{}` is not supported; the ClickHouse client speaks the \
                 HTTP interface only, so database_url must use https:// (or http:// with \
                 allow_insecure_http = true for local development/test)",
                parsed.scheme()
            ));
        }
        // Credentials embedded in the URL are percent-encoded and must decode
        // to valid UTF-8; otherwise `build_client` would either alter the secret
        // (lossy decode) or drop it, and the gear would start and then loop on
        // authentication failures. Only the component name and the decoder's
        // byte offset are reported — never the credential itself.
        // (An absent username is the empty string, which decodes trivially.)
        if let Err(e) = decode_userinfo(parsed.username()) {
            return Err(format!(
                "database_url username is not valid UTF-8 after percent-decoding ({e}); \
                 check the percent-encoding of the embedded credential"
            ));
        }
        if let Some(Err(e)) = parsed.password().map(decode_userinfo) {
            return Err(format!(
                "database_url password is not valid UTF-8 after percent-decoding ({e}); \
                 check the percent-encoding of the embedded credential"
            ));
        }
        if self.request_timeout_secs == 0 {
            return Err("request_timeout_secs must be > 0".to_owned());
        }
        // Ordered after the zero check so a zero value keeps the clearer error.
        // `wait_for_async_insert = 1` charges the server-side buffer flush
        // against the request budget, so too small a budget turns every insert
        // into an intermittent timeout that reads as backend flakiness.
        if self.async_insert && self.request_timeout_secs < MIN_ASYNC_INSERT_TIMEOUT_SECS {
            return Err(format!(
                "request_timeout_secs = {} is too small with async_insert = true: \
                 wait_for_async_insert = 1 blocks each INSERT until the server flushes its \
                 async-insert buffer (async_insert_busy_timeout_ms, server default), so the \
                 request budget must be at least {MIN_ASYNC_INSERT_TIMEOUT_SECS}s; raise \
                 request_timeout_secs or set async_insert = false",
                self.request_timeout_secs
            ));
        }
        if self.retention_period_secs == 0 {
            return Err("retention_period_secs must be > 0".to_owned());
        }
        if self.retention_period_secs > MAX_RETENTION_SECS {
            return Err(format!(
                "retention_period_secs must be <= {MAX_RETENTION_SECS} (100 years); \
                 a larger window overflows the ClickHouse DateTime64 TTL expression"
            ));
        }
        if self.vendor.trim().is_empty() {
            return Err(
                "vendor must not be empty; it is part of the GTS instance identity registered \
                 with types-registry (e.g. vendor = \"constructorfabric\")"
                    .to_owned(),
            );
        }
        // Fail closed on a plaintext database_url: it embeds credentials and
        // carries every usage record, so an unencrypted connection is a
        // credential- and data-exposure risk, not just a style choice. The
        // override must be set explicitly and is not implied by any other
        // field (e.g. a `http://` scheme alone is never sufficient consent).
        if is_plaintext_url(self.database_url.expose_secret()) && !self.allow_insecure_http {
            return Err(
                "database_url uses a plaintext http:// scheme, which sends credentials and \
                 usage data unencrypted; use https:// or set allow_insecure_http = true to \
                 explicitly opt out for local development/test only"
                    .to_owned(),
            );
        }
        if let Some(cluster) = &self.cluster {
            cluster.validate()?;
            // An async insert into a `Distributed` table is buffered on the
            // initiator, not on the shard that commits it, so neither the
            // `wait_for_async_insert` durability ack nor the insert dedup
            // token reaches the replicated table the way `async_insert`'s
            // contract assumes. Refuse the combination rather than weaken it.
            if self.async_insert {
                return Err(
                    "async_insert = true is not supported with a cluster topology: inserts go \
                     through Distributed tables synchronously (distributed_foreground_insert = 1) \
                     so the insert dedup token reaches the replicated shard table; set \
                     async_insert = false"
                        .to_owned(),
                );
            }
            // Through a `Distributed` table every INSERT is synchronous to the
            // owning shard, so the plugin's group commit is the only layer
            // that can turn a request-shaped stream into shared parts.
            if !self.insert_batch.enabled {
                return Err(
                    "a cluster topology requires insert_batch.enabled = true: inserts through \
                     Distributed tables are synchronous per shard and group commit is the only \
                     part-coalescing layer available"
                        .to_owned(),
                );
            }
        }
        // Server-side async inserts under the plugin's own group commit would
        // buffer twice; the flush path pins `async_insert = 0` and the
        // configuration must not disagree with it.
        if self.async_insert && self.insert_batch.enabled {
            return Err(
                "async_insert = true and insert_batch.enabled = true would stack two buffering \
                 layers; set insert_batch.enabled = false to keep server-side async inserts, or \
                 async_insert = false (the default) to use group commit"
                    .to_owned(),
            );
        }
        self.insert_batch.validate()?;
        // The queue wait is charged against the request budget; at least half
        // of it must stay for the flush INSERT itself.
        if self.insert_batch.max_wait_ms.saturating_mul(2)
            > self.request_timeout_secs.saturating_mul(1000)
        {
            return Err(format!(
                "insert_batch.max_wait_ms = {} must be at most half of request_timeout_secs = {} \
                 ({} ms): the queue wait and the flush INSERT share one request budget",
                self.insert_batch.max_wait_ms,
                self.request_timeout_secs,
                self.request_timeout_secs.saturating_mul(1000)
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "config_tests.rs"]
mod config_tests;
