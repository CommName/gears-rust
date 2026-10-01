// These fixtures panic on invalid test input by design, and not every module
// of the binary uses every fixture.
#![allow(dead_code, clippy::expect_used, clippy::unwrap_used)]
//! Shared `ClickHouse` test harness.
//!
//! ## One container, a database per test
//!
//! A `ClickHouse` server takes ~5s to start and a test body ~20ms, so — like the
//! k8s plugin's k3s fixture — this harness starts **one** container per test
//! binary behind a [`tokio::sync::OnceCell`] and isolates every test by its own
//! database instead: [`bring_up`] creates `uc_<uuid>` and points the client at
//! it. The migration DDL names no database and every query is unqualified, so
//! each test sees only its own tables. Helpers reading server-wide system
//! tables (`system.query_log`) filter on `currentDatabase()`.
//!
//! Only the container and its port are shared; **clients are built per test**,
//! because a `clickhouse::Client`'s connection pool is bound to the tokio
//! runtime that created it and each `#[tokio::test]` runs on its own runtime.
//!
//! ## Cleanup, however the process ends
//!
//! The container lives in a `static`, so it is never dropped and outlives the
//! test process — even on a clean exit. `testcontainers` 0.27 ships no reaper,
//! so the harness starts a `Ryuk` sidecar first, registers this session's label
//! with it over TCP, and holds that connection for the life of the process.
//! When the process ends for any reason — exit, panic, abort, `SIGKILL`, the
//! OOM killer — the OS closes the socket and `Ryuk` removes the labelled
//! container ~10s later, then exits (it runs with `AutoRemove`). Every process
//! has its own session label, so a pipeline running in parallel on the same
//! Docker daemon is never touched.
//!
//! `Ryuk` mounts the Docker socket — the same root-equivalent access the test
//! process already uses to start containers. Where that mount is forbidden,
//! `GEARS_TEST_RYUK_DISABLED=1` skips it; a leaked container is then removed by
//! [`reap_stale_containers`] at the next start (its owner process is gone, or
//! it is older than two hours). `GEARS_TEST_DOCKER_SOCKET` overrides the socket
//! path mounted into `Ryuk` (default: `DOCKER_HOST`'s `unix://` path, else
//! `/var/run/docker.sock`).
//!
//! ## Memory
//!
//! The container is capped at `GEARS_TEST_CH_MEMORY_MIB` (default 2048; `0`
//! disables the cap). `ClickHouse` reads its cgroup limit and sizes
//! `max_server_memory_usage` to 90% of it, so the server stays inside its share
//! of a shared runner rather than assuming it owns the host.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rust_decimal::Decimal;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use time::OffsetDateTime;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use usage_collector_sdk::{
    IdempotencyKey, MetadataKey, ResourceRef, SubjectRef, UsageKind, UsageRecord, UsageType,
    UsageTypeGtsId, derive_usage_record_id,
};

use clickhouse_usage_collector_plugin::config::ClickHousePluginConfig;
use clickhouse_usage_collector_plugin::infra::metrics::Metrics;
use clickhouse_usage_collector_plugin::infra::storage::catalog_store::ChCatalogStore;
use clickhouse_usage_collector_plugin::infra::storage::pool::{
    apply_migrations, build_client, ensure_insert_dedup_window, ensure_retention_ttl,
};
use clickhouse_usage_collector_plugin::infra::storage::record_store::ChRecordStore;

/// Per-test handle: a client scoped to the test's own database on the shared
/// `ClickHouse` server.
pub struct ChHarness {
    /// Client pointed at [`Self::database`] on the shared container.
    pub client: clickhouse::Client,
    /// Cancellation token for background workers spawned from this harness.
    pub cancel: CancellationToken,
    /// The database this test owns; nothing else reads or writes it.
    pub database: String,
}

/// Password for the container's `default` user, exposed so tests can assert it
/// never leaks (e.g. through a `Debug` impl).
///
/// MUST be non-empty. The official image's entrypoint only provisions
/// `default` with `<networks><ip>::/0</ip></networks>` when `CLICKHOUSE_USER`
/// is non-default **or** `CLICKHOUSE_PASSWORD` is non-empty; otherwise it
/// writes a `users.d` override restricting `default` to `127.0.0.1`/`::1`,
/// which rejects every connection arriving through the mapped host port.
pub const CH_TEST_PASSWORD: &str = "ch_test_pw";

/// Label every container this harness starts carries, so cleanup never
/// touches an unrelated container.
const FIXTURE_LABEL_KEY: &str = "org.cf-gears.test-fixture";
/// Value of [`FIXTURE_LABEL_KEY`] for this plugin's `ClickHouse` server.
const FIXTURE_LABEL_VALUE: &str = "cf-gears-clickhouse-usage-collector-plugin";
/// Value of [`FIXTURE_LABEL_KEY`] for this plugin's `Ryuk` sidecar. Distinct
/// from [`FIXTURE_LABEL_VALUE`], so the stale reaper never removes a live
/// sidecar (each one exits on its own once its connection closes).
const REAPER_LABEL_VALUE: &str = "cf-gears-clickhouse-usage-collector-plugin-ryuk";
/// Label naming the test process's session; the filter registered with `Ryuk`.
const SESSION_LABEL_KEY: &str = "org.cf-gears.test-session";
/// Label recording `<hostname>:<pid>` of the process that started the server.
const OWNER_LABEL_KEY: &str = "org.cf-gears.test-owner";
/// Label recording the unix second the server was started.
const STARTED_LABEL_KEY: &str = "org.cf-gears.test-started";

/// Set to a truthy value to skip the `Ryuk` sidecar.
const ENV_RYUK_DISABLED: &str = "GEARS_TEST_RYUK_DISABLED";
/// Host path of the Docker socket mounted into `Ryuk`.
const ENV_DOCKER_SOCKET: &str = "GEARS_TEST_DOCKER_SOCKET";
/// Memory cap of the `ClickHouse` container in MiB; `0` disables it.
const ENV_CH_MEMORY_MIB: &str = "GEARS_TEST_CH_MEMORY_MIB";
/// Default for [`ENV_CH_MEMORY_MIB`].
const DEFAULT_CH_MEMORY_MIB: u64 = 2048;
/// Age past which a fixture container is treated as leaked regardless of its
/// owner. The suite runs in about a minute, so no live run's server is this old.
const STALE_AFTER_SECS: u64 = 2 * 60 * 60;
/// Seconds `Ryuk` waits after its last connection closes before reaping.
const RYUK_RECONNECTION_TIMEOUT: &str = "10s";

/// The process-wide `ClickHouse` server every test of this binary shares.
struct SharedCh {
    /// Mapped host port of the server's HTTP interface.
    port: u16,
    /// Kept alive for the process lifetime; never dropped (see module docs).
    _container: ContainerAsync<GenericImage>,
    /// `None` when [`ENV_RYUK_DISABLED`] is set.
    _reaper: Option<Reaper>,
}

/// A `Ryuk` sidecar and the connection that keeps it from reaping.
struct Reaper {
    _container: ContainerAsync<GenericImage>,
    _connection: TcpStream,
}

/// The shared server, or the reason it could not be started. A failure is
/// cached so an unavailable Docker costs one attempt, not one per test.
static SHARED: OnceCell<Result<SharedCh, String>> = OnceCell::const_new();

/// Start the shared server on first use and return its host port.
async fn shared_port() -> anyhow::Result<u16> {
    SHARED
        .get_or_init(|| async { start_shared().await.map_err(|e| format!("{e:#}")) })
        .await
        .as_ref()
        .map(|shared| shared.port)
        .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn start_shared() -> anyhow::Result<SharedCh> {
    reap_stale_containers();

    let session = Uuid::new_v4().to_string();
    // The reaper is registered before the server exists, so there is no
    // window in which a killed process leaves an unregistered container.
    let reaper = if flag_set(ENV_RYUK_DISABLED) {
        eprintln!(
            "{ENV_RYUK_DISABLED} is set: the shared ClickHouse container is not reaped on \
             exit; the next run's stale-container sweep removes it"
        );
        None
    } else {
        Some(start_reaper(&session).await?)
    };

    // No log-based wait strategy: this image sends the server log (including
    // "Ready for connections") to files under /var/log/clickhouse-server
    // inside the container, so it never appears on stdout/stderr and a
    // `message_on_stdout` wait can only ever time out. Readiness is polled
    // over HTTP below instead.
    //
    // Image and tag come from `test_containers`, never a local literal:
    // `cargo xtask check-test-container-pins` enforces it, and the pin is
    // mirrored into `ClickHouseSidecar` in `testing/e2e/lib/sidecars.py`.
    let mut request = test_containers::clickhouse()
        .with_wait_for(WaitFor::Nothing)
        .with_env_var("CLICKHOUSE_USER", "default")
        .with_env_var("CLICKHOUSE_PASSWORD", CH_TEST_PASSWORD)
        .with_env_var("CLICKHOUSE_DB", "default")
        .with_label(FIXTURE_LABEL_KEY, FIXTURE_LABEL_VALUE)
        .with_label(SESSION_LABEL_KEY, session.as_str())
        .with_label(
            OWNER_LABEL_KEY,
            format!("{}:{}", hostname(), std::process::id()),
        )
        .with_label(STARTED_LABEL_KEY, unix_now().to_string());
    if let Some(bytes) = memory_limit_bytes() {
        // Swap equal to memory disables swap, so a server outgrowing its cap
        // fails visibly instead of crawling.
        request = request.with_host_config_modifier(move |hc| {
            hc.memory = Some(bytes);
            hc.memory_swap = Some(bytes);
        });
    }
    let container = request.start().await?;
    let port = container.get_host_port_ipv4(8123).await?;

    let (_, client) = client_for(port, "default")?;
    wait_until_ready(&client).await?;

    Ok(SharedCh {
        port,
        _container: container,
        _reaper: reaper,
    })
}

/// Start `Ryuk` and register `session`'s label with it.
async fn start_reaper(session: &str) -> anyhow::Result<Reaper> {
    let socket = docker_socket();
    let container = test_containers::ryuk()
        .with_exposed_port(8080.tcp())
        .with_wait_for(WaitFor::Nothing)
        .with_mount(Mount::bind_mount(socket.as_str(), "/var/run/docker.sock"))
        .with_env_var("RYUK_RECONNECTION_TIMEOUT", RYUK_RECONNECTION_TIMEOUT)
        .with_label(FIXTURE_LABEL_KEY, REAPER_LABEL_VALUE)
        .with_host_config_modifier(|hc| hc.auto_remove = Some(true))
        .start()
        .await
        .map_err(|e| reaper_error(&socket, &e))?;
    let port = container
        .get_host_port_ipv4(8080)
        .await
        .map_err(|e| reaper_error(&socket, &e))?;

    let filter = format!("label={SESSION_LABEL_KEY}={session}\n");
    let mut last_err = None;
    // Ryuk needs a moment to listen after the container starts.
    for _ in 0..50u8 {
        match register_with_reaper(port, &filter) {
            Ok(connection) => {
                return Ok(Reaper {
                    _container: container,
                    _connection: connection,
                });
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    let e = last_err.map_or_else(|| "no attempt made".to_owned(), |e| e.to_string());
    Err(reaper_error(&socket, &e))
}

/// One registration attempt: connect, send the label filter, await `ACK`.
fn register_with_reaper(port: u16, filter: &str) -> std::io::Result<TcpStream> {
    let mut connection = TcpStream::connect(("127.0.0.1", port))?;
    connection.set_read_timeout(Some(Duration::from_secs(5)))?;
    connection.write_all(filter.as_bytes())?;
    let mut reply = String::new();
    BufReader::new(connection.try_clone()?).read_line(&mut reply)?;
    if reply.trim() == "ACK" {
        // The connection must stay open for the process lifetime; reads
        // never happen again, so the timeout is irrelevant from here on.
        Ok(connection)
    } else {
        Err(std::io::Error::other(format!(
            "unexpected Ryuk reply {reply:?}"
        )))
    }
}

fn reaper_error(socket: &str, cause: &dyn std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!(
        "the Ryuk reaper did not start or acknowledge (Docker socket {socket}): {cause}\n\
         Set {ENV_DOCKER_SOCKET} to the daemon's socket path, or {ENV_RYUK_DISABLED}=1 where \
         mounting the socket is not allowed"
    )
}

/// Host path of the Docker socket to mount into `Ryuk`.
fn docker_socket() -> String {
    if let Some(path) = env_non_empty(ENV_DOCKER_SOCKET) {
        return path;
    }
    if let Some(path) = env_non_empty("DOCKER_HOST")
        .as_deref()
        .and_then(|host| host.strip_prefix("unix://"))
    {
        return path.to_owned();
    }
    "/var/run/docker.sock".to_owned()
}

/// The container's memory cap in bytes, or `None` for no cap.
fn memory_limit_bytes() -> Option<i64> {
    let mib = env_non_empty(ENV_CH_MEMORY_MIB).map_or(DEFAULT_CH_MEMORY_MIB, |raw| {
        raw.trim().parse::<u64>().unwrap_or_else(|_| {
            panic!("{ENV_CH_MEMORY_MIB} must be a whole number of MiB (0 = no limit), got {raw:?}")
        })
    });
    (mib > 0).then(|| {
        i64::try_from(mib.saturating_mul(1024 * 1024))
            .unwrap_or_else(|_| panic!("{ENV_CH_MEMORY_MIB} = {mib} MiB does not fit in i64 bytes"))
    })
}

/// Remove fixture servers leaked by earlier runs: those whose owner process on
/// this host is gone, and any older than [`STALE_AFTER_SECS`].
///
/// Best-effort — a missing or slow Docker is not this sweep's failure (the
/// `start()` that follows reports it). Never removes every fixture container
/// unconditionally: on a shared Docker daemon another pipeline's live server
/// carries the same fixture label.
fn reap_stale_containers() {
    let format = format!(
        "{{{{.ID}}}}\t{{{{.Label \"{OWNER_LABEL_KEY}\"}}}}\t{{{{.Label \"{STARTED_LABEL_KEY}\"}}}}"
    );
    let Ok(out) = std::process::Command::new("docker")
        .args(["ps", "-a", "--no-trunc", "--filter"])
        .arg(format!("label={FIXTURE_LABEL_KEY}={FIXTURE_LABEL_VALUE}"))
        .args(["--format", &format])
        .output()
    else {
        return;
    };
    let me = hostname();
    let now = unix_now();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let mut fields = line.split('\t');
        let (Some(id), Some(owner), Some(started)) = (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        let orphaned = owner
            .rsplit_once(':')
            .is_some_and(|(host, pid)| host == me && !pid_alive(pid));
        let expired = started
            .parse::<u64>()
            .is_ok_and(|started| now.saturating_sub(started) > STALE_AFTER_SECS);
        if orphaned || expired {
            let _removed = std::process::Command::new("docker")
                .args(["rm", "-f", id])
                .output();
        }
    }
}

/// Whether process `pid` exists on this host.
fn pid_alive(pid: &str) -> bool {
    if std::path::Path::new("/proc/self").exists() {
        return std::path::Path::new("/proc").join(pid).exists();
    }
    std::process::Command::new("kill")
        .args(["-0", pid])
        .output()
        .is_ok_and(|out| out.status.success())
}

/// This host's name, as seen from the test process.
fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
        })
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown-host".to_owned())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

fn env_non_empty(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|value| !value.is_empty())
}

fn flag_set(var: &str) -> bool {
    env_non_empty(var).is_some_and(|value| {
        !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        )
    })
}

/// The plugin config and client for `database` on the shared server.
fn client_for(
    port: u16,
    database: &str,
) -> anyhow::Result<(ClickHousePluginConfig, clickhouse::Client)> {
    let cfg: ClickHousePluginConfig = serde_json::from_str(&format!(
        r#"{{ "database_url": "http://default:{CH_TEST_PASSWORD}@127.0.0.1:{port}/{database}",
              "allow_insecure_http": true }}"#
    ))
    .expect("valid test config json");

    // `build_client` fails closed when no rustls `CryptoProvider` is installed
    // process-wide (see `pool.rs::new_base_client`). Production installs one in
    // `toolkit::bootstrap::init_procedure`; this harness does not go through
    // bootstrap, so it installs one itself. The harness talks plain `http://`,
    // so the provider is never exercised — the lookup just has to succeed.
    //
    // Idempotent and race-safe: `install_default` is backed by a process-wide
    // `OnceLock`, so a second call returns `Err`, which is deliberately
    // dropped. `drop` rather than `let _ =` satisfies
    // `clippy::let_underscore_must_use`.
    drop(rustls::crypto::aws_lc_rs::default_provider().install_default());

    let client = build_client(&cfg)?;
    Ok((cfg, client))
}

/// Create a fresh database on the shared server, apply the schema migration
/// to it, and return a harness scoped to it.
pub async fn bring_up() -> anyhow::Result<ChHarness> {
    let port = shared_port().await?;
    let database = format!("uc_{}", Uuid::new_v4().simple());

    let (_, admin) = client_for(port, "default")?;
    tokio::time::timeout(
        TEST_REQUEST_TIMEOUT,
        admin
            .query(&format!("CREATE DATABASE `{database}`"))
            .execute(),
    )
    .await
    .map_err(|_elapsed| anyhow::anyhow!("CREATE DATABASE {database} timed out"))??;

    let (cfg, client) = client_for(port, &database)?;
    apply_migrations(&client, TEST_REQUEST_TIMEOUT).await?;
    ensure_retention_ttl(&client, cfg.retention_period_secs, TEST_REQUEST_TIMEOUT).await?;
    ensure_insert_dedup_window(&client, TEST_REQUEST_TIMEOUT).await?;

    Ok(ChHarness {
        client,
        cancel: CancellationToken::new(),
        database,
    })
}

/// Poll `SELECT 1` until the server answers, or give up after ~60s.
async fn wait_until_ready(client: &clickhouse::Client) -> anyhow::Result<()> {
    let mut last_err = None;
    for _ in 0..120u8 {
        match client.query("SELECT 1").fetch_one::<u8>().await {
            Ok(_) => return Ok(()),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    Err(anyhow::anyhow!(
        "ClickHouse container never became ready: {}",
        last_err.map_or_else(|| "no error recorded".to_owned(), |e| e.to_string())
    ))
}

/// Bring up the harness, or print a Docker-unavailable notice and return
/// `None` for the caller to skip its own test body.
///
/// Skipping is silent to the test harness (the test still reports `ok`), which
/// makes a Docker-less run indistinguishable from a real one — including in a
/// coverage report, where every gated line stays red while the suite claims
/// success. Set `CH_REQUIRE_DOCKER=1` to turn a failed bring-up into a panic
/// instead; coverage runs MUST set it.
pub async fn bring_up_or_skip() -> Option<ChHarness> {
    match bring_up().await {
        Ok(h) => Some(h),
        Err(e) => {
            assert!(
                !std::env::var("CH_REQUIRE_DOCKER").is_ok_and(|v| v == "1"),
                "CH_REQUIRE_DOCKER=1 but the ClickHouse test harness failed to start: {e}"
            );
            eprintln!(
                "DOCKER UNAVAILABLE — skipping test (bring_up failed): {e}\n\
                 Run `cargo test -p cf-gears-clickhouse-usage-collector-plugin \
                 --features clickhouse` with Docker available to execute these tests."
            );
            None
        }
    }
}

/// Build a fresh metric inventory (recording is a no-op without an exporter).
#[must_use]
pub fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new())
}

/// Client-side per-request deadline for stores built by these helpers.
///
/// Generous relative to anything the live suites do, so it stays a backstop
/// against a hang rather than something an assertion can trip over.
pub const TEST_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Build a [`ChRecordStore`] with its own metric handle.
#[must_use]
pub fn record_store(h: &ChHarness) -> ChRecordStore {
    record_store_over(h, h.client.clone())
}

/// Same as [`record_store`], but over a caller-supplied `ClickHouse` client
/// (e.g. [`unreachable_client`]).
///
/// `async_insert` is `true` — the production default — so this tier exercises
/// the shipped write path, including its read-your-writes dependency on
/// `wait_for_async_insert = 1`.
#[must_use]
pub fn record_store_over(_h: &ChHarness, client: clickhouse::Client) -> ChRecordStore {
    ChRecordStore::new(client, metrics(), TEST_REQUEST_TIMEOUT, true)
}

/// Same as [`record_store_over`], but sharing a caller-supplied metric
/// inventory, so a test can assert what the store did to a gauge (e.g.
/// `uc_clickhouse_ready`) across several stores over one series.
#[must_use]
pub fn record_store_with_metrics(
    client: clickhouse::Client,
    metrics: Arc<Metrics>,
) -> ChRecordStore {
    ChRecordStore::new(client, metrics, TEST_REQUEST_TIMEOUT, true)
}

/// Same as [`record_store`], but with `async_insert = false`.
///
/// On the shipped non-replicated `ReplacingMergeTree`, `ClickHouse` enforces
/// `insert_deduplication_token` on synchronous inserts only, so this is the
/// tier that proves the engine-side dedup of racing single-record creates
/// deterministically. The async tier relies on `optimize_on_insert` coalescing
/// within one flush and on merges otherwise (see `config.rs`).
#[must_use]
pub fn record_store_sync(h: &ChHarness) -> ChRecordStore {
    ChRecordStore::new(h.client.clone(), metrics(), TEST_REQUEST_TIMEOUT, false)
}

/// Stop background merges on this test's `usage_records` for the rest of the
/// container's life, so a test asserting "before any merge runs" is guaranteed
/// rather than probable. Scoped to the test's own database, so the other tests
/// sharing the server keep merging.
pub async fn stop_merges(h: &ChHarness) {
    h.client
        .query(&format!(
            "SYSTEM STOP MERGES `{}`.usage_records",
            h.database
        ))
        .execute()
        .await
        .expect("SYSTEM STOP MERGES must succeed");
}

/// Raw physical row count for one `id`, with no version resolution and no
/// marker anti-join — what the engine actually stores.
pub async fn raw_rows_for_id(h: &ChHarness, id: Uuid) -> u64 {
    h.client
        .query("SELECT count() FROM usage_records WHERE id = ?")
        .bind(id.to_string())
        .fetch_one::<u64>()
        .await
        .expect("raw count must be readable")
}

/// Build a [`ChCatalogStore`] with its own metric handle.
#[must_use]
pub fn catalog_store(h: &ChHarness) -> ChCatalogStore {
    catalog_store_over(h, h.client.clone())
}

/// Same as [`catalog_store`], but over a caller-supplied `ClickHouse` client
/// (e.g. [`unreachable_client`]).
#[must_use]
pub fn catalog_store_over(h: &ChHarness, client: clickhouse::Client) -> ChCatalogStore {
    ChCatalogStore::new(client, h.cancel.clone(), metrics(), TEST_REQUEST_TIMEOUT)
}

/// A client pointed at a port with nothing listening, so every statement fails
/// fast with a connection error instead of hanging.
#[must_use]
pub fn unreachable_client() -> clickhouse::Client {
    // Port 1 is reserved and never bound by the test harness.
    clickhouse::Client::default().with_url("http://127.0.0.1:1")
}

/// Process-wide base instant (unix seconds) for fixture `created_at` values.
///
/// Anchored to the current clock, **not** a hardcoded epoch: `usage_records`
/// carries `TTL created_at + INTERVAL retention_period_secs SECOND DELETE`
/// (365 days by default), so a fixture timestamp older than the retention
/// window makes every inserted row immediately TTL-expired — a background
/// merge then drops it mid-test, and
/// any reference/aggregation assertion fails depending on timing. A
/// hardcoded epoch works until it ages past the window and then rots the
/// whole suite.
///
/// Resolved once per process so it stays deterministic within a run: the dedup
/// tests build two fixtures independently and rely on them sharing the same
/// `(tenant_id, gts_id, created_at, idempotency_key)` key. Offset well into the past so
/// callers adding per-record offsets (`base + i`) stay in the past too.
#[must_use]
pub fn fixture_base_ts() -> i64 {
    static BASE: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *BASE.get_or_init(|| {
        OffsetDateTime::now_utc()
            .unix_timestamp()
            .saturating_sub(48 * 60 * 60)
    })
}

/// Build a valid [`UsageTypeGtsId`] from a raw string.
#[must_use]
pub fn fixture_gts_id(gts: &str) -> UsageTypeGtsId {
    UsageTypeGtsId::new(gts).expect("fixture gts_id must be a valid usage-type GTS instance id")
}

/// Build a [`UsageType`] fixture from raw parts.
#[must_use]
pub fn fixture_usage_type(gts: &str, kind: &str, fields: &[&str]) -> UsageType {
    let kind: UsageKind = kind.parse().expect("fixture kind must be counter/gauge");
    let metadata_fields = fields
        .iter()
        .map(|f| MetadataKey::new(*f).expect("fixture metadata field must be valid"))
        .collect();
    UsageType {
        gts_id: fixture_gts_id(gts),
        kind,
        metadata_fields,
    }
}

/// The default fixture event instant, [`fixture_base_ts`] as an
/// [`OffsetDateTime`].
///
/// Pass this to [`fixture_usage_record`] when the test does not care about the
/// timestamp, and [`fixture_created_at_offset`] when it needs distinct instants.
#[must_use]
pub fn fixture_created_at() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(fixture_base_ts())
        .expect("fixture created_at must be a valid unix timestamp")
}

/// [`fixture_created_at`] shifted by `offset_secs`, for tests that need several
/// records at distinct instants.
#[must_use]
pub fn fixture_created_at_offset(offset_secs: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(fixture_base_ts() + offset_secs)
        .expect("fixture created_at must be a valid unix timestamp")
}

/// Build a minimal [`UsageRecord`] fixture at the default [`fixture_created_at`]
/// instant.
///
/// Use [`fixture_usage_record_at`] when the test needs a specific event time —
/// never set `created_at` on the returned record, see that function.
#[must_use]
pub fn fixture_usage_record(gts: &str, tenant_id: Uuid, idem: &str, value: Decimal) -> UsageRecord {
    fixture_usage_record_at(gts, tenant_id, idem, value, fixture_created_at())
}

/// Build a minimal [`UsageRecord`] fixture referencing `gts_id` at `created_at`.
///
/// `id` is derived with [`derive_usage_record_id`], exactly as the gateway
/// stamps it on every dispatch (ADR-0013 / ADR-0014) — `CreateUsageRecordRequest`
/// carries no identity field, so a derived id is the only shape this plugin can
/// ever receive. A synthetic id would let a test pass while the real dedup
/// identity is broken: the dedup lookup keys on the canonical tuple and then
/// compares the stored `id` against the incoming one, so a hand-forged id would
/// read as a corrupted stored row rather than as an exact retry.
///
/// `created_at` is a parameter rather than a default the caller overwrites
/// afterwards, because it is one of the four derivation inputs: mutating it on
/// the returned record would leave a stale `id` behind. The fields tests do
/// mutate (`value`, `metadata`, `corrects_id`, `resource_ref`, `subject_ref`)
/// are not derivation inputs, so they stay safe to set post-construction.
#[must_use]
pub fn fixture_usage_record_at(
    gts: &str,
    tenant_id: Uuid,
    idem: &str,
    value: Decimal,
    created_at: OffsetDateTime,
) -> UsageRecord {
    let gts_id = fixture_gts_id(gts);
    let idempotency_key = IdempotencyKey::new(idem).expect("fixture idempotency_key must be valid");
    UsageRecord {
        id: derive_usage_record_id(tenant_id, &gts_id, &idempotency_key, created_at),
        gts_id,
        tenant_id,
        resource_ref: ResourceRef::new("res-1", "compute.vm")
            .expect("fixture resource_ref must be valid"),
        subject_ref: None,
        metadata: std::collections::BTreeMap::new(),
        value,
        idempotency_key,
        corrects_id: None,
        status: usage_collector_sdk::UsageRecordStatus::Active,
        created_at,
    }
}

/// Build a [`UsageRecord`] fixture with a caller-chosen `resource_id` at
/// `created_at`.
#[must_use]
pub fn fixture_usage_record_with_resource_at(
    gts: &str,
    tenant_id: Uuid,
    idem: &str,
    value: Decimal,
    created_at: OffsetDateTime,
    resource_id: &str,
) -> UsageRecord {
    let mut rec = fixture_usage_record_at(gts, tenant_id, idem, value, created_at);
    rec.resource_ref =
        ResourceRef::new(resource_id, "compute.vm").expect("fixture resource_ref must be valid");
    rec
}

/// Build a [`UsageRecord`] fixture carrying a `subject_ref`.
#[must_use]
pub fn fixture_usage_record_with_subject(
    gts: &str,
    tenant_id: Uuid,
    idem: &str,
    value: Decimal,
    subject_id: &str,
    subject_type: Option<&str>,
) -> UsageRecord {
    let mut rec = fixture_usage_record(gts, tenant_id, idem, value);
    rec.subject_ref =
        Some(SubjectRef::new(subject_id, subject_type).expect("fixture subject_ref must be valid"));
    rec
}
