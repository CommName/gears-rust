use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use toolkit::Gear;
use toolkit::client_hub::ClientScope;
use toolkit::context::GearCtx;
use toolkit::gts::PluginV1;
use toolkit::lifecycle::ReadySignal;
use tracing::info;
use types_registry_sdk::{RegisterResult, TypesRegistryClient};
use usage_collector_sdk::{UsageCollectorPluginSpecV1, UsageCollectorPluginV1};

use crate::config::ClickHousePluginConfig;
use crate::domain::adapter::StorageAdapter;
use crate::domain::ports::{CatalogStore, RecordStore};
use crate::infra::metrics::Metrics;
use crate::infra::storage::catalog_store::ChCatalogStore;
use crate::infra::storage::ingest::IngestQueue;
use crate::infra::storage::ingest::sink::FlushSink;
use crate::infra::storage::migrator::{MigrationOptions, migration_client, run_migrations};
use crate::infra::storage::pool::{build_client, ensure_retention_ttl, validate_cluster_schema};
use crate::infra::storage::record_store::ChRecordStore;
use crate::infra::storage::topology::Topology;

/// Stop budget before `init` has run (nothing to drain).
const UNINITIALISED_STOP_TIMEOUT: Duration = Duration::from_secs(1);

/// What `serve` needs to run the group-commit queue: built in `init`.
struct IngestWiring {
    queue: Arc<IngestQueue>,
    sink: Arc<dyn FlushSink>,
    /// `ClickHousePluginConfig::drain_stop_timeout` for the configured limits.
    stop_timeout: Duration,
}

/// `ClickHouse` Usage Collector storage backend plugin module.
///
/// Conforms to the storage Plugin SPI: connects and migrates a `ClickHouse`
/// database, performs the full GTS registration handshake, then registers
/// the scoped `StorageAdapter` client so the plugin host resolves it on
/// first dispatch.
///
/// It is a stateful gear: its lifecycle entry (`serve`) runs the group-commit
/// ingest queue's batcher ([`crate::infra::storage::ingest`]) and drains it
/// on shutdown within the budget `drain_stop_timeout` returns. With
/// `insert_batch.enabled = false` the entry merely waits for cancellation.
///
/// Its only gear dependency is `types_registry` (for the registration
/// handshake); no cluster or coordination backend is required.
#[toolkit::gear(
    name = "clickhouse-usage-collector-plugin",
    deps = [types_registry],
    capabilities = [stateful],
    lifecycle(entry = "serve", stop_timeout_fn = "drain_stop_timeout", await_ready)
)]
#[derive(Default)]
pub struct ClickHouseUsageCollectorPlugin {
    /// `None` inside: group commit disabled. Unset: `init` has not run.
    ingest: OnceLock<Option<IngestWiring>>,
}

impl std::fmt::Debug for ClickHouseUsageCollectorPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseUsageCollectorPlugin")
            .field("initialised", &self.ingest.get().is_some())
            .field(
                "group_commit",
                &self.ingest.get().is_some_and(Option::is_some),
            )
            .finish()
    }
}

impl ClickHouseUsageCollectorPlugin {
    /// The graceful stop budget the lifecycle grants `serve` before aborting
    /// it: the queue's drain deadline plus a second, or a token second before
    /// `init` has wired anything. Evaluated at stop (`stop_timeout_fn`).
    #[must_use]
    pub fn drain_stop_timeout(&self) -> Duration {
        self.ingest
            .get()
            .and_then(Option::as_ref)
            .map_or(UNINITIALISED_STOP_TIMEOUT, |w| w.stop_timeout)
    }

    /// Lifecycle entry: run the group-commit batcher until `cancel` fires and
    /// its drain completes.
    ///
    /// `cancel` is a child of the runtime's root token, so it fires at process
    /// shutdown — before the host calls `stop` — and the batcher closes
    /// admission at that instant: a request still finishing its pre-checks
    /// then gets `Transient` from the queue, while everything already queued
    /// is flushed and awaited. If the drain outlives the stop budget the
    /// lifecycle aborts this task, and the batcher's guards complete the
    /// remaining waiters with `Transient`.
    ///
    /// # Errors
    ///
    /// Returns an error when called before `init` wired the gear.
    pub async fn serve(
        self: Arc<Self>,
        cancel: CancellationToken,
        ready: ReadySignal,
    ) -> anyhow::Result<()> {
        let wiring = self.ingest.get().ok_or_else(|| {
            anyhow::anyhow!("clickhouse-usage-collector-plugin: serve before init")
        })?;
        let Some(wiring) = wiring else {
            ready.notify();
            cancel.cancelled().await;
            return Ok(());
        };
        let run = Arc::clone(&wiring.queue).run(Arc::clone(&wiring.sink), cancel);
        ready.notify();
        let report = run.await;
        info!(
            outcome = ?report.outcome,
            flushed_calls = report.flushed_calls,
            "ClickHouse usage-collector ingest queue stopped"
        );
        Ok(())
    }
}

#[async_trait]
impl Gear for ClickHouseUsageCollectorPlugin {
    // @cpt-flow:cpt-cf-usage-collector-flow-foundation-plugin-host-binding:p1
    async fn init(&self, ctx: &GearCtx) -> anyhow::Result<()> {
        let cfg: ClickHousePluginConfig = ctx.config_expanded_or_default()?;
        cfg.validate().map_err(|e| {
            anyhow::anyhow!("invalid clickhouse-usage-collector-plugin config: {e}")
        })?;

        // Build the metric inventory once; shared via Arc across all stores.
        // The gear token is bound so the request path stops re-arming the
        // readiness gauge once shutdown begins (`Metrics::rearm_ready`).
        let cancel = ctx.cancellation_token().clone();
        let metrics = Arc::new(Metrics::new().with_shutdown(cancel.clone()));

        // Publish readiness as 0 before any startup I/O so an init that never
        // completes is distinguishable from a gear that never started at all
        // (no series). Flipped to 1 only after the full sequence below.
        metrics.set_ready(false);

        // --- Three-step init sequence ---

        // Steps A and B are raced against the gear's cancellation token so a
        // shutdown mid-startup aborts promptly instead of blocking on each
        // call's own `client_deadline`. The migration-failure counter stays
        // inside the raced block so it still fires on a provisioning error; a
        // cancellation drops the future and is not counted as a failure. The
        // block yields the `client` it built.
        let client_deadline = cfg.client_deadline();
        let topology = Topology::from_config(&cfg);
        let client = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(anyhow::anyhow!("init cancelled during shutdown"));
            }
            res = async {
                // Step A: Build the ClickHouse HTTP client and configure
                // timeouts / pool. Fallible: the transport is built from the
                // process-wide rustls CryptoProvider, which
                // `toolkit::bootstrap::init_procedure` installs before any
                // `Gear::init` runs. Absent one, fail rather than fall back to
                // the `clickhouse` crate's hardcoded provider.
                let client = build_client(&cfg)?;

                // Step B: Provision the schema. Every step is bounded by a
                // client-side deadline — the request path's, or the cluster
                // migration budget for `ON CLUSTER` DDL: a hung init is worse than a failed one, since it
                // never surfaces.
                match (&topology, &cfg.cluster) {
                    // Single node: run the pending versioned migrations, then
                    // reconcile usage_records TTL with the configured
                    // retention window.
                    (Topology::SingleNode, _) => {
                        let opts = MigrationOptions {
                            retention_period_secs: cfg.retention_period_secs,
                        };
                        run_migrations(&client, &topology, opts, client_deadline)
                            .await
                            .inspect_err(|_| metrics.inc_migration_failure())?;
                        ensure_retention_ttl(
                            &client,
                            &topology,
                            cfg.retention_period_secs,
                            client_deadline,
                        )
                        .await
                        .inspect_err(|_| metrics.inc_migration_failure())?;
                    }
                    // Cluster, plugin-managed schema: the same, `ON CLUSTER`,
                    // under the longer migration budget; validation stays the
                    // final gate.
                    (Topology::Cluster { .. }, Some(cluster)) if cluster.manage_schema => {
                        let opts = MigrationOptions {
                            retention_period_secs: cfg.retention_period_secs,
                        };
                        let deadline = cluster.migration_deadline();
                        let migration_client =
                            migration_client(&client, cluster.migration_statement_timeout_secs);
                        run_migrations(&migration_client, &topology, opts, deadline)
                            .await
                            .inspect_err(|_| metrics.inc_migration_failure())?;
                        ensure_retention_ttl(
                            &migration_client,
                            &topology,
                            cfg.retention_period_secs,
                            deadline,
                        )
                            .await
                            .inspect_err(|_| metrics.inc_migration_failure())?;
                        validate_cluster_schema(
                            &client,
                            &topology,
                            cfg.retention_period_secs,
                            client_deadline,
                        )
                        .await
                        .inspect_err(|_| metrics.inc_migration_failure())?;
                    }
                    // Cluster, operator-provisioned schema: only check it.
                    (Topology::Cluster { .. }, _) => {
                        validate_cluster_schema(
                            &client,
                            &topology,
                            cfg.retention_period_secs,
                            client_deadline,
                        )
                        .await
                        .inspect_err(|_| metrics.inc_migration_failure())?;
                    }
                }
                Ok::<_, anyhow::Error>(client)
            } => res?,
        };

        // Step C: Build the domain stores and wire them into the StorageAdapter.
        //
        // Both stores share the same ClickHouse client (cheaply cloneable handle
        // to the shared HTTP pool). The cancel token is threaded in so the
        // catalog-size refresh worker aborts on shutdown. The group-commit
        // queue is built here and run by `serve`; its write side is the record
        // store's own flush sink.
        let limits = cfg.insert_batch.limits(client_deadline);
        let queue = cfg
            .insert_batch
            .enabled
            .then(|| IngestQueue::new(limits, Arc::clone(&metrics)));
        let ch_record_store = ChRecordStore::new(
            client.clone(),
            Arc::clone(&metrics),
            client_deadline,
            cfg.async_insert,
            queue.clone(),
        );
        let wiring = queue.map(|queue| IngestWiring {
            queue,
            sink: ch_record_store.flush_sink(),
            stop_timeout: cfg.drain_stop_timeout(),
        });
        if let Some(w) = &wiring {
            info!(
                max_rows = limits.max_rows,
                max_bytes = limits.max_bytes,
                max_wait_ms = limits.max_wait.as_millis(),
                queue_capacity = limits.queue_capacity,
                max_concurrent_flushes = limits.max_concurrent_flushes,
                drain_deadline_secs = limits.drain_deadline.as_secs_f64(),
                stop_timeout_secs = w.stop_timeout.as_secs_f64(),
                "ClickHouse usage-collector group commit enabled"
            );
        }
        // Set before the adapter is published: a gear whose entry cannot find
        // its wiring must never expose a store that writes into a queue
        // nothing drains.
        self.ingest
            .set(wiring)
            .map_err(|_already| anyhow::anyhow!("init ran twice on one plugin instance"))?;
        let record_store: Arc<dyn RecordStore> = Arc::new(ch_record_store);

        let catalog_store: Arc<dyn CatalogStore> = Arc::new(ChCatalogStore::new(
            client,
            cancel.clone(),
            Arc::clone(&metrics),
            client_deadline,
            topology,
        ));

        // Construct the SPI adapter over the real stores.
        let service: Arc<dyn UsageCollectorPluginV1> =
            Arc::new(StorageAdapter::new(record_store, catalog_store));

        // --- Four-step GTS / types-registry / ClientHub handshake ---

        // Step 1: build registration payload for this plugin instance.
        let (instance_id, instance_json) =
            PluginV1::<UsageCollectorPluginSpecV1>::build_registration(
                "cf.core._.clickhouse_usage_collector.v1",
                cfg.vendor.clone(),
                cfg.priority,
            )?;

        // Step 2: publish to types-registry.
        let registry = ctx.client_hub().get::<dyn TypesRegistryClient>()?;
        let results = registry.register(vec![instance_json]).await?;

        // Step 3: ensure all registrations succeeded.
        RegisterResult::ensure_all_ok(&results)?;

        // Step 4: register the scoped backend client in ClientHub so the plugin
        // host resolves it on first dispatch.
        ctx.client_hub()
            .register_scoped::<dyn UsageCollectorPluginV1>(
                ClientScope::gts_id(&instance_id),
                service,
            );

        // Signal plugin-local readiness after a successful init. From here on
        // the request path owns the gauge's live value: `tracked_ch_err`
        // clears it on a connectivity-class error and `with_deadline` re-arms
        // it on the next successful round-trip (`Metrics::clear_ready` /
        // `Metrics::rearm_ready`); the background catalog-size refresh worker
        // never touches it. The Gear trait exposes no shutdown hook, so the
        // cancellation token is the only shutdown signal: a detached watcher
        // clears the gauge instead of leaving it stuck at 1 for a drained
        // replica, and `rearm_ready` is a no-op once the token has fired.
        // Spawned after `set_ready(true)` so a cancellation that already fired
        // is still observed — `cancelled()` resolves immediately on a
        // cancelled token.
        //
        // The watcher deliberately does not `remove_scoped` the ClientHub
        // registration: the gear token is a child of the process root token
        // and only fires at process shutdown, ClientHub is process-local
        // memory that dies with it, and pulling the entry mid-drain would fail
        // in-flight requests the drain exists to finish. The reference plugin
        // behaves the same way.
        metrics.set_ready(true);
        let ready_metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            cancel.cancelled().await;
            ready_metrics.set_ready(false);
        });

        info!(
            instance_id = %instance_id,
            vendor = %cfg.vendor,
            priority = cfg.priority,
            "Registered ClickHouse usage-collector plugin instance"
        );
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gear_tests.rs"]
mod gear_tests;
