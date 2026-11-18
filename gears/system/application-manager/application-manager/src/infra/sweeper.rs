//! Operation sweeper (DESIGN 3.6): on every replica, the expiry completion of
//! expired open operations every [`SWEEP_INTERVAL`], and the purge of
//! completed operations past `operation_retention` every [`PURGE_INTERVAL`],
//! each in batches of [`MAINTENANCE_BATCH_SIZE`].
//!
//! It calls no section plugin, never deletes an open record, and never
//! touches the history or the application records. A failed step is logged
//! and runs again at the next interval.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;

use crate::constants::{MAINTENANCE_BATCH_SIZE, PURGE_INTERVAL, SWEEP_INTERVAL};
use crate::domain::ports::OperationRepository;
use crate::domain::signals::Signals;

/// How often each sweeper step runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Intervals {
    /// Between expiry steps.
    pub sweep: Duration,
    /// Between purges.
    pub purge: Duration,
}

impl Default for Intervals {
    /// [`SWEEP_INTERVAL`] and [`PURGE_INTERVAL`].
    fn default() -> Self {
        Self {
            sweep: SWEEP_INTERVAL,
            purge: PURGE_INTERVAL,
        }
    }
}

/// The running sweeper.
#[derive(Debug)]
pub struct Sweeper {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

/// What one sweeper loop runs over.
struct Steps {
    repo: Arc<dyn OperationRepository>,
    retention: Duration,
    signals: Arc<dyn Signals>,
}

impl Sweeper {
    /// Start the sweeper; it runs until `cancel` or [`Self::stop`]. The
    /// first expiry step runs at once, the first purge after one purge
    /// interval.
    #[must_use]
    pub fn spawn(
        repo: Arc<dyn OperationRepository>,
        retention: Duration,
        signals: Arc<dyn Signals>,
        intervals: Intervals,
        cancel: CancellationToken,
    ) -> Self {
        let steps = Steps {
            repo,
            retention,
            signals,
        };
        let task = tokio::spawn(steps.run(intervals, cancel.clone()));
        Self { cancel, task }
    }

    /// Stop the sweeper and wait for it; abort it when `deadline` comes
    /// first.
    pub async fn stop(self, deadline: &CancellationToken) {
        self.cancel.cancel();
        let mut task = self.task;
        tokio::select! {
            _ = &mut task => {}
            () = deadline.cancelled() => {
                tracing::warn!("application-manager: sweeper aborted at the stop deadline");
                task.abort();
            }
        }
    }
}

impl Steps {
    async fn run(self, intervals: Intervals, cancel: CancellationToken) {
        let now = Instant::now();
        let mut sweep = interval_at(now, intervals.sweep);
        sweep.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut purge = interval_at(now + intervals.purge, intervals.purge);
        purge.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                _ = sweep.tick() => self.expire().await,
                _ = purge.tick() => self.purge().await,
            }
        }
    }

    async fn expire(&self) {
        match self.repo.expire_batch(MAINTENANCE_BATCH_SIZE).await {
            Ok(completed) => {
                self.signals.sweep("expire", true);
                if completed > 0 {
                    self.signals.operations_expired(completed);
                    tracing::info!(
                        completed,
                        "application-manager: expired operations completed"
                    );
                }
            }
            Err(error) => {
                self.signals.sweep("expire", false);
                self.signals.dependency_unavailable("storage");
                tracing::warn!(%error, "application-manager: expiry sweep failed");
            }
        }
    }

    async fn purge(&self) {
        match self
            .repo
            .purge(self.retention, MAINTENANCE_BATCH_SIZE)
            .await
        {
            Ok(deleted) => {
                self.signals.sweep("purge", true);
                if deleted > 0 {
                    tracing::info!(deleted, "application-manager: completed operations purged");
                }
            }
            Err(error) => {
                self.signals.sweep("purge", false);
                self.signals.dependency_unavailable("storage");
                tracing::warn!(%error, "application-manager: purge failed");
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "sweeper_tests.rs"]
mod tests;
