//! The write side the batcher flushes through.

use std::sync::Arc;

use async_trait::async_trait;
use usage_collector_sdk::UsageCollectorPluginError;

use crate::infra::storage::entity::UsageRecordRow;
pub use crate::infra::storage::error::FailureKind;

/// A failed flush: the plugin error every affected row reports, plus the
/// classification the batcher's retry / isolation policy is decided on. The
/// classification is made where the `ClickHouse` error is still available
/// (`error::classify`); the plugin error carries no server code.
#[derive(Debug)]
pub struct FlushError {
    /// Retry / isolate / fan out.
    pub kind: FailureKind,
    /// The error reported to the rows' waiters.
    pub error: UsageCollectorPluginError,
}

/// One synchronous `INSERT` of exactly `rows`, in that order, under `token`.
///
/// Implemented by the record store over the `ClickHouse` client; a fake in
/// the unit tests. The sink must not reorder, split or deduplicate rows: the
/// token the batcher computed describes this exact set.
#[async_trait]
pub trait FlushSink: Send + Sync + 'static {
    /// Write `rows` as one statement with `insert_deduplication_token = token`.
    ///
    /// # Errors
    ///
    /// A [`FlushError`] classifying the failure for the batcher.
    async fn insert(&self, rows: &[Arc<UsageRecordRow>], token: &str) -> Result<(), FlushError>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
pub(crate) mod test_support {
    //! A scripted in-memory [`FlushSink`] for the batcher tests.

    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use parking_lot::Mutex;
    use tokio::sync::Notify;
    use usage_collector_sdk::UsageCollectorPluginError;

    use super::{FailureKind, FlushError, FlushSink};
    use crate::infra::storage::entity::UsageRecordRow;

    /// What the next `insert` does. The default (script exhausted) is `Ok`.
    #[derive(Clone)]
    pub enum Step {
        /// Succeed.
        Ok,
        /// Fail with the given classification.
        Fail(FailureKind),
        /// Panic inside the sink.
        Panic,
        /// Block until the notify fires, then succeed.
        Gate(Arc<Notify>),
        /// Block until the notify fires, then fail with the classification.
        GateThenFail(Arc<Notify>, FailureKind),
    }

    /// One recorded `insert` call.
    #[derive(Debug, Clone)]
    pub struct Recorded {
        pub rows: Vec<UsageRecordRow>,
        pub token: String,
        /// Paused-clock instant the call started.
        pub at: tokio::time::Instant,
    }

    #[derive(Default)]
    pub struct FakeSink {
        script: Mutex<VecDeque<Step>>,
        calls: Mutex<Vec<Recorded>>,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    impl FakeSink {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        /// Queue `steps` for the next inserts, in order.
        pub fn script(&self, steps: impl IntoIterator<Item = Step>) {
            self.script.lock().extend(steps);
        }

        pub fn calls(&self) -> Vec<Recorded> {
            self.calls.lock().clone()
        }

        pub fn call_count(&self) -> usize {
            self.calls.lock().len()
        }

        pub fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
        }

        fn enter(&self) {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        }

        fn exit(&self) {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl FlushSink for FakeSink {
        async fn insert(
            &self,
            rows: &[Arc<UsageRecordRow>],
            token: &str,
        ) -> Result<(), FlushError> {
            let step = self.script.lock().pop_front().unwrap_or(Step::Ok);
            self.calls.lock().push(Recorded {
                rows: rows.iter().map(|r| r.as_ref().clone()).collect(),
                token: token.to_owned(),
                at: tokio::time::Instant::now(),
            });
            self.enter();
            let failure = |kind: FailureKind| FlushError {
                kind,
                error: match kind {
                    FailureKind::Transient => {
                        UsageCollectorPluginError::transient("fake transient")
                    }
                    FailureKind::RowAttributable | FailureKind::Internal => {
                        UsageCollectorPluginError::internal("fake internal")
                    }
                },
            };
            let result = match step {
                Step::Ok => Ok(()),
                Step::Fail(kind) => Err(failure(kind)),
                Step::Panic => {
                    self.exit();
                    panic!("fake sink panic");
                }
                Step::Gate(gate) => {
                    gate.notified().await;
                    Ok(())
                }
                Step::GateThenFail(gate, kind) => {
                    gate.notified().await;
                    Err(failure(kind))
                }
            };
            self.exit();
            result
        }
    }
}
