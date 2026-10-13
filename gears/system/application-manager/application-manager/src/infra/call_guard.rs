//! Plugin Call Guard (DESIGN §3.2, §4.5).
//!
//! Every plugin call goes through [`PluginCallGuard`]: it enforces a per-plugin
//! in-flight limit, a per-call timeout and panic isolation. The call runs detached
//! from the caller: if the caller times out or is dropped, the call keeps running to
//! completion, holds its slot until it finishes, and a late result is only logged.
//! Detached calls take no cancellation token. Log fields are metadata only.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::{Notify, oneshot};
use tokio::task::JoinError;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use uuid::Uuid;

use crate::domain::ports::PluginCallError;

/// Fixed text of [`PluginCallError::Internal`] produced by the guard.
const INTERNAL_TEXT: &str = "plugin call failed";

/// Kind of plugin a call targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginKind {
    /// A section plugin.
    Section,
    /// The audit plugin.
    Audit,
}

impl PluginKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Section => "section",
            Self::Audit => "audit",
        }
    }
}

/// Kind of plugin call, used for log metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    /// Section plugin `key`.
    Key,
    /// Section plugin `schema`.
    Schema,
    /// Section plugin `apply`.
    Apply,
    /// Section plugin `remove`.
    Remove,
    /// Audit request handoff.
    RequestHandoff,
    /// Audit final-result handoff.
    FinalResultHandoff,
}

impl CallKind {
    /// The kind of plugin this call targets.
    #[must_use]
    pub fn plugin_kind(self) -> PluginKind {
        match self {
            Self::Key | Self::Schema | Self::Apply | Self::Remove => PluginKind::Section,
            Self::RequestHandoff | Self::FinalResultHandoff => PluginKind::Audit,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Key => "key",
            Self::Schema => "schema",
            Self::Apply => "apply",
            Self::Remove => "remove",
            Self::RequestHandoff => "request_handoff",
            Self::FinalResultHandoff => "final_result_handoff",
        }
    }
}

/// Metadata of one guarded call; used only for log fields.
///
/// `section_key` must be a validated SDK section key; it is logged with control
/// characters escaped as a second line of defence.
#[derive(Debug, Clone)]
pub struct CallMeta {
    /// Request the call belongs to, if any.
    pub request_id: Option<Uuid>,
    /// Kind of the call.
    pub call_kind: CallKind,
    /// Section key the call concerns, if any.
    pub section_key: Option<String>,
}

struct State {
    counts: Mutex<HashMap<String, usize>>,
    notify: Notify,
}

impl State {
    /// Locks the counters; a poisoned lock is recovered because the map stays valid.
    fn counts(&self) -> MutexGuard<'_, HashMap<String, usize>> {
        self.counts.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Drop guard holding one in-flight slot of a plugin.
struct Slot {
    state: Arc<State>,
    plugin_id: String,
}

impl Drop for Slot {
    fn drop(&mut self) {
        {
            let mut counts = self.state.counts();
            if let Some(count) = counts.get_mut(&self.plugin_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(&self.plugin_id);
                }
            }
        }
        self.state.notify.notify_waiters();
    }
}

enum Finished<T, E> {
    Done(Result<T, E>),
    Panicked,
    /// The plugin task was cancelled. The guard never cancels plugin work; only a
    /// runtime shutdown can.
    Cancelled,
}

impl<T, E> Finished<T, E> {
    fn from_join(joined: Result<Result<T, E>, JoinError>) -> Self {
        match joined {
            Ok(result) => Self::Done(result),
            Err(e) if e.is_panic() => Self::Panicked,
            Err(_) => Self::Cancelled,
        }
    }

    fn result_kind(&self) -> &'static str {
        match self {
            Self::Done(Ok(_)) => "ok",
            Self::Done(Err(_)) => "error",
            Self::Panicked => "panic",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Metadata for the `plugin_call_late_result` event; never holds the plugin result.
struct LateLog {
    meta: CallMeta,
    started: Instant,
    /// Set when the caller's wait ended by the timeout (else the caller was dropped).
    timed_out: AtomicBool,
}

impl LateLog {
    fn emit(&self, result_kind: &str) {
        let cause = if self.timed_out.load(Ordering::SeqCst) {
            "timeout"
        } else {
            "caller_dropped"
        };
        tracing::warn!(
            request_id = self.meta.request_id.map(tracing::field::display),
            plugin_kind = %self.meta.call_kind.plugin_kind().as_str(),
            call_kind = %self.meta.call_kind.as_str(),
            section_key = self
                .meta
                .section_key
                .as_deref()
                .map(|key| tracing::field::display(key.escape_debug())),
            elapsed_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            result_kind = %result_kind,
            cause = %cause,
            "plugin_call_late_result"
        );
    }
}

/// Owns the result receiver. If the caller goes away (timeout or drop) after the
/// supervisor delivered a result that nobody consumed, the late result is logged here;
/// closing the channel first guarantees that exactly one side logs.
struct ResultReceiver<T, E> {
    rx: oneshot::Receiver<Finished<T, E>>,
    late: Arc<LateLog>,
}

impl<T, E> Drop for ResultReceiver<T, E> {
    fn drop(&mut self) {
        self.rx.close();
        if let Ok(finished) = self.rx.try_recv() {
            self.late.emit(finished.result_kind());
        }
    }
}

/// Guards plugin calls with a per-plugin in-flight limit, a timeout and panic
/// isolation. Cheap to clone; clones share the same counters.
#[derive(Clone)]
pub struct PluginCallGuard {
    timeout: Duration,
    max_in_flight: usize,
    state: Arc<State>,
}

impl PluginCallGuard {
    /// Creates a guard with the per-call `timeout` and the per-plugin `max_in_flight`.
    ///
    /// # Panics
    ///
    /// Panics if `max_in_flight` is zero; configuration validation rejects that value
    /// before the guard is built.
    #[must_use]
    pub fn new(timeout: Duration, max_in_flight: usize) -> Self {
        assert!(
            max_in_flight >= 1,
            "plugin_max_in_flight must be at least 1"
        );
        Self {
            timeout,
            max_in_flight,
            state: Arc::new(State {
                counts: Mutex::new(HashMap::new()),
                notify: Notify::new(),
            }),
        }
    }

    fn reserve(&self, plugin_id: &str) -> Option<Slot> {
        let mut counts = self.state.counts();
        let count = counts.entry(plugin_id.to_owned()).or_insert(0);
        if *count >= self.max_in_flight {
            return None;
        }
        *count += 1;
        drop(counts);
        Some(Slot {
            state: Arc::clone(&self.state),
            plugin_id: plugin_id.to_owned(),
        })
    }

    /// Runs `fut` as a guarded call to `plugin_id`.
    ///
    /// Must be called within a tokio runtime: the call is spawned detached. When the
    /// result and the timeout are ready at the same poll, the result wins.
    ///
    /// Returns [`PluginCallError::NotSent`] when the plugin is at its in-flight limit,
    /// [`PluginCallError::TimedOut`] when the timeout elapses (the call keeps running
    /// and its slot stays held until it finishes), and [`PluginCallError::Internal`]
    /// when the call panics or its task is cancelled. The guard never cancels plugin
    /// work; only a tokio runtime shutdown can.
    ///
    /// # Errors
    ///
    /// See above; a plugin error is returned as [`PluginCallError::Plugin`].
    pub async fn call<T, E, F>(
        &self,
        plugin_id: &str,
        meta: CallMeta,
        fut: F,
    ) -> Result<T, PluginCallError<E>>
    where
        F: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        let Some(slot) = self.reserve(plugin_id) else {
            return Err(PluginCallError::NotSent);
        };
        let late = Arc::new(LateLog {
            meta,
            started: Instant::now(),
            timed_out: AtomicBool::new(false),
        });
        // The detached tasks keep the caller's span as parent.
        let span = tracing::Span::current();
        let (tx, rx) = oneshot::channel();
        let supervisor_late = Arc::clone(&late);
        let supervisor_span = span.clone();
        tokio::spawn(
            async move {
                // The inner task isolates a plugin panic from this supervisor task.
                let joined = tokio::spawn(fut.instrument(span)).await;
                // Release the slot before the result is delivered.
                drop(slot);
                let finished = Finished::from_join(joined);
                if let Err(finished) = tx.send(finished) {
                    // The caller is gone and closed the channel: nobody will read it.
                    supervisor_late.emit(finished.result_kind());
                }
            }
            .instrument(supervisor_span),
        );
        let mut receiver = ResultReceiver { rx, late };
        let finished =
            if let Ok(delivered) = tokio::time::timeout(self.timeout, &mut receiver.rx).await {
                delivered.ok()
            } else {
                // Result and timeout ready together: the result wins. Give the
                // detached call one scheduler turn to deliver before giving up.
                receiver.late.timed_out.store(true, Ordering::SeqCst);
                tokio::task::yield_now().await;
                match receiver.rx.try_recv() {
                    Ok(finished) => Some(finished),
                    Err(_) => return Err(PluginCallError::TimedOut),
                }
            };
        match finished {
            Some(Finished::Done(Ok(value))) => Ok(value),
            Some(Finished::Done(Err(e))) => Err(PluginCallError::Plugin(e)),
            // A dropped sender (supervisor gone) is an internal failure too.
            Some(Finished::Panicked | Finished::Cancelled) | None => {
                Err(PluginCallError::Internal(INTERNAL_TEXT.to_owned()))
            }
        }
    }

    /// Number of calls currently in flight for `plugin_id`. For tests and the future
    /// in-flight gauge.
    #[must_use]
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "used by the future in-flight gauge")
    )]
    pub(crate) fn in_flight(&self, plugin_id: &str) -> usize {
        self.state.counts().get(plugin_id).copied().unwrap_or(0)
    }

    /// Waits until no call is in flight, or until `deadline` is cancelled.
    ///
    /// Returns `0` when drained; otherwise logs `plugin_calls_abandoned` per plugin
    /// and returns the number of calls still in flight.
    ///
    /// Drain does not close admission: callers must set the stopping flag first
    /// (DESIGN §3.2 stop order) so no new call is admitted meanwhile. It may return
    /// while a call's supervisor is still finishing its late-result log.
    pub async fn drain(&self, deadline: &CancellationToken) -> usize {
        loop {
            let notified = self.state.notify.notified();
            tokio::pin!(notified);
            // Register before checking so a release between check and await is seen.
            notified.as_mut().enable();
            if self.state.counts().is_empty() {
                return 0;
            }
            tokio::select! {
                () = &mut notified => {}
                () = deadline.cancelled() => {
                    let mut snapshot: Vec<(String, usize)> = self
                        .state
                        .counts()
                        .iter()
                        .map(|(plugin_id, count)| (plugin_id.clone(), *count))
                        .collect();
                    snapshot.sort();
                    let mut total = 0;
                    for (plugin_id, count) in &snapshot {
                        tracing::warn!(
                            plugin_id = %plugin_id.escape_debug(),
                            count = *count,
                            "plugin_calls_abandoned"
                        );
                        total += *count;
                    }
                    return total;
                }
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "call_guard_tests.rs"]
mod call_guard_tests;
