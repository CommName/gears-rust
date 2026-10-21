//! Plugin call guard (`cpt-cf-application-manager-component-plugin-call-guard`).
//!
//! Runs each plugin call up to `plugin_call_timeout` and drops it at the
//! limit, refuses a call when the plugin already has `plugin_max_in_flight`
//! calls running, and turns a panic into an error. It does not decide what an
//! outcome means for its caller.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use application_manager_sdk::{CallerIdentity, SectionPluginError, SectionSchema};
use async_trait::async_trait;
use futures::FutureExt;
use tokio::sync::Semaphore;

use crate::domain::plugin_set::SectionPlugin;
use crate::domain::ports::{CallFailure, PluginCalls};

/// Why a guarded call produced no plugin answer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GuardError {
    /// The plugin's in-flight cap was full; the plugin was not called.
    #[error("not_sent: plugin in-flight cap reached")]
    NotSent,
    /// The call was dropped at `plugin_call_timeout`; its effect is unknown.
    #[error("timed_out: plugin call exceeded the time limit")]
    TimedOut,
    /// The call panicked.
    #[error("internal: plugin call panicked: {0}")]
    Panicked(String),
}

/// Bounds every plugin call. Holds each plugin's in-flight permits, keyed by
/// its GTS instance id.
#[derive(Debug)]
pub struct CallGuard {
    timeout: Duration,
    max_in_flight: usize,
    in_flight: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl CallGuard {
    /// A guard with the configured time limit and per-plugin in-flight cap.
    #[must_use]
    pub fn new(timeout: Duration, max_in_flight: u32) -> Self {
        let max_in_flight = usize::try_from(max_in_flight)
            .unwrap_or(usize::MAX)
            .min(Semaphore::MAX_PERMITS);
        Self {
            timeout,
            max_in_flight,
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// The in-flight permits of the plugin `instance_id`, created on first use.
    fn permits(&self, instance_id: &str) -> Arc<Semaphore> {
        let mut by_plugin = self
            .in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Arc::clone(
            by_plugin
                .entry(instance_id.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(self.max_in_flight))),
        )
    }

    /// Run `call` to the plugin `instance_id` under its in-flight cap and the
    /// time limit.
    ///
    /// # Errors
    /// [`GuardError`] when the call was not sent, timed out, or panicked; the
    /// plugin's own result is returned unchanged in `Ok`.
    pub async fn call<F, T>(&self, instance_id: &str, call: F) -> Result<T, GuardError>
    where
        F: Future<Output = T>,
    {
        let permits = self.permits(instance_id);
        let Ok(_permit) = permits.try_acquire() else {
            return Err(GuardError::NotSent);
        };
        match tokio::time::timeout(self.timeout, AssertUnwindSafe(call).catch_unwind()).await {
            Err(_) => Err(GuardError::TimedOut),
            Ok(Err(panic)) => Err(GuardError::Panicked(panic_message(panic.as_ref()))),
            Ok(Ok(answer)) => Ok(answer),
        }
    }
}

impl From<GuardError> for CallFailure {
    fn from(err: GuardError) -> Self {
        match err {
            GuardError::NotSent => Self::NotSent,
            GuardError::TimedOut => Self::TimedOut,
            GuardError::Panicked(message) => Self::Panicked(message),
        }
    }
}

#[async_trait]
impl PluginCalls for CallGuard {
    async fn schema(
        &self,
        plugin: &SectionPlugin,
        caller: &CallerIdentity,
    ) -> Result<Result<SectionSchema, SectionPluginError>, CallFailure> {
        self.call(&plugin.instance_id, plugin.client.schema(caller))
            .await
            .map_err(CallFailure::from)
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "call_guard_tests.rs"]
mod tests;
