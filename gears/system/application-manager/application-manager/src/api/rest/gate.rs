//! The request gate (DESIGN 3.2 REST API): a seed or uninstall needs the
//! startup plugin set, an unset stopping flag, and one request-task permit.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::domain::error::DomainError;
use crate::domain::plugin_set::StartupState;

/// Admits request tasks.
pub struct RequestGate {
    startup: Arc<StartupState>,
    permits: Arc<Semaphore>,
    max: u32,
    stopping: AtomicBool,
}

impl fmt::Debug for RequestGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestGate").finish_non_exhaustive()
    }
}

impl RequestGate {
    /// A gate over the startup result with `max_request_tasks` permits.
    #[must_use]
    pub fn new(startup: Arc<StartupState>, max_request_tasks: u32) -> Self {
        let max = u32::try_from(Semaphore::MAX_PERMITS)
            .map_or(max_request_tasks, |cap| max_request_tasks.min(cap));
        Self {
            startup,
            permits: Arc::new(Semaphore::new(usize::try_from(max).unwrap_or(0))),
            max,
            stopping: AtomicBool::new(false),
        }
    }

    /// Refuse every later request: `stop` has begun.
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    /// Wait until every request task has ended: each holds its permit until
    /// its operation is final, so holding every permit means none is left.
    /// Returns `false`, with the count still in flight logged, when
    /// `deadline` comes first. Call it after [`Self::stop`].
    pub async fn drain(&self, deadline: &CancellationToken) -> bool {
        tokio::select! {
            biased;
            drained = Arc::clone(&self.permits).acquire_many_owned(self.max) => drained.is_ok(),
            () = deadline.cancelled() => {
                let in_flight = usize::try_from(self.max)
                    .unwrap_or(0)
                    .saturating_sub(self.permits.available_permits());
                tracing::warn!(in_flight, "application-manager: request tasks still running at the stop deadline");
                false
            }
        }
    }

    /// Take one request-task permit; the request task holds it until the
    /// operation is final.
    ///
    /// # Errors
    /// [`DomainError::NotReady`] before startup ends or after `stop`;
    /// [`DomainError::AtCapacity`] with no free permit.
    pub fn admit(&self) -> Result<OwnedSemaphorePermit, DomainError> {
        if self.stopping.load(Ordering::SeqCst) || self.startup.plugin_set().is_none() {
            return Err(DomainError::NotReady);
        }
        Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| DomainError::AtCapacity)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gate_tests.rs"]
mod tests;
