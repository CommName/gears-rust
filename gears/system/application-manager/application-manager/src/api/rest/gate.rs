//! The request gate (DESIGN 3.2 REST API): a seed or uninstall needs the
//! startup plugin set, an unset stopping flag, and one request-task permit.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::domain::error::DomainError;
use crate::domain::plugin_set::StartupState;

/// Admits request tasks.
pub struct RequestGate {
    startup: Arc<StartupState>,
    permits: Arc<Semaphore>,
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
        Self {
            startup,
            permits: Arc::new(Semaphore::new(
                usize::try_from(max_request_tasks).unwrap_or(Semaphore::MAX_PERMITS),
            )),
            stopping: AtomicBool::new(false),
        }
    }

    /// Refuse every later request: `stop` has begun.
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
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
