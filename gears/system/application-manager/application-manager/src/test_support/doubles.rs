//! Clock, request-id and startup doubles.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

use uuid::Uuid;

use crate::domain::ports::{Clock, RequestIdSource, StartupPort};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Clock that always returns the same instant.
pub struct FixedClock(SystemTime);

impl FixedClock {
    /// Creates a clock fixed at `t`.
    pub fn new(t: SystemTime) -> Self {
        Self(t)
    }
}

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        self.0
    }
}

/// Clock that only moves when advanced.
pub struct ManualClock(Mutex<SystemTime>);

impl ManualClock {
    /// Creates a clock starting at `t`.
    pub fn new(t: SystemTime) -> Self {
        Self(Mutex::new(t))
    }

    /// Moves the clock forward by `d`.
    pub fn advance(&self, d: Duration) {
        *lock(&self.0) += d;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        *lock(&self.0)
    }
}

/// Request-id source yielding consecutive ids (`Uuid::from_u128(n)`).
pub struct SequentialIdSource(Mutex<u128>);

impl SequentialIdSource {
    /// Creates a source starting at 1.
    pub fn new() -> Self {
        Self::starting_at(1)
    }

    /// Creates a source whose first id is `Uuid::from_u128(n)`.
    pub fn starting_at(n: u128) -> Self {
        Self(Mutex::new(n))
    }
}

impl RequestIdSource for SequentialIdSource {
    fn next_id(&self) -> Uuid {
        let mut next = lock(&self.0);
        let id = Uuid::from_u128(*next);
        *next = next.wrapping_add(1);
        id
    }
}

/// In-memory startup state holder.
#[derive(Default)]
pub struct StartupHolder {
    published: AtomicBool,
    stopping: AtomicBool,
}

impl StartupPort for StartupHolder {
    fn publish(&self) {
        self.published.store(true, Ordering::SeqCst);
    }

    fn set_stopping(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    fn is_ready(&self) -> bool {
        self.published.load(Ordering::SeqCst) && !self.stopping.load(Ordering::SeqCst)
    }

    fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "doubles_tests.rs"]
mod doubles_tests;
