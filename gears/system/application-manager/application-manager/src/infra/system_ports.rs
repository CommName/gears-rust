//! Production clock and request-id source (DESIGN §3.2 "Ports and Composition").

use std::time::SystemTime;

use uuid::Uuid;

use crate::domain::ports::{Clock, RequestIdSource};

/// The wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Time-ordered `UUIDv7` request ids.
#[derive(Debug, Clone, Copy, Default)]
pub struct UuidV7RequestIds;

impl RequestIdSource for UuidV7RequestIds {
    fn next_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "system_ports_tests.rs"]
mod system_ports_tests;
