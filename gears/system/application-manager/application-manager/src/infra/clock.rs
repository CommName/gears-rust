//! The system clock.

use time::OffsetDateTime;

use crate::domain::ports::Clock;

/// [`Clock`] over the system time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}
