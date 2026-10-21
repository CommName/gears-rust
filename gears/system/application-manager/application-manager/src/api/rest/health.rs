//! Readiness healthcheck: the gear is not ready until startup publishes the
//! section plugin set (DESIGN 4.3, `cpt-cf-application-manager-nfr-availability`).

use std::sync::Arc;

use async_trait::async_trait;
use toolkit::{Healthcheck, HealthcheckResult};

use crate::domain::plugin_set::StartupState;

/// Readiness backed by the startup result; reads memory only.
#[derive(Debug)]
pub struct StartupHealthcheck {
    startup: Arc<StartupState>,
}

impl StartupHealthcheck {
    /// A readiness check over `startup`.
    #[must_use]
    pub fn new(startup: Arc<StartupState>) -> Self {
        Self { startup }
    }
}

#[async_trait]
impl Healthcheck for StartupHealthcheck {
    fn name(&self) -> &'static str {
        "application-manager-startup"
    }

    /// Unhealthy, not degraded: a degraded check keeps the pod in rotation.
    async fn check(&self) -> HealthcheckResult {
        if self.startup.plugin_set().is_some() {
            HealthcheckResult::healthy()
        } else {
            HealthcheckResult::unhealthy("section plugin set not published").with_code("not_ready")
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "health_tests.rs"]
mod tests;
