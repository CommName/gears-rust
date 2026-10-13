//! Startup result and its one-time-initialised holder (DESIGN §3.2 "Startup and
//! Registration", "Ports and Composition").
//!
//! The startup component resolves the section plugin set and the audit sink once and
//! publishes them as an immutable [`StartupResult`] in the [`StartupResultHolder`].
//! Readiness derives from the stored result, so no holder can be ready without one.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use crate::domain::ports::{AuditSink, SectionPluginPort, StartupPort};

/// One resolved section plugin instance.
///
/// The startup component is the only producer. `instance_id` is the full GTS instance
/// id and the scope id of the plugin's `ClientHub` client; `priority` is the
/// instance's registered priority; `key` passed the SDK key rule and is unique in the
/// result; `plugin` is the adapter over the client of `instance_id`.
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "read by the orchestrator and read service in later phases"
    )
)]
pub struct SectionPluginEntry {
    /// Full GTS instance id of the plugin.
    pub(crate) instance_id: String,
    /// Registered priority of the instance.
    pub(crate) priority: i16,
    /// Section key read at startup, already validated.
    pub(crate) key: String,
    /// The guarded section plugin port.
    pub(crate) plugin: Arc<dyn SectionPluginPort>,
}

/// The immutable outcome of startup: the section plugin set and the audit sink.
pub struct StartupResult {
    sections: Vec<SectionPluginEntry>,
    audit_instance_id: String,
    audit: Arc<dyn AuditSink>,
}

impl StartupResult {
    /// Creates a result keeping the given order of `section_plugins` (the startup
    /// component passes the (priority, instance id) order).
    #[must_use]
    pub(crate) fn new(
        section_plugins: Vec<SectionPluginEntry>,
        audit_instance_id: String,
        audit: Arc<dyn AuditSink>,
    ) -> Self {
        Self {
            sections: section_plugins,
            audit_instance_id,
            audit,
        }
    }

    /// The section plugins in (priority, instance id) order.
    #[must_use]
    pub fn section_plugins(&self) -> &[SectionPluginEntry] {
        &self.sections
    }

    /// The section keys, in [`Self::section_plugins`] order.
    #[must_use]
    pub fn section_keys(&self) -> Vec<&str> {
        self.sections.iter().map(|e| e.key.as_str()).collect()
    }

    /// Full GTS instance id of the audit plugin.
    #[must_use]
    pub fn audit_instance_id(&self) -> &str {
        &self.audit_instance_id
    }

    /// The guarded audit sink.
    #[must_use]
    pub fn audit(&self) -> &Arc<dyn AuditSink> {
        &self.audit
    }
}

/// A startup result was already published.
#[derive(Debug, PartialEq, Eq)]
pub struct AlreadyPublished;

/// One-time-initialised holder of the [`StartupResult`].
///
/// Published by the startup component; read by the orchestrator, the read service and
/// the REST not-ready gate.
#[derive(Default)]
pub struct StartupResultHolder {
    result: OnceLock<Arc<StartupResult>>,
    stopping: AtomicBool,
}

impl StartupResultHolder {
    /// Creates an empty holder: not ready, not stopping.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores the result once; the holder is then ready unless stopping.
    ///
    /// # Errors
    ///
    /// [`AlreadyPublished`] if a result is already stored; the first result stays.
    pub fn publish_result(&self, result: StartupResult) -> Result<(), AlreadyPublished> {
        self.result
            .set(Arc::new(result))
            .map_err(|_| AlreadyPublished)
    }

    /// The published result, `None` until published.
    #[must_use]
    pub fn result(&self) -> Option<Arc<StartupResult>> {
        self.result.get().cloned()
    }
}

impl StartupPort for StartupResultHolder {
    fn set_stopping(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    fn is_ready(&self) -> bool {
        self.result.get().is_some() && !self.is_stopping()
    }

    fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
#[allow(clippy::needless_collect)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "startup_tests.rs"]
mod startup_tests;
