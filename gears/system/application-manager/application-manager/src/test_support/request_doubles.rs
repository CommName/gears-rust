//! Doubles that make the request task itself panic (P7, DESIGN §4.8 "Request task tests").
//!
//! The call guard turns a plugin panic into an ordinary `Internal` failure, so the supervisor
//! path is reached only by a defect inside the request task: a panic in a domain port (the
//! policy port, the registry store, the clock) or in an unguarded call of a plugin port. These
//! wrappers sit on the domain ports, outside the guard, and panic at one chosen point.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginError, CallerIdentity, FinalResultContent,
    RequestHandoffContent, SectionPluginError,
};
use async_trait::async_trait;
use serde_json::Value;

use crate::domain::ports::{AuditSink, Clock, PluginCallError, SectionPluginPort};
use crate::test_support::{AuditCall, EventLog, SectionCall};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A clock that panics once and reads normally otherwise: on its `n`-th reading (1-based), or on
/// the first reading after a named event was recorded in a shared event log. The request task
/// reads the clock once at receive and once for `completed_at`; `after_event` ties the panic to
/// the progress point (for instance the registry write) rather than to the reading count, so a
/// change of how often the task reads the clock cannot move it. The supervisor's own later
/// reading must not panic again.
pub struct PanickingClock {
    inner: Arc<dyn Clock>,
    trigger: ClockTrigger,
    reads: AtomicUsize,
    fired: AtomicBool,
}

enum ClockTrigger {
    Nth(usize),
    From(usize),
    AfterEvent(Arc<EventLog>, String),
}

impl PanickingClock {
    /// Wraps `inner`; the `panic_on`-th reading panics.
    pub fn new(inner: Arc<dyn Clock>, panic_on: usize) -> Self {
        Self::with(inner, ClockTrigger::Nth(panic_on))
    }

    /// Wraps `inner`; the `first`-th reading and every later one panic (a clock that keeps
    /// failing, so a panic inside the supervisor's own recovery is reached).
    pub fn from_reading(inner: Arc<dyn Clock>, first: usize) -> Self {
        Self::with(inner, ClockTrigger::From(first))
    }

    /// Wraps `inner`; the first reading after `marker` (an exact event of `events`, such as
    /// `registry:upsert:app-1`) panics.
    pub fn after_event(inner: Arc<dyn Clock>, events: Arc<EventLog>, marker: &str) -> Self {
        Self::with(inner, ClockTrigger::AfterEvent(events, marker.to_owned()))
    }

    fn with(inner: Arc<dyn Clock>, trigger: ClockTrigger) -> Self {
        Self {
            inner,
            trigger,
            reads: AtomicUsize::new(0),
            fired: AtomicBool::new(false),
        }
    }

    /// Readings so far, the panicking one included.
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl Clock for PanickingClock {
    fn now(&self) -> SystemTime {
        let n = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        let fire = match &self.trigger {
            ClockTrigger::Nth(panic_on) => n == *panic_on,
            ClockTrigger::From(first) => n >= *first,
            ClockTrigger::AfterEvent(events, marker) => {
                events.events().iter().any(|e| e == marker)
                    && !self.fired.swap(true, Ordering::SeqCst)
            }
        };
        assert!(!fire, "clock fake panic on reading {n}");
        self.inner.now()
    }
}

/// An audit sink that makes the call through `inner` and then panics instead of returning its
/// result: the plugin saw (and counted) the hand-off, and the request task never learns the
/// answer. Only the chosen call kind panics.
pub struct PanickingAuditSink {
    inner: Arc<dyn AuditSink>,
    panic_on: AuditCall,
}

impl PanickingAuditSink {
    /// Wraps `inner`; the `panic_on` call panics after it ran.
    pub fn new(inner: Arc<dyn AuditSink>, panic_on: AuditCall) -> Self {
        Self { inner, panic_on }
    }
}

#[async_trait]
impl AuditSink for PanickingAuditSink {
    async fn request_handoff(
        &self,
        caller: &CallerIdentity,
        content: &RequestHandoffContent,
    ) -> Result<String, PluginCallError<AuditPluginError>> {
        let result = self.inner.request_handoff(caller, content).await;
        assert!(
            self.panic_on != AuditCall::RequestHandoff,
            "audit sink fake panic after the request hand-off"
        );
        result
    }

    async fn final_result_handoff(
        &self,
        caller: &CallerIdentity,
        audit_id: &str,
        content: &FinalResultContent,
    ) -> Result<(), PluginCallError<AuditPluginError>> {
        let result = self
            .inner
            .final_result_handoff(caller, audit_id, content)
            .await;
        assert!(
            self.panic_on != AuditCall::FinalResultHandoff,
            "audit sink fake panic after the final-result hand-off"
        );
        result
    }
}

/// An audit sink that fails every call of one kind with `error` without calling the plugin
/// (outside the guard: the guard gives `not_sent` only for a full cap, so the answer is
/// scripted); the other kind goes through.
pub struct ScriptedAuditSink {
    inner: Arc<dyn AuditSink>,
    kind: AuditCall,
    error: PluginCallError<AuditPluginError>,
}

impl ScriptedAuditSink {
    /// Wraps `inner`; every call of `kind` fails with `error`.
    pub fn new(
        inner: Arc<dyn AuditSink>,
        kind: AuditCall,
        error: PluginCallError<AuditPluginError>,
    ) -> Self {
        Self { inner, kind, error }
    }
}

#[async_trait]
impl AuditSink for ScriptedAuditSink {
    async fn request_handoff(
        &self,
        caller: &CallerIdentity,
        content: &RequestHandoffContent,
    ) -> Result<String, PluginCallError<AuditPluginError>> {
        if self.kind == AuditCall::RequestHandoff {
            return Err(self.error.clone());
        }
        self.inner.request_handoff(caller, content).await
    }

    async fn final_result_handoff(
        &self,
        caller: &CallerIdentity,
        audit_id: &str,
        content: &FinalResultContent,
    ) -> Result<(), PluginCallError<AuditPluginError>> {
        if self.kind == AuditCall::FinalResultHandoff {
            return Err(self.error.clone());
        }
        self.inner
            .final_result_handoff(caller, audit_id, content)
            .await
    }
}

/// A section plugin port that panics at the entry of its `nth` call of one kind (1-based),
/// BEFORE the plugin behind it is called: that call never reaches the plugin, and the fake
/// plugin's log shows only the earlier calls. Every other call goes through.
pub struct PanickingSectionPort {
    inner: Arc<dyn SectionPluginPort>,
    kind: SectionCall,
    nth: usize,
    seen: Mutex<usize>,
}

impl PanickingSectionPort {
    /// Wraps `inner`; the `nth` call of `kind` panics.
    pub fn new(inner: Arc<dyn SectionPluginPort>, kind: SectionCall, nth: usize) -> Self {
        Self {
            inner,
            kind,
            nth,
            seen: Mutex::new(0),
        }
    }

    fn enter(&self, kind: SectionCall) {
        if kind != self.kind {
            return;
        }
        let mut seen = lock(&self.seen);
        *seen += 1;
        assert!(
            *seen != self.nth,
            "section port fake panic on call {}",
            *seen
        );
    }
}

#[async_trait]
impl SectionPluginPort for PanickingSectionPort {
    async fn key(&self) -> Result<String, PluginCallError<SectionPluginError>> {
        self.enter(SectionCall::Key);
        self.inner.key().await
    }

    async fn schema(
        &self,
        caller: &CallerIdentity,
    ) -> Result<Value, PluginCallError<SectionPluginError>> {
        self.enter(SectionCall::Schema);
        self.inner.schema(caller).await
    }

    async fn apply(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
        section: Value,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        self.enter(SectionCall::Apply);
        self.inner.apply(caller, envelope, section).await
    }

    async fn remove(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        self.enter(SectionCall::Remove);
        self.inner.remove(caller, envelope).await
    }
}

/// A section plugin port that answers every call of one kind with a fixed guard-level error
/// without calling the plugin behind it (P7: a call the guard refused, `not_sent`, which the
/// real guard gives only when a plugin's in-flight cap is full). Other calls go through.
pub struct ScriptedSectionPort {
    inner: Arc<dyn SectionPluginPort>,
    kind: SectionCall,
    error: PluginCallError<SectionPluginError>,
}

impl ScriptedSectionPort {
    /// Wraps `inner`; every call of `kind` fails with `error`.
    pub fn new(
        inner: Arc<dyn SectionPluginPort>,
        kind: SectionCall,
        error: PluginCallError<SectionPluginError>,
    ) -> Self {
        Self { inner, kind, error }
    }
}

#[async_trait]
impl SectionPluginPort for ScriptedSectionPort {
    async fn key(&self) -> Result<String, PluginCallError<SectionPluginError>> {
        if self.kind == SectionCall::Key {
            return Err(self.error.clone());
        }
        self.inner.key().await
    }

    async fn schema(
        &self,
        caller: &CallerIdentity,
    ) -> Result<Value, PluginCallError<SectionPluginError>> {
        if self.kind == SectionCall::Schema {
            return Err(self.error.clone());
        }
        self.inner.schema(caller).await
    }

    async fn apply(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
        section: Value,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        if self.kind == SectionCall::Apply {
            return Err(self.error.clone());
        }
        self.inner.apply(caller, envelope, section).await
    }

    async fn remove(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        if self.kind == SectionCall::Remove {
            return Err(self.error.clone());
        }
        self.inner.remove(caller, envelope).await
    }
}
