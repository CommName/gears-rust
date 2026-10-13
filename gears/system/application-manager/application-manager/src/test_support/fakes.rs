//! Fake section and audit plugins with scripted behaviour and call records.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginClientV1, AuditPluginError, CallerIdentity, FinalResultContent,
    RequestHandoffContent, SectionPluginClientV1, SectionPluginError,
};
use async_trait::async_trait;
use serde_json::{Value, json};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone)]
enum Action<E> {
    Ok,
    Err(E),
    Panic(String),
    Hang,
}

/// Scripted behaviour of one fake plugin call: an optional delay, then an action.
#[derive(Clone)]
pub struct CallBehavior<E> {
    delay: Duration,
    action: Action<E>,
}

impl<E> CallBehavior<E> {
    /// Succeeds.
    pub fn ok() -> Self {
        Self::with_action(Action::Ok)
    }

    /// Fails with `e`.
    pub fn err(e: E) -> Self {
        Self::with_action(Action::Err(e))
    }

    /// Panics with a default message.
    pub fn panic() -> Self {
        Self::panic_with("fake plugin panic")
    }

    /// Panics with message `m`.
    pub fn panic_with(m: &str) -> Self {
        Self::with_action(Action::Panic(m.to_owned()))
    }

    /// Never completes.
    pub fn hang() -> Self {
        Self::with_action(Action::Hang)
    }

    /// Waits `d` (on the tokio clock) before acting.
    #[must_use]
    pub fn with_delay(mut self, d: Duration) -> Self {
        self.delay = d;
        self
    }

    fn with_action(action: Action<E>) -> Self {
        Self {
            delay: Duration::ZERO,
            action,
        }
    }
}

struct Script<E> {
    persistent: Option<CallBehavior<E>>,
    queue: VecDeque<CallBehavior<E>>,
}

impl<E> Script<E> {
    fn new() -> Self {
        Self {
            persistent: None,
            queue: VecDeque::new(),
        }
    }
}

/// Per-call-kind behaviour scripts: queued behaviours first, then the persistent one,
/// then success.
struct Scripts<K, E>(Mutex<HashMap<K, Script<E>>>);

impl<K: Hash + Eq + Copy, E: Clone> Scripts<K, E> {
    fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }

    fn on(&self, k: K, b: CallBehavior<E>) {
        lock(&self.0)
            .entry(k)
            .or_insert_with(Script::new)
            .persistent = Some(b);
    }

    fn enqueue(&self, k: K, b: CallBehavior<E>) {
        lock(&self.0)
            .entry(k)
            .or_insert_with(Script::new)
            .queue
            .push_back(b);
    }

    fn next(&self, k: K) -> CallBehavior<E> {
        lock(&self.0)
            .get_mut(&k)
            .and_then(|s| s.queue.pop_front().or_else(|| s.persistent.clone()))
            .unwrap_or_else(CallBehavior::ok)
    }
}

async fn act<E>(b: CallBehavior<E>) -> Result<(), E> {
    if !b.delay.is_zero() {
        tokio::time::sleep(b.delay).await;
    }
    match b.action {
        Action::Ok => Ok(()),
        Action::Err(e) => Err(e),
        Action::Panic(m) => panic!("{m}"),
        Action::Hang => std::future::pending().await,
    }
}

/// Kind of a section plugin call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SectionCall {
    /// `key`
    Key,
    /// `schema`
    Schema,
    /// `apply`
    Apply,
    /// `remove`
    Remove,
}

/// Record of one call received by [`FakeSectionPlugin`].
#[derive(Debug, Clone, PartialEq)]
pub struct SectionCallRecord {
    /// Call kind.
    pub kind: SectionCall,
    /// Caller, absent for `key`.
    pub caller: Option<CallerIdentity>,
    /// Envelope, present for `apply` and `remove`.
    pub envelope: Option<ApplicationEnvelope>,
    /// Section value, present for `apply`.
    pub section: Option<Value>,
}

/// Fake section plugin.
pub struct FakeSectionPlugin {
    key: String,
    schema: Value,
    scripts: Scripts<SectionCall, SectionPluginError>,
    log: Mutex<Vec<SectionCallRecord>>,
    completed: AtomicUsize,
}

impl FakeSectionPlugin {
    /// Creates a plugin serving `key` with the schema `{"type": "object"}`.
    pub fn new(key: &str) -> Self {
        Self {
            key: key.to_owned(),
            schema: json!({"type": "object"}),
            scripts: Scripts::new(),
            log: Mutex::new(Vec::new()),
            completed: AtomicUsize::new(0),
        }
    }

    /// Sets the schema returned by `schema`.
    #[must_use]
    pub fn with_schema(mut self, s: Value) -> Self {
        self.schema = s;
        self
    }

    /// Sets the persistent behaviour of call kind `c`.
    pub fn on(&self, c: SectionCall, b: CallBehavior<SectionPluginError>) {
        self.scripts.on(c, b);
    }

    /// Queues a one-shot behaviour for call kind `c`.
    pub fn enqueue(&self, c: SectionCall, b: CallBehavior<SectionPluginError>) {
        self.scripts.enqueue(c, b);
    }

    /// Calls received so far, in order.
    pub fn calls(&self) -> Vec<SectionCallRecord> {
        lock(&self.log).clone()
    }

    /// Number of calls received so far.
    pub fn call_count(&self) -> usize {
        lock(&self.log).len()
    }

    /// Number of calls that ran to completion (not cancelled, not hanging).
    pub fn completed(&self) -> usize {
        self.completed.load(Ordering::SeqCst)
    }

    async fn run(&self, rec: SectionCallRecord) -> Result<(), SectionPluginError> {
        let kind = rec.kind;
        lock(&self.log).push(rec);
        let result = act(self.scripts.next(kind)).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        result
    }
}

#[async_trait]
impl SectionPluginClientV1 for FakeSectionPlugin {
    async fn key(&self) -> Result<String, SectionPluginError> {
        self.run(SectionCallRecord {
            kind: SectionCall::Key,
            caller: None,
            envelope: None,
            section: None,
        })
        .await
        .map(|()| self.key.clone())
    }

    async fn schema(&self, caller: &CallerIdentity) -> Result<Value, SectionPluginError> {
        self.run(SectionCallRecord {
            kind: SectionCall::Schema,
            caller: Some(caller.clone()),
            envelope: None,
            section: None,
        })
        .await
        .map(|()| self.schema.clone())
    }

    async fn apply(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
        section: Value,
    ) -> Result<(), SectionPluginError> {
        self.run(SectionCallRecord {
            kind: SectionCall::Apply,
            caller: Some(caller.clone()),
            envelope: Some(envelope.clone()),
            section: Some(section),
        })
        .await
    }

    async fn remove(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
    ) -> Result<(), SectionPluginError> {
        self.run(SectionCallRecord {
            kind: SectionCall::Remove,
            caller: Some(caller.clone()),
            envelope: Some(envelope.clone()),
            section: None,
        })
        .await
    }
}

/// Kind of an audit plugin call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuditCall {
    /// `request_handoff`
    RequestHandoff,
    /// `final_result_handoff`
    FinalResultHandoff,
}

/// Record of one call received by [`FakeAuditPlugin`].
#[derive(Debug, Clone, PartialEq)]
pub struct AuditCallRecord {
    /// Call kind.
    pub kind: AuditCall,
    /// Caller.
    pub caller: CallerIdentity,
    /// Request content, present for `request_handoff`.
    pub request: Option<RequestHandoffContent>,
    /// Audit id, present for `final_result_handoff`.
    pub audit_id: Option<String>,
    /// Final result, present for `final_result_handoff`.
    pub result: Option<FinalResultContent>,
}

/// Fake audit plugin.
pub struct FakeAuditPlugin {
    audit_id: String,
    scripts: Scripts<AuditCall, AuditPluginError>,
    log: Mutex<Vec<AuditCallRecord>>,
    completed: AtomicUsize,
}

impl Default for FakeAuditPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeAuditPlugin {
    /// Creates a plugin returning the audit id `audit-1`.
    pub fn new() -> Self {
        Self {
            audit_id: "audit-1".to_owned(),
            scripts: Scripts::new(),
            log: Mutex::new(Vec::new()),
            completed: AtomicUsize::new(0),
        }
    }

    /// Sets the audit id returned by `request_handoff`.
    #[must_use]
    pub fn with_audit_id(mut self, id: &str) -> Self {
        self.audit_id = id.to_owned();
        self
    }

    /// Sets the persistent behaviour of call kind `c`.
    pub fn on(&self, c: AuditCall, b: CallBehavior<AuditPluginError>) {
        self.scripts.on(c, b);
    }

    /// Queues a one-shot behaviour for call kind `c`.
    pub fn enqueue(&self, c: AuditCall, b: CallBehavior<AuditPluginError>) {
        self.scripts.enqueue(c, b);
    }

    /// Calls received so far, in order.
    pub fn calls(&self) -> Vec<AuditCallRecord> {
        lock(&self.log).clone()
    }

    /// Number of calls received so far.
    pub fn call_count(&self) -> usize {
        lock(&self.log).len()
    }

    /// Number of calls that ran to completion (not cancelled, not hanging).
    pub fn completed(&self) -> usize {
        self.completed.load(Ordering::SeqCst)
    }

    async fn run(&self, rec: AuditCallRecord) -> Result<(), AuditPluginError> {
        let kind = rec.kind;
        lock(&self.log).push(rec);
        let result = act(self.scripts.next(kind)).await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        result
    }
}

#[async_trait]
impl AuditPluginClientV1 for FakeAuditPlugin {
    async fn request_handoff(
        &self,
        caller: &CallerIdentity,
        content: &RequestHandoffContent,
    ) -> Result<String, AuditPluginError> {
        self.run(AuditCallRecord {
            kind: AuditCall::RequestHandoff,
            caller: caller.clone(),
            request: Some(content.clone()),
            audit_id: None,
            result: None,
        })
        .await
        .map(|()| self.audit_id.clone())
    }

    async fn final_result_handoff(
        &self,
        caller: &CallerIdentity,
        audit_id: &str,
        content: &FinalResultContent,
    ) -> Result<(), AuditPluginError> {
        self.run(AuditCallRecord {
            kind: AuditCall::FinalResultHandoff,
            caller: caller.clone(),
            request: None,
            audit_id: Some(audit_id.to_owned()),
            result: Some(content.clone()),
        })
        .await
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "fakes_tests.rs"]
mod fakes_tests;
