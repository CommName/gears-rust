//! Fake section and audit plugins with scripted behaviour and call records, and (P6) the
//! in-memory registry store, a registry that never answers, a scripted policy port and the
//! shared event log that orders their calls.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::domain::ports::{
    PdpUnavailable, PolicyAction, PolicyDecision, PolicyPort, RegistryError, RegistryStore,
};
use crate::domain::registry::RegistryEntry;
use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginClientV1, AuditPluginError, CallerIdentity, FinalResultContent,
    RequestHandoffContent, SectionPluginClientV1, SectionPluginError,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use toolkit_security::SecurityContext;

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
    schema: Mutex<Value>,
    scripts: Scripts<SectionCall, SectionPluginError>,
    log: Mutex<Vec<SectionCallRecord>>,
    completed: AtomicUsize,
    events: Option<Arc<EventLog>>,
}

impl FakeSectionPlugin {
    /// Creates a plugin serving `key` with the schema `{"type": "object"}`.
    pub fn new(key: &str) -> Self {
        Self {
            key: key.to_owned(),
            schema: Mutex::new(json!({"type": "object"})),
            scripts: Scripts::new(),
            log: Mutex::new(Vec::new()),
            completed: AtomicUsize::new(0),
            events: None,
        }
    }

    /// Records `section:<key>:<call>` in `log` at the start of every call (P6).
    #[must_use]
    pub fn with_event_log(mut self, log: Arc<EventLog>) -> Self {
        self.events = Some(log);
        self
    }

    /// Replaces the schema served by later `schema` calls (a plugin that changes its schema).
    pub fn set_schema(&self, schema: Value) {
        *lock(&self.schema) = schema;
    }

    /// Sets the schema returned by `schema`.
    #[must_use]
    pub fn with_schema(self, s: Value) -> Self {
        *lock(&self.schema) = s;
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
        if let Some(events) = &self.events {
            let call = match kind {
                SectionCall::Key => "key",
                SectionCall::Schema => "schema",
                SectionCall::Apply => "apply",
                SectionCall::Remove => "remove",
            };
            events.record(format!("section:{}:{call}", self.key));
        }
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
        .map(|()| lock(&self.schema).clone())
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
    events: Option<Arc<EventLog>>,
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
            events: None,
        }
    }

    /// Records `audit:request_handoff` or `audit:final_result_handoff` in `log` at the start
    /// of every call (P6).
    #[must_use]
    pub fn with_event_log(mut self, log: Arc<EventLog>) -> Self {
        self.events = Some(log);
        self
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
        if let Some(events) = &self.events {
            events.record(match kind {
                AuditCall::RequestHandoff => "audit:request_handoff",
                AuditCall::FinalResultHandoff => "audit:final_result_handoff",
            });
        }
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

/// Ordered log of the calls of one test, shared by the fakes that were given it. Each fake
/// records one text event per call at the start of the call, so a test can pin the order of
/// calls across ports (PDP, registry, hand-off, schema, dispatch).
#[derive(Default)]
pub struct EventLog(Mutex<Vec<String>>);

impl EventLog {
    /// Creates an empty shared log.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Appends one event.
    pub fn record(&self, event: impl Into<String>) {
        lock(&self.0).push(event.into());
    }

    /// The events so far, in order.
    pub fn events(&self) -> Vec<String> {
        lock(&self.0).clone()
    }
}

/// Operation of the registry store port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegistryOp {
    /// `get`
    Get,
    /// `upsert`
    Upsert,
    /// `delete`
    Delete,
    /// `list`
    List,
}

impl RegistryOp {
    fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Upsert => "upsert",
            Self::Delete => "delete",
            Self::List => "list",
        }
    }
}

/// One call received by a registry fake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryCall {
    /// The operation.
    pub op: RegistryOp,
    /// The id read, deleted or written; `None` for `list`.
    pub id: Option<String>,
}

fn record_registry_call(
    calls: &Mutex<Vec<RegistryCall>>,
    events: Option<&Arc<EventLog>>,
    op: RegistryOp,
    id: Option<&str>,
) {
    if let Some(events) = events {
        events.record(format!("registry:{}:{}", op.name(), id.unwrap_or("")));
    }
    lock(calls).push(RegistryCall {
        op,
        id: id.map(str::to_owned),
    });
}

/// In-memory `RegistryStore` with exact-text keys, a call record and per-operation scripted
/// failures. It passes the shared registry cases (`registry_cases::portable`).
#[derive(Default)]
pub struct InMemoryRegistryStore {
    entries: Mutex<BTreeMap<String, RegistryEntry>>,
    calls: Mutex<Vec<RegistryCall>>,
    failures: Mutex<HashMap<RegistryOp, RegistryError>>,
    failures_after_commit: Mutex<HashMap<RegistryOp, RegistryError>>,
    events: Option<Arc<EventLog>>,
}

impl InMemoryRegistryStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `registry:<op>:<id>` in `log` at the start of every call.
    #[must_use]
    pub fn with_event_log(mut self, log: Arc<EventLog>) -> Self {
        self.events = Some(log);
        self
    }

    /// Stores `entry` without recording a call.
    #[must_use]
    pub fn with_entry(self, entry: RegistryEntry) -> Self {
        lock(&self.entries).insert(entry.application_id().to_owned(), entry);
        self
    }

    /// Every later `op` fails with `error` until [`Self::heal`]; the data is untouched.
    pub fn fail(&self, op: RegistryOp, error: RegistryError) {
        lock(&self.failures).insert(op, error);
    }

    /// Every later `op` APPLIES its change and then fails with `error` until [`Self::heal`]: a
    /// store that committed and lost the answer (a timeout after the commit).
    pub fn fail_after_commit(&self, op: RegistryOp, error: RegistryError) {
        lock(&self.failures_after_commit).insert(op, error);
    }

    /// Ends the scripted failure of `op`, before or after the commit.
    pub fn heal(&self, op: RegistryOp) {
        lock(&self.failures).remove(&op);
        lock(&self.failures_after_commit).remove(&op);
    }

    /// The calls received so far, in order, failed ones included.
    pub fn calls(&self) -> Vec<RegistryCall> {
        lock(&self.calls).clone()
    }

    /// The stored entries ordered by id, without recording a call.
    pub fn entries(&self) -> Vec<RegistryEntry> {
        lock(&self.entries).values().cloned().collect()
    }

    fn committed(&self, op: RegistryOp) -> Result<(), RegistryError> {
        match lock(&self.failures_after_commit).get(&op) {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    fn enter(&self, op: RegistryOp, id: Option<&str>) -> Result<(), RegistryError> {
        record_registry_call(&self.calls, self.events.as_ref(), op, id);
        match lock(&self.failures).get(&op) {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl RegistryStore for InMemoryRegistryStore {
    async fn get(&self, id: &str) -> Result<Option<RegistryEntry>, RegistryError> {
        self.enter(RegistryOp::Get, Some(id))?;
        let found = lock(&self.entries).get(id).cloned();
        self.committed(RegistryOp::Get)?;
        Ok(found)
    }

    async fn upsert(&self, entry: &RegistryEntry) -> Result<(), RegistryError> {
        self.enter(RegistryOp::Upsert, Some(entry.application_id()))?;
        lock(&self.entries).insert(entry.application_id().to_owned(), entry.clone());
        self.committed(RegistryOp::Upsert)
    }

    async fn delete(&self, id: &str) -> Result<(), RegistryError> {
        self.enter(RegistryOp::Delete, Some(id))?;
        lock(&self.entries).remove(id);
        self.committed(RegistryOp::Delete)
    }

    async fn list(&self) -> Result<Vec<RegistryEntry>, RegistryError> {
        self.enter(RegistryOp::List, None)?;
        let all = self.entries();
        self.committed(RegistryOp::List)?;
        Ok(all)
    }
}

/// A registry store that records every call and never answers it.
#[derive(Default)]
pub struct NeverAnsweringRegistry {
    calls: Mutex<Vec<RegistryCall>>,
    events: Option<Arc<EventLog>>,
}

impl NeverAnsweringRegistry {
    /// Creates the double.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `registry:<op>:<id>` in `log` at the start of every call.
    #[must_use]
    pub fn with_event_log(mut self, log: Arc<EventLog>) -> Self {
        self.events = Some(log);
        self
    }

    /// The calls received so far, in order.
    pub fn calls(&self) -> Vec<RegistryCall> {
        lock(&self.calls).clone()
    }
}

#[async_trait]
impl RegistryStore for NeverAnsweringRegistry {
    async fn get(&self, id: &str) -> Result<Option<RegistryEntry>, RegistryError> {
        record_registry_call(&self.calls, self.events.as_ref(), RegistryOp::Get, Some(id));
        std::future::pending().await
    }

    async fn upsert(&self, entry: &RegistryEntry) -> Result<(), RegistryError> {
        record_registry_call(
            &self.calls,
            self.events.as_ref(),
            RegistryOp::Upsert,
            Some(entry.application_id()),
        );
        std::future::pending().await
    }

    async fn delete(&self, id: &str) -> Result<(), RegistryError> {
        record_registry_call(
            &self.calls,
            self.events.as_ref(),
            RegistryOp::Delete,
            Some(id),
        );
        std::future::pending().await
    }

    async fn list(&self) -> Result<Vec<RegistryEntry>, RegistryError> {
        record_registry_call(&self.calls, self.events.as_ref(), RegistryOp::List, None);
        std::future::pending().await
    }
}

/// Scripted answer of [`FakePolicy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyScript {
    /// Permit.
    Permit,
    /// Refuse.
    Refuse,
    /// Cannot answer, for this reason. A PDP that never answers
    /// is scripted at the platform client instead (`AuthzScript::Hang` behind the real
    /// `PolicyAdapter`), because the adapter owns the deadline.
    Unavailable(PdpUnavailable),
}

/// One call received by [`FakePolicy`].
#[derive(Debug, Clone)]
pub struct PolicyCall {
    /// The caller's security context as the port received it (bearer token included).
    pub ctx: SecurityContext,
    /// The action.
    pub action: PolicyAction,
    /// The application id the call carried.
    pub application_id: Option<String>,
}

/// Scripted `PolicyPort` with a call record.
pub struct FakePolicy {
    script: Mutex<PolicyScript>,
    calls: Mutex<Vec<PolicyCall>>,
    events: Option<Arc<EventLog>>,
}

impl FakePolicy {
    /// Creates a policy that answers `script`.
    pub fn new(script: PolicyScript) -> Self {
        Self {
            script: Mutex::new(script),
            calls: Mutex::new(Vec::new()),
            events: None,
        }
    }

    /// Records `pdp:<action>` in `log` at the start of every call.
    #[must_use]
    pub fn with_event_log(mut self, log: Arc<EventLog>) -> Self {
        self.events = Some(log);
        self
    }

    /// Changes the answer of later calls.
    pub fn set(&self, script: PolicyScript) {
        *lock(&self.script) = script;
    }

    /// The calls received so far, in order.
    pub fn calls(&self) -> Vec<PolicyCall> {
        lock(&self.calls).clone()
    }
}

#[async_trait]
impl PolicyPort for FakePolicy {
    async fn decide(
        &self,
        ctx: &SecurityContext,
        action: PolicyAction,
        application_id: Option<&str>,
    ) -> PolicyDecision {
        if let Some(events) = &self.events {
            events.record(format!("pdp:{}", action.name()));
        }
        lock(&self.calls).push(PolicyCall {
            ctx: ctx.clone(),
            action,
            application_id: application_id.map(str::to_owned),
        });
        let script = *lock(&self.script);
        match script {
            PolicyScript::Permit => PolicyDecision::Permit,
            PolicyScript::Refuse => PolicyDecision::Refused,
            PolicyScript::Unavailable(reason) => PolicyDecision::Unavailable(reason),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "fakes_tests.rs"]
mod fakes_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "fakes_registry_tests.rs"]
mod fakes_registry_tests;
