//! Test harness of the pre-dispatch gate (P6): an `Orchestrator` over scripted ports, a real
//! startup result (guarded section plugin adapters over `FakeSectionPlugin`s and a guarded
//! audit sink over `FakeAuditPlugin`), and the shared `EventLog` that orders every call.
#![allow(clippy::duration_suboptimal_units)]

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, UNIX_EPOCH};

use application_manager_sdk::models::{HandoffOperation, OutcomeReport, Problem};
use authz_resolver_sdk::AuthZResolverApi;
use serde_json::{Value, json};
use toolkit_security::SecurityContext;
use uuid::Uuid;

use crate::domain::orchestrator::{
    GateOutcome, GateResult, Orchestrator, OrchestratorSettings, RequestInput, ValidationFn,
};
use crate::domain::ports::{AuditSink, Clock, PolicyPort, RegistryStore, SectionPluginPort};
use crate::domain::request::{AtCapacity, RequestHandle};
use crate::domain::schema_validator::SectionSchema;
use crate::domain::startup::{SectionPluginEntry, StartupResult, StartupResultHolder};
use crate::infra::call_guard::PluginCallGuard;
use crate::infra::plugin_adapters::{GuardedAuditSink, GuardedSectionPlugin};
use crate::infra::policy::PolicyAdapter;
use crate::test_support::{
    AuthzScript, EventLog, FakeAuditPlugin, FakeAuthz, FakePolicy, FakeSectionPlugin, FixedClock,
    InMemoryRegistryStore, NeverAnsweringRegistry, PolicyScript, RegistryCall, SequentialIdSource,
};

/// The plugin call timeout of the harness (the step timeout is the same).
pub const PLUGIN_TIMEOUT: Duration = Duration::from_millis(500);
/// The deadline of the real `PolicyAdapter` a test builds with [`policy_adapter`]
/// (`pdp_call_timeout`); the gate itself adds no PDP bound.
pub const PDP_TIMEOUT: Duration = Duration::from_secs(2);
/// `registry_operation_timeout` of the harness.
pub const REGISTRY_TIMEOUT: Duration = Duration::from_secs(3);
/// `max_envelope_text_length` of the harness.
pub const MAX_ENVELOPE_LEN: usize = 100;
/// The receive time the fixed clock returns, as RFC 3339 text.
pub const RECEIVED_AT: &str = "2027-01-15T08:00:00Z";

/// The caller of every harness request; it carries a bearer token, which only the PDP may
/// receive (F-008).
pub fn security_context() -> SecurityContext {
    SecurityContext::builder()
        .subject_id(Uuid::from_u128(0x5B))
        .subject_tenant_id(Uuid::from_u128(0x7E))
        .subject_type("service")
        .token_scopes(vec!["scope-1".to_owned()])
        .bearer_token("pdp-bearer-token".to_owned())
        .build()
        .expect("a security context")
}

/// A seed body: `{"application": {"id", "version"}, "sections": sections}`.
pub fn seed_body(id: &str, version: &str, sections: &Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "application": {"id": id, "version": version},
        "sections": sections,
    }))
    .expect("serializes")
}

/// Builds a [`Gate`].
pub struct GateBuilder {
    sections: Vec<(String, Value)>,
    policy: PolicyScript,
    registry_hangs: bool,
    entries: Vec<crate::domain::registry::RegistryEntry>,
    publish: bool,
    settings: OrchestratorSettings,
    audit: FakeAuditPlugin,
    guard_cap: usize,
    policy_port: Option<Arc<dyn PolicyPort>>,
    validation: Option<Arc<ValidationFn>>,
    // P7
    clock: Option<Arc<dyn Clock>>,
    policy_delay: Duration,
    wrap_audit: Option<AuditWrap>,
    wrap_sections: Vec<(usize, SectionWrap)>,
    events: Option<Arc<EventLog>>,
    request_ids_from: u128,
}

/// Replaces the audit sink by a wrapper around it (P7: a sink that panics).
type AuditWrap = Box<dyn FnOnce(Arc<dyn AuditSink>) -> Arc<dyn AuditSink>>;

/// Replaces a section plugin port by a wrapper around it (P7: a port that panics).
type SectionWrap = Box<dyn FnOnce(Arc<dyn SectionPluginPort>) -> Arc<dyn SectionPluginPort>>;

impl Default for GateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GateBuilder {
    /// Permitting policy, empty in-memory registry, no section plugin, accepting audit plugin.
    pub fn new() -> Self {
        Self {
            sections: Vec::new(),
            policy: PolicyScript::Permit,
            registry_hangs: false,
            entries: Vec::new(),
            publish: true,
            settings: OrchestratorSettings {
                max_envelope_text_length: MAX_ENVELOPE_LEN,
                registry_operation_timeout: REGISTRY_TIMEOUT,
                step_timeout: PLUGIN_TIMEOUT,
                max_validations: 64,
                max_request_tasks: 64,
            },
            audit: FakeAuditPlugin::new(),
            guard_cap: 64,
            policy_port: None,
            validation: None,
            clock: None,
            policy_delay: Duration::ZERO,
            wrap_audit: None,
            wrap_sections: Vec::new(),
            events: None,
            request_ids_from: 1,
        }
    }

    /// Shares an event log made before the gate (P7: a clock that panics after a given event).
    #[must_use]
    pub fn events(mut self, events: Arc<EventLog>) -> Self {
        self.events = Some(events);
        self
    }

    /// The first request id the gate's id source gives, `Uuid::from_u128(n)` (P7: a test whose
    /// log capture is keyed by its own request ids).
    #[must_use]
    pub fn request_ids_from(mut self, n: u128) -> Self {
        self.request_ids_from = n;
        self
    }

    /// Replaces the fixed clock (P7: a `ManualClock`, or a clock that panics).
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Makes the scripted policy wait `d` before it answers (P7).
    #[must_use]
    pub fn policy_delay(mut self, d: Duration) -> Self {
        self.policy_delay = d;
        self
    }

    /// Wraps the guarded audit sink in the startup result (P7: a sink that panics).
    #[must_use]
    pub fn wrap_audit_sink(
        mut self,
        wrap: impl FnOnce(Arc<dyn AuditSink>) -> Arc<dyn AuditSink> + 'static,
    ) -> Self {
        self.wrap_audit = Some(Box::new(wrap));
        self
    }

    /// Wraps the guarded port of the section plugin at `index` (priority order, 0-based) in the
    /// startup result (P7: a port that panics).
    #[must_use]
    pub fn wrap_section_port(
        mut self,
        index: usize,
        wrap: impl FnOnce(Arc<dyn SectionPluginPort>) -> Arc<dyn SectionPluginPort> + 'static,
    ) -> Self {
        self.wrap_sections.push((index, Box::new(wrap)));
        self
    }

    /// Adds a section plugin (priority order is the order of the calls) serving `schema`.
    #[must_use]
    pub fn section(mut self, key: &str, schema: Value) -> Self {
        self.sections.push((key.to_owned(), schema));
        self
    }

    /// Sets the policy's answer.
    #[must_use]
    pub fn policy(mut self, script: PolicyScript) -> Self {
        self.policy = script;
        self
    }

    /// Replaces the scripted policy by this port (for example the real `PolicyAdapter` over a
    /// `FakeAuthz`); `Gate::policy` then records nothing.
    #[must_use]
    pub fn policy_port(mut self, port: Arc<dyn PolicyPort>) -> Self {
        self.policy_port = Some(port);
        self
    }

    /// Replaces the validation that runs on the blocking pool (see [`ValidationLatch`]).
    #[must_use]
    pub(crate) fn validation(mut self, validation: Arc<ValidationFn>) -> Self {
        self.validation = Some(validation);
        self
    }

    /// Gives the orchestrator a registry that never answers.
    #[must_use]
    pub fn registry_never_answers(mut self) -> Self {
        self.registry_hangs = true;
        self
    }

    /// Pre-seeds the in-memory registry.
    #[must_use]
    pub fn entry(mut self, entry: crate::domain::registry::RegistryEntry) -> Self {
        self.entries.push(entry);
        self
    }

    /// Leaves the startup result unpublished.
    #[must_use]
    pub fn unpublished(mut self) -> Self {
        self.publish = false;
        self
    }

    /// Edits the orchestrator settings.
    #[must_use]
    pub fn settings(mut self, edit: impl FnOnce(&mut OrchestratorSettings)) -> Self {
        edit(&mut self.settings);
        self
    }

    /// The audit plugin the harness uses.
    #[must_use]
    pub fn audit(mut self, audit: FakeAuditPlugin) -> Self {
        self.audit = audit;
        self
    }

    /// The in-flight cap of every plugin's call guard.
    #[must_use]
    pub fn guard_cap(mut self, cap: usize) -> Self {
        self.guard_cap = cap;
        self
    }

    /// Builds the gate.
    pub fn build(self) -> Gate {
        let events = self.events.unwrap_or_default();
        let policy = Arc::new(
            FakePolicy::new(self.policy)
                .with_delay(self.policy_delay)
                .with_event_log(Arc::clone(&events)),
        );
        let memory = {
            let mut store = InMemoryRegistryStore::new().with_event_log(Arc::clone(&events));
            for e in self.entries {
                store = store.with_entry(e);
            }
            Arc::new(store)
        };
        let hung = Arc::new(NeverAnsweringRegistry::new().with_event_log(Arc::clone(&events)));
        let registry: Arc<dyn RegistryStore> = if self.registry_hangs {
            Arc::clone(&hung) as Arc<dyn RegistryStore>
        } else {
            Arc::clone(&memory) as Arc<dyn RegistryStore>
        };
        let guard = PluginCallGuard::new(PLUGIN_TIMEOUT, self.guard_cap);
        let sections: Vec<Arc<FakeSectionPlugin>> = self
            .sections
            .iter()
            .map(|(key, schema)| {
                Arc::new(
                    FakeSectionPlugin::new(key)
                        .with_schema(schema.clone())
                        .with_event_log(Arc::clone(&events)),
                )
            })
            .collect();
        let audit = Arc::new(self.audit.with_event_log(Arc::clone(&events)));
        let holder = Arc::new(StartupResultHolder::new());
        let mut wrap_sections = self.wrap_sections;
        let wrap_audit = self.wrap_audit;
        if self.publish {
            let entries = sections
                .iter()
                .enumerate()
                .map(|(i, plugin)| {
                    let key = self.sections[i].0.clone();
                    let instance_id = format!("section-{i}");
                    let mut adapter: Arc<dyn SectionPluginPort> =
                        Arc::new(GuardedSectionPlugin::new(
                            &instance_id,
                            &key,
                            Arc::clone(plugin) as _,
                            guard.clone(),
                        ));
                    if let Some(at) = wrap_sections.iter().position(|(index, _)| *index == i) {
                        adapter = (wrap_sections.remove(at).1)(adapter);
                    }
                    SectionPluginEntry {
                        instance_id,
                        priority: i16::try_from(i).expect("few sections"),
                        key,
                        plugin: adapter,
                    }
                })
                .collect();
            assert!(
                wrap_sections.is_empty(),
                "a section wrap names a section the gate does not have: {:?}",
                wrap_sections
                    .iter()
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>()
            );
            let mut sink: Arc<dyn AuditSink> = Arc::new(GuardedAuditSink::new(
                "audit-instance",
                Arc::clone(&audit) as _,
                guard,
            ));
            if let Some(wrap) = wrap_audit {
                sink = wrap(sink);
            }
            holder
                .publish_result(StartupResult::new(
                    entries,
                    "audit-instance".to_owned(),
                    sink,
                ))
                .expect("published once");
        } else {
            assert!(
                wrap_sections.is_empty() && wrap_audit.is_none(),
                "a wrap on an unpublished gate would be ignored"
            );
        }
        let policy_port: Arc<dyn PolicyPort> = match self.policy_port {
            Some(port) => port,
            None => Arc::clone(&policy) as _,
        };
        let mut orchestrator = Orchestrator::new(
            policy_port,
            registry,
            Arc::clone(&holder),
            self.clock.unwrap_or_else(|| {
                Arc::new(FixedClock::new(
                    UNIX_EPOCH + Duration::from_secs(1_800_000_000),
                ))
            }),
            Arc::new(SequentialIdSource::starting_at(self.request_ids_from)),
            self.settings,
        );
        if let Some(validation) = self.validation {
            orchestrator = orchestrator.with_validation(validation);
        }
        Gate {
            orchestrator,
            policy,
            memory,
            hung,
            registry_hangs: self.registry_hangs,
            audit,
            sections,
            events,
            holder,
        }
    }
}

/// A startup result with an audit plugin and no section plugin (a published gear to start
/// again, or to refuse a second start).
pub fn startup_result_without_sections() -> StartupResult {
    let sink = Arc::new(GuardedAuditSink::new(
        "audit-instance",
        Arc::new(FakeAuditPlugin::new()) as _,
        PluginCallGuard::new(PLUGIN_TIMEOUT, 4),
    ));
    StartupResult::new(Vec::new(), "audit-instance".to_owned(), sink)
}

/// The real `PolicyAdapter` (deadline [`PDP_TIMEOUT`]) over a fake PDP that answers `script`,
/// and the fake PDP that records what the adapter sent it.
pub fn policy_adapter(script: AuthzScript) -> (Arc<dyn PolicyPort>, Arc<FakeAuthz>) {
    let authz = Arc::new(FakeAuthz::new(script));
    let adapter = PolicyAdapter::new(Arc::clone(&authz) as Arc<dyn AuthZResolverApi>, PDP_TIMEOUT);
    (Arc::new(adapter), authz)
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Default)]
struct LatchState {
    entered: usize,
    finished: usize,
    released: bool,
    gave_up: bool,
}

/// A validation seam that blocks the blocking-pool thread until the test releases it, so a
/// test can hold a validation open without depending on how fast a CPU runs a heavy schema.
/// A validation that nobody releases gives up after `SAFETY` and sets [`Self::gave_up`], so a
/// validation running on the async runtime thread (which could never be released) fails the
/// test instead of hanging it.
#[derive(Clone, Default)]
pub struct ValidationLatch(Arc<(Mutex<LatchState>, Condvar)>);

impl ValidationLatch {
    const SAFETY: Duration = Duration::from_secs(10);

    /// A latch that holds every validation until [`Self::release`].
    pub fn new() -> Self {
        Self::default()
    }

    /// The validation to give [`GateBuilder::validation`]: blocks, then reports no problem.
    pub(crate) fn validation(&self) -> Arc<ValidationFn> {
        let this = self.clone();
        Arc::new(
            move |_schemas: &[SectionSchema],
                  _manifest: Option<&Value>|
                  -> Result<Vec<Problem>, Vec<crate::domain::schema_validator::UnusableSchema>> {
                let (state, wakeup) = &*this.0;
                let mut guard = lock(state);
                guard.entered += 1;
                wakeup.notify_all();
                let deadline = std::time::Instant::now() + Self::SAFETY;
                while !guard.released {
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    if left.is_zero() {
                        guard.gave_up = true;
                        break;
                    }
                    guard = wakeup
                        .wait_timeout(guard, left)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
                guard.finished += 1;
                wakeup.notify_all();
                Ok(Vec::new())
            },
        )
    }

    /// Lets every held and later validation finish.
    pub fn release(&self) {
        let (state, wakeup) = &*self.0;
        lock(state).released = true;
        wakeup.notify_all();
    }

    /// Validations that started.
    pub fn entered(&self) -> usize {
        lock(&self.0.0).entered
    }

    /// Validations that returned.
    pub fn finished(&self) -> usize {
        lock(&self.0.0).finished
    }

    /// Whether any validation gave up waiting for the release.
    pub fn gave_up(&self) -> bool {
        lock(&self.0.0).gave_up
    }

    /// Waits (on a real clock, from an async test) until `finished` validations returned.
    pub async fn wait_finished(&self, finished: usize) {
        let until = std::time::Instant::now() + Self::SAFETY;
        while self.finished() < finished {
            assert!(
                std::time::Instant::now() < until,
                "the held validation never returned"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

/// An orchestrator and every fake it runs over.
pub struct Gate {
    /// The orchestrator under test.
    pub orchestrator: Orchestrator,
    /// The scripted policy.
    pub policy: Arc<FakePolicy>,
    /// The in-memory registry (also the one the orchestrator uses unless it hangs).
    pub memory: Arc<InMemoryRegistryStore>,
    hung: Arc<NeverAnsweringRegistry>,
    registry_hangs: bool,
    /// The audit plugin behind the guarded sink.
    pub audit: Arc<FakeAuditPlugin>,
    /// The section plugins, in priority order.
    pub sections: Vec<Arc<FakeSectionPlugin>>,
    /// The ordered events of every fake.
    pub events: Arc<EventLog>,
    /// The startup result holder.
    pub holder: Arc<StartupResultHolder>,
}

impl Gate {
    /// A seed through the gate.
    pub async fn seed(&self, body: Vec<u8>) -> GateResult {
        self.orchestrator
            .pre_dispatch(&security_context(), RequestInput::Seed { body })
            .await
    }

    /// An uninstall through the gate.
    pub async fn uninstall(&self, id: &str) -> GateResult {
        self.orchestrator
            .pre_dispatch(
                &security_context(),
                RequestInput::Uninstall {
                    application_id: id.to_owned(),
                },
            )
            .await
    }

    /// The full request of a seed (P7): the permit, the detached request task, and its result.
    ///
    /// # Panics
    ///
    /// Panics when the request is refused at the cap or its supervisor fails to join.
    pub async fn run_seed(&self, body: Vec<u8>) -> OutcomeReport {
        self.run(RequestInput::Seed { body }).await
    }

    /// The full request of an uninstall (P7).
    ///
    /// # Panics
    ///
    /// Panics when the request is refused at the cap or its supervisor fails to join.
    pub async fn run_uninstall(&self, id: &str) -> OutcomeReport {
        self.run(RequestInput::Uninstall {
            application_id: id.to_owned(),
        })
        .await
    }

    /// The full request of `input`: what the REST handler does, awaiting the supervisor.
    ///
    /// # Panics
    ///
    /// Panics when the request is refused at the cap or its supervisor fails to join.
    pub async fn run(&self, input: RequestInput) -> OutcomeReport {
        self.start(input).await.expect("the supervisor joins")
    }

    /// Starts the full request and returns the supervisor's handle, which the caller may drop (a
    /// disconnect) or await.
    ///
    /// # Panics
    ///
    /// Panics when the request is refused at the cap.
    pub fn start(&self, input: RequestInput) -> RequestHandle {
        self.try_start(input)
            .expect("the request-task cap is not reached")
    }

    /// Like [`Self::start`], with the cap refusal returned.
    ///
    /// # Errors
    ///
    /// [`AtCapacity`] when no request-task permit is free.
    pub fn try_start(&self, input: RequestInput) -> Result<RequestHandle, AtCapacity> {
        let operation = match &input {
            RequestInput::Seed { .. } => HandoffOperation::Seed,
            RequestInput::Uninstall { .. } => HandoffOperation::Uninstall,
        };
        let permit = self.orchestrator.try_admit(operation)?;
        Ok(self
            .orchestrator
            .start_request(permit, security_context(), input))
    }

    /// The registry calls the orchestrator made.
    pub fn registry_calls(&self) -> Vec<RegistryCall> {
        if self.registry_hangs {
            self.hung.calls()
        } else {
            self.memory.calls()
        }
    }

    /// The events whose text starts with `prefix`.
    pub fn events_with(&self, prefix: &str) -> Vec<String> {
        self.events
            .events()
            .into_iter()
            .filter(|e| e.starts_with(prefix))
            .collect()
    }
}

/// The decided report of a gate result; panics when the gate let the request proceed.
pub fn decided(result: &GateResult) -> &OutcomeReport {
    match &result.outcome {
        GateOutcome::Decided(report) => report,
        GateOutcome::Proceed(p) => panic!("expected a decided outcome, got Proceed: {p:?}"),
    }
}
