//! Test harness of the pre-dispatch gate (P6): an `Orchestrator` over scripted ports, a real
//! startup result (guarded section plugin adapters over `FakeSectionPlugin`s and a guarded
//! audit sink over `FakeAuditPlugin`), and the shared `EventLog` that orders every call.
#![allow(clippy::duration_suboptimal_units)]

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, UNIX_EPOCH};

use application_manager_sdk::models::{OutcomeReport, Problem};
use authz_resolver_sdk::AuthZResolverApi;
use serde_json::{Value, json};
use toolkit_security::SecurityContext;
use uuid::Uuid;

use crate::domain::orchestrator::{
    GateOutcome, GateResult, Orchestrator, OrchestratorSettings, RequestInput, ValidationFn,
};
use crate::domain::ports::{PolicyPort, RegistryStore};
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
}

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
            },
            audit: FakeAuditPlugin::new(),
            guard_cap: 64,
            policy_port: None,
            validation: None,
        }
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
        let events = EventLog::new();
        let policy = Arc::new(FakePolicy::new(self.policy).with_event_log(Arc::clone(&events)));
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
        if self.publish {
            let entries = sections
                .iter()
                .enumerate()
                .map(|(i, plugin)| {
                    let key = self.sections[i].0.clone();
                    let instance_id = format!("section-{i}");
                    let adapter = Arc::new(GuardedSectionPlugin::new(
                        &instance_id,
                        &key,
                        Arc::clone(plugin) as _,
                        guard.clone(),
                    ));
                    SectionPluginEntry {
                        instance_id,
                        priority: i16::try_from(i).expect("few sections"),
                        key,
                        plugin: adapter,
                    }
                })
                .collect();
            let sink = Arc::new(GuardedAuditSink::new(
                "audit-instance",
                Arc::clone(&audit) as _,
                guard,
            ));
            holder
                .publish_result(StartupResult::new(
                    entries,
                    "audit-instance".to_owned(),
                    sink,
                ))
                .expect("published once");
        }
        let policy_port: Arc<dyn PolicyPort> = match self.policy_port {
            Some(port) => port,
            None => Arc::clone(&policy) as _,
        };
        let mut orchestrator = Orchestrator::new(
            policy_port,
            registry,
            Arc::clone(&holder),
            Arc::new(FixedClock::new(
                UNIX_EPOCH + Duration::from_secs(1_800_000_000),
            )),
            Arc::new(SequentialIdSource::new()),
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
