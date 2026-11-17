//! Test fixtures shared by the config and gear tests.

use serde_json::{Value, json};

/// The valid operator configuration, defined once.
pub fn valid_config_json() -> Value {
    json!({
        "plugin_call_timeout": "10s",
        "run_window_margin": "30s",
        "operation_retention": "7d",
        "plugin_max_in_flight": 4,
        "max_request_tasks": 16,
        "max_envelope_text_length": 255,
        "pdp_call_timeout": "2s",
        "allowed_callers": [
            {
                "identity": {
                    "type": "KubernetesServiceAccount",
                    "namespace": "deploy",
                    "service_account": "app-seeder"
                },
                "actions": ["seed", "uninstall", "read_schema", "read_operation"]
            }
        ],
        "storage_call_timeout": "2s"
    })
}

// ── Doubles shared by the domain, infra, REST and gear tests ─────────────────

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use application_manager_sdk::{
    ApplicationManagerSectionPluginSpecV1, CallerIdentity, Envelope, SECTION_BASE_TYPE,
    SectionPluginClientV1, SectionPluginError,
};
use async_trait::async_trait;
use gts::{GtsInstanceId, GtsSchema};
use toolkit::client_hub::{ClientHub, ClientScope};
use toolkit::gts::PluginV1;
use toolkit_security::{PlatformIdentity, PlatformSecurityContext};
use types_registry_sdk::testing::{MockTypesRegistryClient, make_test_instance};
use types_registry_sdk::{GtsTypeId, GtsTypeSchema, TypesRegistryClient};
use uuid::Uuid;

use crate::domain::actions::CallerAction;
use crate::domain::allowed_callers::{AllowedCallers, CallerPattern};
use crate::domain::plugin_set::SectionPlugin;
use crate::domain::schema_validator::compile_section;

/// The namespace of every test service account.
pub const NAMESPACE: &str = "deploy";

/// A platform context holding the service account `name` in [`NAMESPACE`].
pub fn platform_ctx(name: &str) -> PlatformSecurityContext {
    PlatformSecurityContext::new(PlatformIdentity::KubernetesServiceAccount {
        namespace: NAMESPACE.to_owned(),
        service_account: name.to_owned(),
        pod: Some("pod-1".to_owned()),
    })
}

/// Allowed callers granting `actions` to the service account `name` in
/// [`NAMESPACE`].
pub fn allowed_callers(name: &str, actions: Vec<CallerAction>) -> AllowedCallers {
    AllowedCallers::new([(
        CallerPattern::KubernetesServiceAccount {
            namespace: NAMESPACE.to_owned(),
            service_account: name.to_owned(),
        },
        actions,
    )])
}

/// How a [`FakePlugin`] answers `apply` and `remove`.
#[derive(Clone, Debug)]
pub enum Answer {
    Ok,
    Rejected,
    NotPermitted,
    Internal,
    Panic,
    Hang,
}

/// A section plugin that answers every call alike and records each call as
/// `(call, operation id, caller)`.
pub struct FakePlugin {
    answer: Answer,
    pub calls: AtomicUsize,
    pub seen: std::sync::Mutex<Vec<(&'static str, Uuid, CallerIdentity)>>,
}

impl FakePlugin {
    pub fn new() -> Arc<Self> {
        Self::answering(Answer::Ok)
    }

    pub fn answering(answer: Answer) -> Arc<Self> {
        Arc::new(Self {
            answer,
            calls: AtomicUsize::new(0),
            seen: std::sync::Mutex::new(Vec::new()),
        })
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    async fn answer(
        &self,
        call: &'static str,
        caller: &CallerIdentity,
        operation_id: Uuid,
    ) -> Result<(), SectionPluginError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .expect("lock")
            .push((call, operation_id, caller.clone()));
        match &self.answer {
            Answer::Ok => Ok(()),
            Answer::Rejected => Err(SectionPluginError::Rejected {
                detail: "bad".to_owned(),
                location: Some("/a".to_owned()),
            }),
            Answer::NotPermitted => Err(SectionPluginError::NotPermitted {
                detail: "no".to_owned(),
            }),
            Answer::Internal => Err(SectionPluginError::Internal {
                message: "broken".to_owned(),
            }),
            Answer::Panic => panic!("plugin panicked"),
            Answer::Hang => std::future::pending().await,
        }
    }
}

#[async_trait]
impl SectionPluginClientV1 for FakePlugin {
    async fn apply(
        &self,
        caller: &CallerIdentity,
        operation_id: Uuid,
        _envelope: &Envelope,
        _section: &Value,
    ) -> Result<(), SectionPluginError> {
        self.answer("apply", caller, operation_id).await
    }

    async fn remove(
        &self,
        caller: &CallerIdentity,
        operation_id: Uuid,
        _application_id: &str,
    ) -> Result<(), SectionPluginError> {
        self.answer("remove", caller, operation_id).await
    }
}

/// A [`SectionPlugin`] with section key `key`, as startup would build it.
pub fn section_plugin(key: &str, priority: i16, client: &Arc<FakePlugin>) -> SectionPlugin {
    let section_type = format!("{}{key}.v1~", base_type_id());
    let section = compile_section(key, &section_type, &json!({ "type": "object" }))
        .expect("an object schema compiles");
    SectionPlugin {
        key: key.to_owned(),
        section_type,
        priority,
        instance_id: format!("id-{key}"),
        client: client.clone(),
        section: Arc::new(section),
    }
}

/// The GTS id of the section base type.
pub fn base_type_id() -> &'static str {
    SECTION_BASE_TYPE
}

/// The section type id derived from the base type by `segment`.
pub fn section_type(segment: &str) -> String {
    format!("{}{segment}~", base_type_id())
}

/// The full GTS instance id of a section plugin instance named `segment`.
pub fn instance_id(segment: &str) -> String {
    format!(
        "{}{segment}",
        <ApplicationManagerSectionPluginSpecV1 as GtsSchema>::TYPE_ID
    )
}

/// The registry content of a section plugin instance naming `section_type`.
pub fn instance_content(segment: &str, priority: i16, section_type: &str) -> Value {
    serde_json::to_value(PluginV1::<ApplicationManagerSectionPluginSpecV1> {
        id: GtsInstanceId::new(
            <ApplicationManagerSectionPluginSpecV1 as GtsSchema>::TYPE_ID,
            segment,
        ),
        vendor: "constructorfabric".to_owned(),
        priority,
        properties: ApplicationManagerSectionPluginSpecV1 {
            section_type: section_type.to_owned(),
        },
    })
    .expect("registration")
}

/// The registered schema of `type_id`, derived from the section base type
/// when `type_id` is.
pub fn type_schema(type_id: &str, schema: Value) -> GtsTypeSchema {
    let base = Arc::new(
        GtsTypeSchema::try_new(
            GtsTypeId::new(base_type_id()),
            application_manager_sdk::section_base_type_schema(),
            None,
            None,
        )
        .expect("base type"),
    );
    let parent = GtsTypeSchema::derive_parent_type_id(type_id).map(|p| {
        if p.as_ref() == base_type_id() {
            base
        } else {
            Arc::new(types_registry_sdk::testing::make_test_type_schema(
                p.as_ref(),
            ))
        }
    });
    GtsTypeSchema::try_new(GtsTypeId::new(type_id), schema, None, parent).expect("type schema")
}

/// A plugin for [`hub_with`]: instance segment, registry content, client
/// (`None` registers no client), and its section type (`None` registers no
/// type schema).
pub struct PluginSpec {
    pub segment: &'static str,
    pub content: Value,
    pub client: Option<Arc<FakePlugin>>,
    pub section_type: Option<GtsTypeSchema>,
}

impl PluginSpec {
    /// A plugin whose section type is derived from the base type by
    /// `type_segment`, e.g. `cf.test.a.section.v1`.
    pub fn new(segment: &'static str, priority: i16, type_segment: &str) -> Self {
        let type_id = section_type(type_segment);
        Self {
            segment,
            content: instance_content(segment, priority, &type_id),
            client: Some(FakePlugin::new()),
            section_type: Some(type_schema(&type_id, json!({ "type": "object" }))),
        }
    }

    /// A plugin naming `type_id`, registered with a synthetic schema chain.
    pub fn naming(segment: &'static str, priority: i16, type_id: &str) -> Self {
        Self {
            segment,
            content: instance_content(segment, priority, type_id),
            client: Some(FakePlugin::new()),
            section_type: Some(type_schema(type_id, json!({ "type": "object" }))),
        }
    }
}

/// A hub with a mock types registry listing `plugins`, their section types,
/// and their clients.
pub fn hub_with(plugins: &[PluginSpec]) -> Arc<ClientHub> {
    hub_with_registry(plugins, MockTypesRegistryClient::new())
}

/// [`hub_with`] over a prepared registry mock.
pub fn hub_with_registry(
    plugins: &[PluginSpec],
    registry: MockTypesRegistryClient,
) -> Arc<ClientHub> {
    let hub = Arc::new(ClientHub::new());
    let instances = plugins
        .iter()
        .map(|p| make_test_instance(&instance_id(p.segment), p.content.clone()));
    let types = plugins.iter().filter_map(|p| p.section_type.clone());
    let registry: Arc<dyn TypesRegistryClient> =
        Arc::new(registry.with_instances(instances).with_type_schemas(types));
    hub.register::<dyn TypesRegistryClient>(registry);
    for p in plugins {
        if let Some(client) = &p.client {
            let client: Arc<dyn SectionPluginClientV1> = client.clone();
            hub.register_scoped::<dyn SectionPluginClientV1>(
                ClientScope::gts_id(&instance_id(p.segment)),
                client,
            );
        }
    }
    hub
}

// ── Operation storage for the orchestrator, read-service and route tests ─────

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use application_manager_sdk::{OutcomeReport, RequestCategory, SectionOutcome};
use sea_orm_migration::MigratorTrait;
use toolkit_db::migration_runner::run_migrations_for_testing;
use toolkit_db::{ConnectOpts, DBProvider, DbError, connect_db};

use crate::domain::history::HistoryEvent;
use crate::domain::operation::{
    Accepted, ApplicationRecord, NewOperation, OpenOperation, Operation,
};
use crate::domain::ports::{BuildRefused, OperationRepository, StorageError};
use crate::infra::storage::{Migrator, SeaOrmOperationRepository};

/// A repository over a fresh in-memory `SQLite` database built from the
/// gear's migration.
pub async fn sqlite_repo() -> Arc<SeaOrmOperationRepository> {
    let dsn = format!(
        "sqlite:file:application_manager_test_{}?mode=memory&cache=shared",
        Uuid::new_v4()
    );
    let db = connect_db(
        &dsn,
        ConnectOpts {
            max_conns: Some(1),
            min_conns: Some(1),
            ..Default::default()
        },
    )
    .await
    .expect("connect sqlite");
    run_migrations_for_testing(&db, Migrator::migrations())
        .await
        .expect("run migrations");
    Arc::new(SeaOrmOperationRepository::new(
        Arc::new(DBProvider::<DbError>::new(db)),
        Duration::from_secs(10),
    ))
}

/// What a [`HookedRepo`] does instead of, or before, a call.
#[derive(Clone, Copy, Debug)]
pub enum Hook {
    /// Fail with [`StorageError::Unavailable`].
    Fail,
    /// Panic.
    Panic,
    /// Wait, then run the call.
    Sleep(Duration),
    /// `open_operation` only: answer that no operation is open.
    NoneOpen,
}

/// A repository that runs every call on a real store, counts the calls, and
/// applies the hook set for a method.
pub struct HookedRepo {
    inner: Arc<dyn OperationRepository>,
    hooks: Mutex<HashMap<&'static str, Hook>>,
    calls: Mutex<HashMap<&'static str, usize>>,
}

impl HookedRepo {
    pub async fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: sqlite_repo().await,
            hooks: Mutex::new(HashMap::new()),
            calls: Mutex::new(HashMap::new()),
        })
    }

    pub fn hook(&self, method: &'static str, hook: Hook) {
        self.hooks.lock().expect("lock").insert(method, hook);
    }

    pub fn calls(&self, method: &'static str) -> usize {
        self.calls
            .lock()
            .expect("lock")
            .get(method)
            .copied()
            .unwrap_or(0)
    }

    /// Count the call and apply its hook: `Err` to fail it, `Ok(true)` when
    /// `open_operation` must answer that none is open.
    async fn enter(&self, method: &'static str) -> Result<bool, StorageError> {
        *self.calls.lock().expect("lock").entry(method).or_default() += 1;
        let hook = self.hooks.lock().expect("lock").get(method).copied();
        match hook {
            Some(Hook::Fail) => Err(StorageError::Unavailable),
            Some(Hook::Panic) => panic!("injected panic in {method}"),
            Some(Hook::Sleep(wait)) => {
                tokio::time::sleep(wait).await;
                Ok(false)
            }
            Some(Hook::NoneOpen) => Ok(true),
            None => Ok(false),
        }
    }

    /// All history events of `correlation_id`.
    pub async fn events(&self, correlation_id: Uuid) -> Vec<HistoryEvent> {
        self.inner
            .history_page(Some(vec![correlation_id]), None, 100)
            .await
            .expect("history")
    }

    /// The operation once it is completed, waiting for the request task.
    pub async fn completed(&self, operation_id: Uuid) -> Operation {
        for _ in 0..500 {
            if let Ok(Some(op)) = self.inner.read_operation(operation_id).await
                && op.status == crate::domain::operation::OperationStatus::Completed
            {
                return op;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("operation {operation_id} did not complete");
    }
}

#[async_trait]
impl OperationRepository for HookedRepo {
    async fn open_operation(
        &self,
        application_id: &str,
    ) -> Result<Option<OpenOperation>, StorageError> {
        if self.enter("open_operation").await? {
            return Ok(None);
        }
        self.inner.open_operation(application_id).await
    }

    async fn accept(
        &self,
        new: NewOperation,
        accepted: HistoryEvent,
    ) -> Result<Accepted, StorageError> {
        self.enter("accept").await?;
        self.inner.accept(new, accepted).await
    }

    async fn refuse_after_lost_race(
        &self,
        application_id: &str,
        build_refused: BuildRefused,
    ) -> Result<Uuid, StorageError> {
        self.enter("refuse_after_lost_race").await?;
        self.inner
            .refuse_after_lost_race(application_id, build_refused)
            .await
    }

    async fn record_refused(&self, event: HistoryEvent) -> Result<(), StorageError> {
        self.enter("record_refused").await?;
        self.inner.record_refused(event).await
    }

    async fn mark_running(
        &self,
        operation_id: Uuid,
        sections: &[(String, Option<String>)],
    ) -> Result<Duration, StorageError> {
        self.enter("mark_running").await?;
        self.inner.mark_running(operation_id, sections).await
    }

    async fn record_section_outcome(
        &self,
        operation_id: Uuid,
        key: &str,
        outcome: SectionOutcome,
        detail: Option<String>,
        location: Option<String>,
    ) -> Result<(), StorageError> {
        self.enter("record_section_outcome").await?;
        self.inner
            .record_section_outcome(operation_id, key, outcome, detail, location)
            .await
    }

    async fn finish(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        category: RequestCategory,
    ) -> Result<(), StorageError> {
        self.enter("finish").await?;
        self.inner.finish(operation_id, report, category).await
    }

    async fn complete_by_supervisor(&self, operation_id: Uuid) -> Result<(), StorageError> {
        self.enter("complete_by_supervisor").await?;
        self.inner.complete_by_supervisor(operation_id).await
    }

    async fn expire_if_expired(&self, operation_id: Uuid) -> Result<bool, StorageError> {
        self.enter("expire_if_expired").await?;
        self.inner.expire_if_expired(operation_id).await
    }

    async fn expire_batch(&self, limit: u64) -> Result<u64, StorageError> {
        self.enter("expire_batch").await?;
        self.inner.expire_batch(limit).await
    }

    async fn purge(&self, retention: Duration, limit: u64) -> Result<u64, StorageError> {
        self.enter("purge").await?;
        self.inner.purge(retention, limit).await
    }

    async fn read_operation(&self, operation_id: Uuid) -> Result<Option<Operation>, StorageError> {
        self.enter("read_operation").await?;
        self.inner.read_operation(operation_id).await
    }

    async fn history_page(
        &self,
        correlation_ids: Option<Vec<Uuid>>,
        after: Option<Uuid>,
        limit: u64,
    ) -> Result<Vec<HistoryEvent>, StorageError> {
        self.enter("history_page").await?;
        self.inner.history_page(correlation_ids, after, limit).await
    }

    async fn applications_page(
        &self,
        application_ids: Option<Vec<String>>,
        after: Option<String>,
        limit: u64,
    ) -> Result<Vec<ApplicationRecord>, StorageError> {
        self.enter("applications_page").await?;
        self.inner
            .applications_page(application_ids, after, limit)
            .await
    }
}

// ── Access control and signals for the read-service, policy and REST tests ───

use authz_resolver_sdk::AuthZResolverApi;
use authz_resolver_sdk::constraints::{Constraint, EqPredicate, Predicate};
use authz_resolver_sdk::models::{
    EvaluationRequest, EvaluationResponse, EvaluationResponseContext,
};
use toolkit_canonical_errors::CanonicalError;
use toolkit_security::{SecurityContext, pep_properties};

use crate::domain::actions::TenantAction;
use crate::domain::ports::{AccessControl, AccessError};
use crate::domain::signals::Signals;

/// An authenticated tenant-plane caller.
pub fn tenant_ctx() -> SecurityContext {
    SecurityContext::builder()
        .subject_id(Uuid::from_u128(0xA))
        .subject_tenant_id(Uuid::from_u128(0xB))
        .build()
        .expect("test ctx")
}

/// How a [`FakeAuthZ`] answers.
#[derive(Clone, Copy, Debug)]
pub enum Pdp {
    Permit,
    Deny,
    ConstrainedPermit,
    Fail,
    Hang,
}

/// A PDP double that records every request.
pub struct FakeAuthZ {
    answer: Pdp,
    pub requests: Mutex<Vec<EvaluationRequest>>,
}

impl FakeAuthZ {
    pub fn new(answer: Pdp) -> Arc<Self> {
        Arc::new(Self {
            answer,
            requests: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl AuthZResolverApi for FakeAuthZ {
    async fn evaluate(
        &self,
        _ctx: PlatformSecurityContext,
        request: EvaluationRequest,
    ) -> Result<EvaluationResponse, CanonicalError> {
        self.requests.lock().expect("lock").push(request);
        let permit = |constraints| EvaluationResponse {
            decision: true,
            context: EvaluationResponseContext {
                constraints,
                ..Default::default()
            },
        };
        match self.answer {
            Pdp::Permit => Ok(permit(vec![])),
            Pdp::Deny => Ok(EvaluationResponse {
                decision: false,
                context: EvaluationResponseContext::default(),
            }),
            Pdp::ConstrainedPermit => Ok(permit(vec![Constraint {
                predicates: vec![Predicate::Eq(EqPredicate::new(
                    pep_properties::OWNER_TENANT_ID,
                    Uuid::from_u128(0xB),
                ))],
            }])),
            Pdp::Fail => Err(CanonicalError::service_unavailable()
                .with_detail("pdp down")
                .create()),
            Pdp::Hang => std::future::pending().await,
        }
    }
}

/// An [`AccessControl`] double with a fixed answer that records each action.
pub struct FakeAccess {
    answer: Result<(), AccessError>,
    pub actions: Mutex<Vec<TenantAction>>,
}

impl FakeAccess {
    pub fn new(answer: Result<(), AccessError>) -> Arc<Self> {
        Arc::new(Self {
            answer,
            actions: Mutex::new(Vec::new()),
        })
    }

    pub fn permit() -> Arc<Self> {
        Self::new(Ok(()))
    }

    pub fn calls(&self) -> usize {
        self.actions.lock().expect("lock").len()
    }
}

#[async_trait]
impl AccessControl for FakeAccess {
    async fn authorize(
        &self,
        _ctx: &SecurityContext,
        action: TenantAction,
    ) -> Result<(), AccessError> {
        self.actions.lock().expect("lock").push(action);
        self.answer
    }
}

/// Signals recorded as `name:label,label` lines, in order.
#[derive(Default)]
pub struct RecordedSignals {
    pub lines: Mutex<Vec<String>>,
}

impl RecordedSignals {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn push(&self, line: String) {
        self.lines.lock().expect("lock").push(line);
    }

    /// Every recorded line.
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().expect("lock").clone()
    }

    /// How many recorded lines equal `line`.
    pub fn count(&self, line: &str) -> usize {
        self.lines().iter().filter(|l| *l == line).count()
    }
}

impl Signals for RecordedSignals {
    fn request(&self, action: &'static str, outcome: &'static str) {
        self.push(format!("request:{action},{outcome}"));
    }
    fn operation_completed(
        &self,
        action: &'static str,
        category: &'static str,
        reason: &'static str,
    ) {
        self.push(format!("completed:{action},{category},{reason}"));
    }
    fn operation_duration(&self, action: &'static str, _took: Duration) {
        self.push(format!("duration:{action}"));
    }
    fn section_call(&self, key: &str, outcome: &'static str) {
        self.push(format!("section:{key},{outcome}"));
    }
    fn caller_refused(&self, action: &'static str) {
        self.push(format!("refused:{action}"));
    }
    fn request_tasks(&self, delta: i64) {
        self.push(format!("tasks:{delta}"));
    }
    fn history_event(&self, recorded: bool) {
        self.push(format!("history:{recorded}"));
    }
    fn dependency_unavailable(&self, dependency: &'static str) {
        self.push(format!("unavailable:{dependency}"));
    }
    fn sweep(&self, step: &'static str, succeeded: bool) {
        self.push(format!("sweep:{step},{succeeded}"));
    }
    fn operations_expired(&self, count: u64) {
        self.push(format!("expired:{count}"));
    }
    fn startup_failed(&self, reason: &'static str) {
        self.push(format!("startup_failed:{reason}"));
    }
}
