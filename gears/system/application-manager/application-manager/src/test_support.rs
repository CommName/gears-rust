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
    ApplicationManagerSectionPluginSpecV1, CallerIdentity, Envelope, OutcomeReport,
    RequestCategory, SectionPluginClientV1, SectionPluginError, SectionSchema,
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
use crate::domain::actions::Action;
use crate::domain::audit::AuditEvent;
use crate::domain::operation::{NewOperation, OpenOperation, Operation};
use crate::domain::ports::{
    AccessControl, AccessError, BuildRefused, Clock, OperationRepository, StorageError,
};

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
/// How a [`FakePlugin`] answers `apply` (and `remove`).
#[derive(Clone)]
pub enum ApplyAnswer {
    Ok,
    Rejected {
        detail: String,
        location: Option<String>,
    },
    NotPermitted(String),
    Internal,
    Panic,
    Hang,
}

/// A section plugin that answers every call alike and records each call as
/// `(call, operation id, caller)`.
pub struct FakePlugin {
    answer: Answer,
/// How a [`FakePlugin`] answers `remove`.
pub type RemoveAnswer = ApplyAnswer;

/// The ordered log of calls of every plugin that shares it: plugin name and
/// method (`schema`, `apply` or `remove`).
pub type CallLog = Arc<Mutex<Vec<(String, &'static str)>>>;

/// Runs at the start of every `apply` and `remove` of a [`FakePlugin`], with
/// the operation id, before the plugin answers.
pub type CallHook = Arc<dyn Fn(Uuid) -> futures::future::BoxFuture<'static, ()> + Send + Sync>;

/// A section plugin that counts every call.
#[derive(Default)]
pub struct FakePlugin {
    key: KeyAnswer,
    schema: Mutex<SchemaAnswer>,
    apply: Mutex<ApplyAnswer>,
    remove: Mutex<RemoveAnswer>,
    log: Mutex<Option<(String, CallLog)>>,
    hook: Mutex<Option<CallHook>>,
    /// What every `apply` received: operation id, envelope and section.
    pub apply_args: Mutex<Vec<(Uuid, Envelope, Value)>>,
    /// What every `remove` received: operation id and application id.
    pub remove_args: Mutex<Vec<(Uuid, String)>>,
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
        Arc::new(Self::default())
    /// Answers `key` with `key`, and serves `{}` with schema version "1".
    pub fn new(key: KeyAnswer) -> Arc<Self> {
        Self::serving(key, SchemaAnswer::serve(serde_json::json!({}), "1"))
    }

    pub fn serving(key: KeyAnswer, schema: SchemaAnswer) -> Arc<Self> {
        Arc::new(Self {
            key,
            schema: Mutex::new(schema),
            apply: Mutex::new(ApplyAnswer::Ok),
            remove: Mutex::new(ApplyAnswer::Ok),
            log: Mutex::new(None),
            hook: Mutex::new(None),
            apply_args: Mutex::new(Vec::new()),
            remove_args: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
        })
    }

    /// Change how later `apply` calls are answered.
    pub fn set_apply(&self, answer: ApplyAnswer) {
        *self.apply.lock().expect("lock") = answer;
    }

    /// Change how later `remove` calls are answered.
    pub fn set_remove(&self, answer: RemoveAnswer) {
        *self.remove.lock().expect("lock") = answer;
    }

    /// Record every `schema`, `apply` and `remove` call as `name` in `log`.
    pub fn log_to(&self, name: &str, log: &CallLog) {
        *self.log.lock().expect("lock") = Some((name.to_owned(), Arc::clone(log)));
    }

    /// Run `hook` at the start of every `apply` and `remove`.
    pub fn before_call(&self, hook: CallHook) {
        *self.hook.lock().expect("lock") = Some(hook);
    }

    fn record(&self, method: &'static str) {
        if let Some((name, log)) = self.log.lock().expect("lock").as_ref() {
            log.lock().expect("lock").push((name.clone(), method));
        }
    }

    async fn run_hook(&self, operation_id: Uuid) {
        let hook = self.hook.lock().expect("lock").clone();
        if let Some(hook) = hook {
            hook(operation_id).await;
        }
    }

    async fn answer(&self, answer: ApplyAnswer) -> Result<(), SectionPluginError> {
        match answer {
            ApplyAnswer::Ok => Ok(()),
            ApplyAnswer::Rejected { detail, location } => {
                Err(SectionPluginError::Rejected { detail, location })
            }
            ApplyAnswer::NotPermitted(detail) => Err(SectionPluginError::NotPermitted { detail }),
            ApplyAnswer::Internal => Err(SectionPluginError::Internal {
                message: "call failed".to_owned(),
            }),
            ApplyAnswer::Panic => panic!("call panicked"),
            ApplyAnswer::Hang => std::future::pending().await,
        }
    }

    /// Change how later `schema` calls are answered.
    pub fn set_schema(&self, schema: SchemaAnswer) {
        *self.schema.lock().expect("lock") = schema;
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
    async fn key(&self) -> Result<String, SectionPluginError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.key {
            KeyAnswer::Key(k) => Ok(k.to_owned()),
            KeyAnswer::Error => Err(SectionPluginError::Internal {
                message: "key failed".to_owned(),
            }),
            KeyAnswer::Panic => panic!("key panicked"),
            KeyAnswer::Hang => std::future::pending().await,
        }
    }

    async fn schema(&self, _caller: &CallerIdentity) -> Result<SectionSchema, SectionPluginError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.record("schema");
        let answer = self.schema.lock().expect("lock").clone();
        match answer {
            SchemaAnswer::Serve(schema, version) => Ok(SectionSchema::new(schema, version)),
            SchemaAnswer::NotPermitted(detail) => Err(SectionPluginError::NotPermitted { detail }),
            SchemaAnswer::Rejected => Err(SectionPluginError::Rejected {
                detail: "no".to_owned(),
                location: None,
            }),
            SchemaAnswer::Internal => Err(SectionPluginError::Internal {
                message: "schema failed".to_owned(),
            }),
            SchemaAnswer::Panic => panic!("schema panicked"),
            SchemaAnswer::Hang => std::future::pending().await,
        }
    }

    async fn apply(
        &self,
        caller: &CallerIdentity,
        operation_id: Uuid,
        _envelope: &Envelope,
        _section: &Value,
    ) -> Result<(), SectionPluginError> {
        self.answer("apply", caller, operation_id).await
        _caller: &CallerIdentity,
        operation_id: Uuid,
        envelope: &Envelope,
        section: &Value,
    ) -> Result<(), SectionPluginError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.record("apply");
        self.apply_args.lock().expect("lock").push((
            operation_id,
            envelope.clone(),
            section.clone(),
        ));
        self.run_hook(operation_id).await;
        let answer = self.apply.lock().expect("lock").clone();
        self.answer(answer).await
    }

    async fn remove(
        &self,
        caller: &CallerIdentity,
        operation_id: Uuid,
        _application_id: &str,
    ) -> Result<(), SectionPluginError> {
        self.answer("remove", caller, operation_id).await
        _caller: &CallerIdentity,
        operation_id: Uuid,
        application_id: &str,
    ) -> Result<(), SectionPluginError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.record("remove");
        self.remove_args
            .lock()
            .expect("lock")
            .push((operation_id, application_id.to_owned()));
        self.run_hook(operation_id).await;
        let answer = self.remove.lock().expect("lock").clone();
        self.answer(answer).await
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

/// An [`AccessControl`] double with a fixed answer that counts its calls.
pub struct FakeAccess {
    answer: Result<(), AccessError>,
    pub calls: AtomicUsize,
    /// The `application_id` of every call.
    pub application_ids: Mutex<Vec<Option<String>>>,
    /// The action of every call.
    pub actions: Mutex<Vec<Action>>,
}

impl FakeAccess {
    pub fn new(answer: Result<(), AccessError>) -> Arc<Self> {
        Arc::new(Self {
            answer,
            calls: AtomicUsize::new(0),
            application_ids: Mutex::new(Vec::new()),
            actions: Mutex::new(Vec::new()),
        })
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AccessControl for FakeAccess {
    async fn authorize(
        &self,
        _ctx: &SecurityContext,
        action: Action,
        application_id: Option<&str>,
    ) -> Result<(), AccessError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.application_ids
            .lock()
            .expect("lock")
            .push(application_id.map(str::to_owned));
        self.actions.lock().expect("lock").push(action);
        self.answer
    }
}

// ── Read service wiring shared by the domain and REST tests ─────────────────

use std::time::Duration;

use crate::domain::plugin_set::{PluginSet, SectionPlugin, StartupState};
use crate::domain::read_service::ReadService;
use crate::domain::router::SectionPluginRouter;
use crate::domain::schema_validator::SchemaValidator;
use crate::infra::call_guard::CallGuard;

/// The plugin call time limit of [`read_service`].
pub const CALL_LIMIT: Duration = Duration::from_secs(10);

/// A plugin set entry for `client` under `key`.
pub fn section(key: &str, priority: i16, client: &Arc<FakePlugin>) -> SectionPlugin {
    SectionPlugin {
        key: key.to_owned(),
        priority,
        instance_id: format!("id-{key}"),
        client: Arc::clone(client) as Arc<dyn SectionPluginClientV1>,
    }
}

/// A startup state that published `plugins`.
pub fn ready(plugins: Vec<SectionPlugin>) -> Arc<StartupState> {
    let state = Arc::new(StartupState::default());
    state.publish(PluginSet::new(plugins)).expect("publish");
    state
}

/// A read service over a real call guard ([`CALL_LIMIT`], cap 4) and a
/// validator with 255-byte envelope texts.
///
/// It has no storage: a storage call panics. Tests of the operation read use
/// [`read_service_over`].
pub fn read_service(access: Arc<FakeAccess>, startup: Arc<StartupState>) -> ReadService {
    read_service_over(access, startup, HookedRepo::none(), FakeClock::new(T0))
}

/// [`read_service`] over `repo` and `clock`.
pub fn read_service_over(
    access: Arc<FakeAccess>,
    startup: Arc<StartupState>,
    repo: Arc<HookedRepo>,
    clock: Arc<FakeClock>,
) -> ReadService {
    let guard = Arc::new(CallGuard::new(CALL_LIMIT, 4));
    ReadService::new(
        access,
        startup,
        SectionPluginRouter::new(guard),
        Arc::new(SchemaValidator::new(MAX_TEXT).expect("envelope schema")),
        repo,
        clock,
    )
}

/// A request orchestrator over a real call guard (`call_timeout`, cap 4), a
/// validator with [`MAX_TEXT`]-byte envelope texts, and [`MARGIN`].
pub fn orchestrator(
    access: Arc<FakeAccess>,
    startup: Arc<StartupState>,
    repo: Arc<HookedRepo>,
    clock: Arc<FakeClock>,
    call_timeout: Duration,
) -> Arc<RequestOrchestrator> {
    let guard = Arc::new(CallGuard::new(call_timeout, 4));
    Arc::new(RequestOrchestrator::new(
        access,
        repo,
        SectionPluginRouter::new(guard),
        Arc::new(SchemaValidator::new(MAX_TEXT).expect("envelope schema")),
        startup,
        clock,
        OrchestratorSettings {
            plugin_call_timeout: call_timeout,
            run_window_margin: MARGIN,
            max_envelope_text_length: MAX_TEXT,
        },
    ))
}

/// The `max_envelope_text_length` of the test wiring.
pub const MAX_TEXT: u32 = 255;

/// The `run_window_margin` of the test wiring.
pub const MARGIN: Duration = Duration::from_secs(30);

/// The start time of [`FakeClock`] in the tests.
pub const T0: OffsetDateTime = time::macros::datetime!(2026-10-22 10:00:00 UTC);

/// A settable clock.
pub struct FakeClock(Mutex<OffsetDateTime>);

impl FakeClock {
    pub fn new(now: OffsetDateTime) -> Arc<Self> {
        Arc::new(Self(Mutex::new(now)))
    }

    pub fn set(&self, now: OffsetDateTime) {
        *self.0.lock().expect("lock") = now;
    }

    pub fn advance(&self, by: Duration) {
        let mut now = self.0.lock().expect("lock");
        *now += by;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> OffsetDateTime {
        *self.0.lock().expect("lock")
    }
}

// ── Operation storage doubles ────────────────────────────────────────────────

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use time::OffsetDateTime;
use toolkit_db::migration_runner::run_migrations_for_testing;
use toolkit_db::{ConnectOpts, DBProvider, DbError, connect_db};

use crate::domain::orchestrator::{OrchestratorSettings, RequestOrchestrator};
use crate::infra::storage::{Migrator, SeaOrmOperationRepository};

/// A real repository over a private in-memory `SQLite` database built from
/// the gear's own migration. Every call carries its own `now`.
pub async fn sqlite_repo() -> Arc<SeaOrmOperationRepository> {
    use sea_orm_migration::MigratorTrait;

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

/// What an injected fault does to one repository method.
#[derive(Clone, Copy, Debug)]
pub enum Fault {
    /// The method answers `StorageError::Unavailable` and writes nothing.
    Unavailable,
    /// The method panics.
    Panic,
}

/// An [`OperationRepository`] that delegates to an inner repository, counts
/// every call, and injects faults per method name (`"accept"`,
/// `"open_operation"`, `"record_report"`, ...). Without an inner repository
/// every call panics: the test asserts that storage is not used.
pub struct HookedRepo {
    inner: Option<Arc<dyn OperationRepository>>,
    faults: Mutex<HashMap<&'static str, Fault>>,
    hide_open: AtomicBool,
    /// Every call of any method.
    pub calls: AtomicUsize,
    /// The application id of every `open_operation` call.
    pub open_operation_ids: Mutex<Vec<String>>,
    /// Every event passed to `record_refused`, written or not.
    pub refused_events: Mutex<Vec<AuditEvent>>,
}

impl HookedRepo {
    /// No storage: any call panics.
    pub fn none() -> Arc<Self> {
        Arc::new(Self::build(None))
    }

    /// Delegates to `inner`.
    pub fn over(inner: Arc<dyn OperationRepository>) -> Arc<Self> {
        Arc::new(Self::build(Some(inner)))
    }

    fn build(inner: Option<Arc<dyn OperationRepository>>) -> Self {
        Self {
            inner,
            faults: Mutex::new(HashMap::new()),
            hide_open: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            open_operation_ids: Mutex::new(Vec::new()),
            refused_events: Mutex::new(Vec::new()),
        }
    }

    /// Inject `fault` into the method `method` from now on.
    pub fn inject(&self, method: &'static str, fault: Fault) {
        self.faults.lock().expect("lock").insert(method, fault);
    }

    /// Make `open_operation` answer "none" while the winner is in storage, so
    /// the unique index decides the race.
    pub fn hide_open_operations(&self) {
        self.hide_open.store(true, Ordering::SeqCst);
    }

    fn pass(&self, method: &'static str) -> Result<&Arc<dyn OperationRepository>, StorageError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let fault = self.faults.lock().expect("lock").get(method).copied();
        match fault {
            Some(Fault::Panic) => panic!("injected panic in {method}"),
            Some(Fault::Unavailable) => Err(StorageError::Unavailable),
            None => Ok(self
                .inner
                .as_ref()
                .expect("storage must not be called in this test")),
        }
    }
}

#[async_trait]
impl OperationRepository for HookedRepo {
    async fn open_operation(
        &self,
        application_id: &str,
        now: OffsetDateTime,
    ) -> Result<Option<OpenOperation>, StorageError> {
        let inner = self.pass("open_operation")?;
        self.open_operation_ids
            .lock()
            .expect("lock")
            .push(application_id.to_owned());
        if self.hide_open.load(Ordering::SeqCst) {
            return Ok(None);
        }
        inner.open_operation(application_id, now).await
    }

    async fn accept(
        &self,
        new: NewOperation,
        accepted: AuditEvent,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        self.pass("accept")?.accept(new, accepted, now).await
    }

    async fn refuse_after_lost_race(
        &self,
        application_id: &str,
        build_refused: BuildRefused,
    ) -> Result<Uuid, StorageError> {
        self.pass("refuse_after_lost_race")?
            .refuse_after_lost_race(application_id, build_refused)
            .await
    }

    async fn record_refused(&self, event: AuditEvent) -> Result<(), StorageError> {
        let inner = self.pass("record_refused")?;
        self.refused_events
            .lock()
            .expect("lock")
            .push(event.clone());
        inner.record_refused(event).await
    }

    async fn mark_running(
        &self,
        operation_id: Uuid,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        self.pass("mark_running")?
            .mark_running(operation_id, now)
            .await
    }

    async fn record_report(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        self.pass("record_report")?
            .record_report(operation_id, report, now)
            .await
    }

    async fn finish(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        category: RequestCategory,
        finished: AuditEvent,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        self.pass("finish")?
            .finish(operation_id, report, category, finished, now)
            .await
    }

    async fn complete_by_supervisor(
        &self,
        operation_id: Uuid,
        report: &OutcomeReport,
        finished: AuditEvent,
        now: OffsetDateTime,
    ) -> Result<(), StorageError> {
        self.pass("complete_by_supervisor")?
            .complete_by_supervisor(operation_id, report, finished, now)
            .await
    }

    async fn interrupt_if_expired(
        &self,
        operation_id: Uuid,
        now: OffsetDateTime,
    ) -> Result<bool, StorageError> {
        self.pass("interrupt_if_expired")?
            .interrupt_if_expired(operation_id, now)
            .await
    }

    async fn interrupt_expired_batch(
        &self,
        now: OffsetDateTime,
        limit: u64,
    ) -> Result<u64, StorageError> {
        self.pass("interrupt_expired_batch")?
            .interrupt_expired_batch(now, limit)
            .await
    }

    async fn purge_finished(
        &self,
        before: OffsetDateTime,
        limit: u64,
    ) -> Result<u64, StorageError> {
        self.pass("purge_finished")?
            .purge_finished(before, limit)
            .await
    }

    async fn read_operation(
        &self,
        operation_id: Uuid,
        now: OffsetDateTime,
    ) -> Result<Option<Operation>, StorageError> {
        self.pass("read_operation")?
            .read_operation(operation_id, now)
            .await
    }

    async fn audit_events(&self, correlation_id: Uuid) -> Result<Vec<AuditEvent>, StorageError> {
        self.pass("audit_events")?
            .audit_events(correlation_id)
            .await
    }
}
