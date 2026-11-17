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
