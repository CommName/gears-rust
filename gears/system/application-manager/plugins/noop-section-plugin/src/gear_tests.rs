#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Mutex;

use gts::GtsSchema;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use toolkit::client_hub::ClientHub;
use toolkit::config::ConfigProvider;
use toolkit_canonical_errors::CanonicalError;
use types_registry_sdk::{GtsInstance, GtsTypeSchema, InstanceQuery, TypeSchemaQuery};
use uuid::Uuid;

use super::*;

#[test]
fn registration_chains_from_the_section_plugin_type() {
    let cfg = NoopSectionPluginConfig {
        vendor: "constructorfabric".to_owned(),
        priority: 7,
    };
    let (id, payload) = registration(&cfg).unwrap();
    let type_id = <ApplicationManagerSectionPluginSpecV1 as GtsSchema>::TYPE_ID;
    assert_eq!(id, format!("{type_id}{INSTANCE_SEGMENT}"));
    assert_eq!(payload["id"], id);
    assert_eq!(payload["priority"], 7);
    assert_eq!(payload["vendor"], "constructorfabric");
}

/// A types registry that records `register` payloads and answers each with
/// `accept`; every other call is unexpected.
struct Registry {
    accept: bool,
    registered: Mutex<Vec<Value>>,
}

impl Registry {
    fn new(accept: bool) -> Arc<Self> {
        Arc::new(Self {
            accept,
            registered: Mutex::new(Vec::new()),
        })
    }
}

fn unexpected() -> CanonicalError {
    CanonicalError::internal("unexpected registry call").create()
}

#[async_trait]
impl TypesRegistryClient for Registry {
    async fn register(&self, entities: Vec<Value>) -> Result<Vec<RegisterResult>, CanonicalError> {
        let results = entities
            .iter()
            .map(|e| {
                let gts_id = e["id"].as_str().unwrap().to_owned();
                if self.accept {
                    RegisterResult::Ok { gts_id }
                } else {
                    RegisterResult::Err {
                        gts_id: Some(gts_id),
                        error: CanonicalError::internal("rejected").create(),
                    }
                }
            })
            .collect();
        self.registered.lock().unwrap().extend(entities);
        Ok(results)
    }
    async fn register_type_schemas(
        &self,
        _type_schemas: Vec<Value>,
    ) -> Result<Vec<RegisterResult>, CanonicalError> {
        Err(unexpected())
    }
    async fn get_type_schema(&self, _type_id: &str) -> Result<GtsTypeSchema, CanonicalError> {
        Err(unexpected())
    }
    async fn get_type_schema_by_uuid(
        &self,
        _type_uuid: Uuid,
    ) -> Result<GtsTypeSchema, CanonicalError> {
        Err(unexpected())
    }
    async fn get_type_schemas(
        &self,
        _type_ids: Vec<String>,
    ) -> HashMap<String, Result<GtsTypeSchema, CanonicalError>> {
        HashMap::new()
    }
    async fn get_type_schemas_by_uuid(
        &self,
        _type_uuids: Vec<Uuid>,
    ) -> HashMap<Uuid, Result<GtsTypeSchema, CanonicalError>> {
        HashMap::new()
    }
    async fn list_type_schemas(
        &self,
        _query: TypeSchemaQuery,
    ) -> Result<Vec<GtsTypeSchema>, CanonicalError> {
        Err(unexpected())
    }
    async fn register_instances(
        &self,
        _instances: Vec<Value>,
    ) -> Result<Vec<RegisterResult>, CanonicalError> {
        Err(unexpected())
    }
    async fn get_instance(&self, _id: &str) -> Result<GtsInstance, CanonicalError> {
        Err(unexpected())
    }
    async fn get_instance_by_uuid(&self, _uuid: Uuid) -> Result<GtsInstance, CanonicalError> {
        Err(unexpected())
    }
    async fn get_instances(
        &self,
        _ids: Vec<String>,
    ) -> HashMap<String, Result<GtsInstance, CanonicalError>> {
        HashMap::new()
    }
    async fn get_instances_by_uuid(
        &self,
        _uuids: Vec<Uuid>,
    ) -> HashMap<Uuid, Result<GtsInstance, CanonicalError>> {
        HashMap::new()
    }
    async fn list_instances(
        &self,
        _query: InstanceQuery,
    ) -> Result<Vec<GtsInstance>, CanonicalError> {
        Err(unexpected())
    }
}

struct NoConfig;

impl ConfigProvider for NoConfig {
    fn get_gear_config(&self, _gear: &str) -> Option<&Value> {
        None
    }
}

fn ctx(hub: Arc<ClientHub>) -> GearCtx {
    GearCtx::new(
        NoopSectionPlugin::MODULE_NAME,
        Uuid::from_u128(1),
        Arc::new(NoConfig),
        hub,
        CancellationToken::new(),
    )
}

fn hub_with(registry: Arc<Registry>) -> Arc<ClientHub> {
    let hub = Arc::new(ClientHub::new());
    let registry: Arc<dyn TypesRegistryClient> = registry;
    hub.register::<dyn TypesRegistryClient>(registry);
    hub
}

#[tokio::test]
async fn init_registers_the_instance_and_its_client_under_one_id() {
    let registry = Registry::new(true);
    let hub = hub_with(Arc::clone(&registry));
    NoopSectionPlugin
        .init(&ctx(Arc::clone(&hub)))
        .await
        .unwrap();

    let (id, payload) = registration(&NoopSectionPluginConfig::default()).unwrap();
    assert_eq!(*registry.registered.lock().unwrap(), [payload]);
    let client = hub
        .try_get_scoped::<dyn SectionPluginClientV1>(&ClientScope::gts_id(&id))
        .expect("client scoped by the registered instance id");
    assert_eq!(client.key().await.unwrap(), "noop");
}

#[tokio::test]
async fn init_fails_without_registering_a_client_when_registration_fails() {
    let (id, _) = registration(&NoopSectionPluginConfig::default()).unwrap();
    let rejecting = hub_with(Registry::new(false));
    let no_registry = Arc::new(ClientHub::new());
    for hub in [rejecting, no_registry] {
        NoopSectionPlugin
            .init(&ctx(Arc::clone(&hub)))
            .await
            .expect_err("init must fail");
        assert!(
            hub.try_get_scoped::<dyn SectionPluginClientV1>(&ClientScope::gts_id(&id))
                .is_none()
        );
    }
}
