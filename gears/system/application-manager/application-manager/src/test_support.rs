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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use application_manager_sdk::{
    ApplicationManagerSectionPluginSpecV1, CallerIdentity, Envelope, SectionPluginClientV1,
    SectionPluginError, SectionSchema,
};
use async_trait::async_trait;
use authz_resolver_sdk::AuthZResolverApi;
use authz_resolver_sdk::constraints::{Constraint, EqPredicate, Predicate};
use authz_resolver_sdk::models::{
    EvaluationRequest, EvaluationResponse, EvaluationResponseContext,
};
use toolkit::client_hub::{ClientHub, ClientScope};
use toolkit::gts::PluginV1;
use toolkit_canonical_errors::CanonicalError;
use toolkit_security::{PlatformSecurityContext, SecurityContext, pep_properties};
use types_registry_sdk::TypesRegistryClient;
use types_registry_sdk::testing::{MockTypesRegistryClient, make_test_instance};
use uuid::Uuid;

use crate::domain::actions::Action;
use crate::domain::ports::{AccessControl, AccessError};

/// An authenticated caller.
pub fn ctx() -> SecurityContext {
    SecurityContext::builder()
        .subject_id(Uuid::from_u128(0xA))
        .subject_tenant_id(Uuid::from_u128(0xB))
        .build()
        .expect("test ctx")
}

/// How a [`FakePlugin`] answers `key`.
#[derive(Clone)]
pub enum KeyAnswer {
    Key(&'static str),
    Error,
    Panic,
    Hang,
}

/// A section plugin that counts every call.
pub struct FakePlugin {
    key: KeyAnswer,
    pub calls: AtomicUsize,
}

impl FakePlugin {
    pub fn new(key: KeyAnswer) -> Arc<Self> {
        Arc::new(Self {
            key,
            calls: AtomicUsize::new(0),
        })
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
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
        Ok(SectionSchema::new(serde_json::json!({}), "1"))
    }

    async fn apply(
        &self,
        _caller: &CallerIdentity,
        _operation_id: Uuid,
        _envelope: &Envelope,
        _section: &Value,
    ) -> Result<(), SectionPluginError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn remove(
        &self,
        _caller: &CallerIdentity,
        _operation_id: Uuid,
        _application_id: &str,
    ) -> Result<(), SectionPluginError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The full GTS instance id of a section plugin instance named `segment`.
pub fn instance_id(segment: &str) -> String {
    format!(
        "{}{segment}",
        <ApplicationManagerSectionPluginSpecV1 as gts::GtsSchema>::TYPE_ID
    )
}

/// The registry content of a section plugin instance.
pub fn instance_content(segment: &str, priority: i16) -> Value {
    PluginV1::<ApplicationManagerSectionPluginSpecV1>::build_registration(
        segment,
        "constructorfabric".to_owned(),
        priority,
    )
    .expect("registration")
    .1
}

/// A plugin for [`hub_with`]: instance segment, registry content, and client
/// (`None` registers no client).
pub struct PluginSpec {
    pub segment: &'static str,
    pub content: Value,
    pub client: Option<Arc<FakePlugin>>,
}

impl PluginSpec {
    pub fn new(segment: &'static str, priority: i16, key: KeyAnswer) -> Self {
        Self {
            segment,
            content: instance_content(segment, priority),
            client: Some(FakePlugin::new(key)),
        }
    }
}

/// A hub with a mock types registry listing `plugins` and their clients.
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
    let registry: Arc<dyn TypesRegistryClient> = Arc::new(registry.with_instances(instances));
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
}

impl FakeAccess {
    pub fn new(answer: Result<(), AccessError>) -> Arc<Self> {
        Arc::new(Self {
            answer,
            calls: AtomicUsize::new(0),
        })
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AccessControl for FakeAccess {
    async fn authorize(&self, _ctx: &SecurityContext, _action: Action) -> Result<(), AccessError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.answer
    }
}
