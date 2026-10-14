//! Startup test doubles: a fake types registry and a tenant resolver tripwire.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use application_manager_sdk::{
    ApplicationManagerAuditPluginSpecV1, ApplicationManagerSectionPluginSpecV1,
};
use async_trait::async_trait;
use serde_json::Value;
use tenant_resolver_sdk::{
    GetAncestorsOptions, GetAncestorsResponse, GetDescendantsOptions, GetDescendantsResponse,
    GetTenantsOptions, IsAncestorOptions, TenantId, TenantInfo, TenantResolverClient,
    TenantResolverError,
};
use toolkit::gts::PluginV1;
use toolkit_canonical_errors::CanonicalError;
use toolkit_security::SecurityContext;
use types_registry_sdk::testing::{internal, make_test_instance};
use types_registry_sdk::{
    GtsInstance, GtsTypeSchema, InstanceQuery, RegisterResult, TypeSchemaQuery, TypesRegistryClient,
};
use uuid::Uuid;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn matches_pattern(pattern: Option<&str>, id: &str) -> bool {
    match pattern {
        None => true,
        Some(p) => p
            .strip_suffix('*')
            .map_or_else(|| id == p, |prefix| id.starts_with(prefix)),
    }
}

/// Runs `fut` with a one-hour bound on the tokio clock (paused in most tests, so the
/// bound is reached at once when `fut` can never finish). A regression that makes the
/// code under test wait forever then fails the test instead of hanging CI.
///
/// # Panics
///
/// Panics when `fut` does not finish within the bound.
pub async fn bounded<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_hours(1), fut)
        .await
        .expect("the future did not finish within the one-hour bound")
}

#[derive(Clone)]
enum Listing {
    Instances(Vec<GtsInstance>),
    Failure,
    Hang,
}

/// How [`FakeTypesRegistry::register_type_schemas`] answers (P7: `init` registers the two
/// plugin GTS types).
#[derive(Clone)]
enum Registration {
    /// Accepts every schema: one `RegisterResult::Ok` per schema, with its `$id` as the id.
    Accept,
    /// The call itself fails with an internal canonical error.
    CallFails,
    /// The call succeeds but the item at this index (0-based) is rejected.
    ItemRejected(usize),
    /// The call succeeds with no per-item result.
    NoResults,
    /// The call never answers.
    Hang,
    /// The call succeeds with this many `Ok` results, whatever the number of schemas (the ids
    /// are those of the schemas in turn).
    ResultCount(usize),
    /// The call succeeds with one `Ok` per schema, each with an id that is not the schema's.
    OtherIds,
}

/// Fake types registry client.
///
/// `list_instances` and `register_type_schemas` are scripted. Every call to any method of the
/// trait is counted in [`FakeTypesRegistry::total_calls`], so a test can assert that startup made
/// exactly one registry call. Methods other than these two fail with an internal error (or
/// return an empty answer); the startup component must not use them. Since P7 `init` makes
/// one `register_type_schemas` call ([`FakeTypesRegistry::registration_calls`]), which
/// `total_calls` counts too.
pub struct FakeTypesRegistry {
    listing: Listing,
    delay: Duration,
    queries: Mutex<Vec<InstanceQuery>>,
    other_calls: AtomicUsize,
    registration: Registration,
    registered: Mutex<Vec<Vec<Value>>>,
}

impl Default for FakeTypesRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeTypesRegistry {
    /// Creates a registry that lists no instances.
    pub fn new() -> Self {
        Self {
            listing: Listing::Instances(Vec::new()),
            delay: Duration::ZERO,
            queries: Mutex::new(Vec::new()),
            other_calls: AtomicUsize::new(0),
            registration: Registration::Accept,
            registered: Mutex::new(Vec::new()),
        }
    }

    /// Makes `register_type_schemas` fail with an internal canonical error.
    #[must_use]
    pub fn registration_failing(mut self) -> Self {
        self.registration = Registration::CallFails;
        self
    }

    /// Makes `register_type_schemas` reject the schema at `index` (0-based) and accept the rest.
    #[must_use]
    pub fn registration_rejecting(mut self, index: usize) -> Self {
        self.registration = Registration::ItemRejected(index);
        self
    }

    /// Makes `register_type_schemas` answer with no per-item result.
    #[must_use]
    pub fn registration_without_results(mut self) -> Self {
        self.registration = Registration::NoResults;
        self
    }

    /// Makes `register_type_schemas` answer `Ok` with exactly `count` results, however many
    /// schemas it got.
    #[must_use]
    pub fn registration_with_result_count(mut self, count: usize) -> Self {
        self.registration = Registration::ResultCount(count);
        self
    }

    /// Makes `register_type_schemas` answer one `Ok` per schema with an id that is not the
    /// schema's own.
    #[must_use]
    pub fn registration_answering_other_ids(mut self) -> Self {
        self.registration = Registration::OtherIds;
        self
    }

    /// Makes `register_type_schemas` never answer.
    #[must_use]
    pub fn registration_hanging(mut self) -> Self {
        self.registration = Registration::Hang;
        self
    }

    /// The schema batches of the `register_type_schemas` calls, in order.
    pub fn registered(&self) -> Vec<Vec<Value>> {
        lock(&self.registered).clone()
    }

    /// Number of `register_type_schemas` calls received (each is also in `total_calls`).
    pub fn registration_calls(&self) -> usize {
        lock(&self.registered).len()
    }

    /// Lists `instances`, filtered by the query pattern like the real registry does for
    /// the shapes the gear may use: no pattern lists all, a pattern ending in `*` is a
    /// prefix match on the instance id, any other pattern is an exact match.
    #[must_use]
    pub fn with_instances(mut self, instances: Vec<GtsInstance>) -> Self {
        self.listing = Listing::Instances(instances);
        self
    }

    /// Makes `list_instances` fail with an internal canonical error.
    #[must_use]
    pub fn failing(mut self) -> Self {
        self.listing = Listing::Failure;
        self
    }

    /// Makes `list_instances` never answer.
    #[must_use]
    pub fn hanging(mut self) -> Self {
        self.listing = Listing::Hang;
        self
    }

    /// Waits `d` on the tokio clock before `list_instances` answers or fails.
    #[must_use]
    pub fn with_delay(mut self, d: Duration) -> Self {
        self.delay = d;
        self
    }

    /// Number of `list_instances` calls received.
    pub fn list_calls(&self) -> usize {
        lock(&self.queries).len()
    }

    /// The queries of the `list_instances` calls, in order.
    pub fn queries(&self) -> Vec<InstanceQuery> {
        lock(&self.queries).clone()
    }

    /// Number of calls to any trait method, `list_instances` included.
    pub fn total_calls(&self) -> usize {
        self.list_calls() + self.registration_calls() + self.other_calls.load(Ordering::SeqCst)
    }

    fn other(&self) {
        self.other_calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl TypesRegistryClient for FakeTypesRegistry {
    async fn register(&self, _entities: Vec<Value>) -> Result<Vec<RegisterResult>, CanonicalError> {
        self.other();
        Ok(Vec::new())
    }

    async fn register_type_schemas(
        &self,
        type_schemas: Vec<Value>,
    ) -> Result<Vec<RegisterResult>, CanonicalError> {
        lock(&self.registered).push(type_schemas.clone());
        let id_of = |schema: &Value| {
            schema
                .get("$id")
                .and_then(Value::as_str)
                .map(|id| id.strip_prefix("gts://").unwrap_or(id).to_owned())
        };
        match &self.registration {
            Registration::Accept => Ok(type_schemas
                .iter()
                .map(|s| RegisterResult::Ok {
                    gts_id: id_of(s).unwrap_or_default(),
                })
                .collect()),
            Registration::CallFails => Err(internal("fake types registry: registration failed")),
            Registration::ItemRejected(index) => Ok(type_schemas
                .iter()
                .enumerate()
                .map(|(i, s)| {
                    if i == *index {
                        RegisterResult::Err {
                            gts_id: id_of(s),
                            error: internal("fake types registry: schema rejected"),
                        }
                    } else {
                        RegisterResult::Ok {
                            gts_id: id_of(s).unwrap_or_default(),
                        }
                    }
                })
                .collect()),
            Registration::NoResults => Ok(Vec::new()),
            Registration::Hang => std::future::pending().await,
            Registration::ResultCount(count) => Ok((0..*count)
                .map(|i| RegisterResult::Ok {
                    gts_id: type_schemas
                        .get(i % type_schemas.len().max(1))
                        .and_then(id_of)
                        .unwrap_or_default(),
                })
                .collect()),
            Registration::OtherIds => Ok(type_schemas
                .iter()
                .map(|_| RegisterResult::Ok {
                    gts_id: "gts.fake.other.type.v1~".to_owned(),
                })
                .collect()),
        }
    }

    async fn get_type_schema(&self, _type_id: &str) -> Result<GtsTypeSchema, CanonicalError> {
        self.other();
        Err(internal(
            "fake types registry: get_type_schema is not scripted",
        ))
    }

    async fn get_type_schema_by_uuid(
        &self,
        _type_uuid: Uuid,
    ) -> Result<GtsTypeSchema, CanonicalError> {
        self.other();
        Err(internal(
            "fake types registry: get_type_schema_by_uuid is not scripted",
        ))
    }

    async fn get_type_schemas(
        &self,
        _type_ids: Vec<String>,
    ) -> HashMap<String, Result<GtsTypeSchema, CanonicalError>> {
        self.other();
        HashMap::new()
    }

    async fn get_type_schemas_by_uuid(
        &self,
        _type_uuids: Vec<Uuid>,
    ) -> HashMap<Uuid, Result<GtsTypeSchema, CanonicalError>> {
        self.other();
        HashMap::new()
    }

    async fn list_type_schemas(
        &self,
        _query: TypeSchemaQuery,
    ) -> Result<Vec<GtsTypeSchema>, CanonicalError> {
        self.other();
        Ok(Vec::new())
    }

    async fn register_instances(
        &self,
        _instances: Vec<Value>,
    ) -> Result<Vec<RegisterResult>, CanonicalError> {
        self.other();
        Ok(Vec::new())
    }

    async fn get_instance(&self, _id: &str) -> Result<GtsInstance, CanonicalError> {
        self.other();
        Err(internal(
            "fake types registry: get_instance is not scripted",
        ))
    }

    async fn get_instance_by_uuid(&self, _uuid: Uuid) -> Result<GtsInstance, CanonicalError> {
        self.other();
        Err(internal(
            "fake types registry: get_instance_by_uuid is not scripted",
        ))
    }

    async fn get_instances(
        &self,
        _ids: Vec<String>,
    ) -> HashMap<String, Result<GtsInstance, CanonicalError>> {
        self.other();
        HashMap::new()
    }

    async fn get_instances_by_uuid(
        &self,
        _uuids: Vec<Uuid>,
    ) -> HashMap<Uuid, Result<GtsInstance, CanonicalError>> {
        self.other();
        HashMap::new()
    }

    async fn list_instances(
        &self,
        query: InstanceQuery,
    ) -> Result<Vec<GtsInstance>, CanonicalError> {
        let pattern = query.pattern.clone();
        lock(&self.queries).push(query);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        match &self.listing {
            Listing::Instances(instances) => Ok(instances
                .iter()
                .filter(|i| matches_pattern(pattern.as_deref(), i.id.as_ref()))
                .cloned()
                .collect()),
            Listing::Failure => Err(internal("fake types registry: listing failed")),
            Listing::Hang => std::future::pending().await,
        }
    }
}

/// Tenant resolver client that counts every call and answers `ServiceUnavailable`.
///
/// Registered in the `ClientHub` of a startup test to prove that the gear never calls a
/// tenant service (ADR 0007).
#[derive(Default)]
pub struct TenantResolverTripwire {
    calls: AtomicUsize,
}

impl TenantResolverTripwire {
    /// Creates a tripwire with no calls.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of calls received, over all methods.
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn hit<T>(&self) -> Result<T, TenantResolverError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(TenantResolverError::ServiceUnavailable(
            "tenant resolver tripwire".to_owned(),
        ))
    }
}

#[async_trait]
impl TenantResolverClient for TenantResolverTripwire {
    async fn get_tenant(
        &self,
        _ctx: &SecurityContext,
        _id: TenantId,
    ) -> Result<TenantInfo, TenantResolverError> {
        self.hit()
    }

    async fn get_root_tenant(
        &self,
        _ctx: &SecurityContext,
    ) -> Result<TenantInfo, TenantResolverError> {
        self.hit()
    }

    async fn get_tenants(
        &self,
        _ctx: &SecurityContext,
        _ids: &[TenantId],
        _options: &GetTenantsOptions,
    ) -> Result<Vec<TenantInfo>, TenantResolverError> {
        self.hit()
    }

    async fn get_ancestors(
        &self,
        _ctx: &SecurityContext,
        _id: TenantId,
        _options: &GetAncestorsOptions,
    ) -> Result<GetAncestorsResponse, TenantResolverError> {
        self.hit()
    }

    async fn get_descendants(
        &self,
        _ctx: &SecurityContext,
        _id: TenantId,
        _options: &GetDescendantsOptions,
    ) -> Result<GetDescendantsResponse, TenantResolverError> {
        self.hit()
    }

    async fn is_ancestor(
        &self,
        _ctx: &SecurityContext,
        _ancestor_id: TenantId,
        _descendant_id: TenantId,
        _options: &IsAncestorOptions,
    ) -> Result<bool, TenantResolverError> {
        self.hit()
    }
}

/// Kind of a plugin instance built by [`plugin_instance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginFamily {
    /// An `ApplicationManagerSectionPluginSpecV1` instance.
    Section,
    /// An `ApplicationManagerAuditPluginSpecV1` instance.
    Audit,
}

/// Builds a registered plugin instance of `family` with instance segment `segment` and
/// `priority`, as a plugin gear would register it. Returns its GTS instance id (the
/// `ClientHub` scope id) and the instance.
///
/// # Panics
///
/// Panics when `segment` is not a valid GTS instance segment.
pub fn plugin_instance(
    family: PluginFamily,
    segment: &str,
    priority: i16,
) -> (String, GtsInstance) {
    plugin_instance_with(family, segment, |payload| {
        payload["priority"] = Value::from(priority);
    })
}

/// Like [`plugin_instance`], with the instance object edited by `edit` before it is
/// wrapped, so a test can make the priority unreadable.
///
/// # Panics
///
/// Panics when `segment` is not a valid GTS instance segment.
pub fn plugin_instance_with(
    family: PluginFamily,
    segment: &str,
    edit: impl FnOnce(&mut Value),
) -> (String, GtsInstance) {
    let (id, mut payload) = match family {
        PluginFamily::Section => {
            PluginV1::<ApplicationManagerSectionPluginSpecV1>::build_registration(
                segment, "acme", 0,
            )
        }
        PluginFamily::Audit => {
            PluginV1::<ApplicationManagerAuditPluginSpecV1>::build_registration(segment, "acme", 0)
        }
    }
    .expect("plugin registration builds");
    edit(&mut payload);
    let id: String = (id.as_ref() as &str).to_owned();
    let instance = make_test_instance(&id, payload);
    (id, instance)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "startup_fakes_tests.rs"]
mod startup_fakes_tests;
