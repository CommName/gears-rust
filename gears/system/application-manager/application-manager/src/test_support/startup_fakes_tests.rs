//! Unit tests for the startup doubles (DESIGN §4.8 "Startup": "a fake types registry
//! and ClientHub", ADR 0007 "no tenant service").
//!
//! Planned API in `crate::test_support` (cfg(test) only):
//!
//! ```text
//! pub struct FakeTypesRegistry;  // impl TypesRegistryClient; Default
//!   new() / with_instances(Vec<GtsInstance>) / failing() / hanging() / with_delay(Duration)
//!   list_calls() / queries() / total_calls()   // total counts every trait method
//! pub struct TenantResolverTripwire;  // impl TenantResolverClient; new(); calls()
//! pub enum PluginFamily { Section, Audit }
//! pub fn plugin_instance(PluginFamily, segment, priority) -> (String, GtsInstance)
//! pub fn plugin_instance_with(PluginFamily, segment, edit: FnOnce(&mut Value)) -> (..)
//! ```
//!
//! - `fake_registry_lists_the_given_instances_and_records_the_query`
//! - `fake_registry_filters_by_the_query_pattern` (no pattern: all; `prefix*`: prefix; else exact)
//! - `bounded_*`: `pub async fn bounded<F: Future>(F) -> F::Output`, one-hour tokio-clock bound
//! - `fake_registry_failing_listing_returns_an_error`
//! - `fake_registry_hanging_listing_never_answers_and_is_still_counted` (paused clock)
//! - `fake_registry_delay_uses_the_tokio_clock` (paused clock)
//! - `fake_registry_counts_calls_to_other_methods`
//! - `tenant_tripwire_counts_every_call_and_answers_unavailable`
//! - `plugin_instance_ids_use_the_design_type_ids_and_carry_the_priority`
//! - `plugin_instance_with_can_make_the_priority_unreadable`

use std::time::Duration;

use serde_json::json;
use tenant_resolver_sdk::{
    GetAncestorsOptions, GetDescendantsOptions, GetTenantsOptions, IsAncestorOptions, TenantId,
    TenantResolverClient,
};
use tokio::time::Instant;
use types_registry_sdk::{InstanceQuery, TypesRegistryClient};
use uuid::Uuid;

use crate::test_support::{
    FakeTypesRegistry, PluginFamily, TenantResolverTripwire, plugin_instance, plugin_instance_with,
};

const SECTION_TYPE: &str =
    "gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~";
const AUDIT_TYPE: &str =
    "gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_audit.plugin.v1~";

#[tokio::test]
async fn fake_registry_lists_the_given_instances_and_records_the_query() {
    let (_, a) = plugin_instance(PluginFamily::Section, "acme.net.core.a.v1", 1);
    let registry = FakeTypesRegistry::new().with_instances(vec![a.clone()]);

    let listed = registry
        .list_instances(InstanceQuery::new().with_pattern("gts.cf.toolkit.plugins.plugin.v1~*"))
        .await
        .expect("listing succeeds");

    assert_eq!(listed, vec![a]);
    assert_eq!(registry.list_calls(), 1);
    assert_eq!(registry.total_calls(), 1);
    assert_eq!(
        registry.queries()[0].pattern.as_deref(),
        Some("gts.cf.toolkit.plugins.plugin.v1~*")
    );
}

// The fake filters like the registry: no pattern lists all, `prefix*` is a prefix
// match, anything else is exact, so a too-narrow or wrong pattern finds nothing.
#[tokio::test]
async fn fake_registry_filters_by_the_query_pattern() {
    let (section_id, section) = plugin_instance(PluginFamily::Section, "acme.net.core.a.v1", 1);
    let (_, audit) = plugin_instance(PluginFamily::Audit, "acme.audit.core.a.v1", 1);
    let registry = FakeTypesRegistry::new().with_instances(vec![section.clone(), audit.clone()]);
    let list = |pattern: Option<&'static str>| {
        let registry = &registry;
        async move {
            let query =
                pattern.map_or_else(InstanceQuery::new, |p| InstanceQuery::new().with_pattern(p));
            registry
                .list_instances(query)
                .await
                .expect("listing succeeds")
        }
    };

    assert_eq!(list(None).await.len(), 2);
    assert_eq!(
        list(Some("gts.cf.toolkit.plugins.plugin.v1~*")).await.len(),
        2
    );
    assert_eq!(
        list(Some(
            "gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~*"
        ))
        .await,
        vec![section]
    );
    assert!(list(Some("gts.other.*")).await.is_empty());
    assert!(list(Some("*")).await.len() == 2);
    // Exact match: the instance id itself, and a prefix without `*` finds nothing.
    let section_id: &'static str = Box::leak(section_id.into_boxed_str());
    assert_eq!(list(Some(section_id)).await.len(), 1);
    assert!(
        list(Some("gts.cf.toolkit.plugins.plugin.v1~"))
            .await
            .is_empty()
    );
}

#[tokio::test(start_paused = true)]
async fn bounded_returns_the_output_of_a_finishing_future() {
    assert_eq!(crate::test_support::bounded(async { 7 }).await, 7);
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "one-hour bound")]
async fn bounded_fails_a_future_that_never_finishes() {
    crate::test_support::bounded(std::future::pending::<()>()).await;
}

#[tokio::test]
async fn fake_registry_failing_listing_returns_an_error() {
    let registry = FakeTypesRegistry::new().failing();

    assert!(registry.list_instances(InstanceQuery::new()).await.is_err());
    assert_eq!(registry.list_calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn fake_registry_hanging_listing_never_answers_and_is_still_counted() {
    let registry = FakeTypesRegistry::new().hanging();

    let outcome = tokio::time::timeout(
        Duration::from_hours(1),
        registry.list_instances(InstanceQuery::new()),
    )
    .await;

    assert!(outcome.is_err(), "a hanging listing must not answer");
    assert_eq!(registry.list_calls(), 1);
}

#[tokio::test(start_paused = true)]
async fn fake_registry_delay_uses_the_tokio_clock() {
    let registry = FakeTypesRegistry::new().with_delay(Duration::from_millis(300));
    let started = Instant::now();

    registry
        .list_instances(InstanceQuery::new())
        .await
        .expect("listing succeeds");

    assert_eq!(started.elapsed(), Duration::from_millis(300));
}

#[tokio::test]
async fn fake_registry_counts_calls_to_other_methods() {
    let registry = FakeTypesRegistry::new();

    let _ = registry.register(vec![]).await;
    let _ = registry.get_instance("gts.a.b.c.d.v1~x.y.z.w.v1").await;

    assert_eq!(registry.list_calls(), 0);
    assert_eq!(registry.total_calls(), 2);
}

#[tokio::test]
async fn tenant_tripwire_counts_every_call_and_answers_unavailable() {
    let tripwire = TenantResolverTripwire::new();
    let ctx = toolkit_security::SecurityContext::anonymous();
    let id = TenantId(Uuid::from_u128(7));

    assert!(tripwire.get_tenant(&ctx, id).await.is_err());
    assert!(tripwire.get_root_tenant(&ctx).await.is_err());
    assert!(
        tripwire
            .get_tenants(&ctx, &[id], &GetTenantsOptions::default())
            .await
            .is_err()
    );
    assert!(
        tripwire
            .get_ancestors(&ctx, id, &GetAncestorsOptions::default())
            .await
            .is_err()
    );
    assert!(
        tripwire
            .get_descendants(&ctx, id, &GetDescendantsOptions::default())
            .await
            .is_err()
    );
    assert!(
        tripwire
            .is_ancestor(&ctx, id, id, &IsAncestorOptions::default())
            .await
            .is_err()
    );
    assert_eq!(tripwire.calls(), 6);
}

#[test]
fn plugin_instance_ids_use_the_design_type_ids_and_carry_the_priority() {
    let (section_id, section) = plugin_instance(PluginFamily::Section, "acme.net.core.a.v1", 7);
    let (audit_id, audit) = plugin_instance(PluginFamily::Audit, "acme.audit.core.a.v1", -3);

    assert_eq!(section_id, format!("{SECTION_TYPE}acme.net.core.a.v1"));
    assert_eq!(audit_id, format!("{AUDIT_TYPE}acme.audit.core.a.v1"));
    assert_eq!(section.object["priority"], json!(7));
    assert_eq!(audit.object["priority"], json!(-3));
}

#[test]
fn plugin_instance_with_can_make_the_priority_unreadable() {
    let (_, absent) =
        plugin_instance_with(PluginFamily::Section, "acme.net.core.a.v1", |payload| {
            payload
                .as_object_mut()
                .expect("object payload")
                .remove("priority");
        });

    assert!(absent.object.get("priority").is_none());
}
