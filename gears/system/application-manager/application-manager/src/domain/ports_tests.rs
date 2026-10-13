//! Unit tests for the orchestrator ports and the plugin-call error mapping
//! (DESIGN §3.2 "Ports and Composition", §3.3).
//!
//! Planned API in `crate::domain::ports` (traits, value types and mappings only; the
//! clock, request-id and startup doubles live in `crate::test_support`, so no double
//! ships in release builds):
//!
//! ```text
//! /// Result type of every guarded plugin call. `Internal` carries a FIXED or length-bounded
//! /// text that never includes plugin or panic text (a panic message may hold manifest
//! /// values, DESIGN §2.2).
//! #[derive(Debug, Clone, PartialEq, Eq)]
//! pub enum PluginCallError<E> { Plugin(E), TimedOut, NotSent, Internal(String) }
//!
//! impl PluginCallError<SectionPluginError> {
//!     pub fn section_outcome(&self) -> SectionOutcome;   // apply / remove
//!     pub fn schema_reason(&self) -> UnavailableReason;  // schema
//! }
//! impl PluginCallError<AuditPluginError> {
//!     pub fn handoff_reason(&self) -> UnavailableReason; // both hand-offs
//! }
//!
//! pub enum PolicyAction { Seed, Uninstall, List, ReadSchema }
//! impl PolicyAction { pub fn name(&self) -> &'static str }  // the SDK ACTION_* constants
//!
//! // PHASE MARKERS: `Permit` will carry the `AccessScope` and `Unavailable` a reason
//! // (P6, policy adapter); `RegistryStore` will take a scope argument and use the
//! // DESIGN §3.7 `RegistryEntry` instead of `ApplicationEnvelope` (P4, registry).
//! // The P2 shapes below are placeholders for exactly those items.
//! pub enum PolicyDecision { Permit, Refused, Unavailable }
//! pub enum RegistryError { Unavailable }                  // P4 refines
//!
//! #[async_trait] pub trait PolicyPort: Send + Sync {
//!     async fn decide(&self, caller: &CallerIdentity, action: PolicyAction,
//!                     application_id: Option<&str>) -> PolicyDecision;
//! }
//! #[async_trait] pub trait RegistryStore: Send + Sync {
//!     async fn get(&self, id: &str) -> Result<Option<ApplicationEnvelope>, RegistryError>;
//!     async fn upsert(&self, entry: &ApplicationEnvelope) -> Result<(), RegistryError>;
//!     async fn delete(&self, id: &str) -> Result<(), RegistryError>;
//!     async fn list(&self) -> Result<Vec<ApplicationEnvelope>, RegistryError>;
//! }
//! #[async_trait] pub trait SectionPluginPort: Send + Sync {
//!     async fn key(&self) -> Result<String, PluginCallError<SectionPluginError>>;
//!     async fn schema(&self, caller: &CallerIdentity)
//!         -> Result<Value, PluginCallError<SectionPluginError>>;
//!     async fn apply(&self, caller: &CallerIdentity, envelope: &ApplicationEnvelope,
//!         section: Value) -> Result<(), PluginCallError<SectionPluginError>>;
//!     async fn remove(&self, caller: &CallerIdentity, envelope: &ApplicationEnvelope)
//!         -> Result<(), PluginCallError<SectionPluginError>>;
//! }
//! #[async_trait] pub trait AuditSink: Send + Sync {
//!     async fn request_handoff(&self, caller: &CallerIdentity, content: &RequestHandoffContent)
//!         -> Result<String, PluginCallError<AuditPluginError>>;
//!     async fn final_result_handoff(&self, caller: &CallerIdentity, audit_id: &str,
//!         content: &FinalResultContent) -> Result<(), PluginCallError<AuditPluginError>>;
//! }
//! pub trait StartupPort: Send + Sync {   // read and stop only; publication is on the holder
//!     fn set_stopping(&self);
//!     fn is_ready(&self) -> bool;        // published && !stopping
//!     fn is_stopping(&self) -> bool;
//! }
//! pub trait Clock: Send + Sync { fn now(&self) -> SystemTime; }
//! pub trait RequestIdSource: Send + Sync { fn next_id(&self) -> Uuid; }
//! ```
//!
//! Mapping decided by the user (DESIGN §3.3, §4.6): a `schema` call may return only
//! `Internal`; `Rejected` / `NotPermitted` from `schema` are a contract violation and
//! map to `unusable` (defect signal). Panic, transport error and unresolved client map
//! to `unreachable` for `schema` and both hand-offs, and to `internal_failure` for
//! `apply` / `remove`.
//!
//! - `ports_are_object_safe_send_sync`: all seven ports are `dyn` + Send + Sync (§3.2).
//! - `policy_port_signature_is_pinned`: stub called through `Arc<dyn PolicyPort>`.
//! - `registry_store_signature_is_pinned`: stub called through `Arc<dyn RegistryStore>`.
//! - `section_plugin_port_signature_is_pinned`: all four operations through `Arc<dyn>`;
//!   every one returns the guarded error type (§3.3).
//! - `audit_sink_signature_is_pinned`: both hand-offs through `Arc<dyn AuditSink>` (§3.3).
//! - `plugin_call_error_equality_and_debug`: variants differ, `Debug` names the variant.
//! - `section_outcome_table`: every `PluginCallError<SectionPluginError>` variant (§3.3).
//! - `schema_reason_table`: every variant, including the contract-violation rows (§3.3).
//! - `handoff_reason_table`: every `PluginCallError<AuditPluginError>` variant (§3.3).
//! - `policy_action_names_match_sdk_constants`: `name()` equals the `ACTION_*` values.

use std::sync::Arc;

use application_manager_sdk::gts::{
    ACTION_LIST, ACTION_READ_SCHEMA, ACTION_SEED, ACTION_UNINSTALL,
};
use application_manager_sdk::models::{
    HandoffOperation, OutcomeCategory, RegistryEffect, SectionOutcome, UnavailableReason,
};
use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginError, CallerIdentity, FinalResultContent,
    RequestHandoffContent, SectionPluginError,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::domain::ports::{
    AuditSink, Clock, PluginCallError, PolicyAction, PolicyDecision, PolicyPort, RegistryError,
    RegistryStore, RequestIdSource, SectionPluginPort, StartupPort,
};

fn assert_dyn_port<T: ?Sized + Send + Sync + 'static>() {}

fn caller() -> CallerIdentity {
    CallerIdentity {
        subject_id: Uuid::from_u128(1),
        subject_type: None,
        subject_tenant_id: Uuid::from_u128(2),
        token_scopes: vec![],
        request_id: Uuid::from_u128(3),
    }
}

fn envelope() -> ApplicationEnvelope {
    ApplicationEnvelope {
        id: "app".to_owned(),
        version: "1".to_owned(),
    }
}

// Stubs with the EXACT planned signatures. They return values that no other test uses,
// so a wrong signature (for example `apply` without `PluginCallError`) fails to compile.
struct StubPolicy;

#[async_trait]
impl PolicyPort for StubPolicy {
    async fn decide(
        &self,
        _caller: &CallerIdentity,
        _action: PolicyAction,
        _application_id: Option<&str>,
    ) -> PolicyDecision {
        PolicyDecision::Unavailable
    }
}

struct StubRegistry;

#[async_trait]
impl RegistryStore for StubRegistry {
    async fn get(&self, _id: &str) -> Result<Option<ApplicationEnvelope>, RegistryError> {
        Ok(Some(envelope()))
    }
    async fn upsert(&self, _entry: &ApplicationEnvelope) -> Result<(), RegistryError> {
        Err(RegistryError::Unavailable)
    }
    async fn delete(&self, _id: &str) -> Result<(), RegistryError> {
        Ok(())
    }
    async fn list(&self) -> Result<Vec<ApplicationEnvelope>, RegistryError> {
        Ok(vec![envelope()])
    }
}

struct StubSection;

#[async_trait]
impl SectionPluginPort for StubSection {
    async fn key(&self) -> Result<String, PluginCallError<SectionPluginError>> {
        Err(PluginCallError::TimedOut)
    }
    async fn schema(
        &self,
        _caller: &CallerIdentity,
    ) -> Result<Value, PluginCallError<SectionPluginError>> {
        Err(PluginCallError::NotSent)
    }
    async fn apply(
        &self,
        _caller: &CallerIdentity,
        _envelope: &ApplicationEnvelope,
        _section: Value,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        Err(PluginCallError::TimedOut)
    }
    async fn remove(
        &self,
        _caller: &CallerIdentity,
        _envelope: &ApplicationEnvelope,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        Err(PluginCallError::NotSent)
    }
}

struct StubAudit;

#[async_trait]
impl AuditSink for StubAudit {
    async fn request_handoff(
        &self,
        _caller: &CallerIdentity,
        _content: &RequestHandoffContent,
    ) -> Result<String, PluginCallError<AuditPluginError>> {
        Err(PluginCallError::TimedOut)
    }
    async fn final_result_handoff(
        &self,
        _caller: &CallerIdentity,
        _audit_id: &str,
        _content: &FinalResultContent,
    ) -> Result<(), PluginCallError<AuditPluginError>> {
        Err(PluginCallError::NotSent)
    }
}

// DESIGN §3.2: "Domain services hold their ports as trait objects".
#[test]
fn ports_are_object_safe_send_sync() {
    assert_dyn_port::<dyn PolicyPort>();
    assert_dyn_port::<dyn RegistryStore>();
    assert_dyn_port::<dyn SectionPluginPort>();
    assert_dyn_port::<dyn AuditSink>();
    assert_dyn_port::<dyn StartupPort>();
    assert_dyn_port::<dyn Clock>();
    assert_dyn_port::<dyn RequestIdSource>();
}

#[tokio::test]
async fn policy_port_signature_is_pinned() {
    let port: Arc<dyn PolicyPort> = Arc::new(StubPolicy);
    let caller = caller();
    let decision = port.decide(&caller, PolicyAction::Seed, Some("app")).await;
    assert!(matches!(decision, PolicyDecision::Unavailable));
    let decision = port.decide(&caller, PolicyAction::List, None).await;
    assert!(matches!(decision, PolicyDecision::Unavailable));
}

#[tokio::test]
async fn registry_store_signature_is_pinned() {
    let port: Arc<dyn RegistryStore> = Arc::new(StubRegistry);
    assert_eq!(port.get("app").await.expect("get"), Some(envelope()));
    assert!(matches!(
        port.upsert(&envelope()).await,
        Err(RegistryError::Unavailable)
    ));
    assert!(port.delete("app").await.is_ok());
    assert_eq!(port.list().await.expect("list"), vec![envelope()]);
}

#[tokio::test]
async fn section_plugin_port_signature_is_pinned() {
    let port: Arc<dyn SectionPluginPort> = Arc::new(StubSection);
    let caller = caller();
    let envelope = envelope();
    assert_eq!(port.key().await, Err(PluginCallError::TimedOut));
    assert_eq!(port.schema(&caller).await, Err(PluginCallError::NotSent));
    assert_eq!(
        port.apply(&caller, &envelope, json!({})).await,
        Err(PluginCallError::TimedOut)
    );
    assert_eq!(
        port.remove(&caller, &envelope).await,
        Err(PluginCallError::NotSent)
    );
}

#[tokio::test]
async fn audit_sink_signature_is_pinned() {
    let port: Arc<dyn AuditSink> = Arc::new(StubAudit);
    let caller = caller();
    let request = RequestHandoffContent {
        operation: HandoffOperation::Seed,
        request_id: Uuid::from_u128(3),
        received_at: "2026-10-12T00:00:00Z".to_owned(),
        application: None,
        manifest: None,
        decided_category: None,
        id_length_bytes: None,
        id_sha256: None,
    };
    let result = FinalResultContent {
        audit_id: "a".to_owned(),
        request_id: Uuid::from_u128(3),
        validation: None,
        sections: vec![],
        category: OutcomeCategory::Succeeded,
        registry_effect: RegistryEffect::Unchanged,
        completed_at: "2026-10-12T00:00:01Z".to_owned(),
    };
    assert_eq!(
        port.request_handoff(&caller, &request).await,
        Err(PluginCallError::TimedOut)
    );
    assert_eq!(
        port.final_result_handoff(&caller, "a", &result).await,
        Err(PluginCallError::NotSent)
    );
}

#[test]
fn plugin_call_error_equality_and_debug() {
    let variants: Vec<PluginCallError<String>> = vec![
        PluginCallError::Plugin("x".to_owned()),
        PluginCallError::TimedOut,
        PluginCallError::NotSent,
        PluginCallError::Internal("x".to_owned()),
    ];
    for (i, a) in variants.iter().enumerate() {
        for (j, b) in variants.iter().enumerate() {
            assert_eq!(a == b, i == j, "{a:?} vs {b:?}");
        }
    }
    for (variant, name) in variants
        .iter()
        .zip(["Plugin", "TimedOut", "NotSent", "Internal"])
    {
        assert!(format!("{variant:?}").starts_with(name), "{variant:?}");
    }
}

// DESIGN §3.3 "Time-limit outcome": one row per variant, pure mapping, no runtime.
#[test]
fn section_outcome_table() {
    let cases: Vec<(PluginCallError<SectionPluginError>, SectionOutcome)> = vec![
        (
            PluginCallError::Plugin(SectionPluginError::rejected("r", Some("/a"))),
            SectionOutcome::Rejected,
        ),
        (
            PluginCallError::Plugin(SectionPluginError::not_permitted("p")),
            SectionOutcome::NotPermitted,
        ),
        (
            PluginCallError::Plugin(SectionPluginError::internal("i")),
            SectionOutcome::InternalFailure,
        ),
        (PluginCallError::TimedOut, SectionOutcome::TimedOut),
        (PluginCallError::NotSent, SectionOutcome::NotSent),
        (
            PluginCallError::Internal("panic".to_owned()),
            SectionOutcome::InternalFailure,
        ),
    ];
    for (error, outcome) in cases {
        assert_eq!(error.section_outcome(), outcome, "{error:?}");
    }
}

// User decision (F-005 option A): schema may only return Internal; Rejected and
// NotPermitted are a contract violation -> `unusable`.
#[test]
fn schema_reason_table() {
    let cases: Vec<(PluginCallError<SectionPluginError>, UnavailableReason)> = vec![
        (
            PluginCallError::Plugin(SectionPluginError::rejected("r", None)),
            UnavailableReason::Unusable,
        ),
        (
            PluginCallError::Plugin(SectionPluginError::not_permitted("p")),
            UnavailableReason::Unusable,
        ),
        (
            PluginCallError::Plugin(SectionPluginError::internal("i")),
            UnavailableReason::Unreachable,
        ),
        (PluginCallError::TimedOut, UnavailableReason::TimedOut),
        (PluginCallError::NotSent, UnavailableReason::NotSent),
        (
            PluginCallError::Internal("panic".to_owned()),
            UnavailableReason::Unreachable,
        ),
    ];
    for (error, reason) in cases {
        assert_eq!(error.schema_reason(), reason, "{error:?}");
    }
}

// DESIGN §3.3 audit error kinds table plus the guard's own failures.
#[test]
fn handoff_reason_table() {
    let cases: Vec<(PluginCallError<AuditPluginError>, UnavailableReason)> = vec![
        (
            PluginCallError::Plugin(AuditPluginError::refused("no")),
            UnavailableReason::Refused,
        ),
        (
            PluginCallError::Plugin(AuditPluginError::unavailable("down")),
            UnavailableReason::Unreachable,
        ),
        (PluginCallError::TimedOut, UnavailableReason::TimedOut),
        (PluginCallError::NotSent, UnavailableReason::NotSent),
        (
            PluginCallError::Internal("panic".to_owned()),
            UnavailableReason::Unreachable,
        ),
    ];
    for (error, reason) in cases {
        assert_eq!(error.handoff_reason(), reason, "{error:?}");
    }
}

// DESIGN §3.3 / SDK gts: the PDP action names are the SDK constants.
#[test]
fn policy_action_names_match_sdk_constants() {
    let table = [
        (PolicyAction::Seed, ACTION_SEED),
        (PolicyAction::Uninstall, ACTION_UNINSTALL),
        (PolicyAction::List, ACTION_LIST),
        (PolicyAction::ReadSchema, ACTION_READ_SCHEMA),
    ];
    for (action, name) in table {
        assert_eq!(action.name(), name);
    }
}
