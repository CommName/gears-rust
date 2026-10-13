//! Plugin adapters: the section plugin port and the audit sink over the scoped SDK
//! clients, with every call made through the Plugin Call Guard (DESIGN §3.2 "Plugin
//! Call Guard", "Ports and Composition").
//!
//! A guarded call must be `Send + 'static`, so the adapters clone the caller, the
//! envelope and the section into the call.

use std::sync::Arc;

use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginClientV1, AuditPluginError, CallerIdentity, FinalResultContent,
    RequestHandoffContent, SectionPluginClientV1, SectionPluginError,
};
use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::domain::ports::{AuditSink, PluginCallError, SectionPluginPort};
use crate::infra::call_guard::{CallKind, CallMeta, PluginCallGuard};

/// [`SectionPluginPort`] over a scoped [`SectionPluginClientV1`]. Every call goes
/// through the guard under `instance_id`, so the in-flight cap is per instance.
pub struct GuardedSectionPlugin {
    instance_id: String,
    key: String,
    client: Arc<dyn SectionPluginClientV1>,
    guard: PluginCallGuard,
}

impl GuardedSectionPlugin {
    /// Creates the adapter. `key` is the validated section key, used for log metadata.
    #[must_use]
    pub fn new(
        instance_id: &str,
        key: &str,
        client: Arc<dyn SectionPluginClientV1>,
        guard: PluginCallGuard,
    ) -> Self {
        Self {
            instance_id: instance_id.to_owned(),
            key: key.to_owned(),
            client,
            guard,
        }
    }

    fn meta(&self, call_kind: CallKind, request_id: Option<Uuid>) -> CallMeta {
        CallMeta {
            request_id,
            call_kind,
            section_key: Some(self.key.clone()),
        }
    }
}

#[async_trait]
impl SectionPluginPort for GuardedSectionPlugin {
    async fn key(&self) -> Result<String, PluginCallError<SectionPluginError>> {
        let client = Arc::clone(&self.client);
        self.guard
            .call(
                &self.instance_id,
                self.meta(CallKind::Key, None),
                async move { client.key().await },
            )
            .await
    }

    async fn schema(
        &self,
        caller: &CallerIdentity,
    ) -> Result<Value, PluginCallError<SectionPluginError>> {
        let client = Arc::clone(&self.client);
        let meta = self.meta(CallKind::Schema, Some(caller.request_id));
        let caller = caller.clone();
        self.guard
            .call(&self.instance_id, meta, async move {
                client.schema(&caller).await
            })
            .await
    }

    async fn apply(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
        section: Value,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        let client = Arc::clone(&self.client);
        let meta = self.meta(CallKind::Apply, Some(caller.request_id));
        let (caller, envelope) = (caller.clone(), envelope.clone());
        self.guard
            .call(&self.instance_id, meta, async move {
                client.apply(&caller, &envelope, section).await
            })
            .await
    }

    async fn remove(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
    ) -> Result<(), PluginCallError<SectionPluginError>> {
        let client = Arc::clone(&self.client);
        let meta = self.meta(CallKind::Remove, Some(caller.request_id));
        let (caller, envelope) = (caller.clone(), envelope.clone());
        self.guard
            .call(&self.instance_id, meta, async move {
                client.remove(&caller, &envelope).await
            })
            .await
    }
}

/// [`AuditSink`] over a scoped [`AuditPluginClientV1`], guarded under `instance_id`.
pub struct GuardedAuditSink {
    instance_id: String,
    client: Arc<dyn AuditPluginClientV1>,
    guard: PluginCallGuard,
}

impl GuardedAuditSink {
    /// Creates the adapter for the audit plugin instance `instance_id`.
    #[must_use]
    pub fn new(
        instance_id: &str,
        client: Arc<dyn AuditPluginClientV1>,
        guard: PluginCallGuard,
    ) -> Self {
        Self {
            instance_id: instance_id.to_owned(),
            client,
            guard,
        }
    }

    fn meta(call_kind: CallKind, caller: &CallerIdentity) -> CallMeta {
        CallMeta {
            request_id: Some(caller.request_id),
            call_kind,
            section_key: None,
        }
    }
}

#[async_trait]
impl AuditSink for GuardedAuditSink {
    async fn request_handoff(
        &self,
        caller: &CallerIdentity,
        content: &RequestHandoffContent,
    ) -> Result<String, PluginCallError<AuditPluginError>> {
        let client = Arc::clone(&self.client);
        let meta = Self::meta(CallKind::RequestHandoff, caller);
        let (caller, content) = (caller.clone(), content.clone());
        self.guard
            .call(&self.instance_id, meta, async move {
                client.request_handoff(&caller, &content).await
            })
            .await
    }

    async fn final_result_handoff(
        &self,
        caller: &CallerIdentity,
        audit_id: &str,
        content: &FinalResultContent,
    ) -> Result<(), PluginCallError<AuditPluginError>> {
        let client = Arc::clone(&self.client);
        let meta = Self::meta(CallKind::FinalResultHandoff, caller);
        let (caller, content, audit_id) = (caller.clone(), content.clone(), audit_id.to_owned());
        self.guard
            .call(&self.instance_id, meta, async move {
                client
                    .final_result_handoff(&caller, &audit_id, &content)
                    .await
            })
            .await
    }
}

#[cfg(test)]
#[allow(clippy::doc_markdown)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "plugin_adapters_tests.rs"]
mod plugin_adapters_tests;
