//! Compile-level shape tests for the audit plugin API (DESIGN §3.3).
//!
//! Planned API: `AuditPluginClientV1::{request_handoff, final_result_handoff}`.
//! `AuditPluginError` lives in `crate::errors` next to `SectionPluginError`.

use std::sync::Arc;

use async_trait::async_trait;

use crate::audit_plugin_api::AuditPluginClientV1;
use crate::errors::AuditPluginError;
use crate::models::{CallerIdentity, FinalResultContent, RequestHandoffContent};

struct Double;

// DESIGN §3.3 audit-plugin-api: request hand-off returns an opaque audit id;
// final-result hand-off returns acceptance only. The caller identity is an
// argument only and never part of the content. The gear guarantees that the
// content's `request_id` equals the identity's request id, and that the final-result
// content's `audit_id` equals the audit id argument (gear-side rule, not an SDK type
// rule).
#[async_trait]
impl AuditPluginClientV1 for Double {
    async fn request_handoff(
        &self,
        _caller: &CallerIdentity,
        _content: &RequestHandoffContent,
    ) -> Result<String, AuditPluginError> {
        Ok("audit-1".to_owned())
    }

    async fn final_result_handoff(
        &self,
        _caller: &CallerIdentity,
        _audit_id: &str,
        _content: &FinalResultContent,
    ) -> Result<(), AuditPluginError> {
        Err(AuditPluginError::unavailable("down"))
    }
}

// DESIGN §3.3: the audit plugin is a scoped client, so it must be object-safe.
#[test]
fn audit_plugin_client_is_object_safe_and_send_sync() {
    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<dyn AuditPluginClientV1>();
    let _client: Arc<dyn AuditPluginClientV1> = Arc::new(Double);
}

// DESIGN §3.3: the gear awaits plugin calls inside spawned request tasks, so every
// future returned through the `dyn` handle must be `Send`.
#[test]
fn audit_plugin_client_futures_are_send_through_dyn() {
    fn assert_send<T: Send>(_: T) {}
    #[allow(dead_code)]
    fn check(
        client: &dyn AuditPluginClientV1,
        caller: &CallerIdentity,
        request: &RequestHandoffContent,
        result: &FinalResultContent,
    ) {
        assert_send(client.request_handoff(caller, request));
        assert_send(client.final_result_handoff(caller, "audit-1", result));
    }
}
