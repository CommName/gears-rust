//! Unit tests for the plugin adapters (DESIGN §3.2 "Plugin Call Guard": "The guard and
//! the ClientHub adapters together implement the section plugin port and the audit
//! sink port"; §3.2 "Ports and Composition"). Paused tokio clock where time matters.
//!
//! Planned API in `crate::infra::plugin_adapters`:
//!
//! ```text
//! /// SectionPluginPort over a scoped SectionPluginClientV1. Every call goes through
//! /// the guard under `instance_id` (the per-plugin in-flight cap is per instance).
//! pub struct GuardedSectionPlugin;
//! impl GuardedSectionPlugin {
//!     pub fn new(instance_id: &str, key: &str, client: Arc<dyn SectionPluginClientV1>,
//!                guard: PluginCallGuard) -> Self;   // `key`: validated, for log metadata
//! }
//! impl SectionPluginPort for GuardedSectionPlugin;  // key/schema/apply/remove
//!
//! /// AuditSink over a scoped AuditPluginClientV1, guarded under `instance_id`.
//! pub struct GuardedAuditSink;
//! impl GuardedAuditSink {
//!     pub fn new(instance_id: &str, client: Arc<dyn AuditPluginClientV1>,
//!                guard: PluginCallGuard) -> Self;
//! }
//! impl AuditSink for GuardedAuditSink;              // request_handoff/final_result_handoff
//! ```
//!
//! Guard metadata (log labels): `section_key` is the adapter's key on every section call
//! (including `key`); `request_id` is the caller's request id for `schema`, `apply`,
//! `remove` and both hand-offs, and absent for `key` (no request at startup).
//!
//! The adapters clone the caller, the envelope and the section into the call, because
//! a guarded call must be `Send + 'static` (§3.2).
//!
//! - `section_key_delegates_to_the_client_through_the_guard` (§3.2)
//! - `section_calls_reach_the_client_with_their_arguments` (§3.3 pass-through)
//! - `every_section_method_is_guarded` / `every_audit_method_is_guarded_under_the_audit_instance`
//!   (hang: TimedOut at exactly the limit, slot held; panic: Internal, slot freed; §3.2)
//! - `every_plugin_error_passes_through_once` (every error kind, call_count 1)
//! - `late_results_carry_the_call_labels` (plugin kind, call kind, section key, request id)
//! - `in_flight_cap_is_per_instance_and_refuses_with_not_sent` (§3.2)
//! - `audit_request_handoff_returns_the_audit_id_and_passes_content` (§3.3)
//! - `audit_final_result_handoff_passes_audit_id_and_content` (§3.3)

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::models::{HandoffOperation, OutcomeCategory, RegistryEffect};
use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginError, CallerIdentity, FinalResultContent,
    RequestHandoffContent, SectionPluginClientV1, SectionPluginError,
};
use serde_json::{Value, json};
use tokio::time::Instant;
use uuid::Uuid;

use crate::domain::ports::{AuditSink, PluginCallError, SectionPluginPort};
use crate::infra::call_guard::PluginCallGuard;
use crate::infra::plugin_adapters::{GuardedAuditSink, GuardedSectionPlugin};
use crate::test_support::{
    AuditCall, CallBehavior, FakeAuditPlugin, FakeSectionPlugin, SectionCall, bounded,
    capture_logs, has_field,
};

const TIMEOUT: Duration = Duration::from_millis(500);

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

fn request_content() -> RequestHandoffContent {
    RequestHandoffContent {
        operation: HandoffOperation::Seed,
        request_id: Uuid::from_u128(3),
        received_at: "2026-10-13T00:00:00Z".to_owned(),
        application: None,
        manifest: Some(json!({"id": "app"})),
        decided_category: None,
        id_length_bytes: None,
        id_sha256: None,
    }
}

fn final_content() -> FinalResultContent {
    FinalResultContent {
        audit_id: "audit-9".to_owned(),
        request_id: Uuid::from_u128(3),
        validation: None,
        sections: vec![],
        category: OutcomeCategory::Succeeded,
        registry_effect: RegistryEffect::Updated,
        completed_at: "2026-10-13T00:00:01Z".to_owned(),
    }
}

fn section_adapter(
    instance: &str,
    guard: &PluginCallGuard,
) -> (Arc<FakeSectionPlugin>, GuardedSectionPlugin) {
    let fake = Arc::new(FakeSectionPlugin::new("net"));
    let client: Arc<dyn SectionPluginClientV1> = fake.clone();
    let adapter = GuardedSectionPlugin::new(instance, "net", client, guard.clone());
    (fake, adapter)
}

fn audit_adapter(guard: &PluginCallGuard) -> (Arc<FakeAuditPlugin>, GuardedAuditSink) {
    let fake = Arc::new(FakeAuditPlugin::new().with_audit_id("audit-9"));
    let adapter = GuardedAuditSink::new("audit.instance", fake.clone(), guard.clone());
    (fake, adapter)
}

#[tokio::test(start_paused = true)]
async fn section_key_delegates_to_the_client_through_the_guard() {
    let guard = PluginCallGuard::new(TIMEOUT, 4);
    let (fake, adapter) = section_adapter("i1", &guard);

    assert_eq!(adapter.key().await, Ok("net".to_owned()));

    assert_eq!(fake.call_count(), 1);
    assert_eq!(fake.calls()[0].kind, SectionCall::Key);
    assert_eq!(guard.in_flight("i1"), 0);
}

#[tokio::test(start_paused = true)]
async fn section_calls_reach_the_client_with_their_arguments() {
    let guard = PluginCallGuard::new(TIMEOUT, 4);
    let (fake, adapter) = section_adapter("i1", &guard);
    let section = json!({"mtu": 1500});

    let schema = adapter.schema(&caller()).await;
    let applied = adapter.apply(&caller(), &envelope(), section.clone()).await;
    let removed = adapter.remove(&caller(), &envelope()).await;

    assert_eq!(schema, Ok(json!({"type": "object"})));
    assert_eq!(applied, Ok(()));
    assert_eq!(removed, Ok(()));
    let calls = fake.calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].kind, SectionCall::Schema);
    assert_eq!(calls[0].caller, Some(caller()));
    assert_eq!(calls[1].kind, SectionCall::Apply);
    assert_eq!(calls[1].caller, Some(caller()));
    assert_eq!(calls[1].envelope, Some(envelope()));
    assert_eq!(calls[1].section, Some(section));
    assert_eq!(calls[2].kind, SectionCall::Remove);
    assert_eq!(calls[2].caller, Some(caller()));
    assert_eq!(calls[2].envelope, Some(envelope()));
}

// DESIGN §3.2: the cap is per plugin instance; a refused call is `not_sent`, never
// a timeout, and a call in flight is not cancelled.
#[tokio::test(start_paused = true)]
async fn in_flight_cap_is_per_instance_and_refuses_with_not_sent() {
    let guard = PluginCallGuard::new(TIMEOUT, 1);
    let (hung, first) = section_adapter("i1", &guard);
    let (_, other) = section_adapter("i2", &guard);
    hung.on(SectionCall::Apply, CallBehavior::hang());
    assert_eq!(
        first.apply(&caller(), &envelope(), Value::Null).await,
        Err(PluginCallError::TimedOut)
    );

    let refused = first.apply(&caller(), &envelope(), Value::Null).await;
    let unaffected = other.apply(&caller(), &envelope(), Value::Null).await;

    assert_eq!(refused, Err(PluginCallError::NotSent));
    assert_eq!(
        hung.call_count(),
        1,
        "the refused call never reached the plugin"
    );
    assert_eq!(unaffected, Ok(()));
}

#[tokio::test(start_paused = true)]
async fn audit_request_handoff_returns_the_audit_id_and_passes_content() {
    let guard = PluginCallGuard::new(TIMEOUT, 4);
    let (fake, sink) = audit_adapter(&guard);

    let audit_id = sink.request_handoff(&caller(), &request_content()).await;

    assert_eq!(audit_id, Ok("audit-9".to_owned()));
    let calls = fake.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].kind, AuditCall::RequestHandoff);
    assert_eq!(calls[0].caller, caller());
    assert_eq!(calls[0].request, Some(request_content()));
}

#[tokio::test(start_paused = true)]
async fn audit_final_result_handoff_passes_audit_id_and_content() {
    let guard = PluginCallGuard::new(TIMEOUT, 4);
    let (fake, sink) = audit_adapter(&guard);

    let done = sink
        .final_result_handoff(&caller(), "audit-9", &final_content())
        .await;

    assert_eq!(done, Ok(()));
    let calls = fake.calls();
    assert_eq!(calls[0].kind, AuditCall::FinalResultHandoff);
    assert_eq!(calls[0].audit_id.as_deref(), Some("audit-9"));
    assert_eq!(calls[0].result, Some(final_content()));
}

const SECTION_CALLS: [SectionCall; 4] = [
    SectionCall::Key,
    SectionCall::Schema,
    SectionCall::Apply,
    SectionCall::Remove,
];
const AUDIT_CALLS: [AuditCall; 2] = [AuditCall::RequestHandoff, AuditCall::FinalResultHandoff];

// Short names of a guarded outcome, so one table can cover calls of every result type.
fn kind<T, E>(result: &Result<T, PluginCallError<E>>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(PluginCallError::Plugin(_)) => "plugin",
        Err(PluginCallError::TimedOut) => "timed_out",
        Err(PluginCallError::NotSent) => "not_sent",
        Err(PluginCallError::Internal(_)) => "internal",
    }
}

// Runs one section port method and returns its outcome kind.
async fn invoke_section(adapter: &GuardedSectionPlugin, call: SectionCall) -> &'static str {
    match call {
        SectionCall::Key => kind(&bounded(adapter.key()).await),
        SectionCall::Schema => kind(&bounded(adapter.schema(&caller())).await),
        SectionCall::Apply => {
            kind(&bounded(adapter.apply(&caller(), &envelope(), Value::Null)).await)
        }
        SectionCall::Remove => kind(&bounded(adapter.remove(&caller(), &envelope())).await),
    }
}

async fn invoke_audit(sink: &GuardedAuditSink, call: AuditCall) -> &'static str {
    match call {
        AuditCall::RequestHandoff => {
            kind(&bounded(sink.request_handoff(&caller(), &request_content())).await)
        }
        AuditCall::FinalResultHandoff => {
            kind(&bounded(sink.final_result_handoff(&caller(), "audit-9", &final_content())).await)
        }
    }
}

// DESIGN §3.2: EVERY port method goes through the guard under the instance id: a call
// that hangs times out after exactly the limit and keeps its in-flight slot (never
// cancelled); a call that panics ends as Internal and frees its slot. A method that
// called the client directly would show neither.
#[tokio::test(start_paused = true)]
async fn every_section_method_is_guarded() {
    for call in SECTION_CALLS {
        let guard = PluginCallGuard::new(TIMEOUT, 4);
        let (fake, adapter) = section_adapter("i1", &guard);
        fake.on(call, CallBehavior::hang());
        let started = Instant::now();
        assert_eq!(
            invoke_section(&adapter, call).await,
            "timed_out",
            "{call:?} hang"
        );
        assert_eq!(started.elapsed(), TIMEOUT, "{call:?} hang: no margin");
        assert_eq!(
            guard.in_flight("i1"),
            1,
            "{call:?} hang: detached, slot held"
        );
        assert_eq!(fake.call_count(), 1, "{call:?} hang");

        let guard = PluginCallGuard::new(TIMEOUT, 4);
        let (fake, adapter) = section_adapter("i1", &guard);
        fake.on(call, CallBehavior::panic());
        assert_eq!(
            invoke_section(&adapter, call).await,
            "internal",
            "{call:?} panic"
        );
        assert_eq!(guard.in_flight("i1"), 0, "{call:?} panic: slot freed");
        assert_eq!(fake.call_count(), 1, "{call:?} panic");
    }
}

// The audit sink guards both hand-offs under its own instance id.
#[tokio::test(start_paused = true)]
async fn every_audit_method_is_guarded_under_the_audit_instance() {
    for call in AUDIT_CALLS {
        let guard = PluginCallGuard::new(TIMEOUT, 4);
        let (fake, sink) = audit_adapter(&guard);
        fake.on(call, CallBehavior::hang());
        let started = Instant::now();
        assert_eq!(
            invoke_audit(&sink, call).await,
            "timed_out",
            "{call:?} hang"
        );
        assert_eq!(started.elapsed(), TIMEOUT, "{call:?} hang: no margin");
        assert_eq!(
            guard.in_flight("audit.instance"),
            1,
            "{call:?} hang: slot held"
        );
        assert_eq!(fake.call_count(), 1, "{call:?} hang");

        let guard = PluginCallGuard::new(TIMEOUT, 4);
        let (fake, sink) = audit_adapter(&guard);
        fake.on(call, CallBehavior::panic());
        assert_eq!(
            invoke_audit(&sink, call).await,
            "internal",
            "{call:?} panic"
        );
        assert_eq!(
            guard.in_flight("audit.instance"),
            0,
            "{call:?} panic: slot freed"
        );
    }
}

// DESIGN §3.2/§3.3: a plugin's own error of every kind passes through unchanged, once:
// the adapter never retries.
#[tokio::test(start_paused = true)]
async fn every_plugin_error_passes_through_once() {
    let errors = [
        SectionPluginError::rejected("bad mtu", Some("/mtu")),
        SectionPluginError::not_permitted("no right"),
        SectionPluginError::internal("db down"),
    ];
    for call in SECTION_CALLS {
        for error in &errors {
            let guard = PluginCallGuard::new(TIMEOUT, 4);
            let (fake, adapter) = section_adapter("i1", &guard);
            fake.on(call, CallBehavior::err(error.clone()));

            let outcome = match call {
                SectionCall::Key => adapter.key().await.map(|_| ()),
                SectionCall::Schema => adapter.schema(&caller()).await.map(|_| ()),
                SectionCall::Apply => adapter.apply(&caller(), &envelope(), Value::Null).await,
                SectionCall::Remove => adapter.remove(&caller(), &envelope()).await,
            };

            assert_eq!(
                outcome,
                Err(PluginCallError::Plugin(error.clone())),
                "{call:?}"
            );
            assert_eq!(fake.call_count(), 1, "{call:?} {error}: no retry");
        }
    }
    for error in [
        AuditPluginError::refused("not accepted"),
        AuditPluginError::unavailable("down"),
    ] {
        for call in AUDIT_CALLS {
            let guard = PluginCallGuard::new(TIMEOUT, 4);
            let (fake, sink) = audit_adapter(&guard);
            fake.on(call, CallBehavior::err(error.clone()));

            let outcome = match call {
                AuditCall::RequestHandoff => sink
                    .request_handoff(&caller(), &request_content())
                    .await
                    .map(|_| ()),
                AuditCall::FinalResultHandoff => {
                    sink.final_result_handoff(&caller(), "audit-9", &final_content())
                        .await
                }
            };

            assert_eq!(
                outcome,
                Err(PluginCallError::Plugin(error.clone())),
                "{call:?}"
            );
            assert_eq!(fake.call_count(), 1, "{call:?} {error}: no retry");
        }
    }
}

// DESIGN §3.2/§4.6: a result that arrives after the caller stopped waiting is logged
// once as `plugin_call_late_result`, labelled with the plugin kind, the call kind, the
// section key (section calls) and the request id (everything but `key`, which belongs
// to no request). The labels come from the adapter's call metadata.
#[tokio::test(start_paused = true)]
async fn late_results_carry_the_call_labels() {
    let slow = Duration::from_secs(2);
    for call in SECTION_CALLS {
        let (logs, _guard) = capture_logs();
        let guard = PluginCallGuard::new(TIMEOUT, 4);
        let (fake, adapter) = section_adapter("i1", &guard);
        fake.on(call, CallBehavior::ok().with_delay(slow));
        assert_eq!(invoke_section(&adapter, call).await, "timed_out");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let lines = logs.lines_with("plugin_call_late_result");
        assert_eq!(lines.len(), 1, "{call:?}: {lines:?}");
        let line = &lines[0];
        let expected = match call {
            SectionCall::Key => "key",
            SectionCall::Schema => "schema",
            SectionCall::Apply => "apply",
            SectionCall::Remove => "remove",
        };
        assert!(has_field(line, "plugin_kind", "section"), "{line}");
        assert!(has_field(line, "call_kind", expected), "{line}");
        assert!(has_field(line, "section_key", "net"), "{line}");
        assert_eq!(
            has_field(line, "request_id", &Uuid::from_u128(3).to_string()),
            call != SectionCall::Key,
            "{call:?}: {line}"
        );
    }
    for call in AUDIT_CALLS {
        let (logs, _guard) = capture_logs();
        let guard = PluginCallGuard::new(TIMEOUT, 4);
        let (fake, sink) = audit_adapter(&guard);
        fake.on(call, CallBehavior::ok().with_delay(slow));
        assert_eq!(invoke_audit(&sink, call).await, "timed_out");

        tokio::time::sleep(Duration::from_secs(5)).await;

        let lines = logs.lines_with("plugin_call_late_result");
        assert_eq!(lines.len(), 1, "{call:?}: {lines:?}");
        let expected = match call {
            AuditCall::RequestHandoff => "request_handoff",
            AuditCall::FinalResultHandoff => "final_result_handoff",
        };
        assert!(has_field(&lines[0], "plugin_kind", "audit"), "{}", lines[0]);
        assert!(has_field(&lines[0], "call_kind", expected), "{}", lines[0]);
        assert!(
            has_field(&lines[0], "request_id", &Uuid::from_u128(3).to_string()),
            "{}",
            lines[0]
        );
        assert!(!lines[0].contains("section_key"), "{}", lines[0]);
    }
}
