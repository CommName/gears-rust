//! Unit tests for the audit hand-off client (DESIGN §3.2 "Audit Hand-Off Client", §3.3 audit
//! contract "Not accepted", §4.6 logs: "hand-off not accepted, with the hand-off kind and
//! reason").
//!
//! Planned API in `crate::domain::audit_handoff`:
//!
//! ```text
//! pub struct AuditHandoffClient;
//! impl AuditHandoffClient {
//!     pub fn new(sink: Arc<dyn AuditSink>) -> Self;
//!     /// ONE attempt through the sink. `Ok(audit_id)` when accepted. `Err(reason)` when not
//!     /// accepted, with `PluginCallError::handoff_reason()` (refused, unreachable, timed_out,
//!     /// not_sent); no audit id; no retry. Every not-accepted hand-off logs one WARN event
//!     /// `handoff_not_accepted` with fields `kind` (`request`), `reason` (the reason token)
//!     /// and `request_id`; never the content.
//!     pub async fn request_handoff(&self, caller: &CallerIdentity,
//!                                  content: &RequestHandoffContent)
//!                                  -> Result<String, UnavailableReason>;
//! }
//! pub(crate) fn reason_token(reason: UnavailableReason) -> &'static str; // the serde token
//! ```
//!
//! The final-result hand-off is P7 (steps 6-8); the client gains it there. The client does
//! not assemble the hand-off content (the orchestrator does, DESIGN §3.1 "Audit event
//! content") and keeps no counters (hand-off counters by kind come with the metrics
//! catalogue, DESIGN §4.6, P9).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use application_manager_sdk::models::{HandoffOperation, UnavailableReason};
use application_manager_sdk::{
    AuditPluginError, CallerIdentity, FinalResultContent, RequestHandoffContent,
};
use async_trait::async_trait;
use serde_json::json;
use uuid::Uuid;

use crate::domain::audit_handoff::{AuditHandoffClient, reason_token};
use crate::domain::ports::{AuditSink, PluginCallError};
use crate::infra::call_guard::PluginCallGuard;
use crate::infra::plugin_adapters::GuardedAuditSink;
use crate::test_support::{
    AuditCall, CallBehavior, FakeAuditPlugin, bounded, capture_logs, field_value, has_field,
};

const SENTINEL: &str = "SENTINEL-manifest-value-7f3a";

fn caller() -> CallerIdentity {
    CallerIdentity {
        subject_id: Uuid::from_u128(1),
        subject_type: None,
        subject_tenant_id: Uuid::from_u128(2),
        token_scopes: vec![],
        request_id: Uuid::from_u128(0xA11CE),
    }
}

fn content() -> RequestHandoffContent {
    RequestHandoffContent {
        operation: HandoffOperation::Seed,
        request_id: Uuid::from_u128(0xA11CE),
        received_at: "2027-01-15T08:00:00Z".to_owned(),
        application: None,
        manifest: Some(json!({"secret": SENTINEL})),
        decided_category: None,
        id_length_bytes: None,
        id_sha256: None,
    }
}

// A sink with the planned port signature that answers one scripted result and counts calls.
struct ScriptedSink {
    result: Result<String, PluginCallError<AuditPluginError>>,
    calls: Mutex<usize>,
}

impl ScriptedSink {
    fn new(result: Result<String, PluginCallError<AuditPluginError>>) -> Arc<Self> {
        Arc::new(Self {
            result,
            calls: Mutex::new(0),
        })
    }

    fn calls(&self) -> usize {
        *self.calls.lock().expect("lock")
    }
}

#[async_trait]
impl AuditSink for ScriptedSink {
    async fn request_handoff(
        &self,
        _caller: &CallerIdentity,
        _content: &RequestHandoffContent,
    ) -> Result<String, PluginCallError<AuditPluginError>> {
        *self.calls.lock().expect("lock") += 1;
        self.result.clone()
    }

    async fn final_result_handoff(
        &self,
        _caller: &CallerIdentity,
        _audit_id: &str,
        _content: &FinalResultContent,
    ) -> Result<(), PluginCallError<AuditPluginError>> {
        unreachable!("the request hand-off client never makes the final-result hand-off in P6")
    }
}

#[tokio::test]
async fn an_accepted_hand_off_returns_the_audit_id_unchanged() {
    let sink = ScriptedSink::new(Ok("audit-xyz".to_owned()));
    let client = AuditHandoffClient::new(Arc::clone(&sink) as Arc<dyn AuditSink>);

    let result = client.request_handoff(&caller(), &content()).await;

    assert_eq!(result, Ok("audit-xyz".to_owned()));
    assert_eq!(sink.calls(), 1);
}

#[tokio::test]
async fn an_accepted_hand_off_logs_no_not_accepted_event() {
    let (logs, _guard) = capture_logs();
    let client = AuditHandoffClient::new(ScriptedSink::new(Ok("a".to_owned())));

    client
        .request_handoff(&caller(), &content())
        .await
        .expect("accepted");

    logs.assert_live();
    assert!(
        logs.lines_with("handoff_not_accepted").is_empty(),
        "{}",
        logs.text()
    );
}

// DESIGN §3.3: a refusal, an error, a timeout or a cap refusal (and any other failure) is
// not accepted, with reason refused / unreachable / timed_out / not_sent / unreachable.
#[tokio::test]
async fn every_failure_is_not_accepted_with_its_reason() {
    let cases: [(PluginCallError<AuditPluginError>, UnavailableReason, &str); 5] = [
        (
            PluginCallError::Plugin(AuditPluginError::refused("no")),
            UnavailableReason::Refused,
            "refused",
        ),
        (
            PluginCallError::Plugin(AuditPluginError::unavailable("down")),
            UnavailableReason::Unreachable,
            "unreachable",
        ),
        (
            PluginCallError::TimedOut,
            UnavailableReason::TimedOut,
            "timed_out",
        ),
        (
            PluginCallError::NotSent,
            UnavailableReason::NotSent,
            "not_sent",
        ),
        (
            PluginCallError::Internal("plugin call failed".to_owned()),
            UnavailableReason::Unreachable,
            "unreachable",
        ),
    ];
    for (failure, reason, token) in cases {
        let (logs, _guard) = capture_logs();
        let sink = ScriptedSink::new(Err(failure.clone()));
        let client = AuditHandoffClient::new(Arc::clone(&sink) as Arc<dyn AuditSink>);

        let result = client.request_handoff(&caller(), &content()).await;

        assert_eq!(result, Err(reason), "{failure:?}");
        assert_eq!(reason_token(reason), token);
        assert_eq!(
            sink.calls(),
            1,
            "exactly one attempt, no retry: {failure:?}"
        );
        // The reason the client logs is the reason it returns, spelled as its wire token.
        logs.assert_live();
        let lines = logs.lines_with("handoff_not_accepted");
        assert_eq!(lines.len(), 1, "{failure:?}: {lines:?}");
        assert!(has_field(&lines[0], "reason", token), "{}", lines[0]);
        assert!(has_field(&lines[0], "kind", "request"), "{}", lines[0]);
    }
}

// Every reason, from a match without a wildcard: a reason added to the SDK breaks this
// function at compile time, so the token test below cannot silently skip it.
fn every_reason() -> Vec<UnavailableReason> {
    let all = [
        UnavailableReason::Unreachable,
        UnavailableReason::TimedOut,
        UnavailableReason::Refused,
        UnavailableReason::Unusable,
        UnavailableReason::NotSent,
        UnavailableReason::NotReady,
        UnavailableReason::AtCapacity,
    ];
    for reason in all {
        match reason {
            UnavailableReason::Unreachable
            | UnavailableReason::TimedOut
            | UnavailableReason::Refused
            | UnavailableReason::Unusable
            | UnavailableReason::NotSent
            | UnavailableReason::NotReady
            | UnavailableReason::AtCapacity => {}
        }
    }
    all.to_vec()
}

#[test]
fn reason_tokens_are_the_wire_tokens_of_every_reason() {
    for reason in every_reason() {
        assert_eq!(
            json!(reason_token(reason)),
            serde_json::to_value(reason).expect("serializes"),
            "{reason:?}"
        );
    }
}

#[tokio::test]
async fn a_not_accepted_hand_off_logs_one_warn_event_with_kind_reason_and_request_id() {
    let (logs, _guard) = capture_logs();
    let client = AuditHandoffClient::new(ScriptedSink::new(Err(PluginCallError::Plugin(
        AuditPluginError::refused("cannot record"),
    ))));

    client
        .request_handoff(&caller(), &content())
        .await
        .expect_err("refused");

    logs.assert_live();
    let lines = logs.lines_with("handoff_not_accepted");
    assert_eq!(lines.len(), 1, "{lines:?}");
    let line = &lines[0];
    assert!(line.contains("WARN"), "{line}");
    assert!(has_field(line, "kind", "request"), "{line}");
    assert!(has_field(line, "reason", "refused"), "{line}");
    assert_eq!(
        field_value(line, "request_id").as_deref(),
        Some(Uuid::from_u128(0xA11CE).to_string().as_str()),
        "{line}"
    );
}

#[tokio::test]
async fn the_not_accepted_event_never_carries_the_manifest_or_the_plugin_detail() {
    let (logs, _guard) = capture_logs();
    let client = AuditHandoffClient::new(ScriptedSink::new(Err(PluginCallError::Plugin(
        AuditPluginError::unavailable("plugin said: PLUGIN-DETAIL-SECRET"),
    ))));

    client
        .request_handoff(&caller(), &content())
        .await
        .expect_err("unavailable");

    logs.assert_live();
    assert!(!logs.lines_with("handoff_not_accepted").is_empty());
    assert!(!logs.text().contains(SENTINEL), "manifest value in the log");
    assert!(
        !logs.text().contains("PLUGIN-DETAIL-SECRET"),
        "plugin detail in the log"
    );
}

// Through the real guarded adapter (the production composition): the call guard's own
// outcomes reach the client as timeout and cap refusal.
#[tokio::test(start_paused = true)]
async fn through_the_guarded_sink_a_hung_audit_plugin_is_not_accepted_as_timed_out() {
    let plugin = Arc::new(FakeAuditPlugin::new());
    plugin.on(AuditCall::RequestHandoff, CallBehavior::hang());
    let guard = PluginCallGuard::new(Duration::from_millis(500), 4);
    let sink = Arc::new(GuardedAuditSink::new("audit-1", plugin.clone(), guard));
    let client = AuditHandoffClient::new(sink);
    let started = tokio::time::Instant::now();

    let result = bounded(client.request_handoff(&caller(), &content())).await;

    assert_eq!(result, Err(UnavailableReason::TimedOut));
    assert_eq!(
        started.elapsed(),
        Duration::from_millis(500),
        "one plugin time limit"
    );
    assert_eq!(plugin.call_count(), 1, "one attempt");
}

#[tokio::test(start_paused = true)]
async fn through_the_guarded_sink_a_full_cap_is_not_accepted_as_not_sent() {
    let plugin = Arc::new(FakeAuditPlugin::new());
    plugin.on(AuditCall::RequestHandoff, CallBehavior::hang());
    let guard = PluginCallGuard::new(Duration::from_millis(500), 1);
    let sink = Arc::new(GuardedAuditSink::new("audit-1", plugin.clone(), guard));
    let client = AuditHandoffClient::new(sink);
    // The first call hangs past its limit and keeps its slot (detached, never cancelled).
    assert_eq!(
        bounded(client.request_handoff(&caller(), &content())).await,
        Err(UnavailableReason::TimedOut)
    );

    let second = bounded(client.request_handoff(&caller(), &content())).await;

    assert_eq!(second, Err(UnavailableReason::NotSent));
    assert_eq!(
        plugin.call_count(),
        1,
        "a cap refusal never reaches the plugin"
    );
}

#[tokio::test]
async fn through_the_guarded_sink_the_plugin_receives_the_content_and_caller_unchanged() {
    let plugin = Arc::new(FakeAuditPlugin::new().with_audit_id("audit-77"));
    let guard = PluginCallGuard::new(Duration::from_secs(1), 4);
    let client = AuditHandoffClient::new(Arc::new(GuardedAuditSink::new(
        "audit-1",
        plugin.clone(),
        guard,
    )));
    let (caller, content) = (caller(), content());

    let result = client.request_handoff(&caller, &content).await;

    assert_eq!(result, Ok("audit-77".to_owned()));
    let calls = plugin.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].caller, caller);
    assert_eq!(calls[0].request.as_ref(), Some(&content));
}

// DESIGN §3.3 "Audit contract": the audit id is opaque to the gear; it is returned and later
// echoed exactly as the plugin gave it.
#[tokio::test]
async fn an_audit_id_is_opaque_and_returned_unchanged_whatever_it_holds() {
    for id in [
        "  spaced out  ",
        "caf\u{e9}-\u{1f600}",
        "tab\tand\nnewline",
        "",
        " ",
        "audit/with/slashes?and=query",
    ] {
        let sink = ScriptedSink::new(Ok(id.to_owned()));
        let client = AuditHandoffClient::new(sink);

        let result = client.request_handoff(&caller(), &content()).await;

        assert_eq!(result, Ok(id.to_owned()), "audit id {id:?}");
    }
}

#[tokio::test]
async fn an_empty_audit_id_from_the_plugin_is_accepted_and_logs_nothing() {
    let (logs, _guard) = capture_logs();
    let client = AuditHandoffClient::new(ScriptedSink::new(Ok(String::new())));

    let result = client.request_handoff(&caller(), &content()).await;

    assert_eq!(result, Ok(String::new()), "opaque: not judged by the gear");
    logs.assert_live();
    assert!(logs.lines_with("handoff_not_accepted").is_empty());
}
