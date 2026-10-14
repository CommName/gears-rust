//! Unit tests for the final-result hand-off of the audit hand-off client (P7, DESIGN §3.2 "Audit
//! Hand-Off Client" and step 8, §3.3 audit plugin API "Final-result hand-off ... Accepted, or an
//! error kind" with "Any other failure, such as a panic, a transport error, an unresolvable
//! client, a timeout, or a cap refusal, is treated as not accepted. A timeout gives reason
//! `timed_out`, a cap refusal gives reason `not_sent`, and every other failure gives reason
//! `unreachable`", §4.6 `handoff_not_accepted` with `kind`, `reason`, `request_id`, never the
//! content; ADR 0001 no retry).
//!
//! Planned API in `crate::domain::audit_handoff` (the P6 client gains the second hand-off):
//!
//! ```text
//! impl AuditHandoffClient {
//!     /// ONE attempt through the sink; `Ok(())` when accepted. `Err(reason)` when not accepted
//!     /// (`PluginCallError::handoff_reason()`: refused, unreachable, timed_out, not_sent); no
//!     /// retry. Every not-accepted hand-off logs one WARN event `handoff_not_accepted` with
//!     /// `kind="final_result"`, `reason` and `request_id`; never the content.
//!     pub async fn final_handoff(&self, caller: &CallerIdentity, audit_id: &str,
//!                                content: &FinalResultContent) -> Result<(), UnavailableReason>;
//! }
//! ```
#![allow(clippy::duration_suboptimal_units)]

use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::models::{OutcomeCategory, RegistryEffect, UnavailableReason};
use application_manager_sdk::{AuditPluginError, CallerIdentity, FinalResultContent};
use tokio::time::Instant;
use uuid::Uuid;

use crate::domain::audit_handoff::AuditHandoffClient;
use crate::infra::call_guard::PluginCallGuard;
use crate::infra::plugin_adapters::GuardedAuditSink;
use crate::test_support::{
    AuditCall, CallBehavior, FakeAuditPlugin, bounded, capture_logs, field_value, has_field,
};

const SENTINEL: &str = "SENTINEL-final-content-3c9e";

fn caller() -> CallerIdentity {
    CallerIdentity {
        subject_id: Uuid::from_u128(1),
        subject_type: None,
        subject_tenant_id: Uuid::from_u128(2),
        token_scopes: vec![],
        request_id: Uuid::from_u128(0xA11CE),
    }
}

fn content() -> FinalResultContent {
    FinalResultContent {
        audit_id: "audit-xyz".to_owned(),
        request_id: Uuid::from_u128(0xA11CE),
        validation: None,
        sections: Vec::new(),
        category: OutcomeCategory::Succeeded,
        registry_effect: RegistryEffect::Updated,
        completed_at: SENTINEL.to_owned(),
    }
}

fn client(plugin: &Arc<FakeAuditPlugin>, cap: usize) -> AuditHandoffClient {
    let guard = PluginCallGuard::new(Duration::from_millis(500), cap);
    AuditHandoffClient::new(Arc::new(GuardedAuditSink::new(
        "audit-1",
        Arc::clone(plugin) as _,
        guard,
    )))
}

#[tokio::test(start_paused = true)]
async fn an_accepted_final_hand_off_is_one_call_with_the_audit_id_and_the_content() {
    let plugin = Arc::new(FakeAuditPlugin::new());

    let result = client(&plugin, 4)
        .final_handoff(&caller(), "audit-xyz", &content())
        .await;

    assert_eq!(result, Ok(()));
    let calls = plugin.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].kind, AuditCall::FinalResultHandoff);
    assert_eq!(calls[0].audit_id.as_deref(), Some("audit-xyz"));
    assert_eq!(calls[0].result, Some(content()));
    assert_eq!(calls[0].caller, caller());
}

// DESIGN §3.3: the reason of each way the hand-off can fail, one attempt, no retry, one event.
#[tokio::test(start_paused = true)]
async fn a_final_hand_off_that_is_not_accepted_gives_its_reason_logs_once_and_is_not_retried() {
    type B = CallBehavior<AuditPluginError>;
    let cases: [(&str, B, UnavailableReason, &str); 4] = [
        (
            "refused",
            B::err(AuditPluginError::refused("no")),
            UnavailableReason::Refused,
            "refused",
        ),
        (
            "unavailable",
            B::err(AuditPluginError::unavailable("down")),
            UnavailableReason::Unreachable,
            "unreachable",
        ),
        (
            "timeout",
            B::hang(),
            UnavailableReason::TimedOut,
            "timed_out",
        ),
        (
            "panic",
            B::panic(),
            UnavailableReason::Unreachable,
            "unreachable",
        ),
    ];
    for (name, behavior, reason, token) in cases {
        let (log, _guard) = capture_logs();
        let plugin = Arc::new(FakeAuditPlugin::new());
        plugin.on(AuditCall::FinalResultHandoff, behavior);

        let result =
            bounded(client(&plugin, 4).final_handoff(&caller(), "audit-xyz", &content())).await;

        assert_eq!(result, Err(reason), "{name}");
        assert_eq!(plugin.call_count(), 1, "{name}: one attempt");
        let lines = log.lines_with("handoff_not_accepted");
        assert_eq!(lines.len(), 1, "{name}: {lines:?}");
        assert!(lines[0].contains(" WARN "), "{name}: {}", lines[0]);
        assert!(
            has_field(&lines[0], "kind", "final_result"),
            "{name}: {}",
            lines[0]
        );
        assert!(
            has_field(&lines[0], "reason", token),
            "{name}: {}",
            lines[0]
        );
        assert_eq!(
            field_value(&lines[0], "request_id").as_deref(),
            Some(Uuid::from_u128(0xA11CE).to_string().as_str()),
            "{name}: {}",
            lines[0]
        );
        assert!(
            !log.text().contains(SENTINEL),
            "{name}: no content in the log"
        );
    }
}

// A call the guard refuses because the audit plugin's in-flight cap is full is `not_sent`.
#[tokio::test(start_paused = true)]
async fn a_final_hand_off_refused_by_the_in_flight_cap_is_not_sent() {
    let plugin = Arc::new(FakeAuditPlugin::new());
    plugin.on(AuditCall::FinalResultHandoff, CallBehavior::hang());
    let client = client(&plugin, 1);
    // The first call times out and keeps its slot (a detached call still running).
    assert_eq!(
        client
            .final_handoff(&caller(), "audit-xyz", &content())
            .await,
        Err(UnavailableReason::TimedOut)
    );
    let started = Instant::now();

    let second = client
        .final_handoff(&caller(), "audit-xyz", &content())
        .await;

    assert_eq!(second, Err(UnavailableReason::NotSent));
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(
        plugin.call_count(),
        1,
        "the refused call never reached the plugin"
    );
}

// A final-result hand-off logs nothing when accepted.
#[tokio::test(start_paused = true)]
async fn an_accepted_final_hand_off_logs_no_not_accepted_event() {
    let (log, _guard) = capture_logs();
    let plugin = Arc::new(FakeAuditPlugin::new());

    client(&plugin, 4)
        .final_handoff(&caller(), "audit-xyz", &content())
        .await
        .expect("accepted");

    log.assert_live();
    assert!(log.lines_with("handoff_not_accepted").is_empty());
}
