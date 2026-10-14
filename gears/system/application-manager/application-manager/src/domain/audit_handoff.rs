//! Audit hand-off client (DESIGN §3.2 "Audit Hand-Off Client").
//!
//! The orchestrator assembles the request hand-off content; the client makes one attempt
//! through the guarded audit sink (no retry, ADR 0001), maps every failure to a reason and
//! logs `handoff_not_accepted` with the kind, the reason and the request id, never the content.

use std::sync::Arc;

use application_manager_sdk::models::UnavailableReason;
use application_manager_sdk::{CallerIdentity, RequestHandoffContent};

use toolkit_macros::domain_model;

use crate::domain::ports::AuditSink;

/// Token of an unavailable reason, as logged.
#[must_use]
pub(crate) fn reason_token(reason: UnavailableReason) -> &'static str {
    match reason {
        UnavailableReason::Unreachable => "unreachable",
        UnavailableReason::TimedOut => "timed_out",
        UnavailableReason::Refused => "refused",
        UnavailableReason::Unusable => "unusable",
        UnavailableReason::NotSent => "not_sent",
        UnavailableReason::NotReady => "not_ready",
        UnavailableReason::AtCapacity => "at_capacity",
    }
}

/// The `handoff_not_accepted` event of a request hand-off that did not take place or was not
/// accepted: its kind, the reason and the request id, never the content.
pub(crate) fn log_not_accepted(request_id: uuid::Uuid, reason: UnavailableReason) {
    tracing::warn!(
        kind = "request",
        reason = reason_token(reason),
        request_id = %request_id,
        "handoff_not_accepted"
    );
}

/// Makes the request hand-off through the audit sink port, once, and treats every failure as
/// not accepted.
#[domain_model]
pub struct AuditHandoffClient {
    sink: Arc<dyn AuditSink>,
}

impl AuditHandoffClient {
    /// Builds the client over the audit sink.
    #[must_use]
    pub fn new(sink: Arc<dyn AuditSink>) -> Self {
        Self { sink }
    }

    /// One attempt. The audit id is opaque and returned unchanged (an empty one is accepted).
    ///
    /// # Errors
    ///
    /// The reason the hand-off was not accepted; the event is logged before returning.
    pub async fn request_handoff(
        &self,
        caller: &CallerIdentity,
        content: &RequestHandoffContent,
    ) -> Result<String, UnavailableReason> {
        match self.sink.request_handoff(caller, content).await {
            Ok(audit_id) => Ok(audit_id),
            Err(e) => {
                let reason = e.handoff_reason();
                log_not_accepted(caller.request_id, reason);
                Err(reason)
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "audit_handoff_tests.rs"]
mod audit_handoff_tests;
