//! Request orchestrator (`cpt-cf-application-manager-component-request-orchestrator`):
//! the synchronous checks, acceptance, the detached request task, dispatch
//! in priority order, and supervisor completion.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use time::OffsetDateTime;
use tokio::sync::OwnedSemaphorePermit;
use toolkit_macros::domain_model;
use toolkit_security::SecurityContext;
use uuid::Uuid;

use crate::domain::error::DomainError;
use crate::domain::plugin_set::StartupState;
use crate::domain::ports::{AccessControl, Clock, OperationRepository};
use crate::domain::router::SectionPluginRouter;
use crate::domain::schema_validator::SchemaValidator;

/// The ids and time assigned when a request is received (the REST layer
/// builds one before the gate).
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    /// The request id; the operation id when the request is accepted.
    pub request_id: Uuid,
    /// Id of the accepted event.
    pub accepted_event_id: Uuid,
    /// Id of the finished event.
    pub finished_event_id: Uuid,
    /// Id of the refused event.
    pub refused_event_id: Uuid,
    /// Receive time.
    pub received_at: OffsetDateTime,
}

impl Receipt {
    /// Fresh time-ordered ids for a request received at `received_at`.
    #[must_use]
    pub fn new(received_at: OffsetDateTime) -> Self {
        Self {
            request_id: Uuid::now_v7(),
            accepted_event_id: Uuid::now_v7(),
            finished_event_id: Uuid::now_v7(),
            refused_event_id: Uuid::now_v7(),
            received_at,
        }
    }
}

/// A request as the REST layer received it. Bodies are raw: parsing is part
/// of the request task, so a body that is not JSON is audited like any other
/// envelope problem.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestInput {
    /// `POST /applications`: the manifest.
    Seed(Bytes),
    /// `DELETE /applications/{application_id}`: the decoded path segment.
    UninstallById(String),
    /// `POST /applications:uninstall`: a body with `application_id`.
    UninstallBody(Bytes),
}

/// An accepted request.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accepted {
    /// The operation id (the request id).
    pub operation_id: Uuid,
}

/// The settings the orchestrator needs.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrchestratorSettings {
    /// Time limit of every plugin call; unit of the run window.
    pub plugin_call_timeout: Duration,
    /// Added to the run window.
    pub run_window_margin: Duration,
    /// Maximum bytes of the application id and version.
    pub max_envelope_text_length: u32,
}

/// Runs each seed and uninstall as a supervised request task.
#[domain_model]
pub struct RequestOrchestrator {
    #[allow(dead_code, reason = "implemented in PR 5")]
    access: Arc<dyn AccessControl>,
    #[allow(dead_code, reason = "implemented in PR 5")]
    repo: Arc<dyn OperationRepository>,
    #[allow(dead_code, reason = "implemented in PR 5")]
    router: SectionPluginRouter,
    #[allow(dead_code, reason = "implemented in PR 5")]
    validator: Arc<SchemaValidator>,
    #[allow(dead_code, reason = "implemented in PR 5")]
    startup: Arc<StartupState>,
    #[allow(dead_code, reason = "implemented in PR 5")]
    clock: Arc<dyn Clock>,
    #[allow(dead_code, reason = "implemented in PR 5")]
    settings: OrchestratorSettings,
}

impl fmt::Debug for RequestOrchestrator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestOrchestrator")
            .finish_non_exhaustive()
    }
}

impl RequestOrchestrator {
    /// An orchestrator over the ports and the startup result.
    #[must_use]
    pub fn new(
        access: Arc<dyn AccessControl>,
        repo: Arc<dyn OperationRepository>,
        router: SectionPluginRouter,
        validator: Arc<SchemaValidator>,
        startup: Arc<StartupState>,
        clock: Arc<dyn Clock>,
        settings: OrchestratorSettings,
    ) -> Self {
        Self {
            access,
            repo,
            router,
            validator,
            startup,
            clock,
            settings,
        }
    }

    /// Run a request that passed the gate: spawn the detached request task,
    /// which owns `permit` until the operation is final, and wait for its
    /// synchronous answer. The task keeps running when this future is
    /// dropped. A task that ends before it answers is a
    /// [`DomainError::Defect`] with a refused event of category internal
    /// failure.
    ///
    /// # Errors
    /// The refusal of ranks 1 to 4, [`DomainError::StorageUnavailable`] at
    /// acceptance, a conflict naming the winner of a lost race, or
    /// [`DomainError::Defect`].
    pub async fn submit(
        self: Arc<Self>,
        _ctx: SecurityContext,
        _input: RequestInput,
        _receipt: Receipt,
        _permit: OwnedSemaphorePermit,
    ) -> Result<Accepted, DomainError> {
        todo!("PR 5: the request task")
    }

    /// Record the refused event of a gate refusal (`NotReady` or
    /// `AtCapacity`, rank 1). Never fails: a missing event is logged.
    pub async fn refuse_at_gate(
        &self,
        _ctx: &SecurityContext,
        _input: &RequestInput,
        _receipt: &Receipt,
        _refusal: &DomainError,
    ) {
        todo!("PR 5: the gate refusal event")
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "orchestrator_tests.rs"]
mod tests;
