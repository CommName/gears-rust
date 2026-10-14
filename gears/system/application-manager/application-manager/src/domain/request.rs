//! The request task, its progress record and its supervisor, and steps 6-8 of a request:
//! dispatch in priority order, the registry write and the final-result hand-off (DESIGN §3.2
//! "Request Orchestrator", §3.6 "Seed and Re-Seed" and "Uninstall", §4.5, §4.6).
//!
//! Steps 1-8 run as one request task, detached from the HTTP request. The orchestrator starts a
//! supervisor; the supervisor starts the request task and awaits it, and the REST handler only
//! awaits the supervisor through a [`RequestHandle`] (which cannot abort it), so a caller that
//! disconnects cuts no step short. If the request task panics or fails to join, the supervisor
//! ends the request as internal failure and completes the audit steps from the request progress
//! record, without parsing the request again; a panic inside that recovery is contained too. The
//! supervisor calls no section plugin and runs no registry operation; its only plugin calls are
//! the two guarded hand-offs, and each of them is attempted at most once across the task and the
//! supervisor.
//!
//! The request-task permit (the cap `max_request_tasks`), its tracker token (which a drain waits
//! for) and the in-flight gauge are held until the supervisor ends, recovery included.
//!
//! No log event carries request content: only operations, reasons, counts and the request id.

use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::SystemTime;

use application_manager_sdk::errors::truncate_detail;
use application_manager_sdk::models::{
    ApplicationRef, Dependency, FinalSection, HandoffOperation, OutcomeCategory, OutcomeReport,
    RegistryEffect, SectionOutcome, SectionReport, UnavailableEntry, UnavailableReason,
    ValidationResult,
};
use application_manager_sdk::{
    ApplicationEnvelope, CallerIdentity, FinalResultContent, RequestHandoffContent,
    SectionPluginError,
};
use futures::FutureExt;
use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;
use toolkit_macros::domain_model;
use toolkit_security::SecurityContext;
use tracing::Instrument;
use uuid::Uuid;

use crate::domain::audit_handoff::{AuditHandoffClient, reason_token};
use crate::domain::dispatch::{after_dispatch_category, derive_last_result, is_partial};
use crate::domain::orchestrator::{
    GateOutcome, GateResult, Gated, Pipeline, Proceed, RequestInput, rfc3339,
};
use crate::domain::ports::{PluginCallError, RegistryError};
use crate::domain::registry::RegistryEntry;
use crate::domain::startup::StartupResult;

/// A request-task permit: one of the `max_request_tasks`. It carries a token of the task
/// tracker, so a drain waits for it from the moment it was taken, and it is released (permit and
/// token) when the supervisor started with it ends, or when it is dropped unused.
#[domain_model]
pub struct RequestPermit {
    _permit: OwnedSemaphorePermit,
    _token: TaskTrackerToken,
    operation: HandoffOperation,
}

impl RequestPermit {
    /// The operation the permit was admitted for. The request that is started decides its own
    /// operation (the input); this is what the refusal log of the admission named.
    #[must_use]
    pub fn operation(&self) -> HandoffOperation {
        self.operation
    }
}

/// No request-task permit was free: the cap `max_request_tasks` is reached, or admission is
/// closed because the gear is stopping.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtCapacity;

impl AtCapacity {
    /// The report of a request refused at the cap: temporarily unavailable, with the
    /// `audit_plugin` item and reason `at_capacity` (DESIGN §4.5), nothing called and the
    /// registry unchanged.
    #[must_use]
    #[allow(
        clippy::unused_self,
        reason = "reads as a property of the refusal: `at_capacity.report()`"
    )]
    pub fn report(self) -> OutcomeReport {
        OutcomeReport {
            category: OutcomeCategory::TemporarilyUnavailable,
            application: None,
            partial: false,
            sections: Vec::new(),
            problems: Vec::new(),
            unavailable: vec![UnavailableEntry {
                dependency: Dependency::AuditPlugin,
                key: None,
                reason: UnavailableReason::AtCapacity,
            }],
            registry_effect: RegistryEffect::Unchanged,
            audit_id: None,
        }
    }
}

fn operation_token(operation: HandoffOperation) -> &'static str {
    match operation {
        HandoffOperation::Seed => "seed",
        HandoffOperation::Uninstall => "uninstall",
    }
}

/// Takes a permit of `slots` without waiting, with a token of the tracker of `tasks`.
///
/// The token is taken first and dropped if the semaphore refuses: a drain that closes the
/// semaphore after this permit was taken has already been counted by the tracker, so it waits.
///
/// # Errors
///
/// [`AtCapacity`] when none is free or the semaphore is closed; the refusal is logged as a
/// request hand-off that was not accepted, with the operation.
pub(crate) fn try_admit(
    slots: &Arc<Semaphore>,
    tasks: &Tasks,
    operation: HandoffOperation,
) -> Result<RequestPermit, AtCapacity> {
    let token = tasks.tracker.token();
    if let Ok(permit) = Arc::clone(slots).try_acquire_owned() {
        Ok(RequestPermit {
            _permit: permit,
            _token: token,
            operation,
        })
    } else {
        tracing::warn!(
            kind = "request",
            reason = reason_token(UnavailableReason::AtCapacity),
            operation = operation_token(operation),
            "handoff_not_accepted"
        );
        Err(AtCapacity)
    }
}

/// How far the registry write got.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum RegistryProgress {
    /// No write started.
    #[default]
    NoneYet,
    /// Set immediately before the upsert or delete.
    WriteStarted,
    /// The write ended with this effect.
    Result(RegistryEffect),
}

/// What the request hand-off needs from the receive step, retained so the supervisor's recovery
/// never parses the input: the operation, the application, the parsed manifest of a seed, and the
/// byte length and digest of a never-storable uninstall id.
pub(crate) struct Retained {
    pub(crate) operation: HandoffOperation,
    pub(crate) application: Option<ApplicationRef>,
    pub(crate) manifest: Option<Value>,
    pub(crate) id_length_bytes: Option<u64>,
    pub(crate) id_sha256: Option<String>,
}

/// The caller and the receive time of the request.
struct Begun {
    caller: CallerIdentity,
    received_time: SystemTime,
}

/// The fields of the progress record (DESIGN §3.2 "Request progress record").
#[derive(Default)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent progress points, each set once at its own moment"
)]
struct Record {
    begun: Option<Begun>,
    retained: Option<Retained>,
    decided: Option<OutcomeCategory>,
    handoff_attempted: bool,
    audit_id: Option<String>,
    sections: Vec<SectionReport>,
    registry: RegistryProgress,
    final_attempted: bool,
    /// Set immediately before the first section plugin call.
    dispatch_started: bool,
    /// The fingerprints of the schemas of a seed whose validation passed (step 5), by key.
    fingerprints: Vec<(String, String)>,
}

/// The request progress record: what the request task did so far, shared with its supervisor.
/// The task writes each field in one short update, never across a call.
#[derive(Default)]
pub(crate) struct Progress(Mutex<Record>);

/// Redacting: the record holds the request content, so `Debug` shows nothing of it.
impl fmt::Debug for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Progress").finish_non_exhaustive()
    }
}

impl Progress {
    fn update(&self, change: impl FnOnce(&mut Record)) {
        // A panic cannot happen inside an update; a poisoned lock still holds a whole record.
        change(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner));
    }

    /// The caller identity (with the request id) and the receive time.
    pub(crate) fn begin(&self, caller: CallerIdentity, received_time: SystemTime) {
        self.update(|r| {
            r.begun = Some(Begun {
                caller,
                received_time,
            });
        });
    }

    /// The request content the hand-off needs, once the receive step has produced it.
    pub(crate) fn received(&self, retained: Retained) {
        self.update(|r| r.retained = Some(retained));
    }

    /// The category decided before the request hand-off, the moment one applies.
    pub(crate) fn decided(&self, category: OutcomeCategory) {
        self.update(|r| r.decided = Some(category));
    }

    /// Set immediately before the request hand-off call.
    pub(crate) fn handoff_attempted(&self) {
        self.update(|r| r.handoff_attempted = true);
    }

    /// The request hand-off was accepted under `audit_id`.
    pub(crate) fn handoff_accepted(&self, audit_id: &str) {
        self.update(|r| r.audit_id = Some(audit_id.to_owned()));
    }

    /// The schema fingerprints, when the validation of step 5 passed.
    pub(crate) fn fingerprints(&self, fingerprints: Vec<(String, String)>) {
        self.update(|r| r.fingerprints = fingerprints);
    }

    /// Set immediately before a section plugin call (the first one starts the dispatch).
    fn dispatch_started(&self) {
        self.update(|r| r.dispatch_started = true);
    }

    fn section(&self, report: SectionReport) {
        self.update(|r| r.sections.push(report));
    }

    fn registry_write_started(&self) {
        self.update(|r| r.registry = RegistryProgress::WriteStarted);
    }

    fn registry_result(&self, effect: RegistryEffect) {
        self.update(|r| r.registry = RegistryProgress::Result(effect));
    }

    /// Set immediately before the final-result hand-off call.
    fn final_attempted(&self) {
        self.update(|r| r.final_attempted = true);
    }

    /// Moves the record out, for the supervisor's recovery. What is written afterwards (the
    /// recovery's own accepted hand-off) lands in a fresh record.
    fn take(&self) -> Record {
        let mut record = Record::default();
        self.update(|r| std::mem::swap(r, &mut record));
        record
    }
}

/// One in-flight request task of an operation, counted until dropped.
struct Gauge(Arc<AtomicUsize>);

impl Gauge {
    fn enter(count: &Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(count))
    }
}

impl Drop for Gauge {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The supervisors in flight: the tracker, the gauge per operation and the drain.
///
/// Every permit carries one token of the tracker, and the token moves into the supervisor, so the
/// tracker counts a request from the moment it was admitted to the end of its supervisor.
#[derive(Default)]
pub(crate) struct Tasks {
    tracker: TaskTracker,
    seeds: Arc<AtomicUsize>,
    uninstalls: Arc<AtomicUsize>,
}

/// The join handle of a supervisor, as the REST handler holds it: it can be awaited and it can be
/// dropped (a disconnect), but it cannot abort the supervisor, which would free the permit and
/// the gauge while the request task still runs.
#[domain_model]
#[derive(Debug)]
pub struct RequestHandle(JoinHandle<OutcomeReport>);

impl RequestHandle {
    /// Whether the supervisor has ended (nothing about how).
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

impl Future for RequestHandle {
    type Output = Result<OutcomeReport, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

impl Tasks {
    fn gauge(&self, operation: HandoffOperation) -> &Arc<AtomicUsize> {
        match operation {
            HandoffOperation::Seed => &self.seeds,
            HandoffOperation::Uninstall => &self.uninstalls,
        }
    }

    /// Supervisors that have not ended.
    pub(crate) fn in_flight(&self) -> usize {
        self.seeds.load(Ordering::SeqCst) + self.uninstalls.load(Ordering::SeqCst)
    }

    /// Starts the supervisor of one request. The input decides the operation; the gauge counts
    /// the request from here to the end of its supervisor.
    pub(crate) fn start(
        &self,
        pipeline: &Arc<Pipeline>,
        permit: RequestPermit,
        ctx: SecurityContext,
        input: RequestInput,
    ) -> RequestHandle {
        let operation = input.operation();
        let gauge = Gauge::enter(self.gauge(operation));
        RequestHandle(tokio::spawn(
            supervise(Arc::clone(pipeline), permit, gauge, operation, ctx, input)
                .instrument(tracing::Span::current()),
        ))
    }

    /// Closes the tracker to new tokens, then waits for the permits and the running supervisors
    /// until `deadline` is cancelled. Returns how many it abandoned (every token still held) and
    /// logs the count per operation, and the permits that were taken and never used.
    pub(crate) async fn drain(&self, deadline: &CancellationToken) -> usize {
        self.tracker.close();
        tokio::select! {
            biased;
            () = self.tracker.wait() => 0,
            () = deadline.cancelled() => self.log_abandoned(),
        }
    }

    /// Logs what the cancelled deadline leaves behind and returns how many tokens are held.
    fn log_abandoned(&self) -> usize {
        let abandoned = self.tracker.len();
        let mut started = 0;
        for operation in [HandoffOperation::Seed, HandoffOperation::Uninstall] {
            let count = self.gauge(operation).load(Ordering::SeqCst);
            if count > 0 {
                tracing::warn!(
                    operation = operation_token(operation),
                    count,
                    "request_tasks_abandoned"
                );
                started += count;
            }
        }
        let unused = abandoned.saturating_sub(started);
        if unused > 0 {
            tracing::warn!(count = unused, "request_permits_abandoned");
        }
        abandoned
    }
}

/// How a failed request task ended, as logged: `panic` or `cancelled` (never the payload).
fn failure_cause(failure: &JoinError) -> &'static str {
    if failure.is_panic() {
        "panic"
    } else {
        "cancelled"
    }
}

/// The supervisor: starts the request task and awaits it. A panic or join error of the task ends
/// the request as internal failure (the one place a task failure is caught). The permit and the
/// gauge are held until the supervisor ends, recovery included; nothing can abort the
/// supervisor, so they are never freed while the request task runs.
async fn supervise(
    pipeline: Arc<Pipeline>,
    permit: RequestPermit,
    gauge: Gauge,
    operation: HandoffOperation,
    ctx: SecurityContext,
    input: RequestInput,
) -> OutcomeReport {
    let _held = (permit, gauge);
    let progress = Arc::new(Progress::default());
    let task = tokio::spawn(
        run_task(Arc::clone(&pipeline), Arc::clone(&progress), ctx, input)
            .instrument(tracing::Span::current()),
    );
    match task.await {
        Ok(report) => report,
        Err(failure) => {
            pipeline
                .recover_contained(&progress, operation, failure_cause(&failure))
                .await
        }
    }
}

/// Steps 1-8 of one request.
async fn run_task(
    pipeline: Arc<Pipeline>,
    progress: Arc<Progress>,
    ctx: SecurityContext,
    input: RequestInput,
) -> OutcomeReport {
    let gated = pipeline.pre_dispatch(&ctx, input, &progress).await;
    pipeline.finish(gated, &progress).await
}

/// A request whose request hand-off was accepted: the only requests that get a final-result
/// hand-off.
struct Accepted {
    started: Arc<StartupResult>,
    caller: CallerIdentity,
    audit_id: String,
}

/// The section report of one dispatched call. A rejection or a missing permission carries the
/// plugin's caller-facing detail and location, cut to the SDK bound again here (the constructors
/// already cut; this is defence in depth); an internal failure carries none.
fn section_report(
    key: &str,
    result: &Result<(), PluginCallError<SectionPluginError>>,
) -> SectionReport {
    let (outcome, detail, location) = match result {
        Ok(()) => (SectionOutcome::Succeeded, None, None),
        Err(PluginCallError::Plugin(SectionPluginError::Rejected {
            detail, location, ..
        })) => (
            SectionOutcome::Rejected,
            Some(truncate_detail(detail)),
            location.as_deref().map(truncate_detail),
        ),
        Err(PluginCallError::Plugin(SectionPluginError::NotPermitted { detail, .. })) => (
            SectionOutcome::NotPermitted,
            Some(truncate_detail(detail)),
            None,
        ),
        Err(other) => (other.section_outcome(), None, None),
    };
    SectionReport {
        key: key.to_owned(),
        outcome,
        detail,
        location,
    }
}

fn outcomes_of(sections: &[SectionReport]) -> Vec<SectionOutcome> {
    sections.iter().map(|s| s.outcome).collect()
}

/// The final-result sections: the outcomes of `sections`, each with the fingerprint
/// `fingerprint` gives for its key.
fn final_sections(
    sections: &[SectionReport],
    fingerprint: impl Fn(&str) -> Option<String>,
) -> Vec<FinalSection> {
    sections
        .iter()
        .map(|s| FinalSection {
            key: s.key.clone(),
            outcome: s.outcome,
            schema_fingerprint: fingerprint(&s.key),
        })
        .collect()
}

/// A request that ends as internal failure outside the precedence table.
fn internal_failure(
    application: Option<ApplicationRef>,
    sections: Vec<SectionReport>,
    registry_effect: RegistryEffect,
    audit_id: Option<String>,
) -> OutcomeReport {
    OutcomeReport {
        category: OutcomeCategory::InternalFailure,
        partial: is_partial(&outcomes_of(&sections)),
        application,
        sections,
        problems: Vec::new(),
        unavailable: Vec::new(),
        registry_effect,
        audit_id,
    }
}

/// The registry effect the supervisor reports for a task that failed (DESIGN §3.2): a result
/// that is known; a write that started as not confirmed; none yet as unchanged, and as
/// `not_updated` once a seed's dispatch had begun.
fn recovered_effect(
    registry: RegistryProgress,
    operation: HandoffOperation,
    dispatch_begun: bool,
) -> RegistryEffect {
    let seed = operation == HandoffOperation::Seed;
    match registry {
        RegistryProgress::Result(effect) => effect,
        RegistryProgress::WriteStarted if seed => RegistryEffect::NotUpdated,
        RegistryProgress::WriteStarted => RegistryEffect::DeleteFailed,
        RegistryProgress::NoneYet if seed && dispatch_begun => RegistryEffect::NotUpdated,
        RegistryProgress::NoneYet => RegistryEffect::Unchanged,
    }
}

impl Pipeline {
    /// Steps 6-8 on the result of steps 1-5: a decided request gets its final-result hand-off
    /// once its request hand-off was accepted; a proceed is dispatched, its registry write is
    /// made and its final result is handed off.
    pub(crate) async fn finish(&self, gated: Gated, progress: &Progress) -> OutcomeReport {
        let Gated {
            result:
                GateResult {
                    caller,
                    audit_id,
                    outcome,
                    ..
                },
            started,
            received_time,
        } = gated;
        let accepted = match (audit_id, started) {
            (Some(audit_id), Some(started)) => Some(Accepted {
                started,
                caller,
                audit_id,
            }),
            _ => None,
        };
        match (outcome, accepted) {
            (GateOutcome::Proceed(proceed), Some(accepted)) => {
                self.dispatch(&accepted, proceed, received_time, progress)
                    .await
            }
            (GateOutcome::Decided(report), Some(accepted)) => {
                let validation = (report.category == OutcomeCategory::InvalidManifest).then(|| {
                    ValidationResult {
                        valid: false,
                        problems: report.problems.clone(),
                    }
                });
                self.final_handoff(&accepted, &report, validation, Vec::new(), progress)
                    .await;
                report
            }
            // Never dispatched and never handed off: the request hand-off was not accepted.
            (GateOutcome::Decided(report), None) => report,
            // A proceed always carries an accepted request hand-off, so this is not reached;
            // the request still ends without dispatching anything.
            (GateOutcome::Proceed(_), None) => {
                internal_failure(None, Vec::new(), RegistryEffect::Unchanged, None)
            }
        }
    }

    /// Steps 6-8 of an accepted, valid request: a seed applies every section in priority order
    /// and then upserts the entry; an uninstall deletes the entry and then removes every section.
    async fn dispatch(
        &self,
        accepted: &Accepted,
        proceed: Proceed,
        received_time: SystemTime,
        progress: &Progress,
    ) -> OutcomeReport {
        let Proceed {
            envelope,
            manifest,
            schemas: _,
            fingerprints,
        } = proceed;
        let is_seed = manifest.is_some();
        let (sections, registry_effect) = match manifest {
            Some(manifest) => {
                self.seed(accepted, &envelope, manifest, received_time, progress)
                    .await
            }
            None => self.uninstall(accepted, &envelope, progress).await,
        };
        let outcomes = outcomes_of(&sections);
        let registry_failed = matches!(
            registry_effect,
            RegistryEffect::NotUpdated | RegistryEffect::DeleteFailed
        );
        let final_sections = final_sections(&sections, |key| fingerprint_for(&fingerprints, key));
        let report = OutcomeReport {
            category: after_dispatch_category(&outcomes, registry_failed),
            application: Some(ApplicationRef {
                id: Some(envelope.id),
                version: Some(envelope.version),
            }),
            partial: is_partial(&outcomes),
            sections,
            problems: Vec::new(),
            unavailable: Vec::new(),
            registry_effect,
            audit_id: Some(accepted.audit_id.clone()),
        };
        let validation = is_seed.then(|| ValidationResult {
            valid: true,
            problems: Vec::new(),
        });
        self.final_handoff(accepted, &report, validation, final_sections, progress)
            .await;
        report
    }

    /// Seed: `apply` on every section plugin in priority order (a failure does not stop the
    /// rest), then the upsert of the entry with the last result derived from the outcomes.
    async fn seed(
        &self,
        accepted: &Accepted,
        envelope: &ApplicationEnvelope,
        mut manifest: Value,
        received_time: SystemTime,
        progress: &Progress,
    ) -> (Vec<SectionReport>, RegistryEffect) {
        let mut sections = Vec::new();
        for entry in accepted.started.section_plugins() {
            progress.dispatch_started();
            // The validator required exactly the key set, so the value is there; it is moved
            // out of the manifest, not copied.
            let value = manifest
                .get_mut("sections")
                .and_then(|all| all.get_mut(entry.key.as_str()))
                .map_or(Value::Null, Value::take);
            let result = entry.plugin.apply(&accepted.caller, envelope, value).await;
            let report = section_report(&entry.key, &result);
            progress.section(report.clone());
            sections.push(report);
        }
        let request_id = accepted.caller.request_id;
        let effect = if let Ok(entry) = RegistryEntry::new(
            envelope.id.as_str(),
            envelope.version.as_str(),
            derive_last_result(&outcomes_of(&sections)),
            received_time,
            accepted.audit_id.as_str(),
        ) {
            let written = self
                .registry_write("upsert", request_id, progress, self.registry.upsert(&entry))
                .await;
            if written {
                RegistryEffect::Updated
            } else {
                RegistryEffect::NotUpdated
            }
        } else {
            // An id, version or audit id that cannot be stored: nothing was written.
            tracing::warn!(
                op = "upsert",
                reason = "not_storable",
                request_id = %request_id,
                "registry_write_failed"
            );
            RegistryEffect::NotUpdated
        };
        progress.registry_result(effect);
        (sections, effect)
    }

    /// Uninstall: the delete first, so the entry is gone whatever the section plugins return,
    /// then `remove` on every section plugin in priority order.
    async fn uninstall(
        &self,
        accepted: &Accepted,
        envelope: &ApplicationEnvelope,
        progress: &Progress,
    ) -> (Vec<SectionReport>, RegistryEffect) {
        let deleted = self
            .registry_write(
                "delete",
                accepted.caller.request_id,
                progress,
                self.registry.delete(&envelope.id),
            )
            .await;
        let effect = if deleted {
            RegistryEffect::Deleted
        } else {
            RegistryEffect::DeleteFailed
        };
        progress.registry_result(effect);
        let mut sections = Vec::new();
        for entry in accepted.started.section_plugins() {
            progress.dispatch_started();
            let result = entry.plugin.remove(&accepted.caller, envelope).await;
            let report = section_report(&entry.key, &result);
            progress.section(report.clone());
            sections.push(report);
        }
        (sections, effect)
    }

    /// One registry write under `registry_operation_timeout`. The progress record says
    /// "write started" first. `true` when the store confirmed it; a failure or a timeout is
    /// logged as `registry_write_failed` and, after a timeout, may still have committed.
    async fn registry_write(
        &self,
        op: &'static str,
        request_id: Uuid,
        progress: &Progress,
        write: impl Future<Output = Result<(), RegistryError>>,
    ) -> bool {
        progress.registry_write_started();
        let failure =
            match tokio::time::timeout(self.settings.registry_operation_timeout, write).await {
                Ok(Ok(())) => return true,
                Ok(Err(e)) => reason_token(e.reason()),
                Err(_elapsed) => reason_token(UnavailableReason::TimedOut),
            };
        tracing::warn!(op, reason = failure, request_id = %request_id, "registry_write_failed");
        false
    }

    /// Step 8: the one attempt of the final-result hand-off, after everything else the request
    /// does. A failure is logged by the client and changes nothing.
    async fn final_handoff(
        &self,
        accepted: &Accepted,
        report: &OutcomeReport,
        validation: Option<ValidationResult>,
        sections: Vec<FinalSection>,
        progress: &Progress,
    ) {
        let content = FinalResultContent {
            audit_id: accepted.audit_id.clone(),
            request_id: accepted.caller.request_id,
            validation,
            sections,
            category: report.category,
            registry_effect: report.registry_effect,
            completed_at: rfc3339(self.clock.now()),
        };
        progress.final_attempted();
        // Not accepted is logged by the client; the report stays as it is (ADR 0001).
        let _not_accepted = AuditHandoffClient::new(Arc::clone(accepted.started.audit()))
            .final_handoff(&accepted.caller, &accepted.audit_id, &content)
            .await;
    }

    /// The supervisor's recovery after the request task panicked or failed to join, with its own
    /// failure contained: a panic inside [`Self::recover`] ends the request with what the
    /// progress record holds (internal failure, no further hand-off) instead of ending the
    /// supervisor, so the permit is released by the normal end of the supervisor.
    async fn recover_contained(
        &self,
        progress: &Progress,
        operation: HandoffOperation,
        cause: &'static str,
    ) -> OutcomeReport {
        let record = progress.take();
        let request_id = record.begun.as_ref().map(|b| b.caller.request_id);
        tracing::warn!(
            operation = operation_token(operation),
            cause,
            request_id = request_id.map(tracing::field::display),
            "request_task_failed"
        );
        let effect = recovered_effect(record.registry, operation, record.dispatch_started);
        let recorded = record.sections.clone();
        let accepted_before = record.audit_id.clone();
        match AssertUnwindSafe(self.recover(record, operation, progress))
            .catch_unwind()
            .await
        {
            Ok(report) => report,
            Err(_payload) => {
                // Never the payload: it may quote request content.
                tracing::error!(
                    operation = operation_token(operation),
                    request_id = request_id.map(tracing::field::display),
                    "request_recovery_failed"
                );
                let audit_id = progress.take().audit_id.or(accepted_before);
                internal_failure(None, recorded, effect, audit_id)
            }
        }
    }

    /// Ends the request as internal failure from the progress record, which never parses the
    /// input again. The request hand-off is attempted only if the task had not attempted it (and
    /// the receive step had produced its content), the final-result hand-off only if the request
    /// hand-off was accepted and the task had not attempted the final one.
    async fn recover(
        &self,
        record: Record,
        operation: HandoffOperation,
        progress: &Progress,
    ) -> OutcomeReport {
        let Record {
            begun,
            retained,
            decided,
            handoff_attempted,
            audit_id,
            sections: recorded,
            registry,
            final_attempted,
            dispatch_started,
            fingerprints,
        } = record;
        let started = self.startup.result();
        let application = retained.as_ref().and_then(|r| r.application.clone());
        let mut handed_off = None;
        if let (Some(begun), Some(retained), Some(started), false) =
            (&begun, retained, started.as_ref(), handoff_attempted)
        {
            let content = RequestHandoffContent {
                operation: retained.operation,
                request_id: begun.caller.request_id,
                received_at: rfc3339(begun.received_time),
                application: retained.application,
                manifest: retained.manifest,
                decided_category: decided,
                id_length_bytes: retained.id_length_bytes,
                id_sha256: retained.id_sha256,
            };
            handed_off = AuditHandoffClient::new(Arc::clone(started.audit()))
                .request_handoff(&begun.caller, &content)
                .await
                .ok();
            if let Some(id) = &handed_off {
                // Kept for the containment: a later panic still reports the accepted id.
                progress.handoff_accepted(id);
            }
        }
        let audit_id = audit_id.or(handed_off);

        let mut sections = recorded;
        for entry in started.iter().flat_map(|s| s.section_plugins()) {
            if sections.iter().all(|s| s.key != entry.key) {
                sections.push(SectionReport {
                    key: entry.key.clone(),
                    outcome: SectionOutcome::NotDispatched,
                    detail: None,
                    location: None,
                });
            }
        }
        let report = internal_failure(
            application,
            sections,
            recovered_effect(registry, operation, dispatch_started),
            audit_id.clone(),
        );
        if let (Some(audit_id), Some(started), Some(begun), false) =
            (audit_id, started, begun, final_attempted)
        {
            let accepted = Accepted {
                started,
                caller: begun.caller,
                audit_id,
            };
            let sections =
                final_sections(&report.sections, |key| fingerprint_for(&fingerprints, key));
            self.final_handoff(&accepted, &report, None, sections, progress)
                .await;
        }
        report
    }
}

/// The fingerprint of the schema served for `key` in this request, when step 5 recorded one.
fn fingerprint_for(fingerprints: &[(String, String)], key: &str) -> Option<String> {
    fingerprints
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, fingerprint)| fingerprint.clone())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "request_dispatch_tests.rs"]
mod request_dispatch_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "request_uninstall_tests.rs"]
mod request_uninstall_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "request_final_handoff_tests.rs"]
mod request_final_handoff_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "request_task_tests.rs"]
mod request_task_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "request_panic_tests.rs"]
mod request_panic_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "request_logs_tests.rs"]
mod request_logs_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "request_review_tests.rs"]
mod request_review_tests;
