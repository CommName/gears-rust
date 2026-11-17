//! Request orchestrator (`cpt-cf-application-manager-component-request-orchestrator`):
//! the synchronous checks, acceptance, the detached request task, dispatch
//! in priority order, and supervisor completion (DESIGN 3.6).

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use application_manager_sdk::{
    CallerIdentity, Dependency, Envelope, OperationAction, OutcomeReport, ReportApplication,
    ReportProblem, RequestCategory, SectionOutcome, SectionOutcomeEntry, SectionPluginError,
    UnavailableItem, UnavailableReasonCode,
};
use async_trait::async_trait;
use serde_json::{Map, Value};
use time::OffsetDateTime;
use tokio::sync::{OwnedSemaphorePermit, oneshot};
use tokio::time::Instant;
use toolkit_macros::domain_model;
use toolkit_security::PlatformSecurityContext;
use tracing::Instrument;
use uuid::Uuid;

use crate::constants::{LOG_TEXT_MAX_BYTES, PLUGIN_DETAIL_MAX_BYTES, cut};
use crate::domain::actions::CallerAction;
use crate::domain::allowed_callers::{AllowedCallers, identity_fields};
use crate::domain::error::DomainError;
use crate::domain::history::{
    self, ApplicationRef, HistoryEvent, HistoryKind, RefusedResult, SectionKeys,
};
use crate::domain::operation::{
    NewOperation, complete_open_sections, completed_category, run_window,
};
use crate::domain::outcome::{Gate, SyncFacts, category_of, sync_refusal};
use crate::domain::plugin_set::{PluginSet, SectionPlugin, StartupState};
use crate::domain::ports::{CallFailure, CallResult, OperationRepository, StorageError};
use crate::domain::router::{DispatchObserver, SectionPluginRouter};
use crate::domain::schema_validator::{Problem, SchemaValidator, validate_sections};
use crate::domain::signals::{NoSignals, Signals, action_label, category_label, outcome_label};

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

/// An accepted request.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Submitted {
    /// The operation id (the request id).
    pub operation_id: Uuid,
}

/// The settings the orchestrator needs.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrchestratorSettings {
    /// Time limit of every plugin call; unit of the run window.
    pub plugin_call_timeout: Duration,
    /// Bound of every storage access.
    pub storage_call_timeout: Duration,
    /// Added to the run window.
    pub run_window_margin: Duration,
    /// Maximum bytes of the application id and version.
    pub max_envelope_text_length: u32,
}

/// A request after parsing: what every step and history event needs.
#[derive(Debug, Clone)]
struct Parsed {
    caller_action: CallerAction,
    action: OperationAction,
    /// The id the open-operation step uses; empty when there is none.
    application_id: String,
    version: Option<String>,
    /// A seed's `sections`, as sent.
    sections: Option<Value>,
    problems: Vec<Problem>,
    section_keys: Option<SectionKeys>,
    application: ApplicationRef,
}

/// Where the request task is, for its supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Progress {
    /// Still in the synchronous phase.
    Synchronous,
    /// Refused, with its answer sent.
    Refused,
    /// Accepted, with its answer sent.
    Accepted,
}

/// Runs each seed and uninstall as a supervised request task.
#[domain_model]
pub struct RequestOrchestrator {
    allowed: Arc<AllowedCallers>,
    repo: Arc<dyn OperationRepository>,
    router: Arc<SectionPluginRouter>,
    validator: Arc<SchemaValidator>,
    startup: Arc<StartupState>,
    settings: OrchestratorSettings,
    signals: Arc<dyn Signals>,
}

impl fmt::Debug for RequestOrchestrator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestOrchestrator")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

impl RequestOrchestrator {
    /// An orchestrator over the ports and the startup result.
    #[must_use]
    pub fn new(
        allowed: Arc<AllowedCallers>,
        repo: Arc<dyn OperationRepository>,
        router: Arc<SectionPluginRouter>,
        validator: Arc<SchemaValidator>,
        startup: Arc<StartupState>,
        settings: OrchestratorSettings,
    ) -> Self {
        Self {
            allowed,
            repo,
            router,
            validator,
            startup,
            settings,
            signals: Arc::new(NoSignals),
        }
    }

    /// The same orchestrator, raising its signals on `signals`.
    #[must_use]
    pub fn with_signals(mut self, signals: Arc<dyn Signals>) -> Self {
        self.signals = signals;
        self
    }

    /// Seed `manifest`. `admission` is the gate's answer: a refusal records
    /// its refused event and is returned. Otherwise a supervisor task owns
    /// the permit until the operation is final and runs the request task;
    /// this future waits only for the synchronous answer, and the tasks keep
    /// running when it is dropped. A request task that ends before it
    /// answers is a [`DomainError::Defect`] with a refused event of category
    /// internal failure; one that ends after acceptance gets the supervisor
    /// completion.
    ///
    /// # Errors
    /// The refusal of ranks 1 to 4, [`DomainError::StorageUnavailable`] at
    /// acceptance, a conflict naming the winner of a lost race, or
    /// [`DomainError::Defect`].
    pub async fn seed(
        self: Arc<Self>,
        ctx: PlatformSecurityContext,
        manifest: Value,
        receipt: Receipt,
        admission: Result<OwnedSemaphorePermit, DomainError>,
    ) -> Result<Submitted, DomainError> {
        let parsed = self.parse_seed(&manifest);
        self.submit(ctx, parsed, receipt, admission).await
    }

    /// Uninstall `application_id`, as [`Self::seed`].
    ///
    /// # Errors
    /// As [`Self::seed`].
    pub async fn uninstall(
        self: Arc<Self>,
        ctx: PlatformSecurityContext,
        application_id: String,
        receipt: Receipt,
        admission: Result<OwnedSemaphorePermit, DomainError>,
    ) -> Result<Submitted, DomainError> {
        let parsed = self.parse_uninstall(application_id);
        self.submit(ctx, parsed, receipt, admission).await
    }

    async fn submit(
        self: Arc<Self>,
        ctx: PlatformSecurityContext,
        parsed: Parsed,
        receipt: Receipt,
        admission: Result<OwnedSemaphorePermit, DomainError>,
    ) -> Result<Submitted, DomainError> {
        let permit = match admission {
            Ok(permit) => permit,
            Err(refusal) => {
                self.record_refusal(&ctx, &parsed, &receipt, &refusal).await;
                return Err(refusal);
            }
        };
        let (answer, answered) = oneshot::channel();
        let progress = Arc::new(Mutex::new(Progress::Synchronous));
        let task = tokio::spawn(Arc::clone(&self).run(
            ctx.clone(),
            parsed.clone(),
            receipt,
            answer,
            Arc::clone(&progress),
        ));
        self.signals.request_tasks(1);
        tokio::spawn(async move {
            let _permit = permit;
            let _in_flight = InFlight(Arc::clone(&self.signals));
            let Err(failure) = task.await else {
                return;
            };
            let stage = *progress.lock().unwrap_or_else(PoisonError::into_inner);
            tracing::error!(
                panicked = failure.is_panic(),
                stage = ?stage,
                "application-manager: request task failed"
            );
            match stage {
                Progress::Synchronous => {
                    self.record_refusal(&ctx, &parsed, &receipt, &DomainError::Defect)
                        .await;
                }
                Progress::Accepted => {
                    self.supervise(receipt.request_id, parsed.action).await;
                }
                Progress::Refused => {}
            }
        });
        answered.await.unwrap_or(Err(DomainError::Defect))
    }

    /// The request task: the synchronous phase, then, once accepted and
    /// answered, the operation.
    async fn run(
        self: Arc<Self>,
        ctx: PlatformSecurityContext,
        parsed: Parsed,
        receipt: Receipt,
        answer: oneshot::Sender<Result<Submitted, DomainError>>,
        progress: Arc<Mutex<Progress>>,
    ) {
        let settle = |stage| *progress.lock().unwrap_or_else(PoisonError::into_inner) = stage;
        let request_span = tracing::info_span!(
            "application_manager.request",
            request_id = %receipt.request_id,
            action = parsed.caller_action.as_str()
        );
        match self
            .synchronous(&ctx, &parsed, &receipt)
            .instrument(request_span.clone())
            .await
        {
            Err(refusal) => {
                settle(Progress::Refused);
                answer.send(Err(refusal)).ok();
            }
            Ok(set) => {
                settle(Progress::Accepted);
                answer
                    .send(Ok(Submitted {
                        operation_id: receipt.request_id,
                    }))
                    .ok();
                let operation_span = tracing::info_span!(
                    "application_manager.operation",
                    operation_id = %receipt.request_id,
                    action = parsed.caller_action.as_str()
                );
                operation_span.follows_from(&request_span);
                self.operate(&ctx, &parsed, receipt.request_id, &set)
                    .instrument(operation_span)
                    .await;
            }
        }
    }

    /// The synchronous phase (DESIGN 3.6): the checks, ranks 1 to 4, and the
    /// acceptance transaction. Every refusal records its refused event.
    async fn synchronous(
        &self,
        ctx: &PlatformSecurityContext,
        parsed: &Parsed,
        receipt: &Receipt,
    ) -> Result<Arc<PluginSet>, DomainError> {
        let allowed = self.allowed.allows(ctx.identity(), parsed.caller_action);
        if !allowed {
            let (kind, domain, name) = identity_fields(ctx.identity());
            tracing::info!(
                action = parsed.caller_action.as_str(),
                identity_kind = kind,
                identity_domain = domain,
                identity_name = name,
                "application-manager: caller not allowed"
            );
        }
        let open = if allowed {
            Some(self.repo.open_operation(&parsed.application_id).await)
        } else {
            None
        };
        let facts = SyncFacts {
            action: parsed.caller_action,
            gate: Gate::Open,
            allowed,
            open,
            envelope_problems: parsed.problems.clone(),
        };
        if let Some(refusal) = sync_refusal(&facts) {
            self.record_refusal(ctx, parsed, receipt, &refusal).await;
            return Err(refusal);
        }
        let Some(set) = self.startup.plugin_set() else {
            self.record_refusal(ctx, parsed, receipt, &DomainError::NotReady)
                .await;
            return Err(DomainError::NotReady);
        };
        self.accept(ctx, parsed, receipt, &set).await?;
        Ok(set)
    }

    /// The acceptance transaction, and the lost index race.
    async fn accept(
        &self,
        ctx: &PlatformSecurityContext,
        parsed: &Parsed,
        receipt: &Receipt,
        set: &PluginSet,
    ) -> Result<(), DomainError> {
        let Ok(window) = run_window(
            set.plugins().len(),
            self.settings.plugin_call_timeout,
            self.settings.storage_call_timeout,
            self.settings.run_window_margin,
        ) else {
            self.record_refusal(ctx, parsed, receipt, &DomainError::Defect)
                .await;
            return Err(DomainError::Defect);
        };
        let new = NewOperation {
            operation_id: receipt.request_id,
            application_id: parsed.application_id.clone(),
            action: parsed.action,
            run_window: window,
            finished_event_id: receipt.finished_event_id,
            report: initial_report(receipt.request_id, parsed),
        };
        let accepted = HistoryEvent {
            event_id: receipt.accepted_event_id,
            kind: HistoryKind::Accepted,
            correlation_id: receipt.request_id,
            action: parsed.action,
            written_at: receipt.received_at,
            content: history::accepted(
                &parsed.application,
                parsed.section_keys.as_ref(),
                &CallerIdentity::from_platform_context(ctx),
                receipt.received_at,
            ),
        };
        let action = action_label(parsed.action);
        match self.repo.accept(new, accepted).await {
            Ok(_) => {
                self.signals.request(action, "accepted");
                self.signals.history_event(true);
                Ok(())
            }
            Err(StorageError::IndexViolated) => {
                let event = Self::refused_event_builder(ctx, parsed, receipt);
                let lost = self
                    .repo
                    .refuse_after_lost_race(&parsed.application_id, event)
                    .await;
                if let Ok(winner) = lost {
                    self.signals.request(action, "conflict");
                    self.signals.history_event(true);
                    Err(DomainError::Conflict {
                        operation_id: winner,
                    })
                } else {
                    self.signals.request(action, "temporarily_unavailable");
                    self.signals.history_event(false);
                    self.signals.dependency_unavailable("storage");
                    Err(DomainError::StorageUnavailable)
                }
            }
            Err(_) => {
                let refusal = DomainError::StorageUnavailable;
                self.record_refusal(ctx, parsed, receipt, &refusal).await;
                Err(refusal)
            }
        }
    }

    /// The operation after acceptance (DESIGN 3.6): validation (seed), mark
    /// running, dispatch within the run deadline, and the final transaction.
    async fn operate(
        &self,
        ctx: &PlatformSecurityContext,
        parsed: &Parsed,
        operation_id: Uuid,
        set: &PluginSet,
    ) {
        let started = Instant::now();
        let mut report = initial_report(operation_id, parsed);
        if parsed.action == OperationAction::Seed && !self.validate(parsed, set, &mut report).await
        {
            return;
        }
        let seed = parsed.action == OperationAction::Seed;
        let entries: Vec<(String, Option<String>)> = set
            .plugins()
            .iter()
            .map(|p| (p.key.clone(), seed.then(|| p.section_type.clone())))
            .collect();
        let left = match self.repo.mark_running(operation_id, &entries).await {
            Ok(left) => left,
            Err(error) => {
                // An expired or unreadable record is left to the expiry
                // completion.
                if error == StorageError::Unavailable {
                    self.signals.dependency_unavailable("storage");
                }
                tracing::warn!(%error, "application-manager: mark running not written");
                return;
            }
        };
        report.sections = entries
            .into_iter()
            .map(|(key, section_type)| SectionOutcomeEntry {
                key,
                section_type,
                outcome: None,
                detail: None,
                location: None,
            })
            .collect();
        self.dispatch(ctx, parsed, operation_id, set, left, &mut report)
            .await;
        complete_open_sections(&mut report, false);
        for entry in &report.sections {
            if let Some(outcome) = entry.outcome {
                self.signals
                    .section_call(&entry.key, outcome_label(outcome));
            }
        }
        let category = completed_category(&report.sections);
        self.finish(operation_id, report, category).await;
        self.signals
            .operation_duration(action_label(parsed.action), started.elapsed());
    }

    /// Validate a seed's sections against the compiled schemas. With
    /// problems, the operation completes from accepted as invalid manifest
    /// (rank 5), with no application record. Returns whether to go on.
    async fn validate(&self, parsed: &Parsed, set: &PluginSet, report: &mut OutcomeReport) -> bool {
        let sections = parsed.sections.clone().unwrap_or(Value::Null);
        let compiled: Arc<[_]> = set.sections().into();
        let Ok(problems) = validate_sections(compiled, sections).await else {
            tracing::error!("application-manager: section validation aborted");
            self.supervise(report.operation_id, report.action).await;
            return false;
        };
        if problems.is_empty() {
            return true;
        }
        report.problems = problems.iter().map(report_problem).collect();
        self.finish(
            report.operation_id,
            report.clone(),
            RequestCategory::InvalidManifest,
        )
        .await;
        false
    }

    /// Call every plugin in priority order while each call still fits before
    /// the local deadline: the time left at mark running, less one
    /// `storage_call_timeout` for the final transaction.
    async fn dispatch(
        &self,
        ctx: &PlatformSecurityContext,
        parsed: &Parsed,
        operation_id: Uuid,
        set: &PluginSet,
        left: Duration,
        report: &mut OutcomeReport,
    ) {
        let mut observer = Dispatch {
            repo: Arc::clone(&self.repo),
            operation_id,
            report,
            deadline: Instant::now() + left.saturating_sub(self.settings.storage_call_timeout),
            settings: self.settings,
        };
        let caller = CallerIdentity::from_platform_context(ctx);
        if parsed.action == OperationAction::Uninstall {
            self.router
                .remove_all(
                    set,
                    &caller,
                    operation_id,
                    &parsed.application_id,
                    &mut observer,
                )
                .await;
            return;
        }
        let envelope = Envelope {
            id: parsed.application_id.clone(),
            version: parsed.version.clone().unwrap_or_default(),
        };
        let sections = parsed
            .sections
            .as_ref()
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        self.router
            .apply_all(
                set,
                &caller,
                operation_id,
                &envelope,
                &sections,
                &mut observer,
            )
            .await;
    }

    async fn finish(&self, operation_id: Uuid, report: OutcomeReport, category: RequestCategory) {
        match self.repo.finish(operation_id, &report, category).await {
            Ok(()) => self.completed(report.action, category, "run"),
            Err(error) => {
                // A final record, or one the expiry completion will finish.
                self.not_written(error);
                tracing::warn!(%error, "application-manager: final transaction not written");
            }
        }
    }

    async fn supervise(&self, operation_id: Uuid, action: OperationAction) {
        match self.repo.complete_by_supervisor(operation_id).await {
            Ok(()) => self.completed(action, RequestCategory::InternalFailure, "supervisor"),
            Err(error) => {
                self.not_written(error);
                tracing::warn!(%error, "application-manager: supervisor completion not written");
            }
        }
    }

    /// A completion and its finished event were written.
    fn completed(&self, action: OperationAction, category: RequestCategory, reason: &'static str) {
        self.signals.history_event(true);
        self.signals
            .operation_completed(action_label(action), category_label(category), reason);
    }

    /// A completion was not written: storage could not answer, or the record
    /// is already final (its finished event is someone else's).
    fn not_written(&self, error: StorageError) {
        if error == StorageError::Unavailable {
            self.signals.history_event(false);
            self.signals.dependency_unavailable("storage");
        }
    }

    /// A seed's manifest, parsed into what every step needs.
    fn parse_seed(&self, manifest: &Value) -> Parsed {
        let problems = self.validator.check_envelope(manifest);
        let text = |field: &str| {
            manifest["application"][field]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_default()
        };
        let application_id = text("id");
        let version = text("version");
        let sections = Some(manifest["sections"].clone());
        let section_keys = sections
            .as_ref()
            .and_then(Value::as_object)
            .map(|members| self.section_keys(members));
        let application = ApplicationRef::bounded(&application_id, Some(&version), self.max_text());
        Parsed {
            caller_action: CallerAction::Seed,
            action: OperationAction::Seed,
            application_id,
            version: Some(version),
            sections,
            problems,
            section_keys,
            application,
        }
    }

    /// An uninstall's application id, parsed into what every step needs.
    fn parse_uninstall(&self, id: String) -> Parsed {
        let problems = self.validator.check_application_id(&id);
        let application = ApplicationRef::bounded(&id, None, self.max_text());
        Parsed {
            caller_action: CallerAction::Uninstall,
            action: OperationAction::Uninstall,
            application_id: id,
            version: None,
            sections: None,
            problems,
            section_keys: None,
            application,
        }
    }

    fn max_text(&self) -> usize {
        usize::try_from(self.settings.max_envelope_text_length).unwrap_or(usize::MAX)
    }

    /// The keys of the key set a manifest carries, and a count of the others.
    fn section_keys(&self, members: &Map<String, Value>) -> SectionKeys {
        let known: Vec<String> = self
            .startup
            .plugin_set()
            .map(|set| set.keys())
            .unwrap_or_default();
        let keys: Vec<String> = known
            .iter()
            .filter(|key| members.contains_key(*key))
            .cloned()
            .collect();
        let other_keys = members.len() - keys.len();
        SectionKeys { keys, other_keys }
    }

    /// Record the refused event of `refusal`. Never fails: a missing event is
    /// logged, and the refusal keeps its category.
    async fn record_refusal(
        &self,
        ctx: &PlatformSecurityContext,
        parsed: &Parsed,
        receipt: &Receipt,
        refusal: &DomainError,
    ) {
        let category = category_of(refusal);
        self.signals
            .request(action_label(parsed.action), category_label(category));
        match refusal {
            DomainError::NotPermitted { action } => self.signals.caller_refused(action.as_str()),
            DomainError::StorageUnavailable => self.signals.dependency_unavailable("storage"),
            _ => {}
        }
        let event = refused_event(ctx, parsed, receipt, refusal);
        match self.repo.record_refused(event).await {
            Ok(()) => self.signals.history_event(true),
            Err(error) => {
                self.signals.history_event(false);
                tracing::warn!(
                    %error,
                    category = ?category,
                    "application-manager: refused event not recorded"
                );
            }
        }
    }

    /// The refused event of a lost index race, built once the winner is known.
    fn refused_event_builder(
        ctx: &PlatformSecurityContext,
        parsed: &Parsed,
        receipt: &Receipt,
    ) -> Box<dyn Fn(Uuid) -> HistoryEvent + Send + Sync> {
        let (ctx, parsed, receipt) = (ctx.clone(), parsed.clone(), *receipt);
        Box::new(move |winner| {
            refused_event(
                &ctx,
                &parsed,
                &receipt,
                &DomainError::Conflict {
                    operation_id: winner,
                },
            )
        })
    }
}

fn report_problem(problem: &Problem) -> ReportProblem {
    ReportProblem {
        location: problem.location.clone(),
        rule: problem.rule.clone(),
        message: problem.message.clone(),
    }
}

/// The report an operation starts with: no sections until mark running.
fn initial_report(operation_id: Uuid, parsed: &Parsed) -> OutcomeReport {
    OutcomeReport {
        operation_id,
        action: parsed.action,
        category: None,
        application: ReportApplication {
            id: parsed.application_id.clone(),
            version: parsed.version.clone(),
        },
        sections: Vec::new(),
        problems: Vec::new(),
        unavailable: Vec::new(),
    }
}

/// The refused event of `refusal` (DESIGN 3.1 "History event content").
fn refused_event(
    ctx: &PlatformSecurityContext,
    parsed: &Parsed,
    receipt: &Receipt,
    refusal: &DomainError,
) -> HistoryEvent {
    let unavailable = |dependency, reason| vec![UnavailableItem { dependency, reason }];
    let (problems, busy_operation_id, unavailable) = match refusal {
        DomainError::InvalidManifest { problems } => {
            (problems.iter().map(report_problem).collect(), None, vec![])
        }
        DomainError::Conflict { operation_id } => (vec![], Some(*operation_id), vec![]),
        DomainError::NotReady => (
            vec![],
            None,
            unavailable(
                Dependency::ApplicationManager,
                UnavailableReasonCode::NotReady,
            ),
        ),
        DomainError::AtCapacity => (
            vec![],
            None,
            unavailable(
                Dependency::ApplicationManager,
                UnavailableReasonCode::AtCapacity,
            ),
        ),
        DomainError::StorageUnavailable => (
            vec![],
            None,
            unavailable(
                Dependency::OperationStorage,
                UnavailableReasonCode::Unreachable,
            ),
        ),
        _ => (vec![], None, vec![]),
    };
    let result = RefusedResult {
        category: category_of(refusal),
        problems,
        busy_operation_id,
        unavailable,
    };
    HistoryEvent {
        event_id: receipt.refused_event_id,
        kind: HistoryKind::Refused,
        correlation_id: receipt.request_id,
        action: parsed.action,
        written_at: receipt.received_at,
        content: history::refused(
            &parsed.application,
            parsed.section_keys.as_ref(),
            &CallerIdentity::from_platform_context(ctx),
            receipt.received_at,
            &result,
        ),
    }
}

/// The section outcome of a plugin's answer, with the detail and location
/// kept in the report.
fn outcome_of(key: &str, answer: &CallResult) -> (SectionOutcome, Option<String>, Option<String>) {
    let detail = |text: &str| Some(cut(text, PLUGIN_DETAIL_MAX_BYTES).to_owned());
    match answer {
        Ok(Ok(())) => (SectionOutcome::Succeeded, None, None),
        Ok(Err(SectionPluginError::Rejected {
            detail: text,
            location,
        })) => (SectionOutcome::Rejected, detail(text), location.clone()),
        Ok(Err(SectionPluginError::NotPermitted { detail: text })) => {
            (SectionOutcome::NotPermitted, detail(text), None)
        }
        Ok(Err(SectionPluginError::Internal { message })) => {
            tracing::warn!(
                key,
                message = cut(message, LOG_TEXT_MAX_BYTES),
                "application-manager: section plugin failed"
            );
            (SectionOutcome::InternalFailure, None, None)
        }
        Ok(Err(_)) => (SectionOutcome::InternalFailure, None, None),
        Err(CallFailure::Panicked(message)) => {
            tracing::warn!(
                key,
                message = cut(message, LOG_TEXT_MAX_BYTES),
                "application-manager: section plugin panicked"
            );
            (SectionOutcome::InternalFailure, None, None)
        }
        Err(CallFailure::TimedOut) => (SectionOutcome::TimedOut, None, None),
        Err(CallFailure::NotSent) => (SectionOutcome::NotSent, None, None),
    }
}

/// Counts one request task down when its supervisor ends, however it ends.
struct InFlight(Arc<dyn Signals>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.request_tasks(-1);
    }
}

/// The orchestrator's side of one dispatch: the fit check before each call
/// and the section-outcome write after it (DESIGN 3.1 "Run deadline").
struct Dispatch<'a> {
    repo: Arc<dyn OperationRepository>,
    operation_id: Uuid,
    report: &'a mut OutcomeReport,
    deadline: Instant,
    settings: OrchestratorSettings,
}

#[async_trait]
impl DispatchObserver for Dispatch<'_> {
    /// A call starts only while `plugin_call_timeout` + 2 x
    /// `storage_call_timeout` still fit before the local deadline.
    async fn before(&mut self, _plugin: &SectionPlugin) -> bool {
        let needed = self.settings.plugin_call_timeout
            + self.settings.storage_call_timeout
            + self.settings.storage_call_timeout;
        Instant::now() + needed <= self.deadline
    }

    /// Keep the outcome in the report and write it; a failed write ends the
    /// dispatch.
    async fn after(&mut self, plugin: &SectionPlugin, answer: &CallResult) -> bool {
        let (outcome, detail, location) = outcome_of(&plugin.key, answer);
        if let Some(entry) = self
            .report
            .sections
            .iter_mut()
            .find(|entry| entry.key == plugin.key)
        {
            entry.outcome = Some(outcome);
            entry.detail.clone_from(&detail);
            entry.location.clone_from(&location);
        }
        match self
            .repo
            .record_section_outcome(self.operation_id, &plugin.key, outcome, detail, location)
            .await
        {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(
                    %error,
                    key = plugin.key,
                    "application-manager: section outcome not written"
                );
                false
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "orchestrator_tests.rs"]
mod tests;
