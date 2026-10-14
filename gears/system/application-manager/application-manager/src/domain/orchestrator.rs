//! Request orchestrator (DESIGN §3.2 "Request Orchestrator", §3.6 "Pre-Dispatch Gate").
//!
//! Steps: 1 receive (parse, envelope limits), 2 the permission decision (one PDP call, the
//! policy port owns its deadline), 3 the registry step (permitted callers only), 4 the request
//! hand-off to the audit plugin, 5 the live schema fetch and the whole-manifest validation.
//! Precedence: access control, then registry, then audit, then schemas, then validation. The
//! hand-off always comes before any dispatch. Dispatch, the registry write and the
//! final-result hand-off (steps 6-8), the request task and its supervisor are in
//! [`crate::domain::request`]; this module holds the shared [`Pipeline`] of steps 1-5 and the
//! [`Orchestrator`] that owns the pipeline and the request tasks.
//!
//! No log event carries request content: only dependency names, reasons, schema keys and the
//! request id.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use application_manager_sdk::models::{
    ApplicationRef, Dependency, HandoffOperation, OutcomeCategory, OutcomeReport, Problem,
    RegistryEffect, UnavailableEntry, UnavailableReason,
};
use application_manager_sdk::{ApplicationEnvelope, CallerIdentity, RequestHandoffContent};
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::Semaphore;
use tokio::task::{AbortHandle, JoinError};
use tokio::time::error::Elapsed;
use tokio_util::sync::CancellationToken;
use toolkit_macros::domain_model;
use toolkit_security::SecurityContext;
use uuid::Uuid;

use crate::config::ApplicationManagerConfig;
use crate::domain::audit_handoff::{AuditHandoffClient, log_not_accepted, reason_token};
use crate::domain::fingerprint::{schema_fingerprint, sha256_text};
use crate::domain::limits::MAX_VALIDATION_PROBLEMS;
use crate::domain::manifest::{EnvelopeFields, extract_envelope, parse_manifest};
use crate::domain::ports::{
    Clock, PolicyAction, PolicyDecision, PolicyPort, RegistryStore, RequestIdSource,
};
use crate::domain::registry::is_storable_text;
use crate::domain::request::{
    AtCapacity, Progress, RequestHandle, RequestPermit, Retained, Tasks, try_admit,
};
use crate::domain::router::SectionPluginRouter;
use crate::domain::schema_validator::{
    SectionSchema, UnusableCause, UnusableSchema, check_schemas, validate_manifest,
};
use crate::domain::startup::{StartupResult, StartupResultHolder};

const TOO_MANY_PROBLEMS: &str = "too_many_problems";

/// Test seam: a seed body equal to this panics in the parse of step 1, so a test can show that
/// the supervisor's recovery never parses the input again.
#[cfg(test)]
pub(crate) const PARSE_PANIC_MARKER: &[u8] = b"\0test-parse-panic\0";

/// The settings the gate reads.
#[domain_model]
#[derive(Debug, Clone)]
pub struct OrchestratorSettings {
    /// `max_envelope_text_length`.
    pub max_envelope_text_length: usize,
    /// `registry_operation_timeout`.
    pub registry_operation_timeout: Duration,
    /// The step timeout of the schema-validation step.
    pub step_timeout: Duration,
    /// The most validations that may be queued or running on the blocking pool at once: the
    /// request task bound, `max_request_tasks`. A validation abandoned at the step timeout
    /// that is already running keeps its slot until it ends.
    pub max_validations: usize,
    /// `max_request_tasks`: the most request tasks in flight at once.
    pub max_request_tasks: usize,
}

impl OrchestratorSettings {
    /// The step timeout is the one plugin time limit, `plugin_call_timeout`; the validation
    /// bound and the request-task cap are both `max_request_tasks`.
    #[must_use]
    pub fn from_config(config: &ApplicationManagerConfig) -> Self {
        Self {
            max_envelope_text_length: config.max_envelope_text_length,
            registry_operation_timeout: config.registry_operation_timeout,
            step_timeout: config.plugin_call_timeout,
            max_validations: config.max_request_tasks,
            max_request_tasks: config.max_request_tasks,
        }
    }
}

/// What the caller submitted.
#[domain_model]
#[derive(Clone)]
pub enum RequestInput {
    /// The raw seed body.
    Seed { body: Vec<u8> },
    /// The uninstall id, as submitted.
    Uninstall { application_id: String },
}

impl RequestInput {
    /// The operation this input asks for.
    #[must_use]
    pub fn operation(&self) -> HandoffOperation {
        match self {
            Self::Seed { .. } => HandoffOperation::Seed,
            Self::Uninstall { .. } => HandoffOperation::Uninstall,
        }
    }
}

/// Redacting: the body and the id are request content, so `Debug` shows their lengths only.
impl fmt::Debug for RequestInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Seed { body } => f
                .debug_struct("Seed")
                .field("body_len", &body.len())
                .finish(),
            Self::Uninstall { application_id } => f
                .debug_struct("Uninstall")
                .field("application_id_len", &application_id.len())
                .finish(),
        }
    }
}

/// What dispatch needs once the gate lets a request through.
#[domain_model]
#[derive(Clone, PartialEq)]
pub struct Proceed {
    /// Seed: the manifest's id and version. Uninstall: the id and the last seeded version.
    pub envelope: ApplicationEnvelope,
    /// Seed: the parsed manifest.
    pub manifest: Option<Value>,
    /// Seed: the live schemas, in priority order.
    pub schemas: Vec<SectionSchema>,
    /// Seed: the fingerprint of each schema by key, computed with the validation on the blocking
    /// pool. Empty for an uninstall.
    pub fingerprints: Vec<(String, String)>,
}

/// Redacting: the envelope and the manifest are request content, so `Debug` shows only
/// whether they are there and how many schemas there are.
impl fmt::Debug for Proceed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Proceed")
            .field("envelope", &"<redacted>")
            .field("manifest", &self.manifest.as_ref().map(|_| "<redacted>"))
            .field("schemas", &self.schemas.len())
            .field("fingerprints", &self.fingerprints.len())
            .finish()
    }
}

/// The gate's verdict.
#[domain_model]
#[derive(Clone, PartialEq)]
pub enum GateOutcome {
    /// Decided before dispatch.
    Decided(OutcomeReport),
    /// Permitted, handed off and valid.
    Proceed(Proceed),
}

/// Redacting: a decided report can quote request content in its problems and application, so
/// `Debug` shows the category and the counts only.
impl fmt::Debug for GateOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decided(report) => f
                .debug_struct("Decided")
                .field("category", &report.category)
                .field("problems", &report.problems.len())
                .field("unavailable", &report.unavailable.len())
                .finish_non_exhaustive(),
            Self::Proceed(proceed) => f.debug_tuple("Proceed").field(proceed).finish(),
        }
    }
}

/// The result of steps 1-5.
#[domain_model]
#[derive(Clone, PartialEq)]
pub struct GateResult {
    /// The request id.
    pub request_id: Uuid,
    /// The caller identity every port call received.
    pub caller: CallerIdentity,
    /// The audit id; present only when the request hand-off was accepted.
    pub audit_id: Option<String>,
    /// The verdict.
    pub outcome: GateOutcome,
}

/// Redacting through [`GateOutcome`]; the caller identity carries no token.
impl fmt::Debug for GateResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GateResult")
            .field("request_id", &self.request_id)
            .field("caller", &self.caller)
            .field("audit_id", &self.audit_id)
            .field("outcome", &self.outcome)
            .finish()
    }
}

/// Joins the envelope limit problems to the validation problems.
#[must_use]
pub fn join_problems(limit: Vec<Problem>, validation: Vec<Problem>) -> Vec<Problem> {
    let mut truncated = false;
    let mut seen: HashSet<(String, String, String)> = HashSet::new();
    let mut out = Vec::new();
    for p in limit.into_iter().chain(validation) {
        if p.rule == TOO_MANY_PROBLEMS && p.location.is_empty() {
            truncated = true;
            continue;
        }
        if seen.insert((p.location.clone(), p.rule.clone(), p.message.clone())) {
            out.push(p);
        }
    }
    out.sort_by(|a, b| (&a.location, &a.rule, &a.message).cmp(&(&b.location, &b.rule, &b.message)));
    if out.len() > MAX_VALIDATION_PROBLEMS {
        out.truncate(MAX_VALIDATION_PROBLEMS);
        truncated = true;
    }
    if truncated {
        out.push(Problem {
            location: String::new(),
            rule: TOO_MANY_PROBLEMS.to_owned(),
            message: format!(
                "the manifest has more than {MAX_VALIDATION_PROBLEMS} problems; only the first are listed"
            ),
        });
    }
    out
}

fn cause_token(cause: UnusableCause) -> &'static str {
    match cause {
        UnusableCause::Dialect => "dialect",
        UnusableCause::ExternalReference => "external_reference",
        UnusableCause::Pattern => "pattern",
        UnusableCause::Number => "number",
        UnusableCause::TooLarge => "too_large",
        UnusableCause::TooDeep => "too_deep",
        UnusableCause::Compile => "compile",
    }
}

fn dependency_token(d: Dependency) -> &'static str {
    match d {
        Dependency::AccessControl => "access_control",
        Dependency::RegistryStorage => "registry_storage",
        Dependency::AuditPlugin => "audit_plugin",
        Dependency::SectionSchema => "section_schema",
    }
}

/// Renders `t` as RFC 3339 text; a time outside the representable range renders empty.
pub(crate) fn rfc3339(t: SystemTime) -> String {
    // Checked arithmetic: `OffsetDateTime::from(SystemTime)` panics outside its range.
    let moment = match t.duration_since(UNIX_EPOCH) {
        Ok(after) => time::Duration::try_from(after)
            .ok()
            .and_then(|d| OffsetDateTime::UNIX_EPOCH.checked_add(d)),
        Err(before) => time::Duration::try_from(before.duration())
            .ok()
            .and_then(|d| OffsetDateTime::UNIX_EPOCH.checked_sub(d)),
    };
    moment
        .and_then(|m| m.format(&Rfc3339).ok())
        .unwrap_or_default()
}

/// What step 1 extracted.
pub(crate) struct Received {
    operation: HandoffOperation,
    action: PolicyAction,
    /// The id sent to the PDP and read from the registry: present when extracted and storable.
    pdp_id: Option<String>,
    /// Seed: the parsed manifest.
    manifest: Option<Value>,
    /// Seed: the parse failure or the envelope limit problems.
    problems: Vec<Problem>,
    /// Seed: the envelope fields within the limits, as reported and handed off.
    application: Option<ApplicationRef>,
    /// Uninstall: the id as submitted.
    uninstall_id: Option<String>,
    /// Uninstall of an id that can never be stored.
    never_storable: bool,
}

impl Received {
    /// The application as reported and handed off: a seed's envelope fields within the limits;
    /// an uninstall's id as submitted when it can be stored, with the last seeded version when
    /// the entry was found; nothing for a never-storable id.
    pub(crate) fn application(&self, last_version: Option<&str>) -> Option<ApplicationRef> {
        match (&self.uninstall_id, self.never_storable) {
            (Some(id), false) => Some(ApplicationRef {
                id: Some(id.clone()),
                version: last_version.map(str::to_owned),
            }),
            (Some(_), true) => None,
            (None, _) => self.application.clone(),
        }
    }

    /// The request hand-off content (DESIGN §3.1). The manifest moves into it; the caller moves
    /// it back after the hand-off. A never-storable uninstall id travels as its byte length and
    /// digest, never by value.
    pub(crate) fn handoff_content(
        &mut self,
        request_id: Uuid,
        received_at: String,
        application: Option<ApplicationRef>,
        decided_category: Option<OutcomeCategory>,
    ) -> RequestHandoffContent {
        let (id_length_bytes, id_sha256) = self.id_digest();
        RequestHandoffContent {
            operation: self.operation,
            request_id,
            received_at,
            application,
            manifest: self.manifest.take(),
            decided_category,
            id_length_bytes,
            id_sha256,
        }
    }
}

impl Received {
    /// The byte length and digest of a never-storable uninstall id.
    fn id_digest(&self) -> (Option<u64>, Option<String>) {
        match never_storable_id(self) {
            Some(id) => (
                Some(u64::try_from(id.len()).unwrap_or(u64::MAX)),
                Some(sha256_text(id)),
            ),
            None => (None, None),
        }
    }

    /// What the progress record keeps of the receive step, so the supervisor's recovery can hand
    /// the request off without parsing it again.
    pub(crate) fn retained(&self) -> Retained {
        let (id_length_bytes, id_sha256) = self.id_digest();
        Retained {
            operation: self.operation,
            application: self.application(None),
            manifest: self.manifest.clone(),
            id_length_bytes,
            id_sha256,
        }
    }
}

fn ref_of(fields: &EnvelopeFields) -> Option<ApplicationRef> {
    if fields.id.is_none() && fields.version.is_none() {
        None
    } else {
        Some(ApplicationRef {
            id: fields.id.clone(),
            version: fields.version.clone(),
        })
    }
}

/// A verdict reached before dispatch.
struct Decision {
    category: OutcomeCategory,
    unavailable: Vec<UnavailableEntry>,
}

impl Decision {
    fn of(category: OutcomeCategory) -> Self {
        Self {
            category,
            unavailable: Vec::new(),
        }
    }

    fn unavailable(request_id: Uuid, dependency: Dependency, reason: UnavailableReason) -> Self {
        Self {
            category: OutcomeCategory::TemporarilyUnavailable,
            unavailable: vec![unavailable(request_id, dependency, reason)],
        }
    }
}

/// An unavailable item without a key; logs `dependency_unavailable`.
fn unavailable(
    request_id: Uuid,
    dependency: Dependency,
    reason: UnavailableReason,
) -> UnavailableEntry {
    tracing::warn!(
        dependency = dependency_token(dependency),
        reason = reason_token(reason),
        request_id = %request_id,
        "dependency_unavailable"
    );
    UnavailableEntry {
        dependency,
        key: None,
        reason,
    }
}

/// Logs the unavailable section schemas (each with its key); a schema answered as `unusable`
/// by the plugin's own error broke the schema contract.
fn log_schema_unavailable(request_id: Uuid, items: &[UnavailableEntry]) {
    for item in items {
        let key = item.key.as_deref().unwrap_or_default();
        tracing::warn!(
            dependency = "section_schema",
            key,
            reason = reason_token(item.reason),
            request_id = %request_id,
            "dependency_unavailable"
        );
        if item.reason == UnavailableReason::Unusable {
            tracing::warn!(key, cause = "contract", request_id = %request_id, "unusable_schema");
        }
    }
}

/// The schema check and whole-manifest validation that run on the blocking pool.
pub(crate) type ValidationFn = dyn Fn(&[SectionSchema], Option<&Value>) -> Result<Vec<Problem>, Vec<UnusableSchema>>
    + Send
    + Sync;

/// The production validation: `check_schemas` for a body that is not a manifest, otherwise
/// `validate_manifest`. It runs on a blocking thread and makes no tracing call, so no
/// manifest value can reach a log from it.
fn default_validation(
    schemas: &[SectionSchema],
    manifest: Option<&Value>,
) -> Result<Vec<Problem>, Vec<UnusableSchema>> {
    match manifest {
        Some(m) => validate_manifest(schemas, m),
        None => check_schemas(schemas).map(|()| Vec::new()),
    }
}

/// What every result of one request carries.
struct Admitted {
    request_id: Uuid,
    caller: CallerIdentity,
    /// The application as reported and handed off.
    application: Option<ApplicationRef>,
    audit_id: Option<String>,
}

impl Admitted {
    fn decided(
        self,
        category: OutcomeCategory,
        problems: Vec<Problem>,
        unavailable: Vec<UnavailableEntry>,
    ) -> GateResult {
        let outcome = GateOutcome::Decided(report(
            category,
            self.application,
            problems,
            unavailable,
            self.audit_id.clone(),
        ));
        GateResult {
            request_id: self.request_id,
            caller: self.caller,
            audit_id: self.audit_id,
            outcome,
        }
    }

    fn proceed(self, proceed: Proceed) -> GateResult {
        GateResult {
            request_id: self.request_id,
            caller: self.caller,
            audit_id: self.audit_id,
            outcome: GateOutcome::Proceed(proceed),
        }
    }
}

/// What steps 1-5 hand to steps 6-8.
pub(crate) struct Gated {
    /// The result of steps 1-5.
    pub(crate) result: GateResult,
    /// The startup result the request ran against; `None` only when none was published, which
    /// ends the request before any hand-off.
    pub(crate) started: Option<Arc<StartupResult>>,
    /// When the request was received: the seed time of a registry entry.
    pub(crate) received_time: SystemTime,
}

/// The ports and settings every request task shares: steps 1-5 here, steps 6-8 in
/// [`crate::domain::request`].
#[domain_model]
pub(crate) struct Pipeline {
    validation: Arc<ValidationFn>,
    policy: Arc<dyn PolicyPort>,
    pub(crate) registry: Arc<dyn RegistryStore>,
    pub(crate) startup: Arc<StartupResultHolder>,
    pub(crate) clock: Arc<dyn Clock>,
    ids: Arc<dyn RequestIdSource>,
    pub(crate) settings: OrchestratorSettings,
    /// One permit per validation queued or running on the blocking pool.
    validation_slots: Arc<Semaphore>,
    abandoned: AtomicU64,
    refused_at_capacity: AtomicU64,
}

impl Pipeline {
    fn new(
        policy: Arc<dyn PolicyPort>,
        registry: Arc<dyn RegistryStore>,
        startup: Arc<StartupResultHolder>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn RequestIdSource>,
        settings: OrchestratorSettings,
    ) -> Self {
        let validation_slots = Arc::new(Semaphore::new(settings.max_validations));
        Self {
            validation: Arc::new(default_validation),
            policy,
            registry,
            startup,
            clock,
            ids,
            settings,
            validation_slots,
            abandoned: AtomicU64::new(0),
            refused_at_capacity: AtomicU64::new(0),
        }
    }

    /// Validations abandoned at the step timeout since construction.
    fn abandoned_validations(&self) -> u64 {
        self.abandoned.load(Ordering::Relaxed)
    }

    /// Validations refused since construction because `max_validations` were already queued
    /// or running.
    fn validations_refused_at_capacity(&self) -> u64 {
        self.refused_at_capacity.load(Ordering::Relaxed)
    }

    /// Step 1: extracts what the later steps need; nothing is validated beyond the parse and
    /// the envelope limits.
    pub(crate) fn receive(&self, input: RequestInput) -> Received {
        let max = self.settings.max_envelope_text_length;
        #[cfg(test)]
        if let RequestInput::Seed { body } = &input {
            assert!(
                body.as_slice() != PARSE_PANIC_MARKER,
                "test seam: the parse panics"
            );
        }
        match input {
            RequestInput::Seed { body } => match parse_manifest(&body) {
                Ok(manifest) => {
                    let fields = extract_envelope(&manifest);
                    let problems = fields.limit_problems(max);
                    let application = ref_of(&fields.within_limits(max));
                    Received {
                        operation: HandoffOperation::Seed,
                        action: PolicyAction::Seed,
                        pdp_id: fields.id.filter(|id| is_storable_text(id)),
                        manifest: Some(manifest),
                        problems,
                        application,
                        uninstall_id: None,
                        never_storable: false,
                    }
                }
                Err(failure) => Received {
                    operation: HandoffOperation::Seed,
                    action: PolicyAction::Seed,
                    pdp_id: None,
                    manifest: None,
                    problems: vec![failure.to_problem()],
                    application: None,
                    uninstall_id: None,
                    never_storable: false,
                },
            },
            RequestInput::Uninstall { application_id } => {
                let storable = is_storable_text(&application_id);
                Received {
                    operation: HandoffOperation::Uninstall,
                    action: PolicyAction::Uninstall,
                    pdp_id: storable.then(|| application_id.clone()),
                    manifest: None,
                    problems: Vec::new(),
                    application: None,
                    uninstall_id: Some(application_id),
                    never_storable: !storable,
                }
            }
        }
    }

    /// Runs steps 1-5, writing the request progress record at each point (DESIGN §3.2 "Request
    /// progress record"). Every outcome is a decided category or a proceed; nothing is
    /// dispatched, and a proceed always carries an accepted request hand-off.
    pub(crate) async fn pre_dispatch(
        &self,
        ctx: &SecurityContext,
        input: RequestInput,
        progress: &Progress,
    ) -> Gated {
        let request_id = self.ids.next_id();
        let caller = CallerIdentity::from_security_context(ctx, request_id);
        let received_time = self.clock.now();
        progress.begin(caller.clone(), received_time);
        let received_at = rfc3339(received_time);
        let Some(started) = self.startup.result() else {
            // Defensive: the REST not-ready gate answers before a request reaches here.
            log_not_accepted(request_id, UnavailableReason::NotReady);
            let admitted = Admitted {
                request_id,
                caller,
                application: None,
                audit_id: None,
            };
            let item = unavailable(
                request_id,
                Dependency::AuditPlugin,
                UnavailableReason::NotReady,
            );
            let result = admitted.decided(
                OutcomeCategory::TemporarilyUnavailable,
                Vec::new(),
                vec![item],
            );
            return Gated {
                result,
                started: None,
                received_time,
            };
        };
        let mut rx = self.receive(input);
        progress.received(rx.retained());

        // Steps 2 and 3: the registry step is for a permitted caller only.
        let mut decided = self.decide_permission(ctx, &rx, request_id).await;
        let mut last_version = None;
        if decided.is_none() {
            match self.read_registry(&rx, request_id).await {
                Ok(version) => last_version = version,
                Err(decision) => decided = Some(decision),
            }
        }
        if let Some(decision) = &decided {
            progress.decided(decision.category);
        }

        // The application as reported and handed off.
        let application = rx.application(last_version.as_deref());

        // Step 4: the request hand-off, before any dispatch. The manifest moves into the
        // content and back, so it is not cloned.
        let content = rx.handoff_content(
            request_id,
            received_at,
            application.clone(),
            decided.as_ref().map(|d| d.category),
        );
        progress.handoff_attempted();
        let handoff = AuditHandoffClient::new(Arc::clone(started.audit()))
            .request_handoff(&caller, &content)
            .await;
        rx.manifest = content.manifest;
        let mut admitted = Admitted {
            request_id,
            caller,
            application,
            audit_id: None,
        };
        let result = match handoff {
            Ok(audit_id) => {
                progress.handoff_accepted(&audit_id);
                admitted.audit_id = Some(audit_id);
                self.after_handoff(admitted, decided, rx, last_version, &started)
                    .await
            }
            Err(reason) => {
                // An earlier decision of not permitted or not found stands; anything else
                // becomes temporarily unavailable. The audit item comes last.
                let mut decision = decided
                    .unwrap_or_else(|| Decision::of(OutcomeCategory::TemporarilyUnavailable));
                if !matches!(
                    decision.category,
                    OutcomeCategory::NotPermitted | OutcomeCategory::NotFound
                ) {
                    decision.category = OutcomeCategory::TemporarilyUnavailable;
                }
                decision
                    .unavailable
                    .push(unavailable(request_id, Dependency::AuditPlugin, reason));
                admitted.decided(decision.category, Vec::new(), decision.unavailable)
            }
        };
        if let GateOutcome::Proceed(proceed) = &result.outcome {
            progress.fingerprints(proceed.fingerprints.clone());
        }
        Gated {
            result,
            started: Some(started),
            received_time,
        }
    }

    /// What follows an accepted request hand-off: the decided category, an uninstall's proceed,
    /// or step 5 for a seed.
    async fn after_handoff(
        &self,
        admitted: Admitted,
        decided: Option<Decision>,
        rx: Received,
        last_version: Option<String>,
        started: &Arc<StartupResult>,
    ) -> GateResult {
        if let Some(decision) = decided {
            return admitted.decided(decision.category, Vec::new(), decision.unavailable);
        }
        if rx.operation == HandoffOperation::Uninstall {
            let envelope = ApplicationEnvelope {
                id: rx.uninstall_id.unwrap_or_default(),
                version: last_version.unwrap_or_default(),
            };
            return admitted.proceed(Proceed {
                envelope,
                manifest: None,
                schemas: Vec::new(),
                fingerprints: Vec::new(),
            });
        }
        self.validate_seed(admitted, rx, started).await
    }

    /// Step 2: one PDP call. `None` is a permit. The policy port owns the PDP deadline.
    async fn decide_permission(
        &self,
        ctx: &SecurityContext,
        rx: &Received,
        request_id: Uuid,
    ) -> Option<Decision> {
        match self
            .policy
            .decide(ctx, rx.action, rx.pdp_id.as_deref())
            .await
        {
            PolicyDecision::Permit => None,
            PolicyDecision::Refused => Some(Decision::of(OutcomeCategory::NotPermitted)),
            PolicyDecision::Unavailable(reason) => Some(Decision::unavailable(
                request_id,
                Dependency::AccessControl,
                reason.into(),
            )),
        }
    }

    /// Step 3: reads the registry under `registry_operation_timeout`. `Ok` carries the last
    /// seeded version of an uninstall target; `Err` is the decided verdict.
    ///
    /// A seed reads the registry too (the read is the reachability check of the step), for
    /// its id when it has a storable one.
    async fn read_registry(
        &self,
        rx: &Received,
        request_id: Uuid,
    ) -> Result<Option<String>, Decision> {
        if rx.never_storable {
            return Err(Decision::of(OutcomeCategory::NotFound));
        }
        let id = rx.pdp_id.as_deref().unwrap_or_default();
        let read = tokio::time::timeout(
            self.settings.registry_operation_timeout,
            self.registry.get(id),
        )
        .await;
        let entry = match read {
            Err(_elapsed) => {
                return Err(Decision::unavailable(
                    request_id,
                    Dependency::RegistryStorage,
                    UnavailableReason::TimedOut,
                ));
            }
            Ok(Err(e)) => {
                return Err(Decision::unavailable(
                    request_id,
                    Dependency::RegistryStorage,
                    e.reason(),
                ));
            }
            Ok(Ok(entry)) => entry,
        };
        if rx.operation != HandoffOperation::Uninstall {
            return Ok(None);
        }
        match entry {
            None => Err(Decision::of(OutcomeCategory::NotFound)),
            Some(e) => Ok(Some(e.version().to_owned())),
        }
    }

    /// Step 5: the live schema fetch and the whole-manifest validation of a seed.
    async fn validate_seed(
        &self,
        admitted: Admitted,
        mut rx: Received,
        started: &Arc<StartupResult>,
    ) -> GateResult {
        let request_id = admitted.request_id;
        let router = SectionPluginRouter::new(Arc::clone(started));
        let schemas = match router.fetch_schemas(&admitted.caller).await {
            Ok(schemas) => schemas,
            Err(items) => {
                log_schema_unavailable(request_id, &items);
                return admitted.decided(
                    OutcomeCategory::TemporarilyUnavailable,
                    Vec::new(),
                    items,
                );
            }
        };
        let validated = self
            .validate_off_runtime(request_id, schemas, rx.manifest.take())
            .await;
        let (result, schemas, manifest, fingerprints) = match validated {
            Validated::Done {
                result,
                schemas,
                manifest,
                fingerprints,
            } => (result, schemas, manifest, fingerprints),
            // Abandoned at the step timeout, failed, or refused at capacity: nothing to list,
            // the audit id stays.
            Validated::Abandoned | Validated::Failed | Validated::AtCapacity => {
                return admitted.decided(
                    OutcomeCategory::TemporarilyUnavailable,
                    Vec::new(),
                    Vec::new(),
                );
            }
        };
        rx.manifest = manifest;
        match result {
            Err(unusable) => {
                let mut items: Vec<UnavailableEntry> = unusable
                    .into_iter()
                    .map(|u| {
                        tracing::warn!(
                            key = %u.key,
                            cause = cause_token(u.cause),
                            request_id = %request_id,
                            "unusable_schema"
                        );
                        UnavailableEntry {
                            dependency: Dependency::SectionSchema,
                            key: Some(u.key),
                            reason: UnavailableReason::Unusable,
                        }
                    })
                    .collect();
                // The validator lists keys in key order; the items follow the priority order.
                items.sort_by_key(|e| schemas.iter().position(|s| Some(&s.key) == e.key.as_ref()));
                admitted.decided(OutcomeCategory::TemporarilyUnavailable, Vec::new(), items)
            }
            Ok(validation) => {
                // A body that did not parse ends as invalid manifest with its one parse
                // problem; a parsed manifest joins the envelope limit problems to the
                // validation ones.
                let problems = if rx.manifest.is_some() {
                    join_problems(rx.problems, validation)
                } else {
                    rx.problems
                };
                if problems.is_empty() {
                    let fields = rx
                        .manifest
                        .as_ref()
                        .map(extract_envelope)
                        .unwrap_or_default();
                    admitted.proceed(Proceed {
                        envelope: ApplicationEnvelope {
                            id: fields.id.unwrap_or_default(),
                            version: fields.version.unwrap_or_default(),
                        },
                        manifest: rx.manifest,
                        schemas,
                        fingerprints,
                    })
                } else {
                    admitted.decided(OutcomeCategory::InvalidManifest, problems, Vec::new())
                }
            }
        }
    }

    /// Runs the schema check and the validation on the blocking pool under the step timeout.
    ///
    /// A slot of `max_validations` is taken before the task is spawned and moves into the
    /// closure, so it is released when the work ends, and a validation that is still running
    /// after its step timeout keeps counting against the bound. At the bound the request is
    /// refused at once. At the step timeout (or when the request is dropped) the task is
    /// aborted, which cancels a validation still queued on the pool.
    async fn validate_off_runtime(
        &self,
        request_id: Uuid,
        schemas: Vec<SectionSchema>,
        manifest: Option<Value>,
    ) -> Validated {
        let Ok(slot) = Arc::clone(&self.validation_slots).try_acquire_owned() else {
            self.refused_at_capacity.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(request_id = %request_id, "manifest_validation_at_capacity");
            return Validated::AtCapacity;
        };
        let validation = Arc::clone(&self.validation);
        let mut handle = tokio::task::spawn_blocking(move || {
            let result = validation(&schemas, manifest.as_ref());
            // The fingerprints of a manifest that passed, here and not on the async task: the
            // canonical form of a large schema is CPU work.
            let fingerprints = match (&result, &manifest) {
                (Ok(problems), Some(_)) if problems.is_empty() => schemas
                    .iter()
                    .map(|s| (s.key.clone(), schema_fingerprint(&s.schema)))
                    .collect(),
                _ => Vec::new(),
            };
            drop(slot);
            (result, schemas, manifest, fingerprints)
        });
        // Dropped at the end of this call: at the step timeout (or when the request is
        // dropped) it aborts the task.
        let _abort_on_drop = AbortOnDrop(handle.abort_handle());
        let waited = tokio::time::timeout(self.settings.step_timeout, &mut handle).await;
        self.settle(request_id, waited)
    }

    /// Maps how the wait for the blocking task ended to the step's result, with its log event.
    fn settle(
        &self,
        request_id: Uuid,
        waited: Result<Result<ValidationOutput, JoinError>, Elapsed>,
    ) -> Validated {
        match waited {
            Ok(Ok((result, schemas, manifest, fingerprints))) => Validated::Done {
                result,
                schemas,
                manifest,
                fingerprints,
            },
            Ok(Err(failure)) => {
                tracing::warn!(
                    request_id = %request_id,
                    panicked = failure.is_panic(),
                    "manifest_validation_failed"
                );
                Validated::Failed
            }
            Err(_elapsed) => {
                self.abandoned.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(request_id = %request_id, "manifest_validation_abandoned");
                Validated::Abandoned
            }
        }
    }
}

/// The request orchestrator (DESIGN §3.2 "Request Orchestrator"): the shared steps of a request
/// and the owner of the request tasks, their permits and their drain.
#[domain_model]
pub struct Orchestrator {
    pipeline: Arc<Pipeline>,
    /// One permit per request task that may be in flight: `max_request_tasks`.
    task_slots: Arc<Semaphore>,
    tasks: Tasks,
}

impl Orchestrator {
    /// Builds the orchestrator over its ports.
    #[must_use]
    pub fn new(
        policy: Arc<dyn PolicyPort>,
        registry: Arc<dyn RegistryStore>,
        startup: Arc<StartupResultHolder>,
        clock: Arc<dyn Clock>,
        ids: Arc<dyn RequestIdSource>,
        settings: OrchestratorSettings,
    ) -> Self {
        let task_slots = Arc::new(Semaphore::new(settings.max_request_tasks));
        Self {
            pipeline: Arc::new(Pipeline::new(
                policy, registry, startup, clock, ids, settings,
            )),
            task_slots,
            tasks: Tasks::default(),
        }
    }

    /// Replaces the validation that runs on the blocking pool (the test seam: a validation that
    /// blocks until the test releases it). Only before the first request starts.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_validation(mut self, validation: Arc<ValidationFn>) -> Self {
        Arc::get_mut(&mut self.pipeline)
            .expect("the pipeline is not shared before the first request")
            .validation = validation;
        self
    }

    /// The registry store the orchestrator reads (tests only).
    #[cfg(test)]
    pub(crate) fn registry(&self) -> Arc<dyn RegistryStore> {
        Arc::clone(&self.pipeline.registry)
    }

    /// The policy port the orchestrator asks (tests only).
    #[cfg(test)]
    pub(crate) fn policy(&self) -> Arc<dyn PolicyPort> {
        Arc::clone(&self.pipeline.policy)
    }

    /// Runs steps 1-5 only, on the caller's task: nothing is dispatched, no cap or supervisor
    /// applies and no progress is kept (tests only). The REST API runs whole requests through
    /// [`Self::start_request`].
    #[cfg(test)]
    pub(crate) async fn pre_dispatch(
        &self,
        ctx: &SecurityContext,
        input: RequestInput,
    ) -> GateResult {
        self.pipeline
            .pre_dispatch(ctx, input, &Progress::default())
            .await
            .result
    }

    /// Validations abandoned at the step timeout since construction.
    #[must_use]
    pub fn abandoned_validations(&self) -> u64 {
        self.pipeline.abandoned_validations()
    }

    /// Validations refused since construction because `max_validations` were already queued
    /// or running.
    #[must_use]
    pub fn validations_refused_at_capacity(&self) -> u64 {
        self.pipeline.validations_refused_at_capacity()
    }

    /// Takes a request-task permit for a request of `operation`: the REST handler's cap check,
    /// before anything else is asked for the request. The permit is released when the
    /// supervisor started with it ends (or when it is dropped unused), and until then a
    /// [`Self::drain`] waits for it.
    ///
    /// # Errors
    ///
    /// [`AtCapacity`] when `max_request_tasks` permits are taken, or after [`Self::drain`]
    /// closed admission; the refusal is logged as a request hand-off that was not accepted.
    pub fn try_admit(&self, operation: HandoffOperation) -> Result<RequestPermit, AtCapacity> {
        try_admit(&self.task_slots, &self.tasks, operation)
    }

    /// Starts the supervisor of one request, detached from the caller, and returns its handle;
    /// the REST handler only awaits it. Dropping the handle cuts no step short: the request runs
    /// to its end, and the permit is held until then. The input decides the operation (the
    /// in-flight gauge, the abandoned count and the recovered registry effect follow it), not
    /// the operation the permit was admitted for.
    #[must_use]
    pub fn start_request(
        &self,
        permit: RequestPermit,
        ctx: SecurityContext,
        input: RequestInput,
    ) -> RequestHandle {
        self.tasks.start(&self.pipeline, permit, ctx, input)
    }

    /// Request tasks in flight: supervisors that have not ended, whether or not their HTTP
    /// request was dropped.
    #[must_use]
    pub fn in_flight_request_tasks(&self) -> usize {
        self.tasks.in_flight()
    }

    /// Closes admission, then waits for the permits taken and the running supervisors until
    /// `deadline` is cancelled: a request admitted before the drain still runs to its end, and
    /// the drain waits for it. Returns how many it abandoned, logging the count per operation
    /// (and the permits never used); it cancels nothing.
    pub async fn drain(&self, deadline: &CancellationToken) -> usize {
        self.task_slots.close();
        self.tasks.drain(deadline).await
    }
}

/// What the validation task returns: the result and the inputs it was given.
type ValidationOutput = (
    Result<Vec<Problem>, Vec<UnusableSchema>>,
    Vec<SectionSchema>,
    Option<Value>,
    Vec<(String, String)>,
);

/// How the validation step ended.
enum Validated {
    /// The validation returned; its inputs come back with the result.
    Done {
        result: Result<Vec<Problem>, Vec<UnusableSchema>>,
        schemas: Vec<SectionSchema>,
        manifest: Option<Value>,
        /// The schema fingerprints by key, when the manifest passed; empty otherwise.
        fingerprints: Vec<(String, String)>,
    },
    /// The step timeout expired; the task was aborted (a running one finishes on its own).
    Abandoned,
    /// The task panicked or was cancelled by the runtime.
    Failed,
    /// `max_validations` were already queued or running.
    AtCapacity,
}

/// Aborts the blocking task when the request is dropped before the step ends.
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The uninstall id that can never be stored, the one id the hand-off describes by its byte
/// length and digest instead of by value.
fn never_storable_id(rx: &Received) -> Option<&str> {
    if rx.never_storable {
        rx.uninstall_id.as_deref()
    } else {
        None
    }
}

fn report(
    category: OutcomeCategory,
    application: Option<ApplicationRef>,
    problems: Vec<Problem>,
    unavailable: Vec<UnavailableEntry>,
    audit_id: Option<String>,
) -> OutcomeReport {
    OutcomeReport {
        category,
        application,
        partial: false,
        sections: Vec::new(),
        problems,
        unavailable,
        registry_effect: RegistryEffect::Unchanged,
        audit_id,
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "orchestrator_tests.rs"]
mod orchestrator_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "orchestrator_validation_tests.rs"]
mod orchestrator_validation_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "orchestrator_gate_types_tests.rs"]
mod orchestrator_gate_types_tests;
