//! Startup and Registration component (DESIGN §3.2, §3.6, §4.6).
//!
//! `start` runs the discovery steps once each, in order, without retry (ADR 0001):
//! list the plugin instances, read priorities, check the audit plugin, resolve the
//! scoped clients, read every section key concurrently, validate the keys, and publish
//! the [`StartupResult`]. The first failing step ends the start with an error whose
//! text begins with the §4.6 reason token, and one `startup_failed` event is logged.
//! No tenant service is used (ADR 0007).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use application_manager_sdk::gts::{
    ApplicationManagerAuditPluginSpecV1, ApplicationManagerSectionPluginSpecV1,
};
use application_manager_sdk::section_plugin_api::validate_section_key;
use application_manager_sdk::{AuditPluginClientV1, SectionPluginClientV1};
use async_trait::async_trait;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use toolkit::client_hub::{ClientHub, ClientScope};
use toolkit::{Healthcheck, HealthcheckResult, RunnableCapability};
use types_registry_sdk::{GtsInstance, InstanceQuery, TypesRegistryClient};

use crate::config::{ApplicationManagerConfig, GATEWAY_REQUEST_DEADLINE};
use crate::domain::limits::LOG_TEXT_MAX_BYTES;
use crate::domain::ports::StartupPort;
use crate::domain::startup::{SectionPluginEntry, StartupResult, StartupResultHolder};
use crate::infra::call_guard::{CallKind, CallMeta, PluginCallGuard};
use crate::infra::plugin_adapters::{GuardedAuditSink, GuardedSectionPlugin};

/// GTS type id of section plugin instances, from the SDK spec (DESIGN §3.1).
fn section_type() -> String {
    ApplicationManagerSectionPluginSpecV1::gts_type_id().to_string()
}

/// GTS type id of audit plugin instances, from the SDK spec (DESIGN §3.1).
fn audit_type() -> String {
    ApplicationManagerAuditPluginSpecV1::gts_type_id().to_string()
}

/// Listing pattern covering both plugin families: every instance of the base plugin
/// type, derived from the section spec's type id (its first segment). Instances are
/// classified by their full type id afterwards.
fn plugin_pattern() -> String {
    let section = section_type();
    let base = section.split('~').next().unwrap_or_default();
    format!("{base}~*")
}

/// Escapes control characters and cuts the text to [`LOG_TEXT_MAX_BYTES`] at a
/// character boundary, so it is safe to put in one log field.
fn bounded_log_text(text: &str) -> String {
    let mut out = String::new();
    for ch in text.chars() {
        let escaped = ch.escape_debug().to_string();
        if out.len() + escaped.len() > LOG_TEXT_MAX_BYTES {
            break;
        }
        out.push_str(&escaped);
    }
    out
}

/// A failed startup step: the §4.6 reason token, the instance ids involved and a
/// short fixed detail.
struct Failure {
    reason: &'static str,
    instances: Vec<String>,
    detail: String,
    /// Diagnostic text of the underlying error, logged (escaped, bounded) but never
    /// part of the start error.
    cause: Option<String>,
}

impl Failure {
    fn new(reason: &'static str, instances: Vec<String>, detail: impl Into<String>) -> Self {
        Self {
            reason,
            instances,
            detail: detail.into(),
            cause: None,
        }
    }

    fn with_cause(mut self, cause: impl Into<String>) -> Self {
        self.cause = Some(cause.into());
        self
    }

    /// Logs the one `startup_failed` event and converts to the start error.
    fn report(self) -> anyhow::Error {
        let instances = self.instances.join(",");
        tracing::error!(
            reason = %self.reason,
            instances = %bounded_log_text(&instances),
            cause = self.cause.as_deref().map(|c| tracing::field::display(bounded_log_text(c))),
            "startup_failed"
        );
        anyhow::anyhow!("{}: {}", self.reason, self.detail)
    }
}

/// A discovered plugin instance.
struct Found {
    id: String,
    priority: i16,
}

fn priority_of(instance: &GtsInstance) -> Option<i16> {
    instance
        .object
        .get("priority")?
        .as_i64()
        .and_then(|p| i16::try_from(p).ok())
}

/// The hand-implemented start capability of the gear.
pub struct StartupComponent {
    timeout: Duration,
    pdp_timeout: Duration,
    registry_timeout: Duration,
    hub: Arc<ClientHub>,
    holder: Arc<StartupResultHolder>,
    guard: PluginCallGuard,
}

impl StartupComponent {
    /// Builds the component and its Plugin Call Guard from `plugin_call_timeout` and
    /// `plugin_max_in_flight`. Nothing is resolved from `hub` until `start`.
    ///
    /// `config` must be validated (`ApplicationManagerConfig::validate`): the guard
    /// requires `plugin_max_in_flight >= 1`.
    ///
    /// # Panics
    ///
    /// Panics if `config.plugin_max_in_flight` is zero (the guard's assertion).
    #[must_use]
    pub fn new(
        config: &ApplicationManagerConfig,
        hub: Arc<ClientHub>,
        holder: Arc<StartupResultHolder>,
    ) -> Self {
        Self {
            timeout: config.plugin_call_timeout,
            pdp_timeout: config.pdp_call_timeout,
            registry_timeout: config.registry_operation_timeout,
            hub,
            holder,
            guard: PluginCallGuard::new(config.plugin_call_timeout, config.plugin_max_in_flight),
        }
    }

    /// The one guard shared by startup, the published ports and `stop`'s drain.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code, reason = "used by the orchestrator phase"))]
    pub(crate) fn guard(&self) -> &PluginCallGuard {
        &self.guard
    }

    async fn list_plugins(&self) -> Result<Vec<GtsInstance>, Failure> {
        let unreachable = |detail: &str| Failure::new("types_registry_unreachable", vec![], detail);
        let Some(registry) = self.hub.try_get::<dyn TypesRegistryClient>() else {
            return Err(unreachable("types registry client not available"));
        };
        let query = InstanceQuery::new().with_pattern(plugin_pattern());
        match tokio::time::timeout(self.timeout, registry.list_instances(query)).await {
            Ok(Ok(instances)) => Ok(instances),
            Ok(Err(e)) => Err(unreachable("listing failed").with_cause(e.to_string())),
            Err(_) => Err(unreachable("listing timed out")),
        }
    }

    /// Splits the listing into section and audit instances, in listing order; foreign
    /// plugin types are ignored. Every kept instance must have a readable priority.
    fn classify(instances: &[GtsInstance]) -> Result<(Vec<Found>, Vec<Found>), Failure> {
        let (section_type, audit_type) = (section_type(), audit_type());
        let mut sections = Vec::new();
        let mut audits = Vec::new();
        for instance in instances {
            let id: &str = instance.id.as_ref();
            let family = if id.starts_with(&section_type) {
                &mut sections
            } else if id.starts_with(&audit_type) {
                &mut audits
            } else {
                continue;
            };
            let Some(priority) = priority_of(instance) else {
                return Err(Failure::new(
                    "priority_unreadable",
                    vec![id.to_owned()],
                    "priority unreadable",
                ));
            };
            family.push(Found {
                id: id.to_owned(),
                priority,
            });
        }
        Ok((sections, audits))
    }

    fn check_order_and_audit<'a>(
        sections: &mut [Found],
        audits: &'a [Found],
    ) -> Result<&'a Found, Failure> {
        sections.sort_by(|a, b| (a.priority, &a.id).cmp(&(b.priority, &b.id)));
        if let Some(pair) = sections.windows(2).find(|w| w[0].priority == w[1].priority) {
            return Err(Failure::new(
                "equal_priority",
                vec![pair[0].id.clone(), pair[1].id.clone()],
                "equal priority",
            ));
        }
        match audits {
            [] => Err(Failure::new("no_audit_plugin", vec![], "no audit plugin")),
            [audit] => Ok(audit),
            _ => Err(Failure::new(
                "multiple_audit_plugins",
                audits.iter().map(|a| a.id.clone()).collect(),
                "more than one audit plugin",
            )),
        }
    }

    fn unresolved(id: &str) -> Failure {
        Failure::new(
            "plugin_client_unresolved",
            vec![id.to_owned()],
            "no scoped client",
        )
    }

    fn resolve_section_clients(
        &self,
        sections: &[Found],
    ) -> Result<Vec<Arc<dyn SectionPluginClientV1>>, Failure> {
        sections
            .iter()
            .map(|s| {
                self.hub
                    .try_get_scoped::<dyn SectionPluginClientV1>(&ClientScope::gts_id(&s.id))
                    .ok_or_else(|| Self::unresolved(&s.id))
            })
            .collect()
    }

    /// Calls `key` on every section plugin concurrently through the guard: the key
    /// phase takes one time limit, not one per plugin.
    async fn read_keys(
        &self,
        sections: &[Found],
        clients: &[Arc<dyn SectionPluginClientV1>],
    ) -> Result<Vec<String>, Failure> {
        // Driven in this task (not spawned), so every call keeps the span `start` runs
        // in; `join_all` keeps the order of `sections`.
        let calls = sections.iter().zip(clients).map(|(section, client)| {
            let guard = self.guard.clone();
            let client = Arc::clone(client);
            let id = section.id.clone();
            async move {
                let meta = CallMeta {
                    request_id: None,
                    call_kind: CallKind::Key,
                    section_key: None,
                };
                guard
                    .call(&id, meta, async move { client.key().await })
                    .await
            }
        });
        let results = futures::future::join_all(calls).await;
        let mut keys = Vec::with_capacity(results.len());
        let mut failed = Vec::new();
        for (section, result) in sections.iter().zip(results) {
            match result {
                Ok(key) => keys.push(key),
                // Any plugin error, timeout, refusal or panic: the key is unavailable.
                Err(_) => failed.push(section.id.clone()),
            }
        }
        if failed.is_empty() {
            Ok(keys)
        } else {
            Err(Failure::new(
                "section_key_unavailable",
                failed,
                "key call failed",
            ))
        }
    }

    fn check_keys(sections: &[Found], keys: &[String]) -> Result<(), Failure> {
        for (section, key) in sections.iter().zip(keys) {
            if let Err(e) = validate_section_key(key) {
                return Err(Failure::new(
                    "invalid_section_key",
                    vec![section.id.clone()],
                    "invalid section key",
                )
                .with_cause(e));
            }
        }
        let mut seen: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for (section, key) in sections.iter().zip(keys) {
            seen.entry(key).or_default().push(section.id.clone());
        }
        match seen.into_values().find(|ids| ids.len() > 1) {
            Some(ids) => Err(Failure::new("duplicate_section_key", ids, "duplicate key")),
            None => Ok(()),
        }
    }

    /// Warns when the worst-case request budget exceeds the gateway deadline
    /// (DESIGN §4.7): PDP + two registry operations + (N + 3) plugin limits.
    fn warn_on_budget(&self, section_count: usize) {
        let limits = u32::try_from(section_count)
            .unwrap_or(u32::MAX)
            .saturating_add(3);
        let budget = self
            .pdp_timeout
            .saturating_add(self.registry_timeout.saturating_mul(2))
            .saturating_add(self.timeout.saturating_mul(limits));
        if budget > GATEWAY_REQUEST_DEADLINE {
            tracing::warn!(
                n = section_count,
                budget_ms = u64::try_from(budget.as_millis()).unwrap_or(u64::MAX),
                "startup_budget_exceeded"
            );
        }
    }

    async fn run(&self) -> Result<(), Failure> {
        let started = Instant::now();
        if self.holder.result().is_some() {
            return Err(Failure::new(
                "already_started",
                vec![],
                "startup result already published",
            ));
        }
        let listed = self.list_plugins().await?;
        let (mut sections, audits) = Self::classify(&listed)?;
        let audit = Self::check_order_and_audit(&mut sections, &audits)?;
        let clients = self.resolve_section_clients(&sections)?;
        let audit_client = self
            .hub
            .try_get_scoped::<dyn AuditPluginClientV1>(&ClientScope::gts_id(&audit.id))
            .ok_or_else(|| Self::unresolved(&audit.id))?;
        let keys = self.read_keys(&sections, &clients).await?;
        Self::check_keys(&sections, &keys)?;
        self.warn_on_budget(sections.len());

        let entries = sections
            .iter()
            .zip(clients)
            .zip(keys)
            .map(|((section, client), key)| {
                let plugin = Arc::new(GuardedSectionPlugin::new(
                    &section.id,
                    &key,
                    client,
                    self.guard.clone(),
                ));
                SectionPluginEntry {
                    instance_id: section.id.clone(),
                    priority: section.priority,
                    key,
                    plugin,
                }
            })
            .collect();
        let sink = Arc::new(GuardedAuditSink::new(
            &audit.id,
            audit_client,
            self.guard.clone(),
        ));
        let section_count = sections.len();
        // The publish-time refusal is the backstop for a concurrent second start.
        self.holder
            .publish_result(StartupResult::new(entries, audit.id.clone(), sink))
            .map_err(|_| {
                Failure::new(
                    "already_started",
                    vec![],
                    "startup result already published",
                )
            })?;
        tracing::info!(
            sections = section_count,
            audit = %bounded_log_text(&audit.id),
            elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "startup_published"
        );
        Ok(())
    }
}

#[async_trait]
impl RunnableCapability for StartupComponent {
    /// Runs the startup steps once; `cancel` is ignored because startup is bounded by
    /// `plugin_call_timeout` alone and detached plugin calls are never cancelled.
    async fn start(&self, _cancel: CancellationToken) -> anyhow::Result<()> {
        self.run().await.map_err(Failure::report)
    }

    /// Sets the stopping flag, then drains the call guard until `deadline_token` fires.
    async fn stop(&self, deadline_token: CancellationToken) -> anyhow::Result<()> {
        self.holder.set_stopping();
        self.guard.drain(&deadline_token).await;
        Ok(())
    }
}

/// Readiness healthcheck: unhealthy until the result is published and again once
/// stopping, healthy otherwise. Reads only the startup port.
///
/// It is registered with the `rest` capability in a later phase; until then it is
/// built by tests only.
pub struct StartupHealthcheck {
    holder: Arc<dyn StartupPort>,
}

impl StartupHealthcheck {
    /// Creates the healthcheck over `holder`.
    #[must_use]
    pub fn new(holder: Arc<dyn StartupPort>) -> Self {
        Self { holder }
    }
}

#[async_trait]
impl Healthcheck for StartupHealthcheck {
    fn name(&self) -> &'static str {
        "application-manager-startup"
    }

    async fn check(&self) -> HealthcheckResult {
        if self.holder.is_ready() {
            HealthcheckResult::healthy()
        } else {
            HealthcheckResult::unhealthy("not ready")
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::needless_pass_by_value,
    clippy::duration_suboptimal_units,
    clippy::unchecked_time_subtraction
)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(clippy::too_many_lines)]
#[path = "startup_tests.rs"]
mod startup_tests;

#[cfg(test)]
#[allow(clippy::let_underscore_must_use, clippy::duration_suboptimal_units)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "startup_host_tests.rs"]
mod startup_host_tests;
