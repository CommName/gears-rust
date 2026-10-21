//! Startup and registration (`cpt-cf-application-manager-component-startup-registration`).
//!
//! Runs once from `start`: lists the section plugin instances, reads their
//! priorities, resolves their clients, reads every key through the call
//! guard, and checks the set (`cpt-cf-application-manager-fr-startup-checks`).
//! Any failure stops the gear with a DESIGN 4.1 reason; there is no retry.

use std::collections::HashMap;
use std::fmt;

use application_manager_sdk::{ApplicationManagerSectionPluginSpecV1, SectionPluginClientV1};
use gts::GtsSchema;
use toolkit::client_hub::{ClientHub, ClientScope};
use toolkit::gts::PluginV1;
use types_registry_sdk::{InstanceQuery, TypesRegistryClient};

use crate::constants::is_valid_section_key;
use crate::domain::plugin_set::{PluginSet, SectionPlugin};
use crate::infra::call_guard::CallGuard;

/// A DESIGN 4.1 startup failure reason this component reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupReason {
    /// The types registry cannot be reached or cannot list instances.
    TypesRegistryUnreachable,
    /// An instance's content or priority cannot be read.
    PriorityUnreadable,
    /// No `ClientHub` client is registered for an instance.
    PluginClientUnresolved,
    /// A plugin's `key` call failed, timed out, or was not sent.
    SectionKeyUnavailable,
    /// A key breaks the section key grammar.
    InvalidSectionKey,
    /// Two plugins announced the same key.
    DuplicateSectionKey,
    /// Two plugins have the same priority.
    EqualPriority,
}

impl StartupReason {
    /// The DESIGN 4.1 reason name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TypesRegistryUnreachable => "types_registry_unreachable",
            Self::PriorityUnreadable => "priority_unreadable",
            Self::PluginClientUnresolved => "plugin_client_unresolved",
            Self::SectionKeyUnavailable => "section_key_unavailable",
            Self::InvalidSectionKey => "invalid_section_key",
            Self::DuplicateSectionKey => "duplicate_section_key",
            Self::EqualPriority => "equal_priority",
        }
    }
}

impl fmt::Display for StartupReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A startup check that failed; the gear does not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct StartupFailure {
    /// The failed check.
    pub reason: StartupReason,
    /// What failed, for the operator.
    pub detail: String,
}

impl fmt::Display for StartupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "startup_failed: {}: {}", self.reason, self.detail)
    }
}

impl StartupFailure {
    /// A failure of the check `reason`, with `detail` for the operator.
    #[must_use]
    pub fn new(reason: StartupReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }
}

/// An instance whose priority and client are known, before its key is read.
struct Candidate {
    instance_id: String,
    priority: i16,
    client: std::sync::Arc<dyn SectionPluginClientV1>,
}

/// Build the plugin set, or report the first failing check in DESIGN 4.1
/// order. An empty registry answer gives an empty set.
///
/// # Errors
/// [`StartupFailure`] naming the failed check.
pub async fn discover(hub: &ClientHub, guard: &CallGuard) -> Result<PluginSet, StartupFailure> {
    let type_id = <ApplicationManagerSectionPluginSpecV1 as GtsSchema>::TYPE_ID;

    let registry = hub
        .get::<dyn TypesRegistryClient>()
        .map_err(|e| StartupFailure::new(StartupReason::TypesRegistryUnreachable, e.to_string()))?;
    let mut instances = registry
        .list_instances(InstanceQuery::new().with_pattern(format!("{type_id}*")))
        .await
        .map_err(|e| StartupFailure::new(StartupReason::TypesRegistryUnreachable, e.to_string()))?;
    instances.retain(|i| i.id.as_ref().starts_with(type_id));
    instances.sort_by(|a, b| a.id.as_ref().cmp(b.id.as_ref()));

    let mut priorities = Vec::with_capacity(instances.len());
    for instance in &instances {
        let id = instance.id.as_ref();
        let content: PluginV1<ApplicationManagerSectionPluginSpecV1> =
            serde_json::from_value(instance.object.clone()).map_err(|e| {
                StartupFailure::new(StartupReason::PriorityUnreadable, format!("{id}: {e}"))
            })?;
        if content.id.as_ref() != id {
            return Err(StartupFailure::new(
                StartupReason::PriorityUnreadable,
                format!("{id}: content id {} does not match", content.id.as_ref()),
            ));
        }
        priorities.push((id.to_owned(), content.priority));
    }

    let mut candidates = Vec::with_capacity(priorities.len());
    for (instance_id, priority) in priorities {
        let client = hub
            .try_get_scoped::<dyn SectionPluginClientV1>(&ClientScope::gts_id(&instance_id))
            .ok_or_else(|| {
                StartupFailure::new(StartupReason::PluginClientUnresolved, instance_id.clone())
            })?;
        candidates.push(Candidate {
            instance_id,
            priority,
            client,
        });
    }

    let mut plugins = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let key = match guard
            .call(&candidate.instance_id, candidate.client.key())
            .await
        {
            Ok(Ok(key)) => key,
            Ok(Err(e)) => {
                return Err(StartupFailure::new(
                    StartupReason::SectionKeyUnavailable,
                    format!("{}: {e}", candidate.instance_id),
                ));
            }
            Err(e) => {
                return Err(StartupFailure::new(
                    StartupReason::SectionKeyUnavailable,
                    format!("{}: {e}", candidate.instance_id),
                ));
            }
        };
        plugins.push(SectionPlugin {
            key,
            priority: candidate.priority,
            instance_id: candidate.instance_id,
            client: candidate.client,
        });
    }

    if let Some(p) = plugins.iter().find(|p| !is_valid_section_key(&p.key)) {
        return Err(StartupFailure::new(
            StartupReason::InvalidSectionKey,
            format!("{}: {:?}", p.instance_id, p.key),
        ));
    }

    let mut by_key: HashMap<&str, &str> = HashMap::new();
    for p in &plugins {
        if let Some(other) = by_key.insert(&p.key, &p.instance_id) {
            return Err(StartupFailure::new(
                StartupReason::DuplicateSectionKey,
                format!("{:?}: {other} and {}", p.key, p.instance_id),
            ));
        }
    }

    let mut by_priority: HashMap<i16, &str> = HashMap::new();
    for p in &plugins {
        if let Some(other) = by_priority.insert(p.priority, &p.instance_id) {
            return Err(StartupFailure::new(
                StartupReason::EqualPriority,
                format!("{}: {other} and {}", p.priority, p.instance_id),
            ));
        }
    }

    Ok(PluginSet::new(plugins))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "startup_tests.rs"]
mod tests;
