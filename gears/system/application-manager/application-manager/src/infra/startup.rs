//! Startup and registration (`cpt-cf-application-manager-component-startup-registration`).
//!
//! Runs once from `start`: lists the section plugin instances, reads their
//! priorities and section types, resolves each section type and derives its
//! key, resolves the clients, and checks the set
//! (`cpt-cf-application-manager-fr-startup-checks`). It calls no section
//! plugin. Any failure stops the gear with a DESIGN 4.1 reason; there is no
//! retry.

use std::collections::HashMap;
use std::fmt;

use application_manager_sdk::{ApplicationManagerSectionPluginSpecV1, SectionPluginClientV1};
use gts::GtsSchema;
use toolkit::client_hub::{ClientHub, ClientScope};
use toolkit::gts::PluginV1;
use types_registry_sdk::{InstanceQuery, TypesRegistryClient};

use crate::domain::plugin_set::{PluginSet, SectionPlugin, section_key_of};

/// A DESIGN 4.1 startup failure reason this component reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupReason {
    /// The types registry cannot be reached or cannot list instances.
    TypesRegistryUnreachable,
    /// An instance's `priority` or `section_type` cannot be read.
    PluginPropertiesUnreadable,
    /// An instance's section type is not in the types registry.
    SectionTypeUnresolved,
    /// A section type is not derived directly from the section base type as
    /// one segment.
    InvalidSectionType,
    /// No `ClientHub` client is registered for an instance.
    PluginClientUnresolved,
    /// Two plugins have section types with the same key.
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
            Self::PluginPropertiesUnreadable => "plugin_properties_unreadable",
            Self::SectionTypeUnresolved => "section_type_unresolved",
            Self::InvalidSectionType => "invalid_section_type",
            Self::PluginClientUnresolved => "plugin_client_unresolved",
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

/// An instance whose properties are read.
struct Instance {
    id: String,
    priority: i16,
    section_type: String,
}

/// An instance whose section type is resolved and whose key is derived.
struct Resolved {
    instance: Instance,
    key: String,
    schema: serde_json::Value,
}

/// Build the plugin set, or report the first failing check in DESIGN 4.1
/// order. An empty registry answer gives an empty set. Each check runs over
/// every instance before the next check starts.
///
/// # Errors
/// [`StartupFailure`] naming the failed check.
pub async fn discover(hub: &ClientHub) -> Result<PluginSet, StartupFailure> {
    let type_id = <ApplicationManagerSectionPluginSpecV1 as GtsSchema>::TYPE_ID;

    let registry = hub
        .get::<dyn TypesRegistryClient>()
        .map_err(|e| StartupFailure::new(StartupReason::TypesRegistryUnreachable, e.to_string()))?;
    let mut listed = registry
        .list_instances(InstanceQuery::new().with_pattern(format!("{type_id}*")))
        .await
        .map_err(|e| StartupFailure::new(StartupReason::TypesRegistryUnreachable, e.to_string()))?;
    listed.retain(|i| i.id.as_ref().starts_with(type_id));
    listed.sort_by(|a, b| a.id.as_ref().cmp(b.id.as_ref()));

    let mut instances = Vec::with_capacity(listed.len());
    for listed in &listed {
        let id = listed.id.as_ref();
        let unreadable =
            |what: String| StartupFailure::new(StartupReason::PluginPropertiesUnreadable, what);
        let content: PluginV1<ApplicationManagerSectionPluginSpecV1> =
            serde_json::from_value(listed.object.clone())
                .map_err(|e| unreadable(format!("{id}: {e}")))?;
        if content.id.as_ref() != id {
            return Err(unreadable(format!(
                "{id}: content id {} does not match",
                content.id.as_ref()
            )));
        }
        instances.push(Instance {
            id: id.to_owned(),
            priority: content.priority,
            section_type: content.properties.section_type,
        });
    }

    let mut schemas = Vec::with_capacity(instances.len());
    for instance in &instances {
        let resolved = registry
            .get_type_schema(&instance.section_type)
            .await
            .map_err(|e| {
                StartupFailure::new(
                    StartupReason::SectionTypeUnresolved,
                    format!("{}: {:?}: {e}", instance.id, instance.section_type),
                )
            })?;
        schemas.push(resolved.effective_schema());
    }

    let mut resolved = Vec::with_capacity(instances.len());
    for (instance, schema) in instances.into_iter().zip(schemas) {
        let key = section_key_of(&instance.section_type).ok_or_else(|| {
            StartupFailure::new(
                StartupReason::InvalidSectionType,
                format!("{}: {:?}", instance.id, instance.section_type),
            )
        })?;
        resolved.push(Resolved {
            instance,
            key,
            schema,
        });
    }

    let mut plugins = Vec::with_capacity(resolved.len());
    for Resolved {
        instance,
        key,
        schema,
    } in resolved
    {
        let client = hub
            .try_get_scoped::<dyn SectionPluginClientV1>(&ClientScope::gts_id(&instance.id))
            .ok_or_else(|| {
                StartupFailure::new(StartupReason::PluginClientUnresolved, instance.id.clone())
            })?;
        plugins.push(SectionPlugin {
            key,
            section_type: instance.section_type,
            priority: instance.priority,
            instance_id: instance.id,
            client,
            schema,
        });
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
