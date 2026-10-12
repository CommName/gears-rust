//! GTS plugin and resource type definitions (DESIGN §3.1).

use toolkit::gts::PluginV1;
use toolkit_gts::{gts_id, gts_type_schema};

/// GTS type of application-manager section plugin instances.
#[derive(Default)]
#[gts_type_schema(
    dir_path = "schemas",
    base = PluginV1,
    type_id = gts_id!("cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~"),
    description = "Application Manager section plugin specification",
    properties = "",
)]
pub struct ApplicationManagerSectionPluginSpecV1;

/// GTS type of application-manager audit plugin instances.
#[derive(Default)]
#[gts_type_schema(
    dir_path = "schemas",
    base = PluginV1,
    type_id = gts_id!("cf.toolkit.plugins.plugin.v1~cf.core.application_manager_audit.plugin.v1~"),
    description = "Application Manager audit plugin specification",
    properties = "",
)]
pub struct ApplicationManagerAuditPluginSpecV1;

/// GTS type id of the outcome report.
pub const OUTCOME_REPORT_TYPE_ID: &str = gts_id!("cf.core.application_manager.outcome_report.v1~");
/// `PolicyEnforcer` resource type of an application.
pub const APPLICATION_RESOURCE_TYPE: &str = gts_id!("cf.core.application_manager.application.v1~");
/// Authorization action: seed an application.
pub const ACTION_SEED: &str = "seed";
/// Authorization action: uninstall an application.
pub const ACTION_UNINSTALL: &str = "uninstall";
/// Authorization action: list applications.
pub const ACTION_LIST: &str = "list";
/// Authorization action: read a section schema.
pub const ACTION_READ_SCHEMA: &str = "read_schema";

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gts_tests.rs"]
mod gts_tests;
