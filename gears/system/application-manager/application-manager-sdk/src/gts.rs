//! GTS declarations for the Application Manager SDK.

use toolkit::gts::PluginV1;
use toolkit_gts::{gts_id, gts_type_schema};

/// GTS type definition for section plugin instances.
///
/// Each section plugin registers an instance of this type in its own `init`.
/// Its priority is the instance `priority`, unique across plugins.
///
/// # Instance ID Format
///
/// ```text
/// gts.cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~<vendor>.<package>.<name>.v1
/// ```
///
/// # Example
///
/// ```no_run
/// use application_manager_sdk::ApplicationManagerSectionPluginSpecV1;
/// use toolkit::gts::PluginV1;
///
/// fn registration() -> Result<(), Box<dyn std::error::Error>> {
///     let (_instance_id, _payload) =
///         PluginV1::<ApplicationManagerSectionPluginSpecV1>::build_registration(
///             "acme.billing.section.v1",
///             "acme",
///             100,
///         )?;
///     Ok(())
/// }
/// ```
#[derive(Default)]
#[gts_type_schema(
    dir_path = "schemas",
    base = PluginV1,
    type_id = gts_id!("cf.toolkit.plugins.plugin.v1~cf.core.application_manager_section.plugin.v1~"),
    description = "Application Manager section plugin specification",
    properties = "",
)]
pub struct ApplicationManagerSectionPluginSpecV1;

/// GTS resource type of an application, the PDP resource of ADR 0005.
pub const APPLICATION_RESOURCE_TYPE: &str = gts_id!("cf.core.application_manager.application.v1~");

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gts_tests.rs"]
mod tests;
