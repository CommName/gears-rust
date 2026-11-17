//! GTS declarations for the Application Manager SDK.

use toolkit::gts::PluginV1;
use toolkit_gts::{gts_id, gts_type_schema};

/// Abstract base of every section type.
///
/// Each section plugin registers, in its own `init`, a type derived directly
/// from this base. The derived type's schema is the section's content rules as
/// JSON Schema Draft-07, and the gear reads it once at `start`.
///
/// The section key is the derived segment without its version and trailing
/// `~`:
///
/// ```text
/// gts.cf.core.application_manager.section.v1~acme.iam.access.roles.v1.2~  ->  acme.iam.access.roles
/// ```
///
/// Two versions of one section type therefore share a key.
///
/// The base is an open object with no member of its own: the derived type
/// inlines it under `allOf`, so a closed base would refuse every member a
/// section declares. `#[gts_type_schema]` closes every base it generates, so
/// the schema is written here and submitted to the link-time inventory.
pub const SECTION_BASE_TYPE: &str = gts_id!("cf.core.application_manager.section.v1~");

/// The schema of [`SECTION_BASE_TYPE`]: an abstract, open object.
#[must_use]
pub fn section_base_type_schema() -> serde_json::Value {
    serde_json::json!({
        "$id": format!("gts://{SECTION_BASE_TYPE}"),
        "$schema": "http://json-schema.org/draft-07/schema#",
        "description": "Application Manager section base type",
        "type": "object",
        "x-gts-abstract": true
    })
}

toolkit_gts::inventory::submit! {
    toolkit_gts::InventoryTypeSchema {
        type_id: SECTION_BASE_TYPE,
        schema_fn: || section_base_type_schema().to_string(),
    }
}

/// GTS type definition for section plugin instances.
///
/// Each section plugin registers an instance of this type in its own `init`.
/// Its priority is the instance `priority`, unique across plugins, and its
/// `section_type` is the GTS id of the exact version of its derived section
/// type.
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
///             "acme.iam.access_section_plugin.v1",
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
    properties = "section_type",
)]
pub struct ApplicationManagerSectionPluginSpecV1 {
    /// GTS id of the exact version of the plugin's derived section type.
    #[schemars(extend("x-gts-type-ref" = "gts.cf.core.application_manager.section.v1~*"))]
    pub section_type: String,
}

/// GTS resource type of an application, the PDP resource of ADR 0004.
pub const APPLICATION_RESOURCE_TYPE: &str = gts_id!("cf.core.application_manager.application.v1~");

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "gts_tests.rs"]
mod tests;
