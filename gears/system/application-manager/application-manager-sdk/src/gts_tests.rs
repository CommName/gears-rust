//! Tests for the GTS type ids and authz names (DESIGN §3.1, §3.3 interface tables).
//!
//! Planned API: `ApplicationManagerSectionPluginSpecV1`,
//! `ApplicationManagerAuditPluginSpecV1`, `OUTCOME_REPORT_TYPE_ID`,
//! `APPLICATION_RESOURCE_TYPE`, and the `ACTION_*` constants.

use crate::gts::{
    ACTION_LIST, ACTION_READ_SCHEMA, ACTION_SEED, ACTION_UNINSTALL, APPLICATION_RESOURCE_TYPE,
    ApplicationManagerAuditPluginSpecV1, ApplicationManagerSectionPluginSpecV1,
    OUTCOME_REPORT_TYPE_ID,
};
use toolkit_gts::GtsInstanceId;

const PLUGIN_BASE: &str = "gts.cf.toolkit.plugins.plugin.v1~";

// DESIGN §3.3 section-plugin-api: instance ids derive from
// `<PLUGIN_BASE>cf.core.application_manager_section.plugin.v1~`.
#[test]
fn section_plugin_instance_id_uses_design_type_id() {
    let id =
        ApplicationManagerSectionPluginSpecV1::gts_make_instance_id("acme.billing.core.section.v1");
    assert_eq!(
        id.as_ref() as &str,
        format!(
            "{PLUGIN_BASE}cf.core.application_manager_section.plugin.v1~acme.billing.core.section.v1"
        )
    );
    GtsInstanceId::try_new(id.as_ref() as &str).expect("a registrable GTS instance id");
}

// DESIGN §3.3 audit-plugin-api: instance ids derive from
// `<PLUGIN_BASE>cf.core.application_manager_audit.plugin.v1~`.
#[test]
fn audit_plugin_instance_id_uses_design_type_id() {
    let id = ApplicationManagerAuditPluginSpecV1::gts_make_instance_id("acme.pkg.audit.plugin.v1");
    assert_eq!(
        id.as_ref() as &str,
        format!(
            "{PLUGIN_BASE}cf.core.application_manager_audit.plugin.v1~acme.pkg.audit.plugin.v1"
        )
    );
    GtsInstanceId::try_new(id.as_ref() as &str).expect("a registrable GTS instance id");
}

// DESIGN §3.1: the outcome report GTS type id.
#[test]
fn outcome_report_type_id_is_exact() {
    assert_eq!(
        OUTCOME_REPORT_TYPE_ID,
        "gts.cf.core.application_manager.outcome_report.v1~"
    );
}

// DESIGN §3.3 authorization: the PolicyEnforcer resource type.
#[test]
fn application_resource_type_is_exact() {
    assert_eq!(
        APPLICATION_RESOURCE_TYPE,
        "gts.cf.core.application_manager.application.v1~"
    );
}

// DESIGN §3.3 authorization: the four action names the PDP policy matches on.
#[test]
fn authz_action_names_are_exact() {
    assert_eq!(ACTION_SEED, "seed");
    assert_eq!(ACTION_UNINSTALL, "uninstall");
    assert_eq!(ACTION_LIST, "list");
    assert_eq!(ACTION_READ_SCHEMA, "read_schema");
}
