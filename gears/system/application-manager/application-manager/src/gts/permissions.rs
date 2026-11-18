//! Permission catalog: the two tenant-plane reads on the application resource
//! as well-known [`AuthzPermissionV1`] instances (DESIGN 3.3, ADR 0004).
//!
//! The platform-plane actions are decided by the allowed callers, not the PDP,
//! so they have no permission here. Each [`gts_instance!`] submits to the
//! `toolkit-gts` inventory, which types-registry collects at boot. `action`
//! values are the [`TenantAction`] names the access-control adapter sends, so
//! the catalog cannot drift from what the routes enforce.
//!
//! Instance id layout:
//! `gts.cf.toolkit.authz.permission.v1~cf.core.application_manager.<action>.v1`.

use application_manager_sdk::APPLICATION_RESOURCE_TYPE;
use toolkit_gts::{AuthzPermissionV1, gts_instance};

use crate::domain::actions::TenantAction;

gts_instance! {
    AuthzPermissionV1 {
        id: gts_id!("cf.toolkit.authz.permission.v1~cf.core.application_manager.read_history.v1"),
        resource_type: APPLICATION_RESOURCE_TYPE.to_owned(),
        action: TenantAction::ReadHistory.as_str().to_owned(),
        display_name: "Read the application manager operation history".to_owned(),
    }
}
gts_instance! {
    AuthzPermissionV1 {
        id: gts_id!("cf.toolkit.authz.permission.v1~cf.core.application_manager.read_applications.v1"),
        resource_type: APPLICATION_RESOURCE_TYPE.to_owned(),
        action: TenantAction::ReadApplications.as_str().to_owned(),
        display_name: "List installed applications".to_owned(),
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "permissions_tests.rs"]
mod tests;
