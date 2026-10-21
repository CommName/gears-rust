//! Permission catalog: the five gear-level actions on the application resource
//! as well-known [`AuthzPermissionV1`] instances (DESIGN 3.3, ADR 0005).
//!
//! Each [`gts_instance!`] submits to the `toolkit-gts` inventory, which
//! types-registry collects at boot. `action` values are the [`Action`] names the
//! access-control adapter sends, so the catalog cannot drift from what the
//! routes enforce.
//!
//! Instance id layout:
//! `gts.cf.toolkit.authz.permission.v1~cf.core.application_manager.<action>.v1`.

use application_manager_sdk::APPLICATION_RESOURCE_TYPE;
use toolkit_gts::{AuthzPermissionV1, gts_instance};

use crate::domain::actions::Action;

gts_instance! {
    AuthzPermissionV1 {
        id: gts_id!("cf.toolkit.authz.permission.v1~cf.core.application_manager.seed.v1"),
        resource_type: APPLICATION_RESOURCE_TYPE.to_owned(),
        action: Action::Seed.as_str().to_owned(),
        display_name: "Seed an application".to_owned(),
    }
}
gts_instance! {
    AuthzPermissionV1 {
        id: gts_id!("cf.toolkit.authz.permission.v1~cf.core.application_manager.uninstall.v1"),
        resource_type: APPLICATION_RESOURCE_TYPE.to_owned(),
        action: Action::Uninstall.as_str().to_owned(),
        display_name: "Uninstall an application".to_owned(),
    }
}
gts_instance! {
    AuthzPermissionV1 {
        id: gts_id!("cf.toolkit.authz.permission.v1~cf.core.application_manager.read_operation.v1"),
        resource_type: APPLICATION_RESOURCE_TYPE.to_owned(),
        action: Action::ReadOperation.as_str().to_owned(),
        display_name: "Read application manager operations".to_owned(),
    }
}
gts_instance! {
    AuthzPermissionV1 {
        id: gts_id!("cf.toolkit.authz.permission.v1~cf.core.application_manager.read_schema.v1"),
        resource_type: APPLICATION_RESOURCE_TYPE.to_owned(),
        action: Action::ReadSchema.as_str().to_owned(),
        display_name: "Read application manifest schemas".to_owned(),
    }
}
gts_instance! {
    AuthzPermissionV1 {
        id: gts_id!("cf.toolkit.authz.permission.v1~cf.core.application_manager.read_audit.v1"),
        resource_type: APPLICATION_RESOURCE_TYPE.to_owned(),
        action: Action::ReadAudit.as_str().to_owned(),
        display_name: "Read application manager audit events".to_owned(),
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "permissions_tests.rs"]
mod tests;
