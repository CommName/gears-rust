#![allow(clippy::unwrap_used, clippy::expect_used)]

use application_manager_sdk::APPLICATION_RESOURCE_TYPE;

use crate::domain::actions::TenantAction;

const PREFIX: &str = "gts.cf.toolkit.authz.permission.v1~cf.core.application_manager.";

#[test]
fn inventory_holds_exactly_one_permission_per_tenant_action() {
    let instances = toolkit_gts::all_inventory_instances().unwrap();
    let ours: Vec<_> = instances
        .iter()
        .filter(|i| i["id"].as_str().is_some_and(|id| id.starts_with(PREFIX)))
        .collect();
    assert_eq!(ours.len(), TenantAction::ALL.len(), "{ours:?}");
    for action in TenantAction::ALL {
        let id = format!("{PREFIX}{action}.v1");
        let found: Vec<_> = ours.iter().filter(|i| i["id"] == id.as_str()).collect();
        assert_eq!(found.len(), 1, "{id}: {found:?}");
        assert_eq!(found[0]["resource_type"], APPLICATION_RESOURCE_TYPE);
        assert_eq!(found[0]["action"], action.as_str());
    }
}
