#![allow(clippy::unwrap_used, clippy::expect_used)]

use application_manager_sdk::APPLICATION_RESOURCE_TYPE;

use crate::domain::actions::Action;

#[test]
fn inventory_holds_one_permission_per_action() {
    let instances = toolkit_gts::all_inventory_instances().unwrap();
    for action in Action::ALL {
        let id =
            format!("gts.cf.toolkit.authz.permission.v1~cf.core.application_manager.{action}.v1");
        let found: Vec<_> = instances
            .iter()
            .filter(|i| i["id"] == id.as_str())
            .collect();
        assert_eq!(found.len(), 1, "{id}: {found:?}");
        assert_eq!(found[0]["resource_type"], APPLICATION_RESOURCE_TYPE);
        assert_eq!(found[0]["action"], action.as_str());
    }
}
