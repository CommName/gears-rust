#![allow(clippy::unwrap_used, clippy::expect_used)]

use toolkit::HealthcheckStatus;

use super::*;
use crate::domain::plugin_set::PluginSet;

#[tokio::test]
async fn not_ready_until_the_plugin_set_is_published() {
    let startup = Arc::new(StartupState::default());
    let check = StartupHealthcheck::new(Arc::clone(&startup));
    assert_eq!(check.name(), "application-manager-startup");

    let before = check.check().await;
    assert_eq!(before.status, HealthcheckStatus::Unhealthy);
    assert_eq!(before.code.as_deref(), Some("not_ready"));

    // An empty plugin set is a valid startup result.
    startup.publish(PluginSet::default()).unwrap();
    let after = check.check().await;
    assert_eq!(after.status, HealthcheckStatus::Healthy);
    assert!(after.code.is_none());
}
