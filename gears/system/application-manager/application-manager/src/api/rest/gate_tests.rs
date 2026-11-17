#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::domain::plugin_set::PluginSet;

fn ready() -> Arc<StartupState> {
    let startup = Arc::new(StartupState::default());
    startup.publish(PluginSet::new(vec![])).unwrap();
    startup
}

#[test]
fn not_ready_until_startup_publishes() {
    let gate = RequestGate::new(Arc::new(StartupState::default()), 1);
    assert_eq!(gate.admit().err(), Some(DomainError::NotReady));
}

#[test]
fn a_ready_gate_admits_up_to_its_permits_and_releases_them_on_drop() {
    let gate = RequestGate::new(ready(), 2);
    let first = gate.admit().unwrap();
    let _second = gate.admit().unwrap();
    assert_eq!(gate.admit().err(), Some(DomainError::AtCapacity));
    drop(first);
    assert!(gate.admit().is_ok(), "a dropped permit is free again");
}

#[test]
fn a_stopping_gate_is_not_ready() {
    let gate = RequestGate::new(ready(), 2);
    gate.stop();
    assert_eq!(gate.admit().err(), Some(DomainError::NotReady));
}
