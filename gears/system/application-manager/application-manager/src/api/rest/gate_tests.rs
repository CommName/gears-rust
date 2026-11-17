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

#[tokio::test]
async fn drain_returns_at_once_with_nothing_in_flight() {
    let gate = RequestGate::new(ready(), 2);
    gate.stop();
    assert!(gate.drain(&CancellationToken::new()).await);
}

#[tokio::test]
async fn drain_waits_for_the_last_request_task() {
    let gate = Arc::new(RequestGate::new(ready(), 2));
    let permit = gate.admit().unwrap();
    gate.stop();
    let draining = tokio::spawn({
        let gate = Arc::clone(&gate);
        async move { gate.drain(&CancellationToken::new()).await }
    });
    tokio::task::yield_now().await;
    assert!(!draining.is_finished(), "a request task is still running");
    drop(permit);
    assert!(draining.await.unwrap());
}

#[tokio::test]
async fn drain_gives_up_at_the_deadline() {
    let gate = RequestGate::new(ready(), 2);
    let _permit = gate.admit().unwrap();
    gate.stop();
    let deadline = CancellationToken::new();
    deadline.cancel();
    assert!(!gate.drain(&deadline).await);
}
