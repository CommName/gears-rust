#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use application_manager_sdk::{OperationAction, OutcomeReport, ReportApplication, RequestCategory};
use serde_json::json;
use time::OffsetDateTime;
use uuid::Uuid;

use super::*;
use crate::domain::history::{HistoryEvent, HistoryKind};
use crate::domain::operation::{NewOperation, OperationStatus};
use crate::test_support::{Hook, HookedRepo, RecordedSignals};

const FAST: Intervals = Intervals {
    sweep: Duration::from_millis(10),
    purge: Duration::from_millis(10),
};

fn report(n: u128, app: &str) -> OutcomeReport {
    OutcomeReport {
        operation_id: Uuid::from_u128(n),
        action: OperationAction::Seed,
        category: None,
        application: ReportApplication {
            id: app.to_owned(),
            version: Some("1".to_owned()),
        },
        sections: vec![],
        problems: vec![],
        unavailable: vec![],
    }
}

/// Accept seed `n` of `app` with run window `window`.
async fn accept(repo: &HookedRepo, n: u128, app: &str, window: Duration) {
    let new = NewOperation {
        operation_id: Uuid::from_u128(n),
        application_id: app.to_owned(),
        action: OperationAction::Seed,
        run_window: window,
        finished_event_id: Uuid::from_u128(n + 0x2000),
        report: report(n, app),
    };
    let accepted = HistoryEvent {
        event_id: Uuid::from_u128(n + 0x1000),
        kind: HistoryKind::Accepted,
        correlation_id: Uuid::from_u128(n),
        action: OperationAction::Seed,
        written_at: OffsetDateTime::UNIX_EPOCH,
        content: json!({}),
    };
    repo.accept(new, accepted).await.expect("accept");
}

/// Wait until `signals` recorded `line` at least `times` times.
async fn until(signals: &RecordedSignals, line: &str, times: usize) {
    for _ in 0..500 {
        if signals.count(line) >= times {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{line} not recorded {times} times: {:?}", signals.lines());
}

fn spawn(repo: &Arc<HookedRepo>, retention: Duration, signals: &Arc<RecordedSignals>) -> Sweeper {
    Sweeper::spawn(
        Arc::clone(repo) as Arc<dyn OperationRepository>,
        retention,
        Arc::clone(signals) as Arc<dyn Signals>,
        FAST,
        CancellationToken::new(),
    )
}

#[test]
fn the_default_intervals_are_the_design_constants() {
    assert_eq!(
        Intervals::default(),
        Intervals {
            sweep: SWEEP_INTERVAL,
            purge: PURGE_INTERVAL
        }
    );
}

#[tokio::test]
async fn an_expired_open_operation_is_completed_and_counted() {
    let repo = HookedRepo::new().await;
    accept(&repo, 1, "expired.app", Duration::ZERO).await;
    accept(&repo, 2, "open.app", Duration::from_secs(3600)).await;
    let signals = RecordedSignals::new();
    let sweeper = spawn(&repo, Duration::from_secs(3600), &signals);

    until(&signals, "expired:1", 1).await;
    sweeper.stop(&CancellationToken::new()).await;

    let expired = repo
        .read_operation(Uuid::from_u128(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired.status, OperationStatus::Completed);
    let open = repo
        .read_operation(Uuid::from_u128(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        open.status,
        OperationStatus::Accepted,
        "an open operation is kept"
    );
}

#[tokio::test]
async fn the_purge_deletes_completed_operations_only() {
    let repo = HookedRepo::new().await;
    accept(&repo, 1, "done.app", Duration::from_secs(3600)).await;
    repo.mark_running(Uuid::from_u128(1), &[]).await.unwrap();
    repo.finish(
        Uuid::from_u128(1),
        &report(1, "done.app"),
        RequestCategory::Succeeded,
    )
    .await
    .unwrap();
    accept(&repo, 2, "open.app", Duration::from_secs(3600)).await;
    let signals = RecordedSignals::new();
    let sweeper = spawn(&repo, Duration::ZERO, &signals);

    until(&signals, "sweep:purge,true", 2).await;
    sweeper.stop(&CancellationToken::new()).await;

    assert_eq!(repo.read_operation(Uuid::from_u128(1)).await, Ok(None));
    assert!(
        repo.read_operation(Uuid::from_u128(2))
            .await
            .unwrap()
            .is_some(),
        "an open operation is never purged"
    );
    assert_eq!(
        repo.events(Uuid::from_u128(1)).await.len(),
        2,
        "the history is kept"
    );
    assert_eq!(
        repo.applications_page(None, None, 10).await.unwrap().len(),
        1,
        "the application record is kept"
    );
}

#[tokio::test]
async fn a_failing_step_is_counted_and_runs_again() {
    let repo = HookedRepo::new().await;
    repo.hook("expire_batch", Hook::Fail);
    repo.hook("purge", Hook::Fail);
    let signals = RecordedSignals::new();
    let sweeper = spawn(&repo, Duration::ZERO, &signals);

    until(&signals, "sweep:expire,false", 2).await;
    until(&signals, "sweep:purge,false", 2).await;
    sweeper.stop(&CancellationToken::new()).await;
    assert!(signals.count("unavailable:storage") >= 4);
}

#[tokio::test]
async fn stop_ends_the_loop() {
    let repo = HookedRepo::new().await;
    let signals = RecordedSignals::new();
    let sweeper = spawn(&repo, Duration::ZERO, &signals);
    until(&signals, "sweep:expire,true", 1).await;

    sweeper.stop(&CancellationToken::new()).await;
    let calls = repo.calls("expire_batch");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(repo.calls("expire_batch"), calls, "no step after stop");
}

#[tokio::test]
async fn stop_aborts_a_hung_step_at_the_deadline() {
    let repo = HookedRepo::new().await;
    repo.hook("expire_batch", Hook::Sleep(Duration::from_secs(3600)));
    let signals = RecordedSignals::new();
    let sweeper = spawn(&repo, Duration::ZERO, &signals);
    for _ in 0..500 {
        if repo.calls("expire_batch") > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let deadline = CancellationToken::new();
    deadline.cancel();
    tokio::time::timeout(Duration::from_secs(5), sweeper.stop(&deadline))
        .await
        .expect("stop returns at the deadline");
}
