#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::oneshot;

use super::*;

const LIMIT: Duration = Duration::from_secs(10);
const PLUGIN: &str = "plugin-a";

/// Sets its flag when dropped, so a test can see the call was dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn returns_the_plugin_answer_unchanged() {
    let guard = CallGuard::new(LIMIT, 1);
    assert_eq!(guard.call(PLUGIN, async { 7 }).await, Ok(7));
    let err: Result<(), &str> = Err("plugin error");
    assert_eq!(
        guard.call(PLUGIN, async { err }).await,
        Ok(Err("plugin error"))
    );
}

#[tokio::test(start_paused = true)]
async fn drops_the_call_at_the_time_limit_and_releases_the_permit() {
    let guard = CallGuard::new(LIMIT, 1);
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = DropFlag(Arc::clone(&dropped));
    let started = tokio::time::Instant::now();
    let result = guard
        .call(PLUGIN, async move {
            let _flag = flag;
            std::future::pending::<()>().await;
        })
        .await;
    assert_eq!(result, Err(GuardError::TimedOut));
    assert_eq!(started.elapsed(), LIMIT);
    assert!(
        dropped.load(Ordering::SeqCst),
        "the timed-out call must be dropped"
    );
    assert_eq!(
        guard.call(PLUGIN, async { 1 }).await,
        Ok(1),
        "permit released after timeout"
    );
}

#[tokio::test(start_paused = true)]
async fn answer_just_before_the_limit_is_kept() {
    let guard = CallGuard::new(LIMIT, 1);
    let result = guard
        .call(PLUGIN, async {
            tokio::time::sleep(LIMIT.checked_sub(Duration::from_millis(1)).unwrap()).await;
            "late"
        })
        .await;
    assert_eq!(result, Ok("late"));
}

#[tokio::test(start_paused = true)]
async fn refuses_without_calling_while_the_cap_is_held_by_running_calls() {
    let guard = CallGuard::new(LIMIT, 2);
    let (release_a, wait_a) = oneshot::channel::<()>();
    let (release_b, wait_b) = oneshot::channel::<()>();
    let refused_calls = AtomicUsize::new(0);
    let (guard_ref, refused_ref) = (&guard, &refused_calls);

    let running_a = guard.call(PLUGIN, async move { wait_a.await.unwrap() });
    let running_b = guard.call(PLUGIN, async move { wait_b.await.unwrap() });
    let third = async move {
        let refused = guard_ref
            .call(PLUGIN, async move {
                refused_ref.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        let other_plugin = guard_ref.call("plugin-b", async { 2 }).await;
        release_a.send(()).unwrap();
        release_b.send(()).unwrap();
        (refused, other_plugin)
    };

    let (a, b, (refused, other_plugin)) = tokio::join!(running_a, running_b, third);
    assert_eq!((a, b), (Ok(()), Ok(())));
    assert_eq!(refused, Err(GuardError::NotSent));
    assert_eq!(refused_calls.load(Ordering::SeqCst), 0, "plugin not called");
    assert_eq!(other_plugin, Ok(2), "the cap is per plugin");
    assert_eq!(
        guard.call(PLUGIN, async { 3 }).await,
        Ok(3),
        "permits released after completion"
    );
}

#[tokio::test(start_paused = true)]
async fn releases_the_permit_after_completion() {
    let guard = CallGuard::new(LIMIT, 1);
    for _ in 0..3 {
        assert_eq!(guard.call(PLUGIN, async { 1 }).await, Ok(1));
    }
}

#[tokio::test(start_paused = true)]
async fn turns_a_panic_into_an_internal_error() {
    let guard = CallGuard::new(LIMIT, 1);
    let literal = guard
        .call(PLUGIN, async {
            panic!("plugin bug");
        })
        .await;
    assert_eq!(literal, Err(GuardError::Panicked("plugin bug".to_owned())));

    let code = 7;
    let formatted = guard
        .call(PLUGIN, async move {
            panic!("plugin bug {code}");
        })
        .await;
    assert_eq!(
        formatted,
        Err(GuardError::Panicked("plugin bug 7".to_owned()))
    );

    let opaque = guard
        .call(PLUGIN, async {
            std::panic::panic_any(42_u32);
        })
        .await;
    assert_eq!(
        opaque,
        Err(GuardError::Panicked("unknown panic payload".to_owned()))
    );

    assert_eq!(
        guard.call(PLUGIN, async { 1 }).await,
        Ok(1),
        "permit released after panic"
    );
}

#[tokio::test(start_paused = true)]
async fn admits_exactly_the_configured_number_of_calls() {
    let guard = CallGuard::new(LIMIT, 3);
    let (releases, waits): (Vec<_>, Vec<_>) = (0..3).map(|_| oneshot::channel::<()>()).unzip();
    let held = futures::future::join_all(
        waits
            .into_iter()
            .map(|wait| guard.call(PLUGIN, async move { wait.await.unwrap() })),
    );
    let guard_ref = &guard;
    let fourth = async move {
        let refused = guard_ref.call(PLUGIN, async {}).await;
        for release in releases {
            release.send(()).unwrap();
        }
        refused
    };
    let (held, refused) = tokio::join!(held, fourth);
    assert_eq!(held, [Ok(()), Ok(()), Ok(())]);
    assert_eq!(refused, Err(GuardError::NotSent));
}
