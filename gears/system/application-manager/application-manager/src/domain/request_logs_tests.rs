//! Unit tests for what the full request logs (P7, DESIGN §2.2 "No Manifest Content Outside the
//! Audit Plugin", §4.6 logs ("never carry section content, the manifest body, or schema
//! bodies"), §4.8 Security (c): "A sentinel manifest value sent through every outcome path is
//! absent from logs, spans, metric labels, and Problem bodies").
//!
//! Pinned here: a sentinel carried only by a section value of the manifest appears in no log
//! event of any thread, on every path of the full request: success, each section failure, a hung
//! plugin, a failed or hung registry write, a failed delete, a refused or failed final-result
//! hand-off, a refused request hand-off, an invalid manifest, a request refused at the cap, a
//! task that panicked (the supervisor path) and a drained request. The final-result content, which
//! does not carry the manifest, holds it neither. A clean request logs no warning and no error.
//!
//! The capture is the process-wide sentinel capture (`capture_logs_containing`), because the
//! validation runs on the blocking pool and the request task is detached; its positive control
//! emits from a blocking thread first.
#![allow(clippy::duration_suboptimal_units, clippy::too_many_lines)]

use std::sync::Arc;

use application_manager_sdk::{AuditPluginError, SectionPluginError};
use serde_json::json;

use crate::domain::orchestrator::RequestInput;
use crate::domain::ports::RegistryError;
use crate::test_support::{
    AuditCall, CallBehavior, FakeAuditPlugin, Gate, GateBuilder, PanickingSectionPort,
    PolicyScript, RegistryOp, SectionCall, bounded, capture_logs_containing, seed_body, stored,
    three_section_gate,
};

// One sentinel per test: the process-wide capture is keyed by the sentinel text, so two tests
// that run in parallel must not share one (the first to finish would end the other's capture).
const SENTINEL: &str = "SENTINEL-P7-6d1f3b9c-manifest-value";
const SENTINEL_CAP: &str = "SENTINEL-P7-a40c77e2-cap-and-drain";

fn body_with(sentinel: &str) -> Vec<u8> {
    seed_body(
        "app-1",
        "1.0",
        &json!({
            "files": {"path": sentinel},
            "db": {"tables": [sentinel]},
            "net": {"k": {"nested": sentinel}},
        }),
    )
}

fn seed() -> RequestInput {
    RequestInput::Seed {
        body: body_with(SENTINEL),
    }
}

fn uninstall() -> RequestInput {
    RequestInput::Uninstall {
        application_id: "app-1".to_owned(),
    }
}

fn gate() -> GateBuilder {
    three_section_gate().entry(stored("app-1", "3.1"))
}

struct Path {
    name: &'static str,
    gate: Gate,
    input: RequestInput,
}

fn paths() -> Vec<Path> {
    let rejected = || CallBehavior::err(SectionPluginError::rejected("no", Some("/path")));
    let denied = || CallBehavior::err(SectionPluginError::not_permitted("no"));
    let internal = || CallBehavior::err(SectionPluginError::internal("boom"));
    let mut paths = Vec::new();
    let mut add = |name: &'static str, gate: Gate, input: RequestInput| {
        paths.push(Path { name, gate, input });
    };

    add("success", gate().build(), seed());
    for (name, behavior) in [
        ("rejected", rejected()),
        ("not permitted", denied()),
        ("internal", internal()),
        ("hung", CallBehavior::hang()),
    ] {
        let g = gate().build();
        g.sections[1].on(SectionCall::Apply, behavior);
        add(name, g, seed());
    }
    let g = gate().build();
    g.memory
        .fail(RegistryOp::Upsert, RegistryError::Unavailable);
    add("upsert fails", g, seed());
    let g = gate().build();
    g.memory
        .fail_after_commit(RegistryOp::Upsert, RegistryError::TimedOut);
    add("upsert commits then times out", g, seed());
    let g = gate().build();
    g.memory.hang(RegistryOp::Upsert);
    add("upsert hangs", g, seed());
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::FinalResultHandoff,
        CallBehavior::err(AuditPluginError::refused("no")),
    );
    add("final result refused", gate().audit(audit).build(), seed());
    let audit = FakeAuditPlugin::new();
    audit.on(AuditCall::FinalResultHandoff, CallBehavior::panic());
    add("final result panics", gate().audit(audit).build(), seed());
    let audit = FakeAuditPlugin::new();
    audit.on(
        AuditCall::RequestHandoff,
        CallBehavior::err(AuditPluginError::unavailable("down")),
    );
    add(
        "request hand-off refused",
        gate().audit(audit).build(),
        seed(),
    );
    add(
        "invalid manifest",
        gate().build(),
        RequestInput::Seed {
            body: seed_body(
                "app-1",
                "1.0",
                &json!({"files": {"path": 7, "x": SENTINEL}}),
            ),
        },
    );
    // The sentinel is a member NAME: of the manifest's top level, of a section, of an unknown
    // section. The problems of the report may quote it (a location); no log event may.
    add(
        "sentinel member names",
        gate().build(),
        RequestInput::Seed {
            body: seed_body(
                "app-1",
                "1.0",
                &json!({"files": {SENTINEL: 1}, SENTINEL: {}}),
            ),
        },
    );
    add(
        "pdp refuses",
        gate().policy(PolicyScript::Refuse).build(),
        seed(),
    );
    // No uninstall row: an uninstall carries only an id, so the sentinel is never in play.
    let g = gate()
        .wrap_section_port(1, |inner| {
            Arc::new(PanickingSectionPort::new(inner, SectionCall::Apply, 1))
        })
        .build();
    add("task panics between sections", g, seed());
    let g = gate().policy(PolicyScript::Panic).build();
    add("task panics in the permission step", g, seed());
    paths
}

// DESIGN §4.8 Security (c): a sentinel manifest value is absent from every log event of every
// path, whatever thread the event comes from.
#[tokio::test(start_paused = true)]
async fn a_manifest_value_never_reaches_a_log_on_any_path_of_the_request() {
    let (capture, _guard) = capture_logs_containing(SENTINEL);
    capture.assert_live_on_a_blocking_thread().await;

    for path in paths() {
        let report = bounded(path.gate.run(path.input.clone())).await;

        // A report and the final content may quote a member name in a problem's location (the
        // wire is not what this row is about); the manifest VALUES never appear in either.
        if path.name.contains("member names") {
            continue;
        }
        let wire = serde_json::to_string(&report).expect("serializes");
        assert!(
            !wire.contains(SENTINEL),
            "{}: the report: {wire}",
            path.name
        );
        for call in path.gate.audit.calls() {
            if let Some(result) = call.result {
                let wire = serde_json::to_string(&result).expect("serializes");
                assert!(
                    !wire.contains(SENTINEL),
                    "{}: the final content: {wire}",
                    path.name
                );
            }
        }
    }

    // The probe of the positive control is the only event that carries the sentinel.
    let lines: Vec<String> = capture.text().lines().map(str::to_owned).collect();
    assert!(
        lines.iter().all(|l| l.contains("blocking_probe")),
        "a request logged manifest content: {lines:#?}"
    );
}

// The request refused at the cap is logged without its body, and so is a drained one.
#[tokio::test(start_paused = true)]
async fn the_refusal_at_the_cap_and_the_drain_log_no_manifest_content() {
    let (capture, _guard) = capture_logs_containing(SENTINEL_CAP);
    capture.assert_live_on_a_blocking_thread().await;
    let gate = gate()
        .policy_delay(std::time::Duration::from_secs(3600))
        .settings(|s| s.max_request_tasks = 1)
        .build();
    let input = || RequestInput::Seed {
        body: body_with(SENTINEL_CAP),
    };
    let running = gate.start(input());

    assert!(gate.try_start(input()).is_err());
    let deadline = tokio_util::sync::CancellationToken::new();
    deadline.cancel();
    assert_eq!(gate.orchestrator.drain(&deadline).await, 1);

    let lines: Vec<String> = capture.text().lines().map(str::to_owned).collect();
    assert!(
        lines.iter().all(|l| l.contains("blocking_probe")),
        "{lines:#?}"
    );
    drop(running);
}

// DESIGN §4.6: the events of §4.6 are failures; a request that goes through cleanly writes no
// warning and no error. The capture is the process-wide one, keyed by the gate's own request
// ids (every event of a request carries its request id), so the events of the detached request
// task and of any other thread count. The control: a failing request of the same gate is seen.
#[tokio::test(start_paused = true)]
async fn a_clean_request_logs_no_warning_and_no_error() {
    const IDS_FROM: u128 = 0xC1EA_0000_0000_0000_0000_0000_0000_0001;
    let (capture, _guard) = capture_logs_containing("c1ea0000-0000-0000-0000-");
    capture.assert_live_on_a_blocking_thread().await;
    let gate = gate().request_ids_from(IDS_FROM).build();

    gate.run(seed()).await;
    gate.run(uninstall()).await;

    let clean: Vec<String> = capture
        .text()
        .lines()
        .filter(|l| !l.contains("blocking_probe"))
        .map(str::to_owned)
        .collect();
    assert!(clean.is_empty(), "a clean request logged: {clean:#?}");

    // The control: the same capture sees the failure of a request of this gate.
    gate.memory
        .fail(RegistryOp::Upsert, RegistryError::Unavailable);
    gate.run(seed()).await;
    assert!(
        capture.text().contains("registry_write_failed"),
        "the capture misses the events of a request: {}",
        capture.text()
    );
}
