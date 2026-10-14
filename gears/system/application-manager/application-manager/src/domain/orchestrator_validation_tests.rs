//! Unit tests for the orchestrator's schema phase and whole-manifest validation (DESIGN §3.2
//! "Request Orchestrator" step 5 and "Schema Validator", §3.3 problem shapes, §4.3 hostile
//! schemas and the residual 2^d instance-driven cost, §4.8 "Unit" row: "the problem shapes of
//! §3.3, including the cap with `too_many_problems`, the dedupe and sort").
//!
//! Planned API in `crate::domain::orchestrator` (see `orchestrator_tests.rs` for the rest):
//!
//! ```text
//! /// Joins the envelope limit problems (`EnvelopeFields::limit_problems`) to the
//! /// `validate_manifest` problems into ONE list: identical (location, rule, message) problems
//! /// count once; sorted by (location, rule, message); at most `MAX_VALIDATION_PROBLEMS`; when
//! /// the union has more (or an input already ended in the `too_many_problems` marker), the
//! /// first `MAX_VALIDATION_PROBLEMS` of the sorted union followed by ONE final problem with
//! /// rule `too_many_problems` at location `""`.
//! pub fn join_problems(limit: Vec<Problem>, validation: Vec<Problem>) -> Vec<Problem>;
//!
//! /// Seed step 5 runs `check_schemas` and `validate_manifest` on the blocking pool, off the
//! /// async runtime, under `OrchestratorSettings::step_timeout`. On expiry the request ends as
//! /// `TemporarilyUnavailable` with NO unavailable item (no dependency to name), the hand-off
//! /// stays accepted (audit id kept), the blocking task is left running (abandoned and counted
//! /// in `Orchestrator::abandoned_validations()`), and one WARN `manifest_validation_abandoned`
//! /// event (the request id, no content) is logged. The validation closure makes no tracing
//! /// call.
//! impl Orchestrator { pub fn abandoned_validations(&self) -> u64 }
//!
//! /// F-003: a slot of `OrchestratorSettings::max_validations` (`max_request_tasks`) is taken
//! /// before the task is spawned and moves into the closure, so it is released when the work
//! /// ends (a validation abandoned while running keeps its slot). At the bound the request is
//! /// refused at once: `TemporarilyUnavailable`, no item, one WARN
//! /// `manifest_validation_at_capacity` (request id), counted in
//! /// `validations_refused_at_capacity()`. At the step timeout the task is aborted, so a
//! /// validation still queued on the pool never runs.
//! /// F-004: a validation that panics is one WARN `manifest_validation_failed`
//! /// (request id, `panicked`), the same outcome, and is not counted as abandoned.
//!
//! /// The test seam: the validation that runs on the blocking pool is replaceable, so a test
//! /// holds a validation open on a latch it controls instead of timing a heavy schema.
//! pub(crate) type ValidationFn = dyn Fn(&[SectionSchema], Option<&Value>)
//!     -> Result<Vec<Problem>, Vec<UnusableSchema>> + Send + Sync;
//! impl Orchestrator { #[cfg(test)] pub(crate) fn with_validation(self, Arc<ValidationFn>) -> Self }
//! ```
//!
//! The paused tokio clock does not advance while a blocking task runs (tokio test-util), so the
//! step-timeout tests run on the real clock. They never depend on how fast a CPU validates: a
//! test-held latch keeps the validation open, the step timeout is short, and the assertions are
//! about ORDER (the request returned while the validation was still held), never about elapsed
//! time. One test runs the real "two applicators per level" schema (`n` reaches the recursion
//! through `properties` and through `allOf`; a VALID nested instance costs 2^d steps, DESIGN
//! §4.3) at a depth every machine finishes, with a generous step timeout.
#![allow(clippy::duration_suboptimal_units, clippy::type_complexity)]

use std::time::Duration;

use application_manager_sdk::models::{OutcomeCategory, Problem};
use serde_json::{Value, json};

use crate::domain::limits::{MAX_JSON_DEPTH, MAX_VALIDATION_PROBLEMS};
use crate::domain::orchestrator::{GateOutcome, join_problems};
use crate::test_support::{
    CallBehavior, GateBuilder, MAX_ENVELOPE_LEN, SectionCall, ValidationLatch, capture_logs,
    decided, field_value, seed_body,
};

fn p(location: &str, rule: &str, message: &str) -> Problem {
    Problem {
        location: location.to_owned(),
        rule: rule.to_owned(),
        message: message.to_owned(),
    }
}

fn marker() -> Problem {
    p("", "too_many_problems", "m")
}

fn is_marker(problem: &Problem) -> bool {
    problem.rule == "too_many_problems" && problem.location.is_empty()
}

fn numbered(n: usize, location: &str, rule: &str) -> Vec<Problem> {
    (0..n)
        .map(|i| p(location, rule, &format!("message {i:03}")))
        .collect()
}

// ---- join_problems: dedupe, sort, cap, marker ---------------------------------------------------

#[test]
fn joining_nothing_gives_nothing() {
    assert_eq!(join_problems(vec![], vec![]), Vec::<Problem>::new());
}

#[test]
fn the_union_is_sorted_by_location_then_rule_then_message_across_both_lists() {
    let limit = vec![
        p("/application/version", "max_length", "b"),
        p("/application/id", "minLength", "z"),
    ];
    let validation = vec![
        p("/sections/b", "type", "x"),
        p("/application/id", "minLength", "a"),
        p("/application/id", "control_character", "z"),
        p("/sections/a", "required", "x"),
    ];

    let joined = join_problems(limit, validation);

    let keys: Vec<(&str, &str, &str)> = joined
        .iter()
        .map(|p| (p.location.as_str(), p.rule.as_str(), p.message.as_str()))
        .collect();
    assert_eq!(
        keys,
        vec![
            ("/application/id", "control_character", "z"),
            ("/application/id", "minLength", "a"),
            ("/application/id", "minLength", "z"),
            ("/application/version", "max_length", "b"),
            ("/sections/a", "required", "x"),
            ("/sections/b", "type", "x"),
        ]
    );
}

#[test]
fn identical_problems_count_once_across_and_within_the_lists() {
    let same = p("/application/id", "minLength", "id must not be empty");
    let limit = vec![same.clone(), same.clone()];
    let validation = vec![
        same.clone(),
        p("/sections/a", "type", "t"),
        p("/sections/a", "type", "t"),
    ];

    let joined = join_problems(limit, validation);

    assert_eq!(joined, vec![same, p("/sections/a", "type", "t")]);
}

#[test]
fn problems_that_differ_only_in_the_message_are_distinct() {
    let joined = join_problems(vec![p("/x", "r", "one")], vec![p("/x", "r", "two")]);

    assert_eq!(joined.len(), 2);
}

#[test]
fn exactly_the_cap_has_no_marker() {
    let limit = numbered(2, "/a", "r");
    let validation = numbered(MAX_VALIDATION_PROBLEMS - 2, "/b", "r");

    let joined = join_problems(limit, validation);

    assert_eq!(joined.len(), MAX_VALIDATION_PROBLEMS);
    assert!(!joined.iter().any(is_marker));
}

#[test]
fn one_over_the_cap_keeps_the_first_hundred_of_the_sorted_union_and_one_final_marker() {
    let limit = numbered(2, "/application/id", "r");
    let validation = numbered(MAX_VALIDATION_PROBLEMS - 1, "/sections/s", "required"); // 99

    let joined = join_problems(limit.clone(), validation.clone());

    assert_eq!(joined.len(), MAX_VALIDATION_PROBLEMS + 1);
    let (kept, last) = joined.split_at(MAX_VALIDATION_PROBLEMS);
    assert!(is_marker(&last[0]), "the marker is last: {:?}", last[0]);
    assert!(!kept.iter().any(is_marker), "exactly one marker");
    let mut union: Vec<Problem> = limit.into_iter().chain(validation).collect();
    union.sort_by(|a, b| {
        (&a.location, &a.rule, &a.message).cmp(&(&b.location, &b.rule, &b.message))
    });
    union.truncate(MAX_VALIDATION_PROBLEMS);
    assert_eq!(kept, union.as_slice(), "the first 100 of the sorted union");
}

#[test]
fn the_marker_is_last_although_its_empty_location_sorts_first() {
    let joined = join_problems(vec![], numbered(MAX_VALIDATION_PROBLEMS + 5, "/z", "r"));

    assert_eq!(joined.len(), MAX_VALIDATION_PROBLEMS + 1);
    assert!(is_marker(joined.last().expect("a last problem")));
    assert!(!is_marker(&joined[0]));
}

#[test]
fn an_input_that_already_ends_in_the_marker_yields_one_marker_and_no_more_than_the_cap() {
    let mut validation = numbered(MAX_VALIDATION_PROBLEMS, "/sections/s", "required");
    validation.push(marker());

    let joined = join_problems(vec![], validation);

    assert_eq!(
        joined.len(),
        MAX_VALIDATION_PROBLEMS + 1,
        "100 problems and the marker"
    );
    assert_eq!(joined.iter().filter(|x| is_marker(x)).count(), 1);
    assert!(is_marker(joined.last().expect("a last problem")));
}

#[test]
fn a_limit_problem_joined_to_a_cut_validation_list_pushes_out_the_last_sorted_problem() {
    let mut validation = numbered(MAX_VALIDATION_PROBLEMS, "/sections/s", "required");
    validation.push(marker());
    let limit = vec![p("/application/id", "minLength", "id must not be empty")];

    let joined = join_problems(limit.clone(), validation.clone());

    assert_eq!(joined.len(), MAX_VALIDATION_PROBLEMS + 1);
    assert_eq!(
        joined[0], limit[0],
        "the limit problem sorts first and is kept"
    );
    assert!(joined.contains(&validation[MAX_VALIDATION_PROBLEMS - 2]));
    assert!(
        !joined.contains(&validation[MAX_VALIDATION_PROBLEMS - 1]),
        "the last of the sorted union is the one dropped"
    );
    assert!(is_marker(joined.last().expect("a last problem")));
}

#[test]
fn duplicates_do_not_use_up_the_cap() {
    let distinct = numbered(MAX_VALIDATION_PROBLEMS, "/s", "r");
    let limit: Vec<Problem> = distinct.iter().take(50).cloned().collect();

    let joined = join_problems(limit, distinct.clone());

    assert_eq!(
        joined, distinct,
        "100 distinct problems, 50 of them twice: no marker"
    );
}

// ---- through the gate: the joined list in the 422 -----------------------------------------------

fn files_gate() -> GateBuilder {
    GateBuilder::new().section("files", json!({"type": "object"}))
}

#[tokio::test(start_paused = true)]
async fn envelope_limit_problems_join_the_validation_problems_in_one_sorted_list() {
    let gate = files_gate().build();

    let result = gate.seed(seed_body("", "1", &json!({"files": 5}))).await;

    let r = decided(&result);
    assert_eq!(r.category, OutcomeCategory::InvalidManifest);
    let got: Vec<(&str, &str)> = r
        .problems
        .iter()
        .map(|p| (p.location.as_str(), p.rule.as_str()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("/application/id", "minLength"),
            ("/sections/files", "type")
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_manifest_that_is_valid_except_for_the_envelope_limits_is_invalid_with_those_problems() {
    let gate = files_gate()
        .settings(|s| s.max_envelope_text_length = 5)
        .build();

    let result = gate
        .seed(seed_body("toolong", "v", &json!({"files": {}})))
        .await;

    let r = decided(&result);
    assert_eq!(r.category, OutcomeCategory::InvalidManifest);
    assert_eq!(r.problems.len(), 1, "{:?}", r.problems);
    assert_eq!(r.problems[0].location, "/application/id");
    assert_eq!(r.problems[0].rule, "max_length");
}

#[tokio::test(start_paused = true)]
async fn one_value_can_break_two_envelope_limits_and_both_are_reported() {
    let gate = files_gate().build();
    let id = format!("{}\u{1}", "x".repeat(MAX_ENVELOPE_LEN));

    let result = gate.seed(seed_body(&id, "1", &json!({"files": {}}))).await;

    let rules: Vec<&str> = decided(&result)
        .problems
        .iter()
        .map(|p| p.rule.as_str())
        .collect();
    assert_eq!(
        rules,
        vec!["control_character", "max_length"],
        "sorted by rule at one location"
    );
}

#[tokio::test(start_paused = true)]
async fn the_gate_caps_the_joined_list_and_ends_it_with_one_marker() {
    // 99 `required` problems from the section schema plus 2 envelope limit problems = 101.
    let required: Vec<String> = (0..99).map(|i| format!("r{i:03}")).collect();
    let gate = GateBuilder::new()
        .section("files", json!({"type": "object", "required": required}))
        .build();

    let result = gate
        .seed(seed_body("", "\u{1}", &json!({"files": {}})))
        .await;

    let problems = &decided(&result).problems;
    assert_eq!(
        problems.len(),
        MAX_VALIDATION_PROBLEMS + 1,
        "100 problems and the marker"
    );
    assert_eq!(problems[0].rule, "minLength");
    assert_eq!(problems[1].rule, "control_character");
    assert!(
        problems[2..MAX_VALIDATION_PROBLEMS]
            .iter()
            .all(|p| p.rule == "required")
    );
    assert!(is_marker(&problems[MAX_VALIDATION_PROBLEMS]));
    assert_eq!(problems.iter().filter(|x| is_marker(x)).count(), 1);
    let text = problems
        .iter()
        .map(|p| p.message.clone())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("r097"), "the 98th required problem is kept");
    assert!(!text.contains("r098"), "the 99th (last sorted) is cut");
}

#[tokio::test(start_paused = true)]
async fn the_gate_keeps_one_marker_when_the_validator_already_cut_its_list() {
    let required: Vec<String> = (0..150).map(|i| format!("r{i:03}")).collect();
    let gate = GateBuilder::new()
        .section("files", json!({"type": "object", "required": required}))
        .build();

    let result = gate.seed(seed_body("", "1", &json!({"files": {}}))).await;

    let problems = &decided(&result).problems;
    assert_eq!(problems.len(), MAX_VALIDATION_PROBLEMS + 1);
    assert_eq!(
        problems[0].location, "/application/id",
        "the limit problem sorts first"
    );
    assert_eq!(problems.iter().filter(|x| is_marker(x)).count(), 1);
    assert!(is_marker(problems.last().expect("a last problem")));
}

#[tokio::test(start_paused = true)]
async fn a_body_that_does_not_parse_reports_its_one_parse_problem_and_nothing_else() {
    let deep = "[".repeat(MAX_JSON_DEPTH + 1) + &"]".repeat(MAX_JSON_DEPTH + 1);
    let bodies: Vec<(&str, Vec<u8>, &str, &str)> = vec![
        ("syntax", b"{ nope".to_vec(), "", "malformed_json"),
        ("not utf-8", vec![0xff, 0xfe], "", "malformed_json"),
        ("too deep", deep.into_bytes(), "", "malformed_json"),
        (
            "repeated member",
            br#"{"application": {"id": "a", "id": "b"}, "sections": {}}"#.to_vec(),
            "/application",
            "duplicate_member",
        ),
    ];
    for (name, body, location, rule) in bodies {
        let gate = files_gate().build();

        let result = gate.seed(body).await;

        let r = decided(&result);
        assert_eq!(r.category, OutcomeCategory::InvalidManifest, "{name}");
        assert_eq!(r.problems.len(), 1, "{name}: {:?}", r.problems);
        assert_eq!(r.problems[0].location, location, "{name}");
        assert_eq!(r.problems[0].rule, rule, "{name}");
    }
}

// §3.3 "Safe wire errors": validation messages name locations, rule tokens and schema-side
// constraints, never manifest values.
#[tokio::test(start_paused = true)]
async fn problem_messages_never_carry_manifest_values() {
    let sentinel = "SENTINEL-instance-value-31";
    let gate = files_gate().build();

    let result = gate
        .seed(seed_body(
            "id",
            "1",
            &json!({"files": sentinel, "extra": sentinel}),
        ))
        .await;

    let r = decided(&result);
    assert!(r.problems.len() >= 2);
    for problem in &r.problems {
        assert!(
            !format!("{problem:?}").contains(sentinel),
            "a manifest value in {problem:?}"
        );
    }
}

// ---- no cache: every seed fetches the live schemas -------------------------------------------------

#[tokio::test(start_paused = true)]
async fn each_seed_validates_against_the_schema_the_plugin_serves_at_that_moment() {
    let gate = GateBuilder::new()
        .section("files", json!({"type": "object"}))
        .build();
    let body = seed_body("app", "1", &json!({"files": {"n": 1}}));

    let before = gate.seed(body.clone()).await;
    gate.sections[0].set_schema(json!({"type": "object", "additionalProperties": false}));
    let after = gate.seed(body).await;

    assert!(matches!(before.outcome, GateOutcome::Proceed(_)));
    assert_eq!(decided(&after).category, OutcomeCategory::InvalidManifest);
    assert_eq!(gate.sections[0].call_count(), 2, "one schema call per seed");
}

// ---- off the runtime, under the step timeout (real clock) -------------------------------------------

// The two-applicators-per-level schema: the recursion is reached through `properties` and
// through `allOf`, so a VALID instance nested `levels` deep costs 2^levels steps (DESIGN §4.3).
fn exponential_schema() -> Value {
    json!({"$defs": {"n": {"type": ["object", "null"],
        "properties": {"c": {"$ref": "#/$defs/n"}},
        "allOf": [{"properties": {"c": {"$ref": "#/$defs/n"}}}]}},
        "$ref": "#/$defs/n"})
}

fn nested_c(levels: usize) -> Value {
    let mut v = Value::Null;
    for _ in 0..levels {
        v = json!({"c": v});
    }
    v
}

// A step timeout no machine misses: the real validation of these tests must finish inside it.
const GENEROUS: Duration = Duration::from_secs(120);

fn latched_gate(latch: &ValidationLatch, step_timeout: Duration) -> crate::test_support::Gate {
    GateBuilder::new()
        .section("files", json!({"type": "object"}))
        .validation(latch.validation())
        .settings(move |s| s.step_timeout = step_timeout)
        .build()
}

fn valid_body() -> Vec<u8> {
    seed_body("app", "1", &json!({"files": {}}))
}

// DESIGN §4.3: the real validation of the two-applicators schema, a valid deep manifest, passes
// under a generous step timeout (the schema itself is valid and usable).
#[tokio::test]
async fn a_valid_deeply_nested_manifest_passes_the_two_applicators_schema() {
    let gate = GateBuilder::new()
        .section("files", exponential_schema())
        .settings(|s| s.step_timeout = GENEROUS)
        .build();

    let result = gate
        .seed(seed_body("app", "1", &json!({"files": nested_c(14)})))
        .await;

    assert!(
        matches!(result.outcome, GateOutcome::Proceed(_)),
        "a valid deep manifest passes the schema: {result:?}"
    );
    assert_eq!(gate.orchestrator.abandoned_validations(), 0);
}

// DESIGN §3.2 "Schema Validator": "Runs compilation, validation ... on the blocking thread
// pool, off the async executor." Structural: the validation blocks until a task of the SAME
// single-thread runtime releases it. Run on the runtime thread, it would block that thread, the
// release would never run, and the latch would give up.
#[tokio::test]
async fn validation_runs_off_the_async_runtime() {
    let latch = ValidationLatch::new();
    let gate = latched_gate(&latch, GENEROUS);
    let releaser = async {
        while latch.entered() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        latch.release();
    };

    let (result, ()) = tokio::join!(gate.seed(valid_body()), releaser);

    assert!(
        !latch.gave_up(),
        "the validation blocked the runtime thread that had to release it"
    );
    assert!(
        matches!(result.outcome, GateOutcome::Proceed(_)),
        "{result:?}"
    );
    assert_eq!(latch.finished(), 1);
}

// The request is decided at the step timeout and does not wait for the abandoned blocking task:
// when `seed` returns, the held validation has not returned (and is released only afterwards).
#[tokio::test]
async fn a_validation_that_outlives_the_step_timeout_ends_as_temporarily_unavailable_and_is_counted()
 {
    let (logs, _guard) = capture_logs();
    let latch = ValidationLatch::new();
    let gate = latched_gate(&latch, Duration::from_millis(50));

    let result = gate.seed(valid_body()).await;

    assert_eq!(
        latch.finished(),
        0,
        "the request returned while the abandoned validation was still held"
    );
    let r = decided(&result);
    assert_eq!(r.category, OutcomeCategory::TemporarilyUnavailable);
    assert!(
        r.unavailable.is_empty(),
        "no dependency to name: {:?}",
        r.unavailable
    );
    assert!(r.problems.is_empty());
    assert_eq!(
        r.audit_id.as_deref(),
        Some("audit-1"),
        "the request hand-off was accepted"
    );
    assert_eq!(result.audit_id.as_deref(), Some("audit-1"));
    assert_eq!(
        gate.orchestrator.abandoned_validations(),
        1,
        "the blocking task is counted"
    );
    logs.assert_live();
    let lines = logs.lines_with("manifest_validation_abandoned");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("WARN"), "{}", lines[0]);
    assert_eq!(
        field_value(&lines[0], "request_id").as_deref(),
        Some(result.request_id.to_string().as_str()),
        "{}",
        lines[0]
    );
    latch.release();
    latch.wait_finished(1).await;
    assert!(!latch.gave_up());
}

#[tokio::test]
async fn each_abandoned_validation_is_counted_and_a_normal_one_is_not() {
    let normal = files_gate().settings(|s| s.step_timeout = GENEROUS).build();
    normal.seed(valid_body()).await;
    assert_eq!(
        normal.orchestrator.abandoned_validations(),
        0,
        "a validation that finished is not abandoned"
    );
    let latch = ValidationLatch::new();
    let held = latched_gate(&latch, Duration::from_millis(30));

    held.seed(valid_body()).await;
    held.seed(valid_body()).await;

    assert_eq!(held.orchestrator.abandoned_validations(), 2);
    latch.release();
    latch.wait_finished(2).await;
}

// The step timeout bounds only the validation: a fetch failure or an invalid manifest never
// counts as abandoned.
#[tokio::test(start_paused = true)]
async fn requests_that_end_before_or_without_the_timeout_count_nothing() {
    let gate = files_gate().build();
    gate.sections[0].on(SectionCall::Schema, CallBehavior::hang());

    gate.seed(seed_body("a", "1", &json!({"files": {}}))).await;
    let ok = files_gate().build();
    ok.seed(seed_body("", "", &json!({"files": 1}))).await;

    assert_eq!(gate.orchestrator.abandoned_validations(), 0);
    assert_eq!(ok.orchestrator.abandoned_validations(), 0);
}

#[tokio::test(start_paused = true)]
async fn the_validation_of_a_normal_seed_does_not_consume_virtual_time() {
    let gate = files_gate().build();
    let started = tokio::time::Instant::now();

    gate.seed(seed_body("a", "1", &json!({"files": {}}))).await;

    assert_eq!(started.elapsed(), Duration::ZERO);
}

// F-004: a panic in the validation is not silent.
#[tokio::test]
async fn a_validation_that_panics_is_logged_and_ends_as_temporarily_unavailable() {
    let (logs, _guard) = capture_logs();
    let panicking: std::sync::Arc<crate::domain::orchestrator::ValidationFn> =
        std::sync::Arc::new(|_, _| panic!("validation boom"));
    let gate = GateBuilder::new()
        .section("files", json!({"type": "object"}))
        .validation(panicking)
        .settings(|s| s.step_timeout = GENEROUS)
        .build();

    let result = gate.seed(valid_body()).await;

    let r = decided(&result);
    assert_eq!(r.category, OutcomeCategory::TemporarilyUnavailable);
    assert!(r.unavailable.is_empty(), "{:?}", r.unavailable);
    assert_eq!(r.audit_id.as_deref(), Some("audit-1"));
    assert_eq!(
        gate.orchestrator.abandoned_validations(),
        0,
        "a panic is not an abandoned validation"
    );
    logs.assert_live();
    let lines = logs.lines_with("manifest_validation_failed");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].contains("WARN"), "{}", lines[0]);
    assert_eq!(
        field_value(&lines[0], "request_id").as_deref(),
        Some(result.request_id.to_string().as_str()),
        "{}",
        lines[0]
    );
    assert_eq!(
        field_value(&lines[0], "panicked").as_deref(),
        Some("true"),
        "{}",
        lines[0]
    );
    assert!(
        !lines[0].contains("validation boom"),
        "the panic text is not logged: {}",
        lines[0]
    );
    assert!(logs.lines_with("manifest_validation_abandoned").is_empty());
}

/// Seeds until the gate proceeds or `GENEROUS` passes (a slot is released a moment after the
/// validation returns).
async fn seed_until_proceed(gate: &crate::test_support::Gate) {
    let until = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let result = gate.seed(valid_body()).await;
        if matches!(result.outcome, GateOutcome::Proceed(_)) {
            return;
        }
        assert!(std::time::Instant::now() < until, "no slot was released");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

// F-003: with one blocking thread and two slots, the first validation runs (held), the second
// queues behind it and is aborted at its step timeout, so it never runs; both slots stay
// taken (the queued one until the pool reaches it), so a third request is refused at capacity.
#[test]
fn a_timed_out_queued_validation_never_runs_and_a_request_past_the_bound_is_refused() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let (logs, _guard) = capture_logs();
        let latch = ValidationLatch::new();
        let gate = GateBuilder::new()
            .section("files", json!({"type": "object"}))
            .validation(latch.validation())
            .settings(|s| {
                s.step_timeout = Duration::from_millis(80);
                s.max_validations = 2;
            })
            .build();

        gate.seed(valid_body()).await;
        let until = std::time::Instant::now() + Duration::from_secs(10);
        while latch.entered() == 0 {
            assert!(std::time::Instant::now() < until, "the first never started");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        gate.seed(valid_body()).await;
        let third = gate.seed(valid_body()).await;

        assert_eq!(gate.orchestrator.abandoned_validations(), 2);
        assert_eq!(latch.entered(), 1, "the queued validation did not start");
        let r = decided(&third);
        assert_eq!(r.category, OutcomeCategory::TemporarilyUnavailable);
        assert!(r.unavailable.is_empty(), "{:?}", r.unavailable);
        assert!(r.audit_id.is_some(), "the hand-off stays accepted");
        assert_eq!(gate.orchestrator.validations_refused_at_capacity(), 1);
        logs.assert_live();
        let lines = logs.lines_with("manifest_validation_at_capacity");
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("WARN"), "{}", lines[0]);
        assert_eq!(
            field_value(&lines[0], "request_id").as_deref(),
            Some(third.request_id.to_string().as_str()),
            "{}",
            lines[0]
        );
        assert_eq!(
            gate.orchestrator.abandoned_validations(),
            2,
            "a refusal is not an abandonment"
        );

        latch.release();
        latch.wait_finished(1).await;
        seed_until_proceed(&gate).await;
        assert_eq!(
            latch.entered(),
            2,
            "only the first and the one that just proceeded ever ran"
        );
        assert!(!latch.gave_up());
    });
}
