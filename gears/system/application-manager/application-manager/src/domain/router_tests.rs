//! Unit tests for the section plugin router's live schema fetch (DESIGN §3.2 "Section Plugin
//! Router" and "Orchestrator" step 5, §3.3 "Schema errors", §4.5: "A schema is not served, or
//! is unusable", §4.12 phasing: "no cache").
//!
//! Planned API in `crate::domain::router` (dispatch, `apply` and `remove`, is P7):
//!
//! ```text
//! pub struct SectionPluginRouter;
//! impl SectionPluginRouter {
//!     pub fn new(result: Arc<StartupResult>) -> Self;
//!     /// Calls `schema(caller)` on EVERY section plugin of the result, concurrently (one plugin
//!     /// time limit for N plugins), on EVERY call: nothing is cached. `Ok`: one
//!     /// `SectionSchema { key, schema }` per plugin in priority order, exactly as served.
//!     /// `Err`: one `UnavailableEntry { dependency: SectionSchema, key: Some(key), reason }`
//!     /// per plugin whose call failed, in priority order, with
//!     /// `PluginCallError::schema_reason()`: Internal and guard failures `unreachable`,
//!     /// Rejected / NotPermitted `unusable` (a plugin that breaks the schema contract),
//!     /// timeout `timed_out`, cap refusal `not_sent`. The other plugins were still called.
//!     /// The items follow the PRIORITY order whichever plugin answers first.
//!     pub async fn fetch_schemas(&self, caller: &CallerIdentity)
//!         -> Result<Vec<SectionSchema>, Vec<UnavailableEntry>>;
//! }
//! ```

use std::time::Duration;

use application_manager_sdk::models::{Dependency, UnavailableEntry, UnavailableReason};
use application_manager_sdk::{CallerIdentity, SectionPluginError};
use serde_json::json;
use tokio::time::Instant;
use uuid::Uuid;

use crate::domain::router::SectionPluginRouter;
use crate::domain::schema_validator::SectionSchema;
use crate::test_support::{CallBehavior, GateBuilder, PLUGIN_TIMEOUT, SectionCall, bounded};

fn caller() -> CallerIdentity {
    CallerIdentity {
        subject_id: Uuid::from_u128(1),
        subject_type: Some("user".to_owned()),
        subject_tenant_id: Uuid::from_u128(2),
        token_scopes: vec!["s".to_owned()],
        request_id: Uuid::from_u128(0x77),
    }
}

fn unavailable(key: &str, reason: UnavailableReason) -> UnavailableEntry {
    UnavailableEntry {
        dependency: Dependency::SectionSchema,
        key: Some(key.to_owned()),
        reason,
    }
}

fn router(gate: &crate::test_support::Gate) -> SectionPluginRouter {
    SectionPluginRouter::new(gate.holder.result().expect("published"))
}

#[tokio::test]
async fn every_schema_is_fetched_in_priority_order_exactly_as_served() {
    let gate = GateBuilder::new()
        .section("zeta", json!({"type": "object", "title": "z"}))
        .section("alpha", json!({"type": "string"}))
        .build();

    let schemas = router(&gate)
        .fetch_schemas(&caller())
        .await
        .expect("served");

    assert_eq!(
        schemas,
        vec![
            SectionSchema {
                key: "zeta".to_owned(),
                schema: json!({"type": "object", "title": "z"}),
            },
            SectionSchema {
                key: "alpha".to_owned(),
                schema: json!({"type": "string"}),
            },
        ],
        "priority order (registration order here), not key order"
    );
}

#[tokio::test]
async fn an_empty_plugin_set_serves_no_schema_and_calls_nothing() {
    let gate = GateBuilder::new().build();

    let schemas = router(&gate)
        .fetch_schemas(&caller())
        .await
        .expect("served");

    assert_eq!(schemas, Vec::new());
    assert!(gate.events.events().is_empty());
}

#[tokio::test]
async fn each_plugin_receives_the_caller_identity_and_one_schema_call() {
    let gate = GateBuilder::new()
        .section("a", json!({}))
        .section("b", json!({}))
        .build();
    let caller = caller();

    router(&gate).fetch_schemas(&caller).await.expect("served");

    for plugin in &gate.sections {
        let calls = plugin.calls();
        assert_eq!(calls.len(), 1, "one call per fetch");
        assert_eq!(calls[0].kind, SectionCall::Schema);
        assert_eq!(calls[0].caller.as_ref(), Some(&caller));
    }
}

// DESIGN §3.3 "Discovery of Keys and Schemas", §4.12 "no cache": the schema is live.
#[tokio::test]
async fn nothing_is_cached_a_changed_schema_is_seen_by_the_next_fetch() {
    let gate = GateBuilder::new()
        .section("a", json!({"type": "string"}))
        .build();
    let router = router(&gate);

    let first = router.fetch_schemas(&caller()).await.expect("first");
    gate.sections[0].set_schema(json!({"type": "integer"}));
    let second = router.fetch_schemas(&caller()).await.expect("second");

    assert_eq!(first[0].schema, json!({"type": "string"}));
    assert_eq!(second[0].schema, json!({"type": "integer"}));
    assert_eq!(
        gate.sections[0].call_count(),
        2,
        "one plugin call per fetch"
    );
}

// §3.3 "so the key phase takes one time limit": the same holds for the schema phase.
#[tokio::test(start_paused = true)]
async fn the_plugins_are_called_concurrently_one_delay_not_the_sum() {
    let gate = GateBuilder::new()
        .section("a", json!({}))
        .section("b", json!({}))
        .section("c", json!({}))
        .build();
    for plugin in &gate.sections {
        plugin.on(
            SectionCall::Schema,
            CallBehavior::ok().with_delay(Duration::from_millis(300)),
        );
    }
    let started = Instant::now();

    router(&gate)
        .fetch_schemas(&caller())
        .await
        .expect("served");

    assert_eq!(started.elapsed(), Duration::from_millis(300));
}

#[tokio::test(start_paused = true)]
async fn a_hung_plugin_is_timed_out_after_one_limit_and_the_others_are_still_served() {
    let gate = GateBuilder::new()
        .section("fine", json!({}))
        .section("hung", json!({}))
        .build();
    gate.sections[1].on(SectionCall::Schema, CallBehavior::hang());
    let started = Instant::now();

    let result = bounded(router(&gate).fetch_schemas(&caller())).await;

    assert_eq!(
        result,
        Err(vec![unavailable("hung", UnavailableReason::TimedOut)])
    );
    assert_eq!(started.elapsed(), PLUGIN_TIMEOUT, "one plugin time limit");
    assert_eq!(
        gate.sections[0].call_count(),
        1,
        "the healthy plugin was still called"
    );
}

// §3.3 "Schema errors": `schema` may return only `Internal` (reason `unreachable`); a plugin
// that returns Rejected or NotPermitted from `schema` breaks the contract: `unusable`.
#[tokio::test]
async fn plugin_errors_map_to_their_schema_reason() {
    let gate = GateBuilder::new()
        .section("internal", json!({}))
        .section("rejected", json!({}))
        .section("refused", json!({}))
        .section("panics", json!({}))
        .section("ok", json!({}))
        .build();
    gate.sections[0].on(
        SectionCall::Schema,
        CallBehavior::err(SectionPluginError::internal("boom")),
    );
    gate.sections[1].on(
        SectionCall::Schema,
        CallBehavior::err(SectionPluginError::rejected("no", None)),
    );
    gate.sections[2].on(
        SectionCall::Schema,
        CallBehavior::err(SectionPluginError::not_permitted("no")),
    );
    gate.sections[3].on(SectionCall::Schema, CallBehavior::panic());

    let result = router(&gate).fetch_schemas(&caller()).await;

    assert_eq!(
        result,
        Err(vec![
            unavailable("internal", UnavailableReason::Unreachable),
            unavailable("rejected", UnavailableReason::Unusable),
            unavailable("refused", UnavailableReason::Unusable),
            unavailable("panics", UnavailableReason::Unreachable),
        ]),
        "only the failing keys, in priority order"
    );
    assert_eq!(gate.sections[4].call_count(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_plugin_at_its_in_flight_cap_is_not_sent_and_reported_not_sent() {
    let gate = GateBuilder::new()
        .section("a", json!({}))
        .guard_cap(1)
        .build();
    gate.sections[0].on(SectionCall::Schema, CallBehavior::hang());
    // The first fetch times out and leaves its call running, holding the only slot.
    let first = bounded(router(&gate).fetch_schemas(&caller())).await;
    assert_eq!(
        first,
        Err(vec![unavailable("a", UnavailableReason::TimedOut)])
    );

    let second = bounded(router(&gate).fetch_schemas(&caller())).await;

    assert_eq!(
        second,
        Err(vec![unavailable("a", UnavailableReason::NotSent)])
    );
    assert_eq!(
        gate.sections[0].call_count(),
        1,
        "a refused call never reaches the plugin"
    );
}

// The items and the schemas follow the PRIORITY order, not the order the plugins answer in
// (DESIGN §3.2: "in priority order"). The first plugin is the slow one in both cases.
#[tokio::test(start_paused = true)]
async fn failures_are_listed_in_priority_order_when_an_earlier_plugin_is_slower() {
    let gate = GateBuilder::new()
        .section("a", json!({}))
        .section("b", json!({}))
        .build();
    gate.sections[0].on(SectionCall::Schema, CallBehavior::hang());
    gate.sections[1].on(
        SectionCall::Schema,
        CallBehavior::err(SectionPluginError::internal("boom")),
    );
    let started = Instant::now();

    let result = bounded(router(&gate).fetch_schemas(&caller())).await;

    assert_eq!(
        result,
        Err(vec![
            unavailable("a", UnavailableReason::TimedOut),
            unavailable("b", UnavailableReason::Unreachable),
        ]),
        "a timed out first, b (which failed at once) second"
    );
    assert_eq!(started.elapsed(), PLUGIN_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn schemas_are_listed_in_priority_order_when_an_earlier_plugin_answers_later() {
    let gate = GateBuilder::new()
        .section("a", json!({"title": "a"}))
        .section("b", json!({"title": "b"}))
        .build();
    gate.sections[0].on(
        SectionCall::Schema,
        CallBehavior::ok().with_delay(Duration::from_millis(300)),
    );

    let schemas = router(&gate)
        .fetch_schemas(&caller())
        .await
        .expect("served");

    let keys: Vec<&str> = schemas.iter().map(|s| s.key.as_str()).collect();
    assert_eq!(keys, ["a", "b"], "b answered first, a is still first");
}

// A schema reaches the validator exactly as the plugin served it: dialect, identifier,
// definitions, references and an integer past 2^53 all survive (no rewriting, no float
// conversion).
#[tokio::test]
async fn a_served_schema_is_returned_unchanged_with_dialect_id_defs_refs_and_big_integers() {
    let served = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://example.com/schemas/files.json",
        "$defs": {"n": {"type": "integer", "maximum": 9_007_199_254_740_993_u64}},
        "type": "object",
        "properties": {"count": {"$ref": "#/$defs/n"}},
        "additionalProperties": false
    });
    let gate = GateBuilder::new().section("files", served.clone()).build();

    let schemas = router(&gate)
        .fetch_schemas(&caller())
        .await
        .expect("served");

    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].schema, served);
    assert_eq!(
        schemas[0].schema["$defs"]["n"]["maximum"].as_u64(),
        Some(9_007_199_254_740_993),
        "2^53 + 1 is not rounded"
    );
}
