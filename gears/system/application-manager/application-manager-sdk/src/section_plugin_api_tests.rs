//! Shape and bound tests for the section plugin API (DESIGN §3.3, §4.8).
//!
//! Planned API: `SectionPluginClientV1::{key, schema, apply, remove}`,
//! `validate_section_key`, `SECTION_KEY_MAX_LEN`, `SERVED_SCHEMA_MAX_BYTES`, and
//! `served_schema_within_limit`.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::errors::{PLUGIN_DETAIL_MAX_BYTES, SectionPluginError};
use crate::models::{ApplicationEnvelope, CallerIdentity};
use crate::section_plugin_api::{
    SECTION_KEY_MAX_LEN, SERVED_SCHEMA_MAX_BYTES, SectionPluginClientV1,
    served_schema_within_limit, validate_section_key,
};

struct Double;

// DESIGN §3.3 section-plugin-api: `key`, `schema`, `apply`, `remove`.
#[async_trait]
impl SectionPluginClientV1 for Double {
    async fn key(&self) -> Result<String, SectionPluginError> {
        Ok("alpha".to_owned())
    }

    async fn schema(&self, _caller: &CallerIdentity) -> Result<Value, SectionPluginError> {
        Ok(json!({"type": "object"}))
    }

    async fn apply(
        &self,
        _caller: &CallerIdentity,
        _envelope: &ApplicationEnvelope,
        _section: Value,
    ) -> Result<(), SectionPluginError> {
        Ok(())
    }

    async fn remove(
        &self,
        _caller: &CallerIdentity,
        _envelope: &ApplicationEnvelope,
    ) -> Result<(), SectionPluginError> {
        Err(SectionPluginError::internal("x"))
    }
}

fn caller() -> CallerIdentity {
    serde_json::from_value(json!({
        "subject_id": "00000000-0000-0000-0000-000000000001",
        "subject_tenant_id": "00000000-0000-0000-0000-000000000002",
        "token_scopes": ["*"],
        "request_id": "00000000-0000-7000-8000-000000000003"
    }))
    .expect("caller identity fixture")
}

fn envelope() -> ApplicationEnvelope {
    serde_json::from_value(json!({"id": "app-1", "version": "1"})).expect("envelope fixture")
}

// DESIGN §3.3: the plugin is registered as a scoped client, so it must be object-safe
// and shareable.
#[test]
fn section_plugin_client_is_object_safe_and_send_sync() {
    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<dyn SectionPluginClientV1>();
    let _client: Arc<dyn SectionPluginClientV1> = Arc::new(Double);
}

// DESIGN §3.3: request tasks are spawned, so every future returned through the `dyn`
// handle must be `Send`.
#[test]
fn section_plugin_client_futures_are_send_through_dyn() {
    fn assert_send<T: Send>(_: T) {}
    #[allow(dead_code)]
    fn check(
        client: &dyn SectionPluginClientV1,
        caller: &CallerIdentity,
        env: &ApplicationEnvelope,
    ) {
        assert_send(client.key());
        assert_send(client.schema(caller));
        assert_send(client.apply(caller, env, json!({})));
        assert_send(client.remove(caller, env));
    }
}

// DESIGN §3.3: every operation is callable through the `dyn` handle.
#[tokio::test]
async fn section_plugin_client_methods_are_callable_through_dyn() {
    let client: Arc<dyn SectionPluginClientV1> = Arc::new(Double);
    let (caller, env) = (caller(), envelope());
    assert_eq!(client.key().await.expect("key"), "alpha");
    assert_eq!(
        client.schema(&caller).await.expect("schema"),
        json!({"type": "object"})
    );
    client.apply(&caller, &env, json!({})).await.expect("apply");
    assert!(client.remove(&caller, &env).await.is_err(), "remove errors");
}

// DESIGN §3.3 key pattern: the bound is SDK-fixed (value not specified).
#[test]
fn section_key_max_len_is_at_least_one() {
    assert!(
        SECTION_KEY_MAX_LEN >= 1,
        "a key of length 1 must be possible"
    );
}

// DESIGN §3.3 key pattern: each allowed class is accepted.
#[test]
fn section_key_accepts_each_allowed_class() {
    for key in [
        "a",
        "z",
        "0",
        "9",
        "-",
        "_",
        "...",
        ".a",
        "a.",
        "a-b_c.d9",
        "billing.core",
    ] {
        assert!(
            validate_section_key(key).is_ok(),
            "{key:?} must be accepted"
        );
    }
}

// DESIGN §3.3 key pattern: a key of length 1 and of exactly the bound are accepted.
#[test]
fn section_key_accepts_length_one_and_max() {
    assert!(validate_section_key("a").is_ok(), "length 1");
    let at_max = "a".repeat(SECTION_KEY_MAX_LEN);
    assert!(validate_section_key(&at_max).is_ok(), "length max");
}

// DESIGN §3.3 key pattern: the empty key and a key above the bound are rejected.
#[test]
fn section_key_rejects_empty_and_above_max() {
    assert!(validate_section_key("").is_err(), "empty");
    let above = "a".repeat(SECTION_KEY_MAX_LEN + 1);
    assert!(validate_section_key(&above).is_err(), "max + 1");
}

// DESIGN §3.3 key pattern: characters outside the pattern are rejected.
#[test]
fn section_key_rejects_characters_outside_pattern() {
    for key in [
        "Alpha", "ALPHA", "a b", " a", "a ", "a/b", "a\\b", "é", "a\u{e9}", "a\n", "a\0",
    ] {
        assert!(
            validate_section_key(key).is_err(),
            "{key:?} must be rejected"
        );
    }
}

// DESIGN §3.3 served schema bound: SDK-fixed, value not specified. A schema document
// is never smaller than a diagnostic detail.
#[test]
fn served_schema_max_bytes_relates_to_detail_bound() {
    assert!(SERVED_SCHEMA_MAX_BYTES > 0, "the bound must admit a schema");
    assert!(
        SERVED_SCHEMA_MAX_BYTES >= PLUGIN_DETAIL_MAX_BYTES,
        "a served schema may be at least as large as a detail"
    );
}

// DESIGN §3.3: a served schema whose serialized size equals the bound is within it.
#[test]
fn served_schema_at_bound_is_within_limit() {
    // A JSON string serializes to its length plus two quotes.
    let schema = Value::String("a".repeat(SERVED_SCHEMA_MAX_BYTES - 2));
    assert_eq!(
        serde_json::to_vec(&schema).expect("ser").len(),
        SERVED_SCHEMA_MAX_BYTES
    );
    assert!(served_schema_within_limit(&schema), "at the bound");
}

// DESIGN §3.3: one byte above the bound is not within it.
#[test]
fn served_schema_above_bound_is_not_within_limit() {
    let schema = Value::String("a".repeat(SERVED_SCHEMA_MAX_BYTES - 1));
    assert!(!served_schema_within_limit(&schema), "bound + 1");
}

// DESIGN §3.3: `.` and `..` cannot be carried in the schema-read path segment
// (clients and proxies normalise them), so they are not valid keys.
#[test]
fn section_key_rejects_dot_segments() {
    for key in [".", ".."] {
        assert!(
            validate_section_key(key).is_err(),
            "{key:?} must be rejected"
        );
    }
}
