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
    SECTION_KEY_MAX_LEN, SERVED_SCHEMA_MAX_BYTES, SectionPluginClientV1, compact_len_exceeds,
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

// DESIGN §3.3, §4.3: the iterative counter agrees with the serialization at every bound: a value
// whose compact form is `len` bytes does not exceed `len` and exceeds `len - 1`. Covers
// every string escape class (quote, backslash, named and numeric control escapes, DEL), multi-byte
// characters, numbers and the separators of arrays and objects.
#[test]
fn the_counter_is_the_compact_serialization_length_at_the_bound() {
    let ctl = |b: u8| char::from(b).to_string();
    let escapes = [
        "plain".to_owned(),
        "a\"b".to_owned(),
        "a\\b".to_owned(),
        "line\nfeed\rreturn\ttab".to_owned(),
        format!("{}{}{}", ctl(8), ctl(12), ctl(1)),
        format!("{}{}", ctl(0x1f), ctl(0x7f)),
        "caf\u{e9} \u{4e2d}\u{6587} \u{1f600}".to_owned(),
        "a/b".to_owned(),
        String::new(),
    ];
    let mut values = vec![
        json!(null),
        json!(true),
        json!(false),
        json!(0),
        json!(-17),
        json!(u64::MAX),
        json!(i64::MIN),
        json!(1.5),
        json!(-0.0),
        json!(1e300),
        json!(5e-324),
        json!([]),
        json!({}),
        json!([[], {}, [[1, 2], {"a": [null]}]]),
        json!({"b": {"c": [1, 2.5, "x"]}, "a": true, "": null}),
    ];
    for e in &escapes {
        values.push(Value::String(e.clone()));
        values.push(json!([e, e]));
        values.push(json!({ e.clone(): e.clone(), "k": [e] }));
    }
    for v in &values {
        let len = serde_json::to_vec(v).expect("ser").len();
        assert!(!compact_len_exceeds(v, len), "{v} at {len}");
        assert!(!compact_len_exceeds(v, len + 1), "{v} at {len} + 1");
        assert!(compact_len_exceeds(v, len - 1), "{v} at {len} - 1");
    }
}

// The bound itself, with escapes and multi-byte characters: a schema made of them is within the
// limit exactly while its serialization is.
#[test]
fn the_served_bound_is_exact_for_escaped_and_multibyte_text() {
    for unit in ["\n", "\"", "\\", "\u{e9}", "\u{1f600}"] {
        // Fill the string until the serialization reaches the bound exactly.
        let per = serde_json::to_vec(&Value::String(unit.to_owned()))
            .expect("ser")
            .len()
            - 2;
        let n = (SERVED_SCHEMA_MAX_BYTES - 2).div_euclid(per);
        let pad = SERVED_SCHEMA_MAX_BYTES - 2 - n * per;
        let mut v = Value::String(format!("{}{}", unit.repeat(n), "a".repeat(pad)));
        assert_eq!(
            serde_json::to_vec(&v).expect("ser").len(),
            SERVED_SCHEMA_MAX_BYTES,
            "{unit:?}"
        );
        assert!(served_schema_within_limit(&v), "{unit:?} at the bound");
        let Value::String(t) = &mut v else {
            unreachable!()
        };
        t.push('a');
        assert!(!served_schema_within_limit(&v), "{unit:?} over the bound");
    }
}

const DEEP_ENV: &str = "AM_SDK_DEEP_VALUE";

// Not a test of its own: the body of `a_very_deep_value_does_not_overflow_the_stack`, run by it
// in a child process (a stack overflow aborts the process). The 100 000-deep value is built and
// dropped on a large stack (both are recursive); the check runs on a 128 KiB one.
#[test]
#[ignore = "child process of a_very_deep_value_does_not_overflow_the_stack"]
fn deep_value_child() {
    if std::env::var(DEEP_ENV).is_err() {
        return;
    }
    std::thread::Builder::new()
        .stack_size(512 << 20)
        .spawn(|| {
            let mut v = json!(1);
            for _ in 0..100_000 {
                v = Value::Array(vec![v]);
            }
            let mut o = json!(1);
            for _ in 0..100_000 {
                o = Value::Object(std::iter::once(("k".to_owned(), o)).collect());
            }
            for deep in [&v, &o] {
                let within = std::thread::scope(|scope| {
                    std::thread::Builder::new()
                        .stack_size(128 << 10)
                        .spawn_scoped(scope, || served_schema_within_limit(deep))
                        .expect("spawn")
                        .join()
                        .expect("the check does not overflow")
                });
                assert!(within, "a 100 000-deep value is small");
            }
            println!("DEEP-VALUE-OK");
        })
        .expect("spawn")
        .join()
        .expect("the child body");
}

// A value nested 100 000 deep is measured without recursion (the previous implementation
// serialized recursively and overflowed). The measure runs in a child process, so an abort fails
// this test instead of the run.
#[test]
fn a_very_deep_value_does_not_overflow_the_stack() {
    let exe = std::env::current_exe().expect("the test binary");
    let out = std::process::Command::new(exe)
        .args([
            "--exact",
            "section_plugin_api::section_plugin_api_tests::deep_value_child",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(DEEP_ENV, "1")
        .output()
        .expect("run the child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("DEEP-VALUE-OK"),
        "child ended with {}\n{stdout}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
}
