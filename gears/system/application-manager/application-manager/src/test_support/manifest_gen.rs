//! Generators and fixtures for the manifest parser and schema validator tests (P5,
//! DESIGN §3.2 step 1, §3.2 "Schema Validator", §4.8).

use proptest::prelude::*;
use serde_json::{Map, Value};

/// `n` nested containers around one `1`; level `i` (0 is the outermost) is an object
/// `{"a":...}` when `is_object(i)` and an array `[...]` otherwise. The nesting depth of the
/// result is `n` (`[]` and `{}` count 1); `n = 0` is the bare `1`.
#[must_use]
pub fn nested_by(n: usize, is_object: impl Fn(usize) -> bool) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..n {
        out.extend(if is_object(i) {
            &b"{\"a\":"[..]
        } else {
            &b"["[..]
        });
    }
    out.push(b'1');
    for i in (0..n).rev() {
        out.push(if is_object(i) { b'}' } else { b']' });
    }
    out
}

/// `n` levels of nested arrays around one `1`: `[[...1...]]`. Nesting depth `n`.
#[must_use]
pub fn nested_arrays(n: usize) -> Vec<u8> {
    nested_by(n, |_| false)
}

/// `n` levels of nested objects around one `1`: `{"a":{"a":...1...}}`. Nesting depth `n`.
#[must_use]
pub fn nested_objects(n: usize) -> Vec<u8> {
    nested_by(n, |_| true)
}

/// `n` levels alternating object, array, object, ... (an object outermost) around one `1`.
/// Nesting depth `n`.
#[must_use]
pub fn nested_mixed(n: usize) -> Vec<u8> {
    nested_by(n, |i| i % 2 == 0)
}

/// A strategy for arbitrary JSON values with at most four levels of containers, whose
/// objects never repeat a member name (a `Map` cannot). Integer leaves cover the whole `i64`
/// and `u64` ranges; no random floats (their text round trip is `serde_json`'s business).
pub fn arb_json_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::from),
        any::<u64>().prop_map(Value::from),
        ".{0,8}".prop_map(Value::String),
    ];
    leaf.prop_recursive(4, 48, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..5).prop_map(Value::Array),
            prop::collection::vec((".{0,6}", inner), 0..5)
                .prop_map(|pairs| Value::Object(pairs.into_iter().collect::<Map<_, _>>())),
        ]
    })
}
