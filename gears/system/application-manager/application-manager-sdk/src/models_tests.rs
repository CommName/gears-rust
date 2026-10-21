#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use serde_json::json;

fn full_context() -> SecurityContext {
    SecurityContext::builder()
        .subject_id(Uuid::from_u128(1))
        .subject_type("user")
        .subject_tenant_id(Uuid::from_u128(2))
        .token_scopes(vec!["a".to_owned(), "b".to_owned()])
        .bearer_token("secret-token".to_owned())
        .build()
        .unwrap()
}

#[test]
fn caller_identity_from_context_keeps_subject_type_tenant_scopes() {
    let ctx = full_context();
    let caller = CallerIdentity::from_security_context(&ctx);
    assert_eq!(caller.subject_id, Uuid::from_u128(1));
    assert_eq!(caller.subject_type.as_deref(), Some("user"));
    assert_eq!(caller.subject_tenant_id, Uuid::from_u128(2));
    assert_eq!(caller.token_scopes, vec!["a".to_owned(), "b".to_owned()]);
}

#[test]
fn caller_identity_from_context_without_type_keeps_none() {
    let ctx = SecurityContext::builder()
        .subject_id(Uuid::from_u128(1))
        .subject_tenant_id(Uuid::from_u128(2))
        .build()
        .unwrap();
    assert_eq!(
        CallerIdentity::from_security_context(&ctx).subject_type,
        None
    );
}

#[test]
fn caller_identity_round_trip_drops_bearer_token() {
    let ctx = full_context();
    assert!(ctx.bearer_token().is_some());
    let caller = CallerIdentity::from_security_context(&ctx);
    let back = caller.to_security_context().unwrap();
    assert!(back.bearer_token().is_none());
    assert_eq!(back.subject_id(), ctx.subject_id());
    assert_eq!(back.subject_type(), ctx.subject_type());
    assert_eq!(back.subject_tenant_id(), ctx.subject_tenant_id());
    assert_eq!(back.token_scopes(), ctx.token_scopes());
    assert_eq!(CallerIdentity::from_security_context(&back), caller);
}

#[test]
fn caller_identity_serializes_exactly_four_fields() {
    let caller = CallerIdentity {
        subject_id: Uuid::from_u128(1),
        subject_type: Some("user".to_owned()),
        subject_tenant_id: Uuid::from_u128(2),
        token_scopes: vec!["a".to_owned()],
    };
    let value = serde_json::to_value(&caller).unwrap();
    let mut keys: Vec<&str> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "subject_id",
            "subject_tenant_id",
            "subject_type",
            "token_scopes"
        ]
    );
}

#[test]
fn envelope_serializes_as_id_and_version() {
    let envelope = Envelope {
        id: "app".to_owned(),
        version: "1.0.0".to_owned(),
    };
    assert_eq!(
        serde_json::to_value(&envelope).unwrap(),
        json!({"id": "app", "version": "1.0.0"})
    );
}

#[test]
fn caller_identity_anonymous_context_converts_back_to_anonymous_error() {
    let ctx = SecurityContext::anonymous();
    let caller = CallerIdentity::from_security_context(&ctx);
    assert!(matches!(
        caller.to_security_context(),
        Err(CallerIdentityError::Anonymous)
    ));
}

#[test]
fn caller_identity_nil_subject_or_tenant_is_anonymous_error() {
    for (subject, tenant) in [(0, 2), (1, 0)] {
        let caller = CallerIdentity {
            subject_id: Uuid::from_u128(subject),
            subject_type: None,
            subject_tenant_id: Uuid::from_u128(tenant),
            token_scopes: vec![],
        };
        assert!(matches!(
            caller.to_security_context(),
            Err(CallerIdentityError::Anonymous)
        ));
    }
}

#[test]
fn caller_identity_without_subject_type_round_trips_through_context() {
    let caller = CallerIdentity {
        subject_id: Uuid::from_u128(1),
        subject_type: None,
        subject_tenant_id: Uuid::from_u128(2),
        token_scopes: vec!["a".to_owned()],
    };
    let ctx = caller.to_security_context().unwrap();
    assert_eq!(ctx.subject_type(), None);
    assert_eq!(CallerIdentity::from_security_context(&ctx), caller);
}

#[test]
fn envelope_deserializes_round_trip() {
    let envelope: Envelope =
        serde_json::from_value(json!({"id": "app", "version": "1.0.0"})).unwrap();
    assert_eq!(
        envelope,
        Envelope {
            id: "app".to_owned(),
            version: "1.0.0".to_owned()
        }
    );
    let back: Envelope = serde_json::from_value(serde_json::to_value(&envelope).unwrap()).unwrap();
    assert_eq!(back, envelope);
}

#[test]
fn section_schema_serde_uses_schema_and_schema_version_fields() {
    let schema = SectionSchema::new(json!({"type": "object"}), "v1");
    let value = serde_json::to_value(&schema).unwrap();
    assert_eq!(
        value,
        json!({"schema": {"type": "object"}, "schema_version": "v1"})
    );
    let back: SectionSchema = serde_json::from_value(value).unwrap();
    assert_eq!(back, schema);
}
