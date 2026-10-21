#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;

fn caller() -> CallerIdentity {
    let ctx = toolkit_security::SecurityContext::builder()
        .subject_id(Uuid::from_u128(1))
        .subject_tenant_id(Uuid::from_u128(2))
        .build()
        .unwrap();
    CallerIdentity::from_security_context(&ctx)
}

#[tokio::test]
async fn owns_the_noop_key() {
    assert_eq!(NoopSectionClient.key().await.unwrap(), "noop");
}

#[tokio::test]
async fn serves_an_any_object_draft_2020_12_schema() {
    let schema = NoopSectionClient.schema(&caller()).await.unwrap();
    assert_eq!(
        schema.schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    assert_eq!(schema.schema["type"], "object");
    assert_eq!(schema.schema_version, SCHEMA_VERSION);
}

#[tokio::test]
async fn apply_and_remove_accept_everything() {
    let envelope: Envelope =
        serde_json::from_value(serde_json::json!({"id": "app", "version": "1"})).unwrap();
    NoopSectionClient
        .apply(
            &caller(),
            Uuid::nil(),
            &envelope,
            &serde_json::json!({"any": 1}),
        )
        .await
        .unwrap();
    NoopSectionClient
        .remove(&caller(), Uuid::nil(), "app")
        .await
        .unwrap();
}
