#![allow(clippy::unwrap_used, clippy::expect_used)]

use toolkit_security::{PlatformIdentity, PlatformSecurityContext};

use super::*;

fn caller() -> CallerIdentity {
    CallerIdentity::from_platform_context(&PlatformSecurityContext::new(
        PlatformIdentity::KubernetesServiceAccount {
            namespace: "deploy".to_owned(),
            service_account: "app-seeder".to_owned(),
            pod: None,
        },
    ))
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
