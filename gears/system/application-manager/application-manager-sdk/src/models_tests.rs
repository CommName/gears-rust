#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use serde_json::json;

fn service_account(pod: Option<&str>) -> PlatformIdentity {
    PlatformIdentity::KubernetesServiceAccount {
        namespace: "deploy".to_owned(),
        service_account: "app-seeder".to_owned(),
        pod: pod.map(str::to_owned),
    }
}

fn spiffe() -> PlatformIdentity {
    PlatformIdentity::Spiffe {
        trust_domain: "example.org".to_owned(),
        name: "app-seeder".to_owned(),
        version: "1.2.0".to_owned(),
    }
}

#[test]
fn caller_identity_from_platform_context_keeps_identity_unchanged() {
    for identity in [
        service_account(Some("pod-1")),
        service_account(None),
        spiffe(),
    ] {
        let ctx = PlatformSecurityContext::new(identity.clone());
        let caller = CallerIdentity::from_platform_context(&ctx);
        assert_eq!(caller.identity(), &identity);
    }
}

#[test]
fn caller_identity_serializes_without_credential() {
    let caller = CallerIdentity::from_platform_context(&PlatformSecurityContext::new(
        service_account(Some("pod-1")),
    ));
    assert_eq!(
        serde_json::to_value(&caller).unwrap(),
        json!({
            "type": "KubernetesServiceAccount",
            "namespace": "deploy",
            "service_account": "app-seeder",
            "pod": "pod-1"
        })
    );
}

#[test]
fn caller_identity_round_trips_through_json() {
    let caller = CallerIdentity::from_platform_context(&PlatformSecurityContext::new(spiffe()));
    let back: CallerIdentity =
        serde_json::from_value(serde_json::to_value(&caller).unwrap()).unwrap();
    assert_eq!(back, caller);
}

#[test]
fn envelope_serializes_as_id_and_version() {
    let envelope = Envelope {
        id: "app".to_owned(),
        version: "1.0".to_owned(),
    };
    assert_eq!(
        serde_json::to_value(&envelope).unwrap(),
        json!({"id": "app", "version": "1.0"})
    );
}

ASDFADSF
AFDSADF

ASDFADS
