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
fn outcome_report_open_has_no_category_member() {
    let value = serde_json::to_value(open_report()).unwrap();
    let object = value.as_object().unwrap();
    assert!(!object.contains_key("category"), "{value}");
    assert_eq!(object["interrupted"], json!(false));
}

#[test]
fn outcome_report_interrupted_has_no_category_member() {
    let mut report = open_report();
    report.interrupted = true;
    let value = serde_json::to_value(report).unwrap();
    assert!(
        !value.as_object().unwrap().contains_key("category"),
        "{value}"
    );
    assert_eq!(value["interrupted"], json!(true));
}

#[test]
fn outcome_report_completed_carries_snake_case_category() {
    let mut report = open_report();
    report.category = Some(RequestCategory::TemporarilyUnavailable);
    let value = serde_json::to_value(report).unwrap();
    assert_eq!(value["category"], json!("temporarily_unavailable"));
}

#[test]
fn outcome_report_has_exactly_the_design_members() {
    let mut report = open_report();
    report.category = Some(RequestCategory::Succeeded);
    let value = serde_json::to_value(report).unwrap();
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
            "action",
            "application",
            "category",
            "interrupted",
            "operation_id",
            "problems",
            "sections",
            "unavailable"
        ]
    );
    assert_eq!(value["action"], json!("seed"));
    assert_eq!(value["operation_id"], json!(Uuid::from_u128(7).to_string()));
}

#[test]
fn outcome_report_round_trips() {
    let mut report = open_report();
    report.category = Some(RequestCategory::PluginRejected);
    let back: OutcomeReport =
        serde_json::from_value(serde_json::to_value(&report).unwrap()).unwrap();
    assert_eq!(back, report);
}

#[test]
fn uninstall_application_has_only_an_id() {
    let application = ReportApplication {
        id: "acme.app".to_owned(),
        version: None,
    };
    assert_eq!(
        serde_json::to_value(application).unwrap(),
        json!({ "id": "acme.app" })
    );
}

#[test]
fn section_entry_omits_absent_members() {
    let entry = SectionOutcomeEntry {
        key: "a".to_owned(),
        outcome: SectionOutcome::NotDispatched,
        schema_version: None,
        detail: None,
        location: None,
    };
    assert_eq!(
        serde_json::to_value(entry).unwrap(),
        json!({ "key": "a", "outcome": "not_dispatched" })
    );
}

#[test]
fn unknown_enum_text_is_refused() {
    assert!(serde_json::from_value::<RequestCategory>(json!("Succeeded")).is_err());
    assert!(serde_json::from_value::<SectionOutcome>(json!("done")).is_err());
}
