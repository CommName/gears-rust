use toolkit_security::PlatformIdentity;

use super::*;

fn service_account(namespace: &str, name: &str, pod: Option<&str>) -> PlatformIdentity {
    PlatformIdentity::KubernetesServiceAccount {
        namespace: namespace.to_owned(),
        service_account: name.to_owned(),
        pod: pod.map(str::to_owned),
    }
}

fn spiffe(trust_domain: &str, name: &str, version: &str) -> PlatformIdentity {
    PlatformIdentity::Spiffe {
        trust_domain: trust_domain.to_owned(),
        name: name.to_owned(),
        version: version.to_owned(),
    }
}

fn callers() -> AllowedCallers {
    AllowedCallers::new([
        (
            CallerPattern::KubernetesServiceAccount {
                namespace: "deploy".to_owned(),
                service_account: "app-seeder".to_owned(),
            },
            vec![CallerAction::Seed, CallerAction::ReadSchema],
        ),
        (
            CallerPattern::Spiffe {
                trust_domain: "example.org".to_owned(),
                name: "seeder".to_owned(),
            },
            vec![CallerAction::ReadOperation],
        ),
    ])
}

#[test]
fn service_account_matches_any_pod() {
    let callers = callers();
    for pod in [None, Some("seeder-1"), Some("seeder-2")] {
        assert!(callers.allows(
            &service_account("deploy", "app-seeder", pod),
            CallerAction::Seed
        ));
    }
}

#[test]
fn spiffe_matches_any_version() {
    let callers = callers();
    for version in ["1.0.0", "2.3.4"] {
        assert!(callers.allows(
            &spiffe("example.org", "seeder", version),
            CallerAction::ReadOperation
        ));
    }
}

#[test]
fn wrong_namespace_name_or_action_is_refused() {
    let callers = callers();
    assert!(!callers.allows(
        &service_account("other", "app-seeder", None),
        CallerAction::Seed
    ));
    assert!(!callers.allows(
        &service_account("deploy", "other", None),
        CallerAction::Seed
    ));
    assert!(!callers.allows(
        &service_account("deploy", "app-seeder", None),
        CallerAction::Uninstall
    ));
    assert!(!callers.allows(
        &spiffe("other.org", "seeder", "1.0.0"),
        CallerAction::ReadOperation
    ));
    assert!(!callers.allows(
        &spiffe("example.org", "seeder", "1.0.0"),
        CallerAction::Seed
    ));
}

#[test]
fn identity_kinds_do_not_cross_match() {
    let callers = callers();
    // A SPIFFE identity whose trust domain and name equal a service
    // account entry's namespace and name still does not match it.
    assert!(!callers.allows(&spiffe("deploy", "app-seeder", "1.0.0"), CallerAction::Seed));
}

#[test]
fn shared_identity_never_matches() {
    let callers = callers();
    let shared = PlatformIdentity::Shared {
        name: "app-seeder".to_owned(),
    };
    for action in [
        CallerAction::Seed,
        CallerAction::Uninstall,
        CallerAction::ReadSchema,
        CallerAction::ReadOperation,
    ] {
        assert!(!callers.allows(&shared, action));
    }
}

#[test]
fn empty_list_allows_nobody() {
    let callers = AllowedCallers::new([]);
    assert!(!callers.allows(
        &service_account("deploy", "app-seeder", None),
        CallerAction::ReadSchema
    ));
}

#[test]
fn identity_fields_never_include_pod_or_version() {
    assert_eq!(
        identity_fields(&service_account("deploy", "app-seeder", Some("pod-1"))),
        ("kubernetes_service_account", "deploy", "app-seeder")
    );
    assert_eq!(
        identity_fields(&spiffe("example.org", "seeder", "1.0.0")),
        ("spiffe", "example.org", "seeder")
    );
    assert_eq!(
        identity_fields(&PlatformIdentity::Shared {
            name: "dev".to_owned()
        }),
        ("shared", "", "dev")
    );
}
