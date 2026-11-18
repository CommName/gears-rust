//! Allowed callers (`cpt-cf-application-manager-component-allowed-callers`).
//!
//! Built once in `init` from the `allowed_callers` setting. Decides the
//! platform-plane actions from the caller's validated platform identity, in
//! memory, with no unavailable answer and no PDP call (ADR 0004).

use std::collections::HashSet;

use toolkit_macros::domain_model;
use toolkit_security::PlatformIdentity;

use crate::domain::actions::CallerAction;

/// The platform identity one entry matches.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerPattern {
    /// A Kubernetes service account; matches any pod.
    KubernetesServiceAccount {
        /// Namespace of the service account.
        namespace: String,
        /// Service account name.
        service_account: String,
    },
    /// A SPIFFE workload identity; matches any version.
    Spiffe {
        /// SPIFFE trust domain.
        trust_domain: String,
        /// Workload name.
        name: String,
    },
}

impl CallerPattern {
    fn matches(&self, identity: &PlatformIdentity) -> bool {
        match (self, identity) {
            (
                Self::KubernetesServiceAccount {
                    namespace,
                    service_account,
                },
                PlatformIdentity::KubernetesServiceAccount {
                    namespace: n,
                    service_account: s,
                    ..
                },
            ) => namespace == n && service_account == s,
            (
                Self::Spiffe { trust_domain, name },
                PlatformIdentity::Spiffe {
                    trust_domain: t,
                    name: n,
                    ..
                },
            ) => trust_domain == t && name == n,
            _ => false,
        }
    }
}

/// The configured platform identities and the actions each may take.
#[domain_model]
#[derive(Debug, Clone, Default)]
pub struct AllowedCallers {
    entries: Vec<(CallerPattern, HashSet<CallerAction>)>,
}

impl AllowedCallers {
    /// Allowed callers from `(identity, actions)` entries. An empty list
    /// allows nobody.
    #[must_use]
    pub fn new(entries: impl IntoIterator<Item = (CallerPattern, Vec<CallerAction>)>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(pattern, actions)| (pattern, actions.into_iter().collect()))
                .collect(),
        }
    }

    /// Whether `identity` may take `action`. Only a Kubernetes service account
    /// or a SPIFFE identity can match; any other identity kind never does.
    #[must_use]
    pub fn allows(&self, identity: &PlatformIdentity, action: CallerAction) -> bool {
        self.entries
            .iter()
            .any(|(pattern, actions)| actions.contains(&action) && pattern.matches(identity))
    }
}

/// The identity kind, the namespace or trust domain, and the name of
/// `identity`, for a refusal log line. Never includes a pod or a version.
#[must_use]
pub fn identity_fields(identity: &PlatformIdentity) -> (&'static str, &str, &str) {
    match identity {
        PlatformIdentity::KubernetesServiceAccount {
            namespace,
            service_account,
            ..
        } => ("kubernetes_service_account", namespace, service_account),
        PlatformIdentity::Spiffe {
            trust_domain, name, ..
        } => ("spiffe", trust_domain, name),
        PlatformIdentity::Shared { name } => ("shared", "", name),
        _ => ("other", "", ""),
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "allowed_callers_tests.rs"]
mod tests;
