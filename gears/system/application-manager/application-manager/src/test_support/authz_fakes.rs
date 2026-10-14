//! A fake platform PDP (`AuthZResolverApi`) with scripted answers and a request record
//! (P6, DESIGN §3.2 "Ports and Composition": "A test PDP that permits with no constraint,
//! refuses, cannot answer, or returns any constraint, such as a tenant predicate ... it
//! records each request, so tests can assert that none names a context tenant").

use std::sync::{Mutex, MutexGuard, PoisonError};

use serde_json::json;
use std::time::Duration;

use async_trait::async_trait;
use authz_resolver_sdk::constraints::{Constraint, EqPredicate, InPredicate, Predicate};
use authz_resolver_sdk::{
    AuthZResolverApi,
    models::{EvaluationRequest, EvaluationResponse, EvaluationResponseContext},
};
use toolkit_canonical_errors::CanonicalError;
use toolkit_security::PlatformSecurityContext;
use uuid::Uuid;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What the fake PDP answers.
#[derive(Clone)]
pub enum AuthzScript {
    /// `decision = true` with these constraints (empty: an unconstrained permit).
    Permit(Vec<Constraint>),
    /// `decision = false`.
    Deny,
    /// The answer is exactly this response (a deny that carries constraints, say).
    Raw(EvaluationResponse),
    /// The evaluation call fails with an internal error.
    Fail,
    /// The evaluation call fails the way a transport does: the service is unavailable.
    Unavailable,
    /// The call never completes.
    Hang,
    /// An unconstrained permit after this delay (on the tokio clock).
    PermitAfter(Duration),
}

/// A constraint on `property` with one value, such as a tenant predicate.
pub fn constraint_on(property: &str, value: Uuid) -> Constraint {
    Constraint {
        predicates: vec![Predicate::In(InPredicate::new(property, [value]))],
    }
}

/// A constraint with no predicate (malformed: the platform compiler fails it closed).
pub fn empty_constraint() -> Constraint {
    Constraint {
        predicates: Vec::new(),
    }
}

/// A constraint that holds one equality predicate on `property`.
pub fn eq_constraint(property: &str, value: &str) -> Constraint {
    Constraint {
        predicates: vec![Predicate::Eq(EqPredicate {
            property: property.to_owned(),
            value: json!(value),
        })],
    }
}

/// Fake `AuthZResolverApi` that records every request.
pub struct FakeAuthz {
    script: AuthzScript,
    requests: Mutex<Vec<EvaluationRequest>>,
}

impl FakeAuthz {
    /// Creates a PDP that answers `script`.
    pub fn new(script: AuthzScript) -> Self {
        Self {
            script,
            requests: Mutex::new(Vec::new()),
        }
    }

    /// The requests received so far, in order.
    pub fn requests(&self) -> Vec<EvaluationRequest> {
        lock(&self.requests).clone()
    }
}

#[async_trait]
impl AuthZResolverApi for FakeAuthz {
    async fn evaluate(
        &self,
        _ctx: PlatformSecurityContext,
        req: EvaluationRequest,
    ) -> Result<EvaluationResponse, CanonicalError> {
        lock(&self.requests).push(req);
        match self.script.clone() {
            AuthzScript::Permit(constraints) => Ok(EvaluationResponse {
                decision: true,
                context: EvaluationResponseContext {
                    constraints,
                    ..Default::default()
                },
            }),
            AuthzScript::Deny => Ok(EvaluationResponse {
                decision: false,
                context: EvaluationResponseContext::default(),
            }),
            AuthzScript::Raw(response) => Ok(response),
            AuthzScript::Fail => Err(CanonicalError::internal("fake pdp failure").create()),
            AuthzScript::Unavailable => Err(CanonicalError::service_unavailable()
                .with_detail("fake pdp unavailable")
                .create()),
            AuthzScript::Hang => std::future::pending().await,
            AuthzScript::PermitAfter(delay) => {
                tokio::time::sleep(delay).await;
                Ok(EvaluationResponse {
                    decision: true,
                    context: EvaluationResponseContext::default(),
                })
            }
        }
    }
}
