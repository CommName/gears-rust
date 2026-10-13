//! Domain ports and their clock / request-id abstractions (DESIGN §3.2 "Ports and
//! Composition", §3.3).
//!
//! The ports are the seams between the domain and its collaborators: the PDP, the
//! registry store, the section and audit plugins, the startup holder, the clock and
//! the request-id source. Production adapters live in the infra layer; the test
//! doubles live in `test_support`.

use std::time::SystemTime;

use crate::domain::registry::RegistryEntry;
use application_manager_sdk::gts::{
    ACTION_LIST, ACTION_READ_SCHEMA, ACTION_SEED, ACTION_UNINSTALL,
};
use application_manager_sdk::models::{SectionOutcome, UnavailableReason};
use application_manager_sdk::{
    ApplicationEnvelope, AuditPluginError, CallerIdentity, FinalResultContent,
    RequestHandoffContent, SectionPluginError,
};
use async_trait::async_trait;
use serde_json::Value;
use toolkit_macros::domain_model;
use uuid::Uuid;

/// Failure of a plugin call made through the Plugin Call Guard (DESIGN §3.2, §4.5).
///
/// `E` is the plugin's own error type. The guard adds the transport-level outcomes.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginCallError<E> {
    /// The plugin answered with its own error.
    Plugin(E),
    /// The per-call timeout elapsed before the plugin answered.
    TimedOut,
    /// The call was not sent: the plugin's in-flight limit was reached.
    NotSent,
    /// The call failed inside the guard (for example the plugin panicked). The text
    /// is a fixed constant and never carries plugin-provided data.
    Internal(String),
}

impl PluginCallError<SectionPluginError> {
    /// Outcome reported for a section whose plugin call failed (DESIGN §3.3).
    #[must_use]
    pub fn section_outcome(&self) -> SectionOutcome {
        match self {
            Self::Plugin(e) => e.outcome(),
            Self::TimedOut => SectionOutcome::TimedOut,
            Self::NotSent => SectionOutcome::NotSent,
            Self::Internal(_) => SectionOutcome::InternalFailure,
        }
    }

    /// Reason a section schema is unavailable (DESIGN §3.3 "Schema errors", F-005).
    ///
    /// A plugin that rejected the call or is not permitted yields `Unusable`; a plugin
    /// or guard internal failure yields `Unreachable`.
    #[must_use]
    pub fn schema_reason(&self) -> UnavailableReason {
        match self {
            Self::Plugin(SectionPluginError::Internal { .. }) | Self::Internal(_) => {
                UnavailableReason::Unreachable
            }
            Self::Plugin(
                SectionPluginError::Rejected { .. } | SectionPluginError::NotPermitted { .. },
            ) => UnavailableReason::Unusable,
            Self::TimedOut => UnavailableReason::TimedOut,
            Self::NotSent => UnavailableReason::NotSent,
        }
    }
}

impl PluginCallError<AuditPluginError> {
    /// Reason an audit handoff is unavailable (DESIGN §3.3).
    #[must_use]
    pub fn handoff_reason(&self) -> UnavailableReason {
        match self {
            Self::Plugin(e) => e.reason(),
            Self::TimedOut => UnavailableReason::TimedOut,
            Self::NotSent => UnavailableReason::NotSent,
            Self::Internal(_) => UnavailableReason::Unreachable,
        }
    }
}

/// Action an operation asks the PDP to authorize (DESIGN §3.2).
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyAction {
    /// Seed (create or replace) an application.
    Seed,
    /// Uninstall an application.
    Uninstall,
    /// List applications.
    List,
    /// Read an application's section schemas.
    ReadSchema,
}

impl PolicyAction {
    /// GTS action name sent to the PDP.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Seed => ACTION_SEED,
            Self::Uninstall => ACTION_UNINSTALL,
            Self::List => ACTION_LIST,
            Self::ReadSchema => ACTION_READ_SCHEMA,
        }
    }
}

/// Answer of the policy port.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecision {
    /// The caller may perform the action.
    Permit,
    /// The PDP refused the action.
    Refused,
    /// The PDP could not be reached or did not decide.
    Unavailable,
}

/// Failure of the registry store.
///
/// The store reports whether the backing registry was unreachable or the operation
/// timed out; no backend detail crosses this boundary.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    /// The backing registry is unavailable.
    Unavailable,
    /// The operation timeout expired.
    TimedOut,
}

impl RegistryError {
    /// Reason of the `registry_storage` unavailable item.
    #[must_use]
    pub fn reason(&self) -> UnavailableReason {
        match self {
            Self::Unavailable => UnavailableReason::Unreachable,
            Self::TimedOut => UnavailableReason::TimedOut,
        }
    }
}

/// Port to the policy decision point (PDP). The policy adapter is deferred to the
/// policy adapter phase (P6).
#[async_trait]
pub trait PolicyPort: Send + Sync {
    /// Decides whether `caller` may perform `action`, optionally on `application_id`.
    async fn decide(
        &self,
        caller: &CallerIdentity,
        action: PolicyAction,
        application_id: Option<&str>,
    ) -> PolicyDecision;
}

/// Port to the application registry store, implemented by the `SeaORM` adapter in
/// `infra::storage`.
#[async_trait]
pub trait RegistryStore: Send + Sync {
    /// Returns the entry with the given id, if any.
    async fn get(&self, id: &str) -> Result<Option<RegistryEntry>, RegistryError>;
    /// Creates or replaces an entry.
    async fn upsert(&self, entry: &RegistryEntry) -> Result<(), RegistryError>;
    /// Deletes the entry with the given id.
    async fn delete(&self, id: &str) -> Result<(), RegistryError>;
    /// Returns all entries.
    async fn list(&self) -> Result<Vec<RegistryEntry>, RegistryError>;
}

/// Port to one section plugin, with calls made through the Plugin Call Guard.
/// The adapter over the section plugin client is deferred to a later phase.
#[async_trait]
pub trait SectionPluginPort: Send + Sync {
    /// Returns the section key the plugin serves.
    async fn key(&self) -> Result<String, PluginCallError<SectionPluginError>>;
    /// Returns the plugin's JSON schema for its section.
    async fn schema(
        &self,
        caller: &CallerIdentity,
    ) -> Result<Value, PluginCallError<SectionPluginError>>;
    /// Applies `section` of `envelope`.
    async fn apply(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
        section: Value,
    ) -> Result<(), PluginCallError<SectionPluginError>>;
    /// Removes the plugin's data for `envelope`.
    async fn remove(
        &self,
        caller: &CallerIdentity,
        envelope: &ApplicationEnvelope,
    ) -> Result<(), PluginCallError<SectionPluginError>>;
}

/// Port to the audit plugin, with calls made through the Plugin Call Guard.
/// The audit hand-off client is deferred to the audit hand-off phase (P6).
#[async_trait]
pub trait AuditSink: Send + Sync {
    /// Hands the request off for audit and returns the audit id.
    async fn request_handoff(
        &self,
        caller: &CallerIdentity,
        content: &RequestHandoffContent,
    ) -> Result<String, PluginCallError<AuditPluginError>>;
    /// Hands the final result off for audit under `audit_id`.
    async fn final_result_handoff(
        &self,
        caller: &CallerIdentity,
        audit_id: &str,
        content: &FinalResultContent,
    ) -> Result<(), PluginCallError<AuditPluginError>>;
}

/// Read-and-stop port to the startup state holder (DESIGN §3.2).
///
/// Publication is not part of the port: the startup component publishes the startup
/// result through the concrete holder (`StartupResultHolder::publish_result`), and the
/// domain components and the REST not-ready gate only read and stop through this port.
pub trait StartupPort: Send + Sync {
    /// Marks the gear as stopping.
    fn set_stopping(&self);
    /// Whether the gear is published and not stopping.
    fn is_ready(&self) -> bool;
    /// Whether the gear is stopping.
    fn is_stopping(&self) -> bool;
}

/// Source of the current wall-clock time.
pub trait Clock: Send + Sync {
    /// Returns the current time.
    fn now(&self) -> SystemTime;
}

/// Source of request ids.
pub trait RequestIdSource: Send + Sync {
    /// Returns the next request id.
    fn next_id(&self) -> Uuid;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "ports_tests.rs"]
mod ports_tests;
