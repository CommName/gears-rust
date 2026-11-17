//! The no-op section plugin client.

use application_manager_sdk::{
    CallerIdentity, Envelope, SectionPluginClientV1, SectionPluginError,
};
use async_trait::async_trait;
use uuid::Uuid;

/// Accepts every section and does nothing with it.
#[derive(Debug, Default)]
pub struct NoopSectionClient;

#[async_trait]
impl SectionPluginClientV1 for NoopSectionClient {
    async fn apply(
        &self,
        _caller: &CallerIdentity,
        _operation_id: Uuid,
        _envelope: &Envelope,
        _section: &serde_json::Value,
    ) -> Result<(), SectionPluginError> {
        Ok(())
    }

    async fn remove(
        &self,
        _caller: &CallerIdentity,
        _operation_id: Uuid,
        _application_id: &str,
    ) -> Result<(), SectionPluginError> {
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "client_tests.rs"]
mod tests;
