//! The no-op section plugin client.

use application_manager_sdk::{
    CallerIdentity, Envelope, SectionPluginClientV1, SectionPluginError, SectionSchema,
};
use async_trait::async_trait;
use uuid::Uuid;

/// The section key this plugin owns.
pub const SECTION_KEY: &str = "noop";

/// The version of [`section_schema`]; change it whenever the schema changes.
pub const SCHEMA_VERSION: &str = "1";

/// Accepts any object section and does nothing with it.
#[derive(Debug, Default)]
pub struct NoopSectionClient;

/// The schema of the `noop` section: any JSON object.
#[must_use]
pub fn section_schema() -> serde_json::Value {
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object"
    })
}

#[async_trait]
impl SectionPluginClientV1 for NoopSectionClient {
    async fn key(&self) -> Result<String, SectionPluginError> {
        Ok(SECTION_KEY.to_owned())
    }

    async fn schema(&self, _caller: &CallerIdentity) -> Result<SectionSchema, SectionPluginError> {
        Ok(SectionSchema::new(section_schema(), SCHEMA_VERSION))
    }

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
