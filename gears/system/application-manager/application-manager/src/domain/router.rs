//! Section plugin router: the live schema fetch (DESIGN §3.2 "Section Plugin Router", §3.3
//! "Schema errors"). Dispatch (`apply`, `remove`) belongs to the request task, not here.

use std::sync::Arc;

use application_manager_sdk::CallerIdentity;
use application_manager_sdk::models::{Dependency, UnavailableEntry};

use crate::domain::schema_validator::SectionSchema;
use toolkit_macros::domain_model;

use crate::domain::startup::StartupResult;

/// Routes calls to the section plugins of one startup result.
#[domain_model]
pub struct SectionPluginRouter {
    result: Arc<StartupResult>,
}

impl SectionPluginRouter {
    /// Builds the router over the section plugins of `result`.
    #[must_use]
    pub fn new(result: Arc<StartupResult>) -> Self {
        Self { result }
    }

    /// Calls `schema` on every section plugin, concurrently, on every call (no cache).
    ///
    /// # Errors
    ///
    /// One `section_schema` item per plugin whose call failed, in priority order. Nothing is
    /// logged here and no served schema is kept.
    pub async fn fetch_schemas(
        &self,
        caller: &CallerIdentity,
    ) -> Result<Vec<SectionSchema>, Vec<UnavailableEntry>> {
        let calls = self
            .result
            .section_plugins()
            .iter()
            .map(|entry| async move { (entry.key.clone(), entry.plugin.schema(caller).await) });
        let results = futures::future::join_all(calls).await;
        let mut schemas = Vec::new();
        let mut unavailable = Vec::new();
        for (key, result) in results {
            match result {
                Ok(schema) => schemas.push(SectionSchema { key, schema }),
                Err(e) => unavailable.push(UnavailableEntry {
                    dependency: Dependency::SectionSchema,
                    key: Some(key),
                    reason: e.schema_reason(),
                }),
            }
        }
        if unavailable.is_empty() {
            Ok(schemas)
        } else {
            Err(unavailable)
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "router_tests.rs"]
mod router_tests;
