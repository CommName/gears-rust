//! `RegistryStore` adapter over the repository (DESIGN §3.2, §4.7).
//!
//! Supplies the allow-all scope (ADR 0007), bounds each operation by the configured
//! registry operation timeout and maps failures to `RegistryError`.

use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use toolkit_db::secure::AccessScope;
use toolkit_db::{DBProvider, DbError};

use crate::domain::ports::{RegistryError, RegistryStore};
use crate::domain::registry::RegistryEntry;
use crate::infra::storage::repo::{FailureCause, RegistryRepo, log_failure};

/// Registry store backed by the gear database.
#[derive(Clone)]
pub struct SeaRegistryStore {
    provider: DBProvider<DbError>,
    operation_timeout: Duration,
}

impl SeaRegistryStore {
    /// Creates the store; `operation_timeout` bounds every operation.
    #[must_use]
    pub fn new(provider: DBProvider<DbError>, operation_timeout: Duration) -> Self {
        Self {
            provider,
            operation_timeout,
        }
    }

    async fn bounded<T>(
        &self,
        op: &'static str,
        fut: impl Future<Output = Result<T, RegistryError>>,
    ) -> Result<T, RegistryError> {
        if let Ok(result) = tokio::time::timeout(self.operation_timeout, fut).await {
            result
        } else {
            log_failure(op, FailureCause::TimedOut);
            Err(RegistryError::TimedOut)
        }
    }
}

#[async_trait]
impl RegistryStore for SeaRegistryStore {
    async fn get(&self, id: &str) -> Result<Option<RegistryEntry>, RegistryError> {
        self.bounded("get", async {
            let conn = self.provider.conn().map_err(|_| {
                log_failure("get", FailureCause::Connection);
                RegistryError::Unavailable
            })?;
            RegistryRepo::get(&conn, &AccessScope::allow_all(), id).await
        })
        .await
    }

    async fn upsert(&self, entry: &RegistryEntry) -> Result<(), RegistryError> {
        self.bounded("upsert", async {
            let conn = self.provider.conn().map_err(|_| {
                log_failure("upsert", FailureCause::Connection);
                RegistryError::Unavailable
            })?;
            RegistryRepo::upsert(&conn, &AccessScope::allow_all(), entry).await
        })
        .await
    }

    async fn delete(&self, id: &str) -> Result<(), RegistryError> {
        self.bounded("delete", async {
            let conn = self.provider.conn().map_err(|_| {
                log_failure("delete", FailureCause::Connection);
                RegistryError::Unavailable
            })?;
            RegistryRepo::delete(&conn, &AccessScope::allow_all(), id)
                .await
                .map(|_| ())
        })
        .await
    }

    async fn list(&self) -> Result<Vec<RegistryEntry>, RegistryError> {
        self.bounded("list", async {
            let conn = self.provider.conn().map_err(|_| {
                log_failure("list", FailureCause::Connection);
                RegistryError::Unavailable
            })?;
            RegistryRepo::list(&conn, &AccessScope::allow_all()).await
        })
        .await
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "store_tests.rs"]
mod store_tests;
