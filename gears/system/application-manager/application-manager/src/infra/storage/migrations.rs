//! Migrator of the registry schema; returned by `DatabaseCapability::migrations()`.
use sea_orm_migration::prelude::*;

pub mod m20261013_000001_create_application_registry;

/// Migrations of the Application Manager gear, in order.
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(
            m20261013_000001_create_application_registry::Migration,
        )]
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "migrations_tests.rs"]
mod migrations_tests;
