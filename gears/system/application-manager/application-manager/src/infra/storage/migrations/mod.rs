//! Database migrations of the Application Manager gear.

use sea_orm_migration::MigratorTrait;

mod m20261022_000001_initial;

/// Migrator of the `operations` and `audit_ledger` tables.
pub struct Migrator;

impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn sea_orm_migration::MigrationTrait>> {
        vec![Box::new(m20261022_000001_initial::Migration)]
    }
}
