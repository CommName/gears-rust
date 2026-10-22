//! Operation storage: the `operations` table and the insert-only
//! `audit_ledger` (DESIGN 3.7).

pub mod entity;
mod mapping;
pub mod migrations;
pub mod repo;

pub use migrations::Migrator;
pub use repo::SeaOrmOperationRepository;
