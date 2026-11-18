//! Operation storage: the `operations` table, the insert-only
//! `operation_history`, and `applications` (DESIGN 3.7).

pub mod entity;
mod mapping;
pub mod migrations;
pub mod repo;

pub use migrations::Migrator;
pub use repo::SeaOrmOperationRepository;
