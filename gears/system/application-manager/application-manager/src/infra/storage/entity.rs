//! `SeaORM` entity of the `application_registry` table (DESIGN §3.7, ADR 0007).
//!
//! The registry is platform-wide: the entity carries no tenant, owner, resource or type
//! column, so it is declared `#[secure(unrestricted)]` and every query runs under the
//! allow-all access scope.

use sea_orm::entity::prelude::*;
use time::OffsetDateTime;
use toolkit_db::secure::Scopable;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Scopable)]
#[sea_orm(table_name = "application_registry")]
#[secure(unrestricted)]
/// One row of `application_registry`.
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub application_id: String,
    pub version: String,
    pub last_result: String,
    pub last_seeded_at: OffsetDateTime,
    pub audit_id: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
/// The entity has no relations.
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "entity_tests.rs"]
mod entity_tests;
