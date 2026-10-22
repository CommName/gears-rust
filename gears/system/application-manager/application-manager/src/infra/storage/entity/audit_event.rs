//! `SeaORM` entity of the insert-only `audit_ledger` table (DESIGN 3.7).

use sea_orm::entity::prelude::*;
use serde_json::Value;
use time::OffsetDateTime;
use toolkit_db::secure::Scopable;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Scopable)]
#[sea_orm(table_name = "audit_ledger")]
#[secure(unrestricted)]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_id: Uuid,
    pub kind: String,
    pub correlation_id: Uuid,
    pub action: String,
    pub written_at: OffsetDateTime,
    #[sea_orm(column_type = "JsonBinary")]
    pub content: Value,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
