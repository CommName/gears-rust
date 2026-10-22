//! `SeaORM` entity of the `operations` table (DESIGN 3.7).
//!
//! The `MySQL` generated column `open_application_id` is not mapped: the
//! database maintains it, and no statement reads or writes it.

use sea_orm::entity::prelude::*;
use serde_json::Value;
use time::OffsetDateTime;
use toolkit_db::secure::Scopable;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Scopable)]
#[sea_orm(table_name = "operations")]
#[secure(unrestricted)]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub operation_id: Uuid,
    pub application_id: String,
    pub action: String,
    pub status: String,
    pub category: Option<String>,
    #[sea_orm(column_type = "JsonBinary")]
    pub report: Value,
    pub accepted_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub run_deadline: OffsetDateTime,
    pub finished_event_id: Uuid,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
