//! SeaQuery [`Iden`] enums for the plugin's ClickHouse tables.
//!
//! Table names come from the enum name (`UsageRecords` → `usage_records`).
//! These identifiers are the closed allowlist surface for SELECT column lists
//! and filter/order resolution — never interpolate caller-supplied names.

use sea_query::Iden;

/// `usage_records` table and columns (see `migrations/0001_init.sql`).
#[derive(Debug, Clone, Copy, Iden)]
pub enum UsageRecords {
    Table,
    Id,
    TenantId,
    GtsId,
    Value,
    CreatedAt,
    ResourceId,
    ResourceType,
    SubjectId,
    SubjectType,
    IdempotencyKey,
    CorrectsId,
    Status,
    Metadata,
    IngestedAt,
    Version,
}

/// Columns selected for every typed [`crate::infra::storage::entity::UsageRecordRow`]
/// fetch, in `RowBinary` field order.
pub const RECORD_SELECT_COLUMNS: [UsageRecords; 15] = [
    UsageRecords::Id,
    UsageRecords::TenantId,
    UsageRecords::GtsId,
    UsageRecords::Value,
    UsageRecords::CreatedAt,
    UsageRecords::ResourceId,
    UsageRecords::ResourceType,
    UsageRecords::SubjectId,
    UsageRecords::SubjectType,
    UsageRecords::IdempotencyKey,
    UsageRecords::CorrectsId,
    UsageRecords::Status,
    UsageRecords::Metadata,
    UsageRecords::IngestedAt,
    UsageRecords::Version,
];

/// `usage_type_catalog` table and columns.
#[derive(Debug, Clone, Copy, Iden)]
pub enum UsageTypeCatalog {
    Table,
    GtsId,
    Kind,
    MetadataFields,
    Version,
}

/// Columns selected for every typed [`crate::infra::storage::entity::UsageTypeRow`]
/// fetch, in `RowBinary` field order.
pub const TYPE_SELECT_COLUMNS: [UsageTypeCatalog; 4] = [
    UsageTypeCatalog::GtsId,
    UsageTypeCatalog::Kind,
    UsageTypeCatalog::MetadataFields,
    UsageTypeCatalog::Version,
];

/// Map a `usage_records` OData filter/order field name to its column [`Iden`].
///
/// `gts_id` is intentionally absent: it is a typed SPI parameter, not a
/// `$filter` field.
#[must_use]
pub fn record_column_iden(field_name: &str) -> Option<UsageRecords> {
    match field_name {
        "id" => Some(UsageRecords::Id),
        "created_at" => Some(UsageRecords::CreatedAt),
        "tenant_id" => Some(UsageRecords::TenantId),
        "resource_id" => Some(UsageRecords::ResourceId),
        "resource_type" => Some(UsageRecords::ResourceType),
        "subject_id" => Some(UsageRecords::SubjectId),
        "subject_type" => Some(UsageRecords::SubjectType),
        "corrects_id" => Some(UsageRecords::CorrectsId),
        "status" => Some(UsageRecords::Status),
        _ => None,
    }
}

/// Map a `usage_type_catalog` OData filter/order field name to its column [`Iden`].
#[must_use]
pub fn usage_type_column_iden(field_name: &str) -> Option<UsageTypeCatalog> {
    match field_name {
        "gts_id" => Some(UsageTypeCatalog::GtsId),
        "kind" => Some(UsageTypeCatalog::Kind),
        _ => None,
    }
}
