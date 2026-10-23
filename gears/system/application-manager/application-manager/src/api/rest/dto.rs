//! REST DTOs.

/// The section keys in priority order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(response)]
pub struct SectionKeysDto {
    /// Section keys, lowest priority first.
    pub items: Vec<String>,
}

/// One section schema with its section type.
#[derive(Debug, Clone, PartialEq)]
#[toolkit_macros::api_dto(response)]
pub struct SectionSchemaDto {
    /// The section key.
    pub key: String,
    /// The exact GTS id of the section type.
    pub section_type: String,
    /// The section type's JSON Schema (Draft-07), with the `$id`
    /// `urn:cf:application-manager:section:<key>`.
    #[schema(value_type = Object)]
    pub schema: serde_json::Value,
}

/// The section type of one section.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(response)]
pub struct SectionTypeDto {
    /// The section key.
    pub key: String,
    /// The exact GTS id of the section type.
    pub section_type: String,
}

/// The combined schema of a whole manifest.
#[derive(Debug, Clone, PartialEq)]
#[toolkit_macros::api_dto(response)]
pub struct ManifestSchemaDto {
    /// JSON Schema (Draft-07) of a whole manifest: the `application`
    /// envelope and one required section per section key.
    #[schema(value_type = Object)]
    pub schema: serde_json::Value,
    /// Every section's type, in priority order.
    pub sections: Vec<SectionTypeDto>,
}

/// The answer to an accepted seed or uninstall.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(response)]
pub struct AcceptedDto {
    /// The operation id; poll `GET /operations/{operation_id}`.
    pub operation_id: uuid::Uuid,
    /// Always `accepted`.
    pub status: String,
}

/// One operation record and, once it is final, its outcome report.
#[derive(Debug, Clone, PartialEq)]
#[toolkit_macros::api_dto(response)]
pub struct OperationDto {
    /// The operation id.
    pub operation_id: uuid::Uuid,
    /// The application id.
    pub application_id: String,
    /// `seed` or `uninstall`.
    pub action: String,
    /// `accepted`, `running`, `completed` or `interrupted`.
    pub status: String,
    /// The outcome category; set only when `completed`.
    pub category: Option<String>,
    /// Receive time, RFC 3339.
    pub accepted_at: String,
    /// Start time, RFC 3339.
    pub started_at: Option<String>,
    /// End time, RFC 3339; set only when final.
    pub finished_at: Option<String>,
    /// End of the run window, RFC 3339.
    pub run_deadline: String,
    /// The outcome report; present only once the operation is final.
    #[schema(value_type = Option<Object>)]
    pub report: Option<serde_json::Value>,
}
