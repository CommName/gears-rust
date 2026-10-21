//! REST DTOs.

/// The section keys in priority order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(response)]
pub struct SectionKeysDto {
    /// Section keys, lowest priority first.
    pub items: Vec<String>,
}

/// One live section schema with its schema version.
#[derive(Debug, Clone, PartialEq)]
#[toolkit_macros::api_dto(response)]
pub struct SectionSchemaDto {
    /// The section key.
    pub key: String,
    /// The section's JSON Schema (Draft 2020-12), with the `$id`
    /// `urn:cf:application-manager:section:<key>`.
    #[schema(value_type = Object)]
    pub schema: serde_json::Value,
    /// Changes whenever the schema content changes.
    pub schema_version: String,
}

/// The schema version of one section.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(response)]
pub struct SectionVersionDto {
    /// The section key.
    pub key: String,
    /// Changes whenever the section's schema content changes.
    pub schema_version: String,
}

/// The live combined schema of a whole manifest.
#[derive(Debug, Clone, PartialEq)]
#[toolkit_macros::api_dto(response)]
pub struct ManifestSchemaDto {
    /// JSON Schema (Draft 2020-12) of a whole manifest: the `application`
    /// envelope and one required section per section key.
    #[schema(value_type = Object)]
    pub schema: serde_json::Value,
    /// Every section's schema version, in priority order.
    pub sections: Vec<SectionVersionDto>,
}
