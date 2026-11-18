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
