//! REST DTOs.
//!
//! Every DTO carries an `ApplicationManager` schema alias: all gears register
//! into one api-gateway `OpenAPI` registry, and a bare name such as
//! `OperationDto` (types-registry has one) would collide in
//! `components.schemas`.

/// The section keys in priority order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(response)]
#[schema(as = ApplicationManagerSectionKeysDto)]
pub struct SectionKeysDto {
    /// Section keys, lowest priority first.
    pub items: Vec<String>,
}

/// One section schema with its section type.
#[derive(Debug, Clone, PartialEq)]
#[toolkit_macros::api_dto(response)]
#[schema(as = ApplicationManagerSectionSchemaDto)]
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
#[schema(as = ApplicationManagerSectionTypeDto)]
pub struct SectionTypeDto {
    /// The section key.
    pub key: String,
    /// The exact GTS id of the section type.
    pub section_type: String,
}

/// The combined schema of a whole manifest.
#[derive(Debug, Clone, PartialEq)]
#[toolkit_macros::api_dto(response)]
#[schema(as = ApplicationManagerManifestSchemaDto)]
pub struct ManifestSchemaDto {
    /// JSON Schema (Draft-07) of a whole manifest: the `application`
    /// envelope and one required section per section key.
    #[schema(value_type = Object)]
    pub schema: serde_json::Value,
    /// Every section's type, in priority order.
    pub sections: Vec<SectionTypeDto>,
}

/// The envelope of a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(request)]
#[schema(as = ApplicationManagerEnvelopeDto)]
pub struct EnvelopeDto {
    /// Application id: non-empty, at most `max_envelope_text_length` bytes,
    /// no control character.
    pub id: String,
    /// Application version, under the same rule.
    pub version: String,
}

/// A manifest: exactly the envelope and one section per section key. A body
/// that is not JSON is refused before it reaches the gear.
#[derive(Debug, Clone, PartialEq)]
#[toolkit_macros::api_dto(request)]
#[schema(as = ApplicationManagerSeedRequestDto)]
pub struct SeedRequestDto {
    /// The envelope.
    pub application: EnvelopeDto,
    /// One member per section key; see `GET /manifest-schema`.
    #[schema(value_type = Object)]
    pub sections: serde_json::Value,
}

/// An uninstall with the application id in the body. A body of any other
/// shape is refused before it reaches the gear.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(request)]
#[serde(deny_unknown_fields)]
#[schema(as = ApplicationManagerUninstallRequestDto)]
pub struct UninstallRequestDto {
    /// The application id, under the envelope text rule.
    pub application_id: String,
}

/// The answer to an accepted seed or uninstall.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(response)]
#[schema(as = ApplicationManagerAcceptedDto)]
pub struct AcceptedDto {
    /// The operation id; poll `GET /operations/{operation_id}`.
    pub operation_id: uuid::Uuid,
    /// Always `accepted`.
    pub status: String,
}

/// One operation record and, once it is final, its outcome report.
#[derive(Debug, Clone, PartialEq)]
#[toolkit_macros::api_dto(response)]
#[schema(as = ApplicationManagerOperationDto)]
pub struct OperationDto {
    /// The operation id.
    pub operation_id: uuid::Uuid,
    /// The application id.
    pub application_id: String,
    /// `seed` or `uninstall`.
    pub action: String,
    /// `accepted`, `running` or `completed`.
    pub status: String,
    /// The outcome category; set only when `completed`.
    pub category: Option<String>,
    /// Database time of the acceptance, RFC 3339.
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
