//! REST DTOs.

/// The section keys in priority order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[toolkit_macros::api_dto(response)]
pub struct SectionKeysDto {
    /// Section keys, lowest priority first.
    pub items: Vec<String>,
}
