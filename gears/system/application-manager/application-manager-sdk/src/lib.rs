//! Application Manager SDK.
//!
//! The public contract of the `application-manager` gear:
//!
//! - [`SectionPluginClientV1`] - the section plugin contract
//! - [`CallerIdentity`], [`Envelope`] - plugin call models
//! - [`SectionPluginError`] - plugin error outcomes
//! - [`SECTION_BASE_TYPE`], [`ApplicationManagerSectionPluginSpecV1`],
//!   [`APPLICATION_RESOURCE_TYPE`] - GTS ids

pub mod error;
pub mod gts;
pub mod models;
pub mod section_plugin_api;

pub use error::SectionPluginError;
pub use gts::{
    APPLICATION_RESOURCE_TYPE, ApplicationManagerSectionPluginSpecV1, OUTCOME_REPORT_TYPE,
    SECTION_BASE_TYPE, section_base_type_schema,
};
pub use models::{
    CallerIdentity, Dependency, Envelope, OperationAction, OutcomeReport, ReportApplication,
    ReportProblem, RequestCategory, SectionOutcome, SectionOutcomeEntry, UnavailableItem,
    UnavailableReasonCode,
};
pub use section_plugin_api::SectionPluginClientV1;
