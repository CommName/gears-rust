//! Application Manager SDK: the normative contract between the `application-manager`
//! gear and its plugins (DESIGN §3.3).
//!
//! - [`SectionPluginClientV1`]: a section plugin owns one top-level manifest section.
//! - [`AuditPluginClientV1`]: the single audit plugin records each request.
//! - [`SectionPluginError`] / [`AuditPluginError`]: plugin error kinds, with bounded text.
//! - [`CallerIdentity`], [`ApplicationEnvelope`], [`RequestHandoffContent`],
//!   [`FinalResultContent`], [`OutcomeReport`]: the shared models.
//! - [`ApplicationManagerSectionPluginSpecV1`] / [`ApplicationManagerAuditPluginSpecV1`]:
//!   GTS plugin specs for discovery.
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod audit_plugin_api;
pub mod errors;
pub mod gts;
pub mod models;
pub mod section_plugin_api;

pub use audit_plugin_api::AuditPluginClientV1;
pub use errors::{AuditPluginError, SectionPluginError};
pub use gts::{ApplicationManagerAuditPluginSpecV1, ApplicationManagerSectionPluginSpecV1};
pub use models::{
    ApplicationEnvelope, CallerIdentity, FinalResultContent, OutcomeReport, RequestHandoffContent,
};
pub use section_plugin_api::SectionPluginClientV1;
