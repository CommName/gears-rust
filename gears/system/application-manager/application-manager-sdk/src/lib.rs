//! Application Manager SDK.
//!
//! The public contract of the `application-manager` gear:
//!
//! - [`SectionPluginClientV1`] - the section plugin contract
//! - [`CallerIdentity`], [`Envelope`], [`SectionSchema`] - plugin call models
//! - [`CallerIdentityError`] - caller identity conversion failure
//! - [`SectionPluginError`] - plugin error outcomes
//! - [`SCHEMA_VERSION_MAX_BYTES`], [`PLUGIN_DETAIL_MAX_BYTES`], [`SERVED_SCHEMA_MAX_BYTES`] - limits
//! - [`ApplicationManagerSectionPluginSpecV1`], [`APPLICATION_RESOURCE_TYPE`] - GTS ids

pub mod error;
pub mod gts;
pub mod limits;
pub mod models;
pub mod section_plugin_api;

pub use error::SectionPluginError;
pub use gts::{APPLICATION_RESOURCE_TYPE, ApplicationManagerSectionPluginSpecV1};
pub use limits::{PLUGIN_DETAIL_MAX_BYTES, SCHEMA_VERSION_MAX_BYTES, SERVED_SCHEMA_MAX_BYTES};
pub use models::{CallerIdentity, CallerIdentityError, Envelope, SectionSchema};
pub use section_plugin_api::SectionPluginClientV1;
