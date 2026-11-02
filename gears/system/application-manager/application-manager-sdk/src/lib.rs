//! Application Manager SDK.
//!
//! The public contract of the `application-manager` gear:
//!
//! - [`SectionPluginClientV1`] - the section plugin contract
//! - [`CallerIdentity`], [`Envelope`] - plugin call models
//! - [`SectionPluginError`] - plugin error outcomes
//! - [`ApplicationManagerSectionV1`], [`ApplicationManagerSectionPluginSpecV1`],
//!   [`APPLICATION_RESOURCE_TYPE`] - GTS ids

pub mod error;
pub mod gts;
pub mod models;
pub mod section_plugin_api;

pub use error::SectionPluginError;
pub use gts::{
    APPLICATION_RESOURCE_TYPE, ApplicationManagerSectionPluginSpecV1, ApplicationManagerSectionV1,
};
pub use models::{CallerIdentity, Envelope};
pub use section_plugin_api::SectionPluginClientV1;
