//! Application Manager gear.
//!
//! Learns which applications are installed in an environment and dispatches
//! each manifest section to the section plugin that owns it.
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod config;
pub mod constants;
pub mod gear;

// === PUBLIC API (from SDK) ===
// Re-exporting the SDK links its rlib, so the section plugin GTS type reaches
// types-registry through the link-time inventory.
pub use application_manager_sdk as sdk;
pub use application_manager_sdk::{
    ApplicationManagerSectionPluginSpecV1, SectionPluginClientV1, SectionPluginError,
};

pub use gear::ApplicationManager;

#[cfg(test)]
mod test_support;
