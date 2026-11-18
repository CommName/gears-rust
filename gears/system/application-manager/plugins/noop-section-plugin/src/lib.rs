//! No-op section plugin for the Application Manager.
//!
//! Registers the section type `gts.cf.core.application_manager.section.v1~cf.core.noop.section.v1~`
//! (key `cf.core.noop.section`), whose schema accepts any object, and accepts
//! every `apply` and `remove` without doing anything. It shows how a
//! section plugin is added with no change to the Application Manager
//! (`cpt-cf-application-manager-fr-plugin-extensibility`), and gives the gear
//! a plugin to run against in tests.
//!
//! ## Configuration
//!
//! ```yaml
//! gears:
//!   noop-section-plugin:
//!     config:
//!       vendor: "constructorfabric"
//!       priority: 100
//! ```
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod client;
pub mod config;
pub mod gear;
pub mod section;

pub use gear::NoopSectionPlugin;
