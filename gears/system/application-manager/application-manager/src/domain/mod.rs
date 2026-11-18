//! Domain layer: actions, the allowed callers, the startup plugin set, and
//! the read service.
//!
//! `infra` builds the startup result and `gear.rs` composes the components
//! (DESIGN 3.2).

pub mod actions;
pub mod allowed_callers;
pub mod error;
pub mod plugin_set;
pub mod read_service;
