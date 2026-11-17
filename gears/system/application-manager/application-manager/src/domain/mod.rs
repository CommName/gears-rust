//! Domain layer: actions, the allowed callers, the startup plugin set, the
//! schema validator, the section plugin router, ports, and the read service.
//!
//! Domain components call ports; `infra` implements them and `gear.rs`
//! composes them (DESIGN 3.2).

pub mod actions;
pub mod allowed_callers;
pub mod error;
pub mod history;
pub mod operation;
pub mod plugin_set;
pub mod ports;
pub mod read_service;
pub mod router;
pub mod schema_validator;
