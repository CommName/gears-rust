//! Domain layer: actions, ports, the startup plugin set, and the read service.
//!
//! Domain components call ports; `infra` implements them and `gear.rs`
//! composes them (DESIGN 3.2).

pub mod actions;
pub mod error;
pub mod plugin_set;
pub mod ports;
pub mod read_service;
