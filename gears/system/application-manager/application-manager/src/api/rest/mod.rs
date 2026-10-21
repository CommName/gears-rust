//! REST API (`cpt-cf-application-manager-component-rest-api`).

pub mod dto;
pub mod error;
pub mod handlers;
pub mod health;
pub mod routes;

pub use routes::register_routes;
