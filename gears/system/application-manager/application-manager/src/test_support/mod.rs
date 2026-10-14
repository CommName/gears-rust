//! Test-only support: clock, id and startup doubles and the fake section and audit
//! plugins (DESIGN §3.2, §4.8).

mod authz_fakes;
mod doubles;
mod fakes;
mod gate_harness;
mod log_capture;
mod request_doubles;
mod request_helpers;
// Manifest parser and schema validator tests (P5): generators of JSON values and nesting.
pub mod manifest_gen;
// Registry persistence (P4): migrated test databases and the backend-independent cases
// that the SQLite, Postgres and MySQL lanes all run.
pub mod registry_cases;
pub mod registry_db;
#[allow(
    clippy::doc_markdown,
    clippy::manual_assert_eq,
    clippy::let_underscore_must_use
)]
mod startup_fakes;

pub use authz_fakes::*;
pub use doubles::*;
pub use fakes::*;
pub use gate_harness::*;
pub use log_capture::*;
pub use request_doubles::*;
pub use request_helpers::*;
pub use startup_fakes::*;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "p7_doubles_tests.rs"]
mod p7_doubles_tests;
