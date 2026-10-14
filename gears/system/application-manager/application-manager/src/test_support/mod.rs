//! Test-only support: clock, id and startup doubles and the fake section and audit
//! plugins (DESIGN §3.2, §4.8).

mod doubles;
mod fakes;
mod log_capture;
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

pub use doubles::*;
pub use fakes::*;
pub use log_capture::*;
pub use startup_fakes::*;
