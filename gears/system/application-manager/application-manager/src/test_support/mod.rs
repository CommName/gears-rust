//! Test-only support: clock, id and startup doubles and the fake section and audit
//! plugins (DESIGN §3.2, §4.8).

mod doubles;
mod fakes;
mod log_capture;
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
