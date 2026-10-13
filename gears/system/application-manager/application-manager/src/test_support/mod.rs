//! Test-only support: clock, id and startup doubles and the fake section and audit
//! plugins (DESIGN §3.2, §4.8).

mod doubles;
mod fakes;

pub use doubles::*;
pub use fakes::*;
