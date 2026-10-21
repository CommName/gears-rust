//! Constants of the gear.

use std::time::Duration;

/// The gateway request deadline the synchronous budget must fit in (DESIGN 4.3).
pub const GATEWAY_REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// Upper bound of `max_envelope_text_length`, and so of the application id and
/// version, in bytes (DESIGN 4.2). DESIGN names the constant without a value;
/// 255 is the implementation's choice.
pub const APPLICATION_ID_MAX_BYTES: u32 = 255;
