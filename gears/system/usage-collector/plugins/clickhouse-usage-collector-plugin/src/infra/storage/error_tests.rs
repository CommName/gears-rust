use clickhouse::error::Error as ChError;
use usage_collector_sdk::UsageCollectorPluginError;

use super::map_ch_err;

#[test]
fn network_error_maps_to_transient() {
    let err = ChError::Network(Box::new(std::io::Error::other("connection refused")));
    assert!(matches!(
        map_ch_err(&err),
        UsageCollectorPluginError::Transient { .. }
    ));
}

#[test]
fn timed_out_maps_to_transient() {
    let err = ChError::TimedOut;
    assert!(matches!(
        map_ch_err(&err),
        UsageCollectorPluginError::Transient { .. }
    ));
}

#[test]
fn row_not_found_maps_to_internal() {
    let err = ChError::RowNotFound;
    assert!(matches!(
        map_ch_err(&err),
        UsageCollectorPluginError::Internal(_)
    ));
}

/// `BadResponse` is classified as transient after F-002 — protocol-level
/// responses that look syntactically wrong (e.g. an unexpected HTTP 5xx body)
/// are worth retrying rather than failing permanently.
#[test]
fn bad_response_maps_to_transient() {
    let err = ChError::BadResponse("unexpected response".to_owned());
    assert!(matches!(
        map_ch_err(&err),
        UsageCollectorPluginError::Transient { .. }
    ));
}
