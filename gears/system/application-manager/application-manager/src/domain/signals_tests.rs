use super::*;

#[test]
fn labels_match_the_wire_tokens() {
    let token = |value: serde_json::Value| value.as_str().map(str::to_owned);
    for action in [OperationAction::Seed, OperationAction::Uninstall] {
        assert_eq!(
            Some(action_label(action).to_owned()),
            token(serde_json::to_value(action).unwrap_or_default())
        );
    }
    for category in [
        RequestCategory::Succeeded,
        RequestCategory::InvalidManifest,
        RequestCategory::PluginRejected,
        RequestCategory::NotPermitted,
        RequestCategory::Conflict,
        RequestCategory::InternalFailure,
        RequestCategory::TemporarilyUnavailable,
    ] {
        assert_eq!(
            Some(category_label(category).to_owned()),
            token(serde_json::to_value(category).unwrap_or_default())
        );
    }
    for outcome in [
        SectionOutcome::Succeeded,
        SectionOutcome::Rejected,
        SectionOutcome::NotPermitted,
        SectionOutcome::InternalFailure,
        SectionOutcome::TimedOut,
        SectionOutcome::NotSent,
        SectionOutcome::NotDispatched,
        SectionOutcome::Lost,
    ] {
        assert_eq!(
            Some(outcome_label(outcome).to_owned()),
            token(serde_json::to_value(outcome).unwrap_or_default())
        );
    }
}

#[test]
fn no_signals_accepts_every_signal() {
    let signals: &dyn Signals = &NoSignals;
    signals.request("seed", "accepted");
    signals.operation_completed("seed", "succeeded", "run");
    signals.operation_duration("seed", Duration::from_secs(1));
    signals.section_call("noop", "succeeded");
    signals.caller_refused("seed");
    signals.request_tasks(1);
    signals.history_event(true);
    signals.dependency_unavailable("storage");
    signals.sweep("expire", true);
    signals.operations_expired(1);
    signals.startup_failed("plugin_conflict");
}
