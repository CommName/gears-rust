//! Operation history events and their content (DESIGN 3.1 "History event
//! content").
//!
//! Ledger rows carry no manifest content: no section body, no problem
//! location or message, and no plugin detail.

use std::collections::BTreeSet;

use application_manager_sdk::{
    CallerIdentity, OperationAction, OutcomeReport, ReportProblem, RequestCategory, UnavailableItem,
};
use serde_json::{Map, Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use toolkit_macros::domain_model;
use uuid::Uuid;

use crate::domain::operation::CompletionReason;

/// The kind of an history event.
#[domain_model]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryKind {
    /// An accepted request.
    Accepted,
    /// A finished operation, including an expiry or supervisor completion.
    Finished,
    /// A refused request.
    Refused,
}

/// One history row (`operation_history` table, DESIGN 3.7).
#[domain_model]
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryEvent {
    /// Time-ordered event id, assigned at receive.
    pub event_id: Uuid,
    /// The kind.
    pub kind: HistoryKind,
    /// The operation id of an accepted request, else the request id.
    pub correlation_id: Uuid,
    /// `seed` or `uninstall`.
    pub action: OperationAction,
    /// Write time: the database time of the transaction that writes the
    /// event. The repository sets it on insert; a value built before the
    /// write is ignored.
    pub written_at: OffsetDateTime,
    /// The content built by [`accepted`], [`finished`] or [`refused`].
    pub content: Value,
}

/// One envelope text of an event: as sent when within the limits, else its
/// byte length and SHA-256 digest.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedText {
    /// The text, within the limits.
    Within(String),
    /// A text outside the limits.
    Digest {
        /// Byte length of the text.
        bytes: usize,
        /// Lower-case hex SHA-256 of the text.
        sha256: String,
    },
}

impl BoundedText {
    /// [`BoundedText::Within`] when `text` is at most `max_bytes` bytes,
    /// otherwise [`BoundedText::Digest`].
    #[must_use]
    pub fn of(text: &str, max_bytes: usize) -> Self {
        if text.len() <= max_bytes {
            Self::Within(text.to_owned())
        } else {
            Self::Digest {
                bytes: text.len(),
                sha256: sha256_hex(text),
            }
        }
    }
}

/// The application of an event. Each of the id and the version is kept as
/// sent when within the limits, else as its byte length and SHA-256 digest.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationRef {
    /// Application id.
    pub id: BoundedText,
    /// Application version; none for uninstall.
    pub version: Option<BoundedText>,
}

impl ApplicationRef {
    /// The id and the version (when present), each bounded at `max_bytes`
    /// bytes on its own.
    #[must_use]
    pub fn bounded(id: &str, version: Option<&str>, max_bytes: usize) -> Self {
        Self {
            id: BoundedText::of(id, max_bytes),
            version: version.map(|v| BoundedText::of(v, max_bytes)),
        }
    }
}

/// Section keys of a seed: the keys of the key set that the manifest carries,
/// and a count of the other keys.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionKeys {
    /// Keys of the key set.
    pub keys: Vec<String>,
    /// Number of keys outside the key set.
    pub other_keys: usize,
}

/// The result of a refused request.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefusedResult {
    /// The outcome category.
    pub category: RequestCategory,
    /// Problems found; only their count and rule tokens are recorded.
    pub problems: Vec<ReportProblem>,
    /// The open operation of a conflict.
    pub busy_operation_id: Option<Uuid>,
    /// Unavailable items.
    pub unavailable: Vec<UnavailableItem>,
}

/// Lower-case hex SHA-256 of `text`.
fn sha256_hex(text: &str) -> String {
    use std::fmt::Write as _;

    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, text.as_bytes());
    digest.as_ref().iter().fold(String::new(), |mut hex, byte| {
        // Writing to a `String` cannot fail.
        _ = write!(hex, "{byte:02x}");
        hex
    })
}

/// Insert `text` as `name`, or as `name_bytes` and `name_sha256`.
fn insert_text(object: &mut Map<String, Value>, name: &str, text: &BoundedText) {
    match text {
        BoundedText::Within(text) => {
            object.insert(name.to_owned(), json!(text));
        }
        BoundedText::Digest { bytes, sha256 } => {
            object.insert(format!("{name}_bytes"), json!(bytes));
            object.insert(format!("{name}_sha256"), json!(sha256));
        }
    }
}

fn application_json(application: &ApplicationRef) -> Value {
    let mut object = Map::new();
    insert_text(&mut object, "id", &application.id);
    if let Some(version) = &application.version {
        insert_text(&mut object, "version", version);
    }
    Value::Object(object)
}

fn section_keys_json(section_keys: &SectionKeys) -> Value {
    json!({ "keys": section_keys.keys, "other_keys": section_keys.other_keys })
}

/// The caller as the plugins receive it: ids only, never a token.
fn caller_json(caller: &CallerIdentity) -> Value {
    serde_json::to_value(caller).unwrap_or(Value::Null)
}

fn timestamp_json(at: OffsetDateTime) -> Value {
    Value::String(at.format(&Rfc3339).unwrap_or_default())
}

/// The distinct rule tokens of `problems`, sorted. A problem's location and
/// message carry manifest content and are never recorded.
fn rule_tokens(problems: &[ReportProblem]) -> Vec<&str> {
    problems
        .iter()
        .map(|problem| problem.rule.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// A section outcome as the operation history records it: key, section type
/// and outcome. The plugin's detail and location are not recorded.
fn section_json(entry: &application_manager_sdk::SectionOutcomeEntry) -> Value {
    let mut object = Map::new();
    object.insert("key".to_owned(), json!(entry.key));
    if let Some(section_type) = &entry.section_type {
        object.insert("section_type".to_owned(), json!(section_type));
    }
    if let Some(outcome) = entry.outcome {
        object.insert(
            "outcome".to_owned(),
            serde_json::to_value(outcome).unwrap_or(Value::Null),
        );
    }
    Value::Object(object)
}

fn unavailable_json(items: &[UnavailableItem]) -> Value {
    serde_json::to_value(items).unwrap_or(Value::Null)
}

/// Content of an accepted event: `application`, `section_keys` (seed only),
/// `caller` and `received_at`.
#[must_use]
pub fn accepted(
    application: &ApplicationRef,
    section_keys: Option<&SectionKeys>,
    caller: &CallerIdentity,
    received_at: OffsetDateTime,
) -> Value {
    let mut content = Map::new();
    content.insert("application".to_owned(), application_json(application));
    if let Some(keys) = section_keys {
        content.insert("section_keys".to_owned(), section_keys_json(keys));
    }
    content.insert("caller".to_owned(), caller_json(caller));
    content.insert("received_at".to_owned(), timestamp_json(received_at));
    Value::Object(content)
}

/// Content of a finished event: `application` and `result`, with the reason
/// `expired` for an expiry completion.
#[must_use]
pub fn finished(report: &OutcomeReport, reason: Option<CompletionReason>) -> Value {
    // An accepted application was within the limits.
    let application = ApplicationRef {
        id: BoundedText::Within(report.application.id.clone()),
        version: report.application.version.clone().map(BoundedText::Within),
    };
    let mut result = Map::new();
    if let Some(category) = report.category {
        result.insert(
            "category".to_owned(),
            serde_json::to_value(category).unwrap_or(Value::Null),
        );
    }
    if let Some(CompletionReason::Expired) = reason {
        result.insert("reason".to_owned(), json!("expired"));
    }
    result.insert("problem_count".to_owned(), json!(report.problems.len()));
    result.insert("rules".to_owned(), json!(rule_tokens(&report.problems)));
    result.insert(
        "sections".to_owned(),
        Value::Array(report.sections.iter().map(section_json).collect()),
    );
    result.insert(
        "unavailable".to_owned(),
        unavailable_json(&report.unavailable),
    );
    json!({
        "application": application_json(&application),
        "result": Value::Object(result),
    })
}

/// Content of a refused event: `application`, `section_keys` (when the
/// sections are an object), `caller`, `received_at` and `result`.
#[must_use]
pub fn refused(
    application: &ApplicationRef,
    section_keys: Option<&SectionKeys>,
    caller: &CallerIdentity,
    received_at: OffsetDateTime,
    result: &RefusedResult,
) -> Value {
    let mut outcome = Map::new();
    outcome.insert(
        "category".to_owned(),
        serde_json::to_value(result.category).unwrap_or(Value::Null),
    );
    outcome.insert("problem_count".to_owned(), json!(result.problems.len()));
    outcome.insert("rules".to_owned(), json!(rule_tokens(&result.problems)));
    if let Some(busy) = result.busy_operation_id {
        outcome.insert("busy_operation_id".to_owned(), json!(busy.to_string()));
    }
    if !result.unavailable.is_empty() {
        outcome.insert(
            "unavailable".to_owned(),
            unavailable_json(&result.unavailable),
        );
    }

    let mut content = Map::new();
    content.insert("application".to_owned(), application_json(application));
    if let Some(keys) = section_keys {
        content.insert("section_keys".to_owned(), section_keys_json(keys));
    }
    content.insert("caller".to_owned(), caller_json(caller));
    content.insert("received_at".to_owned(), timestamp_json(received_at));
    content.insert("result".to_owned(), Value::Object(outcome));
    Value::Object(content)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "history_tests.rs"]
mod tests;
