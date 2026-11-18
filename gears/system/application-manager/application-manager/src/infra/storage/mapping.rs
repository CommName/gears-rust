//! Mapping between domain types and table rows.
//!
//! Enums are stored as their snake-case tokens (the `CHECK` constraints of the
//! migration list the same tokens). A row that does not map back is corrupt;
//! it is reported as [`RowError`], never silently repaired.

use application_manager_sdk::{OperationAction, OutcomeReport, RequestCategory};
use sea_orm::ActiveValue::Set;
use sea_orm::{DbBackend, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;
use time::{OffsetDateTime, UtcOffset};

use super::entity::{application, history_event, operation};
use crate::domain::history::{HistoryEvent, HistoryKind};
use crate::domain::operation::{ApplicationRecord, NewOperation, Operation, OperationStatus};

/// A stored row does not map to a domain value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("stored row does not map to a domain value")]
pub struct RowError;

/// The instant `backend` stores for `at`: UTC, and on `SQLite` truncated to
/// whole milliseconds, the precision of [`stored_value`]. A comparison made in
/// Rust then sees the same instant the database holds.
pub fn stored_time(backend: DbBackend, at: OffsetDateTime) -> OffsetDateTime {
    let at = at.to_offset(UtcOffset::UTC);
    if backend == DbBackend::Sqlite {
        at.replace_millisecond(at.millisecond()).unwrap_or(at)
    } else {
        at
    }
}

/// The value `backend` binds for `value`. `SQLite` stores a timestamp as
/// `TEXT` and compares it as text, which is time order only at a fixed width:
/// the driver's RFC 3339 trims the fraction, so `10:01:00Z` would sort after
/// `10:01:00.5Z`. There a timestamp binds as `YYYY-MM-DD HH:MM:SS.SSS` in UTC.
/// Any other value, and any other backend, passes through unchanged.
pub fn stored_value(backend: DbBackend, value: Value) -> Value {
    match value {
        Value::TimeDateTimeWithTimeZone(Some(at)) if backend == DbBackend::Sqlite => {
            let at = stored_time(backend, at);
            Value::String(Some(format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
                at.year(),
                u8::from(at.month()),
                at.day(),
                at.hour(),
                at.minute(),
                at.second(),
                at.millisecond(),
            )))
        }
        value => value,
    }
}

pub const fn status_token(status: OperationStatus) -> &'static str {
    match status {
        OperationStatus::Accepted => "accepted",
        OperationStatus::Running => "running",
        OperationStatus::Completed => "completed",
    }
}

fn parse_status(token: &str) -> Result<OperationStatus, RowError> {
    match token {
        "accepted" => Ok(OperationStatus::Accepted),
        "running" => Ok(OperationStatus::Running),
        "completed" => Ok(OperationStatus::Completed),
        _ => Err(RowError),
    }
}

const fn kind_token(kind: HistoryKind) -> &'static str {
    match kind {
        HistoryKind::Accepted => "accepted",
        HistoryKind::Finished => "finished",
        HistoryKind::Refused => "refused",
    }
}

fn parse_kind(token: &str) -> Result<HistoryKind, RowError> {
    match token {
        "accepted" => Ok(HistoryKind::Accepted),
        "finished" => Ok(HistoryKind::Finished),
        "refused" => Ok(HistoryKind::Refused),
        _ => Err(RowError),
    }
}

/// The snake-case token of an SDK enum.
pub fn enum_token<T: Serialize>(value: &T) -> Result<String, RowError> {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(token)) => Ok(token),
        _ => Err(RowError),
    }
}

fn parse_enum<T: DeserializeOwned>(token: &str) -> Result<T, RowError> {
    serde_json::from_value(serde_json::Value::String(token.to_owned())).map_err(|_| RowError)
}

pub fn report_json(report: &OutcomeReport) -> Result<serde_json::Value, RowError> {
    serde_json::to_value(report).map_err(|_| RowError)
}

/// The row the acceptance transaction inserts for `new`, accepted at
/// `accepted_at` with the run deadline `run_deadline`, both by database time.
pub fn operation_row(
    backend: DbBackend,
    new: &NewOperation,
    accepted_at: OffsetDateTime,
    run_deadline: OffsetDateTime,
) -> Result<operation::ActiveModel, RowError> {
    Ok(operation::ActiveModel {
        operation_id: Set(new.operation_id),
        application_id: Set(new.application_id.clone()),
        action: Set(enum_token(&new.action)?),
        status: Set(status_token(OperationStatus::Accepted).to_owned()),
        category: Set(None),
        report: Set(report_json(&new.report)?),
        accepted_at: Set(stored_time(backend, accepted_at)),
        started_at: Set(None),
        finished_at: Set(None),
        run_deadline: Set(stored_time(backend, run_deadline)),
        finished_event_id: Set(new.finished_event_id),
    })
}

pub fn operation_from_row(row: operation::Model) -> Result<Operation, RowError> {
    Ok(Operation {
        operation_id: row.operation_id,
        application_id: row.application_id,
        action: parse_enum::<OperationAction>(&row.action)?,
        status: parse_status(&row.status)?,
        category: row
            .category
            .as_deref()
            .map(parse_enum::<RequestCategory>)
            .transpose()?,
        report: serde_json::from_value(row.report).map_err(|_| RowError)?,
        accepted_at: row.accepted_at,
        started_at: row.started_at,
        finished_at: row.finished_at,
        run_deadline: row.run_deadline,
        finished_event_id: row.finished_event_id,
    })
}

/// The row of `event`, written at `written_at` by database time.
pub fn event_row(
    backend: DbBackend,
    event: &HistoryEvent,
    written_at: OffsetDateTime,
) -> Result<history_event::ActiveModel, RowError> {
    Ok(history_event::ActiveModel {
        event_id: Set(event.event_id),
        kind: Set(kind_token(event.kind).to_owned()),
        correlation_id: Set(event.correlation_id),
        action: Set(enum_token(&event.action)?),
        written_at: Set(stored_time(backend, written_at)),
        content: Set(event.content.clone()),
    })
}

pub fn event_from_row(row: history_event::Model) -> Result<HistoryEvent, RowError> {
    Ok(HistoryEvent {
        event_id: row.event_id,
        kind: parse_kind(&row.kind)?,
        correlation_id: row.correlation_id,
        action: parse_enum::<OperationAction>(&row.action)?,
        written_at: row.written_at,
        content: row.content,
    })
}

pub fn application_row(
    backend: DbBackend,
    record: &ApplicationRecord,
) -> Result<application::ActiveModel, RowError> {
    Ok(application::ActiveModel {
        application_id: Set(record.application_id.clone()),
        version: Set(record.version.clone()),
        operation_id: Set(record.operation_id),
        category: Set(enum_token(&record.category)?),
        finished_at: Set(stored_time(backend, record.finished_at)),
    })
}

pub fn application_from_row(row: application::Model) -> Result<ApplicationRecord, RowError> {
    Ok(ApplicationRecord {
        application_id: row.application_id,
        version: row.version,
        operation_id: row.operation_id,
        category: parse_enum::<RequestCategory>(&row.category)?,
        finished_at: row.finished_at,
    })
}
