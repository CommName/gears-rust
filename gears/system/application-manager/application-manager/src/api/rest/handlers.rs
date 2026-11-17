//! REST handlers.

use std::sync::Arc;

use axum::extract::{Extension, Path};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use toolkit::api::canonical_prelude::*;
use toolkit_security::PlatformSecurityContext;
use uuid::Uuid;

use super::dto::{
    AcceptedDto, ManifestSchemaDto, OperationDto, SectionKeysDto, SectionSchemaDto, SectionTypeDto,
};
use super::error::to_canonical;
use super::gate::RequestGate;
use crate::domain::error::DomainError;
use crate::domain::operation::{Operation, OperationStatus};
use crate::domain::orchestrator::{Receipt, RequestInput, RequestOrchestrator};
use crate::domain::read_service::ReadService;

/// `GET /application-manager/v1/section-keys`.
///
/// # Errors
/// 503 before startup ends; 403 when the allowed callers do not grant
/// `read_schema`.
pub async fn list_section_keys(
    Extension(ctx): Extension<PlatformSecurityContext>,
    Extension(svc): Extension<Arc<ReadService>>,
) -> ApiResult<Json<SectionKeysDto>> {
    let request_id = Uuid::now_v7();
    let span = tracing::info_span!("list_section_keys", %request_id);
    let items = span
        .in_scope(|| svc.list_section_keys(&ctx))
        .map_err(|e| refusal(&span, &e, request_id))?;
    Ok(Json(SectionKeysDto { items }))
}

/// `GET /application-manager/v1/section-keys/{section_key}/schema`.
///
/// # Errors
/// As [`list_section_keys`]; 404 for an unknown key.
pub async fn get_section_schema(
    Extension(ctx): Extension<PlatformSecurityContext>,
    Extension(svc): Extension<Arc<ReadService>>,
    Path(section_key): Path<String>,
) -> ApiResult<Json<SectionSchemaDto>> {
    let request_id = Uuid::now_v7();
    let span = tracing::info_span!("get_section_schema", %request_id);
    let section = span
        .in_scope(|| svc.section_schema(&ctx, &section_key))
        .map_err(|e| refusal(&span, &e, request_id))?;
    Ok(Json(SectionSchemaDto {
        key: section.key().to_owned(),
        section_type: section.section_type().to_owned(),
        schema: section.schema().clone(),
    }))
}

/// `GET /application-manager/v1/manifest-schema`.
///
/// # Errors
/// As [`list_section_keys`].
pub async fn get_manifest_schema(
    Extension(ctx): Extension<PlatformSecurityContext>,
    Extension(svc): Extension<Arc<ReadService>>,
) -> ApiResult<Json<ManifestSchemaDto>> {
    let request_id = Uuid::now_v7();
    let span = tracing::info_span!("get_manifest_schema", %request_id);
    let read = span
        .in_scope(|| svc.manifest_schema(&ctx))
        .map_err(|e| refusal(&span, &e, request_id))?;
    Ok(Json(ManifestSchemaDto {
        schema: read.schema,
        sections: read
            .sections
            .into_iter()
            .map(|(key, section_type)| SectionTypeDto { key, section_type })
            .collect(),
    }))
}

/// `POST /application-manager/v1/applications`.
///
/// # Errors
/// The synchronous refusals of DESIGN 3.3: 503, 403, 409, 400, or 500.
pub async fn seed(
    Extension(ctx): Extension<PlatformSecurityContext>,
    Extension(gate): Extension<Arc<RequestGate>>,
    Extension(orchestrator): Extension<Arc<RequestOrchestrator>>,
    body: Bytes,
) -> ApiResult<Response> {
    submit(ctx, &gate, orchestrator, RequestInput::Seed(body)).await
}

/// `DELETE /application-manager/v1/applications/{application_id}`.
///
/// # Errors
/// As [`seed`].
pub async fn uninstall_by_id(
    Extension(ctx): Extension<PlatformSecurityContext>,
    Extension(gate): Extension<Arc<RequestGate>>,
    Extension(orchestrator): Extension<Arc<RequestOrchestrator>>,
    Path(application_id): Path<String>,
) -> ApiResult<Response> {
    submit(
        ctx,
        &gate,
        orchestrator,
        RequestInput::UninstallById(application_id),
    )
    .await
}

/// `POST /application-manager/v1/applications:uninstall`.
///
/// # Errors
/// As [`seed`].
pub async fn uninstall(
    Extension(ctx): Extension<PlatformSecurityContext>,
    Extension(gate): Extension<Arc<RequestGate>>,
    Extension(orchestrator): Extension<Arc<RequestOrchestrator>>,
    body: Bytes,
) -> ApiResult<Response> {
    submit(ctx, &gate, orchestrator, RequestInput::UninstallBody(body)).await
}

/// The gate, then the request task; 202 with the operation id and its
/// `Location` once accepted.
async fn submit(
    ctx: PlatformSecurityContext,
    gate: &RequestGate,
    orchestrator: Arc<RequestOrchestrator>,
    input: RequestInput,
) -> ApiResult<Response> {
    let receipt = Receipt::new(OffsetDateTime::now_utc());
    let request_id = receipt.request_id;
    let permit = match gate.admit() {
        Ok(permit) => permit,
        Err(refusal) => {
            tracing::info!(%request_id, refusal = %refusal, "application-manager: refused at the gate");
            orchestrator
                .refuse_at_gate(&ctx, &input, &receipt, &refusal)
                .await;
            return Err(to_canonical(&refusal, request_id));
        }
    };
    let submitted = orchestrator
        .submit(ctx, input, receipt, permit)
        .await
        .map_err(|e| to_canonical(&e, request_id))?;
    let location = format!(
        "/application-manager/v1/operations/{}",
        submitted.operation_id
    );
    Ok((
        StatusCode::ACCEPTED,
        [(header::LOCATION, location)],
        Json(AcceptedDto {
            operation_id: submitted.operation_id,
            status: "accepted".to_owned(),
        }),
    )
        .into_response())
}

/// `GET /application-manager/v1/operations/{operation_id}`.
///
/// # Errors
/// 403 when the allowed callers do not grant `read_operation`; 404 for a
/// malformed, unknown, or purged id; 503 when storage cannot answer.
pub async fn get_operation(
    Extension(ctx): Extension<PlatformSecurityContext>,
    Extension(svc): Extension<Arc<ReadService>>,
    Path(operation_id): Path<String>,
) -> ApiResult<Json<OperationDto>> {
    let request_id = Uuid::now_v7();
    let span = tracing::info_span!("get_operation", %request_id);
    let operation = svc
        .read_operation(&ctx, &operation_id)
        .await
        .map_err(|e| refusal(&span, &e, request_id))?;
    Ok(Json(operation_dto(operation)))
}

fn timestamp(at: OffsetDateTime) -> String {
    at.format(&Rfc3339).unwrap_or_default()
}

fn token<T: serde::Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(token)) => token,
        _ => String::new(),
    }
}

/// The record, and the report only once the operation is completed.
fn operation_dto(operation: Operation) -> OperationDto {
    let status = match operation.status {
        OperationStatus::Accepted => "accepted",
        OperationStatus::Running => "running",
        OperationStatus::Completed => "completed",
    };
    let report = (operation.status == OperationStatus::Completed)
        .then(|| serde_json::to_value(&operation.report).ok())
        .flatten();
    OperationDto {
        operation_id: operation.operation_id,
        application_id: operation.application_id,
        action: token(&operation.action),
        status: status.to_owned(),
        category: operation.category.as_ref().map(token),
        accepted_at: timestamp(operation.accepted_at),
        started_at: operation.started_at.map(timestamp),
        finished_at: operation.finished_at.map(timestamp),
        run_deadline: timestamp(operation.run_deadline),
        report,
    }
}

/// The canonical error of a refused read. The read service logs a caller
/// refusal; only not ready is logged here.
fn refusal(span: &tracing::Span, err: &DomainError, request_id: Uuid) -> CanonicalError {
    if *err == DomainError::NotReady {
        span.in_scope(|| tracing::info!("application-manager: refused, not ready"));
    }
    to_canonical(err, request_id)
}
