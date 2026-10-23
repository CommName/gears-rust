//! REST handlers.

use std::sync::Arc;

use axum::extract::{Extension, Path};
use toolkit::api::canonical_prelude::*;
use toolkit_security::PlatformSecurityContext;
use uuid::Uuid;

use super::dto::{ManifestSchemaDto, SectionKeysDto, SectionSchemaDto, SectionTypeDto};
use super::dto::SectionKeysDto;
use axum::body::Bytes;
use axum::response::Response;

use super::dto::{
    ManifestSchemaDto, OperationDto, SectionKeysDto, SectionSchemaDto, SectionVersionDto,
};
use super::error::to_canonical;
use super::gate::RequestGate;
use crate::domain::error::DomainError;
use crate::domain::orchestrator::RequestOrchestrator;
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
/// As [`list_section_keys`]; 404 for an unknown key; 503 when the plugin's
/// schema is not served or unusable; 403 when the plugin refuses the caller.
pub async fn get_section_schema(
    Extension(ctx): Extension<SecurityContext>,
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
    let read = svc
        .section_schema(&ctx, &section_key)
        .instrument(span.clone())
        .await
        .map_err(|e| refusal(&span, &e, request_id))?;
    Ok(Json(SectionSchemaDto {
        key: read.key,
        schema: read.schema,
        schema_version: read.schema_version,
    }))
}

/// `GET /application-manager/v1/manifest-schema`.
///
/// # Errors
/// As [`list_section_keys`].
pub async fn get_manifest_schema(
    Extension(ctx): Extension<PlatformSecurityContext>,
/// As [`list_section_keys`]; 503 when any plugin's schema is not served or
/// unusable, which wins over 403 when a plugin refuses the caller.
pub async fn get_manifest_schema(
    Extension(ctx): Extension<SecurityContext>,
    Extension(svc): Extension<Arc<ReadService>>,
) -> ApiResult<Json<ManifestSchemaDto>> {
    let request_id = Uuid::now_v7();
    let span = tracing::info_span!("get_manifest_schema", %request_id);
    let read = span
        .in_scope(|| svc.manifest_schema(&ctx))
    let read = svc
        .manifest_schema(&ctx)
        .instrument(span.clone())
        .await
        .map_err(|e| refusal(&span, &e, request_id))?;
    Ok(Json(ManifestSchemaDto {
        schema: read.schema,
        sections: read
            .sections
            .into_iter()
            .map(|(key, section_type)| SectionTypeDto { key, section_type })
            .map(|s| SectionVersionDto {
                key: s.key,
                schema_version: s.schema_version,
            })
            .collect(),
    }))
}

/// The canonical error of a refused read. The read service logs a caller
/// refusal; only not ready is logged here.
/// `POST /application-manager/v1/applications`: 202 with the operation id and
/// a `Location` header, or a refusal Problem.
///
/// # Errors
/// The refusals of DESIGN 3.3.
pub async fn seed(
    Extension(_ctx): Extension<SecurityContext>,
    Extension(_gate): Extension<Arc<RequestGate>>,
    Extension(_orchestrator): Extension<Arc<RequestOrchestrator>>,
    _body: Bytes,
) -> ApiResult<Response> {
    todo!("PR 5: seed")
}

/// `DELETE /application-manager/v1/applications/{application_id}`.
///
/// # Errors
/// As [`seed`].
pub async fn uninstall_by_id(
    Extension(_ctx): Extension<SecurityContext>,
    Extension(_gate): Extension<Arc<RequestGate>>,
    Extension(_orchestrator): Extension<Arc<RequestOrchestrator>>,
    Path(_application_id): Path<String>,
) -> ApiResult<Response> {
    todo!("PR 5: uninstall by id")
}

/// `POST /application-manager/v1/applications:uninstall`.
///
/// # Errors
/// As [`seed`].
pub async fn uninstall(
    Extension(_ctx): Extension<SecurityContext>,
    Extension(_gate): Extension<Arc<RequestGate>>,
    Extension(_orchestrator): Extension<Arc<RequestOrchestrator>>,
    _body: Bytes,
) -> ApiResult<Response> {
    todo!("PR 5: uninstall")
}

/// `GET /application-manager/v1/operations/{operation_id}`.
///
/// # Errors
/// 403, 503, and 404 for a malformed or unknown operation id.
pub async fn get_operation(
    Extension(_ctx): Extension<SecurityContext>,
    Extension(_svc): Extension<Arc<ReadService>>,
    Path(_operation_id): Path<String>,
) -> ApiResult<Json<OperationDto>> {
    todo!("PR 5: operation read")
}

/// The canonical error of a refused read. Access and plugin refusals are
/// logged where they are decided; only not ready is logged here.
fn refusal(span: &tracing::Span, err: &DomainError, request_id: Uuid) -> CanonicalError {
    if *err == DomainError::NotReady {
        span.in_scope(|| tracing::info!("application-manager: refused, not ready"));
    }
    to_canonical(err, request_id)
}
