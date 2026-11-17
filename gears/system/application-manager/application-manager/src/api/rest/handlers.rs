//! REST handlers.

use std::sync::Arc;

use axum::extract::{Extension, Path};
use toolkit::api::canonical_prelude::*;
use toolkit_security::PlatformSecurityContext;
use uuid::Uuid;

use super::dto::{ManifestSchemaDto, SectionKeysDto, SectionSchemaDto, SectionTypeDto};
use super::error::to_canonical;
use crate::domain::error::DomainError;
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

/// The canonical error of a refused read. The read service logs a caller
/// refusal; only not ready is logged here.
fn refusal(span: &tracing::Span, err: &DomainError, request_id: Uuid) -> CanonicalError {
    if *err == DomainError::NotReady {
        span.in_scope(|| tracing::info!("application-manager: refused, not ready"));
    }
    to_canonical(err, request_id)
}
