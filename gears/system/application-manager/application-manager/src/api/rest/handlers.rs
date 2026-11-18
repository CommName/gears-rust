//! REST handlers.

use std::sync::Arc;

use axum::extract::Extension;
use toolkit::api::canonical_prelude::*;
use toolkit_security::PlatformSecurityContext;
use uuid::Uuid;

use super::dto::SectionKeysDto;
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
    let items = span.in_scope(|| svc.list_section_keys(&ctx)).map_err(|e| {
        // The read service logs a caller refusal.
        if e == DomainError::NotReady {
            span.in_scope(|| tracing::info!("application-manager: refused, not ready"));
        }
        to_canonical(&e, request_id)
    })?;
    Ok(Json(SectionKeysDto { items }))
}
