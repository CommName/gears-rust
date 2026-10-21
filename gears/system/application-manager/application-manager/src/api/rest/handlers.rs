//! REST handlers.

use std::sync::Arc;

use axum::extract::Extension;
use toolkit::api::canonical_prelude::*;
use toolkit_security::SecurityContext;
use tracing::Instrument;
use uuid::Uuid;

use super::dto::SectionKeysDto;
use super::error::to_canonical;
use crate::domain::error::DomainError;
use crate::domain::read_service::ReadService;

/// `GET /application-manager/v1/section-keys`.
///
/// # Errors
/// 503 before startup ends or when access control cannot answer; 403 when
/// `read_schema` is not permitted.
pub async fn list_section_keys(
    Extension(ctx): Extension<SecurityContext>,
    Extension(svc): Extension<Arc<ReadService>>,
) -> ApiResult<Json<SectionKeysDto>> {
    let request_id = Uuid::now_v7();
    let span = tracing::info_span!("list_section_keys", %request_id);
    let items = svc
        .list_section_keys(&ctx)
        .instrument(span.clone())
        .await
        .map_err(|e| {
            // Access refusals are logged by the access-control adapter.
            if e == DomainError::NotReady {
                span.in_scope(|| tracing::info!("application-manager: refused, not ready"));
            }
            to_canonical(&e, request_id)
        })?;
    Ok(Json(SectionKeysDto { items }))
}
