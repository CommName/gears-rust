//! REST route registration. All routes are under `/application-manager/v1`.

use std::sync::Arc;

use axum::Router;
use axum::http::StatusCode;
use toolkit::api::{OpenApiRegistry, OperationBuilder};

use super::dto::SectionKeysDto;
use super::handlers;
use crate::domain::read_service::ReadService;

const TAG: &str = "Application Manager";

/// Register the routes of this PR's surface.
pub fn register_routes(
    router: Router,
    openapi: &dyn OpenApiRegistry,
    read: Arc<ReadService>,
) -> Router {
    let router = OperationBuilder::get("/application-manager/v1/section-keys")
        .operation_id("application_manager.list_section_keys")
        .summary("List section keys")
        .description("The section keys of the startup plugin set, in priority order.")
        .tag(TAG)
        .authenticated()
        .no_license_required()
        .handler(handlers::list_section_keys)
        .json_response_with_schema::<SectionKeysDto>(openapi, StatusCode::OK, "Section keys")
        .error_401(openapi)
        .error_403(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    router.layer(axum::Extension(read))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "routes_tests.rs"]
mod tests;
