//! REST route registration. All routes are under `/application-manager/v1`.

use std::sync::Arc;

use axum::Router;
use axum::http::StatusCode;
use toolkit::api::operation_builder::{OperationBuilderODataExt, ThrottlingSpec};
use toolkit::api::{OpenApiRegistry, OperationBuilder};
use toolkit_odata::Page;

use super::dto::{
    AcceptedDto, ApplicationDto, HistoryEventDto, ManifestSchemaDto, OperationDto, SectionKeysDto,
    SectionSchemaDto, SeedRequestDto, UninstallRequestDto,
};
use super::gate::RequestGate;
use super::handlers;
use crate::domain::listing::{ApplicationFilterField, HistoryFilterField};
use crate::domain::orchestrator::RequestOrchestrator;
use crate::domain::read_service::ReadService;

const TAG: &str = "Application Manager";

/// Gateway rate-limit zone of the bounded routes (DESIGN 3.3).
pub const RATE_LIMIT_ZONE: &str = "rl_application_manager";

/// Gateway in-flight-limit zone of the bounded routes (DESIGN 3.3).
pub const IN_FLIGHT_LIMIT_ZONE: &str = "ifl_application_manager";

/// Seed, uninstall, the operation read, the combined-schema read, and the
/// tenant-plane reads are bound to the gateway zones (DESIGN 3.3).
fn bounded_throttling() -> ThrottlingSpec {
    ThrottlingSpec {
        rate_limit_zone: Some(RATE_LIMIT_ZONE.to_owned()),
        in_flight_limit_zone: Some(IN_FLIGHT_LIMIT_ZONE.to_owned()),
        ..ThrottlingSpec::default()
    }
}

/// Register the routes.
pub fn register_routes(
    router: Router,
    openapi: &dyn OpenApiRegistry,
    read: Arc<ReadService>,
    orchestrator: Arc<RequestOrchestrator>,
    gate: Arc<RequestGate>,
) -> Router {
    let router = OperationBuilder::post("/application-manager/v1/applications")
        .operation_id("application_manager.seed")
        .summary("Seed an application")
        .description("Submit a manifest; answers 202 with the operation id.")
        .tag(TAG)
        .platform_authenticated()
        .no_license_required()
        .with_throttling(bounded_throttling())
        .json_request::<SeedRequestDto>(openapi, "The manifest")
        .handler(handlers::seed)
        .json_response_with_schema::<AcceptedDto>(openapi, StatusCode::ACCEPTED, "Accepted")
        .error_400(openapi)
        .error_401(openapi)
        .error_403(openapi)
        .error_409(openapi)
        .error_429(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    let router = OperationBuilder::delete("/application-manager/v1/applications/{application_id}")
        .operation_id("application_manager.uninstall_by_id")
        .summary("Uninstall an application by id")
        .description("The id is one percent-encoded path segment; answers 202.")
        .tag(TAG)
        .path_param("application_id", "The application id, percent-encoded")
        .platform_authenticated()
        .no_license_required()
        .with_throttling(bounded_throttling())
        .handler(handlers::uninstall_by_id)
        .json_response_with_schema::<AcceptedDto>(openapi, StatusCode::ACCEPTED, "Accepted")
        .error_400(openapi)
        .error_401(openapi)
        .error_403(openapi)
        .error_409(openapi)
        .error_429(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    let router = OperationBuilder::post("/application-manager/v1/applications:uninstall")
        .operation_id("application_manager.uninstall")
        .summary("Uninstall an application")
        .description("The body carries `application_id`; answers 202.")
        .tag(TAG)
        .platform_authenticated()
        .no_license_required()
        .with_throttling(bounded_throttling())
        .json_request::<UninstallRequestDto>(openapi, "The application to uninstall")
        .handler(handlers::uninstall)
        .json_response_with_schema::<AcceptedDto>(openapi, StatusCode::ACCEPTED, "Accepted")
        .error_400(openapi)
        .error_401(openapi)
        .error_403(openapi)
        .error_409(openapi)
        .error_429(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    let router = OperationBuilder::get("/application-manager/v1/operations/{operation_id}")
        .operation_id("application_manager.get_operation")
        .summary("Read one operation")
        .description("The operation record and, once final, its outcome report.")
        .tag(TAG)
        .path_param("operation_id", "The operation id")
        .platform_authenticated()
        .no_license_required()
        .with_throttling(bounded_throttling())
        .handler(handlers::get_operation)
        .json_response_with_schema::<OperationDto>(openapi, StatusCode::OK, "Operation")
        .error_401(openapi)
        .error_403(openapi)
        .error_404(openapi)
        .error_429(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    let router = OperationBuilder::get("/application-manager/v1/section-keys")
        .operation_id("application_manager.list_section_keys")
        .summary("List section keys")
        .description("The section keys of the startup plugin set, in priority order.")
        .tag(TAG)
        .platform_authenticated()
        .no_license_required()
        .handler(handlers::list_section_keys)
        .json_response_with_schema::<SectionKeysDto>(openapi, StatusCode::OK, "Section keys")
        .error_401(openapi)
        .error_403(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    let router = OperationBuilder::get("/application-manager/v1/section-keys/{section_key}/schema")
        .operation_id("application_manager.get_section_schema")
        .summary("Read one section schema")
        .description(
            "The registered section type schema of one section key, composed under the key, \
             with its section type.",
        )
        .tag(TAG)
        .path_param("section_key", "A section key from the section key list")
        .platform_authenticated()
        .no_license_required()
        .handler(handlers::get_section_schema)
        .json_response_with_schema::<SectionSchemaDto>(openapi, StatusCode::OK, "Section schema")
        .error_401(openapi)
        .error_403(openapi)
        .error_404(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    let router = OperationBuilder::get("/application-manager/v1/manifest-schema")
        .operation_id("application_manager.get_manifest_schema")
        .summary("Read the combined manifest schema")
        .description(
            "The combined schema of a whole manifest, built from every registered section \
             type schema, with every section type.",
        )
        .tag(TAG)
        .platform_authenticated()
        .no_license_required()
        .with_throttling(bounded_throttling())
        .handler(handlers::get_manifest_schema)
        .json_response_with_schema::<ManifestSchemaDto>(
            openapi,
            StatusCode::OK,
            "Combined manifest schema",
        )
        .error_401(openapi)
        .error_403(openapi)
        .error_429(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    let router = register_tenant_reads(router, openapi);

    router
        .layer(axum::Extension(read))
        .layer(axum::Extension(orchestrator))
        .layer(axum::Extension(gate))
}

/// The tenant-plane reads, decided by the PDP (ADR 0004).
fn register_tenant_reads(router: Router, openapi: &dyn OpenApiRegistry) -> Router {
    let router = OperationBuilder::get("/application-manager/v1/operation-history")
        .operation_id("application_manager.read_history")
        .summary("Read the operation history")
        .description(
            "History events in `event_id` order. `$filter` takes only `operation_id eq <id>` \
             or `operation_id in (...)`, matched against the correlation id; `$orderby` \
             and `$select` are refused. Decided by the PDP as `read_history`.",
        )
        .tag(TAG)
        .authenticated()
        .no_license_required()
        .with_throttling(bounded_throttling())
        .query_param_typed(
            "limit",
            false,
            "Page size, at most 100 (the default)",
            "integer",
        )
        .query_param("cursor", false, "The `next_cursor` of the previous page")
        .with_odata_filter::<HistoryFilterField>()
        .handler(handlers::read_history)
        .json_response_with_schema::<Page<HistoryEventDto>>(
            openapi,
            StatusCode::OK,
            "A page of history events",
        )
        .error_400(openapi)
        .error_401(openapi)
        .error_403(openapi)
        .error_429(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    OperationBuilder::get("/application-manager/v1/applications")
        .operation_id("application_manager.list_applications")
        .summary("List installed applications")
        .description(
            "The last finished seed of each application, in `application_id` order. \
             `$filter` takes only `application_id eq <id>` or `application_id in (...)`; \
             `$orderby` and `$select` are refused. Decided by the PDP as \
             `read_applications`.",
        )
        .tag(TAG)
        .authenticated()
        .no_license_required()
        .with_throttling(bounded_throttling())
        .query_param_typed(
            "limit",
            false,
            "Page size, at most 100 (the default)",
            "integer",
        )
        .query_param("cursor", false, "The `next_cursor` of the previous page")
        .with_odata_filter::<ApplicationFilterField>()
        .handler(handlers::list_applications)
        .json_response_with_schema::<Page<ApplicationDto>>(
            openapi,
            StatusCode::OK,
            "A page of applications",
        )
        .error_400(openapi)
        .error_401(openapi)
        .error_403(openapi)
        .error_429(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "routes_tests.rs"]
mod tests;
