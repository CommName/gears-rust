//! REST route registration. All routes are under `/application-manager/v1`.

use std::sync::Arc;

use axum::Router;
use axum::http::StatusCode;
use toolkit::api::operation_builder::ThrottlingSpec;
use toolkit::api::{OpenApiRegistry, OperationBuilder};

use super::dto::{ManifestSchemaDto, SectionKeysDto, SectionSchemaDto};
use super::handlers;
use crate::domain::read_service::ReadService;

const TAG: &str = "Application Manager";

/// Gateway rate-limit zone of the bounded routes (DESIGN 3.3).
pub const RATE_LIMIT_ZONE: &str = "rl_application_manager";

/// Gateway in-flight-limit zone of the bounded routes (DESIGN 3.3).
pub const IN_FLIGHT_LIMIT_ZONE: &str = "ifl_application_manager";

/// The schema reads compile served schemas, so both are bound to the
/// gateway zones (DESIGN 3.3).
fn schema_read_throttling() -> ThrottlingSpec {
    ThrottlingSpec {
        rate_limit_zone: Some(RATE_LIMIT_ZONE.to_owned()),
        in_flight_limit_zone: Some(IN_FLIGHT_LIMIT_ZONE.to_owned()),
        ..ThrottlingSpec::default()
    }
}

/// Register the discovery routes.
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
            "The live schema of one section key, served by its section plugin, with its \
             schema version.",
        )
        .tag(TAG)
        .path_param("section_key", "A section key from the section key list")
        .authenticated()
        .no_license_required()
        .with_throttling(schema_read_throttling())
        .handler(handlers::get_section_schema)
        .json_response_with_schema::<SectionSchemaDto>(openapi, StatusCode::OK, "Section schema")
        .error_401(openapi)
        .error_403(openapi)
        .error_404(openapi)
        .error_429(openapi)
        .error_500(openapi)
        .error_503(openapi)
        .register(router, openapi);

    let router = OperationBuilder::get("/application-manager/v1/manifest-schema")
        .operation_id("application_manager.get_manifest_schema")
        .summary("Read the combined manifest schema")
        .description(
            "The live combined schema of a whole manifest, built from every section \
             plugin's schema, with every schema version.",
        )
        .tag(TAG)
        .authenticated()
        .no_license_required()
        .with_throttling(schema_read_throttling())
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

    router.layer(axum::Extension(read))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "routes_tests.rs"]
mod tests;
