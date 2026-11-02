//! Load `config/e2e-local.yaml` and run the gear's `init` on it: its
//! `application-manager` block must parse and pass validation, and the SDK's
//! GTS types must be linked into this binary. A bad YAML edit fails CI.
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[tokio::test]
async fn yaml_application_manager_block_passes_gear_init() {
    use std::sync::Arc;
    use toolkit::Gear;
    use toolkit::bootstrap::AppConfig;

    let file = "config/e2e-local.yaml";
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .join(file);
    let config = AppConfig::load_or_default(Some(&path)).unwrap();
    let ctx = toolkit::GearCtx::new(
        "application-manager",
        uuid::Uuid::nil(),
        Arc::new(config),
        Arc::new(toolkit::ClientHub::new()),
        tokio_util::sync::CancellationToken::new(),
    );
    application_manager::ApplicationManager::default()
        .init(&ctx)
        .await
        .unwrap_or_else(|e| panic!("{file}: {e:#}"));
}
