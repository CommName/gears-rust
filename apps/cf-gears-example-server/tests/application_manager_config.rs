//! Load `config/e2e-local.yaml` and run the gear's `init` on it: its
//! `application-manager` block must parse and pass validation, the section
//! plugin GTS type must be linked into this binary, and the api
//! gateway must have authentication enabled (the gear is supported only with
//! it). A bad YAML edit fails CI. `config/quickstart.yaml` does not enable the
//! gear, so it is not covered here.
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[tokio::test]
async fn yaml_application_manager_block_passes_gear_init() {
    use std::sync::Arc;
    use toolkit::Gear;
    use toolkit::bootstrap::AppConfig;
    use toolkit::config::ConfigProvider;

    let file = "config/e2e-local.yaml";
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .join(file);
    let config = AppConfig::load_or_default(Some(&path)).unwrap();
    let gateway = config
        .get_gear_config("api-gateway")
        .expect("api-gateway block");
    assert_eq!(
        gateway["config"]["auth_disabled"],
        serde_json::json!(false),
        "{file}: api-gateway auth must be enabled"
    );
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
