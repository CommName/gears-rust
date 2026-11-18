//! Load `config/e2e-local.yaml` and run the gear's `init` on it: its
//! `application-manager` block must parse and pass validation, the SDK's GTS
//! types must be linked into this binary, the api gateway must have
//! authentication enabled (the gear is supported only with it), and the block
//! must name a database server, which `init` requires. A bad YAML edit fails
//! CI. `config/quickstart.yaml` does not enable the gear, so it is not covered
//! here.
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[tokio::test]
async fn yaml_application_manager_block_passes_gear_init() {
    use std::sync::Arc;
    use toolkit::bootstrap::AppConfig;
    use toolkit::{ConfigProvider, Gear};

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
    let gear_config = config
        .get_gear_config("application-manager")
        .expect("application-manager block");
    assert!(
        gear_config["database"]["server"].is_string(),
        "{file}: application-manager needs a database server"
    );
    // The host connects the configured server; the test hands `init` an
    // in-memory SQLite database instead.
    let db = toolkit_db::connect_db(
        &format!(
            "sqlite:file:application_manager_yaml_{}?mode=memory&cache=shared",
            uuid::Uuid::new_v4()
        ),
        toolkit_db::ConnectOpts::default(),
    )
    .await
    .expect("connect sqlite");
    let ctx = toolkit::GearCtx::new(
        "application-manager",
        uuid::Uuid::nil(),
        Arc::new(config),
        Arc::new(toolkit::ClientHub::new()),
        tokio_util::sync::CancellationToken::new(),
    )
    .with_db(toolkit_db::DBProvider::new(db));
    application_manager::ApplicationManager::default()
        .init(&ctx)
        .await
        .unwrap_or_else(|e| panic!("{file}: {e:#}"));
}
