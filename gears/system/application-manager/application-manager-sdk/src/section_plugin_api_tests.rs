#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use super::*;

struct FakePlugin;

#[async_trait]
impl SectionPluginClientV1 for FakePlugin {
    async fn key(&self) -> Result<String, SectionPluginError> {
        Ok("fake".to_owned())
    }

    async fn schema(&self, _caller: &CallerIdentity) -> Result<SectionSchema, SectionPluginError> {
        Ok(SectionSchema::new(serde_json::json!({}), "v1"))
    }

    async fn apply(
        &self,
        _caller: &CallerIdentity,
        _operation_id: Uuid,
        _envelope: &Envelope,
        _section: &serde_json::Value,
    ) -> Result<(), SectionPluginError> {
        Err(SectionPluginError::Internal {
            message: "apply".to_owned(),
        })
    }

    async fn remove(
        &self,
        _caller: &CallerIdentity,
        _operation_id: Uuid,
        _application_id: &str,
    ) -> Result<(), SectionPluginError> {
        Ok(())
    }
}

fn assert_send_sync<T: Send + Sync + ?Sized>() {}

#[test]
fn plugin_trait_is_dyn_compatible_and_send_sync() {
    assert_send_sync::<dyn SectionPluginClientV1>();
    let _plugin: Arc<dyn SectionPluginClientV1> = Arc::new(FakePlugin);
}
