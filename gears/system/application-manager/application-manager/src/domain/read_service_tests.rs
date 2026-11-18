#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use serde_json::json;

use super::*;
use crate::domain::allowed_callers::AllowedCallers;
use crate::domain::history::{HistoryEvent, HistoryKind};
use crate::domain::operation::{NewOperation, OperationStatus};
use crate::domain::plugin_set::PluginSet;
use crate::domain::ports::OperationRepository;
use crate::test_support::{
    FakePlugin, Hook, HookedRepo, allowed_callers, platform_ctx, section_plugin,
};

fn validator() -> Arc<SchemaValidator> {
    Arc::new(SchemaValidator::new(64).unwrap())
}

fn ready_state(plugin: &Arc<FakePlugin>) -> Arc<StartupState> {
    let state = Arc::new(StartupState::default());
    state
        .publish(PluginSet::new(vec![
            section_plugin("second", 2, plugin),
            section_plugin("first", 1, plugin),
        ]))
        .unwrap();
    state
}

async fn service(actions: Vec<CallerAction>, plugin: &Arc<FakePlugin>) -> ReadService {
    ReadService::new(
        Arc::new(allowed_callers("app-seeder", actions)),
        ready_state(plugin),
        validator(),
        HookedRepo::new().await,
    )
}

#[tokio::test]
async fn not_ready_refuses_every_read_before_the_caller_check() {
    let svc = ReadService::new(
        Arc::new(AllowedCallers::default()),
        Arc::new(StartupState::default()),
        validator(),
        HookedRepo::new().await,
    );
    let ctx = platform_ctx("app-seeder");
    assert_eq!(svc.list_section_keys(&ctx), Err(DomainError::NotReady));
    assert_eq!(
        svc.section_schema(&ctx, "first").err(),
        Some(DomainError::NotReady)
    );
    assert_eq!(svc.manifest_schema(&ctx).err(), Some(DomainError::NotReady));
}

#[tokio::test]
async fn keys_follow_the_allowed_callers_and_never_call_a_plugin() {
    let not_permitted = Err(DomainError::NotPermitted {
        action: CallerAction::ReadSchema,
    });
    let cases = [
        (
            "app-seeder",
            vec![CallerAction::ReadSchema],
            Ok(vec!["first".to_owned(), "second".to_owned()]),
        ),
        (
            "someone-else",
            vec![CallerAction::ReadSchema],
            not_permitted.clone(),
        ),
        ("app-seeder", vec![CallerAction::Seed], not_permitted),
    ];
    for (caller, actions, expected) in cases {
        let plugin = FakePlugin::new();
        let svc = service(actions, &plugin).await;
        assert_eq!(
            svc.list_section_keys(&platform_ctx(caller)),
            expected,
            "{caller}"
        );
        assert_eq!(
            plugin.calls(),
            0,
            "no plugin call (nfr-authorized-before-plugin)"
        );
    }
}

#[tokio::test]
async fn one_section_schema_comes_with_its_section_type() {
    let plugin = FakePlugin::new();
    let svc = service(vec![CallerAction::ReadSchema], &plugin).await;
    let section = svc
        .section_schema(&platform_ctx("app-seeder"), "first")
        .unwrap();
    assert_eq!(section.key(), "first");
    assert_eq!(
        section.section_type(),
        "gts.cf.core.application_manager.section.v1~first.v1~"
    );
    assert_eq!(
        section.schema(),
        &json!({ "$id": "urn:cf:application-manager:section:first", "type": "object" })
    );
    assert_eq!(plugin.calls(), 0);
}

#[tokio::test]
async fn an_unknown_key_is_checked_after_the_caller() {
    let plugin = FakePlugin::new();
    let allowed = service(vec![CallerAction::ReadSchema], &plugin).await;
    assert_eq!(
        allowed
            .section_schema(&platform_ctx("app-seeder"), "nope")
            .err(),
        Some(DomainError::UnknownSectionKey)
    );
    let refused = service(vec![CallerAction::Seed], &plugin).await;
    assert_eq!(
        refused
            .section_schema(&platform_ctx("app-seeder"), "nope")
            .err(),
        Some(DomainError::NotPermitted {
            action: CallerAction::ReadSchema
        })
    );
}

#[tokio::test]
async fn the_combined_schema_lists_every_section_type_in_priority_order() {
    let plugin = FakePlugin::new();
    let svc = service(vec![CallerAction::ReadSchema], &plugin).await;
    let read = svc.manifest_schema(&platform_ctx("app-seeder")).unwrap();
    assert_eq!(
        read.sections,
        [
            (
                "first".to_owned(),
                "gts.cf.core.application_manager.section.v1~first.v1~".to_owned()
            ),
            (
                "second".to_owned(),
                "gts.cf.core.application_manager.section.v1~second.v1~".to_owned()
            ),
        ]
    );
    let sections = &read.schema["properties"]["sections"];
    assert_eq!(sections["required"], json!(["first", "second"]));
    assert_eq!(
        sections["properties"]["second"]["$id"],
        "urn:cf:application-manager:section:second"
    );
    assert_eq!(
        read.schema["properties"]["application"]["properties"]["id"]["maxLength"],
        64
    );
    assert_eq!(plugin.calls(), 0);
}

// ---- the operation read ----

/// A read service whose repository holds one open seed of `acme.app`, and
/// that seed's id.
async fn with_open_operation(
    window: std::time::Duration,
) -> (ReadService, Arc<HookedRepo>, uuid::Uuid) {
    let repo = HookedRepo::new().await;
    let id = uuid::Uuid::now_v7();
    repo.accept(
        NewOperation {
            operation_id: id,
            application_id: "acme.app".to_owned(),
            action: application_manager_sdk::OperationAction::Seed,
            run_window: window,
            finished_event_id: uuid::Uuid::now_v7(),
            report: application_manager_sdk::OutcomeReport {
                operation_id: id,
                action: application_manager_sdk::OperationAction::Seed,
                category: None,
                application: application_manager_sdk::ReportApplication {
                    id: "acme.app".to_owned(),
                    version: Some("1".to_owned()),
                },
                sections: vec![],
                problems: vec![],
                unavailable: vec![],
            },
        },
        HistoryEvent {
            event_id: uuid::Uuid::now_v7(),
            kind: HistoryKind::Accepted,
            correlation_id: id,
            action: application_manager_sdk::OperationAction::Seed,
            written_at: time::OffsetDateTime::now_utc(),
            content: json!({}),
        },
    )
    .await
    .unwrap();
    let svc = ReadService::new(
        Arc::new(allowed_callers(
            "app-seeder",
            vec![CallerAction::ReadOperation],
        )),
        ready_state(&FakePlugin::new()),
        validator(),
        Arc::clone(&repo) as Arc<dyn OperationRepository>,
    );
    (svc, repo, id)
}

#[tokio::test]
async fn a_caller_not_allowed_to_read_operations_makes_no_storage_read() {
    let (svc, repo, id) = with_open_operation(std::time::Duration::from_secs(3600)).await;
    assert_eq!(
        svc.read_operation(&platform_ctx("someone-else"), &id.to_string())
            .await
            .err(),
        Some(DomainError::NotPermitted {
            action: CallerAction::ReadOperation
        })
    );
    assert_eq!(repo.calls("read_operation"), 0);
}

#[tokio::test]
async fn an_open_operation_is_read_as_it_stands() {
    let (svc, _, id) = with_open_operation(std::time::Duration::from_secs(3600)).await;
    let op = svc
        .read_operation(&platform_ctx("app-seeder"), &id.to_string())
        .await
        .unwrap();
    assert_eq!(op.status, OperationStatus::Accepted);
    assert_eq!(op.category, None);
}

#[tokio::test]
async fn an_expired_open_operation_is_completed_before_the_read() {
    let (svc, _, id) = with_open_operation(std::time::Duration::ZERO).await;
    let op = svc
        .read_operation(&platform_ctx("app-seeder"), &id.to_string())
        .await
        .unwrap();
    assert_eq!(op.status, OperationStatus::Completed);
    assert_eq!(
        op.category,
        Some(application_manager_sdk::RequestCategory::InternalFailure)
    );
}

#[tokio::test]
async fn an_unknown_or_malformed_operation_is_not_found() {
    let (svc, _, _) = with_open_operation(std::time::Duration::from_secs(3600)).await;
    for id in [uuid::Uuid::now_v7().to_string(), "not-a-uuid".to_owned()] {
        assert_eq!(
            svc.read_operation(&platform_ctx("app-seeder"), &id)
                .await
                .err(),
            Some(DomainError::OperationNotFound),
            "{id}"
        );
    }
}

#[tokio::test]
async fn storage_that_cannot_answer_is_unavailable() {
    let (svc, repo, id) = with_open_operation(std::time::Duration::from_secs(3600)).await;
    repo.hook("read_operation", Hook::Fail);
    assert_eq!(
        svc.read_operation(&platform_ctx("app-seeder"), &id.to_string())
            .await
            .err(),
        Some(DomainError::StorageUnavailable)
    );
}
