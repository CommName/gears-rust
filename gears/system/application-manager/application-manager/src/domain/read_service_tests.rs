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
    FakeAccess, FakePlugin, Hook, HookedRepo, allowed_callers, platform_ctx, section_plugin,
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
        FakeAccess::permit(),
    )
}

#[tokio::test]
async fn not_ready_refuses_every_read_before_the_caller_check() {
    let svc = ReadService::new(
        Arc::new(AllowedCallers::default()),
        Arc::new(StartupState::default()),
        validator(),
        HookedRepo::new().await,
        FakeAccess::permit(),
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
        FakeAccess::permit(),
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

// ---- the tenant-plane reads ----

mod tenant_reads {
    use std::time::Duration;

    use application_manager_sdk::{
        OperationAction, OutcomeReport, ReportApplication, RequestCategory,
    };
    use toolkit_odata::ast::{CompareOperator, Expr, Value};
    use toolkit_odata::{CursorV1, ODataQuery, short_filter_hash};
    use uuid::Uuid;

    use super::*;
    use crate::domain::ports::AccessError;
    use crate::test_support::{RecordedSignals, tenant_ctx};

    /// A read service over `repo` whose PDP answers `answer`.
    fn service_over(
        repo: &Arc<HookedRepo>,
        answer: Result<(), AccessError>,
    ) -> (ReadService, Arc<FakeAccess>, Arc<RecordedSignals>) {
        let access = FakeAccess::new(answer);
        let signals = RecordedSignals::new();
        let svc = ReadService::new(
            Arc::new(AllowedCallers::default()),
            ready_state(&FakePlugin::new()),
            validator(),
            Arc::clone(repo) as Arc<dyn OperationRepository>,
            Arc::clone(&access) as Arc<dyn crate::domain::ports::AccessControl>,
        )
        .with_signals(Arc::clone(&signals) as Arc<dyn crate::domain::signals::Signals>);
        (svc, access, signals)
    }

    /// Refused events `1..=n`, each correlated with itself.
    async fn events(repo: &HookedRepo, n: u128) {
        for i in 1..=n {
            repo.record_refused(HistoryEvent {
                event_id: Uuid::from_u128(i),
                kind: HistoryKind::Refused,
                correlation_id: Uuid::from_u128(i),
                action: OperationAction::Seed,
                written_at: time::OffsetDateTime::UNIX_EPOCH,
                content: json!({}),
            })
            .await
            .unwrap();
        }
    }

    /// A finished seed of each of `apps`.
    async fn applications(repo: &HookedRepo, apps: &[&str]) {
        for (n, app) in (1_u128..).zip(apps) {
            let id = Uuid::from_u128(0x100 + n);
            let report = OutcomeReport {
                operation_id: id,
                action: OperationAction::Seed,
                category: None,
                application: ReportApplication {
                    id: (*app).to_owned(),
                    version: Some("1".to_owned()),
                },
                sections: vec![],
                problems: vec![],
                unavailable: vec![],
            };
            repo.accept(
                NewOperation {
                    operation_id: id,
                    application_id: (*app).to_owned(),
                    action: OperationAction::Seed,
                    run_window: Duration::from_secs(3600),
                    finished_event_id: Uuid::from_u128(0x300 + n),
                    report: report.clone(),
                },
                HistoryEvent {
                    event_id: Uuid::from_u128(0x200 + n),
                    kind: HistoryKind::Accepted,
                    correlation_id: id,
                    action: OperationAction::Seed,
                    written_at: time::OffsetDateTime::UNIX_EPOCH,
                    content: json!({}),
                },
            )
            .await
            .unwrap();
            repo.mark_running(id, &[]).await.unwrap();
            repo.finish(id, &report, RequestCategory::Succeeded)
                .await
                .unwrap();
        }
    }

    fn filtered(field: &str, values: Vec<Value>) -> ODataQuery {
        let filter = if let [value] = values.as_slice() {
            Expr::Compare(
                Box::new(Expr::Identifier(field.to_owned())),
                CompareOperator::Eq,
                Box::new(Expr::Value(value.clone())),
            )
        } else {
            Expr::In(
                Box::new(Expr::Identifier(field.to_owned())),
                values.into_iter().map(Expr::Value).collect(),
            )
        };
        let hash = short_filter_hash(Some(&filter)).unwrap();
        ODataQuery::new().with_filter(filter).with_filter_hash(hash)
    }

    #[tokio::test]
    async fn a_refused_read_makes_no_storage_read() {
        let repo = HookedRepo::new().await;
        let (svc, _, signals) = service_over(&repo, Err(AccessError::NotPermitted));
        let query = ODataQuery::new();
        assert_eq!(
            svc.read_history(&tenant_ctx(), &query).await.err(),
            Some(DomainError::AccessDenied {
                action: TenantAction::ReadHistory
            })
        );
        assert_eq!(
            svc.list_applications(&tenant_ctx(), &query).await.err(),
            Some(DomainError::AccessDenied {
                action: TenantAction::ReadApplications
            })
        );
        assert_eq!(repo.calls("history_page"), 0);
        assert_eq!(repo.calls("applications_page"), 0);
        assert_eq!(signals.count("refused:read_history"), 1);
        assert_eq!(signals.count("refused:read_applications"), 1);
    }

    #[tokio::test]
    async fn a_pdp_that_cannot_answer_is_unavailable() {
        let repo = HookedRepo::new().await;
        let (svc, _, signals) = service_over(&repo, Err(AccessError::Unavailable));
        assert_eq!(
            svc.read_history(&tenant_ctx(), &ODataQuery::new())
                .await
                .err(),
            Some(DomainError::AccessUnavailable)
        );
        assert_eq!(repo.calls("history_page"), 0);
        assert_eq!(signals.count("unavailable:pdp"), 1);
    }

    #[tokio::test]
    async fn the_pdp_decides_before_the_query_is_checked() {
        let repo = HookedRepo::new().await;
        let (svc, access, _) = service_over(&repo, Err(AccessError::NotPermitted));
        let bad = ODataQuery::new().with_select(vec!["kind".to_owned()]);
        assert!(matches!(
            svc.read_history(&tenant_ctx(), &bad).await,
            Err(DomainError::AccessDenied { .. })
        ));
        assert_eq!(
            *access.actions.lock().unwrap(),
            vec![TenantAction::ReadHistory]
        );
    }

    #[tokio::test]
    async fn history_pages_follow_the_cursor_to_the_end() {
        let repo = HookedRepo::new().await;
        events(&repo, 5).await;
        let (svc, _, _) = service_over(&repo, Ok(()));
        let mut query = ODataQuery::new().with_limit(2);
        let mut seen = Vec::new();
        loop {
            let page = svc.read_history(&tenant_ctx(), &query).await.unwrap();
            assert_eq!(page.page_info.limit, 2);
            seen.extend(page.items.iter().map(|e| e.event_id));
            let Some(next) = page.page_info.next_cursor else {
                break;
            };
            query = ODataQuery::new()
                .with_limit(2)
                .with_cursor(CursorV1::decode(&next).unwrap());
        }
        let expected: Vec<Uuid> = (1..=5).map(Uuid::from_u128).collect();
        assert_eq!(seen, expected);
    }

    #[tokio::test]
    async fn the_history_filter_matches_correlation_ids() {
        let repo = HookedRepo::new().await;
        events(&repo, 3).await;
        let (svc, _, _) = service_over(&repo, Ok(()));

        let one = filtered("operation_id", vec![Value::Uuid(Uuid::from_u128(2))]);
        let page = svc.read_history(&tenant_ctx(), &one).await.unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].correlation_id, Uuid::from_u128(2));

        let two = filtered(
            "operation_id",
            vec![
                Value::Uuid(Uuid::from_u128(1)),
                Value::Uuid(Uuid::from_u128(3)),
            ],
        );
        assert_eq!(
            svc.read_history(&tenant_ctx(), &two)
                .await
                .unwrap()
                .items
                .len(),
            2
        );

        let unknown = filtered("operation_id", vec![Value::Uuid(Uuid::from_u128(99))]);
        let page = svc.read_history(&tenant_ctx(), &unknown).await.unwrap();
        assert!(page.items.is_empty());
        assert_eq!(page.page_info.next_cursor, None);
    }

    #[tokio::test]
    async fn applications_are_listed_in_id_order_and_filtered() {
        let repo = HookedRepo::new().await;
        applications(&repo, &["zeta", "alpha", "mid"]).await;
        let (svc, _, _) = service_over(&repo, Ok(()));

        let page = svc
            .list_applications(&tenant_ctx(), &ODataQuery::new())
            .await
            .unwrap();
        let ids: Vec<&str> = page
            .items
            .iter()
            .map(|a| a.application_id.as_str())
            .collect();
        assert_eq!(ids, ["alpha", "mid", "zeta"]);
        assert_eq!(page.items[0].category, RequestCategory::Succeeded);

        let one = filtered("application_id", vec![Value::String("mid".to_owned())]);
        let page = svc.list_applications(&tenant_ctx(), &one).await.unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].version, "1");
    }

    #[tokio::test]
    async fn a_query_the_read_does_not_serve_is_refused() {
        let repo = HookedRepo::new().await;
        let (svc, _, _) = service_over(&repo, Ok(()));
        let other = filtered("kind", vec![Value::String("refused".to_owned())]);
        assert!(matches!(
            svc.read_history(&tenant_ctx(), &other).await,
            Err(DomainError::InvalidQuery {
                parameter: "$filter",
                ..
            })
        ));
        assert_eq!(repo.calls("history_page"), 0);
    }

    #[tokio::test]
    async fn storage_that_cannot_answer_is_unavailable() {
        let repo = HookedRepo::new().await;
        repo.hook("history_page", Hook::Fail);
        repo.hook("applications_page", Hook::Fail);
        let (svc, _, signals) = service_over(&repo, Ok(()));
        assert_eq!(
            svc.read_history(&tenant_ctx(), &ODataQuery::new())
                .await
                .err(),
            Some(DomainError::StorageUnavailable)
        );
        assert_eq!(
            svc.list_applications(&tenant_ctx(), &ODataQuery::new())
                .await
                .err(),
            Some(DomainError::StorageUnavailable)
        );
        assert_eq!(signals.count("unavailable:storage"), 2);
    }
}
