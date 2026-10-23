#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use serde_json::json;

use super::*;
use crate::domain::allowed_callers::AllowedCallers;
use crate::domain::plugin_set::PluginSet;
use crate::test_support::{FakePlugin, allowed_callers, platform_ctx, section_plugin};
use crate::domain::router::UnavailableReason;
use crate::test_support::{
    CALL_LIMIT, FakeAccess, FakeClock, FakePlugin, Fault, HookedRepo, KeyAnswer, SchemaAnswer, T0,
    ctx, read_service, read_service_over, ready, section, sqlite_repo,
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

fn service(actions: Vec<CallerAction>, plugin: &Arc<FakePlugin>) -> ReadService {
    ReadService::new(
        Arc::new(allowed_callers("app-seeder", actions)),
        ready_state(plugin),
        validator(),
    )
}

#[test]
fn not_ready_refuses_every_read_before_the_caller_check() {
    let svc = ReadService::new(
        Arc::new(AllowedCallers::default()),
        Arc::new(StartupState::default()),
        validator(),
    );
    let ctx = platform_ctx("app-seeder");
    assert_eq!(svc.list_section_keys(&ctx), Err(DomainError::NotReady));
    assert_eq!(
        svc.section_schema(&ctx, "first").err(),
        Some(DomainError::NotReady)
    );
    assert_eq!(svc.manifest_schema(&ctx).err(), Some(DomainError::NotReady));
}

#[test]
fn keys_follow_the_allowed_callers_and_never_call_a_plugin() {
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
        let svc = service(actions, &plugin);
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

#[test]
fn one_section_schema_comes_with_its_section_type() {
    let plugin = FakePlugin::new();
    let svc = service(vec![CallerAction::ReadSchema], &plugin);
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

#[test]
fn an_unknown_key_is_checked_after_the_caller() {
    let plugin = FakePlugin::new();
    let allowed = service(vec![CallerAction::ReadSchema], &plugin);
    assert_eq!(
        allowed
            .section_schema(&platform_ctx("app-seeder"), "nope")
            .err(),
        Some(DomainError::UnknownSectionKey)
    );
    let refused = service(vec![CallerAction::Seed], &plugin);
    assert_eq!(
        refused
            .section_schema(&platform_ctx("app-seeder"), "nope")
            .err(),
        Some(DomainError::NotPermitted {
            action: CallerAction::ReadSchema
#[tokio::test]
async fn schema_reads_refused_by_access_control_call_no_plugin() {
    for (answer, expected) in [
        (AccessError::Unavailable, DomainError::AccessUnavailable),
        (
            AccessError::NotPermitted,
            DomainError::NotPermitted {
                action: Action::ReadSchema,
            },
        ),
    ] {
        let plugin = plugin();
        let svc = read_service(FakeAccess::new(Err(answer)), two_keys(&plugin));
        let [_, single, combined] = every_read(&svc).await;
        assert_eq!(single, Err(expected.clone()));
        assert_eq!(combined, Err(expected));
        assert_eq!(plugin.calls(), 0);
    }
}

#[tokio::test]
async fn unknown_key_is_refused_after_the_permit_with_no_plugin_call() {
    let plugin = plugin();
    let access = FakeAccess::new(Ok(()));
    let svc = read_service(Arc::clone(&access), two_keys(&plugin));
    assert_eq!(
        svc.section_schema(&ctx(), "absent").await,
        Err(DomainError::UnknownSectionKey)
    );
    assert_eq!(access.calls(), 1);
    assert_eq!(plugin.calls(), 0);
}

#[tokio::test]
async fn section_schema_is_the_composed_live_schema() {
    let first = serving(json!({ "type": "object" }), "v7");
    let other = plugin();
    let state = ready(vec![
        section("first", 1, &first),
        section("other", 2, &other),
    ]);
    let svc = read_service(FakeAccess::new(Ok(())), state);
    let read = svc.section_schema(&ctx(), "first").await.unwrap();
    assert_eq!(
        read,
        SectionSchemaRead {
            key: "first".to_owned(),
            schema: json!({
                "$id": "urn:cf:application-manager:section:first",
                "type": "object"
            }),
            schema_version: "v7".to_owned(),
        }
    );
    assert_eq!(
        (first.calls(), other.calls()),
        (1, 0),
        "only the key's plugin"
    );
}

#[tokio::test]
async fn schemas_are_fetched_on_every_read() {
    let first = serving(json!({ "type": "object" }), "1");
    let svc = read_service(
        FakeAccess::new(Ok(())),
        ready(vec![section("first", 1, &first)]),
    );
    assert_eq!(
        svc.section_schema(&ctx(), "first")
            .await
            .unwrap()
            .schema_version,
        "1"
    );
    first.set_schema(SchemaAnswer::serve(json!({ "type": "array" }), "2"));
    let read = svc.section_schema(&ctx(), "first").await.unwrap();
    assert_eq!(read.schema_version, "2");
    assert_eq!(read.schema["type"], "array");
    let combined = svc.manifest_schema(&ctx()).await.unwrap();
    assert_eq!(combined.sections[0].schema_version, "2");
    assert_eq!(first.calls(), 3);
}

#[tokio::test]
async fn single_read_maps_each_plugin_answer() {
    let cases = [
        (
            SchemaAnswer::NotPermitted("needs seeder role".to_owned()),
            DomainError::SectionNotPermitted {
                key: "first".to_owned(),
                detail: "needs seeder role".to_owned(),
            },
        ),
        (
            SchemaAnswer::Internal,
            unavailable(&[("first", UnavailableReason::Unreachable)]),
        ),
        (
            SchemaAnswer::Rejected,
            unavailable(&[("first", UnavailableReason::Unreachable)]),
        ),
        (
            SchemaAnswer::Panic,
            unavailable(&[("first", UnavailableReason::Unreachable)]),
        ),
        (
            SchemaAnswer::serve(json!({ "$ref": "https://example.com/s.json" }), "1"),
            unavailable(&[("first", UnavailableReason::Unusable)]),
        ),
        (
            SchemaAnswer::serve(json!({}), ""),
            unavailable(&[("first", UnavailableReason::Unusable)]),
        ),
    ];
    for (answer, expected) in cases {
        let first = answering(answer);
        let svc = read_service(
            FakeAccess::new(Ok(())),
            ready(vec![section("first", 1, &first)]),
        );
        assert_eq!(svc.section_schema(&ctx(), "first").await, Err(expected));
    }
}

#[tokio::test(start_paused = true)]
async fn a_hanging_plugin_is_timed_out() {
    let first = answering(SchemaAnswer::Hang);
    let svc = read_service(
        FakeAccess::new(Ok(())),
        ready(vec![section("first", 1, &first)]),
    );
    let started = tokio::time::Instant::now();
    assert_eq!(
        svc.section_schema(&ctx(), "first").await,
        Err(unavailable(&[("first", UnavailableReason::TimedOut)]))
    );
    assert_eq!(started.elapsed(), CALL_LIMIT);
}

#[tokio::test]
async fn plugin_detail_is_cut_at_the_limit() {
    let long = "\u{e9}".repeat(PLUGIN_DETAIL_MAX_BYTES);
    let first = answering(SchemaAnswer::NotPermitted(long));
    let svc = read_service(
        FakeAccess::new(Ok(())),
        ready(vec![section("first", 1, &first)]),
    );
    let Err(DomainError::SectionNotPermitted { detail, .. }) =
        svc.section_schema(&ctx(), "first").await
    else {
        panic!("expected a plugin refusal");
    };
    assert_eq!(detail.len(), PLUGIN_DETAIL_MAX_BYTES);
    assert!(detail.chars().all(|c| c == '\u{e9}'));
}

#[tokio::test]
async fn manifest_schema_combines_every_schema_in_priority_order() {
    let a = serving(json!({ "type": "object" }), "a1");
    let b = serving(json!({ "type": "array" }), "b1");
    let svc = read_service(
        FakeAccess::new(Ok(())),
        ready(vec![section("b", 2, &b), section("a", 1, &a)]),
    );
    let read = svc.manifest_schema(&ctx()).await.unwrap();
    assert_eq!(
        read.sections,
        [
            SectionVersion {
                key: "a".to_owned(),
                schema_version: "a1".to_owned()
            },
            SectionVersion {
                key: "b".to_owned(),
                schema_version: "b1".to_owned()
            },
        ]
    );
    let sections = &read.schema["properties"]["sections"];
    assert_eq!(sections["required"], json!(["a", "b"]));
    assert_eq!(
        sections["properties"]["b"],
        json!({ "$id": "urn:cf:application-manager:section:b", "type": "array" })
    );
}

#[tokio::test]
async fn combined_read_prefers_unavailable_and_lists_every_key() {
    let ok = plugin();
    let refusing = answering(SchemaAnswer::NotPermitted("no".to_owned()));
    let failing = answering(SchemaAnswer::Internal);
    let unusable = answering(SchemaAnswer::serve(json!(42), "1"));
    let state = ready(vec![
        section("ok", 1, &ok),
        section("refusing", 2, &refusing),
        section("failing", 3, &failing),
        section("unusable", 4, &unusable),
    ]);
    let svc = read_service(FakeAccess::new(Ok(())), state);
    assert_eq!(
        svc.manifest_schema(&ctx()).await,
        Err(unavailable(&[
            ("failing", UnavailableReason::Unreachable),
            ("unusable", UnavailableReason::Unusable),
        ]))
    );
    for p in [&ok, &refusing, &failing, &unusable] {
        assert_eq!(p.calls(), 1, "every plugin asked once");
    }
}

#[tokio::test]
async fn combined_read_with_only_a_plugin_refusal_is_not_permitted() {
    let ok = plugin();
    let refusing = answering(SchemaAnswer::NotPermitted("no".to_owned()));
    let svc = read_service(
        FakeAccess::new(Ok(())),
        ready(vec![
            section("ok", 1, &ok),
            section("refusing", 2, &refusing),
        ]),
    );
    assert_eq!(
        svc.manifest_schema(&ctx()).await,
        Err(DomainError::SectionNotPermitted {
            key: "refusing".to_owned(),
            detail: "no".to_owned(),
        })
    );
}

#[test]
fn the_combined_schema_lists_every_section_type_in_priority_order() {
    let plugin = FakePlugin::new();
    let svc = service(vec![CallerAction::ReadSchema], &plugin);
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
#[tokio::test]
async fn combined_schema_of_an_empty_plugin_set_admits_only_empty_sections() {
    let svc = read_service(FakeAccess::new(Ok(())), ready(vec![]));
    let read = svc.manifest_schema(&ctx()).await.unwrap();
    assert!(read.sections.is_empty());
    let sections = &read.schema["properties"]["sections"];
    assert_eq!(sections["required"], json!([]));
    assert_eq!(sections["additionalProperties"], json!(false));
}

fn unavailable(items: &[(&str, UnavailableReason)]) -> DomainError {
    DomainError::SchemaUnavailable {
        items: items
            .iter()
            .map(|(key, reason)| UnavailableSchema {
                key: (*key).to_owned(),
                reason: *reason,
            })
            .collect(),
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn an_unusable_schema_is_logged_with_its_key_and_cause() {
    for (schema, version, cause) in [
        (
            json!({ "$ref": "https://example.com/s.json" }),
            "1",
            "external_ref",
        ),
        (json!({}), "", "bad_schema_version"),
    ] {
        let first = answering(SchemaAnswer::serve(schema, version));
        let svc = read_service(
            FakeAccess::new(Ok(())),
            ready(vec![section("first", 1, &first)]),
        );
        assert!(svc.section_schema(&ctx(), "first").await.is_err());
        assert!(
            logs_contain(&format!("key=first cause=\"{cause}\"")),
            "{cause}"
        );
    }
}

// ── read_operation ──────────────────────────────────────────────────────────

mod operation_read {
    use application_manager_sdk::{
        OperationAction, OutcomeReport, ReportApplication, RequestCategory,
    };
    use time::Duration as Span;
    use uuid::Uuid;

    use super::*;
    use crate::domain::audit::{AuditEvent, AuditKind};
    use crate::domain::operation::{NewOperation, OperationStatus};
    use crate::domain::ports::OperationRepository;

    const OP: Uuid = Uuid::from_u128(0x51);

    fn report() -> OutcomeReport {
        OutcomeReport {
            operation_id: OP,
            action: OperationAction::Seed,
            category: None,
            interrupted: false,
            application: ReportApplication {
                id: "acme".to_owned(),
                version: Some("1".to_owned()),
            },
            sections: vec![],
            problems: vec![],
            unavailable: vec![],
        }
    }

    fn event(id: u128, kind: AuditKind) -> AuditEvent {
        AuditEvent {
            event_id: Uuid::from_u128(id),
            kind,
            correlation_id: OP,
            action: OperationAction::Seed,
            written_at: T0,
            content: serde_json::json!({}),
        }
    }

    /// A service over a real repository holding the open operation `OP`,
    /// due `deadline` seconds after `T0`.
    async fn service(
        access: Arc<FakeAccess>,
        deadline: i64,
    ) -> (ReadService, Arc<HookedRepo>, Arc<FakeClock>) {
        let store = sqlite_repo().await;
        store
            .accept(
                NewOperation {
                    operation_id: OP,
                    application_id: "acme".to_owned(),
                    action: OperationAction::Seed,
                    accepted_at: T0,
                    run_deadline: T0 + Span::seconds(deadline),
                    finished_event_id: Uuid::from_u128(0x53),
                    report: report(),
                },
                event(0x52, AuditKind::Accepted),
                T0,
            )
            .await
            .unwrap();
        let repo = HookedRepo::over(store);
        let clock = FakeClock::new(T0);
        let svc = read_service_over(access, ready(vec![]), Arc::clone(&repo), Arc::clone(&clock));
        (svc, repo, clock)
    }

    #[tokio::test]
    async fn a_denied_read_makes_no_storage_call() {
        for (answer, expected) in [
            (
                Err(AccessError::NotPermitted),
                DomainError::NotPermitted {
                    action: Action::ReadOperation,
                },
            ),
            (
                Err(AccessError::Unavailable),
                DomainError::AccessUnavailable,
            ),
        ] {
            let access = FakeAccess::new(answer);
            let svc = read_service_over(
                Arc::clone(&access),
                ready(vec![]),
                HookedRepo::none(),
                FakeClock::new(T0),
            );
            assert_eq!(svc.read_operation(&ctx(), OP).await, Err(expected));
            assert_eq!(access.calls(), 1);
            assert_eq!(*access.actions.lock().unwrap(), [Action::ReadOperation]);
            assert_eq!(*access.application_ids.lock().unwrap(), [None]);
        }
    }

    #[tokio::test]
    async fn an_open_operation_is_read_without_a_final_report() {
        let (svc, _, _) = service(FakeAccess::new(Ok(())), 60).await;
        let op = svc.read_operation(&ctx(), OP).await.unwrap();
        assert_eq!(op.operation_id, OP);
        assert_eq!(op.status, OperationStatus::Accepted);
        assert_eq!(op.category, None);
        assert!(!op.report.interrupted);
    }

    #[tokio::test]
    async fn an_unknown_operation_is_not_found() {
        let (svc, _, _) = service(FakeAccess::new(Ok(())), 60).await;
        assert_eq!(
            svc.read_operation(&ctx(), Uuid::from_u128(0x99)).await,
            Err(DomainError::OperationNotFound)
        );
    }

    #[tokio::test]
    async fn an_expired_open_operation_is_interrupted_before_the_read() {
        let (svc, repo, clock) = service(FakeAccess::new(Ok(())), 60).await;
        clock.set(T0 + Span::seconds(61));
        let op = svc.read_operation(&ctx(), OP).await.unwrap();
        assert_eq!(op.status, OperationStatus::Interrupted);
        assert_eq!(
            op.category, None,
            "an interrupted operation has no category"
        );
        assert!(op.report.interrupted);
        let events = repo.audit_events(OP).await.unwrap();
        assert_eq!(events.len(), 2, "the interruption wrote the finished event");
    }

    #[tokio::test]
    async fn a_storage_failure_is_unavailable() {
        let (svc, repo, _) = service(FakeAccess::new(Ok(())), 60).await;
        repo.inject("read_operation", Fault::Unavailable);
        assert_eq!(
            svc.read_operation(&ctx(), OP).await,
            Err(DomainError::StorageUnavailable)
        );
    }

    #[tokio::test]
    async fn a_completed_operation_carries_its_category() {
        let (svc, repo, _) = service(FakeAccess::new(Ok(())), 60).await;
        let mut done = report();
        done.category = Some(RequestCategory::Succeeded);
        repo.finish(
            OP,
            &done,
            RequestCategory::Succeeded,
            event(0x53, AuditKind::Finished),
            T0 + Span::seconds(1),
        )
        .await
        .unwrap();
        let op = svc.read_operation(&ctx(), OP).await.unwrap();
        assert_eq!(op.status, OperationStatus::Completed);
        assert_eq!(op.category, Some(RequestCategory::Succeeded));
    }
}
