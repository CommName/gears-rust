//! Backend-independent registry cases (DESIGN §3.7, §3.2 "Registry Repository", §4.8
//! "Integration" and "Security", ADR 0005, ADR 0007).
//!
//! Each case is an `async fn name(db: &RegistryTestDb)` written once. The `registry_cases!`
//! macro below turns the list into:
//!
//! - the case functions themselves (`pub`), and `all()`, the `(name, case)` list the
//!   Postgres and `MySQL` lanes iterate (`infra::storage::postgres_tests`, `mysql_tests`,
//!   through `run_lane`);
//! - the `SQLite` lane, `sqlite_lane::<name>`: one `#[tokio::test]` per case on a fresh
//!   migrated `SQLite` file database. Run it with
//!   `cargo test -p cf-gears-application-manager registry_cases::sqlite_lane`.
//!
//! So the Postgres and `MySQL` lanes run exactly the bodies the `SQLite` lane runs (DESIGN
//! §4.8: "`SQLite` always; `PostgreSQL` and `MySQL` ... in the integration lane"). Cases that
//! need to control the pool (acquire timeouts, unreachable storage) are `SQLite`-only and
//! live in `infra::storage::store_tests`; a timeout INSIDE a statement (a lock held by a
//! second connection) is a shared case here.
//!
//! Timestamps in these cases are whole microseconds: `RegistryEntry::new` truncates to
//! microseconds, the precision every backend keeps.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sea_orm::ActiveValue::Set;
use sea_orm::EntityTrait;
use time::OffsetDateTime;
use toolkit_db::DbError;
use toolkit_db::secure::{
    AccessScope, ScopeError, SecureEntityExt, SecureInsertExt, secure_insert,
};
use uuid::Uuid;

use crate::domain::limits::REGISTRY_TEXT_MAX_BYTES;
use crate::domain::ports::{RegistryError, RegistryStore};
use crate::domain::registry::{LastResult, RegistryEntry};
use crate::infra::storage::entity;
use crate::infra::storage::migrations::Migrator;
use crate::infra::storage::repo::{DeleteOutcome, RegistryRepo};
use crate::test_support::registry_db::{self, REGISTRY_TABLE, RegistryTestDb, real_bounded};

/// One boxed case.
pub type CaseFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
/// One case as a function pointer.
pub type CaseFn = for<'a> fn(&'a RegistryTestDb) -> CaseFuture<'a>;

fn at(secs: u64, micros: u32) -> SystemTime {
    UNIX_EPOCH + Duration::new(secs, micros * 1_000)
}

fn entry(
    id: &str,
    version: &str,
    result: LastResult,
    seeded_at: SystemTime,
    audit_id: &str,
) -> RegistryEntry {
    RegistryEntry::new(id, version, result, seeded_at, audit_id).expect("a storable entry")
}

fn sample(id: &str) -> RegistryEntry {
    entry(
        id,
        "1.0.0",
        LastResult::Succeeded,
        at(1_800_000_000, 123_456),
        "audit-1",
    )
}

fn store(db: &RegistryTestDb) -> crate::infra::storage::store::SeaRegistryStore {
    db.store(Duration::from_secs(30))
}

fn by_id(mut entries: Vec<RegistryEntry>) -> Vec<RegistryEntry> {
    entries.sort_by(|a, b| a.application_id().cmp(b.application_id()));
    entries
}

/// A row written straight through the entity, bypassing `RegistryEntry` validation, so a
/// case can ask the DATABASE (not the domain type) to reject a value.
fn raw_row(id: &str, version: &str, last_result: &str) -> entity::ActiveModel {
    entity::ActiveModel {
        application_id: Set(id.to_owned()),
        version: Set(version.to_owned()),
        last_result: Set(last_result.to_owned()),
        last_seeded_at: Set(
            OffsetDateTime::from_unix_timestamp(1_800_000_000).expect("a valid unix timestamp")
        ),
        audit_id: Set("audit-raw".to_owned()),
    }
}

async fn insert_raw(
    db: &RegistryTestDb,
    am: entity::ActiveModel,
) -> Result<entity::Model, ScopeError> {
    secure_insert::<entity::Entity>(am, &AccessScope::allow_all(), &db.conn()).await
}

async fn all_rows(
    db: &RegistryTestDb,
    scope: &AccessScope,
) -> Result<Vec<entity::Model>, ScopeError> {
    entity::Entity::find()
        .secure()
        .scope_with(scope)
        .all(&db.conn())
        .await
}

fn tenant_scopes() -> Vec<AccessScope> {
    vec![
        AccessScope::for_tenant(Uuid::from_u128(0xA)),
        AccessScope::for_tenants(vec![Uuid::from_u128(0xA), Uuid::from_u128(0xB)]),
        AccessScope::deny_all(),
    ]
}

/// Runs every case of `all()` on a fresh database of one container (Postgres or `MySQL`
/// lane), each in its own task so a failing or hanging case is named and the rest still
/// run. A case that exceeds `case_bound` is aborted before it is recorded. Returns the
/// failure lines; the caller asserts the list is empty.
pub async fn run_lane<F, Fut>(case_bound: Duration, mut fresh_database: F) -> Vec<String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = RegistryTestDb>,
{
    let mut failures = Vec::new();
    for (name, case) in all() {
        let db = Arc::new(fresh_database().await);
        let mut task = tokio::spawn({
            let db = Arc::clone(&db);
            async move { case(&db).await }
        });
        match tokio::time::timeout(case_bound, &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(join_error)) => failures.push(format!("{name}: {join_error}")),
            Err(_) => {
                task.abort();
                failures.push(format!("{name}: exceeded {case_bound:?}"));
            }
        }
    }
    failures
}

macro_rules! registry_cases {
    ($( $(#[$meta:meta])* async fn $name:ident($db:ident: &RegistryTestDb) $body:block )*) => {
        $( $(#[$meta])* pub async fn $name($db: &RegistryTestDb) $body )*

        /// Every case with its name, for the Postgres lane.
        #[must_use]
        pub fn all() -> Vec<(&'static str, CaseFn)> {
            vec![$((
                stringify!($name),
                {
                    fn call(db: &RegistryTestDb) -> CaseFuture<'_> {
                        Box::pin($name(db))
                    }
                    call as CaseFn
                },
            )),*]
        }

        /// The `SQLite` lane: one test per case on a fresh migrated database.
        mod sqlite_lane {
            use super::real_bounded;
            use crate::test_support::registry_db;
            $(
                #[tokio::test]
                async fn $name() {
                    let db = registry_db::sqlite().await;
                    real_bounded(super::$name(&db)).await;
                }
            )*
        }
    };
}

registry_cases! {
    // ---- store behaviour (DESIGN §3.2 "Registry Repository", §3.7) ----------------------

    async fn upsert_inserts_a_new_entry_and_get_returns_every_field(db: &RegistryTestDb) {
        let store = store(db);
        let e = entry("app-1", "2.3.4", LastResult::PartiallyFailed, at(1_800_000_000, 654_321), "audit-9");

        store.upsert(&e).await.expect("upsert a new entry");

        assert_eq!(store.get("app-1").await.expect("get"), Some(e));
    }

    async fn get_of_a_missing_id_returns_none(db: &RegistryTestDb) {
        let store = store(db);
        assert_eq!(store.get("absent").await.expect("get"), None);

        store.upsert(&sample("present")).await.expect("upsert");
        assert_eq!(store.get("absent").await.expect("get"), None);
        assert_eq!(store.get("presen").await.expect("get"), None, "no prefix match");
        assert_eq!(store.get("present ").await.expect("get"), None, "no trailing-space match");
    }

    async fn upsert_overwrites_every_field_on_reseed(db: &RegistryTestDb) {
        let store = store(db);
        let first = entry("app", "1.0.0", LastResult::Succeeded, at(1_000_000_000, 1), "audit-first");
        let second = entry("app", "2.0.0", LastResult::InternalFailure, at(1_900_000_000, 999_999), "audit-second");
        store.upsert(&first).await.expect("first seed");

        store.upsert(&second).await.expect("re-seed overwrites");

        let got = store.get("app").await.expect("get").expect("present");
        assert_eq!(got, second);
        assert_eq!(got.version(), "2.0.0");
        assert_eq!(got.last_result(), LastResult::InternalFailure);
        assert_eq!(got.last_seeded_at(), at(1_900_000_000, 999_999));
        assert_eq!(got.audit_id(), "audit-second");
        assert_eq!(store.list().await.expect("list").len(), 1, "one row, not two");
    }

    async fn reseed_leaves_other_entries_untouched(db: &RegistryTestDb) {
        let store = store(db);
        let other = entry("other", "9", LastResult::NotPermitted, at(1_700_000_000, 5), "audit-other");
        store.upsert(&other).await.expect("upsert other");
        store.upsert(&sample("app")).await.expect("upsert app");

        store
            .upsert(&entry("app", "2", LastResult::Succeeded, at(1_800_000_001, 0), "audit-2"))
            .await
            .expect("re-seed app");

        assert_eq!(store.get("other").await.expect("get"), Some(other));
        assert_eq!(store.list().await.expect("list").len(), 2);
    }

    async fn delete_removes_only_the_named_entry(db: &RegistryTestDb) {
        let store = store(db);
        let keep = sample("keep");
        store.upsert(&sample("gone")).await.expect("upsert");
        store.upsert(&keep).await.expect("upsert");

        store.delete("gone").await.expect("delete");

        assert_eq!(store.get("gone").await.expect("get"), None);
        assert_eq!(store.get("keep").await.expect("get"), Some(keep));
    }

    async fn delete_of_a_missing_entry_counts_as_deleted(db: &RegistryTestDb) {
        let store = store(db);
        store.delete("never-seeded").await.expect("a missing entry counts as deleted");

        store.upsert(&sample("app")).await.expect("upsert");
        store.delete("app").await.expect("first delete");
        store.delete("app").await.expect("a repeated delete counts as deleted");
        assert_eq!(store.get("app").await.expect("get"), None);
    }

    async fn list_of_an_empty_registry_is_empty(db: &RegistryTestDb) {
        assert_eq!(store(db).list().await.expect("list"), Vec::new());
    }

    async fn list_returns_every_entry_exactly_once(db: &RegistryTestDb) {
        // DESIGN §3.2 and §4.10: the listing is never ordered or paginated, so the case
        // compares the set and never the order.
        let store = store(db);
        let mut expected = Vec::new();
        for (i, result) in LastResult::ALL.into_iter().enumerate() {
            let e = entry(
                &format!("app-{i}"),
                &format!("{i}.0"),
                result,
                at(1_800_000_000 + i as u64, 7),
                &format!("audit-{i}"),
            );
            store.upsert(&e).await.expect("upsert");
            expected.push(e);
        }

        let listed = by_id(store.list().await.expect("list"));

        assert_eq!(listed, by_id(expected));
    }

    async fn every_last_result_value_round_trips(db: &RegistryTestDb) {
        let store = store(db);
        for result in LastResult::ALL {
            let id = format!("app-{}", result.as_token());
            let e = entry(&id, "1", result, at(1_800_000_000, 1), "a");
            store.upsert(&e).await.expect("upsert");
            let got = store.get(&id).await.expect("get").expect("present");
            assert_eq!(got.last_result(), result);
            assert_eq!(got, e);
        }
    }

    async fn text_at_exactly_the_byte_bound_round_trips(db: &RegistryTestDb) {
        let store = store(db);
        let ascii = "a".repeat(REGISTRY_TEXT_MAX_BYTES);
        let three_byte = "\u{20ac}".repeat(85);
        assert_eq!(three_byte.len(), REGISTRY_TEXT_MAX_BYTES, "85 x 3 bytes");
        let two_byte = "\u{e9}".repeat(127);

        for (id, version) in [(&ascii, &ascii), (&three_byte, &three_byte), (&two_byte, &ascii)] {
            let e = entry(id, version, LastResult::Succeeded, at(1_800_000_000, 0), "a");
            store.upsert(&e).await.expect("a value at the bound is stored");
            assert_eq!(store.get(id).await.expect("get"), Some(e), "stored exactly");
        }
        assert_eq!(store.list().await.expect("list").len(), 3);
    }

    async fn timestamps_round_trip_at_microsecond_precision(db: &RegistryTestDb) {
        let store = store(db);
        let times = [
            at(0, 0),
            at(0, 1),
            at(946_684_800, 500_000),
            at(1_800_000_000, 999_999),
            at(4_000_000_000, 1),
            // 9999-12-31T23:59:59.999999Z: the last instant an entry accepts, and the last
            // one a MySQL DATETIME(6) holds (`SeededAtOutOfRange` rejects anything later).
            at(253_402_300_799, 999_999),
        ];
        for (i, t) in times.into_iter().enumerate() {
            let id = format!("t-{i}");
            let e = entry(&id, "1", LastResult::Succeeded, t, "a");
            store.upsert(&e).await.expect("upsert");
            assert_eq!(store.get(&id).await.expect("get").expect("present").last_seeded_at(), t);
        }
        // A nanosecond-precise value is cut by the entry, so every backend agrees.
        let ragged = RegistryEntry::new("ragged", "1", LastResult::Succeeded, at(1_800_000_000, 0) + Duration::from_nanos(123_456_789), "a")
            .expect("entry");
        store.upsert(&ragged).await.expect("upsert");
        assert_eq!(store.get("ragged").await.expect("get"), Some(ragged));
    }

    async fn ids_differing_by_case_space_or_normalization_stay_distinct(db: &RegistryTestDb) {
        // DESIGN §3.7: binary, no-pad comparison on every backend; §4.8 security (f): an
        // uninstall removes exactly the id it names, never a case, space or normalization
        // variant of it.
        let store = store(db);
        let ids = ["app", "App", "APP", "app ", " app", "caf\u{e9}", "cafe\u{301}"];
        let seed_all = || async {
            for (i, id) in ids.iter().enumerate() {
                store
                    .upsert(&entry(id, &format!("v{i}"), LastResult::Succeeded, at(1_800_000_000, 0), "a"))
                    .await
                    .expect("upsert");
            }
        };

        seed_all().await;
        assert_eq!(store.list().await.expect("list").len(), ids.len(), "every id is its own entry");
        for (i, id) in ids.iter().enumerate() {
            let got = store.get(id).await.expect("get").expect("present");
            assert_eq!(got.application_id(), *id);
            assert_eq!(got.version(), format!("v{i}"), "{id:?} must hit its own row");
        }

        for victim in ids {
            seed_all().await;
            store.delete(victim).await.expect("delete");
            for id in ids {
                let present = store.get(id).await.expect("get").is_some();
                assert_eq!(present, id != victim, "after deleting {victim:?}, {id:?} present={present}");
            }
            assert_eq!(store.list().await.expect("list").len(), ids.len() - 1, "only the exact id");
        }
    }

    async fn awkward_ids_round_trip_exactly(db: &RegistryTestDb) {
        // `.`, `..` and `a/b` uninstall through the body form (DESIGN §4.8 security (f)); the
        // rest are SQL and pattern metacharacters, which a parameterized query must not read.
        let store = store(db);
        let ids = [
            ".", "..", "a/b", "%", "_", "a%b", "100%", "a_b", "back\\slash", "\"quoted\"",
            "it's", "'; DROP TABLE application_registry; --", " ", "\u{1f600}",
            "\u{5d0}\u{5d1}\u{5d2}", "a\u{a0}b", "a\u{200b}b", "a\u{2028}b", "\u{feff}bom",
            "x'--", "null", "NULL",
        ];
        for id in ids {
            store.upsert(&sample(id)).await.expect("upsert");
        }
        assert_eq!(store.list().await.expect("list").len(), ids.len());

        for id in ids {
            let got = store.get(id).await.expect("get").expect("present");
            assert_eq!(got.application_id(), id, "stored exactly");
        }
        // Wildcards match only themselves.
        store.delete("%").await.expect("delete");
        store.delete("_").await.expect("delete");
        assert_eq!(store.list().await.expect("list").len(), ids.len() - 2);
        assert!(store.get("a%b").await.expect("get").is_some());
        assert!(store.get("a_b").await.expect("get").is_some());
        assert!(registry_db::table_exists(db, REGISTRY_TABLE).await, "the table survived");
    }

    async fn lookups_of_an_id_that_can_never_be_stored_find_nothing_on_every_backend(db: &RegistryTestDb) {
        // DESIGN §3.2: the repository never interprets the id. Postgres cannot bind a NUL
        // in text, so only an id holding NUL is answered without asking the database; the
        // empty id, control characters and an over-long id are real queries that match
        // nothing. (The never-storable uninstall short-circuit belongs to the orchestrator,
        // through `is_storable_text`.)
        let store = store(db);
        store.upsert(&sample("kept")).await.expect("upsert");
        let too_long = "a".repeat(REGISTRY_TEXT_MAX_BYTES + 1);
        for id in ["", "a\0b", "\0", "a\nb", "a\u{7f}", "a\u{9f}", too_long.as_str()] {
            assert_eq!(store.get(id).await.expect("get of a never-storable id"), None, "{id:?}");
            store.delete(id).await.expect("delete of a never-storable id");
        }
        assert_eq!(store.list().await.expect("list").len(), 1, "nothing was deleted");
    }

    async fn concurrent_upserts_to_one_id_leave_one_complete_entry(db: &RegistryTestDb) {
        // DESIGN §4.2, §4.5: no coordination, the last writer wins, and a row is never a mix.
        let store = store(db);
        let candidates: Vec<RegistryEntry> = LastResult::ALL
            .into_iter()
            .enumerate()
            .map(|(i, result)| {
                let micros = u32::try_from(i).expect("a small index");
                entry("hot", &format!("v{i}"), result, at(1_800_000_000 + u64::from(micros), micros), &format!("audit-{i}"))
            })
            .collect();

        let results = futures::future::join_all(candidates.iter().map(|c| store.upsert(c))).await;

        // One statement per upsert: a competing writer waits for the lock and then wins or
        // loses, it never fails, on any backend.
        assert!(results.iter().all(Result::is_ok), "every concurrent upsert succeeds: {results:?}");
        let got = store.get("hot").await.expect("get").expect("present");
        assert!(candidates.contains(&got), "the row equals one submitted entry as a whole: {got:?}");
        assert_eq!(store.list().await.expect("list").len(), 1);
    }

    async fn a_reader_never_misses_the_row_while_a_writer_re_seeds_it(db: &RegistryTestDb) {
        // DESIGN §4.2: the upsert is one atomic statement, so a concurrent get never finds a
        // gap between "old row removed" and "new row written" (a delete-then-insert upsert
        // would).
        let store = store(db);
        let version = |i: u32| entry("hot", &format!("v{i}"), LastResult::Succeeded, at(1_800_000_000, i), "a");
        store.upsert(&version(0)).await.expect("the first seed");
        let stop = Arc::new(AtomicBool::new(false));
        let reader = tokio::spawn({
            let store = store.clone();
            let stop = Arc::clone(&stop);
            async move {
                let mut reads = 0_u32;
                while !stop.load(Ordering::Relaxed) {
                    match store.get("hot").await {
                        Ok(Some(_)) => reads += 1,
                        other => return Err(format!("read {reads} saw {other:?}")),
                    }
                    tokio::task::yield_now().await;
                }
                Ok(reads)
            }
        });

        for i in 1..=300 {
            store.upsert(&version(i)).await.expect("re-seed");
        }
        stop.store(true, Ordering::Relaxed);

        let reads = reader.await.expect("the reader task").expect("a get never saw a missing or failed row");
        assert!(reads > 0, "the reader ran while the writer wrote");
        assert_eq!(store.get("hot").await.expect("get"), Some(version(300)));
    }

    async fn audit_ids_round_trip_exactly_at_any_length(db: &RegistryTestDb) {
        // The audit id is opaque (DESIGN §3.7 states no bound), so the column must hold
        // 100 KB of multi-byte text: a VARCHAR(255) or a 64 KB TEXT cannot.
        let store = store(db);
        let long = "\u{e9}\u{20ac}\u{1f600}".repeat(11_500);
        assert!(long.len() > 100_000);
        let medium = "x".repeat(300);
        let audits = ["", "\t", "\n", "\u{1f600}", "  padded  ", medium.as_str(), long.as_str()];
        for (i, audit) in audits.into_iter().enumerate() {
            let e = entry(&format!("audit-{i}"), "1", LastResult::Succeeded, at(1_800_000_000, 0), audit);
            store.upsert(&e).await.expect("upsert");
            let got = store.get(e.application_id()).await.expect("get").expect("present");
            assert_eq!(got.audit_id(), audit, "audit id #{i} ({} bytes)", audit.len());
            assert_eq!(got, e);
        }
    }

    async fn an_unreadable_stored_row_is_unavailable_and_never_a_panic(db: &RegistryTestDb) {
        // CHECK constraints do not forbid control characters, so a row written outside the
        // gear can hold one. Only the gear writes the table: such a row means outside
        // tampering. Strict: that id and the whole listing answer Unavailable, the other ids
        // still answer, and nothing panics.
        let store = store(db);
        let good = sample("good");
        store.upsert(&good).await.expect("upsert");

        insert_raw(db, raw_row("bad-version", "line1\nline2", "succeeded"))
            .await
            .expect("the database accepts a control character in the version");
        assert_eq!(store.get("bad-version").await, Err(RegistryError::Unavailable));
        assert_eq!(store.get("good").await, Ok(Some(good.clone())));
        assert_eq!(store.list().await, Err(RegistryError::Unavailable));

        store.delete("bad-version").await.expect("an unreadable row can still be deleted");
        assert_eq!(store.list().await, Ok(vec![good.clone()]));

        insert_raw(db, raw_row("bad\nid", "1", "succeeded"))
            .await
            .expect("the database accepts a control character in the id");
        assert_eq!(store.get("bad\nid").await, Err(RegistryError::Unavailable));
        assert_eq!(store.get("good").await, Ok(Some(good)));
        assert_eq!(store.list().await, Err(RegistryError::Unavailable));
    }

    async fn a_listing_returns_every_entry_even_past_a_thousand(db: &RegistryTestDb) {
        // DESIGN §3.2, §4.10: no ordering, no pagination, no row cap.
        let expected: Vec<RegistryEntry> = (0..1_001_u32)
            .map(|i| {
                entry(
                    &format!("bulk-{i:04}"),
                    "1",
                    LastResult::ALL[(i % 5) as usize],
                    at(1_800_000_000 + u64::from(i), i % 1_000),
                    &format!("audit-{i}"),
                )
            })
            .collect();
        let to_insert = expected.clone();
        db.provider
            .transaction(|tx| {
                Box::pin(async move {
                    for e in &to_insert {
                        RegistryRepo::upsert(tx, &AccessScope::allow_all(), e)
                            .await
                            .map_err(|e| DbError::Other(anyhow::anyhow!("{e:?}")))?;
                    }
                    Ok::<(), DbError>(())
                })
            })
            .await
            .expect("one transaction seeds 1001 entries");

        let listed = by_id(store(db).list().await.expect("list"));

        assert_eq!(listed.len(), 1_001);
        assert_eq!(listed, by_id(expected));
    }

    async fn a_timeout_inside_the_statement_leaves_the_row_old_or_complete(db: &RegistryTestDb) {
        // DESIGN §4.5: the timeout bounds the whole operation, not only the wait for a
        // connection. A second connection holds the lock the upsert needs, so the statement
        // itself blocks; the caller stops at the timeout and the outcome is NOT CONFIRMED:
        // the row is the old one or the complete new one, never a mix.
        const SHORT: Duration = Duration::from_millis(300);
        let patient = store(db);
        let old = sample("locked");
        let new = entry("locked", "2", LastResult::PluginRejected, at(1_900_000_000, 5), "audit-new");
        patient.upsert(&old).await.expect("seed the row the blocker will lock");
        let blocker = registry_db::hold_write_lock(db, "locked").await;
        let impatient = db.store(SHORT);

        let started = Instant::now();
        let result = real_bounded(impatient.upsert(&new)).await;
        let elapsed = started.elapsed();

        assert_eq!(result, Err(RegistryError::TimedOut));
        assert!(elapsed >= SHORT, "returned after {elapsed:?}, before the operation timeout");
        assert!(elapsed < Duration::from_secs(10), "the caller must stop at the timeout: {elapsed:?}");
        blocker.rollback().await.expect("release the lock");

        let got = real_bounded(patient.get("locked")).await.expect("get").expect("the row is still there");
        assert!(got == old || got == new, "neither the old row nor the complete new one: {got:?}");
        assert_eq!(real_bounded(patient.list()).await.expect("list").len(), 1);
    }

    // ---- repository behaviour (DBRunner-generic, scope per call) ------------------------

    async fn repo_delete_reports_deleted_then_not_found(db: &RegistryTestDb) {
        let conn = db.conn();
        let scope = AccessScope::allow_all();
        RegistryRepo::upsert(&conn, &scope, &sample("app")).await.expect("upsert");

        let first = RegistryRepo::delete(&conn, &scope, "app").await.expect("delete");
        let second = RegistryRepo::delete(&conn, &scope, "app").await.expect("delete again");
        let never = RegistryRepo::delete(&conn, &scope, "never").await.expect("delete missing");

        assert_eq!(first, DeleteOutcome::Deleted);
        assert_eq!(second, DeleteOutcome::NotFound, "a missing entry is a distinct result");
        assert_eq!(never, DeleteOutcome::NotFound);
        assert_ne!(DeleteOutcome::Deleted, DeleteOutcome::NotFound);
    }

    async fn repo_runs_every_operation_under_the_allow_all_scope(db: &RegistryTestDb) {
        let conn = db.conn();
        let scope = AccessScope::allow_all();
        let e = sample("app");

        RegistryRepo::upsert(&conn, &scope, &e).await.expect("upsert");

        assert_eq!(RegistryRepo::get(&conn, &scope, "app").await.expect("get"), Some(e.clone()));
        assert_eq!(RegistryRepo::list(&conn, &scope).await.expect("list"), vec![e]);
    }

    async fn repo_sees_and_changes_nothing_under_a_tenant_scope(db: &RegistryTestDb) {
        // ADR 0007, DESIGN §3.7: a query whose scope holds tenant ids is denied on the
        // unrestricted entity. Denied is "no row, no change": an error or an empty result.
        let conn = db.conn();
        let allow_all = AccessScope::allow_all();
        let e = sample("app");
        RegistryRepo::upsert(&conn, &allow_all, &e).await.expect("upsert");

        for scope in tenant_scopes() {
            if let Ok(found) = RegistryRepo::get(&conn, &scope, "app").await {
                assert_eq!(found, None, "get under {scope:?}");
            }
            if let Ok(listed) = RegistryRepo::list(&conn, &scope).await {
                assert!(listed.is_empty(), "list under {scope:?}");
            }
            if let Ok(outcome) = RegistryRepo::delete(&conn, &scope, "app").await {
                assert_eq!(outcome, DeleteOutcome::NotFound, "delete under {scope:?}");
            }
            // A write fails closed: the unrestricted entity makes the platform skip its own
            // insert-scope check, so the repository refuses a scope that is not allow-all.
            let changed = entry("app", "9", LastResult::InternalFailure, at(1_950_000_000, 1), "other");
            let fresh = sample("new-under-tenant");
            assert_eq!(
                RegistryRepo::upsert(&conn, &scope, &changed).await,
                Err(RegistryError::Unavailable),
                "upsert of an existing id under {scope:?}"
            );
            assert_eq!(
                RegistryRepo::upsert(&conn, &scope, &fresh).await,
                Err(RegistryError::Unavailable),
                "upsert of a new id under {scope:?}"
            );
        }
        assert_eq!(
            RegistryRepo::get(&conn, &allow_all, "new-under-tenant").await.expect("get"),
            None,
            "no denied upsert wrote a row"
        );

        assert_eq!(
            RegistryRepo::get(&conn, &allow_all, "app").await.expect("get"),
            Some(e),
            "the entry survived every denied operation"
        );
    }

    async fn repo_works_on_a_transaction_runner_and_rolls_back_with_it(db: &RegistryTestDb) {
        let kept = sample("committed");
        let dropped = sample("rolled-back");

        db.provider
            .transaction(|tx| {
                Box::pin(async move {
                    RegistryRepo::upsert(tx, &AccessScope::allow_all(), &kept)
                        .await
                        .map_err(|e| DbError::Other(anyhow::anyhow!("{e:?}")))
                })
            })
            .await
            .expect("the transaction commits");
        let rolled: Result<(), DbError> = db
            .provider
            .transaction(|tx| {
                Box::pin(async move {
                    RegistryRepo::upsert(tx, &AccessScope::allow_all(), &dropped)
                        .await
                        .map_err(|e| DbError::Other(anyhow::anyhow!("{e:?}")))?;
                    Err(DbError::Other(anyhow::anyhow!("force a rollback")))
                })
            })
            .await;

        assert!(rolled.is_err());
        let store = store(db);
        assert!(store.get("committed").await.expect("get").is_some());
        assert_eq!(store.get("rolled-back").await.expect("get"), None);
    }

    // ---- entity security (ADR 0007, DESIGN §3.7 "Additional info") -----------------------

    async fn secure_insert_with_the_allow_all_scope_needs_no_tenant_id(db: &RegistryTestDb) {
        let model = insert_raw(db, raw_row("secure-1", "1", "succeeded"))
            .await
            .expect("secure_insert needs no tenant id on the unrestricted entity");
        assert_eq!(model.application_id, "secure-1");

        let am = raw_row("secure-2", "1", "succeeded");
        entity::Entity::insert(am.clone())
            .secure()
            .scope_with_model(&AccessScope::allow_all(), &am)
            .expect("the builder form needs no tenant id either")
            .exec(&db.conn())
            .await
            .expect("insert");

        let rows = all_rows(db, &AccessScope::allow_all()).await.expect("select");
        assert_eq!(rows.len(), 2);
    }

    async fn a_tenant_scope_reads_nothing_from_the_unrestricted_entity(db: &RegistryTestDb) {
        insert_raw(db, raw_row("row-1", "1", "succeeded")).await.expect("insert");
        insert_raw(db, raw_row("row-2", "1", "succeeded")).await.expect("insert");
        assert_eq!(all_rows(db, &AccessScope::allow_all()).await.expect("control").len(), 2);

        for scope in tenant_scopes() {
            if let Ok(rows) = all_rows(db, &scope).await {
                assert!(rows.is_empty(), "{scope:?} must not read the registry");
            }
        }
    }

    // ---- constraints enforced by the database itself (DESIGN §3.7 "Constraints") --------

    async fn database_accepts_text_at_the_byte_bound_and_rejects_one_byte_more(db: &RegistryTestDb) {
        let at_bound = [
            "a".repeat(REGISTRY_TEXT_MAX_BYTES),
            "\u{e9}".repeat(127),
            "\u{20ac}".repeat(85),
            "\u{1f600}".repeat(63),
        ];
        let over = [
            "a".repeat(REGISTRY_TEXT_MAX_BYTES + 1),
            "\u{e9}".repeat(128),
            "\u{20ac}".repeat(86),
            "\u{1f600}".repeat(64),
            "\u{e9}".repeat(255),
        ];
        for (i, ok) in at_bound.iter().enumerate() {
            insert_raw(db, raw_row(ok, "1", "succeeded"))
                .await
                .unwrap_or_else(|e| panic!("id of {} bytes must be accepted: {e}", ok.len()));
            insert_raw(db, raw_row(&format!("v-ok-{i}"), ok, "succeeded"))
                .await
                .unwrap_or_else(|e| panic!("version of {} bytes must be accepted: {e}", ok.len()));
        }
        for (i, bad) in over.iter().enumerate() {
            assert!(
                insert_raw(db, raw_row(bad, "1", "succeeded")).await.is_err(),
                "id of {} bytes must be rejected by the database",
                bad.len()
            );
            assert!(
                insert_raw(db, raw_row(&format!("v-bad-{i}"), bad, "succeeded")).await.is_err(),
                "version of {} bytes must be rejected by the database",
                bad.len()
            );
        }
        let stored = all_rows(db, &AccessScope::allow_all()).await.expect("select");
        assert_eq!(stored.len(), at_bound.len() * 2, "no rejected row was stored");
    }

    async fn database_rejects_an_empty_id_or_version(db: &RegistryTestDb) {
        assert!(insert_raw(db, raw_row("", "1", "succeeded")).await.is_err(), "empty id");
        assert!(insert_raw(db, raw_row("app", "", "succeeded")).await.is_err(), "empty version");
        assert!(all_rows(db, &AccessScope::allow_all()).await.expect("select").is_empty());
    }

    async fn database_rejects_a_last_result_outside_the_listed_values(db: &RegistryTestDb) {
        for (i, token) in LastResult::ALL.into_iter().map(LastResult::as_token).enumerate() {
            insert_raw(db, raw_row(&format!("ok-{i}"), "1", token))
                .await
                .unwrap_or_else(|e| panic!("{token} is a listed value: {e}"));
        }
        for (i, bad) in ["", "bogus", "Succeeded", "SUCCEEDED", "not_found", "invalid_manifest", "succeeded "]
            .into_iter()
            .enumerate()
        {
            assert!(
                insert_raw(db, raw_row(&format!("bad-{i}"), "1", bad)).await.is_err(),
                "{bad:?} must be rejected by the database"
            );
        }
        assert_eq!(all_rows(db, &AccessScope::allow_all()).await.expect("select").len(), 5);
    }

    async fn database_rejects_a_seeded_at_outside_the_range(db: &RegistryTestDb) {
        use sea_orm::{ConnectionTrait, DbBackend};
        // (literal, accepted): the CHECK admits 1970-01-01T00:00:00Z up to, excluding,
        // 10000-01-01T00:00:00Z (DESIGN section 3.7).
        let cases: &[(&str, bool)] = match db.backend {
            DbBackend::Postgres => &[
                ("1970-01-01 00:00:00+00", true),
                ("9999-12-31 23:59:59.999999+00", true),
                ("1969-12-31 23:59:59.999999+00", false),
                ("10000-01-01 00:00:00+00", false),
                ("infinity", false),
                ("-infinity", false),
            ],
            DbBackend::MySql => &[
                ("1970-01-01 00:00:00", true),
                ("9999-12-31 23:59:59.999999", true),
                ("1969-12-31 23:59:59.999999", false),
                ("1000-01-01 00:00:00", false),
            ],
            _ => &[
                ("1970-01-01 00:00:00+00:00", true),
                ("9999-12-31 23:59:59.999999+00:00", true),
                ("1969-12-31 23:59:59.999999+00:00", false),
                ("10000-01-01 00:00:00+00:00", false),
                ("-0001-01-01 00:00:00+00:00", false),
            ],
        };
        for (i, (literal, accepted)) in cases.iter().enumerate() {
            let sql = format!(
                "INSERT INTO application_registry \
                 (application_id, version, last_result, last_seeded_at, audit_id) \
                 VALUES ('t-{i}', '1', 'succeeded', '{literal}', 'a')"
            );
            let res = db.raw.execute_unprepared(&sql).await;
            assert_eq!(res.is_ok(), *accepted, "last_seeded_at {literal:?}: {res:?}");
        }
        let kept = cases.iter().filter(|(_, ok)| *ok).count();
        let rows = registry_db::query_rows(db, "SELECT count(*) AS n FROM application_registry").await;
        let stored: i64 = rows[0].try_get("", "n").expect("count");
        assert_eq!(stored, i64::try_from(kept).expect("small"));
    }

    async fn database_rejects_a_second_row_for_the_same_id(db: &RegistryTestDb) {
        insert_raw(db, raw_row("dup", "1", "succeeded")).await.expect("first insert");

        let err = insert_raw(db, raw_row("dup", "2", "succeeded"))
            .await
            .expect_err("the primary key is application_id");

        assert!(err.is_unique_violation(), "{err}");
    }

    // ---- migration (DESIGN §3.7 "Schema", "Constraints", "Additional info") --------------

    async fn migration_creates_the_design_table_without_any_tenant_column(db: &RegistryTestDb) {
        assert!(registry_db::table_exists(db, REGISTRY_TABLE).await);

        let mut cols = registry_db::columns(db, REGISTRY_TABLE).await;
        cols.sort_by(|a, b| a.name.cmp(&b.name));
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            ["application_id", "audit_id", "last_result", "last_seeded_at", "version"],
            "exactly the five columns of DESIGN section 3.7: no tenant, resource, owner or type column"
        );
        assert!(cols.iter().all(|c| c.not_null), "every column is NOT NULL: {cols:?}");
        assert_eq!(registry_db::primary_key(db, REGISTRY_TABLE).await, ["application_id"]);
        assert_eq!(
            registry_db::columns_named_like(db, "tenant").await,
            0,
            "no column named for a tenant anywhere in the gear's schema (ADR 0007)"
        );
        assert_eq!(registry_db::columns_named_like(db, "owner").await, 0);
    }

    async fn migration_adds_no_secondary_index_foreign_key_or_trigger(db: &RegistryTestDb) {
        assert_eq!(registry_db::secondary_index_count(db, REGISTRY_TABLE).await, 0, "the primary key is the only lookup");
        assert_eq!(registry_db::foreign_key_count(db, REGISTRY_TABLE).await, 0);
        assert_eq!(registry_db::trigger_count(db, REGISTRY_TABLE).await, 0);
    }

    async fn migration_is_idempotent_on_a_second_run(db: &RegistryTestDb) {
        use sea_orm_migration::{MigratorTrait, SchemaManager};
        use toolkit_db::migration_runner::run_migrations_for_testing;

        // Through the runner: the history makes the second run a no-op.
        let rerun = run_migrations_for_testing(&db.provider.db(), Migrator::migrations())
            .await
            .expect("a second run");
        assert_eq!(rerun.applied, 0, "nothing is applied twice");

        // Directly, twice more: `up` itself tolerates an existing table (`IF NOT EXISTS`).
        let manager = SchemaManager::new(&db.raw);
        for migration in Migrator::migrations() {
            migration.up(&manager).await.expect("up on an existing table");
            migration.up(&manager).await.expect("up again");
        }
        let store = store(db);
        store.upsert(&sample("still-works")).await.expect("the table is usable");
        assert_eq!(store.list().await.expect("list").len(), 1);
    }

    async fn migration_down_drops_the_table_and_up_recreates_it(db: &RegistryTestDb) {
        use sea_orm_migration::{MigratorTrait, SchemaManager};

        store(db).upsert(&sample("lost-on-down")).await.expect("upsert");
        let manager = SchemaManager::new(&db.raw);

        for migration in Migrator::migrations().iter().rev() {
            migration.down(&manager).await.expect("down");
        }
        assert!(!registry_db::table_exists(db, REGISTRY_TABLE).await, "down drops the table");

        for migration in Migrator::migrations() {
            migration.up(&manager).await.expect("up");
        }
        assert!(registry_db::table_exists(db, REGISTRY_TABLE).await);
        assert_eq!(store(db).list().await.expect("list"), Vec::new(), "re-seeding rebuilds the data");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_case_list_names_every_case_once() {
        let names: Vec<&str> = all().into_iter().map(|(name, _)| name).collect();
        let unique: std::collections::HashSet<&&str> = names.iter().collect();
        assert_eq!(names.len(), unique.len(), "duplicate case names: {names:?}");
        assert!(
            names.len() >= 30,
            "the shared case list shrank: {}",
            names.len()
        );
    }

    #[tokio::test]
    async fn the_case_list_runs_on_sqlite_through_the_function_pointers() {
        // The same path the Postgres lane takes, on SQLite.
        let (name, case) = all().into_iter().next().expect("a case");
        let db = registry_db::sqlite().await;
        real_bounded(case(&db)).await;
        assert!(!name.is_empty());
    }
}
