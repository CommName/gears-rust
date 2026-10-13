//! Unit tests for the `application_registry` entity (DESIGN §3.7 "Table:
//! `application_registry`" and "Additional info", ADR 0007, `docs/toolkit_unified_system/
//! 06_authn_authz_secure_orm.md` "Unrestricted entities").
//!
//! Planned API in `crate::infra::storage::entity` (new file, P4). The module ends with:
//!
//! ```text
//! #[cfg(test)]
//! #[cfg_attr(coverage_nightly, coverage(off))]
//! #[path = "entity_tests.rs"]
//! mod entity_tests;
//! ```
//!
//! ```text
//! #[derive(Clone, Debug, PartialEq, DeriveEntityModel, Scopable)]
//! #[sea_orm(table_name = "application_registry")]
//! #[secure(unrestricted)]
//! pub struct Model {
//!     #[sea_orm(primary_key, auto_increment = false)]
//!     pub application_id: String,   // bounded text, REGISTRY_TEXT_MAX_BYTES, binary collation
//!     pub version: String,          // bounded text, REGISTRY_TEXT_MAX_BYTES
//!     pub last_result: String,      // `LastResult::as_token()`
//!     pub last_seeded_at: time::OffsetDateTime,   // timestamp with time zone
//!     pub audit_id: String,
//! }
//! // + `Relation` (empty), `ActiveModelBehavior`, so `Entity`, `Column`, `PrimaryKey` and
//! // `ActiveModel` exist at `crate::infra::storage::entity::*`.
//! ```
//!
//! No `owner_tenant_id`, `tenant_id`, resource, owner or type column and no `pep_prop`.
//!
//! - `entity_is_declared_unrestricted`: `IS_UNRESTRICTED` (ADR 0007).
//! - `entity_has_no_scoping_dimension`: every dimension column and property resolves to
//!   nothing (ADR 0007).
//! - `entity_maps_the_design_table`: table name, five columns, one primary-key column (§3.7).
//! - `entity_has_no_tenant_like_column`: no column name mentions tenant or owner (ADR 0007).
//! - `model_field_types_are_pinned`: compile-time pin of the stored types (§3.7).
//! - `every_last_result_token_fits_the_entity_text_column`: tokens are plain ASCII text (§3.7).
//! - `database_cases_run_on_the_entity`: the DB-backed security and constraint cases are the
//!   shared cases `registry_cases::sqlite_lane::{secure_insert_*, a_tenant_scope_*,
//!   database_*}` (not repeated here).

use sea_orm::{EntityName, EntityTrait, Iterable, PrimaryKeyToColumn};
use time::OffsetDateTime;
use toolkit_db::secure::ScopableEntity;

use super::{ActiveModel, Column, Entity, Model, PrimaryKey};
use crate::domain::registry::LastResult;

#[test]
fn entity_is_declared_unrestricted() {
    // A runtime assertion: a failing `const` assertion would stop the whole crate compiling.
    let unrestricted = <Entity as ScopableEntity>::IS_UNRESTRICTED;
    assert!(
        unrestricted,
        "the registry is a platform-wide table: #[secure(unrestricted)] (ADR 0007)"
    );
}

#[test]
fn entity_has_no_scoping_dimension() {
    assert!(<Entity as ScopableEntity>::tenant_col().is_none());
    assert!(<Entity as ScopableEntity>::resource_col().is_none());
    assert!(<Entity as ScopableEntity>::owner_col().is_none());
    assert!(<Entity as ScopableEntity>::type_col().is_none());
    assert!(<Entity as ScopableEntity>::scope_columns().is_empty());
    for property in [
        "owner_tenant_id",
        "id",
        "owner_id",
        "application_id",
        "tenant_id",
        "",
    ] {
        assert!(
            <Entity as ScopableEntity>::resolve_property(property).is_none(),
            "no PEP property may resolve on the registry entity: {property:?}"
        );
    }
}

#[test]
fn entity_maps_the_design_table() {
    assert_eq!(Entity.table_name(), "application_registry");

    let columns: Vec<String> = Column::iter()
        .map(|c| sea_orm::sea_query::Iden::to_string(&c))
        .collect();
    assert_eq!(
        columns,
        [
            "application_id",
            "version",
            "last_result",
            "last_seeded_at",
            "audit_id"
        ]
    );

    let keys: Vec<String> = PrimaryKey::iter()
        .map(|k| sea_orm::sea_query::Iden::to_string(&k.into_column()))
        .collect();
    assert_eq!(
        keys,
        ["application_id"],
        "the primary key is application_id alone"
    );
}

#[test]
fn entity_has_no_tenant_like_column() {
    for column in Column::iter() {
        let name = sea_orm::sea_query::Iden::to_string(&column);
        assert!(
            !name.contains("tenant") && !name.contains("owner"),
            "column {name} would reintroduce a scoping value (ADR 0007)"
        );
    }
}

#[test]
fn model_field_types_are_pinned() {
    let model = Model {
        application_id: String::new(),
        version: String::new(),
        last_result: String::new(),
        last_seeded_at: OffsetDateTime::UNIX_EPOCH,
        audit_id: String::new(),
    };
    let Model {
        application_id,
        version,
        last_result,
        last_seeded_at,
        audit_id,
    } = model.clone();
    let _: (String, String, String, OffsetDateTime, String) = (
        application_id,
        version,
        last_result,
        last_seeded_at,
        audit_id,
    );
    assert_eq!(model, model.clone());

    // The active model has exactly the same five columns and no other.
    let active = ActiveModel::from(model);
    assert_eq!(<Entity as EntityTrait>::Column::iter().count(), 5);
    drop(active);
}

#[test]
fn every_last_result_token_fits_the_entity_text_column() {
    for result in LastResult::ALL {
        let token = result.as_token();
        assert!(token.is_ascii() && token.len() <= 32, "{token}");
    }
}
