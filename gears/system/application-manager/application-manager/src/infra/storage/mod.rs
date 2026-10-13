//! Registry persistence of the Application Manager gear (DESIGN §3.7, §3.2 "Registry
//! Repository", ADR 0005, ADR 0007).
//!
//! Layout:
//!
//! - `entity`: the `application_registry` `SeaORM` entity, declared
//!   `#[secure(unrestricted)]`, with no tenant, owner, resource or type column.
//! - `migrations`: `Migrator` and the one dated initial migration, returned by
//!   `DatabaseCapability::migrations()` on the gear.
//! - `repo`: `RegistryRepo`, a stateless, `DBRunner`-generic repository. Every operation
//!   takes the `AccessScope` of the permit (always the allow-all scope, ADR 0007).
//! - `store`: `SeaRegistryStore`, the `RegistryStore` port adapter. It supplies the
//!   allow-all scope, bounds every operation with `registry_operation_timeout` and maps
//!   failures to `RegistryError`.
//!
//! Every sibling `*_tests.rs` of `entity`, `migrations`, `repo` and `store` is wired from
//! its own module file (`#[cfg(test)] #[cfg_attr(coverage_nightly, coverage(off))]
//! #[path = "<module>_tests.rs"] mod <module>_tests;`). The three files below are wired here.

pub mod entity;
pub mod migrations;
pub mod repo;
pub mod store;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "mod_tests.rs"]
mod mod_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "postgres_tests.rs"]
mod postgres_tests;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "mysql_tests.rs"]
mod mysql_tests;
