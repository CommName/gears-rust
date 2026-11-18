//! `SeaORM` entities of the gear's three tables. None is a resource: all are
//! internal bookkeeping with no tenant column (ADR 0004), so all are
//! `unrestricted` and are accessed with the unconstrained scope.

pub mod application;
pub mod history_event;
pub mod operation;
