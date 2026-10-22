//! `SeaORM` entities of the gear's two tables. Neither is a resource: both are
//! internal bookkeeping with no tenant column (ADR 0005), so both are
//! `unrestricted` and are accessed with the unconstrained scope.

pub mod audit_event;
pub mod operation;
