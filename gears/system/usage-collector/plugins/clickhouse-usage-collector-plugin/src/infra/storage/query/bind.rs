//! Value binding: convert a `toolkit_odata` AST value into a storage-typed
//! bind, and apply that bind to a `ClickHouse` [`Query`] or sea-query [`Expr`].
//!
//! `ClickHouse` uses positional `?` placeholders. [`SqlBind`] variants cover
//! the column types in `usage_records` / `usage_type_catalog`.
//!
//! [`Query`]: clickhouse::query::Query

use rust_decimal::Decimal;
use sea_query::{Expr, SimpleExpr};
use uuid::Uuid;

use toolkit_odata::filter::ODataValue;

use super::expr::from_unix_timestamp64_micro;

/// A storage-typed value ready to be bound to a `ClickHouse` `?` placeholder.
#[derive(Debug, Clone)]
pub enum SqlBind {
    /// `UUID` column bind.
    Uuid(Uuid),
    /// `String` column bind.
    Str(String),
    /// `Decimal128(9)` column bind.
    Decimal(Decimal),
    /// `DateTime64(6)` column bind (epoch-microseconds as `i64`).
    DateTime64Micros(i64),
    /// `Boolean` column bind.
    Bool(bool),
    /// Signed 64-bit integer bind.
    I64(i64),
    /// Unsigned 64-bit integer bind.
    U64(u64),
}

impl SqlBind {
    /// Render the SQL placeholder required for this bind's storage type.
    ///
    /// Used by custom fragment builders (keyset / batch dedup / metadata).
    /// Prefer [`sql_bind_to_expr`] when composing sea-query ASTs.
    #[must_use]
    pub fn placeholder(&self) -> &'static str {
        match self {
            Self::DateTime64Micros(_) => "fromUnixTimestamp64Micro(?)",
            _ => "?",
        }
    }
}

/// Convert an [`SqlBind`] into a sea-query RHS expression.
///
/// `DateTime64Micros` becomes `fromUnixTimestamp64Micro(?)` so tuple/`IN`
/// contexts do not hit `DECIMAL_OVERFLOW`.
#[must_use]
pub fn sql_bind_to_expr(bind: SqlBind) -> SimpleExpr {
    match bind {
        SqlBind::Uuid(u) => Expr::val(u.to_string()),
        SqlBind::Str(s) => Expr::val(s),
        SqlBind::Decimal(d) => Expr::val(d.to_string()),
        SqlBind::DateTime64Micros(n) => from_unix_timestamp64_micro(n),
        SqlBind::Bool(b) => Expr::val(b),
        SqlBind::I64(n) => Expr::val(n),
        SqlBind::U64(n) => Expr::val(n),
    }
}

/// Convert an `OData` AST value into a storage-typed [`SqlBind`].
///
/// # Errors
///
/// Returns an error string on `Null` / `Date` / `Time` values or when a
/// numeric value is out of the `rust_decimal::Decimal` range.
pub fn odata_value_to_bind(v: &ODataValue) -> Result<SqlBind, String> {
    match v {
        ODataValue::Uuid(u) => Ok(SqlBind::Uuid(*u)),
        ODataValue::String(s) => Ok(SqlBind::Str(s.clone())),
        ODataValue::Bool(b) => Ok(SqlBind::Bool(*b)),
        ODataValue::Number(n) => n
            .to_string()
            .parse::<Decimal>()
            .map(SqlBind::Decimal)
            .map_err(|e| format!("numeric out of range: {e}")),
        ODataValue::DateTime(dt) => Ok(SqlBind::DateTime64Micros(dt.timestamp_micros())),
        ODataValue::Null => Err("null filter value unsupported".to_owned()),
        ODataValue::Date(_) | ODataValue::Time(_) => {
            Err("date/time-only filter values unsupported".to_owned())
        }
    }
}

/// Apply a single [`SqlBind`] to a `ClickHouse` [`Query`], returning the query
/// with the bind appended.
///
/// [`Query`]: clickhouse::query::Query
pub fn bind_one(q: clickhouse::query::Query, v: &SqlBind) -> clickhouse::query::Query {
    match v {
        SqlBind::Uuid(u) => q.bind(u.to_string()),
        SqlBind::Str(s) => q.bind(s.as_str()),
        SqlBind::Decimal(d) => q.bind(d.to_string()),
        SqlBind::DateTime64Micros(n) | SqlBind::I64(n) => q.bind(*n),
        SqlBind::Bool(b) => q.bind(*b),
        SqlBind::U64(n) => q.bind(*n),
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "bind_tests.rs"]
mod bind_tests;
