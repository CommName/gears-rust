//! Shared sea-query → ClickHouse execute seam.
//!
//! SELECT statements go through [`ClickhouseSelect`] so every read is
//! `FINAL`-qualified. Lightweight `DELETE FROM` is built separately —
//! [`ClickhouseQueryBuilder`] rewrites `Query::delete()` to asynchronous
//! `ALTER TABLE … DELETE`, which is unsuitable for the request path
//! (see `catalog_store` module docs / DESIGN.md §3.6).

use sea_query::{
    Condition, Expr, ExprTrait, IntoCondition, Query, SelectStatement, Value, Values,
};
use sea_query_clickhouse::ClickhouseSelect;

use super::schema::{UsageRecords, UsageTypeCatalog};

/// Wrap a [`SelectStatement`] with ClickHouse `FINAL` and build parameterized SQL.
#[must_use]
pub fn build_select_final(stmt: SelectStatement) -> (String, Values) {
    ClickhouseSelect::new(stmt).r#final().build()
}

/// Build `SELECT count() FROM usage_type_catalog FINAL`.
#[must_use]
pub fn catalog_count_sql() -> (String, Values) {
    let stmt = Query::select()
        .expr(Expr::cust("count()"))
        .from(UsageTypeCatalog::Table)
        .to_owned();
    build_select_final(stmt)
}

/// Build a typed catalog row point-lookup by `gts_id`.
#[must_use]
pub fn catalog_get_by_gts_id(gts_id: &str) -> (String, Values) {
    let stmt = Query::select()
        .columns(super::schema::TYPE_SELECT_COLUMNS)
        .from(UsageTypeCatalog::Table)
        .and_where(Expr::col(UsageTypeCatalog::GtsId).eq(gts_id))
        .to_owned();
    build_select_final(stmt)
}

/// Build `SELECT gts_id FROM usage_type_catalog FINAL WHERE gts_id = ?`.
#[must_use]
pub fn catalog_exists_gts_id(gts_id: &str) -> (String, Values) {
    let stmt = Query::select()
        .column(UsageTypeCatalog::GtsId)
        .from(UsageTypeCatalog::Table)
        .and_where(Expr::col(UsageTypeCatalog::GtsId).eq(gts_id))
        .to_owned();
    build_select_final(stmt)
}

/// Lightweight synchronous delete — **not** `ALTER TABLE … DELETE`.
///
/// `ClickhouseQueryBuilder::prepare_delete_statement` would emit the async
/// mutation form; the plugin requires immediate visibility of the removal.
#[must_use]
pub fn lightweight_delete_usage_type(gts_id: &str) -> (String, Values) {
    (
        "DELETE FROM usage_type_catalog WHERE gts_id = ?".to_owned(),
        Values(vec![Value::String(Some(gts_id.to_owned()))]),
    )
}

/// Reference-count probe: bounded `count()` over `usage_records FINAL`.
#[must_use]
pub fn records_ref_count_probe(gts_id: &str, limit: i64) -> (String, Values) {
    // Nested subquery is awkward in sea-query; keep the proven static shape
    // and bind both parameters through Values for a single execute seam.
    (
        "SELECT count() FROM (SELECT 1 FROM usage_records FINAL WHERE gts_id = ? LIMIT ?) \
         AS sub_ref"
            .to_owned(),
        Values(vec![
            Value::String(Some(gts_id.to_owned())),
            Value::BigInt(Some(limit)),
        ]),
    )
}

/// Point-get a usage record by `id`.
#[must_use]
pub fn record_get_by_id(id: &str) -> (String, Values) {
    let stmt = Query::select()
        .columns(super::schema::RECORD_SELECT_COLUMNS)
        .from(UsageRecords::Table)
        .and_where(Expr::col(UsageRecords::Id).eq(id))
        .to_owned();
    build_select_final(stmt)
}

/// Apply sea-query [`Values`] to a client query, returning an owned query
/// handle so [`Values`] (which is not `Send`) never lives across an `.await`.
///
/// # Errors
///
/// Returns an error string when a [`Value`] variant cannot be bound.
pub fn prepared_query(
    client: &clickhouse::Client,
    sql: &str,
    values: &Values,
) -> Result<clickhouse::query::Query, String> {
    bind_values(client.query(sql), values)
}

/// Build a `FINAL` SELECT and bind parameters inside this sync helper so
/// [`SelectStatement`] / [`Values`] (not `Send`) never appear as async locals.
///
/// # Errors
///
/// Returns an error string when a [`Value`] variant cannot be bound.
pub fn prepared_select(
    client: &clickhouse::Client,
    stmt: SelectStatement,
) -> Result<clickhouse::query::Query, String> {
    let (sql, values) = build_select_final(stmt);
    prepared_query(client, &sql, &values)
}

/// Bind a prebuilt `(sql, Values)` pair inside this sync helper so [`Values`]
/// never appears as an async local.
///
/// # Errors
///
/// Returns an error string when a [`Value`] variant cannot be bound.
pub fn prepared_sql(
    client: &clickhouse::Client,
    built: (String, Values),
) -> Result<clickhouse::query::Query, String> {
    let (sql, values) = built;
    prepared_query(client, &sql, &values)
}

/// Apply sea-query [`Values`] to a `clickhouse::query::Query` in order.
///
/// # Errors
///
/// Returns an error string when a [`Value`] variant cannot be bound.
pub fn bind_values(
    mut q: clickhouse::query::Query,
    values: &Values,
) -> Result<clickhouse::query::Query, String> {
    for v in values.iter() {
        q = bind_sea_value(q, v)?;
    }
    Ok(q)
}

/// Bind one [`sea_query::Value`] onto a ClickHouse query.
///
/// # Errors
///
/// Returns an error when the variant is unsupported on the request path.
pub fn bind_sea_value(
    q: clickhouse::query::Query,
    v: &Value,
) -> Result<clickhouse::query::Query, String> {
    match v {
        Value::Bool(Some(b)) => Ok(q.bind(*b)),
        Value::TinyInt(Some(n)) => Ok(q.bind(i64::from(*n))),
        Value::SmallInt(Some(n)) => Ok(q.bind(i64::from(*n))),
        Value::Int(Some(n)) => Ok(q.bind(i64::from(*n))),
        Value::BigInt(Some(n)) => Ok(q.bind(*n)),
        Value::TinyUnsigned(Some(n)) => Ok(q.bind(u64::from(*n))),
        Value::SmallUnsigned(Some(n)) => Ok(q.bind(u64::from(*n))),
        Value::Unsigned(Some(n)) => Ok(q.bind(u64::from(*n))),
        Value::BigUnsigned(Some(n)) => Ok(q.bind(*n)),
        Value::Float(Some(n)) => Ok(q.bind(f64::from(*n))),
        Value::Double(Some(n)) => Ok(q.bind(*n)),
        Value::String(Some(s)) => Ok(q.bind(s.as_str())),
        Value::Char(Some(c)) => Ok(q.bind(c.to_string())),
        Value::Bytes(Some(b)) => Ok(q.bind(b.as_slice())),
        Value::ChronoDateTimeUtc(Some(dt)) => Ok(q.bind(dt.timestamp_micros())),
        Value::ChronoDateTime(Some(dt)) => Ok(q.bind(dt.and_utc().timestamp_micros())),
        Value::TimeDateTimeWithTimeZone(Some(dt)) => {
            let nanos = dt.unix_timestamp_nanos();
            let micros = nanos.div_euclid(1_000);
            #[allow(
                clippy::cast_possible_truncation,
                reason = "practical timestamps fit in i64"
            )]
            Ok(q.bind(micros as i64))
        }
        Value::TimeDateTime(Some(dt)) => {
            let odt = dt.assume_utc();
            let nanos = odt.unix_timestamp_nanos();
            let micros = nanos.div_euclid(1_000);
            #[allow(
                clippy::cast_possible_truncation,
                reason = "practical timestamps fit in i64"
            )]
            Ok(q.bind(micros as i64))
        }
        Value::Uuid(Some(u)) => Ok(q.bind(u.to_string())),
        Value::Decimal(Some(d)) => Ok(q.bind(d.to_string())),
        other => Err(format!(
            "unsupported sea_query::Value for ClickHouse bind: {other:?}"
        )),
    }
}

/// Attach a [`Condition`] to a select (no-op when the condition is empty).
pub fn apply_condition(stmt: &mut SelectStatement, cond: Condition) {
    if !cond.is_empty() {
        stmt.cond_where(cond);
    }
}

/// Combine optional filter conditions with AND.
#[must_use]
pub fn and_conditions(parts: Vec<Condition>) -> Condition {
    let mut out = Condition::all();
    for p in parts {
        if !p.is_empty() {
            out = out.add(p);
        }
    }
    out
}

/// Turn an into-condition into a [`Condition`] wrapper.
#[must_use]
pub fn into_condition(c: impl IntoCondition) -> Condition {
    c.into_condition()
}
