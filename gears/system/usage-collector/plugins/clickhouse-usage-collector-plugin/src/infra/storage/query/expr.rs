//! ClickHouse-specific [`Expr`] helpers used by the sea-query builders.
//!
//! These wrap dialect functions that have no first-class sea-query AST node
//! (`fromUnixTimestamp64Micro`, map subscript, `toString`, aggregates).

use sea_query::{Expr, SimpleExpr};
use usage_collector_sdk::AggregationOp;

/// Bind an epoch-microsecond `i64` as `DateTime64(6)` via ClickHouse's
/// conversion function. Bare micros in tuple/`IN` contexts can raise
/// `DECIMAL_OVERFLOW` without this wrapper.
#[must_use]
pub fn from_unix_timestamp64_micro(micros: i64) -> SimpleExpr {
    Expr::cust_with_values("fromUnixTimestamp64Micro(?)", [micros])
}

/// Map access `arrayElement(metadata, ?)` with a bound string key.
///
/// Prefer this over `metadata[?]` in sea-query: the tokenizer treats `[…]` as a
/// quoted span, so `?` inside brackets is not a bind placeholder.
#[must_use]
pub fn metadata_get(key: &str) -> SimpleExpr {
    Expr::cust_with_values("arrayElement(metadata, ?)", [key])
}

/// `toString(tenant_id)` for UUID → String grouping keys.
#[must_use]
pub fn to_string_tenant_id() -> SimpleExpr {
    Expr::cust("toString(tenant_id)")
}

/// Aggregate SELECT expression for an [`AggregationOp`].
#[must_use]
pub fn agg_expr(op: AggregationOp) -> SimpleExpr {
    match op {
        AggregationOp::Sum => Expr::cust("SUM(value)"),
        AggregationOp::Count => Expr::cust("COUNT(*)"),
        AggregationOp::Min => Expr::cust("MIN(value)"),
        AggregationOp::Max => Expr::cust("MAX(value)"),
        AggregationOp::Avg => Expr::cust("ROUND(AVG(value), 6)"),
    }
}
