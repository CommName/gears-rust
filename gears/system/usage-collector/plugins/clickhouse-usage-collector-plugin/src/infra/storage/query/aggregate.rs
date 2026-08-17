//! Aggregation SQL builder for `ClickHouse` — inject-safe SELECT-expression
//! builders for the pushed-down `aggregate` query (sea-query [`SimpleExpr`]).

use sea_query::SimpleExpr;
use usage_collector_sdk::{AggregationDimension, AggregationOp, MAX_AGGREGATION_BUCKETS};

use super::expr::{agg_expr, metadata_get, to_string_tenant_id};

/// SQL aggregate expression for an [`AggregationOp`].
#[must_use]
pub fn agg_select_expr(op: AggregationOp) -> SimpleExpr {
    agg_expr(op)
}

/// `corrects_id`-partition `WHERE` clause for an [`AggregationOp`], or `None`.
#[must_use]
pub fn corrects_id_partition_clause(op: AggregationOp) -> Option<&'static str> {
    match op {
        AggregationOp::Sum => Some("corrects_id IS NULL"),
        AggregationOp::Count | AggregationOp::Min | AggregationOp::Max | AggregationOp::Avg => {
            Some("corrects_id IS NULL")
        }
    }
}

/// SQL `String`-returning expression for a group [`AggregationDimension`].
#[must_use]
pub fn dimension_select_expr(dim: &AggregationDimension) -> SimpleExpr {
    match dim {
        AggregationDimension::TenantId => to_string_tenant_id(),
        AggregationDimension::ResourceId => sea_query::Expr::cust("resource_id"),
        AggregationDimension::ResourceType => sea_query::Expr::cust("resource_type"),
        AggregationDimension::SubjectId => sea_query::Expr::cust("subject_id"),
        AggregationDimension::SubjectType => sea_query::Expr::cust("subject_type"),
        AggregationDimension::Metadata(key) => metadata_get(key.as_str()),
    }
}

/// `LIMIT` clause bounding the aggregate's distinct-group cardinality.
#[must_use]
pub fn aggregate_limit_clause(dim_count: usize) -> String {
    if dim_count == 0 {
        String::new()
    } else {
        format!(" LIMIT {}", MAX_AGGREGATION_BUCKETS + 1)
    }
}

/// Numeric limit for sea-query `.limit(...)`, or `None` when uncapped.
#[must_use]
pub fn aggregate_limit(dim_count: usize) -> Option<u64> {
    if dim_count == 0 {
        None
    } else {
        Some(u64::try_from(MAX_AGGREGATION_BUCKETS + 1).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "aggregate_tests.rs"]
mod aggregate_tests;
