use usage_collector_sdk::{AggregationOp, MAX_AGGREGATION_BUCKETS};

use super::{aggregate_limit_clause, corrects_id_partition_clause};

#[test]
fn count_has_corrects_id_is_null_clause() {
    assert_eq!(
        corrects_id_partition_clause(AggregationOp::Count),
        Some("corrects_id IS NULL")
    );
}

#[test]
fn min_max_avg_have_corrects_id_partition() {
    for op in [AggregationOp::Min, AggregationOp::Max, AggregationOp::Avg] {
        assert_eq!(
            corrects_id_partition_clause(op),
            Some("corrects_id IS NULL"),
            "op = {op:?}"
        );
    }
}

#[test]
fn dim_count_zero_yields_empty_limit() {
    assert_eq!(aggregate_limit_clause(0), "");
}

#[test]
fn dim_count_one_yields_max_plus_one_limit() {
    let clause = aggregate_limit_clause(1);
    assert_eq!(clause, format!(" LIMIT {}", MAX_AGGREGATION_BUCKETS + 1));
}

#[test]
fn dim_count_three_yields_max_plus_one_limit() {
    let clause = aggregate_limit_clause(3);
    assert!(clause.contains(&(MAX_AGGREGATION_BUCKETS + 1).to_string()));
}

