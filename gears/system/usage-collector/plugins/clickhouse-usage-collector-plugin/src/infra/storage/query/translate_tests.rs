use sea_query::{Expr, Query};
use sea_query_clickhouse::ClickhouseQueryBuilder;
use toolkit_odata::filter::{FilterField, FilterNode, FilterOp, ODataValue};
use usage_collector_sdk::UsageRecordFilterField;

use super::{
    SqlCtx, record_column, translate_filter, translate_record_filter, translate_usage_type_filter,
    usage_type_column,
};
use crate::infra::storage::query::schema::UsageRecords;

fn record_node_status_eq_active() -> FilterNode<UsageRecordFilterField> {
    let field = <UsageRecordFilterField as FilterField>::from_name("status").unwrap();
    FilterNode::Binary {
        field,
        op: FilterOp::Eq,
        value: ODataValue::String("active".to_owned()),
    }
}

fn record_node_status_op(op: FilterOp) -> FilterNode<UsageRecordFilterField> {
    let field = <UsageRecordFilterField as FilterField>::from_name("status").unwrap();
    FilterNode::Binary {
        field,
        op,
        value: ODataValue::String("active".to_owned()),
    }
}

/// Render a condition's WHERE clause (ClickHouse builder) and bind count.
fn render_where(cond: sea_query::Condition) -> (String, usize) {
    let (sql, values) = Query::select()
        .expr(Expr::cust("1"))
        .from(UsageRecords::Table)
        .cond_where(cond)
        .build(ClickhouseQueryBuilder);
    let where_part = sql
        .split(" WHERE ")
        .nth(1)
        .unwrap_or("")
        .to_owned();
    (where_part, values.0.len())
}

#[test]
fn status_eq_active_yields_parameterised_fragment() {
    let cond = translate_record_filter(&record_node_status_eq_active()).unwrap();
    let (frag, n) = render_where(cond);
    assert!(frag.contains("`status` = ?"), "got: {frag}");
    assert_eq!(n, 1);
}

#[test]
fn in_list_yields_correct_placeholders() {
    let field = <UsageRecordFilterField as FilterField>::from_name("status").unwrap();
    let node: FilterNode<UsageRecordFilterField> = FilterNode::InList {
        field,
        values: vec![
            ODataValue::String("active".to_owned()),
            ODataValue::String("inactive".to_owned()),
        ],
    };

    let cond = translate_record_filter(&node).unwrap();
    let (frag, n) = render_where(cond);
    // OR-of-eq form (supports datetime wrappers uniformly).
    assert!(frag.contains("`status` = ?"), "got: {frag}");
    assert_eq!(n, 2);
}

#[test]
fn unknown_field_is_rejected() {
    use usage_collector_sdk::UsageTypeFilterField;
    let field = <UsageTypeFilterField as FilterField>::from_name("gts_id").unwrap();
    let node = FilterNode::Binary {
        field,
        op: FilterOp::Eq,
        value: ODataValue::String("some-gts-id".to_owned()),
    };
    let result = translate_filter(&node, crate::infra::storage::query::schema::record_column_iden);
    assert!(result.is_err());
}

#[test]
fn usage_type_column_accepts_gts_id_and_kind() {
    assert_eq!(usage_type_column("gts_id"), Some("gts_id"));
    assert_eq!(usage_type_column("kind"), Some("kind"));
    assert_eq!(usage_type_column("unknown"), None);
    assert_eq!(record_column("status"), Some("status"));
}

#[test]
fn composite_and_yields_parenthesised_and() {
    let a = record_node_status_eq_active();
    let b = record_node_status_eq_active();
    let node = FilterNode::Composite {
        op: FilterOp::And,
        children: vec![a, b],
    };
    let cond = translate_record_filter(&node).unwrap();
    let (frag, n) = render_where(cond);
    assert!(frag.contains("AND"), "got: {frag}");
    assert_eq!(n, 2);
}

#[test]
fn every_comparison_operator_maps_to_its_sql_spelling() {
    for (op, sql) in [
        (FilterOp::Eq, "="),
        (FilterOp::Ne, "<>"),
        (FilterOp::Gt, ">"),
        (FilterOp::Ge, ">="),
        (FilterOp::Lt, "<"),
        (FilterOp::Le, "<="),
    ] {
        let cond = translate_record_filter(&record_node_status_op(op)).unwrap();
        let (frag, n) = render_where(cond);
        assert!(
            frag.contains(&format!("`status` {sql} ?")),
            "op = {op:?}, got: {frag}"
        );
        assert_eq!(n, 1, "op = {op:?} binds its value");
    }
}

#[test]
fn non_comparison_operators_are_rejected_by_binary_translation() {
    for op in [
        FilterOp::In,
        FilterOp::Contains,
        FilterOp::StartsWith,
        FilterOp::EndsWith,
        FilterOp::And,
        FilterOp::Or,
    ] {
        let err = translate_record_filter(&record_node_status_op(op))
            .expect_err("non-comparison operator must not translate as a binary comparison");
        assert!(
            err.contains("unsupported operator"),
            "op = {op:?} must report an unsupported operator, got: {err}"
        );
    }
}

#[test]
fn usage_type_filter_translates_catalog_fields() {
    use usage_collector_sdk::UsageTypeFilterField;

    let field = <UsageTypeFilterField as FilterField>::from_name("kind").unwrap();
    let node = FilterNode::Binary {
        field,
        op: FilterOp::Eq,
        value: ODataValue::String("counter".to_owned()),
    };

    let cond = translate_usage_type_filter(&node).unwrap();
    let (sql, values) = Query::select()
        .expr(Expr::cust("1"))
        .from(crate::infra::storage::query::schema::UsageTypeCatalog::Table)
        .cond_where(cond)
        .build(ClickhouseQueryBuilder);
    assert!(sql.contains("`kind` = ?"), "got: {sql}");
    assert_eq!(values.0.len(), 1);
}

#[test]
fn usage_type_filter_rejects_record_only_field() {
    let field = <UsageRecordFilterField as FilterField>::from_name("tenant_id").unwrap();
    let node = FilterNode::Binary {
        field,
        op: FilterOp::Eq,
        value: ODataValue::String("whatever".to_owned()),
    };

    let err = translate_usage_type_filter(&node).expect_err("tenant_id is not a catalog column");
    assert!(
        err.contains("field not allowlisted"),
        "expected an allowlist rejection, got: {err}"
    );
}

#[test]
fn empty_in_list_is_rejected() {
    let field = <UsageRecordFilterField as FilterField>::from_name("status").unwrap();
    let node: FilterNode<UsageRecordFilterField> = FilterNode::InList {
        field,
        values: Vec::new(),
    };

    let err = translate_record_filter(&node).expect_err("empty IN list must be rejected");
    assert!(
        err.contains("IN list must not be empty"),
        "unexpected error: {err}"
    );
}

#[test]
fn composite_or_joins_children_with_or() {
    let node = FilterNode::Composite {
        op: FilterOp::Or,
        children: vec![
            record_node_status_eq_active(),
            record_node_status_op(FilterOp::Ne),
        ],
    };
    let cond = translate_record_filter(&node).unwrap();
    let (frag, n) = render_where(cond);
    assert!(frag.contains("OR"), "got: {frag}");
    assert_eq!(n, 2);
}

#[test]
fn composite_with_comparison_operator_is_rejected() {
    let node = FilterNode::Composite {
        op: FilterOp::Eq,
        children: vec![record_node_status_eq_active()],
    };
    let err = translate_record_filter(&node)
        .expect_err("a comparison operator cannot join composite children");
    assert!(
        err.contains("invalid composite operator"),
        "unexpected error: {err}"
    );
}

#[test]
fn not_wraps_inner_fragment() {
    let node = FilterNode::Not(Box::new(record_node_status_eq_active()));
    let cond = translate_record_filter(&node).unwrap();
    let (frag, n) = render_where(cond);
    assert!(
        frag.contains("NOT") || frag.to_uppercase().contains("NOT"),
        "got: {frag}"
    );
    assert_eq!(n, 1, "the negated child still binds its value");
}

#[test]
fn default_ctx_starts_empty() {
    let ctx = SqlCtx::default();
    assert!(ctx.binds.is_empty());
}
