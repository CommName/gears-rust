//! Injection-safe filter translation: a validated `FilterNode<F>` becomes a
//! sea-query [`Condition`] (bound values, allowlisted identifiers).
//!
//! Identifiers resolve through [`schema::record_column_iden`] /
//! [`schema::usage_type_column_iden`]; values go through [`odata_value_to_bind`]
//! then [`sql_bind_to_expr`] (wrapping `DateTime64` micros in
//! `fromUnixTimestamp64Micro(?)`).

use sea_query::{Condition, Expr, ExprTrait, SimpleExpr};
use toolkit_odata::filter::{FilterField, FilterNode, FilterOp};

pub use super::bind::{SqlBind, bind_one, odata_value_to_bind, sql_bind_to_expr};
pub use toolkit_odata::filter::ODataValue;

use super::schema::{record_column_iden, usage_type_column_iden};

/// Closed allowlist mapping a `usage_records` filter-field name to its column.
///
/// Kept as `&'static str` for keyset helpers and tests; the translate path
/// resolves through [`record_column_iden`].
#[must_use]
pub fn record_column(field_name: &str) -> Option<&'static str> {
    match field_name {
        "id" => Some("id"),
        "created_at" => Some("created_at"),
        "tenant_id" => Some("tenant_id"),
        "resource_id" => Some("resource_id"),
        "resource_type" => Some("resource_type"),
        "subject_id" => Some("subject_id"),
        "subject_type" => Some("subject_type"),
        "corrects_id" => Some("corrects_id"),
        "status" => Some("status"),
        _ => None,
    }
}

/// Closed allowlist mapping a `usage_type_catalog` filter-field name to its column.
#[must_use]
pub fn usage_type_column(field_name: &str) -> Option<&'static str> {
    match field_name {
        "gts_id" => Some("gts_id"),
        "kind" => Some("kind"),
        _ => None,
    }
}

/// Bind accumulator retained for keyset / metadata / batch-dedup paths that
/// still assemble custom `?` fragments alongside sea-query [`Condition`]s.
pub struct SqlCtx {
    /// Accumulated binds in placeholder order for custom SQL fragments.
    pub(crate) binds: Vec<SqlBind>,
}

impl SqlCtx {
    /// Create an empty context.
    #[must_use]
    pub fn new() -> Self {
        Self { binds: Vec::new() }
    }

    /// Append a bind for a custom SQL fragment.
    pub(crate) fn push(&mut self, b: SqlBind) {
        self.binds.push(b);
    }
}

impl Default for SqlCtx {
    fn default() -> Self {
        Self::new()
    }
}

/// Map a comparison [`FilterOp`] onto a binary [`SimpleExpr`].
fn cmp_expr(left: SimpleExpr, op: FilterOp, right: SimpleExpr) -> Result<SimpleExpr, String> {
    match op {
        FilterOp::Eq => Ok(left.eq(right)),
        FilterOp::Ne => Ok(left.ne(right)),
        FilterOp::Gt => Ok(left.gt(right)),
        FilterOp::Ge => Ok(left.gte(right)),
        FilterOp::Lt => Ok(left.lt(right)),
        FilterOp::Le => Ok(left.lte(right)),
        other => Err(format!("unsupported operator: {other:?}")),
    }
}

/// Translate a `usage_records` filter node into a [`Condition`].
///
/// # Errors
///
/// Returns an error string when a field is not on the allowlist, an operator is
/// unsupported, a composite carries a non-`And`/`Or` operator, or a value
/// cannot be converted to a bind.
pub fn translate_record_filter<F: FilterField>(node: &FilterNode<F>) -> Result<Condition, String> {
    translate_filter(node, record_column_iden)
}

/// Translate a `usage_type_catalog` filter node.
///
/// # Errors
///
/// Same conditions as [`translate_record_filter`].
pub fn translate_usage_type_filter<F: FilterField>(
    node: &FilterNode<F>,
) -> Result<Condition, String> {
    translate_filter(node, usage_type_column_iden)
}

/// Shared recursive walker parameterised over the column allowlist.
pub(crate) fn translate_filter<F: FilterField, C>(
    node: &FilterNode<F>,
    col: fn(&str) -> Option<C>,
) -> Result<Condition, String>
where
    C: sea_query::IntoColumnRef + Copy,
{
    match node {
        FilterNode::Binary { field, op, value } => {
            let column = col(field.name())
                .ok_or_else(|| format!("field not allowlisted: {}", field.name()))?;
            let right = sql_bind_to_expr(odata_value_to_bind(value)?);
            let expr = cmp_expr(Expr::col(column).into(), *op, right)?;
            Ok(Condition::all().add(expr))
        }
        FilterNode::InList { field, values } => {
            let column = col(field.name())
                .ok_or_else(|| format!("field not allowlisted: {}", field.name()))?;
            if values.is_empty() {
                return Err("IN list must not be empty".to_owned());
            }
            // OR of equalities so `fromUnixTimestamp64Micro(?)` RHS exprs work
            // the same as plain values (sea-query `is_in` is value-list only).
            let mut any = Condition::any();
            for v in values {
                let right = sql_bind_to_expr(odata_value_to_bind(v)?);
                any = any.add(Expr::col(column).eq(right));
            }
            Ok(Condition::all().add(any))
        }
        FilterNode::Composite { op, children } => {
            let mut cond = match op {
                FilterOp::And => Condition::all(),
                FilterOp::Or => Condition::any(),
                other => return Err(format!("invalid composite operator: {other:?}")),
            };
            for child in children {
                cond = cond.add(translate_filter(child, col)?);
            }
            Ok(cond)
        }
        FilterNode::Not(inner) => Ok(translate_filter(inner, col)?.not()),
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "translate_tests.rs"]
mod translate_tests;
