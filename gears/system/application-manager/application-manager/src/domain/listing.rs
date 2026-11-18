//! Paging of the tenant-plane reads (DESIGN 3.3): the history and the
//! application list. Each is ordered by its key and takes an optional
//! `$filter` on one id field, `eq` or `in` only, `limit` and `cursor`. No
//! other option is served: `$orderby` and `$select` are refused.

use toolkit_macros::domain_model;
use toolkit_odata::filter::{
    FieldKind, FilterField, FilterNode, FilterOp, ODataValue, convert_expr_to_filter_node,
};
use toolkit_odata::{CursorV1, ODataOrderBy, ODataQuery, OrderKey, SortDir};

use crate::domain::error::DomainError;

/// The operators the id filters serve.
const ID_OPS: &[FilterOp] = &[FilterOp::Eq, FilterOp::In];

/// The only `$filter` field of the history read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HistoryFilterField {
    /// The operation id, matched against the correlation id.
    OperationId,
}

impl FilterField for HistoryFilterField {
    const FIELDS: &'static [Self] = &[Self::OperationId];

    fn name(&self) -> &'static str {
        "operation_id"
    }

    fn kind(&self) -> FieldKind {
        FieldKind::Uuid
    }

    fn published_ops(&self) -> Option<&'static [FilterOp]> {
        Some(ID_OPS)
    }
}

/// The only `$filter` field of the application list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ApplicationFilterField {
    /// The exact application id.
    ApplicationId,
}

impl FilterField for ApplicationFilterField {
    const FIELDS: &'static [Self] = &[Self::ApplicationId];

    fn name(&self) -> &'static str {
        "application_id"
    }

    fn kind(&self) -> FieldKind {
        FieldKind::String
    }

    fn published_ops(&self) -> Option<&'static [FilterOp]> {
        Some(ID_OPS)
    }
}

/// One page request after the `OData` options are checked.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListQuery<K> {
    /// The ids the `$filter` names; `None` without a filter.
    pub ids: Option<Vec<K>>,
    /// The key after which the page starts, from the cursor.
    pub after: Option<K>,
    /// The page size: the request's `limit`, at most the page maximum.
    pub limit: u64,
    /// The `$filter` fingerprint the next cursor carries.
    filter_hash: Option<String>,
    /// The key field the list is ordered by.
    key_field: &'static str,
}

/// How a list's key and filter values are read.
pub trait ListKey: Sized + Clone {
    /// The key from a cursor's text, `None` when it is not one.
    fn from_cursor(text: &str) -> Option<Self>;
    /// The key as a cursor carries it.
    fn to_cursor(&self) -> String;
    /// The key a filter value names, `None` when it is not one.
    fn from_value(value: &ODataValue) -> Option<Self>;
}

impl ListKey for uuid::Uuid {
    fn from_cursor(text: &str) -> Option<Self> {
        Self::parse_str(text).ok()
    }

    fn to_cursor(&self) -> String {
        self.to_string()
    }

    fn from_value(value: &ODataValue) -> Option<Self> {
        match value {
            ODataValue::Uuid(id) => Some(*id),
            _ => None,
        }
    }
}

impl ListKey for String {
    fn from_cursor(text: &str) -> Option<Self> {
        Some(text.to_owned())
    }

    fn to_cursor(&self) -> String {
        self.clone()
    }

    fn from_value(value: &ODataValue) -> Option<Self> {
        match value {
            ODataValue::String(text) => Some(text.clone()),
            _ => None,
        }
    }
}

fn invalid(parameter: &'static str, reason: &'static str, message: String) -> DomainError {
    DomainError::InvalidQuery {
        parameter,
        reason,
        message,
    }
}

impl<K: ListKey> ListQuery<K> {
    /// Check `query` for a list ordered by `key_field` with the `$filter`
    /// field `F`, and a page of at most `max` items.
    ///
    /// # Errors
    /// [`DomainError::InvalidQuery`] for a `$select`, an `$orderby`, a filter
    /// on another field or with another operator, or a cursor that is not
    /// one of this list's or belongs to another filter.
    pub fn from_odata<F: FilterField>(
        query: &ODataQuery,
        key_field: &'static str,
        max: u64,
    ) -> Result<Self, DomainError> {
        if query.has_select() {
            return Err(invalid(
                "$select",
                "INVALID_SELECT",
                "the list serves whole items and takes no $select".to_owned(),
            ));
        }
        if !query.order.is_empty() {
            return Err(invalid(
                "$orderby",
                "INVALID_ORDERBY_FIELD",
                format!("the list is ordered by {key_field} and takes no $orderby"),
            ));
        }
        let ids = match query.filter() {
            Some(expr) => Some(filter_ids::<F, K>(expr)?),
            None => None,
        };
        let after = match &query.cursor {
            Some(cursor) => {
                toolkit_odata::validate_cursor_against(
                    cursor,
                    &order(key_field),
                    query.filter_hash.as_deref(),
                )
                .map_err(|e| {
                    let reason = match e {
                        toolkit_odata::Error::FilterMismatch => "FILTER_MISMATCH",
                        toolkit_odata::Error::OrderMismatch => "ORDER_MISMATCH",
                        _ => "INVALID_CURSOR",
                    };
                    invalid("cursor", reason, e.to_string())
                })?;
                let [key] = cursor.k.as_slice() else {
                    return Err(cursor_keys());
                };
                Some(K::from_cursor(key).ok_or_else(cursor_keys)?)
            }
            None => None,
        };
        Ok(Self {
            ids,
            after,
            limit: query.limit.unwrap_or(max).min(max),
            filter_hash: query.filter_hash.clone(),
            key_field,
        })
    }

    /// The cursor of the page after `items`, when the page is full.
    ///
    /// # Errors
    /// [`DomainError::Defect`] when the cursor cannot be encoded.
    pub fn next_cursor<T>(
        &self,
        items: &[T],
        key: impl Fn(&T) -> &K,
    ) -> Result<Option<String>, DomainError> {
        let full = usize::try_from(self.limit).is_ok_and(|limit| items.len() >= limit);
        let Some(last) = items.last().filter(|_| full) else {
            return Ok(None);
        };
        CursorV1 {
            k: vec![key(last).to_cursor()],
            o: SortDir::Asc,
            s: order(self.key_field).to_signed_tokens(),
            f: self.filter_hash.clone(),
            d: "fwd".to_owned(),
        }
        .encode()
        .map(Some)
        .map_err(|_| DomainError::Defect)
    }
}

fn cursor_keys() -> DomainError {
    invalid(
        "cursor",
        "INVALID_CURSOR",
        "invalid cursor: empty or invalid keys".to_owned(),
    )
}

/// The only order a list serves: its key, ascending.
fn order(key_field: &'static str) -> ODataOrderBy {
    ODataOrderBy(vec![OrderKey {
        field: key_field.to_owned(),
        dir: SortDir::Asc,
    }])
}

/// The ids of a `F eq <id>` or `F in (<id>, ...)` filter.
fn filter_ids<F: FilterField, K: ListKey>(
    expr: &toolkit_odata::ast::Expr,
) -> Result<Vec<K>, DomainError> {
    let refused = |message: String| invalid("$filter", "INVALID_FILTER", message);
    let node = convert_expr_to_filter_node::<F>(expr).map_err(|e| refused(e.to_string()))?;
    let values = match node {
        FilterNode::Binary {
            op: FilterOp::Eq,
            value,
            ..
        } => vec![value],
        FilterNode::InList { values, .. } => values,
        _ => {
            return Err(refused(format!(
                "only `{0} eq <id>` or `{0} in (<id>, ...)` is served",
                F::FIELDS.first().map_or("", FilterField::name)
            )));
        }
    };
    values
        .iter()
        .map(|value| K::from_value(value).ok_or_else(|| refused(format!("not an id: {value}"))))
        .collect()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "listing_tests.rs"]
mod tests;
