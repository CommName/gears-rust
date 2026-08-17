//! Order-by rendering, keyset (tuple-comparison) predicates, and cursor
//! encode/decode for keyset pagination — adapted for `ClickHouse` via sea-query.
//!
//! The dialect difference vs Postgres remains: positional `?` (not `$N`), with
//! `DateTime64` cursor keys wrapped in `fromUnixTimestamp64Micro(?)`.

use std::str::FromStr;

use sea_query::{Condition, Expr, Order};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use toolkit_odata::filter::FieldKind;
use toolkit_odata::{CursorV1, ODataOrderBy, SortDir};

use super::bind::{SqlBind, sql_bind_to_expr};
use super::translate::SqlCtx;

/// Reject any cursor whose direction is not forward (`"fwd"`).
///
/// # Errors
///
/// Returns an error string when `cursor.d` is anything other than `"fwd"`.
pub fn ensure_forward_cursor(cursor: &CursorV1) -> Result<(), String> {
    if cursor.d == "fwd" {
        Ok(())
    } else {
        Err(format!(
            "unsupported cursor direction `{}`: only forward paging is supported",
            cursor.d
        ))
    }
}

/// Render an `ORDER BY` column list from an `ODataOrderBy`, resolving each
/// field through `col`.
///
/// # Errors
///
/// Returns an error string when the order is empty or a field is not on the
/// allowlist.
pub fn render_order_by(
    order: &ODataOrderBy,
    col: impl Fn(&str) -> Option<&'static str>,
) -> Result<String, String> {
    if order.is_empty() {
        return Err("order must not be empty".to_owned());
    }
    let parts = order
        .0
        .iter()
        .map(|key| {
            let column = col(&key.field)
                .ok_or_else(|| format!("order field not allowlisted: {}", key.field))?;
            let dir = match key.dir {
                SortDir::Asc => "ASC",
                SortDir::Desc => "DESC",
            };
            Ok(format!("{column} {dir}"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(parts.join(", "))
}

/// Resolve order pairs to `(column_name, Order)` for sea-query `order_by`.
///
/// # Errors
///
/// Same conditions as [`render_order_by`].
pub fn order_by_clauses(
    order: &ODataOrderBy,
    col: impl Fn(&str) -> Option<&'static str>,
) -> Result<Vec<(String, Order)>, String> {
    if order.is_empty() {
        return Err("order must not be empty".to_owned());
    }
    order
        .0
        .iter()
        .map(|key| {
            let column = col(&key.field)
                .ok_or_else(|| format!("order field not allowlisted: {}", key.field))?
                .to_owned();
            let dir = match key.dir {
                SortDir::Asc => Order::Asc,
                SortDir::Desc => Order::Desc,
            };
            Ok((column, dir))
        })
        .collect()
}

/// Build a keyset predicate as a row-value tuple comparison [`Condition`].
///
/// For an all-ascending order: `(c1, c2, …) > ($1, $2, …)`.
/// For an all-descending order: `(c1, c2, …) < ($1, $2, …)`.
/// Mixed directions are unsupported (v1 limitation).
///
/// # Errors
///
/// Returns an error string when `order_pairs` is empty, its length differs from
/// `cursor_keys`, a field is nullable, a field is not on the allowlist,
/// directions are mixed, or a cursor key cannot be parsed.
pub fn keyset_condition(
    order_pairs: &[(&str, bool)],
    cursor_keys: &[String],
    col: impl Fn(&str) -> Option<&'static str>,
    kind: impl Fn(&str) -> Option<FieldKind>,
    keyset_safe: impl Fn(&str) -> bool,
) -> Result<Condition, String> {
    let ((columns, cmp), binds) =
        keyset_tuple_parts(order_pairs, cursor_keys, col, kind, keyset_safe)?;
    let rhs: Vec<Expr> = binds.into_iter().map(sql_bind_to_expr).collect();
    // ClickHouse / ClickhouseQueryBuilder uses positional `?` (not `$N`). Using
    // `$1` here leaves literal `$1` in the SQL and drops the bound expressions.
    let slots = vec!["?"; rhs.len()].join(", ");
    let template = format!("({}) {cmp} ({slots})", columns.join(", "));
    Ok(Condition::all().add(Expr::cust_with_exprs(template, rhs)))
}

/// Legacy string-fragment keyset predicate (still used while stores migrate).
///
/// # Errors
///
/// Same as [`keyset_condition`].
pub fn keyset_predicate(
    order_pairs: &[(&str, bool)],
    cursor_keys: &[String],
    col: impl Fn(&str) -> Option<&'static str>,
    kind: impl Fn(&str) -> Option<FieldKind>,
    keyset_safe: impl Fn(&str) -> bool,
    ctx: &mut SqlCtx,
) -> Result<String, String> {
    let ((columns, cmp), binds) =
        keyset_tuple_parts(order_pairs, cursor_keys, col, kind, keyset_safe)?;
    let mut placeholders = Vec::with_capacity(binds.len());
    for b in binds {
        placeholders.push(b.placeholder());
        ctx.push(b);
    }
    Ok(format!(
        "({}) {cmp} ({})",
        columns.join(", "),
        placeholders.join(", ")
    ))
}

fn keyset_tuple_parts(
    order_pairs: &[(&str, bool)],
    cursor_keys: &[String],
    col: impl Fn(&str) -> Option<&'static str>,
    kind: impl Fn(&str) -> Option<FieldKind>,
    keyset_safe: impl Fn(&str) -> bool,
) -> Result<((Vec<&'static str>, &'static str), Vec<SqlBind>), String> {
    if order_pairs.is_empty() {
        return Err("keyset order must not be empty".to_owned());
    }
    if order_pairs.len() != cursor_keys.len() {
        return Err(format!(
            "cursor key count {} does not match order arity {}",
            cursor_keys.len(),
            order_pairs.len()
        ));
    }

    let all_asc = order_pairs.iter().all(|(_, asc)| *asc);
    let all_desc = order_pairs.iter().all(|(_, asc)| !*asc);
    let cmp = if all_asc {
        ">"
    } else if all_desc {
        "<"
    } else {
        return Err("mixed-direction keyset orders are unsupported in v1".to_owned());
    };

    let mut columns = Vec::with_capacity(order_pairs.len());
    let mut binds = Vec::with_capacity(order_pairs.len());
    for ((field, _), raw) in order_pairs.iter().zip(cursor_keys.iter()) {
        if !keyset_safe(field) {
            return Err(format!(
                "keyset field is nullable and cannot be a keyset ordering key: {field}"
            ));
        }
        let column = col(field).ok_or_else(|| format!("keyset field not allowlisted: {field}"))?;
        let field_kind =
            kind(field).ok_or_else(|| format!("keyset field has no known kind: {field}"))?;
        binds.push(cursor_key_to_bind(field_kind, raw)?);
        columns.push(column);
    }

    Ok(((columns, cmp), binds))
}

/// Parse a raw cursor key string into a typed [`SqlBind`] for `ClickHouse`.
///
/// # Errors
///
/// Returns an error string when the value cannot be parsed for its kind.
pub fn cursor_key_to_bind(kind: FieldKind, raw: &str) -> Result<SqlBind, String> {
    match kind {
        FieldKind::DateTimeUtc => {
            let dt = OffsetDateTime::parse(raw, &Rfc3339)
                .map_err(|e| format!("invalid datetime cursor key `{raw}`: {e}"))?;
            let nanos = dt.unix_timestamp_nanos();
            let micros = nanos.div_euclid(1_000);
            #[allow(
                clippy::cast_possible_truncation,
                reason = "practical timestamps fit in i64"
            )]
            Ok(SqlBind::DateTime64Micros(micros as i64))
        }
        FieldKind::Uuid => Uuid::from_str(raw)
            .map(SqlBind::Uuid)
            .map_err(|e| format!("invalid uuid cursor key `{raw}`: {e}")),
        FieldKind::String => Ok(SqlBind::Str(raw.to_owned())),
        other => Err(format!(
            "cursor key kind `{other}` is not supported as a keyset column"
        )),
    }
}

/// Build and encode the forward cursor for the next page.
///
/// # Errors
///
/// Returns an error string when the order is empty, its arity differs from
/// `last_row_keys`, or serialisation fails.
pub fn encode_next_cursor(
    order: &ODataOrderBy,
    last_row_keys: &[String],
    filter_hash: Option<&str>,
) -> Result<String, String> {
    if order.is_empty() {
        return Err("cursor order must not be empty".to_owned());
    }
    if order.0.len() != last_row_keys.len() {
        return Err(format!(
            "row key count {} does not match order arity {}",
            last_row_keys.len(),
            order.0.len()
        ));
    }
    let primary_dir = order.0.first().map_or(SortDir::Asc, |k| k.dir);
    let cursor = CursorV1 {
        k: last_row_keys.to_vec(),
        o: primary_dir,
        s: order.to_signed_tokens(),
        f: filter_hash.map(str::to_owned),
        d: "fwd".to_owned(),
    };
    cursor
        .encode()
        .map_err(|e| format!("cursor encode failed: {e}"))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "keyset_tests.rs"]
mod keyset_tests;
