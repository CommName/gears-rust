#![allow(clippy::unwrap_used, clippy::expect_used)]

use toolkit_odata::ast::{CompareOperator, Expr, Value};
use toolkit_odata::short_filter_hash;
use uuid::Uuid;

use super::*;

fn ident(name: &str) -> Expr {
    Expr::Identifier(name.to_owned())
}

fn uuid(n: u128) -> Expr {
    Expr::Value(Value::Uuid(Uuid::from_u128(n)))
}

fn text(t: &str) -> Expr {
    Expr::Value(Value::String(t.to_owned()))
}

fn eq(field: &str, value: Expr) -> Expr {
    Expr::Compare(Box::new(ident(field)), CompareOperator::Eq, Box::new(value))
}

/// A query with `filter`, as the extractor builds it: with its hash.
fn filtered(filter: Expr) -> ODataQuery {
    let hash = short_filter_hash(Some(&filter));
    let query = ODataQuery::new().with_filter(filter);
    match hash {
        Some(hash) => query.with_filter_hash(hash),
        None => query,
    }
}

fn history(query: &ODataQuery) -> Result<ListQuery<Uuid>, DomainError> {
    ListQuery::<Uuid>::from_odata::<HistoryFilterField>(query, "event_id", 100)
}

fn applications(query: &ODataQuery) -> Result<ListQuery<String>, DomainError> {
    ListQuery::<String>::from_odata::<ApplicationFilterField>(query, "application_id", 100)
}

fn reason(err: &DomainError) -> (&'static str, &'static str) {
    match err {
        DomainError::InvalidQuery {
            parameter, reason, ..
        } => (parameter, reason),
        other => panic!("not an invalid query: {other:?}"),
    }
}

#[test]
fn no_option_is_the_first_full_page() {
    let list = history(&ODataQuery::new()).unwrap();
    assert_eq!(list.ids, None);
    assert_eq!(list.after, None);
    assert_eq!(list.limit, 100);
}

#[test]
fn limit_is_capped_at_the_page_maximum() {
    assert_eq!(history(&ODataQuery::new().with_limit(7)).unwrap().limit, 7);
    assert_eq!(
        history(&ODataQuery::new().with_limit(1000)).unwrap().limit,
        100
    );
}

#[test]
fn eq_and_in_name_the_ids() {
    let list = history(&filtered(eq("operation_id", uuid(1)))).unwrap();
    assert_eq!(list.ids, Some(vec![Uuid::from_u128(1)]));

    let list = history(&filtered(Expr::In(
        Box::new(ident("operation_id")),
        vec![uuid(1), uuid(2)],
    )))
    .unwrap();
    assert_eq!(list.ids, Some(vec![Uuid::from_u128(1), Uuid::from_u128(2)]));

    let list = applications(&filtered(eq("application_id", text("acme")))).unwrap();
    assert_eq!(list.ids, Some(vec!["acme".to_owned()]));
}

#[test]
fn every_other_filter_is_refused() {
    let cases = [
        ("another field", eq("correlation_id", uuid(1))),
        (
            "another operator",
            Expr::Compare(
                Box::new(ident("operation_id")),
                CompareOperator::Ne,
                Box::new(uuid(1)),
            ),
        ),
        (
            "a conjunction",
            eq("operation_id", uuid(1)).and(eq("operation_id", uuid(2))),
        ),
        ("a text for an id", eq("operation_id", text("x"))),
    ];
    for (name, filter) in cases {
        let err = history(&filtered(filter)).unwrap_err();
        assert_eq!(reason(&err), ("$filter", "INVALID_FILTER"), "{name}");
    }
}

#[test]
fn select_and_orderby_are_refused() {
    let select = ODataQuery::new().with_select(vec!["event_id".to_owned()]);
    assert_eq!(
        reason(&history(&select).unwrap_err()),
        ("$select", "INVALID_SELECT")
    );
    let order = ODataQuery::new().with_order(ODataOrderBy(vec![OrderKey {
        field: "event_id".to_owned(),
        dir: SortDir::Desc,
    }]));
    assert_eq!(
        reason(&history(&order).unwrap_err()),
        ("$orderby", "INVALID_ORDERBY_FIELD")
    );
}

#[test]
fn a_full_page_carries_a_cursor_the_next_request_resumes_after() {
    let list = history(&ODataQuery::new().with_limit(2)).unwrap();
    let items = [Uuid::from_u128(1), Uuid::from_u128(2)];
    let token = list.next_cursor(&items, |id| id).unwrap().unwrap();

    let cursor = CursorV1::decode(&token).unwrap();
    let next = history(&ODataQuery::new().with_limit(2).with_cursor(cursor)).unwrap();
    assert_eq!(next.after, Some(Uuid::from_u128(2)));

    assert_eq!(
        list.next_cursor(&items[..1], |id| id).unwrap(),
        None,
        "a short page is the last"
    );
}

#[test]
fn a_cursor_keeps_its_filter() {
    let query = filtered(eq("operation_id", uuid(1))).with_limit(1);
    let list = history(&query).unwrap();
    let token = list
        .next_cursor(&[Uuid::from_u128(5)], |id| id)
        .unwrap()
        .unwrap();
    let cursor = CursorV1::decode(&token).unwrap();

    let same = query.with_cursor(cursor.clone());
    assert_eq!(history(&same).unwrap().after, Some(Uuid::from_u128(5)));

    let other = filtered(eq("operation_id", uuid(2))).with_cursor(cursor);
    assert_eq!(
        reason(&history(&other).unwrap_err()),
        ("cursor", "FILTER_MISMATCH")
    );
}

#[test]
fn a_cursor_of_another_list_or_with_a_bad_key_is_refused() {
    let applications_cursor = CursorV1 {
        k: vec!["acme".to_owned()],
        o: SortDir::Asc,
        s: "+application_id".to_owned(),
        f: None,
        d: "fwd".to_owned(),
    };
    assert_eq!(
        reason(&history(&ODataQuery::new().with_cursor(applications_cursor)).unwrap_err()),
        ("cursor", "ORDER_MISMATCH")
    );
    let bad_key = CursorV1 {
        k: vec!["not-a-uuid".to_owned()],
        o: SortDir::Asc,
        s: "+event_id".to_owned(),
        f: None,
        d: "fwd".to_owned(),
    };
    assert_eq!(
        reason(&history(&ODataQuery::new().with_cursor(bad_key)).unwrap_err()),
        ("cursor", "INVALID_CURSOR")
    );
}
