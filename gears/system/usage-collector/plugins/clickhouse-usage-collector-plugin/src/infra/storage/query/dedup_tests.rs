use super::{LATEST_ONE, RECORD_DEDUP_KEY, latest_by};

// ── latest_by ────────────────────────────────────────────────────────────────

#[test]
fn latest_by_renders_single_key() {
    assert_eq!(
        latest_by(&["gts_id"]),
        " ORDER BY version DESC LIMIT 1 BY gts_id"
    );
}

#[test]
fn latest_by_renders_full_record_key() {
    assert_eq!(
        latest_by(RECORD_DEDUP_KEY),
        " ORDER BY version DESC LIMIT 1 BY gts_id, tenant_id, created_at, id"
    );
}

#[test]
fn latest_by_starts_with_space_so_it_appends_to_a_query_body() {
    // Call sites concatenate this straight onto `… WHERE x = ?` with no
    // separator of their own.
    assert!(latest_by(&["id"]).starts_with(' '));
    assert!(LATEST_ONE.starts_with(' '));
}

#[test]
fn latest_by_orders_version_descending() {
    // The DESC is the whole point: `LIMIT 1 BY` takes the first row per key in
    // the ordered stream, so ASC would resolve to the *superseded* row.
    assert!(latest_by(&["id"]).contains("version DESC"));
    assert!(LATEST_ONE.contains("version DESC"));
}

// ── RECORD_DEDUP_KEY ─────────────────────────────────────────────────────────

#[test]
fn record_dedup_key_is_pinned() {
    // The schema's ORDER BY is this constant, so changing it changes the sort
    // key of fresh tables — but not of existing ones, which `ORDER BY` cannot
    // be altered on. That tuple is what the engine collapses on, so a column
    // added or dropped here silently resolves the wrong row on old tables.
    assert_eq!(
        RECORD_DEDUP_KEY,
        &["gts_id", "tenant_id", "created_at", "id"]
    );
}
