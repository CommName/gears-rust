//! `ClickHouse`-backed integration suite — a **single** test binary so every
//! test shares **one** `ClickHouse` server.
//!
//! A server takes ~5s to start and a test body ~20ms, so starting one per test
//! — or one per suite file — was the suite's whole cost. The harness keeps the
//! server in a `static` [`tokio::sync::OnceCell`] (see `common`), which shares
//! it across every test *in this process*; collapsing the formerly four
//! separate test binaries into this one is what lets that static serve the
//! whole suite from a single container. Each test still owns its own database,
//! so the sharing is invisible to the tests.
//!
//! Run it with `cargo test` (see `make test-usage-collector-ch`), not nextest:
//! nextest's process-per-test would start a fresh server per test again.
//! Requires Docker, except for the fixture-contract test in `records_ingest`.
#![cfg(feature = "clickhouse")]

mod common;

mod catalog;
mod readiness;
mod records_ingest;
mod records_query;
