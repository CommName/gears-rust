//! Domain-level storage and platform limits (DESIGN §3.7, §4.7).

/// Byte bound of the registry text columns (application id and version), fixed by the
/// initial migration (DESIGN §3.7). Changing it is a schema change.
pub const REGISTRY_TEXT_MAX_BYTES: usize = 255;

/// Byte bound of one free-text value in a log event (escaped, cut at a character
/// boundary), so a plugin- or registry-provided string cannot flood the log.
pub const LOG_TEXT_MAX_BYTES: usize = 1024;

/// Deepest nesting of arrays and objects the manifest parse accepts (`[]` counts as 1).
/// At most the SDK's `HANDOFF_MANIFEST_MAX_DEPTH`, so every accepted manifest round-trips
/// inside the hand-off content, and below the `serde_json` recursion limit (DESIGN §3.2 step 1).
pub const MAX_JSON_DEPTH: usize = 126;

/// Deepest nesting of arrays and objects a served section schema may have (the root counts
/// 1). A deeper schema is unusable (`UnusableCause::TooDeep`): compiling and cloning are
/// recursive, so the bound keeps a hostile schema from exhausting the stack (DESIGN §4.3).
pub const SCHEMA_MAX_DEPTH: usize = 64;

/// Approximate compiled-size limit, in bytes, of one served `pattern` (DESIGN §3.2 Schema
/// Validator).
pub const SCHEMA_REGEX_SIZE_LIMIT: usize = 1024 * 1024;

/// Approximate lazy-DFA cache limit, in bytes, of one served `pattern` (DESIGN §3.2 Schema
/// Validator).
pub const SCHEMA_REGEX_DFA_SIZE_LIMIT: usize = 1024 * 1024;

/// Most distinct reference paths that may reach one schema node of a served schema. A node
/// reached by more paths than this (a reference DAG that fans out, such as two `$ref`s per
/// level) makes the schema unusable (`UnusableCause::TooDeep`): the validation library expands
/// every path, so the work grows exponentially with the depth (DESIGN §4.3). Counted over the
/// acyclic part of the graph of schema children and local references; in a tree a node counts 1,
/// and a `$defs` entry used by a reference counts one more for its place in `$defs`.
pub const SCHEMA_MAX_PATHS: usize = 4096;

/// Most problems one manifest validation returns. The error enumeration stops at this many
/// distinct problems and one final problem with the rule `too_many_problems` at the location
/// `""` marks the cut (DESIGN §3.3, §4.3), so the size of the answer does not depend on the
/// manifest or the served schemas.
pub const MAX_VALIDATION_PROBLEMS: usize = 100;

/// Most library errors one manifest validation examines before it gives up and reports
/// `too_many_problems`. Errors that repeat a (location, rule) pair add no problem, so the
/// problem cap alone would not bound the enumeration.
pub const MAX_VALIDATION_ERRORS_SCANNED: usize = 10_000;

/// Byte bound of the location of one problem: the log text bound. A longer location is cut at
/// a segment boundary (an ancestor of the failing value), so a hostile key cannot make the
/// problem list large.
pub const LOCATION_MAX_BYTES: usize = LOG_TEXT_MAX_BYTES;

/// Most distinct `pattern` and `patternProperties` expressions one served schema may carry
/// (`UnusableCause::Pattern`). Counted before any is compiled: every pattern is compiled into
/// the library, so the heap and the time grow with their number (DESIGN §4.3).
pub const SCHEMA_MAX_PATTERNS: usize = 256;

/// Most total bytes of the distinct patterns of one served schema (`UnusableCause::Pattern`),
/// checked together with [`SCHEMA_MAX_PATTERNS`] before any pattern is compiled.
pub const SCHEMA_PATTERNS_MAX_BYTES: usize = 64 * 1024;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[allow(clippy::non_ascii_literal)]
#[path = "limits_tests.rs"]
mod limits_tests;
