//! Source-inspection tests of the registry persistence layer (DESIGN §2.2, §4.8
//! "Inspection": "no raw SQL"; ADR 0007: no tenant value; the secure ORM clippy rules).
//!
//! These read the gear's own source files, like the other inspection tests of the
//! workspace (`CARGO_MANIFEST_DIR`). Comments are removed with a string-aware scanner, so a
//! `//` inside a string literal is code and a commented-out call is not.
//!
//! - `no_raw_sql_outside_migrations`: production code under `src/` (not `*_tests.rs`, not
//!   `test_support/`, not the migration files) holds none of the raw-SQL entry points (§2.2).
//! - `migrations_are_the_only_place_that_may_hold_raw_sql_and_do_hold_the_checks`: the
//!   migration source carries the CHECK constraints the cases rely on, so the bound is a
//!   database rule and not only a domain rule (§3.7 "Constraints"). The byte-length check is
//!   verified in each backend arm (`SQLite`, Postgres, `MySQL`) on its own, so one arm cannot
//!   vouch for another. The migration source matches on the backend with one arm per
//!   backend (`Sqlite => ..`, `Postgres => ..`, `MySql => ..`).
//! - `entity_and_migration_hold_no_tenant_value`: the entity and migration sources mention
//!   no tenant, owner, resource or type scoping column (ADR 0007).
//! - `the_entity_is_declared_unrestricted_in_source`: `#[secure(unrestricted)]` and no
//!   `pep_prop` (§3.7 "Additional info"); one `DeriveEntityModel` derive.
//! - `the_repository_uses_the_secure_wrappers_only`: per statement (up to its `;`), every
//!   `::find(`, `::find_by_id(`, `::insert(`, `::insert_many(`, `::update(`,
//!   `::update_many(`, `::delete(`, `::delete_many(`, `::delete_by_id(` and `.exec(` has a
//!   `.secure()` chain or goes through `secure_insert` (the clippy `disallowed_methods` rule
//!   does not cover `Insert::exec`).
//! - `nothing_silences_the_secure_orm_clippy_rules`: no `#[allow]`, `#[expect]` or `#[warn]`
//!   of `clippy::disallowed_methods`, on an item or on the crate, anywhere under `src/`.
//! - `the_repository_never_orders_limits_or_paginates`: DESIGN §4.10, no listing window.
//! - `the_scanner_*`: the scanner itself, on small samples.

use std::fs;
use std::path::{Path, PathBuf};

const RAW_SQL_ENTRY_POINTS: [&str; 16] = [
    "execute_unprepared",
    "Statement::from_string",
    "Statement::from_sql_and_values",
    "from_sql_and_values",
    "query_all_raw",
    "query_one_raw",
    "execute_raw",
    "raw_sql",
    "Expr::cust",
    "Func::cust",
    "SimpleExpr::Custom",
    "sea_orm::Statement",
    "sqlx::",
    "find_by_statement",
    "stream_raw",
    "ConnectionTrait",
];

/// Calls that start or run a query; each statement holding one must be a secure chain.
const QUERY_TOKENS: [&str; 10] = [
    "::find(",
    "::find_by_id(",
    "::insert(",
    "::insert_many(",
    "::update(",
    "::update_many(",
    "::delete(",
    "::delete_many(",
    "::delete_by_id(",
    ".exec(",
];

/// Calls and imports that window a listing.
const WINDOW_TOKENS: [&str; 8] = [
    ".order_by",
    ".limit(",
    ".offset(",
    ".paginate(",
    ".cursor_by(",
    "QuerySelect",
    "QueryOrder",
    "PaginatorTrait",
];

fn src_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read a source directory") {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// A source text without comments, in two views of the same length.
struct Scrubbed {
    /// String contents kept (a raw SQL string is code).
    code: String,
    /// String and character contents blanked, so structure (`;`, `(`, `{`) can be scanned.
    shape: String,
}

fn emit(out: &mut Scrubbed, c: char, blank_in_shape: bool) {
    out.code.push(c);
    out.shape
        .push(if blank_in_shape && c != '\n' { ' ' } else { c });
}

/// Index after a `//` comment (the newline itself is kept).
fn skip_line_comment(chars: &[char], mut i: usize) -> usize {
    while i < chars.len() && chars[i] != '\n' {
        i += 1;
    }
    i
}

/// Index after a (nested) `/* */` comment.
fn skip_block_comment(chars: &[char], mut i: usize) -> usize {
    let mut depth = 0_u32;
    while i < chars.len() {
        if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
            depth += 1;
            i += 2;
        } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
            depth -= 1;
            i += 2;
            if depth == 0 {
                break;
            }
        } else {
            i += 1;
        }
    }
    i
}

/// Copies a raw string (`r"..."`, `r#"..."#`, `br"..."`) starting at `i`, if one starts
/// there; returns the index after it.
fn copy_raw_string(chars: &[char], i: usize, out: &mut Scrubbed) -> Option<usize> {
    let start = if chars[i] == 'b' { i + 2 } else { i + 1 };
    let hashes = chars
        .get(start..)?
        .iter()
        .take_while(|&&h| h == '#')
        .count();
    if chars.get(start + hashes) != Some(&'"') {
        return None;
    }
    for &p in &chars[i..=start + hashes] {
        emit(out, p, false);
    }
    let mut j = start + hashes + 1;
    while j < chars.len() {
        let closes =
            chars[j] == '"' && chars[j + 1..].iter().take_while(|&&h| h == '#').count() >= hashes;
        if closes {
            for &p in &chars[j..=j + hashes] {
                emit(out, p, false);
            }
            return Some(j + hashes + 1);
        }
        emit(out, chars[j], true);
        j += 1;
    }
    Some(j)
}

/// Copies a normal string starting at the quote at `i`; returns the index after it.
fn copy_string(chars: &[char], mut i: usize, out: &mut Scrubbed) -> usize {
    emit(out, '"', false);
    i += 1;
    while i < chars.len() && chars[i] != '"' {
        if chars[i] == '\\' && i + 1 < chars.len() {
            emit(out, chars[i], true);
            i += 1;
        }
        emit(out, chars[i], true);
        i += 1;
    }
    if i < chars.len() {
        emit(out, '"', false);
        i += 1;
    }
    i
}

/// Copies a character literal (`'x'`, `'\n'`) or a lifetime (`'a`) starting at `i`.
fn copy_char_or_lifetime(chars: &[char], i: usize, out: &mut Scrubbed) -> usize {
    let literal_end = if chars.get(i + 1) == Some(&'\\') {
        chars[i + 3..]
            .iter()
            .position(|&q| q == '\'')
            .map(|p| i + 3 + p)
    } else if chars.get(i + 2) == Some(&'\'') {
        Some(i + 2)
    } else {
        None
    };
    let Some(end) = literal_end else {
        emit(out, '\'', false);
        return i + 1;
    };
    emit(out, '\'', false);
    for &p in &chars[i + 1..end] {
        emit(out, p, true);
    }
    emit(out, '\'', false);
    end + 1
}

/// Removes `//` and (nested) `/* */` comments; understands normal, raw and byte strings,
/// character literals and lifetimes, so a `//` inside a string is not a comment.
fn scrub(src: &str) -> Scrubbed {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Scrubbed {
        code: String::new(),
        shape: String::new(),
    };
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        let ident_before = i > 0 && (chars[i - 1].is_alphanumeric() || chars[i - 1] == '_');
        let raw_start = (c == 'r' || (c == 'b' && next == Some('r'))) && !ident_before;
        if c == '/' && next == Some('/') {
            i = skip_line_comment(&chars, i);
        } else if c == '/' && next == Some('*') {
            i = skip_block_comment(&chars, i);
            emit(&mut out, ' ', false);
        } else if let Some(after) = raw_start
            .then(|| copy_raw_string(&chars, i, &mut out))
            .flatten()
        {
            i = after;
        } else if c == '"' {
            i = copy_string(&chars, i, &mut out);
        } else if c == '\'' {
            i = copy_char_or_lifetime(&chars, i, &mut out);
        } else {
            emit(&mut out, c, false);
            i += 1;
        }
    }
    out
}

fn scrubbed_file(path: &Path) -> Scrubbed {
    scrub(&fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display())))
}

fn code_of(path: &Path) -> String {
    scrubbed_file(path).code
}

fn is_test_file(path: &Path) -> bool {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name.ends_with("_tests.rs") || path.components().any(|c| c.as_os_str() == "test_support")
}

fn is_migration_file(path: &Path) -> bool {
    let rel = path.strip_prefix(src_root()).expect("under src");
    rel.starts_with("infra/storage/migrations") || rel == Path::new("infra/storage/migrations.rs")
}

fn storage_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    rust_files(&src_root().join("infra/storage"), &mut files);
    files.into_iter().filter(|p| !is_test_file(p)).collect()
}

/// The statement around index `pos` of a `shape` view: from the previous `;`, `{` or `}`
/// (or an unmatched opening bracket) to the next `;` (or unmatched closing bracket).
fn statement_around(shape: &[char], pos: usize) -> (usize, usize) {
    let mut depth = 0_u32;
    let mut start = pos;
    while start > 0 {
        let c = shape[start - 1];
        let stops = match c {
            ')' | ']' => {
                depth += 1;
                false
            }
            '(' | '[' if depth > 0 => {
                depth -= 1;
                false
            }
            '(' | '[' => true,
            ';' | '{' | '}' => depth == 0,
            _ => false,
        };
        if stops {
            break;
        }
        start -= 1;
    }
    let mut depth = 0_u32;
    let mut end = pos;
    while end < shape.len() {
        let stops = match shape[end] {
            '(' | '[' | '{' => {
                depth += 1;
                false
            }
            ')' | ']' | '}' if depth > 0 => {
                depth -= 1;
                false
            }
            ')' | ']' | '}' => true,
            ';' => depth == 0,
            _ => false,
        };
        if stops {
            break;
        }
        end += 1;
    }
    (start, end)
}

/// `RegistryRepo::delete(..)` and friends are the gear's own repository functions (the
/// store calls them), not ORM entry points; their bodies are checked in `repo.rs`.
fn is_repository_call(chars: &[char], at: usize) -> bool {
    let name: String = chars[..at]
        .iter()
        .rev()
        .take_while(|c| c.is_alphanumeric() || **c == '_')
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    name == "RegistryRepo"
}

/// Statements that start or run a query without a `.secure()` chain or `secure_insert`.
fn unsecured_statements(shape: &str) -> Vec<String> {
    let chars: Vec<char> = shape.chars().collect();
    let mut found: Vec<(usize, usize)> = Vec::new();
    for token in QUERY_TOKENS {
        let needle: Vec<char> = token.chars().collect();
        let mut at = 0;
        while at + needle.len() <= chars.len() {
            if chars[at..at + needle.len()] == needle[..] && !is_repository_call(&chars, at) {
                let range = statement_around(&chars, at);
                if !found.contains(&range) {
                    found.push(range);
                }
            }
            at += 1;
        }
    }
    found
        .into_iter()
        .map(|(start, end)| chars[start..end].iter().collect::<String>())
        .filter(|statement| {
            !statement.contains(".secure()") && !statement.contains("secure_insert")
        })
        .map(|statement| statement.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect()
}

/// Attributes (`#[..]`, `#![..]`) that allow, expect or warn on `disallowed_methods`.
fn silencing_attributes(shape: &str) -> Vec<String> {
    let chars: Vec<char> = shape.chars().collect();
    let mut hits = Vec::new();
    let mut i = 0;
    while i + 1 < chars.len() {
        if chars[i] == '#' {
            let open = if chars[i + 1] == '!' { i + 2 } else { i + 1 };
            if chars.get(open) == Some(&'[') {
                let mut depth = 0_u32;
                let mut end = open;
                while end < chars.len() {
                    match chars[end] {
                        '[' => depth += 1,
                        ']' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    end += 1;
                }
                let inner: String = chars[open + 1..end.min(chars.len())]
                    .iter()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                let silences = ["allow(", "expect(", "warn("]
                    .iter()
                    .any(|k| inner.starts_with(k));
                if silences && inner.contains("disallowed_methods") {
                    hits.push(inner);
                }
                i = end;
            }
        }
        i += 1;
    }
    hits
}

#[test]
fn no_raw_sql_outside_migrations() {
    let mut files = Vec::new();
    rust_files(&src_root(), &mut files);
    let mut hits = Vec::new();
    for file in files
        .iter()
        .filter(|f| !is_test_file(f) && !is_migration_file(f))
    {
        let code = code_of(file);
        for token in RAW_SQL_ENTRY_POINTS {
            if code.contains(token) {
                hits.push(format!("{}: {token}", file.display()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "raw SQL outside migrations (DESIGN §2.2): {hits:#?}"
    );
}

/// The text of the `Sqlite`, `Postgres` or `MySql` match arm of the migration source: from
/// the arm's `<name> =>` to the next arm marker (or the end).
fn backend_arm(code: &str, name: &str) -> String {
    let markers = ["sqlite =>", "postgres =>", "mysql =>"];
    let start = code
        .find(name)
        .unwrap_or_else(|| panic!("the migration has no `{name}` arm"));
    let end = markers
        .iter()
        .filter(|m| **m != name)
        .filter_map(|m| {
            code[start + name.len()..]
                .find(m)
                .map(|p| start + name.len() + p)
        })
        .min()
        .unwrap_or(code.len());
    code[start..end].to_owned()
}

#[test]
fn migrations_are_the_only_place_that_may_hold_raw_sql_and_do_hold_the_checks() {
    let mut files = Vec::new();
    rust_files(&src_root().join("infra/storage"), &mut files);
    let code: String = files
        .iter()
        .filter(|f| is_migration_file(f) && !is_test_file(f))
        .map(|f| code_of(f))
        .collect::<Vec<_>>()
        .join("\n")
        .to_lowercase();

    assert!(!code.is_empty(), "the migration source exists");
    assert!(
        code.contains("application_registry"),
        "the migration creates the registry table"
    );
    assert!(
        code.contains("check"),
        "the bounds are CHECK constraints the database enforces"
    );
    for token in [
        "succeeded",
        "partially_failed",
        "plugin_rejected",
        "not_permitted",
        "internal_failure",
    ] {
        assert!(code.contains(token), "the last_result CHECK lists {token}");
    }

    // The text bound counts bytes, in every backend arm on its own: a character-counting
    // `VARCHAR(255)` is not a bound, and one arm's check cannot vouch for another.
    let sqlite = backend_arm(&code, "sqlite =>");
    let postgres = backend_arm(&code, "postgres =>");
    let mysql = backend_arm(&code, "mysql =>");
    assert!(
        sqlite.contains("as blob"),
        "SQLite counts bytes with length(CAST(.. AS BLOB)): {sqlite}"
    );
    assert!(
        postgres.contains("octet_length"),
        "Postgres counts bytes with octet_length: {postgres}"
    );
    assert!(
        mysql.contains("octet_length"),
        "MySQL counts bytes with octet_length: {mysql}"
    );
    for (backend, arm) in [
        ("sqlite", &sqlite),
        ("postgres", &postgres),
        ("mysql", &mysql),
    ] {
        assert!(
            arm.contains("check"),
            "{backend} arm holds CHECK constraints"
        );
        assert_eq!(
            arm.matches("between 1 and 255").count(),
            2,
            "{backend}: id and version are 1..=255 bytes"
        );
    }
}

#[test]
fn entity_and_migration_hold_no_tenant_value() {
    let files: Vec<PathBuf> = storage_files()
        .into_iter()
        .filter(|f| is_migration_file(f) || f.file_name().is_some_and(|n| n == "entity.rs"))
        .collect();
    assert!(
        files.len() >= 2,
        "entity and migration sources found: {files:?}"
    );

    for file in files {
        let code = code_of(&file).to_lowercase();
        for token in ["tenant", "owner", "resource_id", "type_id"] {
            assert!(
                !code.contains(token),
                "{} mentions `{token}`: the registry has no scoping value (ADR 0007)",
                file.display()
            );
        }
    }
}

#[test]
fn the_entity_is_declared_unrestricted_in_source() {
    let entity = code_of(&src_root().join("infra/storage/entity.rs"));
    assert!(entity.contains("#[secure(unrestricted)]"));
    assert!(
        !entity.contains("pep_prop"),
        "pep_prop is forbidden with unrestricted"
    );
    // Count the derive attribute, not the bare token (a `use` of the name would count too).
    let derives = entity
        .split("#[derive(")
        .skip(1)
        .filter(|rest| {
            rest.split(")]")
                .next()
                .is_some_and(|list| list.contains("DeriveEntityModel"))
        })
        .count();
    assert_eq!(derives, 1, "one entity");
}

#[test]
fn the_repository_uses_the_secure_wrappers_only() {
    let mut checked = 0;
    for file in storage_files()
        .into_iter()
        .filter(|f| !is_migration_file(f))
    {
        let shape = scrubbed_file(&file).shape;
        let loose = unsecured_statements(&shape);
        assert!(
            loose.is_empty(),
            "{}: a query statement without .secure() / secure_insert: {loose:#?}",
            file.display()
        );
        checked += QUERY_TOKENS
            .iter()
            .map(|t| shape.matches(t).count())
            .sum::<usize>();
    }
    assert!(
        checked >= 4,
        "the repository runs queries ({checked} query calls found)"
    );
}

#[test]
fn nothing_silences_the_secure_orm_clippy_rules() {
    let mut files = Vec::new();
    rust_files(&src_root(), &mut files);
    assert!(files.len() > 10, "the gear's sources were found");
    let mut hits = Vec::new();
    for file in files {
        for attribute in silencing_attributes(&scrubbed_file(&file).shape) {
            hits.push(format!("{}: #[{attribute}]", file.display()));
        }
    }
    assert!(
        hits.is_empty(),
        "disallowed_methods silenced (plan P4): {hits:#?}"
    );
}

#[test]
fn the_repository_never_orders_limits_or_paginates() {
    // DESIGN §3.2, §4.10: a listing is every row, in no defined order.
    for file in storage_files()
        .into_iter()
        .filter(|f| !is_migration_file(f))
    {
        let shape: String = scrubbed_file(&file).shape.split_whitespace().collect();
        for token in WINDOW_TOKENS {
            assert!(
                !shape.contains(token),
                "{} uses `{token}`: no ordering, limit, offset or pagination (DESIGN §4.10)",
                file.display()
            );
        }
    }
}

#[test]
fn the_scanner_ignores_comments_and_keeps_strings() {
    let src = "let a = \"// not a comment\"; // gone\n/* Entity::find() /* nested */ still gone */\nlet b = r#\"x // y\"#;\nlet c = '\"'; let d: &'static str = \"q\";";
    let scrubbed = scrub(src);

    assert!(scrubbed.code.contains("// not a comment"));
    assert!(!scrubbed.code.contains("gone"));
    assert!(!scrubbed.code.contains("Entity::find"));
    assert!(scrubbed.code.contains("x // y"));
    assert!(scrubbed.code.contains("'static"));
    assert!(
        !scrubbed.shape.contains("not a comment"),
        "string contents are blanked in the shape view"
    );
    assert_eq!(
        scrubbed.code.chars().count(),
        scrubbed.shape.chars().count()
    );
}

#[test]
fn the_scanner_flags_unsecured_statements_and_accepts_wrapped_secure_chains() {
    let secure = "let row = Entity::find_by_id(id)\n    .secure()\n    .scope_with(scope)\n    .one(runner)\n    .await?;\n\
                  Entity::insert(am.clone())\n    .secure()\n    .scope_with_model(scope, &am)?\n    .on_conflict_raw(on_conflict)\n    .exec(runner)\n    .await?;\n\
                  secure_insert::<Entity>(am, scope, runner).await?;\n\
                  Entity::delete_many()\n    .filter(Column::ApplicationId.eq(id))\n    .secure()\n    .scope_with(scope)\n    .exec(runner)\n    .await?\n";
    assert!(unsecured_statements(&scrub(secure).shape).is_empty());

    for bare in [
        "Entity::insert(am).on_conflict(oc).exec(runner).await?;",
        "Entity::find().all(runner).await?;",
        "Entity::delete_by_id(id).exec(runner).await?;",
        "Entity::update_many().col_expr(c, e).exec(runner).await?;",
        "let q = Entity::find_by_id(id);\nq.secure().one(runner).await?;\nEntity::insert_many(rows).exec(runner).await?;",
    ] {
        assert!(
            !unsecured_statements(&scrub(bare).shape).is_empty(),
            "{bare}"
        );
    }
    // The gear's own repository functions are not ORM entry points.
    assert!(
        unsecured_statements(&scrub("RegistryRepo::delete(&conn, &scope, id).await?;").shape)
            .is_empty()
    );
    // A mention in a comment or a string is not a call.
    let quiet = "// Entity::find().all(r)\nlet s = \"Entity::insert(x).exec(r)\";";
    assert!(unsecured_statements(&scrub(quiet).shape).is_empty());
}

#[test]
fn the_scanner_finds_silencing_attributes_on_items_and_on_the_crate() {
    for src in [
        "#[allow(clippy::disallowed_methods)]\nfn f() {}",
        "#![allow(clippy::disallowed_methods)]",
        "#[allow(\n    clippy::needless_return,\n    clippy::disallowed_methods\n)]\nfn f() {}",
        "#[expect(clippy::disallowed_methods)]\nfn f() {}",
    ] {
        assert_eq!(silencing_attributes(&scrub(src).shape).len(), 1, "{src}");
    }
    for src in [
        "#[allow(clippy::doc_markdown)]\nfn f() {}",
        "// #[allow(clippy::disallowed_methods)]\nfn f() {}",
        "let s = \"#[allow(clippy::disallowed_methods)]\";",
        "#[deny(clippy::disallowed_methods)]\nfn f() {}",
    ] {
        assert!(silencing_attributes(&scrub(src).shape).is_empty(), "{src}");
    }
}
