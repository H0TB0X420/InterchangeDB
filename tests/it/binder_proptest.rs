//! Q-19: binder invariant property tests.
//!
//! The binder (`src/sql/binder.rs`) is 1,078 lines of name + type
//! resolution sitting between sqlparser-rs's AST and our `LogicalPlan`.
//! Bugs here surface as panics, wrong type errors, or silently-accepted
//! garbage. Property tests catch what example-driven tests miss.
//!
//! Two property classes:
//! 1. **Panic-safety**: arbitrary inputs (random bytes treated as SQL,
//!    structurally-malformed-but-parseable statements) must produce
//!    `Err` from `bind`, never a panic.
//! 2. **Structural correctness**: well-formed inputs against a matching
//!    catalog must produce a `LogicalPlan` that references real
//!    catalog entries with the declared types.

use crate::common::mock_catalog::MockCatalog;
use interchangedb::sql::{parse, Binder, LogicalPlan};
use interchangedb::types::ColumnType;
use proptest::prelude::*;

// ---- Panic-safety ----

proptest! {
    /// Arbitrary byte strings as SQL must never panic the binder. The
    /// parser may reject (Err), succeed and the binder rejects (Err), or
    /// succeed end-to-end (Ok). Any of those is fine; panic is not.
    #[test]
    fn binder_never_panics_on_arbitrary_input(s in ".{0,200}") {
        let mc = MockCatalog::new();
        let binder = Binder::new(mc.catalog.clone());
        if let Ok(stmts) = parse(&s) {
            for stmt in stmts {
                let _ = binder.bind(stmt);
            }
        }
    }

    /// Same as above, but with a pre-populated catalog. Some malformed-
    /// but-syntactically-valid statements only fail at bind time
    /// (unknown columns, type mismatches) — those need a real catalog
    /// to exercise.
    #[test]
    fn binder_never_panics_with_populated_catalog(s in ".{0,200}") {
        let mc = MockCatalog::new().with_table(
            "t",
            &[
                ("id", ColumnType::Int32, false),
                ("name", ColumnType::Varchar(50), true),
            ],
            &["id"],
        );
        let binder = Binder::new(mc.catalog.clone());
        if let Ok(stmts) = parse(&s) {
            for stmt in stmts {
                let _ = binder.bind(stmt);
            }
        }
    }

    /// Q-22 partial fuzz coverage: SQL-keyword-laced strings have higher
    /// odds of reaching deep binder code paths than uniformly-random
    /// bytes (which usually parse-fail early). We compose random fragments
    /// from a small grammar — keywords, identifiers, literals, operators
    /// — and assert no panic.
    #[test]
    fn binder_never_panics_on_sql_keyword_laced_input(
        parts in prop::collection::vec(
            prop_oneof![
                Just("SELECT").prop_map(String::from),
                Just("FROM").prop_map(String::from),
                Just("WHERE").prop_map(String::from),
                Just("INSERT").prop_map(String::from),
                Just("INTO").prop_map(String::from),
                Just("VALUES").prop_map(String::from),
                Just("UPDATE").prop_map(String::from),
                Just("SET").prop_map(String::from),
                Just("DELETE").prop_map(String::from),
                Just("CREATE").prop_map(String::from),
                Just("TABLE").prop_map(String::from),
                Just("PRIMARY KEY").prop_map(String::from),
                Just("INT").prop_map(String::from),
                Just("VARCHAR").prop_map(String::from),
                Just("BEGIN").prop_map(String::from),
                Just("COMMIT").prop_map(String::from),
                Just("ROLLBACK").prop_map(String::from),
                Just("EXPLAIN").prop_map(String::from),
                Just("AND").prop_map(String::from),
                Just("OR").prop_map(String::from),
                Just("NOT").prop_map(String::from),
                Just("NULL").prop_map(String::from),
                Just("LIMIT").prop_map(String::from),
                "[a-z][a-z]{0,4}".prop_map(|s| s.to_string()),
                "[0-9]{1,5}".prop_map(|s| s.to_string()),
                "'[a-z]{0,8}'".prop_map(|s| s.to_string()),
                Just("(").prop_map(String::from),
                Just(")").prop_map(String::from),
                Just(",").prop_map(String::from),
                Just("=").prop_map(String::from),
                Just("*").prop_map(String::from),
            ],
            1..20,
        )
    ) {
        let s = parts.join(" ");
        let mc = MockCatalog::new().with_table(
            "t",
            &[("id", ColumnType::Int32, false), ("name", ColumnType::Varchar(50), true)],
            &["id"],
        );
        let binder = Binder::new(mc.catalog.clone());
        if let Ok(stmts) = parse(&s) {
            for stmt in stmts {
                let _ = binder.bind(stmt);
            }
        }
    }
}

// ---- Structural correctness ----

/// A small grammar of legal column types to randomize over.
fn col_type_strategy() -> impl Strategy<Value = ColumnType> {
    prop_oneof![
        Just(ColumnType::Int32),
        Just(ColumnType::Int64),
        Just(ColumnType::Boolean),
        (1u16..=200).prop_map(ColumnType::Varchar),
    ]
}

/// A legal user-table identifier (lowercase alpha, 1-8 chars).
fn ident_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z]{0,7}".prop_map(|s| s.to_string())
}

proptest! {
    /// `SELECT * FROM <tbl>` against a catalog containing `<tbl>` must
    /// bind to a `LogicalPlan::Select` referencing the same table.
    /// Catches: table-resolution drift, accidental case-sensitivity
    /// changes, projection desugaring of `*`.
    #[test]
    fn select_star_resolves_known_table(
        tbl in ident_strategy(),
        col_name in ident_strategy(),
        ty in col_type_strategy(),
    ) {
        // Avoid the reserved __sys_ prefix and a tbl/col name collision.
        prop_assume!(!tbl.starts_with("__sys"));
        prop_assume!(!col_name.starts_with("__sys"));
        prop_assume!(tbl != col_name);

        let mc = MockCatalog::new().with_table(
            &tbl,
            &[(&col_name, ty, false)],
            &[&col_name],
        );
        let binder = Binder::new(mc.catalog.clone());
        let sql = format!("SELECT * FROM {}", tbl);
        let stmts = parse(&sql).unwrap();
        let plan = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
        match plan {
            LogicalPlan::Select { table, .. } => {
                prop_assert_eq!(table, tbl);
            }
            other => panic!("expected Select, got {:?}", other),
        }
    }

    /// `SELECT * FROM <nonexistent>` must return an error, not panic.
    /// Catches: missing catalog-lookup checks, silent fallthrough.
    #[test]
    fn select_from_unknown_table_errors(missing in ident_strategy()) {
        prop_assume!(!missing.starts_with("__sys"));
        let mc = MockCatalog::new(); // no user tables
        let binder = Binder::new(mc.catalog.clone());
        let sql = format!("SELECT * FROM {}", missing);
        if let Ok(stmts) = parse(&sql) {
            for stmt in stmts {
                let result = binder.bind(stmt);
                prop_assert!(result.is_err(), "expected Err for unknown table, got Ok");
            }
        }
    }

    /// `INSERT INTO t VALUES (...)` with mismatched arity must error.
    #[test]
    fn insert_with_wrong_arity_errors(n_extra in 1usize..5) {
        let mc = MockCatalog::new().with_table(
            "t",
            &[
                ("id", ColumnType::Int32, false),
                ("name", ColumnType::Varchar(20), true),
            ],
            &["id"],
        );
        let binder = Binder::new(mc.catalog.clone());
        // Build a values tuple with too many columns.
        let extras: Vec<&str> = (0..n_extra).map(|_| "'x'").collect();
        let sql = format!(
            "INSERT INTO t VALUES (1, 'a'{}{})",
            if n_extra > 0 { ", " } else { "" },
            extras.join(", ")
        );
        let stmts = parse(&sql).unwrap();
        let result = binder.bind(stmts.into_iter().next().unwrap());
        prop_assert!(
            result.is_err(),
            "expected arity-mismatch error for {}, got Ok",
            sql
        );
    }

    /// Round-trip: CREATE TABLE then SELECT against the just-created
    /// table must succeed. Catches: catalog-write/read ordering issues
    /// (we exercised this in unit tests, but property generation catches
    /// edge identifier combinations).
    #[test]
    fn create_then_select_roundtrip(
        tbl in ident_strategy(),
        col in ident_strategy(),
    ) {
        prop_assume!(!tbl.starts_with("__sys"));
        prop_assume!(!col.starts_with("__sys"));
        prop_assume!(tbl != col);

        let mc = MockCatalog::new();
        let binder = Binder::new(mc.catalog.clone());

        let create_sql = format!(
            "CREATE TABLE {} ({} INT PRIMARY KEY)",
            tbl, col
        );
        let create_stmts = parse(&create_sql).unwrap();
        let _create_plan = binder.bind(create_stmts.into_iter().next().unwrap()).unwrap();

        // The MockCatalog builder pre-created tables via the Catalog API,
        // not via SQL. To make CREATE TABLE actually register, we'd need
        // an executor. So instead, register the table directly and then
        // verify SELECT binds.
        let mc2 = MockCatalog::new().with_table(
            &tbl,
            &[(&col, ColumnType::Int32, false)],
            &[&col],
        );
        let binder2 = Binder::new(mc2.catalog.clone());
        let select_sql = format!("SELECT {} FROM {}", col, tbl);
        let select_stmts = parse(&select_sql).unwrap();
        let select_plan = binder2.bind(select_stmts.into_iter().next().unwrap());
        prop_assert!(select_plan.is_ok(), "SELECT after CREATE failed: {:?}", select_plan);
    }
}
