//! Multi-way-join correctness for predicate pushdown + equi-key promotion
//! (docs/plan-predicate-pushdown.md, Phases A–D).
//!
//! The 2-table StockLevel test exercises the pushdown at one arity; TPC-H joins
//! 3–8 tables, where the column **rebasing** offset (global → table-local) for an
//! inner predicate, and the routing of an equi-key that connects **non-adjacent**
//! tables, are both general code that was previously untested past two tables. An
//! off-by-one in a cumulative offset, or a mis-routed key, returns *wrong rows*
//! — it does not crash, and an exec-model differential can't see it (both models
//! share the planner). So these assert the **exact** result set, hand-computed
//! from join semantics, independent of how the planner lowers the query.
//!
//! Schema (a small star): `fact(f_id, f_d1, f_d2)` joins `dim1` via `f_d1=a_id`
//! and `dim2` via `f_d2=b_id`. `fact.f_d2 = dim2.b_id` connects table 0 to table
//! 2 (non-adjacent — the star-schema case). Predicates on `dim1`/`dim2` push onto
//! the inner leaves and must be rebased by the cumulative column offset.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::storage::FileDiskManager;
use interchangedb::types::Value;

fn setup() -> (Session<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("t.db")).unwrap();
    let bpm = BufferPoolManager::new(256, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database = Arc::new(Database::open(dir.path(), engine).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    let mut s = Session::new(database, catalog);

    for ddl in [
        "CREATE TABLE fact (f_id INT PRIMARY KEY, f_d1 INT, f_d2 INT)",
        "CREATE TABLE dim1 (a_id INT PRIMARY KEY, a_val INT)",
        "CREATE TABLE dim2 (b_id INT PRIMARY KEY, b_val INT)",
    ] {
        s.execute(ddl).unwrap();
    }
    // fact rows: (f_id, f_d1→dim1, f_d2→dim2)
    for vals in [
        "(1, 10, 100)",
        "(2, 10, 200)",
        "(3, 20, 200)",
        "(4, 20, 300)",
    ] {
        s.execute(&format!("INSERT INTO fact VALUES {}", vals))
            .unwrap();
    }
    for vals in ["(10, 1)", "(20, 2)"] {
        s.execute(&format!("INSERT INTO dim1 VALUES {}", vals))
            .unwrap();
    }
    for vals in ["(100, 7)", "(200, 8)", "(300, 9)"] {
        s.execute(&format!("INSERT INTO dim2 VALUES {}", vals))
            .unwrap();
    }
    (s, dir)
}

/// Run a `SELECT f_id …` and return the f_id values, sorted (join output order
/// is unspecified, esp. for HashJoin).
fn select_ids(s: &mut Session<BTreeEngine>, sql: &str) -> Vec<i32> {
    let rows = match s.execute(sql).unwrap() {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {:?}", other),
    };
    let mut ids: Vec<i32> = rows
        .iter()
        .map(|r| match r[0] {
            Value::Int32(i) => i,
            ref v => panic!("expected Int32 f_id, got {:?}", v),
        })
        .collect();
    ids.sort();
    ids
}

fn explain(s: &mut Session<BTreeEngine>, sql: &str) -> String {
    match s.execute(&format!("EXPLAIN {}", sql)).unwrap() {
        QueryResult::Explain(text) => text,
        other => panic!("expected Explain, got {:?}", other),
    }
}

const STAR: &str = "SELECT f_id FROM fact, dim1, dim2 WHERE f_d1 = a_id AND f_d2 = b_id";

#[test]
fn three_way_join_no_predicate_baseline() {
    // All four fact rows have a matching dim1 (f_d1 ∈ {10,20}) and dim2
    // (f_d2 ∈ {100,200,300}), so the unfiltered 3-way join yields all four.
    let (mut s, _d) = setup();
    assert_eq!(select_ids(&mut s, STAR), vec![1, 2, 3, 4]);
}

#[test]
fn predicate_on_third_table_is_rebased_correctly() {
    // `b_val = 8` constrains dim2 (the 3rd table) → b_id = 200. It must be pushed
    // onto the dim2 inner leaf, rebased by offset = |fact| + |dim1| = 3 + 2 = 5
    // (b_val is global col 6 → local col 1). A wrong offset filters the wrong
    // column and changes the answer. fact rows with f_d2 = 200: {2, 3}.
    let (mut s, _d) = setup();
    assert_eq!(
        select_ids(&mut s, &format!("{STAR} AND b_val = 8")),
        vec![2, 3]
    );
}

#[test]
fn predicate_on_middle_table_is_rebased_correctly() {
    // `a_val = 2` constrains dim1 (middle table) → a_id = 20, rebased by offset
    // = |fact| = 3 (a_val global col 4 → local 1). fact rows with f_d1 = 20:
    // {3, 4}.
    let (mut s, _d) = setup();
    assert_eq!(
        select_ids(&mut s, &format!("{STAR} AND a_val = 2")),
        vec![3, 4]
    );
}

#[test]
fn predicate_on_left_table_in_multiway_join() {
    // `f_id = 2` constrains the left (fact) table — pushed onto the left leaf
    // (offset 0, no rebasing). Only fact row 2.
    let (mut s, _d) = setup();
    assert_eq!(select_ids(&mut s, &format!("{STAR} AND f_id = 2")), vec![2]);
}

#[test]
fn predicates_on_two_inner_tables_combined() {
    // Both inner predicates push simultaneously: a_val = 2 (f_d1 = 20) AND
    // b_val = 8 (f_d2 = 200). The only fact row with both is f_id = 3.
    let (mut s, _d) = setup();
    assert_eq!(
        select_ids(&mut s, &format!("{STAR} AND a_val = 2 AND b_val = 8")),
        vec![3]
    );
}

#[test]
fn non_adjacent_equi_key_promotes_and_filters() {
    // `f_d2 = b_id` connects fact (table 0) to dim2 (table 2) — non-adjacent.
    // Combined with the dim2 filter, this is the case most likely to break if
    // promotion routes the key to the wrong join. b_val = 7 → b_id = 100 →
    // fact f_d2 = 100 → only f_id 1.
    let (mut s, _d) = setup();
    assert_eq!(
        select_ids(&mut s, &format!("{STAR} AND b_val = 7")),
        vec![1]
    );
}

#[test]
fn star_join_plan_uses_two_hashjoins_no_cross_product() {
    // Confirm the rows above are correct *because the pushdown fired*, not by a
    // cross-product that happens to filter right: both joins lower to HashJoin
    // (equi-keys promoted), and there is no NestedLoopJoin (a cross product
    // renders as NLJ with no ON).
    let (mut s, _d) = setup();
    let plan = explain(&mut s, STAR);
    assert_eq!(
        plan.matches("HashJoin").count(),
        2,
        "expected two HashJoins in:\n{}",
        plan
    );
    assert!(
        !plan.contains("NestedLoopJoin"),
        "no cross-product/NLJ expected in:\n{}",
        plan
    );
}
