//! P14.13b: end-to-end Selinger join reordering.
//!
//! A 3-table chain `a — b — c` is queried in the *worst* textual order
//! (big table first). With stats populated by `ANALYZE`, the Selinger
//! planner finds the cheaper small-table-first order and rewrites the
//! plan into it, remapping every column reference via `ColumnRemap`.
//!
//! The load-bearing assertion is **correctness under reordering**: the
//! Selinger result must equal the rule-based result row-for-row (column
//! order preserved), proving the remap is right. A second assertion shows
//! the plans actually differ (the reorder happened).

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::sql::cost::DefaultCostModel;
use interchangedb::sql::selinger::SelingerPlanner;
use interchangedb::sql::Planner;
use interchangedb::storage::FileDiskManager;
use interchangedb::types::Value;

fn setup() -> (Session<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database = Arc::new(Database::open(dir.path(), engine).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    let mut session = Session::new(database.clone(), catalog.clone());

    // a (2 rows) — b (4 rows, b_a → a_id) — c (8 rows, c_b → b_id).
    session.execute("CREATE TABLE a (a_id INT PRIMARY KEY, a_val INT)").unwrap();
    session.execute("CREATE TABLE b (b_id INT PRIMARY KEY, b_a INT, b_val INT)").unwrap();
    session.execute("CREATE TABLE c (c_id INT PRIMARY KEY, c_b INT, c_val INT)").unwrap();

    for a_id in 1..=2 {
        session
            .execute(&format!("INSERT INTO a VALUES ({}, {})", a_id, a_id * 100))
            .unwrap();
    }
    // 4 b rows, two per a.
    for b_id in 1..=4 {
        let b_a = (b_id - 1) % 2 + 1; // 1,2,1,2
        session
            .execute(&format!("INSERT INTO b VALUES ({}, {}, {})", b_id, b_a, b_id * 10))
            .unwrap();
    }
    // 8 c rows, two per b.
    for c_id in 1..=8 {
        let c_b = (c_id - 1) % 4 + 1; // 1..4 cycling
        session
            .execute(&format!("INSERT INTO c VALUES ({}, {}, {})", c_id, c_b, c_id))
            .unwrap();
    }

    session.execute("ANALYZE TABLE a").unwrap();
    session.execute("ANALYZE TABLE b").unwrap();
    session.execute("ANALYZE TABLE c").unwrap();
    (session, dir)
}

// The query lists the big table (c) first textually, so the textual
// left-deep order is the expensive one; Selinger should reverse it.
const QUERY: &str = "SELECT c_val, b_val, a_val FROM c \
                     JOIN b ON c_b = b_id \
                     JOIN a ON b_a = a_id";

fn rows(session: &mut Session<BTreeEngine>, sql: &str) -> Vec<Vec<Value>> {
    match session.execute(sql).unwrap() {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {:?}", other),
    }
}

fn sorted(mut rs: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rs.sort_by(|x, y| format!("{:?}", x).cmp(&format!("{:?}", y)));
    rs
}

fn explain(session: &mut Session<BTreeEngine>, sql: &str) -> String {
    match session.execute(&format!("EXPLAIN {}", sql)).unwrap() {
        QueryResult::Explain(text) => text,
        other => panic!("expected EXPLAIN, got {:?}", other),
    }
}

#[test]
fn selinger_reordered_join_matches_rule_based_results() {
    let (mut session, _d) = setup();

    // Rule-based (default) result — textual order, the reference.
    let rule_rows = sorted(rows(&mut session, QUERY));
    assert!(!rule_rows.is_empty(), "query should return rows");
    // Each row is (c_val, b_val, a_val) — three columns, in SELECT order.
    assert_eq!(rule_rows[0].len(), 3);

    // Selinger result — reordered + remapped — must match exactly.
    session.set_planner(Planner::Selinger(SelingerPlanner::<DefaultCostModel>::default()));
    let selinger_rows = sorted(rows(&mut session, QUERY));

    assert_eq!(
        selinger_rows, rule_rows,
        "Selinger reordered result must equal rule-based result (column order preserved)"
    );
}

#[test]
fn selinger_actually_reorders_the_plan() {
    let (mut session, _d) = setup();

    // Rule-based keeps textual order: the big table c is the base scan.
    let rule_plan = explain(&mut session, QUERY);
    assert!(
        rule_plan.contains("SeqScan(c)"),
        "rule-based should scan c (textual base), got:\n{}",
        rule_plan
    );

    // Selinger reorders to small-table-first: a becomes the base scan,
    // and the plan differs from rule-based.
    session.set_planner(Planner::Selinger(SelingerPlanner::<DefaultCostModel>::default()));
    let selinger_plan = explain(&mut session, QUERY);
    assert_ne!(
        selinger_plan, rule_plan,
        "Selinger plan should differ from rule-based (reorder happened)"
    );
    assert!(
        selinger_plan.contains("SeqScan(a)"),
        "Selinger should scan small table a first, got:\n{}",
        selinger_plan
    );
}

#[test]
fn selinger_preserves_output_column_order() {
    // The SELECT is `c_val, b_val, a_val`. After reordering the *join*,
    // the projection must still emit (c_val, b_val, a_val) per row — the
    // values must line up with their column meaning, not their physical
    // position. Check a known row: c_val=1 (c_id 1) → c_b=1 → b_id=1,
    // b_val=10, b_a=1 → a_id=1, a_val=100.
    let (mut session, _d) = setup();
    session.set_planner(Planner::Selinger(SelingerPlanner::<DefaultCostModel>::default()));
    let result = rows(&mut session, QUERY);
    let row_for_c1 = result
        .iter()
        .find(|r| r[0] == Value::Int32(1))
        .expect("c_val=1 row present");
    assert_eq!(row_for_c1[0], Value::Int32(1), "c_val");
    assert_eq!(row_for_c1[1], Value::Int32(10), "b_val");
    assert_eq!(row_for_c1[2], Value::Int32(100), "a_val");
}
