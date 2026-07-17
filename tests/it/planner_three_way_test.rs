//! Phase 17 gates G2 / G4 / G5 — three-way planner differential.
//!
//! **G2 (results parity)**: a fixed SELECT corpus runs under rule-based,
//! selinger, and volcano-memo; sorted result rows must be identical.
//! The corpus covers the selinger-reorder queries (including the
//! WHERE-driven O10 case), single-table access-path shapes, the
//! aggregate/ORDER BY/LIMIT spine, comma-join promotion, a cross-table
//! non-equi residual, the mixed-type join-differential key sets (NULL
//! keys, out-of-i32-range Int64), and a non-equi ON that exercises the
//! memo's D8 fallback end to end.
//!
//! **G4 (determinism, D10)**: volcano-memo EXPLAINs every corpus query
//! twice — the strings must be identical (the `BaselineCapable` gate).
//!
//! **G5 (planning time, D11)**: volcano-memo plans every corpus query
//! within a generous 50ms debug-build bound (EXPLAIN plans without
//! executing, so it isolates planning).

use std::sync::Arc;
use std::time::Instant;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, IndexBackend, IndexDef};
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::sql::{Planner, RuleBasedPlanner, SelingerPlanner, VolcanoPlanner};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::types::Value;
use interchangedb::wal::SyncMode;

fn setup() -> (Session<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database =
        Arc::new(Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap());
    let catalog = Arc::new(
        Catalog::open_persistent(database.engine_arc().clone(), dir.path().join("indexes"))
            .unwrap(),
    );
    let mut session = Session::new(database.clone(), catalog.clone());

    // Reorder chain a(2 rows) — b(4) — c(8), the selinger_reorder_test
    // fixture: big-table-first textual order is the expensive one.
    session
        .execute("CREATE TABLE a (a_id INT PRIMARY KEY, a_val INT)")
        .unwrap();
    session
        .execute("CREATE TABLE b (b_id INT PRIMARY KEY, b_a INT, b_val INT)")
        .unwrap();
    session
        .execute("CREATE TABLE c (c_id INT PRIMARY KEY, c_b INT, c_val INT)")
        .unwrap();
    for a_id in 1..=2 {
        session
            .execute(&format!("INSERT INTO a VALUES ({}, {})", a_id, a_id * 100))
            .unwrap();
    }
    for b_id in 1..=4 {
        let b_a = (b_id - 1) % 2 + 1;
        session
            .execute(&format!(
                "INSERT INTO b VALUES ({}, {}, {})",
                b_id,
                b_a,
                b_id * 10
            ))
            .unwrap();
    }
    for c_id in 1..=8 {
        let c_b = (c_id - 1) % 4 + 1;
        session
            .execute(&format!(
                "INSERT INTO c VALUES ({}, {}, {})",
                c_id, c_b, c_id
            ))
            .unwrap();
    }

    // Join-differential pair (E11 key sets): ja carries a nullable
    // BIGINT key, jb an indexed INT key — mixed-type equality, NULL
    // exclusion, out-of-i32-range exclusion, unpartnered keys per side.
    session
        .execute("CREATE TABLE ja (ja_id INT PRIMARY KEY, ja_key BIGINT)")
        .unwrap();
    session
        .execute("CREATE TABLE jb (jb_id INT PRIMARY KEY, jb_key INT)")
        .unwrap();
    let jb_table_id = catalog.get_table("jb").unwrap().table_id;
    catalog
        .create_index(IndexDef {
            name: "jb_by_key".into(),
            table_id: jb_table_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();
    for sql in [
        "INSERT INTO ja VALUES (1, 1)",
        "INSERT INTO ja VALUES (2, 2)",
        "INSERT INTO ja VALUES (3, NULL)",
        "INSERT INTO ja VALUES (4, 5000000000)",
        "INSERT INTO ja VALUES (5, 9)",
        "INSERT INTO jb VALUES (1, 1)",
        "INSERT INTO jb VALUES (2, 2)",
        "INSERT INTO jb VALUES (3, 2)",
        "INSERT INTO jb VALUES (4, 7)",
    ] {
        session.execute(sql).unwrap();
    }

    for table in ["a", "b", "c", "ja", "jb"] {
        session.execute(&format!("ANALYZE TABLE {table}")).unwrap();
    }
    (session, dir)
}

/// The G2 corpus: every query must return identical sorted rows under
/// all three planners.
const CORPUS: &[&str] = &[
    // selinger_reorder_test queries — worst textual order + WHERE-driven.
    "SELECT c_val, b_val, a_val FROM c JOIN b ON c_b = b_id JOIN a ON b_a = a_id",
    "SELECT c_val, b_val, a_val FROM a JOIN b ON b_a = a_id JOIN c ON c_b = b_id WHERE c_val = 1",
    // Single-table access-path shapes.
    "SELECT * FROM a",
    "SELECT a_val FROM a WHERE a_id = 1",
    "SELECT b_val FROM b WHERE b_a = 2",
    "SELECT c_val FROM c ORDER BY c_val DESC LIMIT 3",
    "SELECT COUNT(*) FROM c WHERE c_b = 1",
    "SELECT SUM(c_val) FROM c",
    // Comma-join promotion + right-table pushdown.
    "SELECT a_val, b_val FROM a, b WHERE b_a = a_id AND b_val = 20",
    // Cross-table non-equi residual above the join tree.
    "SELECT a_val, c_val FROM a JOIN b ON b_a = a_id JOIN c ON c_b = b_id WHERE c_val < a_val",
    // Mixed-type equi-join (Int64 vs indexed Int32): NULL never matches,
    // 5e9 exceeds i32, 9 and 7 are unpartnered.
    "SELECT ja_id, jb_id FROM ja JOIN jb ON ja_key = jb_key",
    // Non-equi ON → the memo's D8 fallback path, end to end.
    "SELECT a_val, b_val FROM a JOIN b ON b_a < a_id",
];

fn rows(session: &mut Session<BTreeEngine>, sql: &str) -> Vec<Vec<Value>> {
    match session.execute(sql).unwrap() {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows for {sql}, got {other:?}"),
    }
}

fn sorted(mut rs: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rs.sort_by(|x, y| format!("{x:?}").cmp(&format!("{y:?}")));
    rs
}

fn explain(session: &mut Session<BTreeEngine>, sql: &str) -> String {
    match session.execute(&format!("EXPLAIN {sql}")).unwrap() {
        QueryResult::Explain(text) => text,
        other => panic!("expected Explain for {sql}, got {other:?}"),
    }
}

#[test]
fn g2_results_identical_across_planners() {
    let (mut session, _dir) = setup();
    for sql in CORPUS {
        session.set_planner(Planner::RuleBased(RuleBasedPlanner));
        let rule = sorted(rows(&mut session, sql));
        session.set_planner(Planner::Selinger(SelingerPlanner::default()));
        let selinger = sorted(rows(&mut session, sql));
        session.set_planner(Planner::VolcanoMemo(VolcanoPlanner::default()));
        let memo = sorted(rows(&mut session, sql));

        assert_eq!(selinger, rule, "selinger diverged on: {sql}");
        assert_eq!(memo, rule, "volcano-memo diverged on: {sql}");
    }
}

#[test]
fn g4_volcano_memo_explain_is_deterministic() {
    let (mut session, _dir) = setup();
    session.set_planner(Planner::VolcanoMemo(VolcanoPlanner::default()));
    for sql in CORPUS {
        let first = explain(&mut session, sql);
        let second = explain(&mut session, sql);
        assert_eq!(first, second, "nondeterministic EXPLAIN for: {sql}");
    }
}

#[test]
fn g5_volcano_memo_plans_within_debug_bound() {
    let (mut session, _dir) = setup();
    session.set_planner(Planner::VolcanoMemo(VolcanoPlanner::default()));
    for sql in CORPUS {
        let start = Instant::now();
        let _ = explain(&mut session, sql);
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_millis() < 50,
            "planning `{sql}` took {elapsed:?} (bound: 50ms debug)"
        );
    }
}

#[test]
fn volcano_memo_name_is_distinct() {
    let (mut session, _dir) = setup();
    session.set_planner(Planner::VolcanoMemo(VolcanoPlanner::default()));
    assert_eq!(session.planner_name(), "volcano-memo");
}
