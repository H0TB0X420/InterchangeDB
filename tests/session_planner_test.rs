//! P14.14: runtime planner selection on a live `Session`.
//!
//! The `Planner` enum lets a session swap `PlannerStrategy` at runtime.
//! These tests drive the swap through the real `Session::execute` path —
//! the same SQL, planned by rule-based vs Selinger, observed via
//! `EXPLAIN`. Rule-based is the default; selecting Selinger changes the
//! join algorithm on an unindexed join (NestedLoopJoin → HashJoin)
//! without touching anything else.

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

fn setup() -> (Session<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database = Arc::new(Database::open(dir.path(), engine).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    let mut session = Session::new(database.clone(), catalog.clone());
    // Two tables joined on a non-PK column (no index on the join key), so
    // the planners diverge: rule-based → NestedLoopJoin, Selinger → Hash.
    session
        .execute("CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_name VARCHAR(20))")
        .unwrap();
    session
        .execute("CREATE TABLE district (d_id INT PRIMARY KEY, d_w_id INT, d_name VARCHAR(20))")
        .unwrap();
    (session, dir)
}

fn explain(session: &mut Session<BTreeEngine>, sql: &str) -> String {
    match session.execute(sql).unwrap() {
        QueryResult::Explain(text) => text,
        other => panic!("expected EXPLAIN, got {:?}", other),
    }
}

#[test]
fn default_planner_is_rule_based() {
    let (session, _d) = setup();
    assert_eq!(session.planner_name(), "rule-based");
}

#[test]
fn set_planner_switches_active_strategy_name() {
    let (mut session, _d) = setup();
    session.set_planner(Planner::Selinger(
        SelingerPlanner::<DefaultCostModel>::default(),
    ));
    assert_eq!(session.planner_name(), "selinger");
    session.set_planner(Planner::default());
    assert_eq!(session.planner_name(), "rule-based");
}

#[test]
fn swapping_planner_changes_the_join_algorithm() {
    let (mut session, _d) = setup();
    let sql = "EXPLAIN SELECT w_id, d_id FROM warehouse JOIN district ON w_id = d_w_id";

    // Default (rule-based) → NestedLoopJoin.
    let rule_plan = explain(&mut session, sql);
    assert!(
        rule_plan.contains("NestedLoopJoin"),
        "rule-based should use NestedLoopJoin, got:\n{}",
        rule_plan
    );

    // Swap to Selinger on the same live session → HashJoin.
    session.set_planner(Planner::Selinger(
        SelingerPlanner::<DefaultCostModel>::default(),
    ));
    let selinger_plan = explain(&mut session, sql);
    assert!(
        selinger_plan.contains("HashJoin"),
        "Selinger should use HashJoin, got:\n{}",
        selinger_plan
    );

    // Swap back → NestedLoopJoin again; the choice is per-statement.
    session.set_planner(Planner::default());
    let back = explain(&mut session, sql);
    assert!(back.contains("NestedLoopJoin"), "got:\n{}", back);
}

#[test]
fn non_select_statements_are_unaffected_by_planner_choice() {
    // The planner choice only touches SELECT join lowering; DML returns
    // identical results under either planner.
    let (mut session, _d) = setup();
    session
        .execute("INSERT INTO warehouse VALUES (1, 'DC1')")
        .unwrap();

    session.set_planner(Planner::Selinger(
        SelingerPlanner::<DefaultCostModel>::default(),
    ));
    let affected = match session
        .execute("INSERT INTO warehouse VALUES (2, 'DC2')")
        .unwrap()
    {
        QueryResult::Affected(n) => n,
        other => panic!("expected Affected, got {:?}", other),
    };
    assert_eq!(affected, 1);
}
