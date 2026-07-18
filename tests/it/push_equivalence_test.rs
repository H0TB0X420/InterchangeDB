//! Phase 15 / Increment 2b.5: Volcano vs. Push equivalence.
//!
//! The proof that the two `ExecutionModel`s are interchangeable. The same SQL,
//! run through pull (`Volcano`) and push (`Push`) on the same session, must
//! return byte-identical rows across the whole operator set — genuine push
//! pipelines (scan/filter/projection/limit) and the operators Push delegates
//! to the shared builder (sort, aggregate, join), plus their compositions.
//!
//! This is what makes the `PhysOp` + `ExecutionModel` seam from 2a/2b real:
//! one plan, either evaluation strategy, identical answers.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::ExecModel;
use interchangedb::session::{QueryResult, Session};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::types::Value;
use interchangedb::wal::SyncMode;
use interchangedb::Database;

/// Build a session over a fresh BTree-backed database seeded with two small
/// tables: `item(i_id, i_price)` (5 rows) and `stock(s_id, s_qty)` (3 rows).
fn setup() -> (Session<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database =
        Arc::new(Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    let mut session = Session::new(database, catalog);

    session
        .execute("CREATE TABLE item (i_id INT PRIMARY KEY, i_price BIGINT)")
        .unwrap();
    for (id, price) in [(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)] {
        session
            .execute(&format!("INSERT INTO item VALUES ({}, {})", id, price))
            .unwrap();
    }
    session
        .execute("CREATE TABLE stock (s_id INT PRIMARY KEY, s_qty BIGINT)")
        .unwrap();
    for (id, qty) in [(1, 10), (2, 20), (3, 30)] {
        session
            .execute(&format!("INSERT INTO stock VALUES ({}, {})", id, qty))
            .unwrap();
    }
    (session, dir)
}

fn select(session: &mut Session<BTreeEngine>, sql: &str) -> Vec<Vec<Value>> {
    match session.execute(sql).unwrap() {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows for `{}`, got {:?}", sql, other),
    }
}

/// Run `sql` through Volcano then Push; assert the row sets are identical.
fn assert_equivalent(session: &mut Session<BTreeEngine>, sql: &str) {
    session.set_execution_model(ExecModel::Volcano);
    let volcano = select(session, sql);
    session.set_execution_model(ExecModel::Push);
    let push = select(session, sql);
    session.set_execution_model(ExecModel::Volcano);
    assert_eq!(volcano, push, "Volcano vs Push diverged for: {}", sql);
}

#[test]
fn select_shapes_are_equivalent() {
    let (mut s, _dir) = setup();
    let queries = [
        "SELECT * FROM item",                                              // bare scan
        "SELECT i_id FROM item",                                           // projection
        "SELECT i_price FROM item WHERE i_id > 2",                         // filter (+ proj)
        "SELECT * FROM item LIMIT 3", // limit (streaming early-stop)
        "SELECT * FROM item ORDER BY i_price DESC", // sort (delegated)
        "SELECT i_id FROM item WHERE i_id > 1 ORDER BY i_id DESC LIMIT 2", // composition
        "SELECT COUNT(*) FROM item",  // aggregate (delegated)
        "SELECT SUM(i_price) FROM item",
        "SELECT MIN(i_price) FROM item",
        "SELECT MAX(i_price) FROM item",
    ];
    for q in queries {
        assert_equivalent(&mut s, q);
    }
}

// H1: grouped aggregation — Push runs it as a NATIVE sink (not the
// bridge), so equivalence here proves the push kernel against the pull
// operator: duplicate keys, a NULL-key group, HAVING, ORDER BY over
// aggregate output, and every aggregate function. Both models emit
// canonical key order, so `assert_equivalent`'s raw (unsorted)
// comparison is exact — order divergence fails, by design.
#[test]
fn grouped_aggregate_is_equivalent() {
    let (mut s, _dir) = setup();
    s.execute("CREATE TABLE ev (e_id INT PRIMARY KEY, kind VARCHAR(8), amt BIGINT)")
        .unwrap();
    for (id, kind, amt) in [
        (1, "'a'", 10),
        (2, "'b'", 20),
        (3, "'a'", 30),
        (4, "'b'", 40),
        (5, "'c'", 50),
        (6, "NULL", 60),
        (7, "NULL", 7),
    ] {
        s.execute(&format!(
            "INSERT INTO ev VALUES ({}, {}, {})",
            id, kind, amt
        ))
        .unwrap();
    }
    let queries = [
        "SELECT kind, COUNT(*), SUM(amt) FROM ev GROUP BY kind",
        "SELECT kind, SUM(amt) FROM ev GROUP BY kind HAVING SUM(amt) > 40",
        "SELECT kind, COUNT(*) FROM ev GROUP BY kind ORDER BY COUNT(*) DESC, kind ASC",
        "SELECT kind, MIN(amt), MAX(amt), AVG(amt) FROM ev GROUP BY kind",
        "SELECT kind, COUNT(*) FROM ev WHERE amt > 15 GROUP BY kind",
    ];
    for q in queries {
        assert_equivalent(&mut s, q);
    }
}

#[test]
fn join_is_equivalent() {
    let (mut s, _dir) = setup();
    // item ⨝ stock on id — both models delegate the join to `build_executor`,
    // so this confirms the delegation path round-trips through Push correctly.
    assert_equivalent(
        &mut s,
        "SELECT a.i_id, b.s_qty FROM item a JOIN stock b ON a.i_id = b.s_id",
    );
}

#[test]
fn dml_round_trips_through_push() {
    // DML delegates to the shared operator builder, so running it under Push
    // mutates exactly as under Volcano. Apply an UPDATE in Push mode, then
    // verify the change is visible (and that a SELECT agrees across models).
    let (mut s, _dir) = setup();
    s.set_execution_model(ExecModel::Push);
    match s
        .execute("UPDATE item SET i_price = 999 WHERE i_id = 1")
        .unwrap()
    {
        QueryResult::Affected(n) => assert_eq!(n, 1),
        other => panic!("expected affected count, got {:?}", other),
    }
    assert_equivalent(&mut s, "SELECT i_price FROM item WHERE i_id = 1");
    // The push-mode UPDATE took effect.
    assert_eq!(
        select(&mut s, "SELECT i_price FROM item WHERE i_id = 1"),
        vec![vec![Value::Int64(999)]]
    );
}
