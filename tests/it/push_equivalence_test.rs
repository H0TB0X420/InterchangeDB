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
        // H2b: an ungrouped computed display runs as the native ComputeSink
        // (a bare column mixed with an arithmetic item) — prove it against
        // Volcano's Compute operator.
        "SELECT i_id, i_price * 2 FROM item WHERE i_id > 1",
        // H4a: a derived table (FROM-subquery). Push bridges the DerivedScan
        // leaf through `ExecutorSource` (no native push sink), so its output
        // must match Volcano's materialized DerivedScan row-for-row — including
        // the outer WHERE filtering the derived output.
        "SELECT id2, p2 FROM (SELECT i_id, i_price FROM item) AS d (id2, p2) WHERE p2 > 200",
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
        // H2a: aggregates over arithmetic expressions run through the same
        // native push sink; the compiled arg closure must agree with pull.
        "SELECT kind, SUM(amt * 2) FROM ev GROUP BY kind",
        "SELECT kind, MIN(amt + 1), AVG(amt) FROM ev GROUP BY kind",
        // H2b: free display order — a Compute reorders the grouped push
        // sink's output (native aggregate feeding the native ComputeSink).
        "SELECT COUNT(*), kind FROM ev GROUP BY kind",
        // H3.3: a searched CASE inside a grouped aggregate (the Q12 shape) —
        // the branch predicates and results compile once and run through the
        // same native push sink; this pins the CASE closure against pull.
        "SELECT kind, SUM(CASE WHEN amt > 30 THEN 1 ELSE 0 END) FROM ev GROUP BY kind",
    ];
    for q in queries {
        assert_equivalent(&mut s, q);
    }

    // H2b: TPC-H Q14's post-aggregate arithmetic over a grouped push
    // aggregate — the native grouped sink feeds the native ComputeSink, so
    // this proves BOTH push kernels against Volcano on the Q14 shape. Both the
    // parenthesized and the VERBATIM left-associative forms are checked; H3.1
    // made Div native max-scale, so `100.00 * SUM(x) / SUM(y)` — scale4 ÷
    // scale2 → scale4 — now runs and equals the parenthesized form.
    s.execute(
        "CREATE TABLE q14 (q_id INT PRIMARY KEY, grp VARCHAR(8), x DECIMAL(12,2), y DECIMAL(12,2))",
    )
    .unwrap();
    for (id, grp, x, y) in [
        (1, "'a'", "10.00", "40.00"),
        (2, "'a'", "30.00", "60.00"),
        (3, "'b'", "25.00", "50.00"),
        (4, "'b'", "25.00", "50.00"),
    ] {
        s.execute(&format!(
            "INSERT INTO q14 VALUES ({}, {}, {}, {})",
            id, grp, x, y
        ))
        .unwrap();
    }
    assert_equivalent(
        &mut s,
        "SELECT grp, 100.00 * (SUM(x) / SUM(y)) FROM q14 GROUP BY grp",
    );
    assert_equivalent(
        &mut s,
        "SELECT grp, 100.00 * SUM(x) / SUM(y) FROM q14 GROUP BY grp",
    );
}

// H3.2: a DATE query — range predicate over a folded INTERVAL literal, plus
// EXTRACT(YEAR) in the display — runs identically under both models. The date
// filter and the ExtractYear Compute both flow through the shared operator
// build, so this pins Push against Volcano on the new date surface.
#[test]
fn date_query_is_equivalent() {
    let (mut s, _dir) = setup();
    s.execute("CREATE TABLE dorders (d_id INT PRIMARY KEY, d_date DATE)")
        .unwrap();
    for sql in [
        "INSERT INTO dorders VALUES (1, DATE '1993-06-15')",
        "INSERT INTO dorders VALUES (2, DATE '1994-01-01')",
        "INSERT INTO dorders VALUES (3, DATE '1994-07-04')",
        "INSERT INTO dorders VALUES (4, DATE '1998-09-02')",
    ] {
        s.execute(sql).unwrap();
    }
    assert_equivalent(
        &mut s,
        "SELECT d_id, EXTRACT(YEAR FROM d_date) FROM dorders \
         WHERE d_date <= DATE '1994-12-01' - INTERVAL '90' DAY ORDER BY d_id",
    );
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
    // H3b LEFT OUTER: items 4 and 5 have no stock row (stock has ids 1..3), so
    // they emit padded with NULL s_qty. The outer join bridges through the
    // same ExecutorSource, so Push must reproduce the pads byte-for-byte.
    assert_equivalent(
        &mut s,
        "SELECT a.i_id, b.s_qty FROM item a LEFT OUTER JOIN stock b ON a.i_id = b.s_id",
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
