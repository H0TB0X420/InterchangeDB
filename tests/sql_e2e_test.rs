//! Phase 11 end-to-end: SQL strings → execute → verify.
//!
//! Drives the full stack through `Session::execute`:
//!
//!   SQL string → parse → bind → plan → operator tree → TxnEngine →
//!   Table → BTreeEngine → BPM → FileDiskManager
//!
//! Each test rehearses a TPC-C-shaped flow with real SQL syntax.
//! Workload log is enabled where relevant so we also verify the
//! capture-everything contract.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::common::Error;
use interchangedb::database::Database;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::sql::workload_log::WorkloadLog;
use interchangedb::storage::FileDiskManager;
use interchangedb::types::Value;

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct Env {
    session: Session<BTreeEngine>,
    database: Arc<Database<BTreeEngine>>,
    catalog: Arc<Catalog<BTreeEngine>>,
    _dir: TempDir,
}

fn setup() -> Env {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let dm = FileDiskManager::create(&db_path).unwrap();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database = Arc::new(Database::open(dir.path(), engine).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    let session = Session::new(database.clone(), catalog.clone());
    Env {
        session,
        database,
        catalog,
        _dir: dir,
    }
}

fn setup_with_log() -> (Env, std::path::PathBuf) {
    let mut env = setup();
    let log_path = env._dir.path().join("workload.jsonl");
    let log = Arc::new(WorkloadLog::open(&log_path).unwrap());
    env.session = Session::new(env.database.clone(), env.catalog.clone()).with_log(log);
    (env, log_path)
}

fn affected(r: QueryResult) -> u64 {
    match r {
        QueryResult::Affected(n) => n,
        other => panic!("expected Affected, got {:?}", other),
    }
}

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// TPC-C-shaped scenarios
// ---------------------------------------------------------------------------

#[test]
fn tpcc_payment_shape_via_sql() {
    // Mirrors TPC-C Payment txn against a simplified warehouse:
    //   1. Create warehouse table
    //   2. Seed two warehouses
    //   3. BEGIN
    //   4. UPDATE warehouse SET w_ytd = w_ytd + 100 WHERE w_id = 1
    //   5. UPDATE warehouse SET w_ytd = w_ytd + 50  WHERE w_id = 2   (same txn)
    //   6. COMMIT
    //   7. SELECT — both updates visible.
    let mut env = setup();

    env.session
        .execute(
            "CREATE TABLE warehouse (\
                w_id INT NOT NULL, \
                w_ytd BIGINT NOT NULL, \
                w_name VARCHAR(10) NOT NULL, \
                PRIMARY KEY (w_id))",
        )
        .unwrap();

    let n = affected(
        env.session
            .execute("INSERT INTO warehouse VALUES (1, 1000, 'north'), (2, 2000, 'south')")
            .unwrap(),
    );
    assert_eq!(n, 2);

    env.session.execute("BEGIN").unwrap();
    assert_eq!(
        affected(
            env.session
                .execute("UPDATE warehouse SET w_ytd = w_ytd + 100 WHERE w_id = 1")
                .unwrap()
        ),
        1
    );
    assert_eq!(
        affected(
            env.session
                .execute("UPDATE warehouse SET w_ytd = w_ytd + 50 WHERE w_id = 2")
                .unwrap()
        ),
        1
    );
    env.session.execute("COMMIT").unwrap();

    // Verify post-commit state.
    let r1 = rows(
        env.session
            .execute("SELECT w_ytd FROM warehouse WHERE w_id = 1")
            .unwrap(),
    );
    assert_eq!(r1[0][0], Value::Int64(1100));
    let r2 = rows(
        env.session
            .execute("SELECT w_ytd FROM warehouse WHERE w_id = 2")
            .unwrap(),
    );
    assert_eq!(r2[0][0], Value::Int64(2050));
}

#[test]
fn tpcc_new_order_inserts_multiple_rows() {
    // TPC-C NewOrder inserts ~10 order_line rows per order. Batch insert
    // semantics + post-insert visibility.
    let mut env = setup();
    env.session
        .execute(
            "CREATE TABLE order_line (\
                ol_id INT NOT NULL, \
                ol_o_id INT NOT NULL, \
                ol_quantity INT NOT NULL, \
                ol_amount BIGINT NOT NULL, \
                PRIMARY KEY (ol_id))",
        )
        .unwrap();

    let n = affected(
        env.session
            .execute(
                "INSERT INTO order_line VALUES \
                    (1, 100, 5, 500), \
                    (2, 100, 3, 300), \
                    (3, 100, 7, 700), \
                    (4, 100, 1, 100), \
                    (5, 100, 9, 900)",
            )
            .unwrap(),
    );
    assert_eq!(n, 5);

    // Read them all back.
    let r = rows(env.session.execute("SELECT * FROM order_line").unwrap());
    assert_eq!(r.len(), 5);
    // Verify last row's amount.
    let r5 = rows(
        env.session
            .execute("SELECT ol_amount FROM order_line WHERE ol_id = 5")
            .unwrap(),
    );
    assert_eq!(r5[0][0], Value::Int64(900));
}

#[test]
fn tpcc_order_status_point_lookup() {
    // TPC-C OrderStatus is a PK lookup on customer + scan of order_lines.
    let mut env = setup();
    env.session
        .execute(
            "CREATE TABLE customer (\
                c_id INT NOT NULL, \
                c_balance BIGINT NOT NULL, \
                c_first VARCHAR(16) NOT NULL, \
                PRIMARY KEY (c_id))",
        )
        .unwrap();
    env.session
        .execute(
            "INSERT INTO customer VALUES (1, 100, 'alice'), (2, 200, 'bob'), (3, 300, 'carol')",
        )
        .unwrap();

    let r = rows(
        env.session
            .execute("SELECT c_first, c_balance FROM customer WHERE c_id = 2")
            .unwrap(),
    );
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Varchar("bob".into()));
    assert_eq!(r[0][1], Value::Int64(200));
}

#[test]
fn tpcc_delivery_delete_and_update() {
    // TPC-C Delivery DELETEs from new_order and UPDATEs orders. Compose
    // both DML shapes within one transaction.
    let mut env = setup();
    env.session
        .execute(
            "CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, PRIMARY KEY (no_o_id))",
        )
        .unwrap();
    env.session
        .execute(
            "CREATE TABLE orders (o_id INT NOT NULL, o_carrier_id INT NOT NULL, PRIMARY KEY (o_id))",
        )
        .unwrap();
    env.session
        .execute("INSERT INTO new_order VALUES (1, 1), (2, 1), (3, 1)")
        .unwrap();
    env.session
        .execute("INSERT INTO orders VALUES (1, 0), (2, 0), (3, 0)")
        .unwrap();

    env.session.execute("BEGIN").unwrap();
    let d = affected(
        env.session
            .execute("DELETE FROM new_order WHERE no_o_id = 1")
            .unwrap(),
    );
    assert_eq!(d, 1);
    let u = affected(
        env.session
            .execute("UPDATE orders SET o_carrier_id = 7 WHERE o_id = 1")
            .unwrap(),
    );
    assert_eq!(u, 1);
    env.session.execute("COMMIT").unwrap();

    // Post-commit: new_order no longer has row 1, orders.o_carrier_id = 7.
    let r = rows(
        env.session
            .execute("SELECT * FROM new_order WHERE no_o_id = 1")
            .unwrap(),
    );
    assert!(r.is_empty());
    let o = rows(
        env.session
            .execute("SELECT o_carrier_id FROM orders WHERE o_id = 1")
            .unwrap(),
    );
    assert_eq!(o[0][0], Value::Int32(7));
}

// ---------------------------------------------------------------------------
// Transaction lifecycle
// ---------------------------------------------------------------------------

#[test]
fn rollback_via_sql_discards_all_writes() {
    let mut env = setup();
    env.session
        .execute("CREATE TABLE t (id INT NOT NULL, n BIGINT NOT NULL, PRIMARY KEY (id))")
        .unwrap();

    env.session.execute("BEGIN").unwrap();
    env.session
        .execute("INSERT INTO t VALUES (1, 100), (2, 200)")
        .unwrap();
    env.session
        .execute("UPDATE t SET n = 999 WHERE id = 1")
        .unwrap();
    env.session.execute("ROLLBACK").unwrap();

    // Both the insert and the update vanish.
    let r = rows(env.session.execute("SELECT * FROM t").unwrap());
    assert!(r.is_empty(), "ROLLBACK must discard all writes in the txn");
}

#[test]
fn read_your_own_writes_within_explicit_txn() {
    // TPC-C NewOrder reads its own writes within the same txn.
    let mut env = setup();
    env.session
        .execute("CREATE TABLE t (id INT NOT NULL, n BIGINT NOT NULL, PRIMARY KEY (id))")
        .unwrap();

    env.session.execute("BEGIN").unwrap();
    env.session
        .execute("INSERT INTO t VALUES (1, 100)")
        .unwrap();
    // Inside the same txn, the insert is visible.
    let r = rows(env.session.execute("SELECT n FROM t WHERE id = 1").unwrap());
    assert_eq!(r[0][0], Value::Int64(100));
    env.session
        .execute("UPDATE t SET n = 200 WHERE id = 1")
        .unwrap();
    let r2 = rows(env.session.execute("SELECT n FROM t WHERE id = 1").unwrap());
    assert_eq!(r2[0][0], Value::Int64(200));
    env.session.execute("COMMIT").unwrap();
}

// ---------------------------------------------------------------------------
// Two-session snapshot isolation
// ---------------------------------------------------------------------------

#[test]
fn snapshot_isolation_across_sessions() {
    // Two sessions share Database+Catalog. T1 begins, T2 begins+commits a
    // write, T1's snapshot doesn't see it.
    let env = setup();
    let mut s1 = Session::new(env.database.clone(), env.catalog.clone());
    let mut s2 = Session::new(env.database.clone(), env.catalog.clone());

    s1.execute("CREATE TABLE t (id INT NOT NULL, n BIGINT NOT NULL, PRIMARY KEY (id))")
        .unwrap();
    s1.execute("INSERT INTO t VALUES (1, 1000)").unwrap();

    s1.execute("BEGIN").unwrap();
    // s1 reads 1000 inside its txn (snapshot fixed here).
    let pre = rows(s1.execute("SELECT n FROM t WHERE id = 1").unwrap());
    assert_eq!(pre[0][0], Value::Int64(1000));

    // s2 commits a write.
    s2.execute("UPDATE t SET n = 9999 WHERE id = 1").unwrap();

    // s1's next read inside the txn still sees the snapshot value.
    let mid = rows(s1.execute("SELECT n FROM t WHERE id = 1").unwrap());
    assert_eq!(
        mid[0][0],
        Value::Int64(1000),
        "s1's snapshot must not see s2's post-begin commit"
    );
    s1.execute("COMMIT").unwrap();

    // After s1 commits, a fresh read sees the latest value.
    let post = rows(s1.execute("SELECT n FROM t WHERE id = 1").unwrap());
    assert_eq!(post[0][0], Value::Int64(9999));
}

// ---------------------------------------------------------------------------
// EXPLAIN
// ---------------------------------------------------------------------------

#[test]
fn explain_select_with_where_renders_tree() {
    let mut env = setup();
    env.session
        .execute("CREATE TABLE t (id INT NOT NULL, n BIGINT NOT NULL, PRIMARY KEY (id))")
        .unwrap();
    let r = env
        .session
        .execute("EXPLAIN SELECT n FROM t WHERE id = 1")
        .unwrap();
    match r {
        QueryResult::Explain(tree) => {
            assert!(tree.contains("Projection"));
            assert!(tree.contains("Filter"));
            assert!(tree.contains("SeqScan(t)"));
        }
        other => panic!("expected Explain, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// Workload log integration
// ---------------------------------------------------------------------------

#[test]
fn workload_log_captures_all_executed_sql() {
    let (mut env, log_path) = setup_with_log();
    env.session
        .execute("CREATE TABLE t (id INT NOT NULL, PRIMARY KEY (id))")
        .unwrap();
    env.session.execute("INSERT INTO t VALUES (1)").unwrap();
    env.session.execute("SELECT * FROM t").unwrap();
    env.session.execute("BEGIN").unwrap();
    env.session.execute("INSERT INTO t VALUES (2)").unwrap();
    env.session.execute("COMMIT").unwrap();
    // A statement that fails to parse — log should still capture it.
    let _ = env.session.execute("THIS IS NOT SQL");

    let lines: Vec<String> = BufReader::new(File::open(&log_path).unwrap())
        .lines()
        .map(|l| l.unwrap())
        .collect();
    assert_eq!(lines.len(), 7);
    // Ids monotonically increase from 1.
    for (i, l) in lines.iter().enumerate() {
        let parsed: serde_json::Value = serde_json::from_str(l).unwrap();
        assert_eq!(parsed["id"].as_u64().unwrap(), (i as u64) + 1);
    }
    // Last entry was the bad SQL — captured even though it failed.
    assert!(lines[6].contains("THIS IS NOT SQL"));
}

// ---------------------------------------------------------------------------
// Error surface
// ---------------------------------------------------------------------------

#[test]
fn int32_column_increment_via_sql() {
    // TPC-C NewOrder bumps `d_next_o_id` which is INT. Tests that the
    // recursive narrow_expression pass handles `col = col + 1` on Int32:
    // both operands must narrow to Int32 so the result matches the
    // column's type at write time.
    let mut env = setup();
    env.session
        .execute(
            "CREATE TABLE district (\
                d_id INT NOT NULL, \
                d_next_o_id INT NOT NULL, \
                PRIMARY KEY (d_id))",
        )
        .unwrap();
    env.session
        .execute("INSERT INTO district VALUES (1, 3001)")
        .unwrap();

    affected(
        env.session
            .execute("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = 1")
            .unwrap(),
    );
    affected(
        env.session
            .execute("UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = 1")
            .unwrap(),
    );

    let r = rows(
        env.session
            .execute("SELECT d_next_o_id FROM district WHERE d_id = 1")
            .unwrap(),
    );
    assert_eq!(r[0][0], Value::Int32(3003));
}

#[test]
fn write_conflict_surfaces_through_session() {
    // TPC-C concurrency: two sessions both update the same row. The
    // second-to-commit gets WriteConflict (first-committer-wins).
    let env = setup();
    let mut s1 = Session::new(env.database.clone(), env.catalog.clone());
    let mut s2 = Session::new(env.database.clone(), env.catalog.clone());

    s1.execute("CREATE TABLE t (id INT NOT NULL, n BIGINT NOT NULL, PRIMARY KEY (id))")
        .unwrap();
    s1.execute("INSERT INTO t VALUES (1, 1000)").unwrap();

    s1.execute("BEGIN").unwrap();
    s2.execute("BEGIN").unwrap();

    // Both txns try to update — the second-to-write hits the X-lock first.
    s1.execute("UPDATE t SET n = 1100 WHERE id = 1").unwrap();
    s1.execute("COMMIT").unwrap();

    // s2 now tries to update the same row. Its snapshot precedes s1's
    // commit, so SI first-committer-wins kicks in.
    let result = s2.execute("UPDATE t SET n = 1200 WHERE id = 1");
    match result {
        Err(Error::WriteConflict { .. }) => {}
        Err(Error::Deadlock(_)) | Err(Error::LockTimeout) => {}
        other => panic!("expected WriteConflict/Deadlock, got {:?}", other),
    }
    s2.execute("ROLLBACK").unwrap();

    let final_val = rows(s1.execute("SELECT n FROM t WHERE id = 1").unwrap());
    assert_eq!(final_val[0][0], Value::Int64(1100));
}
