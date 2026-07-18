//! Phase 11 end-to-end: SQL strings → execute → verify.
//!
//! Drives the full stack through `Session::execute`:
//!
//!   SQL string → parse → bind → plan → operator tree → TxnEngine →
//!   Table → BTreeEngine → BPM → FileDiskManager
//!
//! The single-session TPC-C-shaped flows moved to declarative
//! sqllogictest data (`tests/slt/e2e.slt`); what remains here are the
//! tests that need two sessions (snapshot isolation, write conflict) or
//! filesystem assertions (workload log capture-everything contract).

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::common::Error;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::sql::workload_log::WorkloadLog;
use interchangedb::storage::MemoryDiskManager;
use interchangedb::types::Value;
use interchangedb::wal::SyncMode;

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
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database =
        Arc::new(Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap());
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

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// TPC-C-shaped scenarios
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Transaction lifecycle
// ---------------------------------------------------------------------------

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
