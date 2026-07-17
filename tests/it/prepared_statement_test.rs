//! P13.7: prepared statements.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::Catalog;
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::storage::MemoryDiskManager;
#[allow(unused_imports)]
use interchangedb::storage::StorageEngine;
use interchangedb::types::Value; // for Database engine accessor
use interchangedb::wal::SyncMode;

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
        .execute("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)")
        .unwrap();
    session
        .execute("INSERT INTO accounts VALUES (1, 100)")
        .unwrap();
    session
        .execute("INSERT INTO accounts VALUES (2, 200)")
        .unwrap();
    session
        .execute("INSERT INTO accounts VALUES (3, 300)")
        .unwrap();
    (session, dir)
}

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got: {:?}", other),
    }
}

#[test]
fn prepare_select_with_one_parameter_runs_multiple_times() {
    let (mut s, _d) = setup();
    let ps = s
        .prepare("SELECT id, balance FROM accounts WHERE id = $1")
        .unwrap();

    let r1 = rows(s.execute_prepared(&ps, &[Value::Int32(1)]).unwrap());
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0][1], Value::Int32(100));

    let r2 = rows(s.execute_prepared(&ps, &[Value::Int32(2)]).unwrap());
    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0][1], Value::Int32(200));

    let r3 = rows(s.execute_prepared(&ps, &[Value::Int32(3)]).unwrap());
    assert_eq!(r3.len(), 1);
    assert_eq!(r3[0][1], Value::Int32(300));
}

#[test]
fn prepare_with_missing_parameter_errors() {
    let (mut s, _d) = setup();
    let ps = s.prepare("SELECT id FROM accounts WHERE id = $1").unwrap();
    let err = s.execute_prepared(&ps, &[]).unwrap_err();
    assert!(matches!(err, interchangedb::Error::SqlParse(_)));
}

#[test]
fn prepared_update_substitutes_into_set_clause_and_where() {
    let (mut s, _d) = setup();
    let ps = s
        .prepare("UPDATE accounts SET balance = $1 WHERE id = $2")
        .unwrap();
    let _ = s
        .execute_prepared(&ps, &[Value::Int32(999), Value::Int32(1)])
        .unwrap();
    let r = rows(
        s.execute("SELECT balance FROM accounts WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(r[0][0], Value::Int32(999));
}

#[test]
fn prepared_delete_with_parameter() {
    let (mut s, _d) = setup();
    let ps = s.prepare("DELETE FROM accounts WHERE id = $1").unwrap();
    let _ = s.execute_prepared(&ps, &[Value::Int32(2)]).unwrap();
    let remaining = rows(s.execute("SELECT id FROM accounts").unwrap());
    let ids: Vec<i32> = remaining
        .iter()
        .map(|r| match r[0] {
            Value::Int32(i) => i,
            _ => panic!(),
        })
        .collect();
    assert!(!ids.contains(&2));
    assert_eq!(ids.len(), 2);
}

#[test]
fn anonymous_question_mark_rejected_with_clear_message() {
    let (s, _d) = setup();
    let err = s
        .prepare("SELECT id FROM accounts WHERE id = ?")
        .unwrap_err();
    assert!(
        matches!(err, interchangedb::Error::SqlParse(ref m) if m.contains("$1")),
        "expected helpful $N error, got: {:?}",
        err
    );
}

#[test]
fn prepare_select_without_parameters_still_works() {
    let (mut s, _d) = setup();
    // A prepared statement with no placeholders should still execute via
    // the prepared path (with an empty params array).
    let ps = s.prepare("SELECT id FROM accounts WHERE id = 1").unwrap();
    let r = rows(s.execute_prepared(&ps, &[]).unwrap());
    assert_eq!(r.len(), 1);
}

#[test]
fn prepare_multiple_parameters_with_compound_predicate() {
    let (mut s, _d) = setup();
    let ps = s
        .prepare("SELECT id FROM accounts WHERE id = $1 OR id = $2")
        .unwrap();
    let r = rows(
        s.execute_prepared(&ps, &[Value::Int32(1), Value::Int32(3)])
            .unwrap(),
    );
    assert_eq!(r.len(), 2);
}
