//! Direct tests for `TxnEngine<E>` — the MVCC + locking + WAL wrapper that
//! Phase 10's executor will sit on top of.
//!
//! These tests construct a TxnEngine handle directly (bypassing Database)
//! to verify the relocated MVCC ops in isolation, before the Database
//! shim (Step 2.E) routes all existing tests through it.
//!
//! Coverage:
//!   - Read-your-own-writes within a transaction.
//!   - Snapshot freezing across concurrent commits.
//!   - Tombstone visibility (delete → get None).
//!   - Scan returns newest visible version per key.
//!   - AUTO_COMMIT reads see committed data.
//!   - ReadOnly txns reject writes.
//!   - Table<TxnEngine<E>, RowLayout> round-trips correctly.

use std::sync::Arc;

use tempfile::{tempdir, TempDir};

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{ColumnDef, Schema, TableId};
use interchangedb::common::Error;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::layout::RowLayout;
use interchangedb::storage::{MemoryDiskManager, StorageEngine};
use interchangedb::table::Table;
use interchangedb::txn::engine::TxnEngine;
use interchangedb::txn::{TransactionManager, TxnId, TxnMode};
use interchangedb::types::{ColumnType, Value};
use interchangedb::wal::{LogRecord, Lsn, Wal};

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/// Bundles the pieces a TxnEngine handle needs. Keeps `_dir` alive so the
/// temp files outlive the test scope.
struct Env {
    engine: Arc<BTreeEngine>,
    txn_mgr: Arc<TransactionManager>,
    wal: Arc<Wal>,
    _dir: TempDir,
}

fn setup() -> Env {
    let dir = tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(256, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    let wal = Arc::new(Wal::open(&dir.path().join("wal")).unwrap());
    let txn_mgr = Arc::new(TransactionManager::new());
    Env {
        engine,
        txn_mgr,
        wal,
        _dir: dir,
    }
}

fn handle(env: &Env, txn_id: TxnId) -> TxnEngine<BTreeEngine> {
    TxnEngine::new(
        env.engine.clone(),
        env.txn_mgr.clone(),
        env.wal.clone(),
        txn_id,
    )
}

/// Mirrors `Database::begin_txn` for ReadWrite. Allocates the txn_id, writes
/// the Begin record, updates last_lsn — same dance, no Database wrapper.
fn begin_rw(env: &Env) -> TxnId {
    let txn_id = env.txn_mgr.begin(TxnMode::ReadWrite, Lsn::INVALID).unwrap();
    let mut record = LogRecord::begin(txn_id.0);
    let begin_lsn = env.wal.append(&mut record).unwrap();
    env.txn_mgr.update_last_lsn(txn_id, begin_lsn).unwrap();
    txn_id
}

fn begin_ro(env: &Env) -> TxnId {
    env.txn_mgr.begin(TxnMode::ReadOnly, Lsn::INVALID).unwrap()
}

/// Mirrors `Database::commit_txn` for ReadWrite.
fn commit(env: &Env, txn_id: TxnId) {
    let prev_lsn = env.txn_mgr.last_lsn(txn_id).unwrap();
    let commit_ts = env.txn_mgr.assign_commit_ts(txn_id).unwrap();
    let mut record = LogRecord::commit(txn_id.0, prev_lsn, commit_ts.0);
    let commit_lsn = env.wal.append(&mut record).unwrap();
    env.wal.sync_to(commit_lsn).unwrap();
    env.txn_mgr.lock_manager().release_all(txn_id);
    env.txn_mgr.commit(txn_id).unwrap();
}

fn abort(env: &Env, txn_id: TxnId) {
    let prev_lsn = env.txn_mgr.last_lsn(txn_id).unwrap_or(Lsn::INVALID);
    if prev_lsn != Lsn::INVALID {
        let mut record = LogRecord::abort(txn_id.0, prev_lsn);
        let abort_lsn = env.wal.append(&mut record).unwrap();
        env.wal.sync_to(abort_lsn).unwrap();
    }
    env.txn_mgr.lock_manager().release_all(txn_id);
    env.txn_mgr.abort(txn_id).unwrap();
}

// ---------------------------------------------------------------------------
// Direct TxnEngine MVCC tests
// ---------------------------------------------------------------------------

#[test]
fn put_then_get_in_same_txn_sees_own_write() {
    let env = setup();
    let txn = begin_rw(&env);
    let h = handle(&env, txn);

    h.lock_for_write(b"k").unwrap();
    h.put(b"k", b"v1").unwrap();
    assert_eq!(h.get(b"k").unwrap(), Some(b"v1".to_vec()));

    commit(&env, txn);
}

#[test]
fn get_under_later_txn_sees_committed_value() {
    let env = setup();

    let t1 = begin_rw(&env);
    let h1 = handle(&env, t1);
    h1.lock_for_write(b"k").unwrap();
    h1.put(b"k", b"v1").unwrap();
    commit(&env, t1);

    let t2 = begin_rw(&env);
    let h2 = handle(&env, t2);
    assert_eq!(h2.get(b"k").unwrap(), Some(b"v1".to_vec()));
    commit(&env, t2);
}

#[test]
fn snapshot_hides_concurrent_write() {
    // T1 begins, T2 begins, T1 writes K and commits.
    // T2's snapshot was taken before T1 committed, so T2 must not see K.
    let env = setup();

    let t1 = begin_rw(&env);
    let t2 = begin_rw(&env);

    let h1 = handle(&env, t1);
    h1.lock_for_write(b"k").unwrap();
    h1.put(b"k", b"v1").unwrap();

    // T1 has not committed yet — uncommitted version invisible.
    let h2 = handle(&env, t2);
    assert_eq!(
        h2.get(b"k").unwrap(),
        None,
        "uncommitted write must be invisible"
    );

    commit(&env, t1);

    // T1's commit_ts > T2's begin_ts → still invisible to T2.
    assert_eq!(
        h2.get(b"k").unwrap(),
        None,
        "post-T2-begin commit must remain invisible to T2's frozen snapshot"
    );
    abort(&env, t2);
}

#[test]
fn delete_writes_tombstone_visible_as_none() {
    let env = setup();

    let t1 = begin_rw(&env);
    let h1 = handle(&env, t1);
    h1.lock_for_write(b"k").unwrap();
    h1.put(b"k", b"v1").unwrap();
    commit(&env, t1);

    let t2 = begin_rw(&env);
    let h2 = handle(&env, t2);
    h2.lock_for_write(b"k").unwrap();
    h2.delete(b"k").unwrap();
    commit(&env, t2);

    let t3 = begin_rw(&env);
    let h3 = handle(&env, t3);
    assert_eq!(h3.get(b"k").unwrap(), None, "tombstoned key reads as None");
    commit(&env, t3);
}

#[test]
fn scan_emits_newest_visible_version_per_key() {
    // Seed three keys, then update one and delete another. Scan must reflect
    // the post-commit state for each key (newest visible version).
    let env = setup();

    let t1 = begin_rw(&env);
    let h1 = handle(&env, t1);
    for (k, v) in [
        (b"k1".as_slice(), b"v1".as_slice()),
        (b"k2".as_slice(), b"v2".as_slice()),
        (b"k3".as_slice(), b"v3".as_slice()),
    ] {
        h1.lock_for_write(k).unwrap();
        h1.put(k, v).unwrap();
    }
    commit(&env, t1);

    let t2 = begin_rw(&env);
    let h2 = handle(&env, t2);
    h2.lock_for_write(b"k2").unwrap();
    h2.put(b"k2", b"v2_updated").unwrap();
    h2.lock_for_write(b"k3").unwrap();
    h2.delete(b"k3").unwrap();
    commit(&env, t2);

    let t3 = begin_rw(&env);
    let h3 = handle(&env, t3);
    let pairs: Vec<_> = h3.scan(..).map(|r| r.unwrap()).collect();
    let by_key: std::collections::HashMap<Vec<u8>, Vec<u8>> = pairs.into_iter().collect();

    assert_eq!(by_key.get(b"k1".as_slice()), Some(&b"v1".to_vec()));
    assert_eq!(by_key.get(b"k2".as_slice()), Some(&b"v2_updated".to_vec()));
    assert!(
        !by_key.contains_key(b"k3".as_slice()),
        "tombstoned key must not appear in scan output"
    );
    commit(&env, t3);
}

#[test]
fn auto_commit_handle_reads_committed_data() {
    let env = setup();

    let t1 = begin_rw(&env);
    let h1 = handle(&env, t1);
    h1.lock_for_write(b"k").unwrap();
    h1.put(b"k", b"committed_value").unwrap();
    commit(&env, t1);

    // AUTO_COMMIT handle: snapshot constructed fresh from oracle.
    let h_auto = handle(&env, TxnId::AUTO_COMMIT);
    assert_eq!(h_auto.get(b"k").unwrap(), Some(b"committed_value".to_vec()));
}

#[test]
fn readonly_txn_rejects_writes() {
    let env = setup();
    let txn = begin_ro(&env);
    let h = handle(&env, txn);

    let put_result = h.put(b"k", b"v");
    assert!(
        matches!(put_result, Err(Error::TxnReadOnly(_))),
        "expected TxnReadOnly, got: {:?}",
        put_result
    );

    let delete_result = h.delete(b"k");
    assert!(
        matches!(delete_result, Err(Error::TxnReadOnly(_))),
        "expected TxnReadOnly, got: {:?}",
        delete_result
    );

    // Abort the read-only txn (no WAL records to write).
    env.txn_mgr.abort(txn).unwrap();
}

// ---------------------------------------------------------------------------
// Table<TxnEngine<E>, RowLayout> integration tests
// ---------------------------------------------------------------------------
//
// Validates the Phase 9 → Phase 10 transition: any code written against
// `Table<E, L>` now becomes transactional simply by swapping E for
// TxnEngine<E>. The X-lock is acquired via the trait's `lock_for_write`
// override automatically.

fn account_schema() -> Schema {
    Schema {
        name: "account".into(),
        table_id: TableId(1),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "balance".into(),
                ty: ColumnType::Int64,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    }
}

fn account_row(id: i32, balance: i64) -> Vec<Value> {
    vec![Value::Int32(id), Value::Int64(balance)]
}

#[test]
fn table_insert_then_get_via_txn_engine() {
    let env = setup();
    let txn = begin_rw(&env);

    let h = Arc::new(handle(&env, txn));
    let schema = Arc::new(account_schema());
    let table = Table::new(h, schema, RowLayout);

    table.insert(&account_row(1, 100)).unwrap();
    let back = table.get_by_pk(&[Value::Int32(1)]).unwrap();
    assert_eq!(back, Some(account_row(1, 100)));

    commit(&env, txn);
}

#[test]
fn table_update_via_txn_engine_persists_across_txn() {
    // Insert under T1, update under T2, read under T3.
    // Tests the get-then-put pair inside Table::update_by_pk now goes
    // through TxnEngine's MVCC, with the X-lock acquired via lock_for_write.
    let env = setup();
    let schema = Arc::new(account_schema());

    let t1 = begin_rw(&env);
    let h1 = Arc::new(handle(&env, t1));
    let table1 = Table::new(h1, schema.clone(), RowLayout);
    table1.insert(&account_row(1, 100)).unwrap();
    commit(&env, t1);

    let t2 = begin_rw(&env);
    let h2 = Arc::new(handle(&env, t2));
    let table2 = Table::new(h2, schema.clone(), RowLayout);
    table2
        .update_by_pk(&[Value::Int32(1)], &account_row(1, 250))
        .unwrap();
    commit(&env, t2);

    let t3 = begin_rw(&env);
    let h3 = Arc::new(handle(&env, t3));
    let table3 = Table::new(h3, schema, RowLayout);
    assert_eq!(
        table3.get_by_pk(&[Value::Int32(1)]).unwrap(),
        Some(account_row(1, 250))
    );
    commit(&env, t3);
}
