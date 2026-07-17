//! Phase 10 end-to-end: TPC-C-shaped operator trees over the full stack.
//!
//!   MemoryDiskManager → BPM → BTreeEngine → TxnEngine → Table → Executor tree
//!
//! Each test exercises a complete transaction lifecycle: begin → build
//! operator tree → drive `next()` → commit (or abort) → verify with a
//! fresh transaction.
//!
//! Schema mirrors TPC-C's WAREHOUSE table shape (simplified: Int64 in
//! place of Decimal for `w_ytd`, fewer string columns).

use std::sync::Arc;
use tempfile::{tempdir, TempDir};

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{ColumnDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::{
    Delete, Executor, Filter, Insert, Projection, SeqScan, SetExpr, Update,
};
use interchangedb::layout::RowLayout;
use interchangedb::storage::MemoryDiskManager;
use interchangedb::table::Table;
use interchangedb::txn::engine::TxnEngine;
use interchangedb::txn::{TransactionManager, TxnId, TxnMode};
use interchangedb::types::{ColumnType, Value};
use interchangedb::wal::{LogRecord, Lsn, Wal};

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

struct Env {
    engine: Arc<BTreeEngine>,
    txn_mgr: Arc<TransactionManager>,
    wal: Arc<Wal>,
    _dir: TempDir,
}

fn setup() -> Env {
    let dir = tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(512, dm);
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

fn handle(env: &Env, txn_id: TxnId) -> Arc<TxnEngine<BTreeEngine>> {
    Arc::new(TxnEngine::new(
        env.engine.clone(),
        env.txn_mgr.clone(),
        env.wal.clone(),
        txn_id,
    ))
}

fn begin_rw(env: &Env) -> TxnId {
    let txn_id = env.txn_mgr.begin(TxnMode::ReadWrite, Lsn::INVALID).unwrap();
    let mut record = LogRecord::begin(txn_id.0);
    let begin_lsn = env.wal.append(&mut record).unwrap();
    env.txn_mgr.update_last_lsn(txn_id, begin_lsn).unwrap();
    txn_id
}

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

fn warehouse_schema() -> Schema {
    Schema {
        name: "warehouse".into(),
        table_id: TableId(1),
        columns: vec![
            ColumnDef {
                name: "w_id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_ytd".into(),
                ty: ColumnType::Int64,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_name".into(),
                ty: ColumnType::Varchar(10),
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    }
}

fn wh(id: i32, ytd: i64, name: &str) -> Vec<Value> {
    vec![
        Value::Int32(id),
        Value::Int64(ytd),
        Value::Varchar(name.into()),
    ]
}

fn warehouse_table(env: &Env, txn: TxnId) -> Arc<Table<TxnEngine<BTreeEngine>, RowLayout>> {
    let h = handle(env, txn);
    Arc::new(Table::new(h, Arc::new(warehouse_schema()), RowLayout))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn update_one_row_via_operator_tree() {
    // TPC-C Payment-style: UPDATE warehouse SET w_ytd = w_ytd + 100 WHERE w_id = 1
    let env = setup();

    // Seed: insert two warehouses under txn 1.
    let t_seed = begin_rw(&env);
    let table_seed = warehouse_table(&env, t_seed);
    let mut seed = Insert::new(table_seed, vec![wh(1, 1000, "north"), wh(2, 2000, "south")]);
    assert_eq!(seed.next().unwrap(), Some(vec![Value::Int64(2)]));
    commit(&env, t_seed);

    // Build and run the UPDATE tree under txn 2.
    let t_update = begin_rw(&env);
    let table = warehouse_table(&env, t_update);

    let scan = Box::new(SeqScan::new(&*table).unwrap());
    let filt = Box::new(Filter::new(scan, |row| row[0] == Value::Int32(1)));
    let set_exprs: Vec<SetExpr> = vec![(
        1,
        Box::new(|row| match row[1] {
            Value::Int64(n) => Value::Int64(n + 100),
            _ => Value::Null,
        }),
    )];
    let mut op = Update::new(table.clone(), filt, set_exprs);

    assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(1)]));
    commit(&env, t_update);

    // Verify under a fresh txn.
    let t_verify = begin_rw(&env);
    let table_v = warehouse_table(&env, t_verify);
    assert_eq!(
        table_v.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap()[1],
        Value::Int64(1100)
    );
    assert_eq!(
        table_v.get_by_pk(&[Value::Int32(2)]).unwrap().unwrap()[1],
        Value::Int64(2000),
        "non-matching row must be untouched"
    );
    commit(&env, t_verify);
}

#[test]
fn insert_via_operator_tree_persists_after_commit() {
    let env = setup();

    let t1 = begin_rw(&env);
    let table = warehouse_table(&env, t1);
    let mut op = Insert::new(
        table,
        vec![wh(1, 100, "a"), wh(2, 200, "b"), wh(3, 300, "c")],
    );
    assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(3)]));
    commit(&env, t1);

    let t2 = begin_rw(&env);
    let table_v = warehouse_table(&env, t2);
    for (id, ytd) in [(1, 100), (2, 200), (3, 300)] {
        assert_eq!(
            table_v.get_by_pk(&[Value::Int32(id)]).unwrap().unwrap()[1],
            Value::Int64(ytd)
        );
    }
    commit(&env, t2);
}

#[test]
fn delete_via_operator_tree_removes_filtered_rows() {
    let env = setup();

    let t1 = begin_rw(&env);
    let table = warehouse_table(&env, t1);
    let mut seed = Insert::new(
        table.clone(),
        (1..=5).map(|i| wh(i, i as i64 * 100, "x")).collect(),
    );
    seed.next().unwrap();
    commit(&env, t1);

    // DELETE FROM warehouse WHERE w_id > 3
    let t2 = begin_rw(&env);
    let table = warehouse_table(&env, t2);
    let scan = Box::new(SeqScan::new(&*table).unwrap());
    let filt = Box::new(Filter::new(
        scan,
        |row| matches!(row[0], Value::Int32(n) if n > 3),
    ));
    let mut op = Delete::new(table.clone(), filt);
    assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(2)]));
    commit(&env, t2);

    let t3 = begin_rw(&env);
    let table_v = warehouse_table(&env, t3);
    for id in 1..=3 {
        assert!(table_v.get_by_pk(&[Value::Int32(id)]).unwrap().is_some());
    }
    for id in 4..=5 {
        assert!(table_v.get_by_pk(&[Value::Int32(id)]).unwrap().is_none());
    }
    commit(&env, t3);
}

#[test]
fn select_path_seq_scan_filter_projection() {
    // SELECT w_id, w_name FROM warehouse WHERE w_ytd >= 200 — verifies the
    // pull-based pipeline produces the right rows in the right shape.
    let env = setup();

    let t1 = begin_rw(&env);
    let table = warehouse_table(&env, t1);
    let mut seed = Insert::new(
        table.clone(),
        vec![
            wh(1, 100, "a"),
            wh(2, 200, "b"),
            wh(3, 300, "c"),
            wh(4, 50, "d"),
        ],
    );
    seed.next().unwrap();
    commit(&env, t1);

    let t2 = begin_rw(&env);
    let table = warehouse_table(&env, t2);
    let scan = Box::new(SeqScan::new(&*table).unwrap());
    let filt = Box::new(Filter::new(
        scan,
        |row| matches!(row[1], Value::Int64(n) if n >= 200),
    ));
    // Project w_id (col 0) and w_name (col 2) — drop w_ytd.
    let proj = Projection::new(filt, vec![0, 2]).unwrap();

    let mut out: Vec<Vec<Value>> = Vec::new();
    let mut op = proj;
    while let Some(t) = op.next().unwrap() {
        out.push(t);
    }

    // In PK order, the rows ≥ 200 are (2, "b") and (3, "c").
    assert_eq!(
        out,
        vec![
            vec![Value::Int32(2), Value::Varchar("b".into())],
            vec![Value::Int32(3), Value::Varchar("c".into())],
        ]
    );
    commit(&env, t2);
}

#[test]
fn abort_rolls_back_operator_tree_writes() {
    // Update via operator tree, then abort. Subsequent reads see the
    // pre-update value.
    let env = setup();

    let t_seed = begin_rw(&env);
    let table_seed = warehouse_table(&env, t_seed);
    let mut seed = Insert::new(table_seed, vec![wh(1, 1000, "seed")]);
    seed.next().unwrap();
    commit(&env, t_seed);

    let t_doomed = begin_rw(&env);
    let table_d = warehouse_table(&env, t_doomed);
    let scan = Box::new(SeqScan::new(&*table_d).unwrap());
    let set_exprs: Vec<SetExpr> = vec![(1, Box::new(|_| Value::Int64(99999)))];
    let mut op = Update::new(table_d, scan, set_exprs);
    assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(1)]));
    abort(&env, t_doomed);

    // Fresh txn sees pre-update value because the aborted txn's versions
    // are in known_not_committed (Issue 2 fix).
    let t_verify = begin_rw(&env);
    let table_v = warehouse_table(&env, t_verify);
    assert_eq!(
        table_v.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap()[1],
        Value::Int64(1000),
        "aborted update must not be visible"
    );
    commit(&env, t_verify);
}

#[test]
fn snapshot_isolation_holds_under_concurrent_commit() {
    // Reader begins, writer commits, reader's SeqScan still shows the
    // pre-commit view because Filter sees the reader's frozen snapshot.
    let env = setup();

    let t_seed = begin_rw(&env);
    let table_seed = warehouse_table(&env, t_seed);
    let mut seed = Insert::new(table_seed, vec![wh(1, 1000, "before")]);
    seed.next().unwrap();
    commit(&env, t_seed);

    // Reader begins first.
    let t_reader = begin_rw(&env);

    // Writer commits a new value.
    let t_writer = begin_rw(&env);
    let table_w = warehouse_table(&env, t_writer);
    let scan_w = Box::new(SeqScan::new(&*table_w).unwrap());
    let set_exprs: Vec<SetExpr> = vec![(1, Box::new(|_| Value::Int64(9999)))];
    let mut update = Update::new(table_w, scan_w, set_exprs);
    update.next().unwrap();
    commit(&env, t_writer);

    // Reader's SeqScan must still see 1000 — its snapshot precedes the writer's commit.
    let table_r = warehouse_table(&env, t_reader);
    let mut scan_r = SeqScan::new(&*table_r).unwrap();
    let row = scan_r.next().unwrap().unwrap();
    assert_eq!(
        row[1],
        Value::Int64(1000),
        "reader's snapshot must not see writer's post-begin commit"
    );
    commit(&env, t_reader);
}

#[test]
fn explain_renders_tpc_c_shaped_plan() {
    // EXPLAIN UPDATE warehouse SET w_ytd = w_ytd + 100 WHERE w_id = 1
    let env = setup();
    let t = begin_rw(&env);
    let table = warehouse_table(&env, t);

    let scan = Box::new(SeqScan::new(&*table).unwrap());
    let filt = Box::new(Filter::new(scan, |_| true));
    let set_exprs: Vec<SetExpr> = vec![(1, Box::new(|_| Value::Int64(0)))];
    let op = Update::new(table, filt, set_exprs);

    let tree = op.explain(0);
    let expected = "\
Update(warehouse, set_cols=[1])
  Filter
    SeqScan(warehouse)
";
    assert_eq!(tree, expected);

    abort(&env, t);
}

#[test]
fn update_then_select_within_same_txn_sees_own_writes() {
    // Read-your-own-writes within a single txn — important for TPC-C's
    // NEW_ORDER which reads after writing within the same transaction.
    let env = setup();

    let t_seed = begin_rw(&env);
    let table_seed = warehouse_table(&env, t_seed);
    let mut seed = Insert::new(table_seed, vec![wh(1, 100, "x")]);
    seed.next().unwrap();
    commit(&env, t_seed);

    let t = begin_rw(&env);
    let table = warehouse_table(&env, t);

    // UPDATE w_ytd = 500
    let scan = Box::new(SeqScan::new(&*table).unwrap());
    let set_exprs: Vec<SetExpr> = vec![(1, Box::new(|_| Value::Int64(500)))];
    let mut update = Update::new(table.clone(), scan, set_exprs);
    update.next().unwrap();

    // SELECT in the same txn — must see 500, not 100.
    let scan2 = SeqScan::new(&*table).unwrap();
    let mut s = scan2;
    let row = s.next().unwrap().unwrap();
    assert_eq!(row[1], Value::Int64(500), "must see own write within txn");

    commit(&env, t);
}
