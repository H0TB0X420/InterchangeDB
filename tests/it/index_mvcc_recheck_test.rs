//! E1/O12 — IndexScan MVCC recheck.
//!
//! Secondary indexes are UNVERSIONED: `Table` mutations physically delete the
//! old entry and insert the new one in a raw (non-`TxnEngine`) engine, while
//! table reads are MVCC-versioned. A reader whose snapshot predates an
//! indexed-column update can therefore reach a *visible* row through a stale
//! index entry whose current indexed value no longer matches the row version
//! the snapshot returns.
//!
//! Methodology: two sessions on one database. Session A opens a transaction
//! (pinning its snapshot), session B then updates the indexed column and
//! commits. Session A queries through the index by the NEW value:
//!
//! - Without the planner's recheck Filter, the index entry for the new value
//!   dereferences to the snapshot-visible OLD row version → a row that
//!   violates the WHERE is returned (SELECT) or mutated (UPDATE/DELETE).
//! - With the recheck (`IndexLowering::Matched::recheck`), the predicate is
//!   re-evaluated against the row version actually read → the false positive
//!   is dropped.
//!
//! The symmetric FALSE NEGATIVE (querying by the OLD value misses the row the
//! snapshot still considers matching, because the writer physically removed
//! the index entry) is NOT fixable by a recheck — it needs versioned index
//! entries or abort-time index undo. The last test pins the current (wrong)
//! behavior with a NOTE so any architecture change surfaces here.

use std::sync::Arc;

use tempfile::{tempdir, TempDir};

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::storage::FileDiskManager;
use interchangedb::types::{ColumnType, Value};

struct Env {
    database: Arc<Database<BTreeEngine>>,
    catalog: Arc<Catalog<BTreeEngine>>,
    _dir: TempDir,
}

/// WAL-mode database + persistent catalog with table `t(id INT PK,
/// name VARCHAR(20))` indexed on `name`. The index is created before any
/// rows exist, so index maintenance runs purely through the mutation path
/// under test.
fn setup() -> Env {
    let dir = tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    let engine = BTreeEngine::new(BufferPoolManager::new(512, dm)).unwrap();
    let database = Arc::new(Database::open(dir.path(), engine).unwrap());
    let catalog = Arc::new(
        Catalog::open_persistent(database.engine_arc().clone(), dir.path().join("indexes"))
            .unwrap(),
    );

    let table_id = catalog
        .create_table(
            "t".into(),
            Schema {
                name: "t".into(),
                table_id: TableId(0),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        ty: ColumnType::Int32,
                        nullable: false,
                        default: None,
                    },
                    ColumnDef {
                        name: "name".into(),
                        ty: ColumnType::Varchar(20),
                        nullable: false,
                        default: None,
                    },
                ],
                primary_key: vec![0],
            },
        )
        .unwrap();
    catalog
        .create_index(IndexDef {
            name: "t_by_name".into(),
            table_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();

    Env {
        database,
        catalog,
        _dir: dir,
    }
}

fn session(env: &Env) -> Session<BTreeEngine> {
    Session::new(env.database.clone(), env.catalog.clone())
}

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {:?}", other),
    }
}

fn affected(r: QueryResult) -> u64 {
    match r {
        QueryResult::Affected(n) => n,
        other => panic!("expected Affected, got {:?}", other),
    }
}

/// Guard: the query under test must actually route through the index —
/// a plan regression to SeqScan+Filter would silently vacate these tests.
fn assert_uses_index_scan(s: &mut Session<BTreeEngine>, sql: &str) {
    let explained = match s.execute(&format!("EXPLAIN {}", sql)).unwrap() {
        QueryResult::Explain(text) => text,
        other => panic!("expected Explain, got {:?}", other),
    };
    assert!(
        explained.contains("IndexScan"),
        "test premise broken — plan must use IndexScan:\n{}",
        explained
    );
}

// A snapshot reader querying by the NEW value reaches the row through the
// fresh index entry, but the version its snapshot returns still has the OLD
// value — the recheck Filter must drop it. Without the recheck this returns
// row 1 with name='alpha' from a WHERE name='beta' query.
#[test]
fn snapshot_reader_gets_no_false_positive_via_stale_index_entry() {
    let env = setup();
    let mut writer = session(&env);
    let mut reader = session(&env);

    affected(writer.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap());

    // Pin the reader's snapshot before the update.
    reader.execute("BEGIN").unwrap();

    affected(
        writer
            .execute("UPDATE t SET name = 'beta' WHERE id = 1")
            .unwrap(),
    );

    assert_uses_index_scan(&mut reader, "SELECT id FROM t WHERE name = 'beta'");
    let got = rows(
        reader
            .execute("SELECT id FROM t WHERE name = 'beta'")
            .unwrap(),
    );
    assert!(
        got.is_empty(),
        "stale index entry leaked a false positive: {:?}",
        got
    );
    reader.execute("COMMIT").unwrap();

    // Post-commit sanity: a fresh snapshot sees the new value through the
    // same index path.
    let after = rows(
        reader
            .execute("SELECT id FROM t WHERE name = 'beta'")
            .unwrap(),
    );
    assert_eq!(after, vec![vec![Value::Int32(1)]]);
}

// Same interleaving on the mutation path (O12): an UPDATE whose WHERE matches
// only via the stale index entry must mutate NOTHING. Without the recheck it
// would rewrite a row that, at this transaction's snapshot, does not satisfy
// the predicate — silent wrong-row corruption.
#[test]
fn snapshot_update_does_not_mutate_wrong_row_via_stale_index_entry() {
    let env = setup();
    let mut writer = session(&env);
    let mut mutator = session(&env);

    affected(writer.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap());
    affected(writer.execute("INSERT INTO t VALUES (2, 'gamma')").unwrap());

    mutator.execute("BEGIN").unwrap();

    affected(
        writer
            .execute("UPDATE t SET name = 'beta' WHERE id = 1")
            .unwrap(),
    );

    let n = affected(
        mutator
            .execute("UPDATE t SET name = 'oops' WHERE name = 'beta'")
            .unwrap(),
    );
    assert_eq!(n, 0, "recheck must stop the wrong-row update");
    mutator.execute("COMMIT").unwrap();

    // Neither row was touched by the mutator.
    let all = rows(writer.execute("SELECT id, name FROM t").unwrap());
    assert_eq!(
        all,
        vec![
            vec![Value::Int32(1), Value::Varchar("beta".into())],
            vec![Value::Int32(2), Value::Varchar("gamma".into())],
        ]
    );
}

// NOTE: documents the UNFIXED false-negative half of E1. The writer's update
// physically deleted the 'alpha' index entry, so the reader's index scan by
// 'alpha' finds nothing — even though the reader's snapshot says row 1 still
// has name='alpha' (a SeqScan-planned query would return it). Fixing this
// requires versioned index entries (or abort-time index undo); if that lands,
// this assertion should flip to `vec![vec![Int32(1)]]`.
#[test]
fn snapshot_reader_false_negative_by_old_value_is_a_known_gap() {
    let env = setup();
    let mut writer = session(&env);
    let mut reader = session(&env);

    affected(writer.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap());

    reader.execute("BEGIN").unwrap();

    affected(
        writer
            .execute("UPDATE t SET name = 'beta' WHERE id = 1")
            .unwrap(),
    );

    let got = rows(
        reader
            .execute("SELECT id FROM t WHERE name = 'alpha'")
            .unwrap(),
    );
    assert!(
        got.is_empty(),
        "false-negative gap closed? update this test to assert the row IS \
         returned, and close the versioned-index finding (E1)"
    );
    reader.execute("COMMIT").unwrap();
}
