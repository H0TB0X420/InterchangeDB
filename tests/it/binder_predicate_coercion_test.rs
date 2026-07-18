//! O2/O13 — bind-time reconciliation of WHERE literals with column types.
//!
//! The binder binds literals untyped (ints → Int64, strings → Varchar,
//! fractionals → Decimal at textual scale). Runtime comparison is exact
//! across numeric representations (`Value::compare_sql`), but the ACCESS
//! PATH is not: index/PK lowering encodes the literal against the indexed
//! column's key type, and `keyenc::encode_one` matches (value, type)
//! strictly — so `WHERE int32_indexed = 5` used to hand an Int64 prefix to
//! an Int32-keyed IndexScan and fail (or, for Char columns, silently match
//! nothing because Varchar('x') ≠ Char('x') by variant).
//!
//! `bind_predicate` now narrows literal operands to the compared column's
//! type when the conversion is value-preserving (`Value::coerce_exact`);
//! non-representable literals are left as-is and correctly match no rows.

use std::sync::Arc;

use tempfile::{tempdir, TempDir};

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::types::{ColumnType, Value};
use interchangedb::wal::SyncMode;

struct Env {
    session: Session<BTreeEngine>,
    _dir: TempDir,
}

/// `t(id INT PK, code INT indexed, tag CHAR(4) indexed)` — both secondary
/// indexes exist so the planner lowers the WHERE to an IndexScan, which is
/// the path that encodes literals against the column key type.
fn setup() -> Env {
    let dir = tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let engine = BTreeEngine::new(BufferPoolManager::new(512, dm)).unwrap();
    let database =
        Arc::new(Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap());
    let catalog = Arc::new(
        Catalog::open_persistent(
            database.engine_arc().clone(),
            dir.path().join("indexes"),
            interchangedb::default_index_opener(),
        )
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
                        name: "code".into(),
                        ty: ColumnType::Int32,
                        nullable: false,
                        default: None,
                    },
                    ColumnDef {
                        name: "tag".into(),
                        ty: ColumnType::Char(4),
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
            name: "t_by_code".into(),
            table_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();
    catalog
        .create_index(IndexDef {
            name: "t_by_tag".into(),
            table_id,
            columns: vec![2],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();

    let mut session = Session::new(database.clone(), catalog.clone());
    session
        .execute("INSERT INTO t VALUES (1, 7, 'abcd')")
        .unwrap();
    session
        .execute("INSERT INTO t VALUES (2, 8, 'wxyz')")
        .unwrap();
    Env { session, _dir: dir }
}

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {:?}", other),
    }
}

/// The literal binds as Int64; the indexed column is Int32. The IndexScan
/// prefix must carry an Int32 or key encoding fails outright.
#[test]
fn int_literal_narrows_to_int32_indexed_column() {
    let mut env = setup();
    let got = rows(
        env.session
            .execute("SELECT id FROM t WHERE code = 7")
            .unwrap(),
    );
    assert_eq!(got, vec![vec![Value::Int32(1)]]);
}

/// String literals bind as Varchar; the column stores Char. Without
/// narrowing the comparison is UNKNOWN (variant mismatch) and the index
/// prefix encoding fails — either way, zero rows for a row that exists.
#[test]
fn string_literal_narrows_to_char_indexed_column() {
    let mut env = setup();
    let got = rows(
        env.session
            .execute("SELECT id FROM t WHERE tag = 'abcd'")
            .unwrap(),
    );
    assert_eq!(got, vec![vec![Value::Int32(1)]]);
}

/// A literal no Int32 can equal (out of range) must match zero rows —
/// cleanly, not via an encoding error.
#[test]
fn out_of_range_literal_matches_nothing() {
    let mut env = setup();
    let got = rows(
        env.session
            .execute("SELECT id FROM t WHERE code = 5000000000")
            .unwrap(),
    );
    assert!(got.is_empty());
}
