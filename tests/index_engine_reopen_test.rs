//! P12.3: per-index engine factory + create_index auto-allocation +
//! reopen rehydration.
//!
//! Verifies that `Catalog::open_persistent(engine, dir)`:
//! - Allocates a fresh engine of the chosen backend at create_index time.
//! - Re-instantiates each existing index's engine on reopen, using the
//!   backend recorded in `__sys_indexes`.
//! - Data written to an index before reopen is still readable after.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::{FileDiskManager, StorageEngine};
use interchangedb::types::ColumnType;

fn open_catalog_at(dir: &std::path::Path) -> Catalog<BTreeEngine> {
    let dm = FileDiskManager::open_or_create(dir.join("cat.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    Catalog::open_persistent(engine, dir.join("indexes")).unwrap()
}

fn warehouse_schema() -> Schema {
    Schema {
        name: "warehouse".into(),
        table_id: TableId(0),
        columns: vec![
            ColumnDef {
                name: "w_id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_name".into(),
                ty: ColumnType::Varchar(20),
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    }
}

#[test]
fn create_index_auto_allocates_engine() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let table_id = cat.create_table("warehouse".into(), warehouse_schema()).unwrap();

    let idx = cat
        .create_index(IndexDef {
            name: "warehouse_by_name".into(),
            table_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();

    // Auto-allocated handle is immediately usable.
    let engine = cat.index_engine(idx).expect("auto-registered");
    engine.put(b"hello", b"world").unwrap();
    assert_eq!(engine.get(b"hello").unwrap(), Some(b"world".to_vec()));
    assert_eq!(engine.name(), "btree");
}

#[test]
fn lsm_backed_index_is_lsm_engine() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let table_id = cat.create_table("warehouse".into(), warehouse_schema()).unwrap();
    let idx = cat
        .create_index(IndexDef {
            name: "warehouse_lsm".into(),
            table_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::Lsm,
        })
        .unwrap();

    let engine = cat.index_engine(idx).unwrap();
    assert_eq!(engine.name(), "lsm");
}

#[test]
fn reopen_repopulates_index_engines_with_correct_backend() {
    let dir = tempfile::tempdir().unwrap();
    let btree_idx_id;
    let lsm_idx_id;
    {
        let cat = open_catalog_at(dir.path());
        let table_id = cat
            .create_table("warehouse".into(), warehouse_schema())
            .unwrap();

        btree_idx_id = cat
            .create_index(IndexDef {
                name: "ix_btree".into(),
                table_id,
                columns: vec![1],
                unique: false,
                backend: IndexBackend::BTree,
            })
            .unwrap();
        lsm_idx_id = cat
            .create_index(IndexDef {
                name: "ix_lsm".into(),
                table_id,
                columns: vec![1],
                unique: false,
                backend: IndexBackend::Lsm,
            })
            .unwrap();

        // Write distinguishable data to each.
        cat.index_engine(btree_idx_id)
            .unwrap()
            .put(b"k", b"from_btree")
            .unwrap();
        cat.index_engine(lsm_idx_id)
            .unwrap()
            .put(b"k", b"from_lsm")
            .unwrap();

        // Flush both the catalog engine and the LSM index (BTreeEngine
        // flush propagates through its BPM; LSM needs explicit flush to
        // persist its memtable).
        cat.engine().flush().unwrap();
        cat.index_engine(btree_idx_id).unwrap().flush().unwrap();
        cat.index_engine(lsm_idx_id).unwrap().flush().unwrap();
    }

    // Reopen. The catalog must rehydrate both index engines at the right
    // backend type and find the data we wrote.
    let cat2 = open_catalog_at(dir.path());
    let b = cat2.index_engine(btree_idx_id).expect("btree index missing post-reopen");
    let l = cat2.index_engine(lsm_idx_id).expect("lsm index missing post-reopen");
    assert_eq!(b.name(), "btree");
    assert_eq!(l.name(), "lsm");
    assert_eq!(b.get(b"k").unwrap(), Some(b"from_btree".to_vec()));
    assert_eq!(l.get(b"k").unwrap(), Some(b"from_lsm".to_vec()));
}

#[test]
fn catalog_open_without_dir_skips_auto_allocation() {
    // `Catalog::open` (no dir) is the test path — manual registration only.
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("cat.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    let cat = Catalog::open(engine).unwrap();
    let table_id = cat.create_table("warehouse".into(), warehouse_schema()).unwrap();
    let idx = cat
        .create_index(IndexDef {
            name: "ix".into(),
            table_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();

    // Auto-allocation skipped — no engine registered yet.
    assert!(cat.index_engine(idx).is_none());
}
