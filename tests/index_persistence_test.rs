//! P12.2: `IndexDef::backend` field + `__sys_indexes` persistence.
//!
//! Writes a mix of BTree- and LSM-backed indexes via `Catalog::create_index`,
//! reopens the catalog (which reads back from `__sys_indexes`), and confirms
//! the backend choice survived. The actual engine re-instantiation on
//! reopen lands in P12.3 — this test covers only the metadata roundtrip.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::system_tables::read_all_index_rows;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::{FileDiskManager, StorageEngine};
use interchangedb::types::ColumnType;

fn fresh_catalog_in(dir: &std::path::Path) -> Catalog<BTreeEngine> {
    let dm = FileDiskManager::open_or_create(dir.join("cat.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    Catalog::open(engine).unwrap()
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
fn index_backend_round_trips_through_sys_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let cat = fresh_catalog_in(dir.path());
    let table_id = cat.create_table("warehouse".into(), warehouse_schema()).unwrap();

    let btree_idx = cat
        .create_index(IndexDef {
            name: "warehouse_by_name_btree".into(),
            table_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();
    let lsm_idx = cat
        .create_index(IndexDef {
            name: "warehouse_by_name_lsm".into(),
            table_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::Lsm,
        })
        .unwrap();

    let rows = read_all_index_rows(&**cat.engine()).unwrap();
    let mut found_btree = false;
    let mut found_lsm = false;
    for (id, def) in &rows {
        if *id == btree_idx {
            assert_eq!(def.backend, IndexBackend::BTree);
            assert_eq!(def.name, "warehouse_by_name_btree");
            assert_eq!(def.columns, vec![1]);
            found_btree = true;
        }
        if *id == lsm_idx {
            assert_eq!(def.backend, IndexBackend::Lsm);
            assert_eq!(def.name, "warehouse_by_name_lsm");
            assert_eq!(def.columns, vec![1]);
            found_lsm = true;
        }
    }
    assert!(found_btree, "btree index missing from __sys_indexes");
    assert!(found_lsm, "lsm index missing from __sys_indexes");
}

#[test]
fn index_metadata_survives_catalog_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let table_id;
    let idx_id;
    {
        let cat = fresh_catalog_in(dir.path());
        table_id = cat.create_table("warehouse".into(), warehouse_schema()).unwrap();
        idx_id = cat
            .create_index(IndexDef {
                name: "warehouse_by_name".into(),
                table_id,
                columns: vec![1],
                unique: true,
                backend: IndexBackend::Lsm,
            })
            .unwrap();
        // BPM doesn't auto-flush on drop; explicit flush keeps the writes
        // alive across reopen. Matches `btree_reopen_recovers_user_tables`'s
        // pattern in `tests/catalog_table_test.rs`.
        cat.engine().flush().unwrap();
    }

    let cat2 = fresh_catalog_in(dir.path());
    let rows = read_all_index_rows(&**cat2.engine()).unwrap();
    let (_, def) = rows.iter().find(|(id, _)| *id == idx_id).expect("missing");
    assert_eq!(def.name, "warehouse_by_name");
    assert_eq!(def.table_id, table_id);
    assert_eq!(def.columns, vec![1]);
    assert!(def.unique);
    assert_eq!(def.backend, IndexBackend::Lsm);
}

#[test]
fn backend_discriminator_stable() {
    // Lock down the on-disk discriminator values. Renumbering would
    // silently corrupt existing databases on reopen.
    assert_eq!(IndexBackend::BTree.as_i32(), 0);
    assert_eq!(IndexBackend::Lsm.as_i32(), 1);
    assert_eq!(IndexBackend::from_i32(0).unwrap(), IndexBackend::BTree);
    assert_eq!(IndexBackend::from_i32(1).unwrap(), IndexBackend::Lsm);
    assert!(IndexBackend::from_i32(99).is_err());
}
