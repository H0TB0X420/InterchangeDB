//! P12.4: secondary-index maintenance in `Table::insert/upsert/update/delete`.
//!
//! Every mutation must keep index entries consistent with the PK table.
//! Tests construct a `Table` with secondary indexes attached and then drive
//! every mutation path, asserting:
//! - INSERT writes one index entry per attached index.
//! - DELETE removes those entries.
//! - UPDATE rewrites entries when indexed columns change (and leaves them
//!   alone when they don't — both end states are observably consistent).
//! - UPSERT removes old entries and writes new ones.
//!
//! The "consistency" check: scan the index's underlying engine and count
//! entries. Encoding details are an implementation concern; what callers
//! care about is "there's exactly one index entry per live row, per index."

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::layout::RowLayout;
use interchangedb::storage::FileDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Value};

fn open_catalog_at(dir: &std::path::Path) -> Arc<Catalog<BTreeEngine>> {
    let dm = FileDiskManager::open_or_create(dir.join("cat.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    Arc::new(Catalog::open_persistent(engine, dir.join("indexes")).unwrap())
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

/// Build a `Table` with a single secondary index on `w_name`.
fn make_table_with_name_index(
    cat: Arc<Catalog<BTreeEngine>>,
    backend: IndexBackend,
) -> (
    Table<BTreeEngine, RowLayout>,
    interchangedb::catalog::IndexId,
) {
    let table_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    let idx_id = cat
        .create_index(IndexDef {
            name: "warehouse_by_name".into(),
            table_id,
            columns: vec![1],
            unique: false,
            backend,
        })
        .unwrap();
    let schema = cat.get_table("warehouse").unwrap();
    let indexes = cat.indexes_for_table(table_id, &schema).unwrap();
    let table = Table::with_indexes(cat.engine().clone(), schema, RowLayout, indexes);
    (table, idx_id)
}

/// Count how many entries are in the index's underlying engine.
fn index_entry_count(cat: &Catalog<BTreeEngine>, id: interchangedb::catalog::IndexId) -> usize {
    cat.index_engine(id)
        .unwrap()
        .scan_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
        .count()
}

#[test]
fn insert_writes_one_index_entry_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let (table, idx_id) = make_table_with_name_index(cat.clone(), IndexBackend::BTree);

    table
        .insert(&[Value::Int32(1), Value::Varchar("alpha".into())])
        .unwrap();
    table
        .insert(&[Value::Int32(2), Value::Varchar("bravo".into())])
        .unwrap();
    table
        .insert(&[Value::Int32(3), Value::Varchar("charlie".into())])
        .unwrap();

    assert_eq!(index_entry_count(&cat, idx_id), 3);
}

#[test]
fn delete_removes_index_entry() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let (table, idx_id) = make_table_with_name_index(cat.clone(), IndexBackend::BTree);

    table
        .insert(&[Value::Int32(1), Value::Varchar("alpha".into())])
        .unwrap();
    table
        .insert(&[Value::Int32(2), Value::Varchar("bravo".into())])
        .unwrap();
    assert_eq!(index_entry_count(&cat, idx_id), 2);

    table.delete_by_pk(&[Value::Int32(1)]).unwrap();
    assert_eq!(index_entry_count(&cat, idx_id), 1);

    table.delete_by_pk(&[Value::Int32(2)]).unwrap();
    assert_eq!(index_entry_count(&cat, idx_id), 0);
}

#[test]
fn update_by_pk_rewrites_index_entry_when_indexed_column_changes() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let (table, idx_id) = make_table_with_name_index(cat.clone(), IndexBackend::BTree);

    table
        .insert(&[Value::Int32(1), Value::Varchar("alpha".into())])
        .unwrap();
    assert_eq!(index_entry_count(&cat, idx_id), 1);

    // Replace row — new w_name means new index entry.
    table
        .update_by_pk(
            &[Value::Int32(1)],
            &[Value::Int32(1), Value::Varchar("zeta".into())],
        )
        .unwrap();
    // Still exactly one entry — old removed, new added.
    assert_eq!(index_entry_count(&cat, idx_id), 1);
}

#[test]
fn update_columns_rewrites_index_entry() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let (table, idx_id) = make_table_with_name_index(cat.clone(), IndexBackend::BTree);

    table
        .insert(&[Value::Int32(1), Value::Varchar("alpha".into())])
        .unwrap();
    table
        .update_columns(&[Value::Int32(1)], &[(1, Value::Varchar("renamed".into()))])
        .unwrap();
    assert_eq!(index_entry_count(&cat, idx_id), 1);
}

#[test]
fn upsert_replaces_index_entry_on_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let (table, idx_id) = make_table_with_name_index(cat.clone(), IndexBackend::BTree);

    table
        .upsert(&[Value::Int32(1), Value::Varchar("alpha".into())])
        .unwrap();
    table
        .upsert(&[Value::Int32(1), Value::Varchar("alpha2".into())])
        .unwrap();
    // Same PK, new name. Exactly one entry; the old "alpha" entry is gone.
    assert_eq!(index_entry_count(&cat, idx_id), 1);
}

#[test]
fn lsm_backed_index_maintenance_works() {
    // Same shape as the BTree test, but the index is LSM-backed.
    // Proves the per-index backend choice is transparent at the
    // maintenance layer.
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let (table, idx_id) = make_table_with_name_index(cat.clone(), IndexBackend::Lsm);

    table
        .insert(&[Value::Int32(1), Value::Varchar("alpha".into())])
        .unwrap();
    table
        .insert(&[Value::Int32(2), Value::Varchar("bravo".into())])
        .unwrap();
    table.delete_by_pk(&[Value::Int32(1)]).unwrap();

    assert_eq!(index_entry_count(&cat, idx_id), 1);
}

#[test]
fn table_without_indexes_does_not_touch_any_index() {
    // Regression guard: `Table::new` (no indexes) is the existing API.
    // It must continue to work without side effects on any index map.
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let table_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    let schema = cat.get_table("warehouse").unwrap();
    let table = Table::new(cat.engine().clone(), schema, RowLayout);

    table
        .insert(&[Value::Int32(1), Value::Varchar("alpha".into())])
        .unwrap();
    assert_eq!(
        table.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap()[1],
        Value::Varchar("alpha".into())
    );
    // Sanity: table_id had no indexes registered.
    assert_eq!(
        cat.indexes_for_table(table_id, &cat.get_table("warehouse").unwrap())
            .unwrap()
            .len(),
        0
    );
}
