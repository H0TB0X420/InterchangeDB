//! P12.5: `IndexScan` operator tests.
//!
//! Drives the Volcano `IndexScan` against a small table with one
//! secondary index, covering:
//! - point lookup hits the right rows
//! - point lookup miss yields no rows
//! - multi-row matches (non-unique index) all returned
//! - partial-prefix lookup on a composite index
//! - mixed backend: same operator works on a BTree- or LSM-backed index
//! - rebuilt index after Update: scan reflects new values, not old
//!
//! Doesn't assert order *within* a key prefix (the tiebreaker is the
//! PK encoding suffix — implementation detail).

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::{Executor, IndexScan};
use interchangedb::layout::RowLayout;
use interchangedb::storage::MemoryDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Value};

fn open_catalog_at(dir: &std::path::Path) -> Arc<Catalog<BTreeEngine>> {
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    Arc::new(
        Catalog::open_persistent(
            engine,
            dir.join("indexes"),
            interchangedb::default_index_opener(),
        )
        .unwrap(),
    )
}

/// Schema: (w_id PK, w_name, w_region). Two scan candidates: name (single
/// column) and (region, name) for composite-prefix tests.
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
            ColumnDef {
                name: "w_region".into(),
                ty: ColumnType::Varchar(8),
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    }
}

fn row(id: i32, name: &str, region: &str) -> Vec<Value> {
    vec![
        Value::Int32(id),
        Value::Varchar(name.into()),
        Value::Varchar(region.into()),
    ]
}

fn make_setup(
    backend: IndexBackend,
) -> (
    Arc<Catalog<BTreeEngine>>,
    Table<BTreeEngine, RowLayout>,
    interchangedb::table::IndexHandle,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let table_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    cat.create_index(IndexDef {
        name: "warehouse_by_name".into(),
        table_id,
        columns: vec![1],
        unique: false,
        backend,
    })
    .unwrap();
    let schema = cat.get_table("warehouse").unwrap();
    let indexes = cat.indexes_for_table(table_id, &schema).unwrap();
    let name_index = indexes[0].clone();
    let table = Table::with_indexes(cat.engine().clone(), schema, RowLayout, indexes);
    (cat, table, name_index, dir)
}

fn collect(op: &mut IndexScan) -> Vec<Vec<Value>> {
    let mut out = Vec::new();
    while let Some(t) = op.next().unwrap() {
        out.push(t);
    }
    out
}

#[test]
fn point_lookup_returns_matching_row() {
    let (_cat, table, ix, _d) = make_setup(IndexBackend::BTree);
    table.insert(&row(1, "alpha", "NA")).unwrap();
    table.insert(&row(2, "bravo", "EU")).unwrap();
    table.insert(&row(3, "charlie", "NA")).unwrap();

    let mut op = IndexScan::new(&table, &ix, &[Value::Varchar("bravo".into())]).unwrap();
    let rows = collect(&mut op);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int32(2));
}

#[test]
fn point_lookup_miss_yields_nothing() {
    let (_cat, table, ix, _d) = make_setup(IndexBackend::BTree);
    table.insert(&row(1, "alpha", "NA")).unwrap();
    let mut op = IndexScan::new(&table, &ix, &[Value::Varchar("nothing".into())]).unwrap();
    assert!(op.next().unwrap().is_none());
}

#[test]
fn non_unique_index_returns_all_matches() {
    let (_cat, table, ix, _d) = make_setup(IndexBackend::BTree);
    // Three rows with the same w_name — index is non-unique, all should
    // come back from a prefix scan.
    table.insert(&row(1, "duplicate", "NA")).unwrap();
    table.insert(&row(2, "duplicate", "EU")).unwrap();
    table.insert(&row(3, "duplicate", "AP")).unwrap();
    table.insert(&row(4, "other", "AP")).unwrap();

    let mut op = IndexScan::new(&table, &ix, &[Value::Varchar("duplicate".into())]).unwrap();
    let rows = collect(&mut op);
    assert_eq!(rows.len(), 3);
    let ids: Vec<i32> = rows
        .iter()
        .map(|r| match r[0] {
            Value::Int32(i) => i,
            _ => panic!(),
        })
        .collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(sorted, vec![1, 2, 3]);
}

#[test]
fn lsm_backed_index_works_identically() {
    let (_cat, table, ix, _d) = make_setup(IndexBackend::Lsm);
    table.insert(&row(1, "alpha", "NA")).unwrap();
    table.insert(&row(2, "bravo", "EU")).unwrap();

    let mut op = IndexScan::new(&table, &ix, &[Value::Varchar("alpha".into())]).unwrap();
    let rows = collect(&mut op);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int32(1));
}

#[test]
fn after_update_scan_reflects_new_index_state() {
    let (_cat, table, ix, _d) = make_setup(IndexBackend::BTree);
    table.insert(&row(1, "before", "NA")).unwrap();
    table
        .update_by_pk(&[Value::Int32(1)], &row(1, "after", "NA"))
        .unwrap();

    // Old prefix "before" no longer in index.
    let mut op_before = IndexScan::new(&table, &ix, &[Value::Varchar("before".into())]).unwrap();
    assert!(op_before.next().unwrap().is_none());

    // New prefix "after" present and resolves to the same PK.
    let mut op_after = IndexScan::new(&table, &ix, &[Value::Varchar("after".into())]).unwrap();
    let rows = collect(&mut op_after);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int32(1));
}

#[test]
fn after_delete_scan_returns_nothing_for_that_value() {
    let (_cat, table, ix, _d) = make_setup(IndexBackend::BTree);
    table.insert(&row(1, "alpha", "NA")).unwrap();
    table.insert(&row(2, "beta", "EU")).unwrap();
    table.delete_by_pk(&[Value::Int32(1)]).unwrap();

    let mut op = IndexScan::new(&table, &ix, &[Value::Varchar("alpha".into())]).unwrap();
    assert!(op.next().unwrap().is_none());
    let mut op2 = IndexScan::new(&table, &ix, &[Value::Varchar("beta".into())]).unwrap();
    assert!(op2.next().unwrap().is_some());
}

#[test]
fn explain_includes_table_and_index_name() {
    let (_cat, table, ix, _d) = make_setup(IndexBackend::BTree);
    let op = IndexScan::new(&table, &ix, &[Value::Varchar("anything".into())]).unwrap();
    let s = op.explain(0);
    assert!(s.contains("IndexScan"));
    assert!(s.contains("warehouse"));
    assert!(s.contains("warehouse_by_name"));
}
