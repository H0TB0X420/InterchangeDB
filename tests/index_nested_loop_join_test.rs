//! P12.8: `IndexNestedLoopJoin` tests.
//!
//! Outer SeqScan over warehouse; inner indexed table (district) probed
//! by an index on the join column. Asserts:
//! - Correct join pairs returned.
//! - Algorithm name is `"index-nested-loop"`.
//! - Probe-key arity is validated up front.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::execution::{Executor, IndexNestedLoopJoin, JoinStrategy, SeqScan};
use interchangedb::index::btree::BTreeEngine;
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

fn district_schema() -> Schema {
    Schema {
        name: "district".into(),
        table_id: TableId(0),
        columns: vec![
            ColumnDef {
                name: "d_id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "d_w_id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "d_name".into(),
                ty: ColumnType::Varchar(20),
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    }
}

#[test]
fn index_nested_loop_join_produces_matching_pairs() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());

    // Warehouse — outer; SeqScan.
    let w_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    let w_schema = cat.get_table("warehouse").unwrap();
    let w_table = Table::with_indexes(
        cat.engine().clone(),
        w_schema,
        RowLayout,
        cat.indexes_for_table(w_id, &cat.get_table("warehouse").unwrap())
            .unwrap(),
    );
    w_table
        .insert(&[Value::Int32(1), Value::Varchar("DC1".into())])
        .unwrap();
    w_table
        .insert(&[Value::Int32(2), Value::Varchar("DC2".into())])
        .unwrap();
    w_table
        .insert(&[Value::Int32(3), Value::Varchar("DC3".into())])
        .unwrap();

    // District — inner; indexed on d_w_id (column 1).
    let d_id = cat
        .create_table("district".into(), district_schema())
        .unwrap();
    cat.create_index(IndexDef {
        name: "district_by_w_id".into(),
        table_id: d_id,
        columns: vec![1],
        unique: false,
        backend: IndexBackend::BTree,
    })
    .unwrap();
    let d_schema = cat.get_table("district").unwrap();
    let d_indexes = cat.indexes_for_table(d_id, &d_schema).unwrap();
    let d_index = d_indexes[0].clone();
    let d_table = Arc::new(Table::with_indexes(
        cat.engine().clone(),
        d_schema,
        RowLayout,
        d_indexes,
    ));
    d_table
        .insert(&[
            Value::Int32(10),
            Value::Int32(1),
            Value::Varchar("east".into()),
        ])
        .unwrap();
    d_table
        .insert(&[
            Value::Int32(11),
            Value::Int32(1),
            Value::Varchar("west".into()),
        ])
        .unwrap();
    d_table
        .insert(&[
            Value::Int32(20),
            Value::Int32(2),
            Value::Varchar("only".into()),
        ])
        .unwrap();
    // w_id=3 has no districts → that outer row should produce zero joined rows.

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    // Outer's column 0 (w_id) supplies the probe key.
    let mut join = IndexNestedLoopJoin::new(outer, d_table, d_index, vec![0]).unwrap();

    let mut joined: Vec<Vec<Value>> = Vec::new();
    while let Some(t) = join.next().unwrap() {
        joined.push(t);
    }

    // 3 expected pairs: (w=1, d=10), (w=1, d=11), (w=2, d=20).
    assert_eq!(joined.len(), 3);
    for row in &joined {
        let w_id = match row[0] {
            Value::Int32(i) => i,
            _ => panic!(),
        };
        let d_w_id = match row[3] {
            Value::Int32(i) => i,
            _ => panic!(),
        };
        assert_eq!(w_id, d_w_id);
    }

    assert_eq!(join.algorithm(), "index-nested-loop");
}

#[test]
fn arity_mismatch_errors_at_construction() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());

    let w_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    let w_table = Table::new(
        cat.engine().clone(),
        cat.get_table("warehouse").unwrap(),
        RowLayout,
    );

    let d_id = cat
        .create_table("district".into(), district_schema())
        .unwrap();
    cat.create_index(IndexDef {
        name: "district_by_w_id".into(),
        table_id: d_id,
        columns: vec![1],
        unique: false,
        backend: IndexBackend::BTree,
    })
    .unwrap();
    let d_indexes = cat
        .indexes_for_table(d_id, &cat.get_table("district").unwrap())
        .unwrap();
    let d_index = d_indexes[0].clone();
    let d_table = Arc::new(Table::with_indexes(
        cat.engine().clone(),
        cat.get_table("district").unwrap(),
        RowLayout,
        d_indexes,
    ));

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    // Pass two key columns to an index expecting one — must reject.
    let res = IndexNestedLoopJoin::new(outer, d_table, d_index, vec![0, 1]);
    assert!(
        matches!(res, Err(interchangedb::Error::IndexLookupArity { .. })),
        "expected IndexLookupArity error"
    );
    // Suppress unused warning.
    let _ = w_id;
}

#[test]
fn outer_with_no_inner_match_yields_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let w_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    let _ = w_id;
    let w_table = Table::new(
        cat.engine().clone(),
        cat.get_table("warehouse").unwrap(),
        RowLayout,
    );
    w_table
        .insert(&[Value::Int32(99), Value::Varchar("orphan".into())])
        .unwrap();

    let d_id = cat
        .create_table("district".into(), district_schema())
        .unwrap();
    cat.create_index(IndexDef {
        name: "district_by_w_id".into(),
        table_id: d_id,
        columns: vec![1],
        unique: false,
        backend: IndexBackend::BTree,
    })
    .unwrap();
    let d_indexes = cat
        .indexes_for_table(d_id, &cat.get_table("district").unwrap())
        .unwrap();
    let d_index = d_indexes[0].clone();
    let d_table = Arc::new(Table::with_indexes(
        cat.engine().clone(),
        cat.get_table("district").unwrap(),
        RowLayout,
        d_indexes,
    ));
    // No districts inserted.

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let mut join = IndexNestedLoopJoin::new(outer, d_table, d_index, vec![0]).unwrap();
    assert!(join.next().unwrap().is_none());
}
