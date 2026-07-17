//! P14.5: HashJoin operator tests.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{ColumnDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::{Executor, HashJoin, JoinStrategy, SeqScan};
use interchangedb::layout::RowLayout;
use interchangedb::storage::FileDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Value};

fn fresh_engine() -> (Arc<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    (Arc::new(BTreeEngine::new(bpm).unwrap()), dir)
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
        table_id: TableId(2),
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
fn hash_join_returns_matching_pairs() {
    let (engine, _d) = fresh_engine();
    let w_schema = Arc::new(warehouse_schema());
    let d_schema = Arc::new(district_schema());
    let w_table = Table::new(engine.clone(), w_schema.clone(), RowLayout);
    let d_table = Table::new(engine.clone(), d_schema.clone(), RowLayout);

    w_table
        .insert(&[Value::Int32(1), Value::Varchar("DC1".into())])
        .unwrap();
    w_table
        .insert(&[Value::Int32(2), Value::Varchar("DC2".into())])
        .unwrap();
    w_table
        .insert(&[Value::Int32(3), Value::Varchar("DC3".into())])
        .unwrap();

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
    // w_id=3 has no districts.

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    // Join key: outer.col 0 (w_id) = inner.col 1 (d_w_id).
    let mut join = HashJoin::new(outer, inner, 0, 1).unwrap();

    let mut rows = Vec::new();
    while let Some(t) = join.next().unwrap() {
        rows.push(t);
    }
    // 3 matching pairs: (w=1,d=10), (w=1,d=11), (w=2,d=20). w=3 → none.
    assert_eq!(rows.len(), 3);
    for row in &rows {
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
}

#[test]
fn hash_join_with_no_matches_yields_nothing() {
    let (engine, _d) = fresh_engine();
    let w_table = Table::new(engine.clone(), Arc::new(warehouse_schema()), RowLayout);
    let d_table = Table::new(engine.clone(), Arc::new(district_schema()), RowLayout);
    w_table
        .insert(&[Value::Int32(1), Value::Varchar("DC1".into())])
        .unwrap();
    d_table
        .insert(&[
            Value::Int32(10),
            Value::Int32(99),
            Value::Varchar("orphan".into()),
        ])
        .unwrap();

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let mut join = HashJoin::new(outer, inner, 0, 1).unwrap();
    assert!(join.next().unwrap().is_none());
}

#[test]
fn hash_join_with_empty_inner_yields_nothing() {
    let (engine, _d) = fresh_engine();
    let w_table = Table::new(engine.clone(), Arc::new(warehouse_schema()), RowLayout);
    let d_table = Table::new(engine.clone(), Arc::new(district_schema()), RowLayout);
    w_table
        .insert(&[Value::Int32(1), Value::Varchar("DC1".into())])
        .unwrap();
    // No inner rows.

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let mut join = HashJoin::new(outer, inner, 0, 1).unwrap();
    assert!(join.next().unwrap().is_none());
}

#[test]
fn hash_join_with_empty_outer_yields_nothing() {
    let (engine, _d) = fresh_engine();
    let w_table = Table::new(engine.clone(), Arc::new(warehouse_schema()), RowLayout);
    let d_table = Table::new(engine.clone(), Arc::new(district_schema()), RowLayout);
    d_table
        .insert(&[
            Value::Int32(10),
            Value::Int32(1),
            Value::Varchar("d".into()),
        ])
        .unwrap();
    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let mut join = HashJoin::new(outer, inner, 0, 1).unwrap();
    assert!(join.next().unwrap().is_none());
}

#[test]
fn hash_join_emits_one_row_per_match_for_many_to_one() {
    let (engine, _d) = fresh_engine();
    let w_table = Table::new(engine.clone(), Arc::new(warehouse_schema()), RowLayout);
    let d_table = Table::new(engine.clone(), Arc::new(district_schema()), RowLayout);
    // Five outer rows all pointing at the same inner key.
    for i in 1..=5 {
        w_table
            .insert(&[Value::Int32(i), Value::Varchar("w".into())])
            .unwrap();
    }
    // One inner row keyed at w_id = 3.
    d_table
        .insert(&[
            Value::Int32(10),
            Value::Int32(3),
            Value::Varchar("only_match".into()),
        ])
        .unwrap();

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let mut join = HashJoin::new(outer, inner, 0, 1).unwrap();
    let mut rows = Vec::new();
    while let Some(t) = join.next().unwrap() {
        rows.push(t);
    }
    // Only outer.w_id=3 should match.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int32(3));
}

#[test]
fn hash_join_one_to_many_outputs_cross_product_per_outer_row() {
    let (engine, _d) = fresh_engine();
    let w_table = Table::new(engine.clone(), Arc::new(warehouse_schema()), RowLayout);
    let d_table = Table::new(engine.clone(), Arc::new(district_schema()), RowLayout);
    // One outer row.
    w_table
        .insert(&[Value::Int32(1), Value::Varchar("DC1".into())])
        .unwrap();
    // Three inner rows all keyed at w_id = 1.
    for i in 10..13 {
        d_table
            .insert(&[Value::Int32(i), Value::Int32(1), Value::Varchar("x".into())])
            .unwrap();
    }
    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let mut join = HashJoin::new(outer, inner, 0, 1).unwrap();
    let mut count = 0;
    while join.next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 3);
}

#[test]
fn hash_join_skips_null_keys_on_both_sides() {
    // Use a schema where the inner join column is nullable so we can
    // insert NULL there.
    let null_inner = Arc::new(Schema {
        name: "ni".into(),
        table_id: TableId(3),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "k".into(),
                ty: ColumnType::Int32,
                nullable: true,
                default: None,
            },
        ],
        primary_key: vec![0],
    });
    let null_outer = Arc::new(Schema {
        name: "no".into(),
        table_id: TableId(4),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "k".into(),
                ty: ColumnType::Int32,
                nullable: true,
                default: None,
            },
            ColumnDef {
                name: "lbl".into(),
                ty: ColumnType::Varchar(8),
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    });
    let (engine, _d) = fresh_engine();
    let outer_t = Table::new(engine.clone(), null_outer.clone(), RowLayout);
    let inner_t = Table::new(engine.clone(), null_inner.clone(), RowLayout);
    // Outer: (id=1, k=1, "a"), (id=2, k=NULL, "b"). Inner: (10, 1), (11, NULL), (12, 1).
    outer_t
        .insert(&[Value::Int32(1), Value::Int32(1), Value::Varchar("a".into())])
        .unwrap();
    outer_t
        .insert(&[Value::Int32(2), Value::Null, Value::Varchar("b".into())])
        .unwrap();
    inner_t
        .insert(&[Value::Int32(10), Value::Int32(1)])
        .unwrap();
    inner_t.insert(&[Value::Int32(11), Value::Null]).unwrap();
    inner_t
        .insert(&[Value::Int32(12), Value::Int32(1)])
        .unwrap();

    let mut join = HashJoin::new(
        Box::new(SeqScan::new(&outer_t).unwrap()),
        Box::new(SeqScan::new(&inner_t).unwrap()),
        1, // outer key column: k
        1, // inner key column: k
    )
    .unwrap();
    let mut count = 0;
    while join.next().unwrap().is_some() {
        count += 1;
    }
    // Outer row k=1 matches inner k=1 twice → 2.
    // Outer row k=NULL → no match.
    // Inner row k=NULL → not in hash table.
    assert_eq!(count, 2);
}

#[test]
fn algorithm_name_is_hash() {
    let (engine, _d) = fresh_engine();
    let w_table = Table::new(engine.clone(), Arc::new(warehouse_schema()), RowLayout);
    let d_table = Table::new(engine.clone(), Arc::new(district_schema()), RowLayout);
    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let j = HashJoin::new(outer, inner, 0, 1).unwrap();
    assert_eq!(j.algorithm(), "hash");
}
