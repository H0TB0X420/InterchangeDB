//! P12.7: `NestedLoopJoin` operator tests.
//!
//! Hand-builds executor trees (no SQL surface yet — JOIN parsing lands
//! in a separate task) and asserts that the join operator emits the
//! expected concatenated rows.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{ColumnDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::{Executor, JoinStrategy, NestedLoopJoin, SeqScan};
use interchangedb::layout::RowLayout;
use interchangedb::storage::MemoryDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Value};

fn fresh_engine() -> (Arc<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(64, dm);
    (Arc::new(BTreeEngine::new(bpm).unwrap()), dir)
}

fn warehouse_schema(id: u32) -> Schema {
    Schema {
        name: format!("warehouse_{}", id),
        table_id: TableId(id),
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

fn district_schema(id: u32) -> Schema {
    Schema {
        name: format!("district_{}", id),
        table_id: TableId(id),
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
fn join_returns_pairs_satisfying_predicate() {
    let (engine, _d) = fresh_engine();
    let w_schema = Arc::new(warehouse_schema(1));
    let d_schema = Arc::new(district_schema(2));
    let w_table = Table::new(engine.clone(), w_schema.clone(), RowLayout);
    let d_table = Table::new(engine.clone(), d_schema.clone(), RowLayout);

    w_table
        .insert(&[Value::Int32(1), Value::Varchar("DC1".into())])
        .unwrap();
    w_table
        .insert(&[Value::Int32(2), Value::Varchar("DC2".into())])
        .unwrap();

    d_table
        .insert(&[
            Value::Int32(10),
            Value::Int32(1),
            Value::Varchar("d-east".into()),
        ])
        .unwrap();
    d_table
        .insert(&[
            Value::Int32(11),
            Value::Int32(1),
            Value::Varchar("d-west".into()),
        ])
        .unwrap();
    d_table
        .insert(&[
            Value::Int32(20),
            Value::Int32(2),
            Value::Varchar("d-only".into()),
        ])
        .unwrap();

    // Join condition: w.w_id == d.d_w_id (outer cols: [0,1], inner cols: [0,1,2])
    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let predicate: interchangedb::execution::JoinPredicate =
        Box::new(|outer_row, inner_row| outer_row[0] == inner_row[1]);
    let mut join = NestedLoopJoin::new(outer, inner, predicate).unwrap();

    let mut joined: Vec<Vec<Value>> = Vec::new();
    while let Some(t) = join.next().unwrap() {
        joined.push(t);
    }

    // 3 expected pairs: (w=1, d=10), (w=1, d=11), (w=2, d=20).
    assert_eq!(joined.len(), 3);
    // Verify schema is concatenated (2 + 3 = 5 columns).
    assert_eq!(join.schema().columns.len(), 5);
    // Each joined row carries outer || inner contents.
    for row in &joined {
        let w_id = match row[0] {
            Value::Int32(i) => i,
            _ => panic!(),
        };
        let d_w_id = match row[3] {
            Value::Int32(i) => i,
            _ => panic!(),
        };
        assert_eq!(w_id, d_w_id, "join predicate violated: {:?}", row);
    }
}

#[test]
fn join_with_no_matches_yields_nothing() {
    let (engine, _d) = fresh_engine();
    let w_schema = Arc::new(warehouse_schema(1));
    let d_schema = Arc::new(district_schema(2));
    let w_table = Table::new(engine.clone(), w_schema, RowLayout);
    let d_table = Table::new(engine.clone(), d_schema, RowLayout);
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
    let predicate: interchangedb::execution::JoinPredicate =
        Box::new(|outer_row, inner_row| outer_row[0] == inner_row[1]);
    let mut join = NestedLoopJoin::new(outer, inner, predicate).unwrap();
    assert!(join.next().unwrap().is_none());
}

#[test]
fn join_with_empty_outer_yields_nothing() {
    let (engine, _d) = fresh_engine();
    let w_schema = Arc::new(warehouse_schema(1));
    let d_schema = Arc::new(district_schema(2));
    let w_table = Table::new(engine.clone(), w_schema, RowLayout);
    let d_table = Table::new(engine.clone(), d_schema, RowLayout);
    // Outer empty, inner non-empty.
    d_table
        .insert(&[
            Value::Int32(10),
            Value::Int32(1),
            Value::Varchar("d".into()),
        ])
        .unwrap();

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let predicate: interchangedb::execution::JoinPredicate = Box::new(|_, _| true);
    let mut join = NestedLoopJoin::new(outer, inner, predicate).unwrap();
    assert!(join.next().unwrap().is_none());
}

#[test]
fn join_with_empty_inner_yields_nothing() {
    let (engine, _d) = fresh_engine();
    let w_schema = Arc::new(warehouse_schema(1));
    let d_schema = Arc::new(district_schema(2));
    let w_table = Table::new(engine.clone(), w_schema, RowLayout);
    let d_table = Table::new(engine.clone(), d_schema, RowLayout);
    w_table
        .insert(&[Value::Int32(1), Value::Varchar("DC1".into())])
        .unwrap();
    // Inner empty.

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let predicate: interchangedb::execution::JoinPredicate = Box::new(|_, _| true);
    let mut join = NestedLoopJoin::new(outer, inner, predicate).unwrap();
    assert!(join.next().unwrap().is_none());
}

#[test]
fn cartesian_product_when_predicate_is_constant_true() {
    let (engine, _d) = fresh_engine();
    let w_schema = Arc::new(warehouse_schema(1));
    let d_schema = Arc::new(district_schema(2));
    let w_table = Table::new(engine.clone(), w_schema, RowLayout);
    let d_table = Table::new(engine.clone(), d_schema, RowLayout);
    for i in 1..=3 {
        w_table
            .insert(&[Value::Int32(i), Value::Varchar("w".into())])
            .unwrap();
    }
    for i in 1..=4 {
        d_table
            .insert(&[Value::Int32(i), Value::Int32(i), Value::Varchar("d".into())])
            .unwrap();
    }

    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let predicate: interchangedb::execution::JoinPredicate = Box::new(|_, _| true);
    let mut join = NestedLoopJoin::new(outer, inner, predicate).unwrap();

    let mut count = 0;
    while join.next().unwrap().is_some() {
        count += 1;
    }
    assert_eq!(count, 3 * 4, "expected 3×4 = 12 cartesian-product rows");
}

#[test]
fn join_strategy_algorithm_name() {
    let (engine, _d) = fresh_engine();
    let w_schema = Arc::new(warehouse_schema(1));
    let d_schema = Arc::new(district_schema(2));
    let w_table = Table::new(engine.clone(), w_schema, RowLayout);
    let d_table = Table::new(engine.clone(), d_schema, RowLayout);
    let outer = Box::new(SeqScan::new(&w_table).unwrap());
    let inner = Box::new(SeqScan::new(&d_table).unwrap());
    let predicate: interchangedb::execution::JoinPredicate = Box::new(|_, _| true);
    let join = NestedLoopJoin::new(outer, inner, predicate).unwrap();
    assert_eq!(join.algorithm(), "nested-loop");
}

#[test]
fn join_disambiguates_duplicate_column_names_via_table_prefix() {
    // Both schemas have w_id-like columns? In our test, only d_w_id has
    // "d_w_id" — but let's exercise the schema concat with two tables
    // that DO share a name to confirm prefixing kicks in.
    let mut a = warehouse_schema(1);
    a.columns[0].name = "common".into();
    let mut b = district_schema(2);
    b.columns[0].name = "common".into();

    let (engine, _d) = fresh_engine();
    let a_table = Table::new(engine.clone(), Arc::new(a), RowLayout);
    let b_table = Table::new(engine.clone(), Arc::new(b), RowLayout);

    let outer = Box::new(SeqScan::new(&a_table).unwrap());
    let inner = Box::new(SeqScan::new(&b_table).unwrap());
    let predicate: interchangedb::execution::JoinPredicate = Box::new(|_, _| false);
    let join = NestedLoopJoin::new(outer, inner, predicate).unwrap();

    let names: Vec<&str> = join
        .schema()
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    // Outer "common" stays; inner "common" gets prefixed.
    assert_eq!(names[0], "common");
    assert!(
        names
            .iter()
            .any(|n| n.ends_with(".common") && n.contains("district")),
        "expected disambiguated inner column, got names: {:?}",
        names
    );
}
