//! P13.5: `Sort` operator tests.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{ColumnDef, Schema, TableId};
use interchangedb::execution::{Executor, SeqScan, Sort, SortDir};
use interchangedb::index::btree::BTreeEngine;
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

fn customer_schema() -> Schema {
    Schema {
        name: "customer".into(),
        table_id: TableId(1),
        columns: vec![
            ColumnDef { name: "id".into(), ty: ColumnType::Int32, nullable: false, default: None },
            ColumnDef { name: "last".into(), ty: ColumnType::Varchar(20), nullable: false, default: None },
            ColumnDef { name: "first".into(), ty: ColumnType::Varchar(20), nullable: true, default: None },
        ],
        primary_key: vec![0],
    }
}

fn customer_table() -> (Table<BTreeEngine, RowLayout>, tempfile::TempDir) {
    let (e, d) = fresh_engine();
    (Table::new(e, Arc::new(customer_schema()), RowLayout), d)
}

fn row(id: i32, last: &str, first: Option<&str>) -> Vec<Value> {
    vec![
        Value::Int32(id),
        Value::Varchar(last.into()),
        first.map(|s| Value::Varchar(s.into())).unwrap_or(Value::Null),
    ]
}

fn drain(op: &mut Sort) -> Vec<Vec<Value>> {
    let mut out = Vec::new();
    while let Some(t) = op.next().unwrap() {
        out.push(t);
    }
    out
}

#[test]
fn empty_input_yields_nothing() {
    let (t, _d) = customer_table();
    let child = Box::new(SeqScan::new(&t).unwrap());
    let mut op = Sort::new(child, vec![(0, SortDir::Asc)]).unwrap();
    assert!(op.next().unwrap().is_none());
}

#[test]
fn single_key_ascending() {
    let (t, _d) = customer_table();
    t.insert(&row(3, "smith", Some("c"))).unwrap();
    t.insert(&row(1, "jones", Some("a"))).unwrap();
    t.insert(&row(2, "kim", Some("b"))).unwrap();
    let child = Box::new(SeqScan::new(&t).unwrap());
    let mut op = Sort::new(child, vec![(0, SortDir::Asc)]).unwrap();
    let rows = drain(&mut op);
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn single_key_descending() {
    let (t, _d) = customer_table();
    t.insert(&row(3, "smith", None)).unwrap();
    t.insert(&row(1, "jones", None)).unwrap();
    t.insert(&row(2, "kim", None)).unwrap();
    let child = Box::new(SeqScan::new(&t).unwrap());
    let mut op = Sort::new(child, vec![(0, SortDir::Desc)]).unwrap();
    let rows = drain(&mut op);
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    assert_eq!(ids, vec![3, 2, 1]);
}

#[test]
fn multi_key_breaks_ties_by_second_key() {
    let (t, _d) = customer_table();
    // Two customers named "kim" — tie on last name, broken by first.
    t.insert(&row(1, "kim", Some("zoe"))).unwrap();
    t.insert(&row(2, "kim", Some("abe"))).unwrap();
    t.insert(&row(3, "smith", Some("bob"))).unwrap();
    let child = Box::new(SeqScan::new(&t).unwrap());
    // ORDER BY last ASC, first ASC.
    let mut op = Sort::new(child, vec![(1, SortDir::Asc), (2, SortDir::Asc)]).unwrap();
    let rows = drain(&mut op);
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    // kim/abe (2), kim/zoe (1), smith/bob (3).
    assert_eq!(ids, vec![2, 1, 3]);
}

#[test]
fn nulls_sort_last_under_asc() {
    let (t, _d) = customer_table();
    t.insert(&row(1, "kim", Some("a"))).unwrap();
    t.insert(&row(2, "kim", None)).unwrap();      // NULL first name
    t.insert(&row(3, "kim", Some("b"))).unwrap();
    let child = Box::new(SeqScan::new(&t).unwrap());
    // ORDER BY first ASC.
    let mut op = Sort::new(child, vec![(2, SortDir::Asc)]).unwrap();
    let rows = drain(&mut op);
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    // 'a', 'b', NULL.
    assert_eq!(ids, vec![1, 3, 2]);
}

#[test]
fn nulls_sort_last_under_desc() {
    let (t, _d) = customer_table();
    t.insert(&row(1, "kim", Some("a"))).unwrap();
    t.insert(&row(2, "kim", None)).unwrap();
    t.insert(&row(3, "kim", Some("b"))).unwrap();
    let child = Box::new(SeqScan::new(&t).unwrap());
    let mut op = Sort::new(child, vec![(2, SortDir::Desc)]).unwrap();
    let rows = drain(&mut op);
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    // DESC: 'b', 'a', then NULL (still last — convention).
    assert_eq!(ids, vec![3, 1, 2]);
}

#[test]
fn stable_sort_preserves_original_order_on_ties() {
    // Three rows all with last = "kim" and the same first. Insertion
    // order: id 1, 2, 3. Sort by (last) should yield 1, 2, 3 due to
    // sort_by's stability.
    let (t, _d) = customer_table();
    t.insert(&row(1, "kim", Some("same"))).unwrap();
    t.insert(&row(2, "kim", Some("same"))).unwrap();
    t.insert(&row(3, "kim", Some("same"))).unwrap();
    let child = Box::new(SeqScan::new(&t).unwrap());
    let mut op = Sort::new(child, vec![(1, SortDir::Asc)]).unwrap();
    let rows = drain(&mut op);
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn schema_matches_child_schema() {
    let (t, _d) = customer_table();
    let child = Box::new(SeqScan::new(&t).unwrap());
    let op = Sort::new(child, vec![(0, SortDir::Asc)]).unwrap();
    assert_eq!(op.schema().columns.len(), 3);
    assert_eq!(op.schema().columns[0].name, "id");
}

#[test]
fn explain_lists_sort_keys() {
    let (t, _d) = customer_table();
    let child = Box::new(SeqScan::new(&t).unwrap());
    let op = Sort::new(child, vec![(1, SortDir::Asc), (2, SortDir::Desc)]).unwrap();
    let s = op.explain(0);
    assert!(s.contains("Sort["));
    assert!(s.contains("1 ASC"));
    assert!(s.contains("2 DESC"));
    assert!(s.contains("SeqScan"));
}

#[test]
fn no_keys_errors_at_construction() {
    let (t, _d) = customer_table();
    let child = Box::new(SeqScan::new(&t).unwrap());
    let res = Sort::new(child, vec![]);
    assert!(res.is_err(), "Sort with no keys should error");
}
