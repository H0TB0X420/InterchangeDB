//! P13.6: SQL `ORDER BY` parsing + binding + planning + execution.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, Schema, TableId};
use interchangedb::index::btree::BTreeEngine;
use interchangedb::layout::RowLayout;
use interchangedb::sql::{parse, plan, Binder, PhysicalPlan};
use interchangedb::storage::FileDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Value};

struct Setup {
    catalog: Arc<Catalog<BTreeEngine>>,
    engine: Arc<BTreeEngine>,
    _dir: tempfile::TempDir,
}

fn setup() -> Setup {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("cat.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    let catalog =
        Arc::new(Catalog::open_persistent(engine.clone(), dir.path().join("indexes")).unwrap());

    catalog
        .create_table(
            "customer".into(),
            Schema {
                name: "customer".into(),
                table_id: TableId(0),
                columns: vec![
                    ColumnDef { name: "id".into(), ty: ColumnType::Int32, nullable: false, default: None },
                    ColumnDef { name: "last".into(), ty: ColumnType::Varchar(20), nullable: false, default: None },
                    ColumnDef { name: "first".into(), ty: ColumnType::Varchar(20), nullable: true, default: None },
                ],
                primary_key: vec![0],
            },
        )
        .unwrap();

    let schema = catalog.get_table("customer").unwrap();
    let indexes = catalog.indexes_for_table(schema.table_id, &schema).unwrap();
    let table = Table::with_indexes(engine.clone(), schema, RowLayout, indexes);
    // Insert in scrambled order so the result-order assertions exercise Sort.
    table.insert(&[Value::Int32(3), Value::Varchar("smith".into()), Value::Varchar("c".into())]).unwrap();
    table.insert(&[Value::Int32(1), Value::Varchar("jones".into()), Value::Varchar("a".into())]).unwrap();
    table.insert(&[Value::Int32(2), Value::Varchar("kim".into()),   Value::Varchar("b".into())]).unwrap();
    table.insert(&[Value::Int32(4), Value::Varchar("kim".into()),   Value::Null]).unwrap();

    Setup { catalog, engine, _dir: dir }
}

fn run_select(s: &Setup, sql: &str) -> Vec<Vec<Value>> {
    let stmts = parse(sql).unwrap();
    let binder = Binder::new(s.catalog.clone());
    let logical = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
    let p = plan(logical, s.engine.clone(), &s.catalog).unwrap();
    let mut exec = match p {
        PhysicalPlan::Executor(e) => e,
        _ => panic!(),
    };
    let mut out = Vec::new();
    while let Some(t) = exec.next().unwrap() {
        out.push(t);
    }
    out
}

#[test]
fn order_by_single_column_asc() {
    let s = setup();
    let rows = run_select(&s, "SELECT * FROM customer ORDER BY id ASC");
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[test]
fn order_by_single_column_desc() {
    let s = setup();
    let rows = run_select(&s, "SELECT * FROM customer ORDER BY id DESC");
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    assert_eq!(ids, vec![4, 3, 2, 1]);
}

#[test]
fn order_by_default_direction_is_asc() {
    let s = setup();
    // No ASC/DESC modifier — should default to ASC.
    let rows = run_select(&s, "SELECT * FROM customer ORDER BY id");
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[test]
fn order_by_two_keys_breaks_ties() {
    let s = setup();
    // Two "kim" rows — last name ties, first name tiebreaks.
    // first values: id 2 → "b", id 4 → NULL. NULLs sort last under ASC.
    let rows = run_select(&s, "SELECT * FROM customer ORDER BY last ASC, first ASC");
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    // jones (1), kim/b (2), kim/NULL (4), smith (3).
    assert_eq!(ids, vec![1, 2, 4, 3]);
}

#[test]
fn order_by_with_limit_takes_top_n() {
    let s = setup();
    let rows = run_select(&s, "SELECT * FROM customer ORDER BY id DESC LIMIT 2");
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    assert_eq!(ids, vec![4, 3]);
}

#[test]
fn order_by_with_where_runs_after_filter() {
    let s = setup();
    let rows = run_select(&s, "SELECT * FROM customer WHERE last = 'kim' ORDER BY id ASC");
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn order_by_unknown_column_errors() {
    let s = setup();
    let stmts = parse("SELECT * FROM customer ORDER BY nonexistent ASC").unwrap();
    let binder = Binder::new(s.catalog.clone());
    let res = binder.bind(stmts.into_iter().next().unwrap());
    assert!(matches!(res, Err(interchangedb::Error::SqlParse(_))));
}

#[test]
fn order_by_with_qualified_column() {
    let s = setup();
    let rows = run_select(&s, "SELECT * FROM customer ORDER BY customer.id ASC");
    let ids: Vec<i32> = rows.iter().map(|r| match r[0] { Value::Int32(i) => i, _ => panic!() }).collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}
