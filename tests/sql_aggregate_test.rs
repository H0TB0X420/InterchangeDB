//! P13.4: SQL aggregate function parsing + binding + planning.
//!
//! `SELECT COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`,
//! `SUM/MIN/MAX/AVG(col)` from the SQL surface produce a plan that
//! includes the `HashAggregate` operator and runs to the right answer.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, Schema, TableId};
use interchangedb::execution::{ExecutionModel, Volcano};
use interchangedb::index::btree::BTreeEngine;
use interchangedb::layout::RowLayout;
use interchangedb::sql::{parse, plan, Binder, PhysicalPlan};
use interchangedb::storage::FileDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Decimal, Value};

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

    // Payments: (id PK Int32, amount Int64, price Decimal(10,2)).
    catalog
        .create_table(
            "payments".into(),
            Schema {
                name: "payments".into(),
                table_id: TableId(0),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        ty: ColumnType::Int32,
                        nullable: false,
                        default: None,
                    },
                    ColumnDef {
                        name: "amount".into(),
                        ty: ColumnType::Int64,
                        nullable: false,
                        default: None,
                    },
                    ColumnDef {
                        name: "price".into(),
                        ty: ColumnType::Decimal {
                            precision: 10,
                            scale: 2,
                        },
                        nullable: false,
                        default: None,
                    },
                ],
                primary_key: vec![0],
            },
        )
        .unwrap();

    let schema = catalog.get_table("payments").unwrap();
    let indexes = catalog.indexes_for_table(schema.table_id, &schema).unwrap();
    let table = Table::with_indexes(engine.clone(), schema, RowLayout, indexes);
    table
        .insert(&[
            Value::Int32(1),
            Value::Int64(10),
            Value::Decimal(Decimal::from_i64_with_scale(100, 2)),
        ])
        .unwrap();
    table
        .insert(&[
            Value::Int32(2),
            Value::Int64(30),
            Value::Decimal(Decimal::from_i64_with_scale(250, 2)),
        ])
        .unwrap();
    table
        .insert(&[
            Value::Int32(3),
            Value::Int64(20),
            Value::Decimal(Decimal::from_i64_with_scale(100, 2)),
        ])
        .unwrap();

    Setup {
        catalog,
        engine,
        _dir: dir,
    }
}

fn run_select(s: &Setup, sql: &str) -> Vec<Vec<Value>> {
    let stmts = parse(sql).unwrap();
    let binder = Binder::new(s.catalog.clone());
    let logical = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
    let physop = match plan(logical, &s.catalog).unwrap() {
        PhysicalPlan::Query(physop) => physop,
        _ => panic!("expected Query plan"),
    };
    let (_schema, rows) = Volcano.execute(&physop, &s.engine, &s.catalog).unwrap();
    rows
}

#[test]
fn count_star_returns_row_count() {
    let s = setup();
    let rows = run_select(&s, "SELECT COUNT(*) FROM payments");
    assert_eq!(rows, vec![vec![Value::Int64(3)]]);
}

#[test]
fn count_col_returns_non_null_count() {
    let s = setup();
    let rows = run_select(&s, "SELECT COUNT(amount) FROM payments");
    assert_eq!(rows, vec![vec![Value::Int64(3)]]);
}

#[test]
fn count_distinct_returns_unique_count() {
    let s = setup();
    // price values are 1.00, 2.50, 1.00 → 2 distinct.
    let rows = run_select(&s, "SELECT COUNT(DISTINCT price) FROM payments");
    assert_eq!(rows, vec![vec![Value::Int64(2)]]);
}

#[test]
fn sum_returns_total() {
    let s = setup();
    let rows = run_select(&s, "SELECT SUM(amount) FROM payments");
    assert_eq!(rows, vec![vec![Value::Int64(60)]]); // 10 + 30 + 20
}

#[test]
fn min_and_max_return_extremes() {
    let s = setup();
    let rows_min = run_select(&s, "SELECT MIN(amount) FROM payments");
    let rows_max = run_select(&s, "SELECT MAX(amount) FROM payments");
    assert_eq!(rows_min, vec![vec![Value::Int64(10)]]);
    assert_eq!(rows_max, vec![vec![Value::Int64(30)]]);
}

#[test]
fn avg_returns_decimal_scale_4() {
    let s = setup();
    let rows = run_select(&s, "SELECT AVG(amount) FROM payments");
    // (10 + 30 + 20) / 3 = 20.0000 → mantissa 200000 at scale 4.
    assert_eq!(
        rows,
        vec![vec![Value::Decimal(Decimal::from_i64_with_scale(
            200000, 4
        ))]]
    );
}

#[test]
fn aggregate_with_filter_runs_after_where() {
    let s = setup();
    let rows = run_select(&s, "SELECT SUM(amount) FROM payments WHERE id = 1");
    assert_eq!(rows, vec![vec![Value::Int64(10)]]);
}

#[test]
fn aggregate_filtered_to_empty_returns_null() {
    let s = setup();
    let rows = run_select(&s, "SELECT SUM(amount) FROM payments WHERE id = 999");
    assert_eq!(rows, vec![vec![Value::Null]]);
}

#[test]
fn multiple_aggregates_in_one_query() {
    let s = setup();
    let rows = run_select(
        &s,
        "SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM payments",
    );
    assert_eq!(
        rows,
        vec![vec![
            Value::Int64(3),
            Value::Int64(60),
            Value::Int64(10),
            Value::Int64(30),
        ]]
    );
}

#[test]
fn mixed_column_and_aggregate_errors_without_group_by() {
    let s = setup();
    // `SELECT id, COUNT(*) FROM payments` requires GROUP BY in standard SQL.
    let stmts = parse("SELECT id, COUNT(*) FROM payments").unwrap();
    let binder = Binder::new(s.catalog.clone());
    let result = binder.bind(stmts.into_iter().next().unwrap());
    assert!(
        matches!(result, Err(interchangedb::Error::SqlParse(ref m)) if m.contains("GROUP BY")),
        "expected GROUP-BY-required error, got: {:?}",
        result
    );
}

#[test]
fn unsupported_function_errors() {
    let s = setup();
    // We don't have UPPER (or any scalar function) yet.
    let stmts = parse("SELECT UPPER(amount) FROM payments").unwrap();
    let binder = Binder::new(s.catalog.clone());
    let result = binder.bind(stmts.into_iter().next().unwrap());
    assert!(
        matches!(result, Err(interchangedb::Error::SqlParse(_))),
        "expected SqlParse error, got: {:?}",
        result
    );
}

#[test]
fn sum_distinct_unsupported_errors() {
    let s = setup();
    let stmts = parse("SELECT SUM(DISTINCT amount) FROM payments").unwrap();
    let binder = Binder::new(s.catalog.clone());
    let result = binder.bind(stmts.into_iter().next().unwrap());
    assert!(
        matches!(result, Err(interchangedb::Error::SqlParse(ref m)) if m.contains("DISTINCT")),
        "expected SUM(DISTINCT …) rejection, got: {:?}",
        result
    );
}

#[test]
fn count_with_qualified_column_resolves() {
    let s = setup();
    let rows = run_select(&s, "SELECT COUNT(payments.amount) FROM payments");
    assert_eq!(rows, vec![vec![Value::Int64(3)]]);
}
