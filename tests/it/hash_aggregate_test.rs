//! P13.2: `HashAggregate` operator tests (no GROUP BY yet).

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{ColumnDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::{AggregateFn, Executor, HashAggregate, SeqScan};
use interchangedb::layout::RowLayout;
use interchangedb::storage::FileDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Decimal, Value};

fn fresh_engine() -> (Arc<BTreeEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let dm = FileDiskManager::create(dir.path().join("test.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    (Arc::new(BTreeEngine::new(bpm).unwrap()), dir)
}

/// Schema: (id PK Int32, amount Int64, price Decimal(10,2), maybe_null Int64 nullable).
fn payments_schema() -> Schema {
    Schema {
        name: "payments".into(),
        table_id: TableId(1),
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
            ColumnDef {
                name: "maybe_null".into(),
                ty: ColumnType::Int64,
                nullable: true,
                default: None,
            },
        ],
        primary_key: vec![0],
    }
}

fn payments_table() -> (Table<BTreeEngine, RowLayout>, tempfile::TempDir) {
    let (engine, dir) = fresh_engine();
    let schema = Arc::new(payments_schema());
    (Table::new(engine, schema, RowLayout), dir)
}

fn row(id: i32, amount: i64, price_mantissa: i64, maybe: Option<i64>) -> Vec<Value> {
    vec![
        Value::Int32(id),
        Value::Int64(amount),
        Value::Decimal(Decimal::from_i64_with_scale(price_mantissa, 2)),
        maybe.map(Value::Int64).unwrap_or(Value::Null),
    ]
}

fn collect_one(op: &mut HashAggregate) -> Vec<Value> {
    let row = op.next().unwrap().expect("HashAggregate must emit one row");
    assert_eq!(
        op.next().unwrap(),
        None,
        "HashAggregate must emit exactly one row"
    );
    row
}

#[test]
fn count_star_on_empty_input_returns_zero() {
    let (table, _d) = payments_table();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::CountStar]).unwrap();
    let row = collect_one(&mut op);
    assert_eq!(row, vec![Value::Int64(0)]);
}

#[test]
fn sum_on_empty_input_returns_null() {
    let (table, _d) = payments_table();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Sum(1)]).unwrap();
    let row = collect_one(&mut op);
    assert_eq!(row, vec![Value::Null]);
}

// E13: SUM over Int64 must raise NumericOverflow instead of silently
// wrapping to a (confidently wrong) negative number.
#[test]
fn sum_int64_overflow_errors_instead_of_wrapping() {
    let (table, _d) = payments_table();
    table.insert(&row(1, i64::MAX, 100, None)).unwrap();
    table.insert(&row(2, 1, 100, None)).unwrap();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Sum(1)]).unwrap();
    let err = op.next().expect_err("SUM overflow must error");
    assert!(
        matches!(err, interchangedb::common::Error::NumericOverflow(_)),
        "expected NumericOverflow, got {:?}",
        err
    );
}

// E13: the AVG finalizer widens sum×10_000 in i128 and then narrows to
// the result Decimal's i64 mantissa — a near-i64::MAX average must error,
// not silently truncate the mantissa.
#[test]
fn avg_int64_mantissa_overflow_errors() {
    let (table, _d) = payments_table();
    table.insert(&row(1, i64::MAX, 100, None)).unwrap();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Avg(1)]).unwrap();
    let err = op.next().expect_err("AVG mantissa overflow must error");
    assert!(
        matches!(err, interchangedb::common::Error::NumericOverflow(_)),
        "expected NumericOverflow, got {:?}",
        err
    );
}

#[test]
fn count_sum_min_max_int64() {
    let (table, _d) = payments_table();
    table.insert(&row(1, 10, 100, None)).unwrap();
    table.insert(&row(2, 30, 200, None)).unwrap();
    table.insert(&row(3, 20, 150, None)).unwrap();

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(
        child,
        vec![
            AggregateFn::CountStar,
            AggregateFn::Sum(1),
            AggregateFn::Min(1),
            AggregateFn::Max(1),
        ],
    )
    .unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Int64(3)); // COUNT(*)
    assert_eq!(r[1], Value::Int64(60)); // SUM(amount)
    assert_eq!(r[2], Value::Int64(10)); // MIN
    assert_eq!(r[3], Value::Int64(30)); // MAX
}

#[test]
fn count_col_skips_nulls() {
    let (table, _d) = payments_table();
    table.insert(&row(1, 10, 100, Some(5))).unwrap();
    table.insert(&row(2, 20, 200, None)).unwrap(); // maybe_null = NULL
    table.insert(&row(3, 30, 150, Some(7))).unwrap();

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op =
        HashAggregate::new(child, vec![AggregateFn::CountStar, AggregateFn::Count(3)]).unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Int64(3)); // COUNT(*) = all rows
    assert_eq!(r[1], Value::Int64(2)); // COUNT(maybe_null) = non-NULL only
}

#[test]
fn sum_skips_null_inputs() {
    let (table, _d) = payments_table();
    table.insert(&row(1, 10, 100, Some(5))).unwrap();
    table.insert(&row(2, 20, 200, None)).unwrap();
    table.insert(&row(3, 30, 150, Some(7))).unwrap();

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Sum(3)]).unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Int64(12)); // 5 + 7
}

#[test]
fn min_max_decimal() {
    let (table, _d) = payments_table();
    table.insert(&row(1, 10, 1234, None)).unwrap(); // 12.34
    table.insert(&row(2, 20, 999, None)).unwrap(); //  9.99
    table.insert(&row(3, 30, 5000, None)).unwrap(); // 50.00

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Min(2), AggregateFn::Max(2)]).unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Decimal(Decimal::from_i64_with_scale(999, 2))); // 9.99
    assert_eq!(r[1], Value::Decimal(Decimal::from_i64_with_scale(5000, 2))); // 50.00
}

#[test]
fn sum_decimal_preserves_scale() {
    let (table, _d) = payments_table();
    table.insert(&row(1, 10, 100, None)).unwrap(); // 1.00
    table.insert(&row(2, 20, 250, None)).unwrap(); // 2.50
    table.insert(&row(3, 30, 175, None)).unwrap(); // 1.75

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Sum(2)]).unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Decimal(Decimal::from_i64_with_scale(525, 2))); // 5.25
}

#[test]
fn avg_int_yields_decimal_scale_4() {
    // AVG of integers promotes to Decimal(scale=4) so a non-integer mean
    // is representable. Single-row input: 100 / 1 = 100.0000.
    let (table, _d) = payments_table();
    table.insert(&row(1, 10, 100, None)).unwrap();
    table.insert(&row(2, 30, 100, None)).unwrap();

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Avg(1)]).unwrap();
    let r = collect_one(&mut op);
    // (10 + 30) / 2 = 20 → 20.0000 in scale-4 decimal → mantissa 200000.
    assert_eq!(
        r[0],
        Value::Decimal(Decimal::from_i64_with_scale(200000, 4))
    );
}

// E14: AVG rounds half away from zero instead of truncating —
// AVG([1, 2, 2]) = 1.6667 at scale 4, not the truncated 1.6666.
#[test]
fn avg_int_rounds_instead_of_truncating() {
    let (table, _d) = payments_table();
    table.insert(&row(1, 1, 100, None)).unwrap();
    table.insert(&row(2, 2, 100, None)).unwrap();
    table.insert(&row(3, 2, 100, None)).unwrap();

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Avg(1)]).unwrap();
    let r = collect_one(&mut op);
    // 5/3 = 1.66666… → mantissa round(50000/3) = 16667.
    assert_eq!(r[0], Value::Decimal(Decimal::from_i64_with_scale(16667, 4)));
}

#[test]
fn avg_on_empty_input_returns_null() {
    let (table, _d) = payments_table();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Avg(1)]).unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Null);
}

#[test]
fn output_schema_names_and_types() {
    let (table, _d) = payments_table();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let op = HashAggregate::new(
        child,
        vec![
            AggregateFn::CountStar,
            AggregateFn::Count(1),
            AggregateFn::Sum(1),
            AggregateFn::Min(2),
            AggregateFn::Avg(1),
        ],
    )
    .unwrap();
    let s = op.schema();
    assert_eq!(s.columns.len(), 5);
    assert_eq!(s.columns[0].name, "count_star");
    assert_eq!(s.columns[0].ty, ColumnType::Int64);
    assert_eq!(s.columns[1].name, "count_amount");
    assert_eq!(s.columns[1].ty, ColumnType::Int64);
    assert_eq!(s.columns[2].name, "sum_amount");
    assert_eq!(s.columns[2].ty, ColumnType::Int64);
    assert_eq!(s.columns[3].name, "min_price");
    assert_eq!(
        s.columns[3].ty,
        ColumnType::Decimal {
            precision: 10,
            scale: 2
        }
    );
    assert_eq!(s.columns[4].name, "avg_amount");
    assert_eq!(
        s.columns[4].ty,
        ColumnType::Decimal {
            precision: 18,
            scale: 4
        }
    );
}

#[test]
fn sum_int32_promotes_output_to_int64() {
    // Build a table with an Int32 column to confirm SUM output promotes.
    let schema = Arc::new(Schema {
        name: "small".into(),
        table_id: TableId(2),
        columns: vec![
            ColumnDef {
                name: "id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "n".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec![0],
    });
    let (engine, _d) = fresh_engine();
    let table = Table::new(engine, schema, RowLayout);
    table
        .insert(&[Value::Int32(1), Value::Int32(i32::MAX)])
        .unwrap();
    table.insert(&[Value::Int32(2), Value::Int32(1)]).unwrap();

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::Sum(1)]).unwrap();
    // i32::MAX + 1 = 2_147_483_648 — would overflow Int32 but fits Int64.
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Int64(i32::MAX as i64 + 1));
    assert_eq!(op.schema().columns[0].ty, ColumnType::Int64);
}

// ---- P13.3: COUNT(DISTINCT col) ----

#[test]
fn count_distinct_empty_input_returns_zero() {
    let (table, _d) = payments_table();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::CountDistinct(1)]).unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Int64(0));
}

#[test]
fn count_distinct_skips_nulls_and_dedups() {
    // Six rows; column 3 (maybe_null) has values: 5, NULL, 5, 7, NULL, 7.
    // Distinct non-null = {5, 7} → 2.
    let (table, _d) = payments_table();
    table.insert(&row(1, 10, 100, Some(5))).unwrap();
    table.insert(&row(2, 20, 100, None)).unwrap();
    table.insert(&row(3, 30, 100, Some(5))).unwrap();
    table.insert(&row(4, 40, 100, Some(7))).unwrap();
    table.insert(&row(5, 50, 100, None)).unwrap();
    table.insert(&row(6, 60, 100, Some(7))).unwrap();

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::CountDistinct(3)]).unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Int64(2));
}

#[test]
fn count_distinct_vs_count_disagree_when_duplicates_present() {
    // Tests both alongside each other so we lock down the *difference*.
    // 3 rows, all with amount = 100. COUNT(amount) = 3, COUNT(DISTINCT
    // amount) = 1.
    let (table, _d) = payments_table();
    table.insert(&row(1, 100, 100, None)).unwrap();
    table.insert(&row(2, 100, 100, None)).unwrap();
    table.insert(&row(3, 100, 100, None)).unwrap();

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(
        child,
        vec![AggregateFn::Count(1), AggregateFn::CountDistinct(1)],
    )
    .unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Int64(3));
    assert_eq!(r[1], Value::Int64(1));
}

#[test]
fn count_distinct_on_decimal_column_works() {
    // Hash-equality on Decimal: same mantissa + scale → same bucket.
    let (table, _d) = payments_table();
    table.insert(&row(1, 10, 1234, None)).unwrap(); // 12.34
    table.insert(&row(2, 20, 1234, None)).unwrap(); // 12.34 again
    table.insert(&row(3, 30, 5000, None)).unwrap(); // 50.00

    let child = Box::new(SeqScan::new(&table).unwrap());
    let mut op = HashAggregate::new(child, vec![AggregateFn::CountDistinct(2)]).unwrap();
    let r = collect_one(&mut op);
    assert_eq!(r[0], Value::Int64(2));
}

#[test]
fn count_distinct_output_schema_naming() {
    let (table, _d) = payments_table();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let op = HashAggregate::new(child, vec![AggregateFn::CountDistinct(1)]).unwrap();
    assert_eq!(op.schema().columns[0].name, "count_distinct_amount");
    assert_eq!(op.schema().columns[0].ty, ColumnType::Int64);
}

#[test]
fn count_distinct_explain_label() {
    let (table, _d) = payments_table();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let op = HashAggregate::new(child, vec![AggregateFn::CountDistinct(1)]).unwrap();
    let s = op.explain(0);
    assert!(s.contains("COUNT(DISTINCT 1)"), "got:\n{}", s);
}

#[test]
fn explain_includes_aggregate_labels() {
    let (table, _d) = payments_table();
    let child = Box::new(SeqScan::new(&table).unwrap());
    let op = HashAggregate::new(
        child,
        vec![
            AggregateFn::CountStar,
            AggregateFn::Sum(1),
            AggregateFn::Max(1),
        ],
    )
    .unwrap();
    let s = op.explain(0);
    assert!(s.contains("HashAggregate"));
    assert!(s.contains("COUNT(*)"));
    assert!(s.contains("SUM(1)"));
    assert!(s.contains("MAX(1)"));
    assert!(s.contains("SeqScan"), "should include child operator");
}
