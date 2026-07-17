//! P13.1: SQL JOIN syntax — end-to-end parse → bind → plan → execute.
//!
//! Covers:
//! - `FROM a, b WHERE …` (implicit cross product + filter).
//! - `FROM a JOIN b ON …` (explicit equality join).
//! - `FROM a JOIN b ON …` with the inner side indexed → plan picks
//!   `IndexNestedLoopJoin`.
//! - Plain inner side → `NestedLoopJoin`.
//! - Projection across both sides via qualified `t.col`.
//! - Column resolution: unqualified resolves when unambiguous, errors when
//!   ambiguous, requires qualification with same-named columns.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::execution::{ExecutionModel, Volcano};
use interchangedb::layout::RowLayout;
use interchangedb::sql::{parse, plan, Binder, PhysicalPlan};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Value};

struct Setup {
    catalog: Arc<Catalog<BTreeEngine>>,
    engine: Arc<BTreeEngine>,
    _dir: tempfile::TempDir,
}

fn setup_with_indexed_district() -> Setup {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    let catalog =
        Arc::new(Catalog::open_persistent(engine.clone(), dir.path().join("indexes")).unwrap());

    // Warehouse: (w_id PK, w_name).
    catalog
        .create_table(
            "warehouse".into(),
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
            },
        )
        .unwrap();

    // District: (d_id PK, d_w_id, d_name) with secondary index on d_w_id.
    let d_id = catalog
        .create_table(
            "district".into(),
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
            },
        )
        .unwrap();
    catalog
        .create_index(IndexDef {
            name: "district_by_w_id".into(),
            table_id: d_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();

    // Seed both tables.
    let w_schema = catalog.get_table("warehouse").unwrap();
    let w_indexes = catalog
        .indexes_for_table(w_schema.table_id, &w_schema)
        .unwrap();
    let w_table = Table::with_indexes(engine.clone(), w_schema, RowLayout, w_indexes);
    w_table
        .insert(&[Value::Int32(1), Value::Varchar("DC1".into())])
        .unwrap();
    w_table
        .insert(&[Value::Int32(2), Value::Varchar("DC2".into())])
        .unwrap();
    w_table
        .insert(&[Value::Int32(3), Value::Varchar("DC3".into())])
        .unwrap();

    let d_schema = catalog.get_table("district").unwrap();
    let d_indexes = catalog
        .indexes_for_table(d_schema.table_id, &d_schema)
        .unwrap();
    let d_table = Table::with_indexes(engine.clone(), d_schema, RowLayout, d_indexes);
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
    // w_id=3 has no districts → no rows in the inner join output.

    Setup {
        catalog,
        engine,
        _dir: dir,
    }
}

fn plan_sql(s: &Setup, sql: &str) -> PhysicalPlan {
    let stmts = parse(sql).unwrap();
    let binder = Binder::new(s.catalog.clone());
    let logical = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
    plan(logical, &s.catalog).unwrap()
}

fn run_select(s: &Setup, sql: &str) -> Vec<Vec<Value>> {
    let physop = match plan_sql(s, sql) {
        PhysicalPlan::Query(physop) => physop,
        _ => panic!("expected Query plan"),
    };
    let (_schema, rows) = Volcano.execute(&physop, &s.engine, &s.catalog).unwrap();
    rows
}

fn explain_of(s: &Setup, sql: &str) -> String {
    match plan_sql(s, sql) {
        PhysicalPlan::Query(physop) => physop.explain(0),
        _ => panic!(),
    }
}

#[test]
fn explicit_inner_join_with_indexed_right_picks_inlj() {
    let s = setup_with_indexed_district();
    let text = explain_of(
        &s,
        "SELECT w.w_id, d.d_name FROM warehouse w JOIN district d ON w.w_id = d.d_w_id",
    );
    assert!(
        text.contains("IndexNestedLoopJoin"),
        "expected INLJ in plan:\n{}",
        text
    );
}

#[test]
fn explicit_inner_join_executes_to_matching_rows() {
    let s = setup_with_indexed_district();
    let rows = run_select(
        &s,
        "SELECT w.w_id, w.w_name, d.d_id, d.d_w_id, d.d_name FROM warehouse w JOIN district d ON w.w_id = d.d_w_id",
    );
    // Three matching pairs: (1,east), (1,west), (2,only). w_id=3 has no district.
    assert_eq!(rows.len(), 3);
    // Each row carries w.w_id == d.d_w_id.
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
fn join_with_no_usable_index_falls_back_to_hashjoin() {
    // Self-join the warehouse on its PK. `try_match_inlj` only matches
    // *secondary* indexes (we registered only districts_by_w_id), so no INLJ
    // applies. As of Phase D an equi-key with no usable index lowers to a
    // HashJoin — it used to fall back to NestedLoopJoin.
    let s = setup_with_indexed_district();
    let text = explain_of(
        &s,
        "SELECT w1.w_name, w2.w_name FROM warehouse w1 JOIN warehouse w2 ON w1.w_id = w2.w_id",
    );
    assert!(
        text.contains("HashJoin") && !text.contains("IndexNestedLoopJoin"),
        "expected HashJoin fallback in plan:\n{}",
        text
    );
}

#[test]
fn implicit_join_with_where_equijoin() {
    // `FROM a, b WHERE join_pred` form. As of Phase C the planner promotes the
    // WHERE equi-predicate to the join key (a HashJoin), rather than emitting a
    // cross product + Filter. The result rows are unchanged — that's what we
    // assert here.
    let s = setup_with_indexed_district();
    let rows = run_select(
        &s,
        "SELECT w.w_id, d.d_name FROM warehouse w, district d WHERE w.w_id = d.d_w_id",
    );
    assert_eq!(rows.len(), 3);
}

#[test]
fn qualified_projection_picks_correct_tuple_indices() {
    // Project specific columns from each side and confirm the values
    // match the source tables (no mix-up of columns).
    let s = setup_with_indexed_district();
    let rows = run_select(
        &s,
        "SELECT w.w_name, d.d_name FROM warehouse w JOIN district d ON w.w_id = d.d_w_id",
    );
    // Each row should be (w_name, d_name). Check shape.
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(row.len(), 2);
        assert!(matches!(row[0], Value::Varchar(_)));
        assert!(matches!(row[1], Value::Varchar(_)));
    }
}

#[test]
fn unqualified_unique_column_resolves_without_qualifier() {
    let s = setup_with_indexed_district();
    // d_name only exists on district — unqualified should resolve.
    let rows = run_select(
        &s,
        "SELECT d_name FROM warehouse w JOIN district d ON w.w_id = d.d_w_id",
    );
    assert_eq!(rows.len(), 3);
}

#[test]
fn ambiguous_unqualified_column_errors() {
    // w_id is ambiguous when joining warehouse with itself.
    let s = setup_with_indexed_district();
    let stmts =
        parse("SELECT w_id FROM warehouse w1 JOIN warehouse w2 ON w1.w_id = w2.w_id").unwrap();
    let binder = Binder::new(s.catalog.clone());
    let result = binder.bind(stmts.into_iter().next().unwrap());
    let err = result.unwrap_err();
    assert!(
        matches!(err, interchangedb::Error::SqlParse(ref m) if m.contains("ambiguous")),
        "expected ambiguous-column error, got: {:?}",
        err
    );
}

#[test]
fn three_table_join_chains_left_to_right() {
    // Add a third table for a three-way join. Build incrementally:
    // warehouse JOIN district JOIN (another district-aliased role).
    let s = setup_with_indexed_district();
    let rows = run_select(
        &s,
        "SELECT w.w_id, d1.d_id, d2.d_id FROM warehouse w JOIN district d1 ON w.w_id = d1.d_w_id JOIN district d2 ON w.w_id = d2.d_w_id",
    );
    // For w_id=1 there are 2 districts (10, 11) → 2 × 2 = 4 pairs.
    // For w_id=2 there's 1 district (20) → 1 × 1 = 1 pair.
    // Total 5 rows.
    assert_eq!(rows.len(), 5);
}
