//! Phase 9 integration tests: full Catalog + Table + RowLayout stack
//! exercised end-to-end against both `BTreeEngine` and `LsmEngine`.
//!
//! These are the "if every layer works individually, do they work together?"
//! tests. They use a TPC-C-style WAREHOUSE schema as a realistic exemplar.
//!
//! V1's per-engine pattern: file-per-engine. We use functions that take a
//! `StorageEngine`-implementor closure, then tests instantiate the closure
//! over each engine — modest duplication at the test-file boundary, which
//! V1 already accepts.

use std::sync::Arc;

use tempfile::TempDir;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexDef, Schema, TableId};
use interchangedb::common::Error;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::index::lsm::LsmEngine;
use interchangedb::layout::RowLayout;
use interchangedb::storage::DiskManager;
use interchangedb::storage::StorageEngine;
use interchangedb::table::Table;
use interchangedb::types::{ColumnType, Decimal, Value};

// ---- engine fixtures ------------------------------------------------------

fn fresh_btree() -> (Arc<BTreeEngine>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let dm = DiskManager::create(&path).unwrap();
    let bpm = BufferPoolManager::new(256, dm);
    (Arc::new(BTreeEngine::new(bpm).unwrap()), dir)
}

fn fresh_lsm() -> (Arc<LsmEngine>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (Arc::new(LsmEngine::new(dir.path()).unwrap()), dir)
}

// ---- TPC-C WAREHOUSE schema ----------------------------------------------

/// TPC-C WAREHOUSE table schema, simplified to V2 type set:
///   w_id      Int32   PK
///   w_name    Varchar(10)
///   w_street  Varchar(20)
///   w_city    Varchar(20)
///   w_zip     Char(9)
///   w_tax     Decimal(4,4)
///   w_ytd     Decimal(12,2)  default 0
fn warehouse_schema() -> Schema {
    Schema {
        name: "warehouse".into(),
        table_id: TableId(0), // catalog assigns
        columns: vec![
            ColumnDef {
                name: "w_id".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_name".into(),
                ty: ColumnType::Varchar(10),
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_street".into(),
                ty: ColumnType::Varchar(20),
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_city".into(),
                ty: ColumnType::Varchar(20),
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_zip".into(),
                ty: ColumnType::Char(9),
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_tax".into(),
                ty: ColumnType::Decimal {
                    precision: 4,
                    scale: 4,
                },
                nullable: false,
                default: None,
            },
            ColumnDef {
                name: "w_ytd".into(),
                ty: ColumnType::Decimal {
                    precision: 12,
                    scale: 2,
                },
                nullable: false,
                default: Some(Value::Decimal(Decimal::from_i64_with_scale(0, 2))),
            },
        ],
        primary_key: vec![0],
    }
}

fn warehouse_row(id: i32, name: &str) -> Vec<Value> {
    vec![
        Value::Int32(id),
        Value::Varchar(name.into()),
        Value::Varchar("123 Main".into()),
        Value::Varchar("Anytown".into()),
        Value::Char("12345----".into()), // padded to 9
        Value::Decimal(Decimal::from_i64_with_scale(750, 4)), // 7.50%
        Value::Decimal(Decimal::from_i64_with_scale(300_000_00, 2)), // 300,000.00
    ]
}

// ---- generic scenarios ----------------------------------------------------

/// Bootstrap an engine into a catalog: system tables present, no user tables.
fn scenario_bootstrap_empty_engine<E: StorageEngine + 'static>(engine: Arc<E>) {
    let catalog = Catalog::open(engine).unwrap();
    let names = catalog.list_tables();
    assert_eq!(names.len(), 3, "fresh catalog should have 3 system tables");
    for sys in ["__sys_tables", "__sys_columns", "__sys_indexes"] {
        assert!(names.contains(&sys.to_string()), "missing {}", sys);
    }
}

/// Create a TPC-C WAREHOUSE table, insert one row, get it back byte-equal.
fn scenario_tpcc_warehouse_create_insert_lookup<E: StorageEngine + 'static>(engine: Arc<E>) {
    let catalog = Catalog::open(engine.clone()).unwrap();
    let table_id = catalog
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();

    let schema = catalog.get_table("warehouse").unwrap();
    assert_eq!(schema.table_id, table_id);

    let table = Table::new(engine, schema, RowLayout);
    let row = warehouse_row(1, "DC1");
    table.insert(&row).unwrap();

    let back = table.get_by_pk(&[Value::Int32(1)]).unwrap();
    assert_eq!(back, Some(row));
}

/// Insert N rows in scrambled order, scan returns them PK-sorted.
fn scenario_insert_many_scan_sorted<E: StorageEngine + 'static>(engine: Arc<E>) {
    let catalog = Catalog::open(engine.clone()).unwrap();
    catalog
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    let schema = catalog.get_table("warehouse").unwrap();
    let table = Table::new(engine, schema, RowLayout);

    for id in [3, 1, 4, 1, 5, 9, 2, 6, 5, 3] {
        // upsert dedupes — final set will be {1, 2, 3, 4, 5, 6, 9}.
        table.upsert(&warehouse_row(id, "DC")).unwrap();
    }

    let rows = table.scan().unwrap();
    let ids: Vec<i32> = rows
        .iter()
        .map(|r| match r[0] {
            Value::Int32(n) => n,
            _ => panic!("expected Int32"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 9]);
}

/// Two user tables with the same encoded PK don't collide (table_id prefix
/// in storage keys does its job).
fn scenario_multi_table_isolation<E: StorageEngine + 'static>(engine: Arc<E>) {
    let catalog = Catalog::open(engine.clone()).unwrap();
    catalog
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    let mut alt = warehouse_schema();
    alt.name = "warehouse_alt".into();
    catalog.create_table("warehouse_alt".into(), alt).unwrap();

    let t1 = Table::new(
        engine.clone(),
        catalog.get_table("warehouse").unwrap(),
        RowLayout,
    );
    let t2 = Table::new(
        engine.clone(),
        catalog.get_table("warehouse_alt").unwrap(),
        RowLayout,
    );

    t1.insert(&warehouse_row(1, "T1")).unwrap();
    t2.insert(&warehouse_row(1, "T2")).unwrap();

    let r1 = t1.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap();
    let r2 = t2.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap();
    assert_eq!(r1[1], Value::Varchar("T1".into()));
    assert_eq!(r2[1], Value::Varchar("T2".into()));
}

/// Constraint violations propagate from Table to caller as typed errors.
fn scenario_constraint_violations_surface<E: StorageEngine + 'static>(engine: Arc<E>) {
    let catalog = Catalog::open(engine.clone()).unwrap();
    catalog
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    let schema = catalog.get_table("warehouse").unwrap();
    let table = Table::new(engine, schema, RowLayout);

    // Wrong arity.
    let err = table.insert(&[Value::Int32(1)]).unwrap_err();
    assert!(matches!(err, Error::ConstraintViolation { .. }));

    // Insert a valid row, then duplicate-key error.
    table.insert(&warehouse_row(1, "DC")).unwrap();
    let err = table.insert(&warehouse_row(1, "DC")).unwrap_err();
    assert!(matches!(err, Error::DuplicateKey { .. }));
}

/// Indexes register in __sys_indexes (Phase 9 has no maintenance, just
/// metadata persistence).
fn scenario_index_metadata_persists<E: StorageEngine + 'static>(engine: Arc<E>) {
    let catalog = Catalog::open(engine).unwrap();
    let table_id = catalog
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    let id = catalog
        .create_index(IndexDef {
            name: "warehouse_by_name".into(),
            table_id,
            columns: vec![1],
            unique: false,
        })
        .unwrap();
    assert_eq!(id.0, 1);

    // Duplicate index name fails.
    let err = catalog
        .create_index(IndexDef {
            name: "warehouse_by_name".into(),
            table_id,
            columns: vec![2],
            unique: false,
        })
        .unwrap_err();
    assert!(matches!(err, Error::IndexAlreadyExists { .. }));
}

// ---- BTreeEngine tests ----------------------------------------------------

#[test]
fn btree_bootstrap_empty_engine() {
    let (engine, _dir) = fresh_btree();
    scenario_bootstrap_empty_engine(engine);
}

#[test]
fn btree_tpcc_warehouse_create_insert_lookup() {
    let (engine, _dir) = fresh_btree();
    scenario_tpcc_warehouse_create_insert_lookup(engine);
}

#[test]
fn btree_insert_many_scan_sorted() {
    let (engine, _dir) = fresh_btree();
    scenario_insert_many_scan_sorted(engine);
}

#[test]
fn btree_multi_table_isolation() {
    let (engine, _dir) = fresh_btree();
    scenario_multi_table_isolation(engine);
}

#[test]
fn btree_constraint_violations_surface() {
    let (engine, _dir) = fresh_btree();
    scenario_constraint_violations_surface(engine);
}

#[test]
fn btree_index_metadata_persists() {
    let (engine, _dir) = fresh_btree();
    scenario_index_metadata_persists(engine);
}

/// BTreeEngine specific: reopen recovers user tables. (LsmEngine reopen has
/// different semantics around manifest replay; not tested here.)
#[test]
fn btree_reopen_recovers_user_tables() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    {
        let dm = DiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let catalog = Catalog::open(engine.clone()).unwrap();
        catalog
            .create_table("warehouse".into(), warehouse_schema())
            .unwrap();
        let schema = catalog.get_table("warehouse").unwrap();
        let table = Table::new(engine.clone(), schema, RowLayout);
        table.insert(&warehouse_row(42, "DC42")).unwrap();
        engine.flush().unwrap();
    }
    let dm = DiskManager::open(&path).unwrap();
    let bpm = BufferPoolManager::new(256, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    let catalog = Catalog::open(engine.clone()).unwrap();
    let schema = catalog.get_table("warehouse").unwrap();
    let table = Table::new(engine, schema, RowLayout);

    let back = table.get_by_pk(&[Value::Int32(42)]).unwrap().unwrap();
    assert_eq!(back[1], Value::Varchar("DC42".into()));
}

// ---- LsmEngine tests ------------------------------------------------------

#[test]
fn lsm_bootstrap_empty_engine() {
    let (engine, _dir) = fresh_lsm();
    scenario_bootstrap_empty_engine(engine);
}

#[test]
fn lsm_tpcc_warehouse_create_insert_lookup() {
    let (engine, _dir) = fresh_lsm();
    scenario_tpcc_warehouse_create_insert_lookup(engine);
}

#[test]
fn lsm_insert_many_scan_sorted() {
    let (engine, _dir) = fresh_lsm();
    scenario_insert_many_scan_sorted(engine);
}

#[test]
fn lsm_multi_table_isolation() {
    let (engine, _dir) = fresh_lsm();
    scenario_multi_table_isolation(engine);
}

#[test]
fn lsm_constraint_violations_surface() {
    let (engine, _dir) = fresh_lsm();
    scenario_constraint_violations_surface(engine);
}

#[test]
fn lsm_index_metadata_persists() {
    let (engine, _dir) = fresh_lsm();
    scenario_index_metadata_persists(engine);
}
