//! P14.2: ANALYZE TABLE end-to-end.
//!
//! Builds a Session, populates a table, runs `ANALYZE TABLE t` through
//! the SQL surface, then reads stats back out via the catalog accessors.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::system_tables::HISTOGRAM_KIND_EQUI_WIDTH_INT;
use interchangedb::catalog::{Catalog, TableId};
use interchangedb::database::Database;
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::session::{QueryResult, Session};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::wal::SyncMode;

fn setup() -> (
    Session<BTreeEngine>,
    Arc<Catalog<BTreeEngine>>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(512, dm);
    let engine = BTreeEngine::new(bpm).unwrap();
    let database =
        Arc::new(Database::open_with_sync_mode(dir.path(), engine, SyncMode::NoSync).unwrap());
    let catalog = Arc::new(Catalog::open(database.engine_arc().clone()).unwrap());
    let session = Session::new(database, catalog.clone());
    (session, catalog, dir)
}

fn table_id(catalog: &Catalog<BTreeEngine>, name: &str) -> TableId {
    catalog.get_table(name).unwrap().table_id
}

#[test]
fn analyze_empty_table_writes_zero_row_count() {
    let (mut s, cat, _d) = setup();
    s.execute("CREATE TABLE t (id INT PRIMARY KEY, balance BIGINT)")
        .unwrap();
    let r = s.execute("ANALYZE TABLE t").unwrap();
    assert!(matches!(r, QueryResult::Ack));

    let tid = table_id(&cat, "t");
    let table_stats = cat.get_table_stats(tid).unwrap().unwrap();
    assert_eq!(table_stats.row_count, 0);

    // Per-column stats present, NDV = 0.
    let c0 = cat.get_column_stats(tid, 0).unwrap().unwrap();
    let c1 = cat.get_column_stats(tid, 1).unwrap().unwrap();
    assert_eq!(c0.ndv, 0);
    assert_eq!(c1.ndv, 0);
    assert_eq!(c0.null_count, 0);
    assert_eq!(c1.null_count, 0);
}

#[test]
fn analyze_counts_rows_and_ndv() {
    let (mut s, cat, _d) = setup();
    s.execute("CREATE TABLE t (id INT PRIMARY KEY, kind INT)")
        .unwrap();
    for i in 1..=10 {
        // kind cycles 0,1,2,0,1,2,... → 3 distinct values across 10 rows.
        s.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i % 3))
            .unwrap();
    }
    s.execute("ANALYZE TABLE t").unwrap();

    let tid = table_id(&cat, "t");
    let table_stats = cat.get_table_stats(tid).unwrap().unwrap();
    assert_eq!(table_stats.row_count, 10);

    let c_id = cat.get_column_stats(tid, 0).unwrap().unwrap();
    let c_kind = cat.get_column_stats(tid, 1).unwrap().unwrap();
    assert_eq!(c_id.ndv, 10); // every id distinct
    assert_eq!(c_kind.ndv, 3); // 0, 1, 2 only
}

#[test]
fn analyze_writes_equi_width_histogram_for_int_columns() {
    let (mut s, cat, _d) = setup();
    s.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    for i in 1..=16 {
        s.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }
    s.execute("ANALYZE TABLE t").unwrap();

    let tid = table_id(&cat, "t");
    let c = cat.get_column_stats(tid, 0).unwrap().unwrap();
    assert_eq!(c.histogram_kind, HISTOGRAM_KIND_EQUI_WIDTH_INT);
    assert!(!c.histogram_blob.is_empty());

    // Decode header: min:i64 | max:i64 | num_buckets:u16.
    let min = i64::from_le_bytes(c.histogram_blob[0..8].try_into().unwrap());
    let max = i64::from_le_bytes(c.histogram_blob[8..16].try_into().unwrap());
    let nb = u16::from_le_bytes(c.histogram_blob[16..18].try_into().unwrap()) as usize;
    assert_eq!(min, 1);
    assert_eq!(max, 16);
    assert_eq!(nb, 16);

    // Read bucket counts; sum must equal row count.
    let mut total: u32 = 0;
    for b in 0..nb {
        let off = 18 + b * 4;
        let cnt = u32::from_le_bytes(c.histogram_blob[off..off + 4].try_into().unwrap());
        total = total.saturating_add(cnt);
    }
    assert_eq!(total, 16);
}

#[test]
fn analyze_skips_histogram_for_non_int_columns() {
    let (mut s, cat, _d) = setup();
    s.execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20))")
        .unwrap();
    s.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    s.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
    s.execute("ANALYZE TABLE t").unwrap();

    let tid = table_id(&cat, "t");
    let c_name = cat.get_column_stats(tid, 1).unwrap().unwrap();
    // Varchar histogram not built — kind=0 (NONE), blob empty.
    assert_eq!(c_name.histogram_kind, 0);
    assert!(c_name.histogram_blob.is_empty());
    // NDV still correct.
    assert_eq!(c_name.ndv, 2);
}

#[test]
fn analyze_idempotent_replays_overwrite_stats() {
    let (mut s, cat, _d) = setup();
    s.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    s.execute("INSERT INTO t VALUES (1)").unwrap();
    s.execute("ANALYZE TABLE t").unwrap();

    let tid = table_id(&cat, "t");
    assert_eq!(cat.get_table_stats(tid).unwrap().unwrap().row_count, 1);

    // Add rows and re-analyze; row_count must update.
    s.execute("INSERT INTO t VALUES (2)").unwrap();
    s.execute("INSERT INTO t VALUES (3)").unwrap();
    s.execute("ANALYZE TABLE t").unwrap();
    assert_eq!(cat.get_table_stats(tid).unwrap().unwrap().row_count, 3);
    assert_eq!(cat.get_column_stats(tid, 0).unwrap().unwrap().ndv, 3);
}

#[test]
fn analyze_unknown_table_errors_at_bind() {
    let (mut s, _cat, _d) = setup();
    let err = s.execute("ANALYZE TABLE no_such_table").unwrap_err();
    assert!(matches!(err, interchangedb::Error::TableNotFound { .. }));
}
