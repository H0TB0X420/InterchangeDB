//! P14.1: `__sys_table_stats` + `__sys_column_stats` round-trip through
//! reopen. ANALYZE TABLE (P14.2) will be the producer; this test
//! exercises the persistence path with hand-written `TableStats` /
//! `ColumnStats` rows.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::system_tables::{
    ColumnStats, TableStats, HISTOGRAM_KIND_EQUI_WIDTH_INT, HISTOGRAM_KIND_NONE,
};
use interchangedb::catalog::{Catalog, ColumnDef, Schema, TableId};
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::{FileDiskManager, StorageEngine};
use interchangedb::types::ColumnType;

fn open_catalog_at(dir: &std::path::Path) -> Catalog<BTreeEngine> {
    let dm = FileDiskManager::open_or_create(dir.join("cat.db")).unwrap();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    Catalog::open(engine).unwrap()
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

#[test]
fn read_returns_none_before_first_write() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let table_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();
    // No ANALYZE yet → no stats rows.
    assert!(cat.get_table_stats(table_id).unwrap().is_none());
    assert!(cat.get_column_stats(table_id, 0).unwrap().is_none());
    assert!(cat.get_column_stats(table_id, 1).unwrap().is_none());
}

#[test]
fn table_stats_round_trip_within_one_open() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let table_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();

    cat.put_table_stats(table_id, &TableStats { row_count: 100_000 })
        .unwrap();
    let stats = cat.get_table_stats(table_id).unwrap().unwrap();
    assert_eq!(stats.row_count, 100_000);

    // Idempotent overwrite — later ANALYZE replaces earlier.
    cat.put_table_stats(table_id, &TableStats { row_count: 7 })
        .unwrap();
    assert_eq!(cat.get_table_stats(table_id).unwrap().unwrap().row_count, 7);
}

#[test]
fn column_stats_round_trip_with_blob() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let table_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();

    let blob = vec![0x01, 0x02, 0x03, 0xFF];
    let stats = ColumnStats {
        ndv: 1234,
        null_count: 5,
        histogram_kind: HISTOGRAM_KIND_EQUI_WIDTH_INT,
        histogram_blob: blob.clone(),
    };
    cat.put_column_stats(table_id, 0, &stats).unwrap();

    let back = cat.get_column_stats(table_id, 0).unwrap().unwrap();
    assert_eq!(back.ndv, 1234);
    assert_eq!(back.null_count, 5);
    assert_eq!(back.histogram_kind, HISTOGRAM_KIND_EQUI_WIDTH_INT);
    assert_eq!(back.histogram_blob, blob);
}

#[test]
fn column_stats_per_column_independent() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    let table_id = cat
        .create_table("warehouse".into(), warehouse_schema())
        .unwrap();

    cat.put_column_stats(
        table_id,
        0,
        &ColumnStats {
            ndv: 1,
            null_count: 0,
            histogram_kind: HISTOGRAM_KIND_NONE,
            histogram_blob: vec![],
        },
    )
    .unwrap();
    cat.put_column_stats(
        table_id,
        1,
        &ColumnStats {
            ndv: 99,
            null_count: 7,
            histogram_kind: HISTOGRAM_KIND_NONE,
            histogram_blob: b"hello".to_vec(),
        },
    )
    .unwrap();

    let c0 = cat.get_column_stats(table_id, 0).unwrap().unwrap();
    let c1 = cat.get_column_stats(table_id, 1).unwrap().unwrap();
    assert_eq!(c0.ndv, 1);
    assert_eq!(c1.ndv, 99);
    assert_eq!(c1.histogram_blob, b"hello");
}

#[test]
fn stats_survive_catalog_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let table_id;
    {
        let cat = open_catalog_at(dir.path());
        table_id = cat
            .create_table("warehouse".into(), warehouse_schema())
            .unwrap();
        cat.put_table_stats(table_id, &TableStats { row_count: 42_000 })
            .unwrap();
        cat.put_column_stats(
            table_id,
            0,
            &ColumnStats {
                ndv: 7,
                null_count: 0,
                histogram_kind: HISTOGRAM_KIND_EQUI_WIDTH_INT,
                histogram_blob: vec![0xAB; 16],
            },
        )
        .unwrap();
        // Flush BPM before drop so writes are durable across reopen.
        cat.engine().flush().unwrap();
    }
    let cat2 = open_catalog_at(dir.path());
    let t = cat2.get_table_stats(table_id).unwrap().unwrap();
    assert_eq!(t.row_count, 42_000);
    let c = cat2.get_column_stats(table_id, 0).unwrap().unwrap();
    assert_eq!(c.ndv, 7);
    assert_eq!(c.histogram_kind, HISTOGRAM_KIND_EQUI_WIDTH_INT);
    assert_eq!(c.histogram_blob, vec![0xAB; 16]);
}

#[test]
fn stats_for_unknown_table_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let cat = open_catalog_at(dir.path());
    // No such table_id — read should return None, not error.
    assert!(cat.get_table_stats(TableId(99_999)).unwrap().is_none());
    assert!(cat.get_column_stats(TableId(99_999), 0).unwrap().is_none());
}
