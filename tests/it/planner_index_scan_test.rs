//! P12.6: planner lowers indexable WHERE predicates into `IndexScan`.
//!
//! Verifies the planner emits `IndexScan` in place of `SeqScan` + `Filter`
//! when the WHERE clause is a simple equality on a column with a matching
//! single-column index. Asserts on the rendered plan structure (no
//! execution) — execution correctness is covered by P12.5 IndexScan tests.

use std::sync::Arc;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, IndexBackend, IndexDef, Schema, TableId};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::sql::{parse, plan, Binder, PhysicalPlan};
use interchangedb::storage::MemoryDiskManager;
use interchangedb::types::ColumnType;

struct Setup {
    catalog: Arc<Catalog<BTreeEngine>>,
    _dir: tempfile::TempDir,
}

fn setup_with_name_index() -> Setup {
    let dir = tempfile::tempdir().unwrap();
    let dm = MemoryDiskManager::new();
    let bpm = BufferPoolManager::new(64, dm);
    let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
    let catalog = Arc::new(
        Catalog::open_persistent(
            engine.clone(),
            dir.path().join("indexes"),
            interchangedb::default_index_opener(),
        )
        .unwrap(),
    );

    let table_id = catalog
        .create_table(
            "t".into(),
            Schema {
                name: "t".into(),
                table_id: TableId(0),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        ty: ColumnType::Int32,
                        nullable: false,
                        default: None,
                    },
                    ColumnDef {
                        name: "name".into(),
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
            name: "t_by_name".into(),
            table_id,
            columns: vec![1],
            unique: false,
            backend: IndexBackend::BTree,
        })
        .unwrap();

    Setup { catalog, _dir: dir }
}

fn plan_sql(s: &Setup, sql: &str) -> PhysicalPlan {
    let stmts = parse(sql).unwrap();
    let binder = Binder::new(s.catalog.clone());
    let logical = binder.bind(stmts.into_iter().next().unwrap()).unwrap();
    plan(logical, &s.catalog).unwrap()
}

fn explain_of(p: PhysicalPlan) -> String {
    match p {
        PhysicalPlan::Query(physop) => physop.explain(0),
        other => panic!(
            "expected Query plan, got: {:?}",
            std::mem::discriminant(&other)
        ),
    }
}

#[test]
fn select_with_equality_on_indexed_column_lowers_to_indexscan() {
    let s = setup_with_name_index();
    let text = explain_of(plan_sql(&s, "SELECT id FROM t WHERE name = 'alpha'"));
    assert!(
        text.contains("IndexScan"),
        "expected IndexScan in plan:\n{}",
        text
    );
    assert!(
        text.contains("t_by_name"),
        "expected index name in plan:\n{}",
        text
    );
    // MVCC recheck: the predicate is re-applied as a Filter above the
    // IndexScan. Secondary indexes are unversioned, so a stale entry can
    // dereference to a visible row whose indexed value no longer matches;
    // the recheck drops those false positives (E1).
    assert!(
        text.contains("Filter"),
        "recheck Filter should sit above the IndexScan:\n{}",
        text
    );
}

#[test]
fn select_with_pk_equality_lowers_to_pk_lookup() {
    let s = setup_with_name_index();
    // id is the PK → a point lookup, ahead of both the scan and the secondary
    // index on `name`.
    let text = explain_of(plan_sql(&s, "SELECT name FROM t WHERE id = 1"));
    assert!(
        text.contains("PkLookup"),
        "expected PkLookup in plan:\n{}",
        text
    );
    assert!(!text.contains("SeqScan"), "PK lookup, not scan:\n{}", text);
    assert!(
        !text.contains("IndexScan"),
        "id is the PK, not a secondary index:\n{}",
        text
    );
}

#[test]
fn select_without_where_uses_seqscan() {
    let s = setup_with_name_index();
    let text = explain_of(plan_sql(&s, "SELECT * FROM t"));
    assert!(text.contains("SeqScan"));
    assert!(!text.contains("IndexScan"));
}

#[test]
fn select_with_range_predicate_falls_back_to_seqscan() {
    let s = setup_with_name_index();
    // `name > 'm'` isn't an equality predicate — first cut doesn't lower it.
    let text = explain_of(plan_sql(&s, "SELECT * FROM t WHERE name > 'm'"));
    assert!(text.contains("SeqScan"), "got:\n{}", text);
    assert!(text.contains("Filter"));
    assert!(!text.contains("IndexScan"));
}

#[test]
fn update_with_indexed_predicate_uses_indexscan() {
    let s = setup_with_name_index();
    let text = explain_of(plan_sql(&s, "UPDATE t SET id = 99 WHERE name = 'alpha'"));
    assert!(
        text.contains("IndexScan"),
        "expected index-driven UPDATE:\n{}",
        text
    );
    assert!(text.contains("Update"));
}

#[test]
fn delete_with_indexed_predicate_uses_indexscan() {
    let s = setup_with_name_index();
    let text = explain_of(plan_sql(&s, "DELETE FROM t WHERE name = 'alpha'"));
    assert!(
        text.contains("IndexScan"),
        "expected index-driven DELETE:\n{}",
        text
    );
    assert!(text.contains("Delete"));
}

#[test]
fn literal_on_left_is_recognized_symmetrically() {
    let s = setup_with_name_index();
    // `'alpha' = name` should lower just like `name = 'alpha'`.
    let text = explain_of(plan_sql(&s, "SELECT id FROM t WHERE 'alpha' = name"));
    assert!(
        text.contains("IndexScan"),
        "expected symmetric lowering:\n{}",
        text
    );
}
