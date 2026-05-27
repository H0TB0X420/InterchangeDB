//! Ergonomic builder for `Catalog<BTreeEngine>` populated with arbitrary
//! schemas. Replaces ~15 lines of boilerplate per test.
//!
//! Design choice: this wraps the *real* `Catalog` backed by an in-memory
//! `BTreeEngine` via tempdir, not an abstract mock. Reason: `Catalog<E>` is
//! a concrete struct (not a trait), so a true mock would require refactoring
//! the binder / planner / executor to accept `&dyn CatalogRead`. That
//! refactor is out of scope for Q-02 and risks destabilizing Phase 11 right
//! before Phase 12 starts. The boilerplate-shrinking value of the helper is
//! orthogonal to whether the underlying catalog is real or mocked.

use interchangedb::buffer::BufferPoolManager;
use interchangedb::catalog::{Catalog, ColumnDef, Schema, TableId};
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::FileDiskManager;
use interchangedb::types::ColumnType;
use std::sync::Arc;
use tempfile::TempDir;

pub struct MockCatalog {
    pub engine: Arc<BTreeEngine>,
    pub catalog: Arc<Catalog<BTreeEngine>>,
    _dir: TempDir,
}

impl MockCatalog {
    /// Fresh catalog with only the system tables populated. Buffer pool size
    /// matches the existing planner tests (512 frames).
    pub fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(512, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let catalog = Arc::new(Catalog::open(engine.clone()).unwrap());
        Self { engine, catalog, _dir: dir }
    }

    /// Register a user table.
    ///
    /// - `columns`: `(name, type, nullable)` tuples in declaration order.
    /// - `pk`: column names that form the primary key, in PK order. Must be
    ///   non-empty; PK columns must be non-nullable (enforced by `Catalog`).
    ///
    /// Panics if a PK name doesn't appear in `columns`. Mock-side panics are
    /// preferred over `Result` returns — test fixtures must be obviously
    /// correct at the call site.
    pub fn with_table(
        self,
        name: &str,
        columns: &[(&str, ColumnType, bool)],
        pk: &[&str],
    ) -> Self {
        let column_defs: Vec<ColumnDef> = columns
            .iter()
            .map(|(n, ty, nullable)| ColumnDef {
                name: (*n).to_string(),
                ty: *ty,
                nullable: *nullable,
                default: None,
            })
            .collect();
        let pk_indices: Vec<usize> = pk
            .iter()
            .map(|pk_name| {
                column_defs
                    .iter()
                    .position(|c| c.name == *pk_name)
                    .unwrap_or_else(|| {
                        panic!("mock_catalog: PK column '{}' not in column list", pk_name)
                    })
            })
            .collect();
        let schema = Schema {
            name: name.to_string(),
            table_id: TableId(0), // catalog assigns the real id
            columns: column_defs,
            primary_key: pk_indices,
        };
        self.catalog.create_table(name.to_string(), schema).unwrap();
        self
    }

    /// TPC-C `warehouse` table, abbreviated. Single-column PK on `w_id`.
    pub fn with_tpcc_warehouse(self) -> Self {
        self.with_table(
            "warehouse",
            &[
                ("w_id", ColumnType::Int32, false),
                ("w_ytd", ColumnType::Int64, false),
                ("w_name", ColumnType::Varchar(10), false),
            ],
            &["w_id"],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_catalog_lists_only_system_tables() {
        let mc = MockCatalog::new();
        for t in mc.catalog.list_tables() {
            assert!(t.starts_with("__sys_"), "unexpected non-system table: {}", t);
        }
    }

    #[test]
    fn with_table_registers_user_schema() {
        let mc = MockCatalog::new().with_table(
            "t",
            &[
                ("id", ColumnType::Int32, false),
                ("name", ColumnType::Varchar(50), true),
            ],
            &["id"],
        );
        let s = mc.catalog.get_table("t").unwrap();
        assert_eq!(s.name, "t");
        assert_eq!(s.columns.len(), 2);
        assert_eq!(s.primary_key, vec![0]);
        assert_eq!(s.columns[0].nullable, false);
        assert_eq!(s.columns[1].nullable, true);
    }

    #[test]
    fn tpcc_warehouse_has_expected_shape() {
        let mc = MockCatalog::new().with_tpcc_warehouse();
        let s = mc.catalog.get_table("warehouse").unwrap();
        assert_eq!(s.primary_key, vec![0]);
        assert_eq!(s.columns[0].name, "w_id");
        assert_eq!(s.columns[2].ty, ColumnType::Varchar(10));
    }

    #[test]
    fn engine_handle_is_exposed_for_planner_wiring() {
        // The planner's `plan()` takes an `Arc<E>` separately from the catalog
        // (DML uses TxnEngine wrapping the same storage; DDL uses raw).
        // Tests that drive planning need the engine handle.
        let mc = MockCatalog::new().with_tpcc_warehouse();
        let _engine: Arc<BTreeEngine> = mc.engine.clone();
    }

    #[test]
    fn builder_chains_multiple_tables() {
        let mc = MockCatalog::new()
            .with_table("a", &[("x", ColumnType::Int32, false)], &["x"])
            .with_table("b", &[("y", ColumnType::Int64, false)], &["y"]);
        assert!(mc.catalog.get_table("a").is_ok());
        assert!(mc.catalog.get_table("b").is_ok());
    }

    #[test]
    #[should_panic(expected = "PK column 'missing' not in column list")]
    fn missing_pk_column_panics_with_clear_message() {
        MockCatalog::new().with_table(
            "t",
            &[("id", ColumnType::Int32, false)],
            &["missing"],
        );
    }
}
