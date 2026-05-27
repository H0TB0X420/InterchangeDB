//! `PkLookup` — single-row read by primary key.
//!
//! The workhorse for TPC-C: every WAREHOUSE/DISTRICT/CUSTOMER access is a
//! PK lookup. Trivial wrapper around `Table::get_by_pk` that exposes the
//! single result as a Volcano stream (yields zero or one tuple, then None).
//!
//! "IndexScan" in the broader sense — range scans over a secondary index —
//! arrives in Phase 12 when secondary indexes are introduced. PkLookup is
//! the primary-key-only fast path.

use std::sync::Arc;

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{Executor, Tuple};
use crate::layout::DataLayout;
use crate::storage::StorageEngine;
use crate::table::Table;
use crate::types::Value;

/// Volcano leaf operator: yields the row at `pk` if it exists, then None.
pub struct PkLookup {
    schema: Arc<Schema>,
    /// The fetched row, or `None` if the PK was absent. Taken on first
    /// `next()`; subsequent calls return `Ok(None)`.
    row: Option<Tuple>,
    done: bool,
}

impl PkLookup {
    /// Construct a PkLookup. Performs the get_by_pk eagerly so any
    /// constraint or engine error surfaces at construction.
    pub fn new<E: StorageEngine, L: DataLayout>(
        table: &Table<E, L>,
        pk: &[Value],
    ) -> Result<Self> {
        let row = table.get_by_pk(pk)?;
        let schema = Arc::new(table.schema().clone());
        Ok(Self {
            schema,
            row,
            done: false,
        })
    }
}

impl Executor for PkLookup {
    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        Ok(self.row.take())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        format!("{}PkLookup({})\n", "  ".repeat(indent), self.schema.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{ColumnDef, TableId};
    use crate::index::btree::BTreeEngine;
    use crate::layout::RowLayout;
    use crate::storage::FileDiskManager;
    use crate::types::ColumnType;
    use tempfile::TempDir;

    fn fresh_table() -> (Table<BTreeEngine, RowLayout>, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let schema = Arc::new(Schema {
            name: "account".into(),
            table_id: TableId(1),
            columns: vec![
                ColumnDef { name: "id".into(), ty: ColumnType::Int32, nullable: false, default: None },
                ColumnDef { name: "balance".into(), ty: ColumnType::Int64, nullable: false, default: None },
            ],
            primary_key: vec![0],
        });
        (Table::new(engine, schema, RowLayout), dir)
    }

    fn row(id: i32, balance: i64) -> Vec<Value> {
        vec![Value::Int32(id), Value::Int64(balance)]
    }

    #[test]
    fn present_key_yields_row_then_none() {
        let (table, _dir) = fresh_table();
        table.insert(&row(42, 999)).unwrap();

        let mut op = PkLookup::new(&table, &[Value::Int32(42)]).unwrap();
        assert_eq!(op.next().unwrap(), Some(row(42, 999)));
        assert_eq!(op.next().unwrap(), None);
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn absent_key_yields_none() {
        let (table, _dir) = fresh_table();
        // Table empty; lookup of any PK returns None.
        let mut op = PkLookup::new(&table, &[Value::Int32(42)]).unwrap();
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn lookup_isolates_to_the_requested_pk() {
        let (table, _dir) = fresh_table();
        table.insert(&row(1, 100)).unwrap();
        table.insert(&row(2, 200)).unwrap();
        table.insert(&row(3, 300)).unwrap();

        let mut op = PkLookup::new(&table, &[Value::Int32(2)]).unwrap();
        assert_eq!(op.next().unwrap(), Some(row(2, 200)));
        assert_eq!(op.next().unwrap(), None);
    }
}
