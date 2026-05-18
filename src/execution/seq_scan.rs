//! `SeqScan` — full-table scan in PK order.
//!
//! Eagerly collects the table's scan output into an owned `Vec<Tuple>` at
//! construction. Subsequent `next()` calls iterate the Vec.
//!
//! ## Why eager collect
//!
//! `DataLayout::scan_table` returns `Box<dyn Iterator + 'a>` borrowing from
//! the engine reference. To hold that iterator inside SeqScan alongside the
//! engine would be self-referential, which Rust doesn't allow without
//! unsafe / pin-projection. Collecting up front sidesteps the problem.
//!
//! ## Cost
//!
//! Memory cost is proportional to total tuple bytes (one `Vec<Value>` per
//! row, all rows in memory). Fine for OLTP table sizes (TPC-C warehouse /
//! district / item / customer — tens of thousands of rows max). Not fine
//! for analytical full scans of `order_line` at scale > 10.
//!
//! NOTE (perf): Phase-11 streaming refactor. Either (a) thread engine
//! ownership into the iterator via `Arc<Table>` and an iterator that
//! holds the Arc, or (b) restructure `DataLayout::scan_table` to return
//! an owned-state iterator.

use std::sync::Arc;

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{Executor, Tuple};
use crate::layout::DataLayout;
use crate::storage::StorageEngine;
use crate::table::Table;

/// Volcano leaf operator: yields every row of a table in PK order.
pub struct SeqScan {
    schema: Arc<Schema>,
    rows: std::vec::IntoIter<Tuple>,
}

impl SeqScan {
    /// Construct a SeqScan over `table`. Performs the full scan and
    /// collects the rows. Cost is paid once, here.
    pub fn new<E: StorageEngine, L: DataLayout>(table: &Table<E, L>) -> Result<Self> {
        let rows = table.scan()?;
        let schema = Arc::new(table.schema().clone());
        Ok(Self {
            schema,
            rows: rows.into_iter(),
        })
    }
}

impl Executor for SeqScan {
    fn next(&mut self) -> Result<Option<Tuple>> {
        Ok(self.rows.next())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        format!("{}SeqScan({})\n", "  ".repeat(indent), self.schema.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{ColumnDef, TableId};
    use crate::index::btree::BTreeEngine;
    use crate::layout::RowLayout;
    use crate::storage::DiskManager;
    use crate::types::{ColumnType, Value};
    use tempfile::TempDir;

    fn fresh_engine() -> (Arc<BTreeEngine>, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        (Arc::new(BTreeEngine::new(bpm).unwrap()), dir)
    }

    fn account_schema() -> Schema {
        Schema {
            name: "account".into(),
            table_id: TableId(1),
            columns: vec![
                ColumnDef { name: "id".into(), ty: ColumnType::Int32, nullable: false, default: None },
                ColumnDef { name: "balance".into(), ty: ColumnType::Int64, nullable: false, default: None },
            ],
            primary_key: vec![0],
        }
    }

    fn row(id: i32, balance: i64) -> Vec<Value> {
        vec![Value::Int32(id), Value::Int64(balance)]
    }

    fn fresh_table() -> (Table<BTreeEngine, RowLayout>, TempDir) {
        let (engine, dir) = fresh_engine();
        let schema = Arc::new(account_schema());
        (Table::new(engine, schema, RowLayout), dir)
    }

    #[test]
    fn empty_table_yields_none_immediately() {
        let (table, _dir) = fresh_table();
        let mut op = SeqScan::new(&table).unwrap();
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn single_row_yields_then_none() {
        let (table, _dir) = fresh_table();
        table.insert(&row(1, 100)).unwrap();

        let mut op = SeqScan::new(&table).unwrap();
        assert_eq!(op.next().unwrap(), Some(row(1, 100)));
        assert_eq!(op.next().unwrap(), None);
        // Idempotent post-stream: still None on further calls.
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn yields_all_rows_in_pk_order() {
        let (table, _dir) = fresh_table();
        // Insert scrambled to prove the ordering comes from the scan, not insertion.
        for id in [3, 1, 4, 1, 5, 9, 2, 6] {
            table.upsert(&row(id, id as i64 * 10)).unwrap();
        }

        let mut op = SeqScan::new(&table).unwrap();
        let mut ids = Vec::new();
        while let Some(t) = op.next().unwrap() {
            if let Value::Int32(n) = t[0] { ids.push(n); }
        }
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 9]);
    }

    #[test]
    fn schema_matches_table_schema() {
        let (table, _dir) = fresh_table();
        let op = SeqScan::new(&table).unwrap();
        assert_eq!(op.schema().name, "account");
        assert_eq!(op.schema().columns.len(), 2);
    }
}
