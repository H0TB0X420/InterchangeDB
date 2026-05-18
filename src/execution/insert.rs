//! `Insert` — writes a constant row list into a target table.
//!
//! Yields a single summary tuple `[Int64(rows_inserted)]` on first `next()`,
//! then None. Stops on the first error (constraint violation, duplicate PK,
//! engine I/O). The caller decides whether to commit or abort the
//! enclosing transaction.
//!
//! `INSERT … SELECT` (insert from a child stream) is a planner-side
//! transformation: the planner collects the child's output and passes it
//! as the `rows` Vec to `Insert::new`. For TPC-C this is sufficient — the
//! NEW_ORDER transaction inserts ~10 known rows.

use std::sync::Arc;

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{count_result_schema, Executor, Tuple};
use crate::layout::DataLayout;
use crate::storage::StorageEngine;
use crate::table::Table;
use crate::types::Value;

pub struct Insert<E: StorageEngine, L: DataLayout> {
    table: Arc<Table<E, L>>,
    rows: Option<Vec<Tuple>>,
    schema: Arc<Schema>,
    done: bool,
}

impl<E: StorageEngine, L: DataLayout> Insert<E, L> {
    pub fn new(table: Arc<Table<E, L>>, rows: Vec<Tuple>) -> Self {
        Self {
            table,
            rows: Some(rows),
            schema: Arc::new(count_result_schema("rows_inserted")),
            done: false,
        }
    }
}

impl<E: StorageEngine + 'static, L: DataLayout> Executor for Insert<E, L> {
    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            return Ok(None);
        }
        let rows = self.rows.take().unwrap_or_default();
        let mut count: i64 = 0;
        for row in rows {
            self.table.insert(&row)?;
            count += 1;
        }
        self.done = true;
        Ok(Some(vec![Value::Int64(count)]))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let pending = self.rows.as_ref().map(|r| r.len()).unwrap_or(0);
        format!(
            "{}Insert({} rows → {})\n",
            "  ".repeat(indent),
            pending,
            self.table.schema().name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{ColumnDef, TableId};
    use crate::common::Error;
    use crate::index::btree::BTreeEngine;
    use crate::layout::RowLayout;
    use crate::storage::DiskManager;
    use crate::types::ColumnType;
    use tempfile::TempDir;

    fn fresh_table() -> (Arc<Table<BTreeEngine, RowLayout>>, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::create(&path).unwrap();
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
        (Arc::new(Table::new(engine, schema, RowLayout)), dir)
    }

    fn row(id: i32, bal: i64) -> Tuple {
        vec![Value::Int32(id), Value::Int64(bal)]
    }

    #[test]
    fn inserts_rows_and_yields_count() {
        let (table, _dir) = fresh_table();
        let mut op = Insert::new(table.clone(), vec![row(1, 10), row(2, 20), row(3, 30)]);

        assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(3)]));
        assert_eq!(op.next().unwrap(), None);

        // Rows are now in the table.
        assert_eq!(table.get_by_pk(&[Value::Int32(1)]).unwrap(), Some(row(1, 10)));
        assert_eq!(table.get_by_pk(&[Value::Int32(2)]).unwrap(), Some(row(2, 20)));
        assert_eq!(table.get_by_pk(&[Value::Int32(3)]).unwrap(), Some(row(3, 30)));
    }

    #[test]
    fn empty_input_yields_zero() {
        let (table, _dir) = fresh_table();
        let mut op = Insert::new(table, vec![]);
        assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(0)]));
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn duplicate_pk_stops_at_failure() {
        let (table, _dir) = fresh_table();
        table.insert(&row(1, 100)).unwrap();
        // Two new rows + one that collides with id=1.
        let mut op = Insert::new(table.clone(), vec![row(2, 20), row(1, 999), row(3, 30)]);

        let err = op.next();
        assert!(matches!(err, Err(Error::DuplicateKey { .. })));

        // Pre-failure rows persisted (id=2). Post-failure row (id=3) was not attempted.
        assert!(table.get_by_pk(&[Value::Int32(2)]).unwrap().is_some());
        assert!(table.get_by_pk(&[Value::Int32(3)]).unwrap().is_none());
    }
}
