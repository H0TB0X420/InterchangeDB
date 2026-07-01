//! `Delete` — removes rows pulled from a child operator.
//!
//! Two phases, like `Update` (Halloween Problem): the READ phase drains
//! the child and stages each target row's PK; the WRITE phase then calls
//! `Table::delete_by_pk` for each. Deleting inside the read loop would
//! mutate the B-tree under a live scan cursor once scans stream. Yields
//! a single `[Int64(rows_deleted)]` summary tuple.
//!
//! Same child-schema-must-match-target assumption as `Update`.

use std::sync::Arc;

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{count_result_schema, Executor, Tuple};
use crate::layout::DataLayout;
use crate::storage::StorageEngine;
use crate::table::Table;
use crate::types::Value;

pub struct Delete<E: StorageEngine, L: DataLayout> {
    table: Arc<Table<E, L>>,
    child: Box<dyn Executor>,
    schema: Arc<Schema>,
    done: bool,
}

impl<E: StorageEngine, L: DataLayout> Delete<E, L> {
    pub fn new(table: Arc<Table<E, L>>, child: Box<dyn Executor>) -> Self {
        Self {
            table,
            child,
            schema: Arc::new(count_result_schema("rows_deleted")),
            done: false,
        }
    }
}

impl<E: StorageEngine + 'static, L: DataLayout> Executor for Delete<E, L> {
    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            return Ok(None);
        }
        let pk_indices = self.table.schema().primary_key.clone();
        // READ phase: stage every target PK before touching the table
        // (see module doc — Halloween Problem). Buffer bound: O(matching
        // rows), the same class as the eager scan output feeding it.
        let mut staged: Vec<Vec<Value>> = Vec::new();
        while let Some(row) = self.child.next()? {
            debug_assert_eq!(
                row.len(),
                self.table.schema().columns.len(),
                "Delete child tuple width must match the target table schema"
            );
            let pk_values: Vec<Value> = pk_indices.iter().map(|&i| row[i].clone()).collect();
            staged.push(pk_values);
        }
        // WRITE phase: the child is fully drained; apply the staged deletes.
        let count = staged.len() as i64;
        for pk_values in &staged {
            self.table.delete_by_pk(pk_values)?;
        }
        self.done = true;
        Ok(Some(vec![Value::Int64(count)]))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let mut out = format!(
            "{}Delete({})\n",
            "  ".repeat(indent),
            self.table.schema().name
        );
        out.push_str(&self.child.explain(indent + 1));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::catalog::{ColumnDef, TableId};
    use crate::execution::filter::Filter;
    use crate::execution::seq_scan::SeqScan;
    use crate::index::btree::BTreeEngine;
    use crate::layout::RowLayout;
    use crate::storage::FileDiskManager;
    use crate::types::ColumnType;
    use tempfile::TempDir;

    fn fresh_table() -> (Arc<Table<BTreeEngine, RowLayout>>, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = FileDiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        let engine = Arc::new(BTreeEngine::new(bpm).unwrap());
        let schema = Arc::new(Schema {
            name: "account".into(),
            table_id: TableId(1),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "balance".into(),
                    ty: ColumnType::Int64,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec![0],
        });
        (Arc::new(Table::new(engine, schema, RowLayout)), dir)
    }

    fn row(id: i32, bal: i64) -> Tuple {
        vec![Value::Int32(id), Value::Int64(bal)]
    }

    #[test]
    fn deletes_all_rows_yielded_by_child() {
        let (table, _dir) = fresh_table();
        for i in 1..=3 {
            table.insert(&row(i, 100)).unwrap();
        }
        let child = Box::new(SeqScan::new(&*table).unwrap());
        let mut op = Delete::new(table.clone(), child);

        assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(3)]));
        for i in 1..=3 {
            assert!(table.get_by_pk(&[Value::Int32(i)]).unwrap().is_none());
        }
    }

    #[test]
    fn deletes_only_filtered_rows() {
        let (table, _dir) = fresh_table();
        for i in 1..=5 {
            table.insert(&row(i, 100)).unwrap();
        }
        let scan = Box::new(SeqScan::new(&*table).unwrap());
        let filt = Box::new(Filter::new(
            scan,
            |t| matches!(t[0], Value::Int32(n) if n > 3),
        ));
        let mut op = Delete::new(table.clone(), filt);

        assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(2)]));
        // 1, 2, 3 remain; 4, 5 gone.
        for i in 1..=3 {
            assert!(table.get_by_pk(&[Value::Int32(i)]).unwrap().is_some());
        }
        for i in 4..=5 {
            assert!(table.get_by_pk(&[Value::Int32(i)]).unwrap().is_none());
        }
    }

    #[test]
    fn empty_child_yields_zero_count() {
        let (table, _dir) = fresh_table();
        let scan = Box::new(SeqScan::new(&*table).unwrap());
        let mut op = Delete::new(table, scan);
        assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(0)]));
    }

    /// Child that proves read/write phase separation: on EVERY pull —
    /// including the final `None` — it re-reads the table and asserts no
    /// delete has landed yet. Fails loudly if the operator interleaves
    /// deletes into the read loop (mutating the tree under a live cursor).
    struct PhaseProbe {
        table: Arc<Table<BTreeEngine, RowLayout>>,
        baseline: Vec<Tuple>,
        cursor: usize,
    }

    impl Executor for PhaseProbe {
        fn next(&mut self) -> Result<Option<Tuple>> {
            assert_eq!(
                self.table.scan().unwrap(),
                self.baseline,
                "table mutated before the read phase finished (Halloween)"
            );
            let row = self.baseline.get(self.cursor).cloned();
            self.cursor += 1;
            Ok(row)
        }

        fn schema(&self) -> &Schema {
            self.table.schema()
        }
    }

    #[test]
    fn deletes_are_deferred_until_child_is_fully_drained() {
        let (table, _dir) = fresh_table();
        for i in 1..=3 {
            table.insert(&row(i, 100)).unwrap();
        }
        let probe = Box::new(PhaseProbe {
            table: table.clone(),
            baseline: table.scan().unwrap(),
            cursor: 0,
        });
        let mut op = Delete::new(table.clone(), probe);

        assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(3)]));
        // All deletes landed — after the drain.
        for i in 1..=3 {
            assert!(table.get_by_pk(&[Value::Int32(i)]).unwrap().is_none());
        }
    }
}
