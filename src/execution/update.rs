//! `Update` — applies set-expressions to rows pulled from a child.
//!
//! Drains the child stream, extracts PK from each row using the target
//! table's schema, computes new column values via `set_exprs`, and calls
//! `Table::update_columns` for each. Yields a single
//! `[Int64(rows_updated)]` summary tuple, then None.
//!
//! Stops on the first error. Caller decides commit vs abort.
//!
//! ## Assumption
//!
//! The child's tuples must have the same schema as the target table —
//! specifically the PK columns must sit at the same indices declared in
//! the table's schema. The planner is responsible for enforcing this
//! (typically by passing a `SeqScan`/`PkLookup` over the same table
//! through an optional `Filter`).

use std::sync::Arc;

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{count_result_schema, Executor, Tuple};
use crate::layout::DataLayout;
use crate::storage::StorageEngine;
use crate::table::Table;
use crate::types::Value;

/// `(column_index_in_schema, value_fn_from_input_row)`.
pub type SetExpr = (usize, Box<dyn Fn(&Tuple) -> Value + Send>);

pub struct Update<E: StorageEngine, L: DataLayout> {
    table: Arc<Table<E, L>>,
    child: Box<dyn Executor>,
    set_exprs: Vec<SetExpr>,
    schema: Arc<Schema>,
    done: bool,
}

impl<E: StorageEngine, L: DataLayout> Update<E, L> {
    pub fn new(
        table: Arc<Table<E, L>>,
        child: Box<dyn Executor>,
        set_exprs: Vec<SetExpr>,
    ) -> Self {
        Self {
            table,
            child,
            set_exprs,
            schema: Arc::new(count_result_schema("rows_updated")),
            done: false,
        }
    }
}

impl<E: StorageEngine + 'static, L: DataLayout> Executor for Update<E, L> {
    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            return Ok(None);
        }
        // NOTE (perf, plan deviation): cloning `primary_key` on every
        // `next()` is wasteful — should be captured once at construction
        // and held as a field. Skipped here to keep the struct lean; the
        // Vec is typically 1-3 elements so the clone is cheap. Revisit if
        // EXPLAIN output or benchmarking shows this is a hot copy.
        let pk_indices = self.table.schema().primary_key.clone();
        let mut count: i64 = 0;
        while let Some(row) = self.child.next()? {
            let pk_values: Vec<Value> =
                pk_indices.iter().map(|&i| row[i].clone()).collect();
            let changes: Vec<(usize, Value)> = self
                .set_exprs
                .iter()
                .map(|(idx, f)| (*idx, f(&row)))
                .collect();
            self.table.update_columns(&pk_values, &changes)?;
            count += 1;
        }
        self.done = true;
        Ok(Some(vec![Value::Int64(count)]))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let cols: Vec<usize> = self.set_exprs.iter().map(|(i, _)| *i).collect();
        let mut out = format!(
            "{}Update({}, set_cols={:?})\n",
            "  ".repeat(indent),
            self.table.schema().name,
            cols
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
    use crate::execution::seq_scan::SeqScan;
    use crate::execution::filter::Filter;
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
    fn updates_all_rows_yielded_by_child() {
        let (table, _dir) = fresh_table();
        for i in 1..=3 {
            table.insert(&row(i, 100)).unwrap();
        }
        // Child is a full scan; set balance = 999 for every row.
        let child = Box::new(SeqScan::new(&*table).unwrap());
        let set_exprs: Vec<SetExpr> = vec![(1, Box::new(|_| Value::Int64(999)))];
        let mut op = Update::new(table.clone(), child, set_exprs);

        assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(3)]));
        assert_eq!(op.next().unwrap(), None);

        for i in 1..=3 {
            let r = table.get_by_pk(&[Value::Int32(i)]).unwrap().unwrap();
            assert_eq!(r[1], Value::Int64(999));
        }
    }

    #[test]
    fn updates_only_filtered_rows() {
        let (table, _dir) = fresh_table();
        for i in 1..=5 {
            table.insert(&row(i, 100)).unwrap();
        }
        // Filter: only id == 3.
        let scan = Box::new(SeqScan::new(&*table).unwrap());
        let filt = Box::new(Filter::new(scan, |t| t[0] == Value::Int32(3)));
        let set_exprs: Vec<SetExpr> = vec![(1, Box::new(|_| Value::Int64(777)))];
        let mut op = Update::new(table.clone(), filt, set_exprs);

        assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(1)]));

        for i in 1..=5 {
            let r = table.get_by_pk(&[Value::Int32(i)]).unwrap().unwrap();
            if i == 3 {
                assert_eq!(r[1], Value::Int64(777));
            } else {
                assert_eq!(r[1], Value::Int64(100));
            }
        }
    }

    #[test]
    fn empty_child_yields_zero_count() {
        let (table, _dir) = fresh_table();
        let scan = Box::new(SeqScan::new(&*table).unwrap());
        let mut op = Update::new(table, scan, vec![]);
        assert_eq!(op.next().unwrap(), Some(vec![Value::Int64(0)]));
    }

    #[test]
    fn explain_renders_full_operator_tree() {
        // Update[balance] ── Filter ── SeqScan(account)
        let (table, _dir) = fresh_table();
        let scan = Box::new(SeqScan::new(&*table).unwrap());
        let filt = Box::new(Filter::new(scan, |_| true));
        let set_exprs: Vec<SetExpr> = vec![(1, Box::new(|_| Value::Int64(0)))];
        let op = Update::new(table, filt, set_exprs);

        let tree = op.explain(0);
        // Three lines, three indent levels, correct nesting.
        let expected = "Update(account, set_cols=[1])\n  Filter\n    SeqScan(account)\n";
        assert_eq!(tree, expected);
    }

    #[test]
    fn set_expr_can_read_input_row_for_increment() {
        // "SET balance = balance + 50"
        let (table, _dir) = fresh_table();
        table.insert(&row(1, 100)).unwrap();

        let scan = Box::new(SeqScan::new(&*table).unwrap());
        let set_exprs: Vec<SetExpr> = vec![(
            1,
            Box::new(|t| match t[1] {
                Value::Int64(n) => Value::Int64(n + 50),
                _ => Value::Null,
            }),
        )];
        let mut op = Update::new(table.clone(), scan, set_exprs);

        op.next().unwrap();
        assert_eq!(
            table.get_by_pk(&[Value::Int32(1)]).unwrap().unwrap()[1],
            Value::Int64(150)
        );
    }
}
