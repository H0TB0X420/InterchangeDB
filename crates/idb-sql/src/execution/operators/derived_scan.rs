//! `DerivedScan` — a materialized FROM-subquery leaf (H4a).
//!
//! A leaf of the outer operator tree that presents an inner subquery's output
//! as though it were a table scan. On the first `next()` it drains the child
//! (the built subplan) into an owned buffer, then iterates the buffer across
//! subsequent calls — the same eager-collect shape as `SeqScan` /
//! `HashAggregate`, for the same reason (the child's borrow can't be held
//! inside a self-referential iterator).
//!
//! The output `schema` travels in the `PhysOp::DerivedScan` IR, so the builder
//! constructs this leaf with NO catalog lookup (a derived table has no catalog
//! entry — its alias is its only name).

use std::sync::Arc;

use crate::catalog::{ColumnDef, Schema, TableId};
use crate::common::Result;
use crate::execution::{Executor, Tuple};

/// Explicit bound on the rows a derived table materializes (limit on
/// everything): 2^20 rows — the same rationale and scale as
/// `HashAggregate`'s `MAX_GROUP_COUNT`. The whole subquery output is buffered
/// in memory; exceeding this is a workload we never promised to handle, so
/// crash loudly rather than grow without bound. (Recorded lever: a streaming
/// derived scan / spill; today the buffer is eager.)
pub(crate) const MAX_DERIVED_ROWS: usize = 1 << 20;

/// Volcano leaf operator: materialize a subquery's rows, then yield them.
pub struct DerivedScan {
    schema: Arc<Schema>,
    /// The FROM-clause alias — for `explain`.
    alias: String,
    /// The built subplan executor; drained once into `rows`.
    child: Box<dyn Executor>,
    /// `None` until the first `next()` materializes; then the buffered rows.
    rows: Option<std::vec::IntoIter<Tuple>>,
}

impl DerivedScan {
    /// Construct a `DerivedScan` over a built subplan `child`. `columns` is the
    /// derived table's output schema (carried in the IR); the synthetic
    /// `Schema` reuses the alias as its name, has no PK, and `TableId(0)`
    /// (not a catalog table).
    pub fn new(alias: String, child: Box<dyn Executor>, columns: Vec<ColumnDef>) -> Self {
        let schema = Arc::new(Schema {
            name: alias.clone(),
            table_id: TableId(0),
            columns,
            primary_key: vec![],
        });
        Self {
            schema,
            alias,
            child,
            rows: None,
        }
    }

    /// Drain the child into an owned buffer, bounded by `MAX_DERIVED_ROWS`.
    fn materialize(&mut self) -> Result<Vec<Tuple>> {
        let mut buffer = Vec::new();
        while let Some(row) = self.child.next()? {
            assert!(
                buffer.len() < MAX_DERIVED_ROWS,
                "derived table '{}' exceeded {} materialized rows",
                self.alias,
                MAX_DERIVED_ROWS
            );
            buffer.push(row);
        }
        Ok(buffer)
    }
}

impl Executor for DerivedScan {
    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.rows.is_none() {
            let buffer = self.materialize()?;
            self.rows = Some(buffer.into_iter());
        }
        Ok(self.rows.as_mut().unwrap().next())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        // Matches `PhysOp::DerivedScan`'s IR rendering: `DerivedScan(alias)`
        // header, then the materialized subplan indented one level.
        let pad = "  ".repeat(indent);
        let mut out = format!("{pad}DerivedScan({})\n", self.alias);
        out.push_str(&self.child.explain(indent + 1));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::test_util::VecExecutor;
    use crate::types::{ColumnType, Value};

    fn one_int_schema() -> Schema {
        Schema {
            name: "inner".into(),
            table_id: TableId(1),
            columns: vec![ColumnDef {
                name: "x".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            }],
            primary_key: vec![0],
        }
    }

    // The scan yields exactly the child's rows, in order, then None — and the
    // output schema is the alias-named derived schema, not the child's.
    #[test]
    fn yields_child_rows_under_derived_schema() {
        let child = Box::new(VecExecutor::new(
            one_int_schema(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        ));
        let columns = vec![ColumnDef {
            name: "y".into(),
            ty: ColumnType::Int32,
            nullable: true,
            default: None,
        }];
        let mut op = DerivedScan::new("d".into(), child, columns);
        assert_eq!(op.schema().name, "d");
        assert_eq!(op.schema().columns[0].name, "y");
        assert_eq!(op.next().unwrap(), Some(vec![Value::Int32(1)]));
        assert_eq!(op.next().unwrap(), Some(vec![Value::Int32(2)]));
        assert_eq!(op.next().unwrap(), None);
        // Idempotent post-stream.
        assert_eq!(op.next().unwrap(), None);
    }

    // Empty subquery → no rows.
    #[test]
    fn empty_subquery_yields_none() {
        let child = Box::new(VecExecutor::new(one_int_schema(), vec![]));
        let columns = one_int_schema().columns;
        let mut op = DerivedScan::new("d".into(), child, columns);
        assert_eq!(op.next().unwrap(), None);
    }
}
