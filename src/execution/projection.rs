//! `Projection` — narrows or reorders columns from a child.
//!
//! Maps each input tuple to an output tuple containing only the columns
//! listed in `cols`, in the order requested. Out-of-range column indices
//! are caught at construction time so the per-row `next()` path never
//! re-validates.

use std::sync::Arc;

use crate::catalog::Schema;
use crate::common::{Error, Result};
use crate::execution::{Executor, Tuple};

/// Volcano pipeline operator: column subset / reorder.
pub struct Projection {
    child: Box<dyn Executor>,
    cols: Vec<usize>,
    schema: Arc<Schema>,
}

impl Projection {
    /// Construct a Projection. `cols` are indices into the child's schema;
    /// the output tuple has values in the same order as `cols`.
    pub fn new(child: Box<dyn Executor>, cols: Vec<usize>) -> Result<Self> {
        let child_schema = child.schema();
        for &c in &cols {
            if c >= child_schema.columns.len() {
                return Err(Error::StorageCorrupted(format!(
                    "projection column index {} out of range (child has {} columns)",
                    c,
                    child_schema.columns.len()
                )));
            }
        }
        let columns = cols
            .iter()
            .map(|&c| child_schema.columns[c].clone())
            .collect();
        let schema = Arc::new(Schema {
            name: child_schema.name.clone(),
            table_id: child_schema.table_id,
            columns,
            // Projection may drop or reorder PK columns; output has no
            // table-level PK constraint. The planner is responsible for
            // checking PK-dependent invariants before this point.
            primary_key: vec![],
        });
        Ok(Self {
            child,
            cols,
            schema,
        })
    }
}

impl Executor for Projection {
    fn next(&mut self) -> Result<Option<Tuple>> {
        match self.child.next()? {
            Some(tuple) => Ok(Some(self.cols.iter().map(|&c| tuple[c].clone()).collect())),
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let mut out = format!(
            "{}Projection({:?})\n",
            "  ".repeat(indent),
            self.cols
        );
        out.push_str(&self.child.explain(indent + 1));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnDef, TableId};
    use crate::execution::test_util::VecExecutor;
    use crate::types::{ColumnType, Value};

    fn three_col_schema() -> Schema {
        Schema {
            name: "t".into(),
            table_id: TableId(1),
            columns: vec![
                ColumnDef { name: "a".into(), ty: ColumnType::Int32, nullable: false, default: None },
                ColumnDef { name: "b".into(), ty: ColumnType::Int64, nullable: false, default: None },
                ColumnDef { name: "c".into(), ty: ColumnType::Boolean, nullable: false, default: None },
            ],
            primary_key: vec![0],
        }
    }

    fn t(a: i32, b: i64, c: bool) -> Tuple {
        vec![Value::Int32(a), Value::Int64(b), Value::Boolean(c)]
    }

    #[test]
    fn picks_subset_in_requested_order() {
        let child = Box::new(VecExecutor::new(
            three_col_schema(),
            vec![t(1, 10, true), t(2, 20, false)],
        ));
        // Pick columns 2, 0 (boolean then int32, in that order).
        let mut op = Projection::new(child, vec![2, 0]).unwrap();
        assert_eq!(
            op.next().unwrap(),
            Some(vec![Value::Boolean(true), Value::Int32(1)])
        );
        assert_eq!(
            op.next().unwrap(),
            Some(vec![Value::Boolean(false), Value::Int32(2)])
        );
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn out_of_range_index_errors_at_construction() {
        let child = Box::new(VecExecutor::new(three_col_schema(), vec![]));
        match Projection::new(child, vec![0, 99]) {
            Err(Error::StorageCorrupted(_)) => {}
            other => panic!("expected StorageCorrupted, got {:?}", other.err()),
        }
    }

    #[test]
    fn output_schema_reflects_selected_columns() {
        let child = Box::new(VecExecutor::new(three_col_schema(), vec![]));
        let op = Projection::new(child, vec![2, 1]).unwrap();
        assert_eq!(op.schema().columns.len(), 2);
        assert_eq!(op.schema().columns[0].name, "c");
        assert_eq!(op.schema().columns[1].name, "b");
        assert!(op.schema().primary_key.is_empty());
    }

    #[test]
    fn identity_projection_passes_all_columns() {
        let child = Box::new(VecExecutor::new(three_col_schema(), vec![t(7, 70, true)]));
        let mut op = Projection::new(child, vec![0, 1, 2]).unwrap();
        assert_eq!(op.next().unwrap(), Some(t(7, 70, true)));
        assert_eq!(op.next().unwrap(), None);
    }
}
