//! `Limit` — yields at most `max_rows` tuples from a child, then None.
//!
//! Counter-bounded passthrough. Schema is identical to child's. The child
//! is not advanced beyond the limit — `next()` returns `Ok(None)` without
//! pulling once the budget is exhausted, so `LIMIT 0` is a true zero-cost
//! short-circuit.

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{Executor, Tuple};

/// Volcano pipeline operator: caps output row count.
///
/// Tracks `max_rows` (the plan's declared cap) and `yielded` (runtime
/// counter) as separate fields so EXPLAIN can show the plan-level bound
/// independent of execution progress.
pub struct Limit {
    child: Box<dyn Executor>,
    max_rows: usize,
    yielded: usize,
}

impl Limit {
    pub fn new(child: Box<dyn Executor>, max_rows: usize) -> Self {
        Self {
            child,
            max_rows,
            yielded: 0,
        }
    }
}

impl Executor for Limit {
    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.yielded >= self.max_rows {
            return Ok(None);
        }
        match self.child.next()? {
            Some(t) => {
                self.yielded += 1;
                Ok(Some(t))
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn explain(&self, indent: usize) -> String {
        let mut out = format!("{}Limit({})\n", "  ".repeat(indent), self.max_rows);
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

    fn int_schema() -> Schema {
        Schema {
            name: "t".into(),
            table_id: TableId(1),
            columns: vec![ColumnDef {
                name: "n".into(),
                ty: ColumnType::Int32,
                nullable: false,
                default: None,
            }],
            primary_key: vec![0],
        }
    }

    fn t(n: i32) -> Tuple {
        vec![Value::Int32(n)]
    }

    #[test]
    fn yields_at_most_max_rows() {
        let child = Box::new(VecExecutor::new(
            int_schema(),
            vec![t(1), t(2), t(3), t(4), t(5)],
        ));
        let mut op = Limit::new(child, 3);
        assert_eq!(op.next().unwrap(), Some(t(1)));
        assert_eq!(op.next().unwrap(), Some(t(2)));
        assert_eq!(op.next().unwrap(), Some(t(3)));
        assert_eq!(op.next().unwrap(), None);
        // Past-end is also None.
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn limit_zero_yields_none_without_pulling() {
        let child = Box::new(VecExecutor::new(int_schema(), vec![t(1), t(2)]));
        let mut op = Limit::new(child, 0);
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn limit_larger_than_input_yields_all() {
        let child = Box::new(VecExecutor::new(int_schema(), vec![t(1), t(2)]));
        let mut op = Limit::new(child, 100);
        assert_eq!(op.next().unwrap(), Some(t(1)));
        assert_eq!(op.next().unwrap(), Some(t(2)));
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn schema_matches_child() {
        let child = Box::new(VecExecutor::new(int_schema(), vec![]));
        let op = Limit::new(child, 5);
        assert_eq!(op.schema().name, "t");
    }
}
