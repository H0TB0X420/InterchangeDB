//! `Filter` — selects rows from a child that match a predicate.
//!
//! Pulls from `child` one tuple at a time, applies `predicate`, and yields
//! only those that pass. Output schema is identical to child's.

use crate::catalog::Schema;
use crate::common::Result;
use crate::execution::{Executor, Tuple};
use crate::sql::ir::expr::CorrelatedFault;

/// Volcano pipeline operator: row-wise selection.
pub struct Filter {
    child: Box<dyn Executor>,
    predicate: Box<dyn Fn(&Tuple) -> bool + Send>,
    /// Shared error channel for a correlated predicate (H4c): the compiled
    /// closure records a correlated evaluation fault here, and `next` surfaces
    /// it after each predicate call. `None` for a plain predicate (the common
    /// case) — no per-row check, exactly the pre-H4c behavior.
    fault: Option<CorrelatedFault>,
}

impl Filter {
    /// Construct a Filter. Predicate is wrapped internally — no `Box::new`
    /// at the call site.
    pub fn new<F>(child: Box<dyn Executor>, predicate: F) -> Self
    where
        F: Fn(&Tuple) -> bool + Send + 'static,
    {
        Self {
            child,
            predicate: Box::new(predicate),
            fault: None,
        }
    }

    /// Construct from an already-boxed predicate. Used by the planner when
    /// binding compiled `Predicate` closures — avoids the double-Box that
    /// `Filter::new(child, |t| f(t))` would produce.
    pub fn from_boxed(
        child: Box<dyn Executor>,
        predicate: Box<dyn Fn(&Tuple) -> bool + Send>,
    ) -> Self {
        Self {
            child,
            predicate,
            fault: None,
        }
    }

    /// Construct from a boxed correlated predicate (H4c) and the shared `fault`
    /// cell its closure records into — checked after each predicate call.
    pub fn from_boxed_correlated(
        child: Box<dyn Executor>,
        predicate: Box<dyn Fn(&Tuple) -> bool + Send>,
        fault: CorrelatedFault,
    ) -> Self {
        Self {
            child,
            predicate,
            fault: Some(fault),
        }
    }
}

impl Executor for Filter {
    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            match self.child.next()? {
                Some(tuple) => {
                    let keep = (self.predicate)(&tuple);
                    // A correlated predicate may have recorded a fault (an inner
                    // execution error, or a scalar subquery yielding >1 rows) —
                    // surface it loudly before emitting, regardless of `keep`.
                    if let Some(fault) = &self.fault {
                        if let Some(err) = fault.lock().take() {
                            return Err(err);
                        }
                    }
                    if keep {
                        return Ok(Some(tuple));
                    }
                }
                None => return Ok(None),
            }
        }
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn explain(&self, indent: usize) -> String {
        // NOTE (plan deviation): predicate is a closure — no programmatic
        // description available without the planner attaching one. Phase 11
        // will pass a predicate description string at construction.
        let mut out = format!("{}Filter\n", "  ".repeat(indent));
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
    fn passes_only_matching_tuples() {
        let child = Box::new(VecExecutor::new(
            int_schema(),
            vec![t(1), t(2), t(3), t(4), t(5)],
        ));
        let mut op = Filter::new(child, |row| matches!(row[0], Value::Int32(n) if n % 2 == 0));
        assert_eq!(op.next().unwrap(), Some(t(2)));
        assert_eq!(op.next().unwrap(), Some(t(4)));
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn empty_child_yields_none() {
        let child = Box::new(VecExecutor::new(int_schema(), vec![]));
        let mut op = Filter::new(child, |_| true);
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn predicate_that_rejects_all_yields_none() {
        let child = Box::new(VecExecutor::new(int_schema(), vec![t(1), t(2), t(3)]));
        let mut op = Filter::new(child, |_| false);
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn predicate_that_accepts_all_passes_through() {
        let child = Box::new(VecExecutor::new(int_schema(), vec![t(1), t(2)]));
        let mut op = Filter::new(child, |_| true);
        assert_eq!(op.next().unwrap(), Some(t(1)));
        assert_eq!(op.next().unwrap(), Some(t(2)));
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn schema_matches_child() {
        let child = Box::new(VecExecutor::new(int_schema(), vec![]));
        let op = Filter::new(child, |_| true);
        assert_eq!(op.schema().name, "t");
        assert_eq!(op.schema().columns.len(), 1);
    }
}
