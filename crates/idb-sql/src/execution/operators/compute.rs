//! `Compute` — row-level computed projection (H2b).
//!
//! Emits one output column per `Expression`, evaluated against the input
//! tuple. Where `Projection` only selects/reorders columns, `Compute`
//! carries arithmetic (`SELECT a + b`, `SELECT 100.00 * SUM(x) / SUM(y)`)
//! and free display order (`SELECT COUNT(*), region`). The expression
//! closures compile once at construction; the per-row `next()` path just
//! evaluates them.

use std::sync::Arc;

use crate::catalog::{ColumnDef, Schema};
use crate::common::{Error, Result};
use crate::execution::{Executor, Tuple};
use crate::sql::ir::expr::Expression;
use crate::types::Value;

/// One compiled per-row output-column evaluator (`Expression::compile`'s
/// return type). Shared with the push model's `ComputeSink`.
pub(crate) type ColumnEvaluator = Box<dyn Fn(&Tuple) -> Value + Send>;

/// Volcano pipeline operator: per-row computed projection.
pub struct Compute {
    child: Box<dyn Executor>,
    /// The un-compiled expressions, kept for `explain` (the compiled
    /// closures below can't render themselves).
    exprs: Vec<Expression>,
    /// One compiled evaluator per output column, parallel to `exprs`.
    evals: Vec<ColumnEvaluator>,
    schema: Arc<Schema>,
}

impl Compute {
    /// Construct a Compute. `exprs` index the child's schema; each yields
    /// one output column. The output schema is inferred once here (see
    /// `build_compute_schema`); an uninferable expression is a bad plan and
    /// errors loudly rather than reaching the per-row loop.
    pub fn new(child: Box<dyn Executor>, exprs: Vec<Expression>) -> Result<Self> {
        let schema = Arc::new(build_compute_schema(child.schema(), &exprs)?);
        let evals = exprs.iter().cloned().map(Expression::compile).collect();
        Ok(Self {
            child,
            exprs,
            evals,
            schema,
        })
    }
}

impl Executor for Compute {
    fn next(&mut self) -> Result<Option<Tuple>> {
        match self.child.next()? {
            Some(tuple) => Ok(Some(self.evals.iter().map(|f| f(&tuple)).collect())),
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        // Matches `PhysOp::Compute`'s IR rendering: each expr via
        // `Expression::Display` (`Column(i) → c{i}`).
        let items: Vec<String> = self.exprs.iter().map(|e| e.to_string()).collect();
        let mut out = format!("{}Compute[{}]\n", "  ".repeat(indent), items.join(", "));
        out.push_str(&self.child.explain(indent + 1));
        out
    }
}

/// Output schema of a `Compute`: one column per expression. The name is the
/// input column's name for a bare `Column(i)` (so a passed-through column
/// keeps its label) and "expr" otherwise. The type is
/// `Expression::column_type` over the child's column types — a `None`
/// inference (e.g. `Varchar + Int`, or the scale-mismatched
/// `SUM(x) / SUM(y)`) is a bad plan and errors loudly, the same "cannot
/// infer" refusal the aggregate kernel makes. Nullability splits by arm: a
/// bare `Column(i)` is a pure passthrough and inherits the input column's
/// flag (matching `Projection` and `build_aggregate_schema`'s group keys); a
/// computed column is nullable because arithmetic (NULL propagation,
/// div-by-zero) can produce NULL even from non-nullable inputs.
pub(crate) fn build_compute_schema(input: &Schema, exprs: &[Expression]) -> Result<Schema> {
    let types = input.column_types();
    let mut columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let ty = expr.column_type(&types).ok_or_else(|| {
            Error::SqlParse(format!(
                "compute: cannot infer type of expression `{}`",
                expr
            ))
        })?;
        let (name, nullable) = match expr {
            // Passthrough: keep the input column's label and nullability.
            Expression::Column(i) => (input.columns[*i].name.clone(), input.columns[*i].nullable),
            // Computed: named "expr", nullable (arithmetic can produce NULL).
            _ => ("expr".to_string(), true),
        };
        columns.push(ColumnDef {
            name,
            ty,
            nullable,
            default: None,
        });
    }
    Ok(Schema {
        name: input.name.clone(),
        table_id: input.table_id,
        columns,
        primary_key: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableId;
    use crate::execution::test_util::VecExecutor;
    use crate::sql::ir::expr::BinaryOp;
    use crate::types::ColumnType;

    fn two_int32_schema() -> Schema {
        Schema {
            name: "t".into(),
            table_id: TableId(1),
            columns: vec![
                ColumnDef {
                    name: "a".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: "b".into(),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec![0],
        }
    }

    fn add(l: Expression, r: Expression) -> Expression {
        Expression::BinaryOp {
            op: BinaryOp::Add,
            left: Box::new(l),
            right: Box::new(r),
        }
    }

    #[test]
    fn evaluates_arithmetic_per_row() {
        let child = Box::new(VecExecutor::new(
            two_int32_schema(),
            vec![
                vec![Value::Int32(1), Value::Int32(10)],
                vec![Value::Int32(2), Value::Int32(20)],
            ],
        ));
        // SELECT a + b, a  → two output columns.
        let mut op = Compute::new(
            child,
            vec![
                add(Expression::Column(0), Expression::Column(1)),
                Expression::Column(0),
            ],
        )
        .unwrap();
        assert_eq!(
            op.next().unwrap(),
            Some(vec![Value::Int32(11), Value::Int32(1)])
        );
        assert_eq!(
            op.next().unwrap(),
            Some(vec![Value::Int32(22), Value::Int32(2)])
        );
        assert_eq!(op.next().unwrap(), None);
    }

    #[test]
    fn bare_column_keeps_name_computed_is_expr() {
        let child = Box::new(VecExecutor::new(two_int32_schema(), vec![]));
        let op = Compute::new(
            child,
            vec![
                Expression::Column(1),
                add(Expression::Column(0), Expression::Column(1)),
            ],
        )
        .unwrap();
        assert_eq!(op.schema().columns.len(), 2);
        assert_eq!(op.schema().columns[0].name, "b");
        assert_eq!(op.schema().columns[1].name, "expr");
        assert_eq!(op.schema().columns[1].ty, ColumnType::Int32);
    }

    #[test]
    fn uninferable_expression_errors_loudly() {
        // Column 0 is Int32; adding a Varchar literal has no `eval_binary_op`
        // arm → column_type None → loud "cannot infer" at construction.
        let child = Box::new(VecExecutor::new(two_int32_schema(), vec![]));
        let bad = add(
            Expression::Column(0),
            Expression::Literal(Value::Varchar("x".into())),
        );
        match Compute::new(child, vec![bad]) {
            Err(Error::SqlParse(msg)) => assert!(msg.contains("cannot infer"), "{msg}"),
            other => panic!("expected SqlParse cannot-infer, got {:?}", other.err()),
        }
    }
}
