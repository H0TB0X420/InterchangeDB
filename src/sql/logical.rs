//! `LogicalPlan` — our internal, catalog-resolved query representation.
//!
//! Sits between sqlparser's AST (generic SQL) and the executor tree (our
//! `Box<dyn Executor>`). The AST → LogicalPlan conversion (Step 4)
//! resolves table & column names against the `Catalog`, validates types,
//! and produces this IR. The planner (Step 5) lowers each variant into a
//! `Box<dyn Executor>`.
//!
//! ## Why a separate IR
//!
//! - **Resolution happens once.** Names become indices; type checks run
//!   at bind time, not per-tuple.
//! - **Decouples from sqlparser.** If we ever swap parser libraries,
//!   only the binder changes — the planner is unaffected.
//! - **Optimizer-ready.** Phase 14 (Selinger) and Phase 17/18 (Cascades)
//!   operate on `LogicalPlan`, not on parser AST. The shape is ours to
//!   evolve as the optimizer needs.

use serde::{Deserialize, Serialize};

use crate::catalog::ColumnDef;
use crate::sql::expr::{Expression, Predicate};
use crate::types::Value;

/// One catalog-resolved SQL statement.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    // --- DDL (catalog-only; not executed by operator tree) -----------------
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        /// Column indices that compose the primary key.
        primary_key: Vec<usize>,
    },

    // --- DML (run through executor tree) ----------------------------------
    /// `SELECT [projection|aggregates] FROM table [JOIN …]* [WHERE filter] [LIMIT limit]`.
    ///
    /// `joins` is empty for a single-table SELECT (today's common case).
    /// Column indices in `projection` and `filter` are tuple-global: they
    /// index into the concatenated `table || joins[0] || joins[1] || …`
    /// row produced by the operator tree.
    ///
    /// Exactly one of `projection` and `aggregates` is non-empty:
    /// - `projection` non-empty, `aggregates` empty: column-projection (or
    ///   `SELECT *` when both are empty).
    /// - `aggregates` non-empty, `projection` empty: whole-table
    ///   aggregation (no GROUP BY).
    /// - Both non-empty: GROUP BY semantics (rejected by the binder for
    ///   now — lands in Phase 14 alongside `HashAggregate`'s grouped path).
    Select {
        table: String,
        joins: Vec<JoinClause>,
        /// Column indices in the requested output order. Empty = `SELECT *`
        /// (when aggregates is also empty) or whole-table aggregation (when
        /// aggregates is non-empty).
        projection: Vec<usize>,
        /// Aggregate functions to compute over the rows. Empty means no
        /// aggregation.
        aggregates: Vec<AggregateSpec>,
        filter: Option<Predicate>,
        /// `ORDER BY (col, dir)+`. Column indices are tuple-global (same
        /// scope as `projection` / `filter`). Empty means unsorted.
        order_by: Vec<(usize, OrderDir)>,
        limit: Option<usize>,
    },

    /// `INSERT INTO table VALUES (row1), (row2), …`.
    ///
    /// Each row is a vector of *expressions* in schema order. Most are
    /// literals; `Expression::Parameter(i)` slots support prepared
    /// statements. Computed inserts (`INSERT … SELECT`) are Phase 14.
    Insert {
        table: String,
        rows: Vec<Vec<Expression>>,
    },

    /// `UPDATE table SET col = expr, … [WHERE filter]`.
    Update {
        table: String,
        /// `(column_index, value_expression)` pairs. Indices into the
        /// target table's schema.
        set_clauses: Vec<(usize, Expression)>,
        filter: Option<Predicate>,
    },

    /// `DELETE FROM table [WHERE filter]`.
    Delete {
        table: String,
        filter: Option<Predicate>,
    },

    // --- Catalog admin ----------------------------------------------------
    /// `ANALYZE TABLE t` (P14.2) — scan `t` once, compute row count,
    /// per-column NDV, and per-column equi-width histogram, persist into
    /// `__sys_table_stats` and `__sys_column_stats`.
    Analyze {
        table: String,
    },

    // --- Transaction control ----------------------------------------------
    BeginTxn,
    CommitTxn,
    AbortTxn,

    // --- Plan introspection -----------------------------------------------
    /// `EXPLAIN <stmt>` — wraps any other plan; planner returns the
    /// child's executor-tree explain string instead of executing it.
    Explain(Box<LogicalPlan>),
}

/// One JOIN clause attached to a SELECT. The right side is always a
/// single table at this stage (3+-way joins compose by repeating). The
/// `on` predicate is `None` for implicit `FROM a, b` (cross product) and
/// `Some(p)` for explicit `JOIN b ON p`. WHERE filters are kept separate
/// — Phase 14's cost-based planner will revisit predicate pushdown.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub right_table: String,
    pub right_alias: Option<String>,
    pub on: Option<Predicate>,
}

/// SQL-level aggregate function spec. The planner translates each
/// variant into the corresponding `execution::AggregateFn`. Column
/// references are tuple-global indices (same convention as
/// `Predicate`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateSpec {
    CountStar,
    Count { col: usize, distinct: bool },
    Sum(usize),
    Min(usize),
    Max(usize),
    Avg(usize),
}

/// SQL-level sort direction. Separate from `execution::SortDir` so the
/// SQL IR has no executor dependency; the planner translates between
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderDir {
    Asc,
    Desc,
}

impl LogicalPlan {
    /// Walk the plan tree and substitute every `Expression::Parameter(i)`
    /// with `Expression::Literal(params[i])`. Used by
    /// `PreparedStatement::execute` to bind parameters before planning.
    pub fn substitute_params(self, params: &[Value]) -> crate::common::Result<LogicalPlan> {
        match self {
            LogicalPlan::Select {
                table,
                joins,
                projection,
                aggregates,
                filter,
                order_by,
                limit,
            } => {
                let joins = joins
                    .into_iter()
                    .map(|j| {
                        Ok::<JoinClause, crate::common::Error>(JoinClause {
                            right_table: j.right_table,
                            right_alias: j.right_alias,
                            on: match j.on {
                                Some(p) => Some(p.substitute_params(params)?),
                                None => None,
                            },
                        })
                    })
                    .collect::<crate::common::Result<Vec<_>>>()?;
                let filter = match filter {
                    Some(p) => Some(p.substitute_params(params)?),
                    None => None,
                };
                Ok(LogicalPlan::Select {
                    table,
                    joins,
                    projection,
                    aggregates,
                    filter,
                    order_by,
                    limit,
                })
            }
            LogicalPlan::Insert { table, rows } => {
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|e| e.substitute_params(params))
                            .collect::<crate::common::Result<Vec<_>>>()
                    })
                    .collect::<crate::common::Result<Vec<_>>>()?;
                Ok(LogicalPlan::Insert { table, rows })
            }
            LogicalPlan::Update {
                table,
                set_clauses,
                filter,
            } => {
                let set_clauses = set_clauses
                    .into_iter()
                    .map(|(idx, expr)| {
                        Ok::<_, crate::common::Error>((idx, expr.substitute_params(params)?))
                    })
                    .collect::<crate::common::Result<Vec<_>>>()?;
                let filter = match filter {
                    Some(p) => Some(p.substitute_params(params)?),
                    None => None,
                };
                Ok(LogicalPlan::Update {
                    table,
                    set_clauses,
                    filter,
                })
            }
            LogicalPlan::Delete { table, filter } => {
                let filter = match filter {
                    Some(p) => Some(p.substitute_params(params)?),
                    None => None,
                };
                Ok(LogicalPlan::Delete { table, filter })
            }
            // No parameters in DDL or TC.
            other => Ok(other),
        }
    }
}
