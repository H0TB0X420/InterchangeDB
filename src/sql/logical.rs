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

use crate::catalog::ColumnDef;
use crate::sql::expr::{Expression, Predicate};
use crate::types::Value;

/// One catalog-resolved SQL statement.
#[derive(Debug)]
pub enum LogicalPlan {
    // --- DDL (catalog-only; not executed by operator tree) -----------------
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        /// Column indices that compose the primary key.
        primary_key: Vec<usize>,
    },

    // --- DML (run through executor tree) ----------------------------------
    /// `SELECT [projection] FROM table [WHERE filter] [LIMIT limit]`.
    Select {
        table: String,
        /// Column indices in the requested output order. Empty = `SELECT *`.
        projection: Vec<usize>,
        filter: Option<Predicate>,
        limit: Option<usize>,
    },

    /// `INSERT INTO table VALUES (row1), (row2), …`.
    ///
    /// Each row is a vector of literal values in schema order. Computed
    /// inserts (`INSERT … SELECT`) are Phase 12.
    Insert { table: String, rows: Vec<Vec<Value>> },

    /// `UPDATE table SET col = expr, … [WHERE filter]`.
    Update {
        table: String,
        /// `(column_index, value_expression)` pairs. Indices into the
        /// target table's schema.
        set_clauses: Vec<(usize, Expression)>,
        filter: Option<Predicate>,
    },

    /// `DELETE FROM table [WHERE filter]`.
    Delete { table: String, filter: Option<Predicate> },

    // --- Transaction control ----------------------------------------------
    BeginTxn,
    CommitTxn,
    AbortTxn,

    // --- Plan introspection -----------------------------------------------
    /// `EXPLAIN <stmt>` — wraps any other plan; planner returns the
    /// child's executor-tree explain string instead of executing it.
    Explain(Box<LogicalPlan>),
}
