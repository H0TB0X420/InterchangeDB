//! Query execution — Volcano operator model.
//!
//! Operators implement [`Executor`] and compose into a tree. Each call to
//! `next()` pulls one tuple from children, applies the operator's own logic,
//! and yields. `Ok(None)` signals end-of-stream; `Err(_)` is fatal and
//! short-circuits the query.
//!
//! ## Context threading
//!
//! There is no ambient `ExecutionContext`. Each operator is constructed
//! with whatever it needs (a `TxnEngine` handle, a `Table`, a child
//! operator) bundled in at construction time. Snapshot/txn binding lives
//! on the `TxnEngine` handle the operator holds.
//!
//! ## Why pull-based (Volcano) for V1
//!
//! Single-tuple-at-a-time iteration is the simplest model that handles
//! arbitrary operator trees correctly. Push-based / vectorized executors
//! (HyPer, DuckDB) outperform Volcano on analytical workloads but add
//! significant complexity to operator state management. The [`ExecutionModel`]
//! trait (`model.rs`) names this choice: [`Volcano`] is the pull-based first
//! impl; a push-based second impl is the Phase 15 work in progress — the
//! interchangeability promise. TPC-C is OLTP, where Volcano is competitive.

pub mod build;
pub mod model;
pub mod operators;
pub mod push;

#[cfg(test)]
pub(crate) mod test_util;

pub use model::{ExecModel, ExecutionModel, Volcano};
pub use operators::delete::Delete;
pub use operators::filter::Filter;
pub use operators::hash_aggregate::{AggregateFn, HashAggregate};
pub use operators::index_scan::IndexScan;
pub use operators::insert::Insert;
pub use operators::join::{
    HashJoin, IndexNestedLoopJoin, JoinPredicate, JoinStrategy, MergeJoin, NestedLoopJoin,
};
pub use operators::limit::Limit;
pub use operators::pk_lookup::PkLookup;
pub use operators::projection::Projection;
pub use operators::seq_scan::SeqScan;
pub use operators::sort::{Sort, SortDir};
pub use operators::update::{SetExpr, Update};

use crate::catalog::{ColumnDef, Schema, TableId};
use crate::common::Result;
use crate::types::{ColumnType, Value};

/// A row of values in schema order. Reuses the shape `Table::scan` already
/// emits, so layout-level I/O and execution speak the same tuple type.
pub type Tuple = Vec<Value>;

/// Pull-based query operator.
///
/// Returns:
///   - `Ok(Some(tuple))` — one row produced.
///   - `Ok(None)`        — end of stream; subsequent calls also return `Ok(None)`.
///   - `Err(e)`          — fatal error; caller aborts the query.
///
/// `schema()` describes the tuples this operator produces. May differ from
/// any child's schema — e.g., `Projection` narrows columns, `Join` widens.
///
/// The trait is object-safe by design: parents hold `Box<dyn Executor>`
/// children so a single tree can mix operator types and engine bindings.
pub trait Executor: Send {
    /// Produce the next tuple, or `None` at end of stream.
    fn next(&mut self) -> Result<Option<Tuple>>;

    /// Shape of tuples this operator yields.
    fn schema(&self) -> &Schema;

    /// Render this operator (and recursively its children) as an indented
    /// tree string. Each line ends with `\n`; the final line includes one
    /// too so trees concatenate cleanly.
    ///
    /// Default impl emits a generic stub — every concrete operator should
    /// override to identify itself and recurse into its children.
    fn explain(&self, indent: usize) -> String {
        format!("{}<unnamed-operator>\n", "  ".repeat(indent))
    }
}

/// Synthesized one-column schema for DML operators that yield a row-count
/// summary tuple. `col_name` is the operation tag ("rows_inserted",
/// "rows_updated", "rows_deleted").
///
/// `table_id` is `TableId(0)` — synthetic; not a real table.
pub(crate) fn count_result_schema(col_name: &str) -> Schema {
    Schema {
        name: "dml_result".into(),
        table_id: TableId(0),
        columns: vec![ColumnDef {
            name: col_name.into(),
            ty: ColumnType::Int64,
            nullable: false,
            default: None,
        }],
        primary_key: vec![],
    }
}
