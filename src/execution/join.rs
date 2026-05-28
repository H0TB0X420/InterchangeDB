//! Join operators (Phase 12).
//!
//! ## `JoinStrategy` trait
//!
//! Marker trait for join algorithms. Every join algorithm impls
//! `JoinStrategy` *and* `Executor` — the marker is a Phase-12 anchor for
//! the interchangeability thesis (Phase 14's `HashJoin` is the second
//! impl; Phase 18's planner-comparison harness picks among them).
//!
//! ## `NestedLoopJoin`
//!
//! Classic two-loop join: for each row of `outer`, scan `inner` and
//! emit `outer || inner` whenever `predicate(outer_row, inner_row)`
//! returns true. Output schema is the concatenation of outer + inner
//! schemas.
//!
//! Inner-only (no LEFT/RIGHT/FULL outer) for P12.7. The plan documents
//! the trait shape evolution if outer joins are needed before Phase 22.
//!
//! ## Cost shape
//!
//! O(outer × inner). The inner side is re-iterated for every outer row,
//! so this is the worst-case join algorithm — fine for small inners
//! (TPC-C's district / warehouse tables) or correctness baselines, not
//! for analytical workloads. Phase 14 introduces `HashJoin` as the
//! second `JoinStrategy` for those cases.

use std::sync::Arc;

use crate::catalog::{ColumnDef, Schema, TableId};
use crate::common::Result;
use crate::execution::{Executor, Tuple};

/// Marker trait for join-algorithm operators. Every implementor also
/// implements `Executor`; this exists so the planner / cost model can
/// reason about join strategies as a distinct family.
pub trait JoinStrategy: Executor {
    /// Identifier of the join algorithm — e.g., `"nested-loop"`,
    /// `"index-nested-loop"`, `"hash"`. Used in EXPLAIN output and by
    /// the Phase-14 cost model for algorithm-aware plan comparison.
    fn algorithm(&self) -> &'static str;
}

/// Predicate over a pair of `(outer, inner)` tuples. The join operator
/// owns the closure and calls it per pair. Returns `true` to emit a
/// joined row.
pub type JoinPredicate = Box<dyn Fn(&Tuple, &Tuple) -> bool + Send>;

/// Inner nested-loop join. Eager-materializes the inner side once, then
/// iterates the outer side lazily, scanning the buffered inner on each
/// outer row.
pub struct NestedLoopJoin {
    schema: Arc<Schema>,
    outer: Box<dyn Executor>,
    inner_rows: Vec<Tuple>,
    predicate: JoinPredicate,

    /// Cached current outer row; we walk through all inner rows for each.
    current_outer: Option<Tuple>,
    inner_cursor: usize,
}

impl NestedLoopJoin {
    /// Construct from two children + a join predicate.
    ///
    /// `outer` is the driving side (iterated once); `inner` is buffered
    /// up-front by draining it via `next()` and stored in a `Vec`. The
    /// caller decides which side is outer — heuristic: pick the smaller
    /// expected output as the inner to minimize buffered memory.
    /// (Phase 14's cost model automates this choice.)
    pub fn new(
        outer: Box<dyn Executor>,
        mut inner: Box<dyn Executor>,
        predicate: JoinPredicate,
    ) -> Result<Self> {
        // Pre-materialize the inner side.
        let mut inner_rows = Vec::new();
        while let Some(t) = inner.next()? {
            inner_rows.push(t);
        }
        let schema = Arc::new(concat_schemas(outer.schema(), inner.schema()));
        Ok(Self {
            schema,
            outer,
            inner_rows,
            predicate,
            current_outer: None,
            inner_cursor: 0,
        })
    }
}

impl Executor for NestedLoopJoin {
    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            // If we don't have a current outer row, pull the next one.
            if self.current_outer.is_none() {
                self.current_outer = self.outer.next()?;
                self.inner_cursor = 0;
                if self.current_outer.is_none() {
                    // Outer exhausted → join is done.
                    return Ok(None);
                }
            }
            let outer_row = self.current_outer.as_ref().unwrap();

            // Scan inner from the current cursor; emit on first match.
            while self.inner_cursor < self.inner_rows.len() {
                let inner_row = &self.inner_rows[self.inner_cursor];
                self.inner_cursor += 1;
                if (self.predicate)(outer_row, inner_row) {
                    let mut joined = outer_row.clone();
                    joined.extend_from_slice(inner_row);
                    return Ok(Some(joined));
                }
            }

            // Inner exhausted for this outer row → advance outer.
            self.current_outer = None;
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        let mut out = format!("{}NestedLoopJoin\n", pad);
        out.push_str(&self.outer.explain(indent + 1));
        // The inner side was consumed at construction time so we can't
        // recurse into it for EXPLAIN. Render a placeholder so plan
        // readers know an inner existed. Phase 14 (where join planning
        // becomes interesting) will retain the inner plan separately.
        out.push_str(&format!(
            "{}<materialized inner: {} rows>\n",
            "  ".repeat(indent + 1),
            self.inner_rows.len()
        ));
        out
    }
}

impl JoinStrategy for NestedLoopJoin {
    fn algorithm(&self) -> &'static str {
        "nested-loop"
    }
}

// ===========================================================================
// IndexNestedLoopJoin
// ===========================================================================

/// Index nested-loop join. For each outer row, build a key from the
/// configured outer columns and probe the inner table's secondary
/// index. Emits one joined row per matching inner row.
///
/// Cost: O(outer × index_lookup) — typically O(outer × log inner) for
/// BTree-backed indexes. Strictly better than `NestedLoopJoin` when the
/// inner has a usable index on the join key; Phase 14's cost model
/// picks between them.
pub struct IndexNestedLoopJoin<E: crate::storage::StorageEngine + 'static, L: crate::layout::DataLayout> {
    schema: Arc<Schema>,
    outer: Box<dyn Executor>,
    inner_table: Arc<crate::table::Table<E, L>>,
    inner_index: crate::table::IndexHandle,
    /// Column indices on the outer side that supply the index probe key.
    /// e.g. for `WHERE w.w_id = d.d_w_id` joined with an index on
    /// `district.d_w_id`, this is `[outer position of w_id]`.
    outer_key_cols: Vec<usize>,

    /// Cached current outer row + buffered matches we're streaming back.
    current_outer: Option<crate::execution::Tuple>,
    current_inner_buffer: Vec<crate::execution::Tuple>,
    inner_cursor: usize,
}

impl<E: crate::storage::StorageEngine + 'static, L: crate::layout::DataLayout> IndexNestedLoopJoin<E, L> {
    pub fn new(
        outer: Box<dyn Executor>,
        inner_table: Arc<crate::table::Table<E, L>>,
        inner_index: crate::table::IndexHandle,
        outer_key_cols: Vec<usize>,
    ) -> Result<Self> {
        // Validate arity now so test failures are precise. The probe key
        // arity must match the index's indexed-column count.
        if outer_key_cols.len() != inner_index.def.columns.len() {
            return Err(crate::common::Error::IndexLookupArity {
                index: inner_index.def.name.clone(),
                expected: inner_index.def.columns.len(),
                actual: outer_key_cols.len(),
            });
        }
        let schema = Arc::new(concat_schemas(outer.schema(), inner_table.schema()));
        Ok(Self {
            schema,
            outer,
            inner_table,
            inner_index,
            outer_key_cols,
            current_outer: None,
            current_inner_buffer: Vec::new(),
            inner_cursor: 0,
        })
    }

    /// Re-fill `current_inner_buffer` by probing the index with the
    /// current outer row's key.
    fn refill_inner(&mut self) -> Result<()> {
        self.current_inner_buffer.clear();
        self.inner_cursor = 0;
        let outer_row = match &self.current_outer {
            Some(r) => r,
            None => return Ok(()),
        };
        let prefix: Vec<crate::types::Value> = self
            .outer_key_cols
            .iter()
            .map(|&i| outer_row[i].clone())
            .collect();
        let mut scan = crate::execution::IndexScan::new(
            &*self.inner_table,
            &self.inner_index,
            &prefix,
        )?;
        while let Some(row) = scan.next()? {
            self.current_inner_buffer.push(row);
        }
        Ok(())
    }
}

impl<E: crate::storage::StorageEngine + 'static, L: crate::layout::DataLayout> Executor
    for IndexNestedLoopJoin<E, L>
{
    fn next(&mut self) -> Result<Option<crate::execution::Tuple>> {
        loop {
            if self.current_outer.is_none() {
                self.current_outer = self.outer.next()?;
                if self.current_outer.is_none() {
                    return Ok(None);
                }
                self.refill_inner()?;
            }
            // Drain the current inner buffer.
            if self.inner_cursor < self.current_inner_buffer.len() {
                let inner_row = &self.current_inner_buffer[self.inner_cursor];
                self.inner_cursor += 1;
                let outer_row = self.current_outer.as_ref().unwrap();
                let mut joined = outer_row.clone();
                joined.extend_from_slice(inner_row);
                return Ok(Some(joined));
            }
            // Inner buffer exhausted for this outer row — advance.
            self.current_outer = None;
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        let mut out = format!(
            "{}IndexNestedLoopJoin(probe via {})\n",
            pad, self.inner_index.def.name
        );
        out.push_str(&self.outer.explain(indent + 1));
        out.push_str(&format!(
            "{}<inner: IndexScan on {}>\n",
            "  ".repeat(indent + 1),
            self.inner_index.def.name
        ));
        out
    }
}

impl<E: crate::storage::StorageEngine + 'static, L: crate::layout::DataLayout> JoinStrategy
    for IndexNestedLoopJoin<E, L>
{
    fn algorithm(&self) -> &'static str {
        "index-nested-loop"
    }
}

/// Build the output schema for a join: outer columns then inner columns,
/// with column names disambiguated by table-name prefix when both sides
/// have a column of the same name.
fn concat_schemas(outer: &Schema, inner: &Schema) -> Schema {
    let mut columns: Vec<ColumnDef> = Vec::with_capacity(outer.columns.len() + inner.columns.len());
    columns.extend_from_slice(&outer.columns);
    for col in &inner.columns {
        let qualified = if outer.columns.iter().any(|c| c.name == col.name) {
            ColumnDef {
                name: format!("{}.{}", inner.name, col.name),
                ty: col.ty,
                nullable: col.nullable,
                default: col.default.clone(),
            }
        } else {
            col.clone()
        };
        columns.push(qualified);
    }
    Schema {
        name: format!("{}_join_{}", outer.name, inner.name),
        // Joins yield synthetic tuples — no underlying table. Sentinel id.
        table_id: TableId(0),
        columns,
        // Joined tuples have no PK — primary_key positions would alias
        // ambiguously across sides.
        primary_key: vec![],
    }
}
