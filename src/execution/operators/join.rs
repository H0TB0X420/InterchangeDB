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

    /// Debug-only bound: rows emitted for the current outer row. Exceeds
    /// `inner_rows.len()` only if the scan cursor regressed and re-matched
    /// a row — the emit-path analogue of the structurally bounded scan in
    /// `next()` (explicit-limits rule: crash, don't stream forever).
    #[cfg(debug_assertions)]
    emitted_for_outer: usize,
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
            #[cfg(debug_assertions)]
            emitted_for_outer: 0,
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
                #[cfg(debug_assertions)]
                {
                    self.emitted_for_outer = 0;
                }
                if self.current_outer.is_none() {
                    // Outer exhausted → join is done.
                    return Ok(None);
                }
            }
            let outer_row = self.current_outer.as_ref().unwrap();

            // Scan inner from the current cursor; emit on first match.
            // `for` over the remaining range is structurally bounded, so
            // the no-match scan terminates even if the cursor bookkeeping
            // regresses; the emit path is bounded by `emitted_for_outer`
            // above (together: the explicit-limits rule — a progress bug
            // crashes in debug instead of spinning or streaming forever).
            for idx in self.inner_cursor..self.inner_rows.len() {
                self.inner_cursor = idx + 1;
                let inner_row = &self.inner_rows[idx];
                if (self.predicate)(outer_row, inner_row) {
                    #[cfg(debug_assertions)]
                    {
                        self.emitted_for_outer += 1;
                        debug_assert!(
                            self.emitted_for_outer <= self.inner_rows.len(),
                            "nested-loop emitted more rows for one outer row than the inner holds"
                        );
                    }
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
pub struct IndexNestedLoopJoin<
    E: crate::storage::StorageEngine + 'static,
    L: crate::layout::DataLayout,
> {
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

impl<E: crate::storage::StorageEngine + 'static, L: crate::layout::DataLayout>
    IndexNestedLoopJoin<E, L>
{
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
        // Build the probe prefix, coercing each outer key to the indexed
        // column's type (`Int64(5)` outer vs `Int32` indexed column must
        // match, exactly as NestedLoopJoin's predicate would say). A NULL
        // key never matches (SQL equi-join), and a value not exactly
        // representable in the indexed type can't equal any entry — both
        // mean an empty probe result, not an error.
        let mut prefix: Vec<crate::types::Value> = Vec::with_capacity(self.outer_key_cols.len());
        for &i in &self.outer_key_cols {
            if matches!(outer_row[i], crate::types::Value::Null) {
                return Ok(());
            }
            let indexed_col = self.inner_index.def.columns[prefix.len()];
            let ty = &self.inner_table.schema().columns[indexed_col].ty;
            match outer_row[i].coerce_exact(ty) {
                Some(v) => prefix.push(v),
                None => return Ok(()),
            }
        }
        let mut scan =
            crate::execution::IndexScan::new(&*self.inner_table, &self.inner_index, &prefix)?;
        while let Some(row) = scan.next()? {
            // Recheck: secondary indexes are unversioned, so a stale entry
            // can dereference to a visible row whose indexed columns no
            // longer equal the probe key (same exposure as the planner's
            // IndexScan recheck Filter, which INLJ bypasses because the ON
            // predicate is consumed at plan time).
            let matches_probe =
                self.inner_index
                    .def
                    .columns
                    .iter()
                    .zip(&prefix)
                    .all(|(&col, probe)| {
                        row[col].compare_sql(probe) == Some(std::cmp::Ordering::Equal)
                    });
            if matches_probe {
                self.current_inner_buffer.push(row);
            }
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

// ===========================================================================
// HashJoin (P14.5)
// ===========================================================================

/// Equi-join via hash table. The `inner` side is fully materialized into
/// a `HashMap<Value, Vec<Tuple>>` keyed by the inner join column; the
/// `outer` side is then iterated and each outer row probes the table
/// once.
///
/// Cost shape: build is O(|inner|) memory + O(|inner|) CPU; probe is
/// O(|outer|) lookups. Strictly linear in total — beats `NestedLoopJoin`
/// (O(|outer| × |inner|)) past a few hundred rows per side, which is
/// why the cost model (P14.3) ranks `HashJoin` cheaper at scale.
///
/// ## Limitations
///
/// - **Equi-join only.** The join condition must be `outer.col =
///   inner.col`. Non-equi predicates (`<`, range) fall back to
///   `NestedLoopJoin` and are not lowered here.
/// - **Inner only.** No LEFT/RIGHT/FULL outer joins; matching the
///   `NestedLoopJoin` story.
/// - **NULL-skipping.** NULLs in either join column never match
///   (standard SQL equi-join semantics). The `HashMap` uses `Value`'s
///   own `Hash` (derived in P13.3) so other types hash deterministically.
/// - **In-memory only.** Spill-to-disk for oversized inner sides is
///   Phase 20. Phase 14's harness fits.
pub struct HashJoin {
    schema: Arc<Schema>,
    outer: Box<dyn Executor>,
    inner_table: std::collections::HashMap<crate::types::Value, Vec<crate::execution::Tuple>>,
    outer_key_col: usize,
    /// Cached current outer row + probe cursor through matched inner rows.
    current_outer: Option<crate::execution::Tuple>,
    matches_for_current: Vec<crate::execution::Tuple>,
    match_cursor: usize,
}

impl HashJoin {
    /// Construct. Drains `inner` immediately to build the hash table.
    /// `outer_key_col` / `inner_key_col` are the column positions of
    /// the join keys within each side's tuple.
    pub fn new(
        outer: Box<dyn Executor>,
        mut inner: Box<dyn Executor>,
        outer_key_col: usize,
        inner_key_col: usize,
    ) -> Result<Self> {
        let schema = Arc::new(concat_schemas(outer.schema(), inner.schema()));
        let mut inner_table: std::collections::HashMap<
            crate::types::Value,
            Vec<crate::execution::Tuple>,
        > = std::collections::HashMap::new();
        while let Some(row) = inner.next()? {
            if matches!(row[inner_key_col], crate::types::Value::Null) {
                // SQL equi-join: NULL never matches.
                continue;
            }
            // Canonical key form: hash-table equality must agree with
            // `Value::compare_sql` (Int32(5) joins Int64(5), 5 joins 5.00)
            // or the same query returns different rows under HashJoin than
            // under NestedLoopJoin — strategy inequivalence (E11).
            let key = row[inner_key_col].join_key_normalized();
            inner_table.entry(key).or_default().push(row);
        }
        Ok(Self {
            schema,
            outer,
            inner_table,
            outer_key_col,
            current_outer: None,
            matches_for_current: Vec::new(),
            match_cursor: 0,
        })
    }
}

impl Executor for HashJoin {
    fn next(&mut self) -> Result<Option<crate::execution::Tuple>> {
        loop {
            // Drain pending matches for the current outer row first.
            if self.match_cursor < self.matches_for_current.len() {
                let inner = &self.matches_for_current[self.match_cursor];
                self.match_cursor += 1;
                let outer = self.current_outer.as_ref().unwrap();
                let mut joined = outer.clone();
                joined.extend_from_slice(inner);
                return Ok(Some(joined));
            }
            // Need a new outer row.
            let row = match self.outer.next()? {
                Some(r) => r,
                None => return Ok(None),
            };
            let key = &row[self.outer_key_col];
            self.matches_for_current = if matches!(key, crate::types::Value::Null) {
                Vec::new()
            } else {
                // Probe with the same canonical form the build side used.
                let key = key.join_key_normalized();
                self.inner_table.get(&key).cloned().unwrap_or_default()
            };
            self.match_cursor = 0;
            self.current_outer = Some(row);
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        let mut out = format!(
            "{}HashJoin(outer_key={}, build_size={})\n",
            pad,
            self.outer_key_col,
            self.inner_table.len()
        );
        out.push_str(&self.outer.explain(indent + 1));
        out.push_str(&format!(
            "{}<hash-table inner: {} key groups>\n",
            "  ".repeat(indent + 1),
            self.inner_table.len()
        ));
        out
    }
}

impl JoinStrategy for HashJoin {
    fn algorithm(&self) -> &'static str {
        "hash"
    }
}

// ===========================================================================
// MergeJoin (T17-B.1)
// ===========================================================================

/// Merge equi-join over two inputs sorted ascending on their key
/// columns. The *caller* (the memo planner's Sort enforcer, 17-B.2)
/// guarantees the sortedness; this operator exploits it — and debug
/// builds verify it per pulled row, because an unsorted input would
/// silently drop matches (passed keys are discarded for good), which is
/// exactly the corruption the assert-and-crash doctrine exists to catch.
///
/// Algorithm: advance the lesser side via `Value::compare_sql` — the
/// canonical comparator, REQUIRED so `Int32(5)` merges with `Int64(5)`
/// exactly as Hash/NLJ match them (strategy equivalence, E11). On key
/// equality, buffer the full equal-key **run** from the right side and
/// emit its cross product with every left row sharing the key. NULL
/// keys never match (skipped, consistent with Hash/INLJ).
///
/// Both sides stream; memory is bounded by the largest equal-key run on
/// the right (not the input), plus a one-row lookahead. NOTE: the plan
/// (§7.1) allowed eager-materializing the right side if streaming was
/// awkward — it wasn't, so the streaming form landed.
pub struct MergeJoin {
    schema: Arc<Schema>,
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    left_key_col: usize,
    right_key_col: usize,

    current_left: Option<Tuple>,
    /// Right rows sharing `run_key`, replayed (from `run_cursor`) for
    /// every left row that equals the key.
    run: Vec<Tuple>,
    run_key: Option<crate::types::Value>,
    run_cursor: usize,
    /// First right row past the current run — un-consumed lookahead.
    right_pending: Option<Tuple>,
    right_exhausted: bool,
    #[cfg(debug_assertions)]
    debug: MergeDebug,
}

/// Debug-build guards for `MergeJoin` (review fix #2): per-side
/// sortedness verification plus pull counters for the no-progress
/// backstop in `next()`. Compiled out of release builds entirely.
#[cfg(debug_assertions)]
#[derive(Default)]
struct MergeDebug {
    last_left_key: Option<crate::types::Value>,
    last_right_key: Option<crate::types::Value>,
    left_pulled: u64,
    right_pulled: u64,
}

#[cfg(debug_assertions)]
impl MergeDebug {
    fn on_left_row(&mut self, key: &crate::types::Value) {
        self.left_pulled += 1;
        Self::assert_ascending(&mut self.last_left_key, key, "left");
    }

    fn on_right_row(&mut self, key: &crate::types::Value) {
        self.right_pulled += 1;
        Self::assert_ascending(&mut self.last_right_key, key, "right");
    }

    /// NULL keys are skipped by the merge, so their sort position is
    /// irrelevant; incomparable keys carry no order to violate.
    fn assert_ascending(
        last: &mut Option<crate::types::Value>,
        key: &crate::types::Value,
        side: &str,
    ) {
        if matches!(key, crate::types::Value::Null) {
            return;
        }
        if let Some(previous) = last {
            if let Some(ordering) = key.compare_sql(previous) {
                assert!(
                    ordering != std::cmp::Ordering::Less,
                    "merge join {side} input is not sorted ascending: {key:?} after {previous:?}"
                );
            }
        }
        *last = Some(key.clone());
    }
}

impl MergeJoin {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        left_key_col: usize,
        right_key_col: usize,
    ) -> Result<Self> {
        // Key columns must exist — fail precisely at construction, never
        // per-tuple (E16 discipline, matching Projection::new).
        if left_key_col >= left.schema().columns.len() {
            return Err(crate::common::Error::Internal(format!(
                "merge join left key column {} out of range ({} columns)",
                left_key_col,
                left.schema().columns.len()
            )));
        }
        if right_key_col >= right.schema().columns.len() {
            return Err(crate::common::Error::Internal(format!(
                "merge join right key column {} out of range ({} columns)",
                right_key_col,
                right.schema().columns.len()
            )));
        }
        let schema = Arc::new(concat_schemas(left.schema(), right.schema()));
        Ok(Self {
            schema,
            left,
            right,
            left_key_col,
            right_key_col,
            current_left: None,
            run: Vec::new(),
            run_key: None,
            run_cursor: 0,
            right_pending: None,
            right_exhausted: false,
            #[cfg(debug_assertions)]
            debug: MergeDebug::default(),
        })
    }

    /// Advance the right side to `target`'s equal-key run: keys below the
    /// target are dropped for good (the ascending left side can never
    /// need them again); the first key above it parks in `right_pending`.
    /// Leaves `run_key` as `None` when no right key equals the target.
    fn build_run(&mut self, target: &crate::types::Value) -> Result<()> {
        debug_assert!(
            !matches!(target, crate::types::Value::Null),
            "NULL left keys are skipped before run building"
        );
        self.run.clear();
        self.run_key = None;
        self.run_cursor = 0;
        loop {
            let row = match self.right_pending.take() {
                Some(row) => row,
                None => {
                    if self.right_exhausted {
                        return Ok(());
                    }
                    match self.right.next()? {
                        Some(row) => {
                            #[cfg(debug_assertions)]
                            self.debug.on_right_row(&row[self.right_key_col]);
                            row
                        }
                        None => {
                            self.right_exhausted = true;
                            return Ok(());
                        }
                    }
                }
            };
            if matches!(row[self.right_key_col], crate::types::Value::Null) {
                continue; // SQL equi-join: NULL right keys never match.
            }
            // Incomparable key domains (a join the binder let through,
            // e.g. Varchar vs Int) merge as "no match" — drop the right
            // row, mirroring Hash/NLJ, which simply find no pairs.
            let ordering = row[self.right_key_col]
                .compare_sql(target)
                .unwrap_or(std::cmp::Ordering::Less);
            match ordering {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => {
                    self.right_pending = Some(row);
                    return Ok(());
                }
                std::cmp::Ordering::Equal => {
                    self.run_key = Some(target.clone());
                    self.run.push(row);
                    // Collect the rest of the equal run; the first
                    // non-equal row parks as lookahead.
                    // Bounded: every iteration consumes one right row
                    // or returns.
                    loop {
                        let next_row = match self.right.next()? {
                            Some(row) => {
                                #[cfg(debug_assertions)]
                                self.debug.on_right_row(&row[self.right_key_col]);
                                row
                            }
                            None => {
                                self.right_exhausted = true;
                                return Ok(());
                            }
                        };
                        let equal = next_row[self.right_key_col].compare_sql(target)
                            == Some(std::cmp::Ordering::Equal);
                        if equal {
                            self.run.push(next_row);
                        } else {
                            self.right_pending = Some(next_row);
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

impl Executor for MergeJoin {
    fn next(&mut self) -> Result<Option<Tuple>> {
        // Each pass either returns a joined row, consumes a left row, or
        // advances the right side — progress is bounded by |left| + |right|.
        // The explicit-limits backstop below makes that claim mechanical:
        // two consecutive iterations with identical progress state would
        // be a livelock bug and crash debug builds.
        #[cfg(debug_assertions)]
        let mut previous_state = None;
        loop {
            #[cfg(debug_assertions)]
            {
                let state = (
                    self.debug.left_pulled,
                    self.debug.right_pulled,
                    self.run_cursor,
                    self.current_left.is_some(),
                    self.run_key.is_some(),
                );
                assert!(
                    previous_state != Some(state),
                    "merge join made no progress across an iteration"
                );
                previous_state = Some(state);
            }
            if self.current_left.is_none() {
                self.current_left = self.left.next()?;
                self.run_cursor = 0;
                if self.current_left.is_none() {
                    return Ok(None); // left exhausted → join done
                }
                #[cfg(debug_assertions)]
                self.debug
                    .on_left_row(&self.current_left.as_ref().unwrap()[self.left_key_col]);
            }
            let left_row = self.current_left.as_ref().unwrap();
            let left_key = &left_row[self.left_key_col];
            if matches!(left_key, crate::types::Value::Null) {
                // SQL equi-join: NULL left keys never match.
                self.current_left = None;
                continue;
            }

            if let Some(run_key) = &self.run_key {
                match left_key.compare_sql(run_key) {
                    Some(std::cmp::Ordering::Equal) => {
                        if self.run_cursor < self.run.len() {
                            let right_row = &self.run[self.run_cursor];
                            self.run_cursor += 1;
                            debug_assert!(self.run_cursor <= self.run.len());
                            let mut joined = left_row.clone();
                            joined.extend_from_slice(right_row);
                            return Ok(Some(joined));
                        }
                        // Run drained for this left row; the next left
                        // row may share the key and replay the run.
                        self.current_left = None;
                        continue;
                    }
                    Some(std::cmp::Ordering::Less) => {
                        // Left key below the run: the right side is
                        // already past every possible partner.
                        self.current_left = None;
                        continue;
                    }
                    // Greater or incomparable: the run is stale — build
                    // the next one below.
                    _ => {}
                }
            }

            let left_key = left_key.clone();
            self.build_run(&left_key)?;
            if self.run_key.is_none() {
                // No right key equals this left key.
                self.current_left = None;
            }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn explain(&self, indent: usize) -> String {
        let pad = "  ".repeat(indent);
        let mut out = format!(
            "{}MergeJoin(left_key={}, right_key={})\n",
            pad, self.left_key_col, self.right_key_col
        );
        out.push_str(&self.left.explain(indent + 1));
        out.push_str(&self.right.explain(indent + 1));
        out
    }
}

impl JoinStrategy for MergeJoin {
    fn algorithm(&self) -> &'static str {
        "merge"
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

#[cfg(test)]
mod merge_join_tests {
    use super::*;
    use crate::execution::test_util::VecExecutor;
    use crate::types::{ColumnType, Value};

    fn side_schema(name: &str, key_ty: ColumnType) -> Schema {
        Schema {
            name: name.into(),
            table_id: TableId(0),
            columns: vec![
                ColumnDef {
                    name: format!("{name}_id"),
                    ty: ColumnType::Int32,
                    nullable: false,
                    default: None,
                },
                ColumnDef {
                    name: format!("{name}_key"),
                    ty: key_ty,
                    nullable: true,
                    default: None,
                },
            ],
            primary_key: vec![0],
        }
    }

    fn exec(name: &str, key_ty: ColumnType, rows: Vec<Tuple>) -> Box<dyn Executor> {
        Box::new(VecExecutor::new(side_schema(name, key_ty), rows))
    }

    /// `(id, key)` row with an Int32 key; `None` → SQL NULL.
    fn row32(id: i32, key: Option<i32>) -> Tuple {
        vec![
            Value::Int32(id),
            key.map(Value::Int32).unwrap_or(Value::Null),
        ]
    }

    /// `(id, key)` row with an Int64 key; `None` → SQL NULL.
    fn row64(id: i32, key: Option<i64>) -> Tuple {
        vec![
            Value::Int32(id),
            key.map(Value::Int64).unwrap_or(Value::Null),
        ]
    }

    fn drain(mut join: MergeJoin) -> Vec<Tuple> {
        let mut out = Vec::new();
        while let Some(t) = join.next().unwrap() {
            out.push(t);
        }
        out
    }

    // HOW: duplicate keys on BOTH sides — the buffered right run must be
    // replayed for every equal left row, yielding the full cross product
    // per key group (2×3 for key 1, 1×2 for key 2).
    #[test]
    fn duplicate_key_runs_emit_full_cross_product() {
        let left = exec(
            "l",
            ColumnType::Int32,
            vec![row32(1, Some(1)), row32(2, Some(1)), row32(3, Some(2))],
        );
        let right = exec(
            "r",
            ColumnType::Int32,
            vec![
                row32(10, Some(1)),
                row32(11, Some(1)),
                row32(12, Some(1)),
                row32(13, Some(2)),
                row32(14, Some(2)),
            ],
        );
        let join = MergeJoin::new(left, right, 1, 1).unwrap();
        let rows = drain(join);
        assert_eq!(rows.len(), 2 * 3 + 2);
        // Positive space: every emitted pair actually matches on the key.
        for row in &rows {
            assert_eq!(row[1].compare_sql(&row[3]), Some(std::cmp::Ordering::Equal));
        }
    }

    #[test]
    fn empty_inputs_yield_no_rows() {
        let one = || vec![row32(1, Some(1))];
        let join = MergeJoin::new(
            exec("l", ColumnType::Int32, Vec::new()),
            exec("r", ColumnType::Int32, one()),
            1,
            1,
        )
        .unwrap();
        assert!(drain(join).is_empty(), "empty left");

        let join = MergeJoin::new(
            exec("l", ColumnType::Int32, one()),
            exec("r", ColumnType::Int32, Vec::new()),
            1,
            1,
        )
        .unwrap();
        assert!(drain(join).is_empty(), "empty right");

        let join = MergeJoin::new(
            exec("l", ColumnType::Int32, Vec::new()),
            exec("r", ColumnType::Int32, Vec::new()),
            1,
            1,
        )
        .unwrap();
        assert!(drain(join).is_empty(), "both empty");
    }

    // HOW: NULL keys on both sides, placed first as an ascending Sort
    // would deliver them — SQL equi-join semantics say NULL matches
    // nothing, including another NULL.
    #[test]
    fn null_keys_never_match() {
        let left = exec(
            "l",
            ColumnType::Int32,
            vec![row32(1, None), row32(2, Some(1))],
        );
        let right = exec(
            "r",
            ColumnType::Int32,
            vec![row32(10, None), row32(11, Some(1))],
        );
        let join = MergeJoin::new(left, right, 1, 1).unwrap();
        let rows = drain(join);
        assert_eq!(rows.len(), 1, "only the non-NULL key pair joins");
        assert_eq!(rows[0][0], Value::Int32(2));
        assert_eq!(rows[0][2], Value::Int32(11));
    }

    // HOW: Int64 left keys against Int32 right keys — the canonical
    // comparator must merge Int64(2) with Int32(2) (E11 strategy
    // equivalence), and 5e9 (unrepresentable in i32) must match nothing.
    #[test]
    fn mixed_int32_int64_keys_match_via_compare_sql() {
        let left = exec(
            "l",
            ColumnType::Int64,
            vec![
                row64(1, Some(1)),
                row64(2, Some(2)),
                row64(3, Some(5_000_000_000)),
            ],
        );
        let right = exec(
            "r",
            ColumnType::Int32,
            vec![row32(10, Some(1)), row32(11, Some(2)), row32(12, Some(7))],
        );
        let join = MergeJoin::new(left, right, 1, 1).unwrap();
        let rows = drain(join);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int32(1));
        assert_eq!(rows[1][0], Value::Int32(2));
    }

    // Review fix #2: sortedness is a hard precondition — violating it
    // silently loses matches, so debug builds must crash instead. The
    // violating key must actually be PULLED, hence the preceding
    // matching keys.
    #[test]
    #[should_panic(expected = "not sorted ascending")]
    fn unsorted_right_input_panics_in_debug() {
        let left = exec("l", ColumnType::Int32, vec![row32(1, Some(5))]);
        let right = exec(
            "r",
            ColumnType::Int32,
            vec![row32(10, Some(5)), row32(11, Some(3))],
        );
        let join = MergeJoin::new(left, right, 1, 1).unwrap();
        drain(join);
    }

    #[test]
    #[should_panic(expected = "not sorted ascending")]
    fn unsorted_left_input_panics_in_debug() {
        let left = exec(
            "l",
            ColumnType::Int32,
            vec![row32(1, Some(5)), row32(2, Some(3))],
        );
        let right = exec("r", ColumnType::Int32, vec![row32(10, Some(5))]);
        let join = MergeJoin::new(left, right, 1, 1).unwrap();
        drain(join);
    }

    // Negative space: a key column outside the child schema must fail at
    // construction, never per-tuple.
    #[test]
    fn out_of_range_key_column_errors_at_construction() {
        let result = MergeJoin::new(
            exec("l", ColumnType::Int32, Vec::new()),
            exec("r", ColumnType::Int32, Vec::new()),
            9,
            1,
        );
        assert!(result.is_err());
    }
}
