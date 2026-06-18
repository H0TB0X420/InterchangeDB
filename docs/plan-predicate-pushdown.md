# Plan: Predicate Pushdown & Equi-Join Lowering

**Motivation.** Lever #5 in `docs/scalability-investigation.md`. At `--scale big`
the TPC-C **StockLevel** transaction plans as a **cross-product**:

```sql
SELECT COUNT(DISTINCT s_i_id) FROM order_line, stock
WHERE ol_o_id = $1 AND s_i_id = ol_i_id AND s_quantity < $2
```
```
HashAggregate(COUNT DISTINCT)
  Filter(ol_o_id=$1 AND s_i_id=ol_i_id AND s_quantity<$2)
    NestedLoopJoin[cross]( SeqScan(order_line), SeqScan(stock) )   ← |OL| × |S| tuples
```

Comma joins bind to `on: None` (`src/sql/logical.rs`), so `plan_select`
(`src/sql/planner.rs`) dumps the **entire** `WHERE` as one `Filter` *on top of*
the cross product. It materializes `|order_line| × |stock|` tuples (~32k × 3.2k =
**100M, tens of seconds** at `big`) and keeps a handful.

**Goal.** Make `plan_select` split the conjunctive `WHERE` and route the pieces:
push single-table predicates onto the base scans, promote equi conjuncts to the
join key, and let the join become a HashJoin. For StockLevel this turns the
outer from ~32k rows into ~5 (`ol_o_id = $1` on `order_line`) and replaces the
cross product with a real equi-join — from O(|OL|×|S|) to O(|OL|+|S|).

**Non-goals (V1).** No `LogicalPlan` restructure, no composite (multi-column)
join keys, no outer joins (the DB has only inner/cross joins), no new execution
operators (`HashJoin` already exists in `src/execution/join.rs`).

---

## Where this lives (architecture)

Two layers, in order of value:

1. **Predicate pushdown + join-key promotion** — the real win, **absent today**,
   **planner-agnostic**. Lives as a split step in `plan_select`
   (`src/sql/planner.rs`), shared by both `JoinSelection::Heuristic` and
   `JoinSelection::CostBased` (it runs before/around the `selection` branch).
2. **HashJoin selection** — secondary, mostly built. `CostBased` (Selinger)
   *already* picks `HashJoin` via `choose_join_algorithm`
   (`src/sql/join_order.rs`) once an equi-key is visible — today it can't,
   because the key is buried in the residual `Filter` (`on: None`). Only the
   `Heuristic` arm needs a new rule (equi-`on` + no usable index → `HashJoin`).

**Canonical future home.** Predicate pushdown is a `LogicalPlan → LogicalPlan`
rewrite that belongs *before* physical planning — which needs `LogicalPlan`
restructured into a proper operator tree (Scan / Filter / Join nodes), the
Cascades-era refactor (plan.md V3 Phase 17/18). V1 deliberately does the split
inside `plan_select` (physical) to stay localized; the helpers (`flatten_conjuncts`,
`referenced_columns`, table-range mapping) are written so a later logical pass
can reuse them.

**Correctness invariant (the whole plan rides on this).** For inner/cross joins,
moving a single-table predicate below the join, and turning an equi residual
predicate into the join key, is **semantics-preserving** — the output row set is
identical. Every phase asserts this with row-equivalence tests, not just plan
goldens.

---

## Types this builds on (verified)

- `Predicate::{ Compare { op, left, right }, And(Box, Box), Or(Box, Box) }`
  (`src/sql/expr.rs`).
- `Expression::{ Literal(Value), Column(usize), Parameter(usize) }`. Column
  indices are **global** to the join's running tuple.
- `JoinClause { right_table: String, on: Option<Predicate> }` — comma joins set
  `on: None`.
- `plan_select` column ranges: left table = `[0, L)` (`L =
  left_schema.columns.len()`); join *i*'s right table = `[offset_i, offset_i +
  R_i)`, where `outer_offset` starts at `L` and grows by each right table's
  column count. **A predicate pushed below a join must rebase its column
  indices** by subtracting that table's range start.
- Reusable helpers: `extract_equi_join_keys(pred, outer_offset) -> Option<(outer,
  inner_local)>` (already does the outer/inner range split), `try_lower_pk_lookup`,
  `try_lower_index_predicate` + `IndexLowering`, `try_match_inlj`,
  `choose_join_algorithm`.

---

# Phase A: Conjunct-splitting infrastructure (no behavior change)

**Goal.** Pure helpers + tests; `plan_select` unchanged. Lets every later phase
be a small, reviewable wiring change.

## Task A.1: Flatten conjunctions
- [ ] `fn flatten_conjuncts(pred: Predicate) -> Vec<Predicate>` in `planner.rs`:
  recursively split top-level `Predicate::And(a, b)` into a flat list; `Or` and
  `Compare` are **leaf conjuncts** (never split — `A OR B` is one unit).
- [ ] Unit tests: `A` → `[A]`; `A AND B AND C` (left/right nested) → 3 conjuncts;
  `A AND (B OR C)` → `[A, (B OR C)]`; a bare `Or` stays whole.

## Task A.2: Collect referenced columns
- [ ] `fn referenced_columns(pred: &Predicate, out: &mut BTreeSet<usize>)` —
  walk `Compare`/`And`/`Or`, collecting every `Expression::Column(i)`. (A
  `Parameter` contributes no column; by plan time params are substituted to
  `Literal` anyway.)
- [ ] Unit tests: `col=lit` → {i}; `colA=colB` → {a,b}; nested `And`/`Or` unions.

## Task A.3: Table-range map
- [ ] `struct TableRange { start: usize, end: usize }` + `fn table_ranges(left:
  &Schema, joins: &[JoinClause], catalog) -> Vec<TableRange>` — `[0,L)` then one
  range per right table from the catalog's column counts. (Index 0 = left.)
- [ ] `fn bucket_of(cols: &BTreeSet<usize>, ranges: &[TableRange]) ->
  Bucket` where `Bucket ∈ { SingleTable(table_idx), Spans(set), Empty }`:
  `SingleTable(k)` iff every referenced column falls in range *k*.
- [ ] Unit tests: all-left → `SingleTable(0)`; all-right-of-join-1 →
  `SingleTable(1)`; mixed → `Spans`; no columns (`lit op lit`) → `Empty` (treat
  as left/anywhere — keep as residual or constant-fold later).

**Success Criteria.** Helpers covered by unit tests; full suite still green
(nothing calls them yet).

---

# Phase B: Push single-table predicates onto the LEFT leaf

**Goal.** The dominant StockLevel win — `ol_o_id = $1` lands on `order_line`,
shrinking the outer from ~32k to ~5 rows. Lowest-risk because the left leaf
already has the full PK/index/seqscan lowering; today it's just gated behind
`joins.is_empty()`.

## Task B.1: Reach the left lowering even with joins
- [ ] In `plan_select`, replace the `if joins.is_empty()` gate around the left
  leaf with: `flatten_conjuncts(filter)` → bucket by `table_ranges`. Feed the
  conjuncts in `SingleTable(0)` (left) into the existing lowering chain
  (`try_lower_pk_lookup` → `try_lower_index_predicate` → `SeqScan` + local
  `Filter`). For a multi-predicate left bucket, AND them back into one predicate
  for the leaf `Filter` (or chain `try_lower_*` on the equality conjunct and keep
  the rest as a leaf `Filter`).
- [ ] Everything not in `SingleTable(0)` stays in a `residual: Vec<Predicate>`
  that is re-`And`-ed and wrapped as the top `Filter` after the join loop
  (preserving today's behavior for the not-yet-pushed pieces).
- [ ] NOTE comment: left columns are already local (`[0,L)`), so **no rebasing**
  on the left.

## Task B.2: Goldens + equivalence
- [ ] `tests/explain_goldens.rs`: StockLevel-shaped golden showing `ol_o_id=$1`
  on the left scan (PkLookup/IndexScan/Filter) and the remainder still on top.
  Add a 2-table golden where a left-only predicate is pushed and a cross-table
  predicate is not.
- [ ] Row-equivalence: a `tests/` case that runs a join+filter query and asserts
  the result set is unchanged vs the pre-B plan (the invariant). Reuse the
  sql_e2e harness; both planners must agree.

**Success Criteria.** Left single-table predicates are pushed; StockLevel's outer
is filtered before the join; all existing tests green (join+filter goldens that
change are updated intentionally).

---

# Phase C: Join-key promotion + right-side pushdown

**Goal.** Replace the cross product with a real equi-join and filter the inner.

## Task C.1: Promote equi conjuncts to the join `on`
- [ ] In the join loop, before choosing the algorithm for join *i*: scan the
  residual conjuncts for one where `extract_equi_join_keys(conjunct,
  outer_offset)` is `Some` **and** its inner column lands in join *i*'s range.
  If the original `join.on` is `None`, set `on := Some(that conjunct)` and
  **remove it from the residual**. (If `join.on` was already `Some`, leave it —
  explicit `ON` wins; the conjunct stays residual.)
- [ ] Single key only: if several equi conjuncts connect the outer to join *i*,
  promote the first; the rest stay residual (composite keys are a documented
  non-goal). Add a `NOTE:` per CLAUDE.md.

## Task C.2: Push right-table single-table predicates onto the inner
- [ ] For join *i*'s right leaf (the `SeqScan { table }` inside NLJ/HashJoin —
  and the INLJ inner where applicable): take `SingleTable(i)` conjuncts,
  **rebase** their columns (`global − offset_i`), and lower them against the
  right table (`try_lower_pk_lookup` / `try_lower_index_predicate` / `SeqScan` +
  local `Filter`). Remove them from the residual.
- [ ] Rebasing is the off-by-one risk site — assert `start ≤ col < end` before
  subtracting; a stray index is a planner bug → crash (debug_assert).

## Task C.3: Goldens + equivalence
- [ ] Goldens: StockLevel now `HashJoin/NLJ[on s_i_id=ol_i_id]` with
  `s_quantity<$2` pushed onto the stock scan and `ol_o_id=$1` on order_line —
  **no top Filter** (all conjuncts placed). A same-table equality (`a.x=a.y`)
  golden proving it pushes as a *filter*, not a join key. An un-pushable
  cross-table non-equi (`a.x+b.y>5`) golden proving it *stays* on top.
- [ ] Row-equivalence across both planners; full suite green.

**Success Criteria.** StockLevel plans as a filtered equi-join with no
cross-product; results identical; residual holds only genuinely un-pushable
conjuncts.

---

# Phase D: Heuristic HashJoin rule

**Goal.** Make the rule-based planner (the harness default) pick HashJoin once an
equi-key exists — `CostBased` already does.

## Task D.1: Equi-`on` → HashJoin in the heuristic
- [ ] In `JoinSelection::Heuristic`: after the INLJ check fails
  (`try_match_inlj` is `None`), if `extract_equi_join_keys(on, outer_offset)` is
  `Some`, emit `PhysOp::HashJoin { outer, inner: SeqScan(right), outer_key_col,
  inner_key_col }` instead of `NestedLoopJoin`. Non-equi / cross (`on: None`
  after C found no key) still falls to `NestedLoopJoin`.
- [ ] Keep textual join order (no reordering — matches `CostBased`'s invariant).

## Task D.2: Goldens + equivalence
- [ ] Golden: StockLevel under `--planner rule-based` emits `HashJoin`.
- [ ] Equivalence: Heuristic vs Selinger produce identical rows for the
  StockLevel shape and a 3-table chain (extend the push-equivalence pattern).

**Success Criteria.** Both planners produce a HashJoin for equi-joins with no
usable inner index; identical results; full suite green.

---

# Phase E: Measure + document

**Goal.** Confirm the blowup is gone, to the doc's drift-controlled standard.

## Task E.1: A/B
- [ ] Drift-controlled A/B (HEAD-vs-feature, back-to-back, same box): a
  StockLevel micro-measurement (single-txn latency at `--scale big`) should drop
  from **seconds → ~ms**; the `--scale big` full mix should become runnable at
  16×16 / 32×32 (no minute-long deadline overshoot). Record reps + fsync/commit.
- [ ] Sanity: `--scale smoke` full-mix tpmC unchanged (StockLevel is ~4% there
  and tables are tiny — no regression expected).

## Task E.2: Document
- [ ] `docs/scalability-investigation.md`: move lever #5 to **landed**, with the
  before/after StockLevel latency and the plan shape.
- [ ] Update the memory benchmark note.

---

## Risks & edge cases

- **Column rebasing off-by-one** (C.2) — the highest-risk site. Guarded by
  `debug_assert!(start <= col && col < end)` + exhaustive goldens (push-to-left,
  push-to-right, mixed, residual-stays).
- **Equi between same-table columns** (`a.x = a.y`) — must bucket as a
  single-table *filter*, not a join key. `extract_equi_join_keys` already
  returns `None` when both columns share a range, so it naturally falls to the
  single-table bucket. Covered by a golden.
- **`OR` / non-conjunctive WHERE** — never split; the whole `Or` is one residual
  conjunct. Covered in A.1.
- **Explicit `ON` vs promoted key** — explicit `JoinClause.on` wins; promotion
  only fills `on: None` (comma joins). Avoids double-applying a condition.
- **Composite / multi-key joins** — only the first equi-key is promoted; the
  rest stay residual `Filter` (a documented non-goal; correct, just not optimal).
- **Outer joins** — none exist in this DB; pushdown rules differ for them (can't
  push predicates through the null-producing side). If outer joins are ever
  added, the bucketing must gate on join type. Note in the code.
- **Constant-only conjuncts** (`Empty` bucket, `lit op lit`) — keep as residual
  (or constant-fold in a later pass); don't misattribute to a table.

## Success criteria (whole plan)

1. StockLevel plans as a **filtered equi-join**, no cross product, under **both**
   planners.
2. **Row-equivalence**: every join+filter query returns identical results
   before/after (semantics-preserving).
3. `--scale big` StockLevel latency **seconds → ms**; full mix runnable at high
   concurrency.
4. `--scale smoke` tpmC unregressed.
5. Full suite green; new goldens cover push-left / push-right / promote-key /
   residual-stays / same-table-equi / OR-whole.
