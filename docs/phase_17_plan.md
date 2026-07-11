# Phase 17 — Volcano-Memo Planner: Execution Plan

Status: PLAN (no code). Written to be executed without further design
decisions — every open question is decided here, with rationale. Refer to
plan.md §Phase 17 (line ~2603) for the charter; this document supersedes
it on any detail conflict.

**Prime references:** Graefe & McKenna, *The Volcano Optimizer Generator*
(1993). CMU `optd` (https://github.com/cmu-db/optd) is recommended
pre-reading for memo internals but is NOT a dependency — every structure
this plan needs is specified below. If web access is unavailable, skip
the reading and build from this spec.

---

## 0. Decisions (settled — do not reopen)

| # | Decision | Rationale |
|---|---|---|
| D1 | Phase splits into **17-A (memo at parity)** then **17-B (order properties + merge join)**. 17-A lands and gates before any 17-B code. | Highest-variance phase in V3; the subtle machinery (memo, search) lands against a fixed behavioral target before new operators enter. |
| D2 | Memo groups are **keyed by relation-set bitmask** (`RelSet = u32`, same convention as `sql/join_order.rs`). One group per connected relation subset; logical expressions within a group are subset **splits**. | For the equi-join-graph fragment this IS the closure of join commutativity + associativity — split enumeration realizes the "fixed rule set" exactly, and dedup becomes a `HashMap<RelSet, GroupId>` lookup instead of expression-tree hashing (the classic memo failure mode). Extensible rule machinery is Phase 18's job by charter. |
| D3 | The memo optimizes the **join/filter/access-path core only**. The statement spine — residual cross-table filters, aggregates, ORDER BY, projection, LIMIT, and all non-SELECT statements — remains fixed logic applied above the memo's output, factored into a shared helper (T17-A.6). | Matches Selinger's scope; keeps the parity gate meaningful. Aggregate/limit pushdown are explicit non-goals. |
| D4 | **Bushy joins allowed** (both sides of a split may be multi-relation). | It is the memo's natural space; strictly a superset of Selinger's left-deep space, so the cost-dominance gate (G3) stays provable. |
| D5 | Logical `Join` stores its split **unordered-canonical** (left = the side containing the smallest relation index). Physical implementation rules generate **both orientations** (either side as outer). | Halves logical expression count; physical asymmetry (build/probe, driving side) is where orientation actually matters. |
| D6 | Leaf access paths are chosen **inside the memo** via the existing `try_lower_pk_lookup` / `try_lower_index_predicate` (`src/sql/planner.rs`, make `pub(crate)` as needed). | Fixes the O9 gap (Selinger costs leaves as seq scans) for this planner, AND inherits the E1/O12 recheck + `coerce_exact` semantics verbatim. Re-implementing leaf lowering is forbidden — it would re-open the index-recheck bug class. |
| D7 | Group **cardinality is a group property**, computed once per group from its relation set: `Π row_count(rel_i) × Π local_sel(rel_i) × Π edge_sel(internal edges)` — independence assumption, same formulas/functions as `join_order.rs` (`estimate_predicate_selectivity`, `join_selectivity`). Never computed per-split. | Split-invariant cardinalities prevent the search from "shopping" for optimistic estimates and make winner comparison sound. |
| D8 | Unsupported shapes (any join without a single-`Column = Column` equi ON after promotion, > `MAX_RELATIONS`, non-SELECT statements) **fall back** to `plan_inner(logical, catalog, &JoinSelection::CostBased{…})` — byte-identical to `SelingerPlanner`'s bail path. | Total coverage; the memo never errors on valid SQL it doesn't handle. |
| D9 | 17-B's first order consumer is **merge join** (not ordered index scans). Index-order delivery is a recorded non-goal. | Merge join is the classic enforcer/property exercise and feeds Phases 18/20. Index-order reasoning (prefix→key-order) is a separable later step. |
| D10 | `BaselineCapable` is a **marker trait with no methods**, implemented by `VolcanoPlanner`, plus a determinism test (same input ⇒ identical EXPLAIN twice). | Phase 19 needs "this planner's plans are reproducible baselines"; determinism is the actual property, the marker is the gate. |
| D11 | Planning-time guard: memo `plan()` on TPC-C-shaped queries must stay under a generous absolute bound (test at 50ms debug-build per statement). | The memo must never make OLTP planning pathological; catches accidental exponential blowup. |

**Non-goals (do not build):** Cascades task stack / rule trait (18);
aggregate, projection, or LIMIT pushdown; ordered-index-scan delivery;
plan caching for prepared statements; TPC-H-shaped rules (20/21); any
change to executor operators other than adding `MergeJoin` (17-B).

---

## 1. Existing code you build on (read these first)

| What | Where | Why it matters |
|---|---|---|
| `PlannerStrategy` trait, `Planner` enum, `plan_inner`, `JoinSelection` | `src/sql/planner.rs` (~80–170) | The socket. Add `Planner::VolcanoMemo(VolcanoPlanner)` and match arms in `Planner::{plan,name}`. |
| Leaf lowering + recheck semantics | `src/sql/planner.rs`: `try_lower_pk_lookup`, `try_lower_index_predicate` (returns `IndexLowering::Matched { handle, prefix, recheck }`) | D6. The recheck Filter above IndexScan is a correctness invariant (unversioned indexes; see `docs/plan-versioned-indexes.md` §1). |
| Spine construction | `src/sql/planner.rs::plan_select` tail (~line 481–516: residual Filter → HashAggregate → Sort → Projection → Limit) | Factor into `pub(crate) fn apply_select_spine(...)` (T17-A.6); both `plan_select` and the memo emission call it. |
| Conjunct routing helpers | `src/sql/planner.rs`: `flatten_conjuncts`, `referenced_columns`, `rebase_predicate` (already `pub(crate)`) | Normalization (T17-A.1) routes WHERE conjuncts exactly as `selinger.rs::apply_local_selectivities` does. |
| Join-graph extraction | `src/sql/selinger.rs::build_join_graph` (+`decompose`, `has_single_col_index`) | The memo's normalizer is a sibling of this function; reuse its edge-extraction logic (factor shared parts rather than copying). |
| Column remapping | `src/sql/column_map.rs::ColumnRemap` | PhysOp emission converts textual-coordinate predicates to the chosen physical layout, exactly as `selinger.rs::rewrite_select` does. |
| Cost + stats + selectivity | `src/sql/cost.rs` (`CostModel`, `DefaultCostModel`), `src/sql/stats.rs` (`QueryStats::gather`), `src/sql/selectivity.rs` (`estimate_predicate_selectivity`, `join_selectivity`), `src/sql/join_order.rs` (`ROWS_PER_PAGE_ESTIMATE`, `MIN_CARD`, `MAX_RELATIONS`, `JoinEdge`) | D7. Use the same constants and formulas so cost comparisons across planners are apples-to-apples. |
| Output IR | `src/sql/physical.rs::PhysOp` | The memo emits `PhysOp` directly (unlike Selinger, which rewrites `LogicalPlan`), because it chooses access paths and join algorithms, not just order. |
| Test scaffolding | `tests/selinger_reorder_test.rs`, `tests/join_strategy_equivalence_test.rs`, `tests/planner_index_scan_test.rs`, `tests/sql_e2e_test.rs`, `tests/explain_goldens.rs` | Gates G1–G5 extend these patterns. |

---

## 2. Module layout (new code)

```
src/sql/memo/
  mod.rs        — module docs, re-exports, VolcanoPlanner (PlannerStrategy impl)
  normalize.rs  — LogicalPlan::Select → NormalizedQuery (core + spine parts)
  memo.rs       — Memo, Group, LogicalSplit, PhysicalCandidate, winners
  search.rs     — top-down optimize() with branch-and-bound
  emit.rs       — winner tree → PhysOp (leaf lowering, ON rebuild, ColumnRemap, spine)
  props.rs      — 17-B ONLY: OrderProp, required/derived, enforcer logic
```
Register `pub mod memo;` in `src/sql/mod.rs`; re-export `VolcanoPlanner`.
Estimated sizes: normalize ~150 lines, memo ~200, search ~180, emit ~250,
mod ~80, props ~150. Unit tests inline per file; integration tests below.

---

## 3. Core data structures (17-A)

Textual coordinates: relations are indexed `0..n` in textual order (FROM
table = 0, `joins[i].right_table` = i+1), identical to `selinger.rs`.
`RelSet = u32` bitmask over those indices. A `(rel, local_col)` pair is a
**stable column reference** independent of join order.

```rust
/// Output of normalization: the optimizable core + the untouched spine.
pub(crate) struct NormalizedQuery {
    /// Per-relation: name, alias, schema, local predicate (rebased to
    /// local columns; conjunction of the single-table WHERE conjuncts),
    /// local selectivity (D7 inputs), index handles.
    relations: Vec<RelInfo>,
    /// Equi-join edges in textual coordinates. Same extraction rules as
    /// build_join_graph: explicit ONs plus promoted WHERE equi-conjuncts
    /// (the Phase C.1 promotion in plan_select — replicate its rule:
    /// promote only when the join has no explicit ON).
    edges: Vec<Edge>,           // { left_rel, left_col, right_rel, right_col, both sides' indexed flags }
    /// Cross-table WHERE conjuncts that are not equi-edges (stay in the
    /// spine as a residual Filter, in textual-global coordinates).
    residual: Vec<Predicate>,
    /// Spine: aggregates, order_by, projection, limit — untouched.
    spine: SpineParts,
}

pub(crate) struct Memo {
    groups: Vec<Group>,                 // GroupId = index
    by_relset: HashMap<RelSet, GroupId>,
}

pub(crate) struct Group {
    relset: RelSet,
    cardinality: f64,                   // D7: computed at group creation
    /// Logical alternatives. Leaf groups (single bit) have none; join
    /// groups hold canonical splits (D5).
    splits: Vec<(RelSet, RelSet)>,      // (left, right), left contains min bit
    /// 17-A: single winner. 17-B: HashMap<OrderKey, Winner>.
    winner: Option<Winner>,
}

pub(crate) struct Winner {
    cost: Cost,                         // sql/cost.rs Cost
    plan: PhysChoice,
    /// True when computed with an infinite limit — safe to reuse for any
    /// caller; false means "best found under some budget" and must be
    /// recomputed if a caller's limit exceeds the recorded search bound.
    optimal: bool,
}

/// Physical choice tree — small, memo-internal; emit.rs converts to PhysOp.
pub(crate) enum PhysChoice {
    // Leaves (built via the D6 shared lowering; recheck data captured):
    SeqScan { rel: RelId },                                  // + local filter in emit
    IndexScan { rel: RelId, index: String, prefix: Vec<Value>, recheck: Predicate },
    PkLookup { rel: RelId, pk: Vec<Value>, residual: Option<Predicate> },
    // Joins (orientation explicit; children are groups):
    NestedLoop { outer: GroupId, inner: GroupId, edges: Vec<usize> },
    Hash       { outer: GroupId, inner: GroupId, edge: usize },
    IndexNested{ outer: GroupId, inner_rel: RelId, index: String, edge: usize },
    // 17-B adds: Merge { left: GroupId, right: GroupId, edge: usize },
    //            SortEnforcer { child: GroupId, key: OrderKey } — see §7.
}
```

Group construction: insert leaf groups for each relation, then create
join groups lazily during search (`group_for(relset)`), enumerating
splits at creation: all `(l, r)` with `l ∪ r = relset`, `l ∩ r = ∅`,
both sides **connected** via `edges` and connected to each other (at
least one crossing edge). Connectivity check mirrors
`join_order.rs::connecting_edges`. Splits where no crossing edge exists
are discarded (cross joins unsupported → normalization already bailed,
D8). Canonicalize per D5. Split count per group is `O(2^k)` for k
relations in the set — bounded by `MAX_RELATIONS` (reuse the constant;
bail at normalization if exceeded, D8).

---

## 4. Search (17-A)

```text
optimize(memo, gid, limit) -> Option<Winner>
  g = memo[gid]
  if g.winner is Some(w):
      if w.optimal or w.cost <= limit: return Some(w) if w.cost <= limit else None
      // else: cached under a tighter bound that pruned; re-search below
  best = None
  for candidate in physical_candidates(g):          // §5
      // candidate local cost uses g.cardinality + children groups' cards
      running = candidate.local_cost
      if scalar(running) > scalar(limit): continue   // prune before recursion
      ok = true
      for child_gid in candidate.children, ordered by ascending child card:  // promise, T17-A.5
          child_limit = limit - running              // component-wise via scalar budget
          match optimize(memo, child_gid, child_limit):
              Some(cw) => running += cw.cost
              None     => { ok = false; break }      // pruned
      if ok and running < best: best = candidate with running
  record winner { cost, plan, optimal: limit was INF }
  return best if best.cost <= limit
```

Concretes:
- `limit` is a scalar budget (`cost_model.scalar(Cost)`); root call uses
  `f64::INFINITY`. Child budget = parent limit − scalar(cost so far).
  Lower bounds for unvisited siblings are 0 in 17-A (documented
  simplification; tightening is a recorded future lever).
- The `optimal` flag prevents the classic B&B-memoization bug: a winner
  found under a finite budget may be re-searched when a later caller
  arrives with a larger budget. Root optimization runs with INF, so in
  practice every group's final winner is optimal; the flag keeps the
  invariant honest and testable.
- Termination/bounds: recursion depth ≤ relation count; group count
  ≤ 2^MAX_RELATIONS but only connected subsets materialize; assert
  `groups.len()` stays under a hard cap (e.g. 4096) as the
  "put a limit on everything" guard.

## 5. Physical candidates (17-A)

For a **leaf group** (single relation `r` with local predicate `p`):
1. `PkLookup` — iff `try_lower_pk_lookup(p, schema)` matches. Residual =
   none (single-conjunct parity with plan_select; if `p` had multiple
   conjuncts, the non-PK rest becomes the leaf's filter — mirror
   `build_left_leaf`'s access/leftover split exactly).
2. `IndexScan { …, recheck }` — iff `try_lower_index_predicate` returns
   `Matched`. The recheck predicate MUST be carried and emitted as a
   Filter above the scan (E1/O12 invariant).
3. `SeqScan` — always available; local predicate becomes a Filter.
Costing: `cost_seq_scan(pages, rows)` (+`cost_filter`), `cost_index_scan
(matching_rows)`, PkLookup ≈ `cost_index_scan(1.0)` — match
`JoinSelection::CostBased`'s treatment where one exists; otherwise use
the cheapest faithful mapping and document it in code.

For a **join group**, per split `(l, r)` and crossing edges `E`:
- `Hash` for each orientation, iff |E| ≥ 1 (use the first edge as the
  hash key, remaining edges become an emitted residual join Filter —
  same single-key limitation as today's HashJoin lowering).
- `IndexNested` iff the inner side is a **leaf** relation with a
  single-column index on its edge column (`has_single_col_index`), per
  orientation where inner is a leaf.
- `NestedLoop` both orientations, always.
Cost formulas: reuse `cost_hash_join`, `cost_index_nested_loop_join`,
`cost_nested_loop_join` with the same argument conventions as
`join_order.rs::best_algorithm` (read it; keep argument-for-argument
parity so G3 compares like against like).

## 6. Emission (17-A)

Input: root winner over the full relset + `NormalizedQuery`.
1. Walk the `PhysChoice` tree; derive the **physical relation order**
   (in-order traversal of leaves) → `ColumnRemap::new(&widths, &order)`.
2. Leaves → `PhysOp::{SeqScan, IndexScan, PkLookup}` with local Filters
   (recheck included) — column indices are relation-local, no remap.
3. Joins → `PhysOp::{HashJoin, NestedLoopJoin, IndexNestedLoopJoin}`,
   rebuilding ON predicates from edges with remapped tuple-global
   indices (mirror `selinger.rs::build_on_predicate`); Hash/INLJ take
   `outer_key_col`/`inner_key_col`/`outer_key_cols` per `PhysOp`'s
   existing field conventions (`src/sql/physical.rs`).
4. Apply the spine via the factored `apply_select_spine`: residual
   Filter (remapped) → aggregates → Sort → Projection → Limit.
Assert at emission: every relation appears exactly once; remap indices
in-bounds (`debug_assert!`, crash-on-corruption doctrine).

---

## 7. 17-B — Order properties, Sort enforcer, MergeJoin

Only start after every 17-A gate is green.

1. **`MergeJoin` operator** (`src/execution/join.rs`): inner equi-join
   over two inputs sorted ascending on their key columns. Algorithm:
   advance the lesser side via `Value::compare_sql` (the canonical
   comparator — REQUIRED, keeps strategy equivalence); on key equality,
   buffer the full equal-key run from the right side, emit the cross
   product with each matching left row; NULL keys never match (skip,
   consistent with Hash/INLJ). Implements `JoinStrategy`
   (`algorithm() = "merge"`). Eager-materialize the right side like
   NestedLoopJoin does if streaming both sides is awkward under the
   current Executor model — correctness first, note the perf deviation.
2. **Cost**: add `fn cost_merge_join(&self, left_rows: f64, right_rows: f64) -> Cost`
   to `CostModel` — linear in inputs. (This re-adds an aggregate-style
   trait method WITH a caller, unlike the deleted `cost_hash_aggregate`.)
3. **Order property**: `OrderKey = Vec<(TextualColRef, OrderDir)>` in
   `props.rs`. Winners become `HashMap<Option<OrderKey>, Winner>` per
   group (`None` = no requirement). `optimize(gid, limit, required)`.
4. **Implementation rule**: Join → `Merge` per orientation, requiring
   `Order([edge col])` from both children. **Enforcer**: any candidate
   may satisfy a required order by wrapping a no-order winner in
   `SortEnforcer` (+`cost_sort`). Delivered orders: `SortEnforcer`
   delivers its key; `Merge` delivers its left input's order; everything
   else delivers `None` (index-order delivery is out of scope, D9).
5. **ORDER BY consumption**: in emission, if the query's ORDER BY maps
   to a single relation's columns and the root winner (searched with
   that requirement) delivers it, skip the spine Sort. This is the
   user-visible payoff and the interesting-orders test target.
6. **PhysOp**: add `PhysOp::MergeJoin { left, right, left_key_col,
   right_key_col }` + `build_executor` arm + push-model bridge (falls to
   the ExecutorSource bridge automatically — verify, don't assume).

---

## 8. Wiring & flags

- `Planner::VolcanoMemo(VolcanoPlanner)` variant + match arms
  (`src/sql/planner.rs:115–147`); `VolcanoPlanner::name() = "volcano-memo"`.
- `src/bin/tpcc.rs::parse_planner`: accept `volcano-memo`; add to the
  sweep matrix (planner axis becomes 3-wide).
- `pub trait BaselineCapable {}` in `src/sql/memo/mod.rs`, implemented
  by `VolcanoPlanner` (D10).

## 9. Task list with gates

**17-A**
- [x] A.1 `normalize.rs` + unit tests (edge extraction parity with
      `build_join_graph` on shared fixtures; bail cases → D8 fallback).
- [x] A.2 `memo.rs`: groups, lazy creation, split enumeration,
      cardinality (D7). Unit tests: split counts on chain/star/clique
      graphs; connectivity filtering; canonical form (D5); group-count cap.
- [x] A.3 `search.rs`: optimize() with B&B + `optimal` flag. Unit tests:
      3-relation chain — winner cost equals brute-force enumeration of
      all bushy plans (write the brute force in the test); pruning
      actually skips (assert via a counting cost model).
- [x] A.4 `emit.rs` + `apply_select_spine` factor-out. **Gate G1**:
      `plan_select` refactor is behavior-neutral — full suite green
      before memo emission lands on top.
- [x] A.5 promise ordering (ascending child cardinality) — measurable in
      the A.3 counting test (fewer optimize() calls than unordered).
- [x] A.6 `VolcanoPlanner` + wiring + fallback (D8). **Gates G2–G5.**
      (All gates green 2026-07-07: G2 `tests/planner_three_way_test.rs`,
      G3 unit test in `memo/mod.rs`, G4/G5 same test file; full suite
      90 suites / 1371 tests / 0 failures; clippy+fmt clean.)

**Gates for 17-A completion:**
- **G2 (results parity)**: new `tests/planner_three_way_test.rs` runs a
  fixed corpus (every SELECT from `sql_e2e_test`, `selinger_reorder_test`
  incl. the WHERE-driven case, the join differential schema) under
  rule-based / selinger / volcano-memo; sorted results identical.
- **G3 (cost dominance)**: for the reorder-test queries, memo winner's
  `scalar(cost)` ≤ Selinger's chosen order costed via `cost_of_order`.
- **G4 (determinism, D10)**: plan the corpus twice; EXPLAIN strings equal.
- **G5 (planning-time, D11)** + clippy/fmt + full suite green.

**17-B**
- [x] B.1 `MergeJoin` operator + inline unit tests (runs of duplicate
      keys both sides; empty inputs; NULL keys; mixed Int32/Int64 keys
      via compare_sql). Extend `tests/join_strategy_equivalence_test.rs`
      to 4 strategies × 2 models (sorted inputs supplied via Sort).
      (NOTE: `PhysOp::MergeJoin` + build arm + push-bridge verification
      landed here rather than §7.6's slot — the 4×2 equivalence test
      drives plans through `ExecModel`, which requires the IR variant.)
- [x] B.2 cost_merge_join + props.rs + winners-per-order + enforcer.
      Unit test: with sorted-input requirement, memo picks
      Sort+Merge vs Hash correctly as relative costs flip (drive with a
      hand-built cost model in the test).
- [x] B.3 ORDER BY consumption + an integration test where EXPLAIN shows
      no Sort node (order delivered by Merge) and results stay correct.
      (`tests/merge_order_consumption_test.rs`: no Sort above the
      MergeJoin — the Sorts below it are the children's enforcers.
      NOTE: emitted only when strictly cheaper than unordered+Sort;
      ties keep the spine Sort so plan shapes stay stable.)
- [x] B.4 re-run ALL 17-A gates + full suite; add merge join to the tpcc
      sweep only if the planner actually selects it on TPC-C shapes
      (likely not — document either way in `docs/tpcc-results.md`).
      (Gates + full suite green 2026-07-07: 91 suites / 1378 tests / 0
      failures, clippy+fmt clean. Merge join provably never selected on
      TPC-C shapes under the default model — documented in
      docs/tpcc-results.md; consumption covered by tests instead.)

## 10. Risks & guards

- **Memo/search subtlety** (the plan.md risk callout): contained by D2
  (no expression hashing), the brute-force-equality test (A.3), and the
  `optimal` flag rule (§4).
- **Coordinate bugs** (textual vs physical columns): every historical
  reorder bug lived here. `ColumnRemap` is the single conversion point;
  G2's corpus includes the output-column-order test from
  `selinger_reorder_test`.
- **Recheck regression**: D6 makes leaf lowering shared; add one memo
  EXPLAIN test asserting the Filter above IndexScan exists (mirror
  `planner_index_scan_test`).
- **Scope creep**: the non-goals list is binding; anything tempting
  belongs in Phase 18 notes, not code.
