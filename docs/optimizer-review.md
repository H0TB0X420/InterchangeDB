# Optimizer Review — Findings Log

A running log from the interactive walkthrough of `src/sql/` (the query optimizer:
parse → bind → logical plan → cost-based physical plan), checking each component for
**correctness** and **faithfulness to the source papers** — principally
**Selinger et al. 1979** ("Access Path Selection in a Relational Database Management
System": dynamic-programming join ordering, the cost model, and selectivity factors).

Companion to `docs/exec-optimizer-review.md` (the executor pass, findings `E*`). This
file uses `O*` finding IDs so the two never collide. Report-only — nothing here has been
changed; findings are for the user to triage later.

## Legend

- **Priority:** P1 correctness bug · P2 correctness-relevant / depends-on-assumption ·
  P3 simplification / dead code · P4 perf / fidelity gap (documented) · P5 nit.
- **Confidence:** how sure the finding is real (high / medium / low).
- **Status:** open · verifying · confirmed · dismissed · fixed.

## Cross-references into the executor pass (`E*`)

- **E7** — Filter's NULL / three-valued-logic correctness was deferred to this pass; it is
  resolved in Unit 1 (`expr.rs`, `Predicate::compile` / `eval_compare`).
- **E1 / E11** — the planner's *dropped-predicate* decisions (single-col-eq → IndexScan;
  equi-join → HashJoin/INLJ) originate in `planner.rs`; revisited in the planner unit.
- **E12** — cost model assumes HashJoin builds on the smaller side; executor always builds
  on `inner`. Revisited in the cost-model unit.

---

## Optimizer findings (`sql/`)

### O1 — Predicate evaluation collapses SQL 3VL to 2VL; wrong under `NOT` (resolves E7)
- **Where:** `sql/expr.rs:268-271` (`eval_compare` returns `false` for any NULL operand) +
  `expr.rs:183-186` (`Not` compiles to boolean `!`).
- **Priority:** P2 · **Confidence:** high · **Status:** FIXED (2026-07-01) —
  `Predicate::compile_3vl` (Kleene, `Option<bool>`); collapse to bool only at the WHERE
  boundary. Tests: `not_over_null_comparison_drops_row`, `kleene_and_or_with_unknown`.
- **Detail:** SQL is three-valued; a NULL-operand comparison is UNKNOWN, and `WHERE` keeps
  only TRUE. Collapsing UNKNOWN→FALSE at the leaf is correct for NOT-free (monotone)
  predicates (UNKNOWN and FALSE both drop the row under AND/OR), which is why the suite is
  green. But `NOT` inverts it: `WHERE NOT (x = 5)` with `x` NULL → SQL drops the row
  (`NOT UNKNOWN = UNKNOWN`), this impl **keeps** it (`!false = true`). Also affects
  `NOT (… AND/OR …)` over NULLs (De Morgan). Fix: evaluate predicates in real 3VL
  (`Option<bool>` / True-False-Unknown), keep only `True`.

### O2 — `eval_compare` type matrix incomplete → silent empty results
- **Where:** `sql/expr.rs:286-301` (ordering arm falls to `_ => return false`).
- **Priority:** P2 · **Confidence:** high (mechanism + reachability) · **Status:** FIXED
  (2026-07-01) — ordering matrix completed via `Value::compare_sql` (Int↔Decimal,
  cross-scale Decimal exact in i128, Timestamp/Char/Bytes; Char/Varchar unified as one
  string domain). Remaining un-coerced shape: string literals vs Timestamp columns
  (no timestamp literal parser exists anywhere yet — INSERT can't bind them either).
- **Detail:** Ordering (`<,<=,>,>=`) only covers Int32/Int64/Varchar/Boolean/same-scale
  Decimal; everything else returns `false` — i.e. matches **zero rows**, silently. Two live
  gaps: (a) **Int↔Decimal comparisons** return false (`WHERE c_balance > 0` with a Decimal
  column vs an Int64 literal → no rows) — inconsistent with `eval_binary_op`, which *does*
  promote Int↔Decimal; (b) **Char/Bytes/Timestamp ordering** returns false (`WHERE ts >
  '2020-01-01'` → no rows, though Timestamp is an orderable i64). Equality works for all
  types; only ordering is missing. **Reachability confirmed (Unit 5b):** the binder binds
  WHERE literals *untyped* (int→Int64, string→Varchar, fractional→Decimal-by-text-scale) and
  does **no** comparison type-check/coercion, so `WHERE decimal_col > 5`, `WHERE ts > 'str'`,
  and even `WHERE price > 9.99` on a `DECIMAL(_,4)` column (scale mismatch) all silently
  return no rows. This is also the canonical 4th copy of the comparator (see E15) — any
  consolidation must fold in O1 + O2. Fix belongs in the binder (see O13) or `eval_compare`.

### O13 — Binder does name resolution but not expression/predicate type-checking (root of O2)
- **Where:** `sql/binder.rs:1088-1096` (`bind_predicate`, no type reconciliation) +
  `binder.rs:1046` (`ast_value_to_value_unconstrained`, untyped literals). Contrast the
  existing coercion at `binder.rs:1194` (`narrow_value`, used only for UPDATE SET) and the
  typed INSERT path (`binder.rs:881`).
- **Priority:** P2 · **Confidence:** high · **Status:** FIXED (2026-07-01) —
  `narrow_compare_operands` in `bind_predicate` (via the new `Value::coerce_exact`),
  PLUS the same coercion in the planner's index/PK lowering — prepared statements
  substitute parameters after binding, so the planner layer is load-bearing for them.
  Tests: `tests/binder_predicate_coercion_test.rs` (verified red pre-fix — the Int64
  literal vs Int32-indexed column case actually ERRORED at key encoding).
- **Detail:** The binder resolves names/types but defers all operand type reconciliation to
  the runtime `eval_*` functions, which silently degrade (O2 returns no-rows, O3 wraps). The
  coercion primitive already exists (`narrow_value` for SET) and INSERT already binds typed —
  applying the same coercion of the literal side to the column type inside `bind_predicate`
  would close O2 at the source and turn silent-wrong into correct (or a clean bind-time error).

### O3 — Arithmetic uses wrapping ops (silent overflow); div-by-zero → NULL
- **Where:** `sql/expr.rs:199-222` (`wrapping_add/sub/mul`), `203-208` (div-by-zero → Null).
- **Priority:** P3 · **Confidence:** high · **Status:** FIXED (2026-07-01) — `checked_*`
  with overflow → NULL (consistent with the module's Decimal-overflow/div-zero → NULL
  semantic; closures are infallible). NOTE'd deviation from SQL's raise-an-error.
  `checked_div` also covers `MIN / -1`.
- **Detail:** Same silent-overflow theme as E13. Int32/Int64 arithmetic wraps rather than
  erroring/promoting. Division by zero yields `Value::Null`; SQL standard raises an error
  (NULL is defensible but non-standard).

### O4 — Decimal division is integer-truncating, not SQL decimal division
- **Where:** `sql/expr.rs:258` (`div_keeping_scale`).
- **Priority:** P3 · **Confidence:** high · **Status:** FIXED (2026-07-18, TPC-H H3.1) —
  `Decimal::div` now performs true rounded division (round half away from zero) at result
  scale `max(s1, s2)` (`10.00 / 4.00 = 2.50`); the truncating `div_keeping_scale` is gone.
- **Detail:** `Decimal / Decimal` divides raw mantissas with integer truncation and keeps the
  dividend's scale → `10.00 / 4.00 = 0.02`, not `2.50`. A test NOTE flags this. Latent
  (TPC-C doesn't divide decimals) but a real semantic gap; AVG-style division needs true
  decimal division. Related: `Int + Decimal` additively → NULL by design (non-standard).

### O5 — Join predicates hit the `col = col` fallback (0.1), not `join_selectivity` (verify routing)
- **Where:** `sql/selectivity.rs:109-113` (`estimate_compare` returns `EQ_FALLBACK` when both
  sides are columns) vs `join_selectivity` (separate fn, `1/max_ndv`).
- **Priority:** P4 · **Confidence:** medium · **Status:** verify in DP/planner units
- **Detail:** `estimate_predicate_selectivity` returns `0.1` for a `col = col` predicate;
  proper join selectivity is only produced by the separate `join_selectivity`. Correctness
  depends on the planner routing join edges to `join_selectivity` and only single-table
  `WHERE` predicates to `estimate_predicate_selectivity`. A safety-net fallback, not a bug —
  confirm the routing when reviewing `join_order.rs`/`planner.rs`.

### O6 — LIMIT cost ignores early termination
- **Where:** `sql/cost.rs:190-198` (`cost_limit` charges `output_rows`, no child discount).
- **Priority:** P4 · **Confidence:** high · **Status:** open (fidelity, dormant)
- **Detail:** `cost_limit` doesn't discount the child's cost for stopping early. Accurate to
  the current eager Volcano executor (E3 means `Limit` doesn't save scan work), so not wrong
  today — but will over-cost limited plans once streaming scans land. Pair with the E3/
  streaming work.

### O7 — `groups` param unused in `cost_hash_aggregate`
- **Where:** `sql/cost.rs:209` (`_groups`).
- **Priority:** P5 · **Confidence:** high · **Status:** open
- **Detail:** The default model drives aggregate cost off `input_rows` only; `groups` is kept
  in the trait for a future group-aware model. Harmless interface/impl slack.

### Cross-ref — E12 (HashJoin build side) resolved as dormant
- `cost_hash_join = build + probe` is symmetric, so the planner's `min/max` build assignment
  is cost-neutral (dead computation) and the executor's build-on-`inner` has no cost effect in
  the memory-free model. Becomes real only with Phase-20 spill costing. See E12 in
  `docs/exec-optimizer-review.md`.

### O8 — Interesting orders deferred (Selinger's signature idea) — justified
- **Where:** `sql/join_order.rs:37-45` (doc), whole-file (keeps cheapest-per-subset only).
- **Priority:** P4 · **Confidence:** high · **Status:** open (deliberate, correct deferral)
- **Detail:** Selinger keeps the cheapest plan *per interesting sort order* to avoid redundant
  sorts (merge join, ORDER BY). This impl keeps cheapest-per-subset only. Deferred because no
  operator delivers/exploits order yet (no merge join) — and while that's true,
  cheapest-per-subset is *provably optimal*, so the deferral loses nothing today. The hook
  lands with merge join (Phase 17). Recorded as the one conscious divergence from the paper.

### O9 — DP leaf is always a seq scan; access-path & join-order selection decoupled
- **Where:** `sql/join_order.rs:262-282` (`sub_leaf` always `cost_seq_scan`).
- **Priority:** P4 · **Confidence:** medium · **Status:** open (verify planner lowering in Unit 5)
- **Detail:** Selinger enumerated per-relation access paths *inside* the DP. Here `sub_leaf`
  always costs a seq scan (+filter), never an IndexScan/PkLookup for a selective local
  predicate; the planner lowers leaves to index access *after* the join order is fixed. So a
  leaf that executes as a cheap index point-lookup is costed as a full scan during ordering →
  possible suboptimal order. Usually minor (join costs dominate; local_selectivity already
  shrinks downstream cardinalities). Confirm the post-DP lowering in the planner unit.

### E12 — confirmed at a second site (join_order.rs)
- `best_algorithm` (`join_order.rs:341-346`) also computes `(build,probe)=min/max` that the
  symmetric `cost_hash_join` ignores — duplicate dead computation, same dormant status.

### O10 — Selinger DP ignores single-table WHERE selectivity (`local_selectivity ≡ 1.0`)
- **Where:** `sql/selinger.rs:259` (`build_join_graph` sets `local_selectivity: 1.0`, the only
  production value anywhere) → consumed but never fed at `join_order.rs:272, 461`.
- **Priority:** P2 · **Confidence:** high · **Status:** FIXED (2026-07-01) —
  `apply_local_selectivities` in `selinger.rs` routes single-table WHERE conjuncts into
  `JoinRelation::local_selectivity` exactly as prescribed below; WHERE-driven reorder
  test added (red without the wiring). Was the **headline optimizer finding**.
- **Detail:** The DP is fully wired to use per-table selectivity (`sub_leaf` computes
  `out_card = raw_rows * local_selectivity` + a filter charge), and a correct, tested
  `estimate_predicate_selectivity` exists (Unit 2) — but the driver never calls it. So
  cost-based join **ordering is cardinality-blind to WHERE filters**: `SELECT … FROM big JOIN
  small WHERE big.x = 5` costs `big` at full size and may order it last when the filter makes
  it tiny. The whole selectivity/histogram apparatus is unused for ordering. Pushdown still
  filters correctly at lowering time (results right), but the order is chosen blind. Fix is
  local: in `build_join_graph`, split the WHERE with the existing
  `flatten_conjuncts`/`referenced_columns`, route single-table conjuncts to their relation, and
  fill `local_selectivity` via `estimate_predicate_selectivity`. The reorder tests only
  exercise base-size-driven reordering, so this gap is untested — add a WHERE-driven reorder
  test.

### O12 — Index-driven UPDATE/DELETE drop the predicate: mutating wrong rows (E1 amplified)
- **Where:** `sql/planner.rs:1010-1017` (`plan_update`), `1067-1074` (`plan_delete`) —
  `IndexLowering::Matched` sets `residual_filter = None`, no recheck Filter.
- **Priority:** P2 · **Confidence:** high (mechanism) / medium (triggerability) ·
  **Status:** FIXED (2026-07-01) — `plan_update`/`plan_delete` re-apply the predicate as
  a recheck Filter above the IndexScan. Wrong-row-mutation scenario proven and pinned by
  `snapshot_update_does_not_mutate_wrong_row_via_stale_index_entry`.
- **Detail:** `UPDATE/DELETE … WHERE indexed_col = v` lowers to `Update/Delete(IndexScan)`
  with the predicate dropped and no recheck. Same stale-index exposure as **E1** (concurrent
  MVCC, if the index engine isn't snapshot-versioned), but the consequence is
  **mutating/deleting a row that doesn't match the predicate** — data corruption, not just a
  bad read. Concentrates on the exact path E17 (Halloween) flags. PkLookup lowering is safe to
  drop (single-col PK is exact/unique); the concern is the non-unique **IndexScan** lowering.
  Raises the priority of resolving E1's index-engine-versioning question — it decides whether
  this is latent or live.

### O5 — RESOLVED: join predicates correctly routed to `join_selectivity`
- Confirmed in Unit 5a: the cost-based join loop uses `join_selectivity(0, inner_ndv)`
  (`planner.rs:423`), not `estimate_predicate_selectivity`. Sub-note: outer NDV is unavailable
  (P14.12), so it uses `1/inner_ndv` only (partial containment estimate) — documented.

### O9 — CONFIRMED: leaf lowered to PkLookup/IndexScan after the DP costed a seq scan
- `build_left_leaf` / no-join path lower leaves post-DP (`planner.rs:869-885`), so join order is
  chosen on seq-scan leaf costs that don't match the executed access path. Usually minor.

### Checked & cleared (Unit 4b)
- **Reorder/remap coverage:** `selinger.rs`'s only *unit* test exercises the no-reorder
  passthrough, but `tests/selinger_reorder_test.rs` covers the reorder path (results match
  rule-based → `ColumnRemap` correct; reordering happens; output column order preserved). No
  finding.

### Positive note — stats.rs error discipline is the model the executor should follow
- `stats.rs` correctly separates **corruption (`Err` → propagate/crash)** from **missing
  data (`Ok(None)` → fall back to defaults)** — exactly the distinction the executor blurs in
  E9/E13 (silent degradation of corruption). Cited as the reference for those fixes.

---

## Scope notes (not bugs — known gaps / deferred work)

- **O-S1 — Independence assumption** for AND/OR selectivity (`s(a)*s(b)` / inclusion-
  exclusion). Wrong under correlation (TPC-H `nation`/`region`); Selinger's own first-cut,
  documented, deferred to Phase 16 tuning.
- **O-S2 — NDV not propagated through joins.** Multi-join estimates reuse base-column NDVs
  at every level (Selinger's original simplification), documented (`selectivity.rs:86`).
- **O-S3 — Histograms are INT-only** (equi-width). Decimal/Timestamp/Varchar range
  predicates fall back to `RANGE_FALLBACK = 1/3`. Degrades gracefully (unlike the evaluation
  path, O2, which returns false/no-rows for the same types).
- **O-S4 — `DEFAULT_ROW_COUNT = 1000`** for unanalyzed tables — a heuristic so the DP
  doesn't treat an un-`ANALYZE`d table as free. Standard "run ANALYZE" caveat.
- **O-S5 — Binder surface gaps** (documented via clean bind-time errors): no `IS NULL` /
  `IS NOT NULL`, no `BETWEEN` / `IN` / `LIKE`, anonymous `?` placeholder deferred (only `$N`).
  Combined with O1, NULL handling in `WHERE` is limited.
