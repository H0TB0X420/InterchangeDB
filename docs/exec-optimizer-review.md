# Executor + Optimizer Review — Findings Log

A running log from the interactive walkthrough of `src/execution/` (executor) and
`src/sql/` (optimizer), checking each component for **correctness** and
**faithfulness to the source papers** (Graefe's Volcano; Neumann 2011 push model;
Selinger et al. 1979 access-path selection).

This file accumulates findings as the review proceeds. Nothing here has been
*changed* — it is report-only, for the user to triage (e.g. add `// CLAUDE:`
comments + `/snipe`) later.

## Legend

- **Priority:** P1 correctness bug · P2 correctness-relevant / depends-on-assumption ·
  P3 simplification / dead code · P4 perf / fidelity gap (documented) · P5 nit.
- **Confidence:** how sure the finding is real (high / medium / low).
- **Status:** open · verifying · confirmed · dismissed · fixed.

---

## Executor (`src/execution/`)

### E1 — IndexScan drops the predicate with no recheck (MVCC false-positive risk)
- **Where:** `sql/planner.rs:693` (`try_lower_index_predicate` → `IndexLowering::Matched`
  consumes/drops the predicate) + `execution/index_scan.rs` (yields the base-table row
  without re-verifying the indexed column still equals the prefix).
- **Priority:** P2 · **Confidence:** medium · **Status:** FIXED (2026-07-01) — verification
  found the exposure LIVE (index engines raw/unversioned; see `review-fix-plan.md` §0).
  Recheck Filter re-applied at all four lowering sites kills every false positive;
  false negatives remain (versioned-index decision pending). Test:
  `tests/index_mvcc_recheck_test.rs`.
- **Detail:** When `col = literal` matches a single-column index, the planner lowers it
  to `IndexScan` and drops the `WHERE` predicate ("IndexScan guarantees no false
  positives for this shape"). IndexScan does not recheck. Sound for single-version /
  serial execution (mutations delete the old index entry, insert the new — confirmed in
  `table/mod.rs`). The exposure is **concurrent MVCC**: a reader at snapshot S can scan
  the index by the *new* value `v'`, deref the PK, and `get_by_pk` returns the
  snapshot-S version with `col = v` → yields a row that violates `WHERE col = v'`
  (false positive); symmetrically a scan by `v` can miss a row still equal to `v` at S
  (false negative). Real systems put a recheck filter above lossy / non-snapshot-
  consistent index scans (cf. Postgres heap recheck). Triggerability depends on whether
  the **index engine itself is MVCC-versioned** (storage-layer question, outside
  exec/sql).
- **Suggested verification:** test — update an indexed column under a concurrent reader,
  then query by both the old and new values via the index; trace whether
  `delete_index_entries` physically removes the old key or writes a versioned tombstone.
- **Amplified on the mutation path (see O12 in `optimizer-review.md`):** `plan_update` /
  `plan_delete` use the same predicate-dropping IndexScan lowering, so a stale entry means
  *mutating/deleting the wrong row*, not just a bad read. This makes resolving the
  index-engine-versioning question the single highest-priority follow-up of the whole review.

### E2 — `PkLookup.done` is redundant state
- **Where:** `execution/pk_lookup.rs:26-51`.
- **Priority:** P3 · **Confidence:** high · **Status:** open
- **Detail:** `next()` guards on `self.done`, but `self.row.take()` already returns
  `None` on every call after the first (the `Option` is empty post-take). The whole body
  reduces to `Ok(self.row.take())`, dropping the `done: bool` field and the early return.
  Only argument to keep it is readability (explicit "exhausted"); against the codebase's
  "if a line isn't earning its keep, delete it" rule. Judgment call.

### E3 — Eager leaf materialization breaks the Volcano pipeline at build time
- **Where:** `execution/seq_scan.rs:43`, `execution/index_scan.rs:67` (both collect the
  full scan into a `Vec` in the constructor).
- **Priority:** P4 · **Confidence:** high · **Status:** open (documented, by design)
- **Detail:** Volcano collapses open/next/close into construct/Drop here. Because
  `build_executor` builds the whole tree before any `next()` is pulled, every `SeqScan` /
  `IndexScan` runs its *entire* scan at build time and buffers all rows — so e.g.
  `SELECT … LIMIT 1` over a huge table still scans it fully, and memory is O(rows).
  Acknowledged in the `NOTE (perf)` comments; root cause is the self-referential-iterator
  problem in Rust. The largest fidelity gap vs. Volcano's pipelined ideal. Bounded to
  OLTP table sizes; the push model + a streaming-scan refactor are the levers.

### E4 — `build_executor` recursion has no explicit depth bound
- **Where:** `execution/build.rs:34`.
- **Priority:** P5 · **Confidence:** high · **Status:** open
- **Detail:** Recurses on plan-tree height with no `MAX_PLAN_DEPTH` guard. CLAUDE.md says
  "put a limit on everything," but plan trees are shallow and built from validated SQL, so
  the risk is theoretical. Noted for completeness.

### E5 — Low assertion density in operator constructors
- **Where:** `execution/seq_scan.rs`, `pk_lookup.rs`, `index_scan.rs` (so far). Also
  `projection.rs` validates `cols` against the *schema* length but never asserts the
  *runtime tuple* has that many values, so `tuple[c]` would panic on a malformed upstream
  tuple instead of a checked error.
- **Priority:** P5 · **Confidence:** medium · **Status:** open (tracking)
- **Detail:** Constructors carry few/no pre/postcondition assertions vs. CLAUDE.md's
  "~2 per function" target. Tracking whether this holds across the remaining operators
  before drawing a conclusion.

### E6 — Projection uses `Error::StorageCorrupted` for a planner/programmer error
- **Where:** `execution/projection.rs:28`.
- **Priority:** P3 · **Confidence:** high · **Status:** open
- **Detail:** An out-of-range projection column index is an internal invariant violation
  (the planner emitted a bad index), not on-disk corruption. The variant name is
  misleading. Per CLAUDE.md ("assertions detect programmer errors / crash on corruption")
  this is arguably a `debug_assert!` / `Error::Internal`-class condition, not a
  `StorageCorrupted` `Result`. Cosmetic but worth aligning with the error taxonomy.

### E7 — Filter NULL / three-valued-logic semantics live in the compiled predicate (verify)
- **Where:** `execution/filter.rs` (delegates) → `sql/expr.rs` `Predicate::compile`.
- **Priority:** P2 · **Confidence:** high · **Status:** RESOLVED → see O1 in
  `docs/optimizer-review.md`
- **Detail:** Filter's predicate is `Fn(&Tuple) -> bool` (two-valued). SQL `WHERE` is
  three-valued: a predicate evaluating to NULL/UNKNOWN must exclude the row. **Resolved:**
  `Predicate::compile` collapses UNKNOWN→false at the leaf — correct for NOT-free predicates,
  but wrong under `NOT` (`WHERE NOT(x=5)` with x NULL keeps the row). Filter itself is fine;
  the bug is in `expr.rs` (O1).

### E8 — Sort NULL ordering ("always last") diverges from reference engines
- **Where:** `execution/sort.rs:123-130` (`compare_key` resolves NULL before direction).
- **Priority:** P2 · **Confidence:** high · **Status:** open (intentional / documented)
- **Detail:** NULLs sort **last under both ASC and DESC**. Postgres/Oracle/DB2 default to
  NULL=+∞ (ASC→last, DESC→**first**); MySQL to NULL=−∞ (ASC→first, DESC→last). This impl
  matches none consistently (matches PG for ASC, not DESC). Documented as an intentional
  OLTP choice, but the project differential-tests against Oracle/DB2/PG, so
  `ORDER BY … DESC` over nullable columns disagrees with the reference. Fix options: add
  `NULLS FIRST/LAST` (binder already anticipated) or adopt NULL=+∞ to match PG.

### E9 — Sort comparator silently returns `Equal` on unexpected type pairings
- **Where:** `execution/sort.rs:131-145` (cross-scale `Decimal` and the `_ => Equal`
  catch-all). Shared shape with `compare_values` / `eval_compare`.
- **Priority:** P3 · **Confidence:** high · **Status:** FIXED (2026-07-01) — sort/MIN-MAX
  now use `Value::compare_sql` and `debug_assert!` on incomparable non-NULL pairs.
- **Detail:** Cross-scale Decimals and any unexpected type pair fall to
  `Ordering::Equal`, i.e. "these sort as equal." Not currently triggerable: the catalog
  enforces fixed per-column Decimal scale (`DecimalScaleMismatch`) and single-typed
  columns, so the same-scale arm always wins. But per "assert negative space / crash on
  corruption," a mismatch reaching here is an upstream constraint bug and should
  `debug_assert!`/panic, not silently mis-order.

### E10 — IndexNestedLoopJoin omits NULL-key exclusion and predicate recheck
- **Where:** `execution/join.rs:214-232` (`refill_inner` builds a prefix straight from the
  outer key, no NULL check, no ON recheck).
- **Priority:** P2 · **Confidence:** medium · **Status:** FIXED (2026-07-01) —
  `refill_inner` now skips NULL keys, coerces the probe to the indexed column's type
  (`Value::coerce_exact`), and rechecks fetched rows against the probe key.
- **Detail:** `HashJoin` skips NULL keys and `NestedLoopJoin` excludes them via the 3VL
  predicate; INLJ does neither. A NULL outer join key probes the index for NULL entries and
  emits spurious `NULL = NULL` matches (if nullable join columns are indexed). Also inherits
  **E1**: IndexScan never rechecks the indexed value, so under concurrent MVCC INLJ can emit
  rows whose current key ≠ the probe key. Triggerability depends on NULLs being indexed and
  the planner choosing INLJ for a nullable key. Test via a `{NLJ, HashJoin, INLJ}`
  equivalence differential over nullable join keys.

### E11 — HashJoin/INLJ drop the join predicate; equality ≠ NLJ's `eval_compare` (strategy inequivalence)
- **Where:** `execution/join.rs` (HashJoin keys on `Value` derived `Hash`/`Eq`; INLJ on index
  prefix) + `sql/planner.rs:379` (ON consumed, not re-applied) + `planner.rs:534`
  (`extract_equi_join_keys` checks column positions only, **no type guard**).
- **Priority:** P1/P2 · **Confidence:** high (mechanism) / medium (triggerability) ·
  **Status:** FIXED (2026-07-01) — HashJoin keys via `Value::join_key_normalized`
  (normalize-equal ⟺ `compare_sql`-equal, property-tested), INLJ probes via
  `coerce_exact`. Differential: `tests/join_strategy_equivalence_test.rs`
  ({NLJ,Hash,INLJ} × {Volcano,Push}); verified red without the fix.
- **Detail:** `NestedLoopJoin` evaluates the compiled ON predicate, which cross-promotes
  numeric types (`Int32(5) == Int64(5)`). `HashJoin` matches by raw `Value` equality
  (`Int32(5) != Int64(5)`, different hash) and `IndexNestedLoopJoin` by index-prefix
  encoding — neither rechecks the predicate. So the **same query returns different rows
  depending on the chosen join strategy** whenever join keys are of different-but-comparable
  numeric types. Violates the strategy-equivalence property the testkit enforces. Latent for
  same-typed keys (TPC-C); real for mixed-int joins. Repro: `assert_all_equal` differential
  over `{NLJ, HashJoin, INLJ}` on a mixed `Int32`/`Int64` join key.

### E12 — Cost model costs HashJoin build on the smaller side; executor always builds on `inner`
- **Where:** `sql/planner.rs:569` (`build = min(outer, inner)`) vs `execution/join.rs:330`
  (`HashJoin::new` always builds from `inner`).
- **Priority:** P3 · **Confidence:** high · **Status:** DORMANT (revisited in Opt Unit 3)
- **Detail:** The cost estimate assumes the hash table is built on the smaller side; the
  operator unconditionally builds on `inner`. In left-deep plans `inner` is usually the
  smaller base table so it's often fine, but when `outer < inner` the cost model
  under-estimates build/memory cost. Align one to the other (build smaller side, or cost
  `build = inner`).
- **Update (Opt Unit 3):** `cost_hash_join = build_rows + probe_rows` is **symmetric**, so
  the planner's `(build,probe)=min/max(outer,inner)` has no effect on cost (dead computation)
  and the executor's always-build-`inner` has no cost consequence in the current memory-free
  model. **Dormant** until memory/spill cost is modeled (Phase 20) — at which point build
  side matters and this must be aligned.

### E13 — SUM/AVG accumulate with `wrapping_add` → silent overflow
- **Where:** `execution/hash_aggregate.rs:286-289, 346-350` (`SumInt`, `AvgInt`).
- **Priority:** P2 · **Confidence:** high · **Status:** FIXED (2026-07-01) —
  `checked_add` → `Error::NumericOverflow` (new variant); AVG's i128→i64 mantissa
  narrowing also checked. Tests in `tests/hash_aggregate_test.rs`.
- **Detail:** `SUM(Int64)` and the integer `AVG` sum use `wrapping_add`, so overflow silently
  wraps and returns a wrong number instead of erroring. SQL engines raise numeric overflow.
  `SUM(Int32)` is promoted to i64 first (safe for realistic row counts); `SUM(Int64)`
  overflows with a few large values. Prefer `checked_add` → overflow error ("crash on
  corruption" over silent-wrong).

### E14 — AVG truncates instead of rounds; Decimal AVG doesn't widen scale
- **Where:** `execution/hash_aggregate.rs:416` (integer AVG), `432` (Decimal AVG).
- **Priority:** P3 · **Confidence:** high · **Status:** open
- **Detail:** Integer AVG uses integer division (`(sum*10_000)/count`) → truncates toward
  zero (`AVG([1,2,2]) = 1.6666`, SQL rounds to `1.6667`). Decimal AVG divides the mantissa at
  the input scale, truncating and not widening result scale as reference engines do. Minor
  precision/fidelity divergence for differential tests.

### E15 — Numeric comparator duplicated across 3 sites (root of the E9/E11 cluster)
- **Where:** `execution/sort.rs:123` (`compare_key`), `execution/hash_aggregate.rs:444`
  (`compare_values`), `sql/expr.rs` (`eval_compare`) — plus join-key matching uses a *fourth*
  equality (`Value` derived `Hash`/`Eq` in `HashJoin`).
- **Priority:** P3 · **Confidence:** high · **Status:** FIXED (2026-07-01) — all four
  sites now derive from `Value::compare_sql` / `join_key_normalized` in `types/value.rs`.
- **Detail:** Three near-identical numeric comparators (Int32↔Int64 promotion, same-scale
  Decimal, silent `_ => Equal`) that must stay in sync; if one is fixed and the others aren't,
  strategies diverge. **This is the structural root of E9 (silent `Equal` in every copy) and
  E11 (HashJoin's separate `Value` equality ≠ `eval_compare`).** Fix: one canonical `Value`
  ordering + equality in `types/`, consumed by sort, aggregate, filter predicate, and
  join-key normalization. Collapses E9 + E11 + E15 into a single change.

### E16 — Push `ProjectionSink` skips the column-range validation Volcano does
- **Where:** `execution/push.rs:118-122` (`ProjectionSink::push`), `269` (`project_schema`)
  vs `execution/projection.rs:24-34` (`Projection::new` validates).
- **Priority:** P4 · **Confidence:** high · **Status:** open
- **Detail:** An out-of-range projection index panics under Push (`tuple[i]`) but errors
  cleanly under Volcano. Never triggers for a valid plan, but the same invalid plan fails
  two different ways across models. Fold into the E6 validation cleanup.

### E17 — UPDATE/DELETE lack read/write phase separation (latent Halloween Problem)
- **Where:** `execution/update.rs:63-72`, `execution/delete.rs:44-48` (write interleaved into
  the child-read loop). Interacts with **E3** (eager scans) and `planner.rs:1012,1069`
  (index-driven updates).
- **Priority:** P2 (latent) · **Confidence:** high (mechanism) · **Status:** FIXED
  (2026-07-01) — two-phase read-then-write in `update.rs`/`delete.rs`; SET expressions
  evaluate against the row as read; `PhaseProbe` tests assert no write lands while the
  child is still being drained. Child-schema `debug_assert` added (the "Also" item).
- **Detail:** The Halloween Problem (System R, 1976): a mutation that changes a column the
  access path depends on can re-visit and re-mutate rows. `Update`/`Delete` interleave the
  write into the read loop with no phase separation. Safe **today only because E3 makes
  `SeqScan`/`IndexScan` fully materialize their output before the first write** — an
  accidental safeguard the perf NOTEs plan to remove. Once scans stream (E3 fix),
  `UPDATE … SET k=… WHERE <indexed k>` over a lazy index scan will re-update rows, and
  `DELETE` will mutate the B-tree under a live iterator cursor. Fix (textbook): materialize
  the target PK set before applying any mutation — land it before/with the streaming refactor.
- **Also:** `Update`/`Delete` extract PKs by the *table's* `primary_key` indices assuming the
  child schema matches the table; unchecked (E5 family). A `debug_assert!` on child-schema
  alignment would catch a mis-plan loudly instead of mutating the wrong PK.

---

## Scope notes (not bugs — known gaps / deferred work)

- **S1 — IndexScan covers single-column equality only.** `try_lower_index_predicate`
  (`planner.rs:693`) matches exactly `col = literal` vs. a single-column index; composite
  indexes, range predicates, and AND-decomposition are explicitly deferred. Honest +
  documented.
- **S2 — No covering-index optimization.** IndexScan always dereferences to the base
  table; it never returns columns straight from the index key. Standard optimization,
  absent, not incorrect.
- **S3 — Per-entry PK descent, no batching.** An IndexScan of N entries does N
  independent `get_by_pk` B-tree descents (random base-table access). Faithful to real
  cost; relevant to whether the optimizer's cost model reflects it (to check in the
  optimizer review).
- **S4 — Limit has no OFFSET.** `Limit` caps row count only; no `OFFSET`/skip support.
  Expected for V1.
- **S5 — Filter EXPLAIN omits the predicate.** `Filter::explain` prints `Filter` with no
  predicate description (the closure carries none); documented as a NOTE plan-deviation.
  EXPLAIN readability only.
- **S6 — Sort is in-memory only.** Buffers the whole child with no `max_rows` guard
  (only bound is "fits in memory"); external/spill merge sort reserved for Phase 20.
- **S7 — "Block" nested-loop is a misnomer.** `PhysOp`'s `NestedLoopJoin` doc says *block*
  nested-loop, but the impl is tuple-at-a-time over a fully-buffered inner (not classic
  block-NLJ). I/O-equivalent (inner read once), but the name doesn't match the algorithm.
- **S8 — Joins are inner-only.** No LEFT/RIGHT/FULL outer across all three strategies;
  HashJoin/HashJoin spill and Grace partitioning deferred to Phase 20.
- **S9 — Join match-copy overhead.** HashJoin clones the whole match `Vec` per probe;
  INLJ recreates an `IndexScan` (which itself buffers) per outer row. Perf only.
- **S10 — Model-equivalence test is example-based and orthogonal to strategy-equivalence.**
  `tests/push_equivalence_test.rs` compares Volcano vs Push for a fixed plan (hand-picked
  queries), so it can never catch **E11** (a divergence between join *strategies*, not
  *models*). A `for_each` workload matrix crossing both models *and* join strategies would
  cover both axes.
- **S11 — Push pipeline is `!Send`** (`Rc`/`RefCell` collector) vs the `Send` Volcano
  `Executor`. Fine for single-threaded per-query execution; note if intra-query parallelism
  is added.
- **S12 — Push is a produce/consume *interpreter*, not a Neumann *compiler*.** Per-tuple
  `Box<dyn Sink>` dispatch; the win is streaming (skipping the eager scan `Vec`), not
  codegen. Only 4 linear operators are native sinks; the rest delegate to Volcano operators.

---

## Optimizer (`src/sql/`)

_(pending — review begins after the executor pass)_
