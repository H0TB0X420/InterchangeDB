# Executor + Optimizer Review — Prioritized Fix Plan

Cross-cutting, prioritized fix list distilled from the two findings logs:

- **`docs/exec-optimizer-review.md`** — executor findings (`E1`–`E17`, scope `S*`).
- **`docs/optimizer-review.md`** — optimizer findings (`O1`–`O13`, scope `O-S*`).

Ranked by severity × reachability. Report-only origin — nothing has been changed yet.

> **2026-07-01 — implemented.** Sections 0, 1 (all correctness fixes) AND 2 (O10
> selectivity wire-up, E8 NULL sort order) are DONE; see the per-item ✅ marks below
> and the flipped `Status: fixed` lines in the findings logs. Remaining: the
> Section-3 cleanups and the versioned-index architecture decision (§0 verdict).
> New tests: `tests/index_mvcc_recheck_test.rs`, `tests/join_strategy_equivalence_test.rs`,
> `tests/binder_predicate_coercion_test.rs`, `where_selectivity_drives_reordering`
> (selinger_reorder_test), plus operator/unit tests in `update.rs`, `delete.rs`,
> `hash_aggregate_test.rs`, `expr.rs`, `value.rs`, `sort_operator_test.rs`.

---

## 0 — Gating investigation (do first; sets the severity of the P1 index block)

- **Resolve secondary-index MVCC versioning.** Trace whether `delete_index_entries`
  physically removes the old key or writes a snapshot-versioned tombstone. This single fact
  decides whether **E1** (wrong reads) and **O12** (wrong writes on UPDATE/DELETE) are latent
  or *live data corruption*. The entire P1 index block hinges on the answer.

  **✅ RESOLVED (2026-07-01) — the exposure was LIVE.** Index engines are raw
  `Arc<dyn StorageEngine>` (`catalog/manager.rs:353`) — never `TxnEngine`-wrapped, so
  index entries are unversioned, un-WAL'd, and physically deleted on update
  (`table/mod.rs:111`). `txn_abort` does no index undo (`database.rs:481`).
  Consequences and disposition:
  - **False positives** (stale entry → snapshot-visible row that violates the WHERE;
    wrong-row UPDATE/DELETE): **fixed** by the recheck Filter (E1/O12) and the INLJ
    recheck (E10). Proven by `tests/index_mvcc_recheck_test.rs`.
  - **False negatives** (old entry physically gone → a snapshot-visible row is
    unreachable via the index; permanent index divergence after an abort that touched
    indexed columns): **NOT fixable by a recheck** — needs versioned index entries or
    abort-time index undo. Architectural decision, deliberately not built in this pass;
    pinned by the `known_gap` test in `index_mvcc_recheck_test.rs`.

---

## 1 — Correctness (silent-wrong / contract violations)

| Fix | Findings | Notes |
|---|---|---|
| ✅ Add a recheck `Filter` (or stop dropping the predicate) for non-unique **IndexScan** lowering — SELECT *and* UPDATE/DELETE | E1, O12 | DONE — `IndexLowering::Matched` now carries `recheck`; all four lowering sites re-apply it. PkLookup keeps dropping (exact/unique). |
| ✅ **Read/write phase separation** in UPDATE/DELETE (materialize target PKs before mutating) | E17 | DONE — two-phase `next()` in `update.rs`/`delete.rs`; `PhaseProbe` tests pin the contract. |
| ✅ **One canonical `Value` comparison + equality** in `types/`, used by sort, aggregate, filter, *and* join-key normalization | E9 + E11 + E15 | DONE — `Value::compare_sql` / `join_key_normalized` / `coerce_exact` in `types/value.rs`; 6-way `{NLJ,Hash,INLJ}×{Volcano,Push}` differential in `tests/join_strategy_equivalence_test.rs`. |
| ✅ **Real three-valued logic** in predicate eval (`Option<bool>`/Kleene; keep only `True`) | O1 | DONE — `Predicate::compile_3vl`, collapse to bool only at the WHERE boundary. |
| ✅ **Coerce WHERE literals to column type** in `bind_predicate` (reuse existing `narrow_value`) and/or complete `eval_compare`'s type matrix | O2, O13 | DONE — both layers: `narrow_compare_operands` in the binder AND `coerce_exact` in the planner's index/PK lowering (prepared statements bypass the binder). `eval_compare` matrix completed via `compare_sql`. |
| ✅ **`checked_add`** for SUM and integer arithmetic → overflow error | E13, O3 | DONE — SUM/AVG raise `Error::NumericOverflow`; expression arithmetic yields NULL (infallible closures; NOTE'd deviation). |

---

## 2 — Fidelity / performance (measurable plan quality)

| Fix | Findings | Notes |
|---|---|---|
| ✅ **Wire `local_selectivity` into the DP** (split WHERE per table via existing `flatten_conjuncts`/`referenced_columns`, call `estimate_predicate_selectivity`) | O10 | DONE 2026-07-01 — `apply_local_selectivities` in `selinger.rs::build_join_graph`; `where_selectivity_drives_reordering` test (verified red pre-fix). |
| ✅ **Match reference-engine NULL sort order** (or add `NULLS FIRST/LAST`) | E8 | DONE 2026-07-01 — NULL = +∞ direction-aware in `sort.rs::compare_key` (ASC last, DESC first, PG/Oracle/DB2 default). |

---

## 3 — Hardening & cleanups (low risk; align with the stated doctrine)

- **Assert/crash instead of silent `Equal`** on unexpected type pairings in comparators — E9.
- **Consolidate projection column-range validation** across Volcano/Push; fix the misleading
  `StorageCorrupted` variant — E6, E16.
- **AVG rounding/precision + decimal division semantics** — E14, O4.
- **Drop `PkLookup.done`** (redundant) and the **unused `groups` param** — E2, O7.

---

## Tracked as scope (not fixes — deferred by design)

Streaming scans (E3) → then LIMIT early-termination cost (O6); external sort (S6); spill
costing (which makes E12 live); interesting orders with merge join (O8); covering indexes
(S2); `IS NULL` / `BETWEEN` / `IN` / anonymous `?` (O-S5).

---

## Suggested sequencing

1. **Investigation 0** — index MVCC versioning (gates the rest of the index block).
2. **Index recheck + phase separation** — E1 / O12 / E17 (the wrong-write cluster).
3. **Canonical comparator** — E9 / E11 / E15 (cross-strategy equivalence).
4. **3VL + type coercion** — O1 / O2 / O13 (silent-wrong evaluation).
5. **O10 wire-up** — the performance payoff, once correctness is solid.

Steps 1–4 are correctness; step 5 is the cost-based-planner performance win.
