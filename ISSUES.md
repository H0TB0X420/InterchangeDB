# Quality Tracker

Anchored to commit `0aaff20` (May 24, 2026 — Phase 11 SQL surface landing).

This document is the single source of truth for open quality items in
InterchangeDB. It supersedes `ISSUES.md` — narrow code defects (formerly
`ISS-NNN`) and broader quality investments now share one numbering
surface (`Q-NN`) here. Resolved items are intentionally omitted; closed
items live only in the audit history at the bottom.

---

## Plan-vs-code drift findings

The May 24 audit identified five checkboxes in `plan.md` that overstate
the actual state of the code. These are corrected here; `plan.md` should
be updated to match.

| Plan claim                                                                              | Actual state                                                                                                                                  | Action                                                                                    |
| --------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Task 8.3: "Policy swap under load" `[x]`                                                | No such test exists. `tests/concurrency_stress_test.rs` has 5 tests, none swap a policy. `policy_swaps` counter exists; stress test does not. | Mark `[ ]`; tracked as Q-01.                                                              |
| Task 6.5: Hermitage tests (G0/G1a/G1b/G1c/G2-item/G2) `[x]`                             | Zero hits for "hermitage" or any G-anomaly name across the codebase.                                                                          | Mark `[ ]`; tracked as Q-24.                                                              |
| Task 8.6: "shuttle dev-dep" `[x]`                                                       | `shuttle` not in `Cargo.toml`. Manual-barrier substitute shipped in `tests/deterministic_test.rs` with documented rationale.                  | Re-phrase plan checkbox to acknowledge the substitute, or adopt shuttle. Tracked as Q-25. |
| Task 8.5: "B-Tree / LSM / Transaction proptest" `[x]`                                   | Partial. MVCC encoding, BTreeEngine put-get, GC, and txn lifecycle covered. LSM and lock manager have zero proptest coverage.                 | Mark `[~]` partial; tracked as Q-08.                                                      |
| Phase 11 frontloaded: `tests/common/mock_catalog.rs`, `mock_stats.rs`, `golden_plan.rs` | `tests/common/` does not exist as of `0aaff20`.                                                                                               | Phase 11 scaffolding obligation; tracked as Q-02.                                         |

The meta-issue (plan-as-aspiration vs. plan-as-record) is tracked as
Q-23: a checkbox audit pass against the entire plan.

---

## Open quality items

Severity: Critical / High / Medium / Low.
Effort: S (hours), M (1–2 days), L (several days), XL (week+).

### Phase 11 — blockers for the current phase

| #    | Item                                                                                                                                                                                                                                                                  | Sev      | Effort | Status |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ------ | ------ |
| Q-23 | Plan-vs-code checkbox audit. Walk every `[x]` in `plan.md`, verify against code, downgrade overstated ones. Cost: hours. Value: removes credibility risk.                                                                                                             | High     | M      | Open   |

### Open correctness defects

| #    | Item                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Sev    | Effort | Status |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------ | ------ | ------ |

### Foundation gaps (cost rises with delay)

| #    | Item                                                                                                                                                                                                                                                                                                                                                                                                     | Sev    | Effort                  | Status |
| ---- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ----------------------- | ------ |
| Q-25 | Adopt shuttle/loom for exhaustive interleaving exploration, or update `plan.md` to acknowledge the manual-barrier substitute currently in `tests/deterministic_test.rs`. The current plan claim is incorrect either way.                                                                                                                                                                                 | Medium | M (adopt) / S (correct) | Open   |
| Q-08 | **Property-based test breadth** (was ISS-006, partially resolved). Current proptest coverage: MVCC encoding roundtrip + ordering, BTreeEngine put-get consistency, GC visibility, txn lifecycle. Missing: LSM invariants (memtable + SSTable merge equivalence to a HashMap oracle), lock manager (no false-positive deadlocks, lock table empty at quiescence). Files: extend `tests/proptest_test.rs`. | Medium | M                       | Open   |
| Q-09 | Cross-engine randomized differential test. Same random op sequence against `BTreeEngine` and `LsmEngine`, observable state identical at every checkpoint. Validates the interchange thesis at the storage layer.                                                                                                                                                                                         | Medium | M                       | Open   |

### Phase 11 binder / planner coverage

| #    | Item                                                                                                                                                                                                                  | Sev    | Effort | Status |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ------ | ------ |
| Q-19 | Binder invariant tests. `src/sql/binder.rs` is 1,078 lines of name and type resolution; needs property-level coverage (random schemas + queries → catalog consistency, type coherence, no panics on malformed input). | Medium | M      | Open   |
| Q-20 | Planner determinism property: same logical plan → byte-identical physical plan across runs.                                                                                                                           | Medium | S      | Open   |

### Lower-priority hygiene

| #    | Item                                                                                                                                                                                                                                                                                                                                                   | Sev | Effort      | Status |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --- | ----------- | ------ |
| Q-15 | Back-fill invariant docstrings on older modules to match the new SQL files' documentation standard (module-level scope notes, deferred-work markers, contract statements).                                                                                                                                                                             | Low | M (ongoing) | Open   |
| Q-22 | Parser/binder fuzz target. `sqlparser-rs` may be fuzzed upstream; the binder layer is custom and unfuzzed.                                                                                                                                                                                                                                             | Low | M           | Open   |
| Q-16 | **Near-limit key/value sizes untested** (was ISS-007). No tests exercise keys/values near the maximum that fits in a single page. B+Tree: a key larger than ~4060 bytes (PAGE_SIZE − leaf header) fails to encode. LSM WAL records cap key/value at `u16::MAX` (65535 bytes). Add tests at exactly the max, one byte over, and at page-boundary sizes. | Low | S           | Open   |
| Q-17 | **Large-scale integration tests missing** (was ISS-008). Current scale tests use 5K–10K keys. No tests exercise 100K+ keys to verify performance doesn't degrade non-linearly, memory stays bounded, and BPM eviction works under sustained pressure. Add benchmarks/tests at 100K and 1M key scale with bounded pool sizes.                           | Low | M           | Open   |
| Q-26 | `Database::gc_status() -> GcStatus` observability hook. Plan Task 7.3 lists this as complete; not implemented. `Database::gc()` returns post-collection `GcStats`, but there's no way to inspect current GC state (low-water mark, version count by table) without running a collection. Not urgent until Phase 16 perf work, but plan was overstated. Files: `src/database.rs`, `src/txn/gc.rs`.                                       | Low | S           | Open   |

---

## Items deferred to future phases

These are quality investments the plan correctly places in future phases.
Listed here so they don't accidentally get re-tracked as current gaps.

| Item                                                               | Owning phase                             |
| ------------------------------------------------------------------ | ---------------------------------------- |
| Cost model calibration (predicted vs actual I/O per operator)      | Phase 14 — Selinger + stats + CostModel  |
| Statistics correctness (ANALYZE on known distributions)            | Phase 14                                 |
| Planner-comparison harness (interchange-thesis empirical evidence) | Phase 18 — Cascades + comparison harness |
| TPC-C harness, tpmC measurement                                    | Phase 16                                 |
| TPC-H readiness (SQL surface, external spill, harness, SF1)        | Phase 20                                 |
| Vectorized `ExecutionModel` (third impl)                           | Open-Ended (post-Phase 22)               |
| ClickBench harness                                                 | Open-Ended                               |
| Q21 full decorrelation                                             | Open-Ended                               |

---

## Suggested ordering

Cheap items first to lock in current state, then high-impact correctness
work, then larger refactors.

1. **Q-23** plan checkbox audit (hours, no risk)
2. **Q-21** `render_explain` nested fix (hours)
3. **Q-02** Phase 11 scaffolding (highest leverage for current and future planner phases)
4. **Q-11** EXPLAIN goldens (depends on Q-02; lock down before Phase 14)
5. **Q-04** LSM atomic flush (real defect, small fix)
6. **Q-05** B+Tree iterator bounds check (small)
7. **Q-06 → Q-07** DiskManager refactor + fault injection
8. **Q-01** Hot-swap stress (full coverage benefits from fault-injection from Q-07; needed before Phase 16)
9. **Q-24** Hermitage SI suite (closes a plan-overstated correctness item)
10. **Q-19 → Q-20** Binder + planner property tests
11. **Q-08** proptest breadth to LSM + lock manager
12. **Q-09** Cross-engine differential test
13. **Q-25** Shuttle adoption or plan correction
14. **Q-15** Docstring back-fill (ongoing alongside other work)
15. **Q-22, Q-16, Q-17** remaining low-priority items

---

## How to use this document

- New quality concerns get the next free `Q-NN` number. Preserve
  numbering even when items close — historical references should always
  resolve.
- When closing an item, move it out of the table and append a one-line
  entry to the audit history at the bottom with the closing commit.
- At each phase transition, re-verify the "Plan-vs-code drift" section
  against the actual code state. The lesson of the May 24 audit is that
  checkboxes drift silently between intent and reality.
- This file supersedes `ISSUES.md`; the old `ISS-NNN` IDs are referenced
  parenthetically inside the relevant `Q-NN` rows for historical
  traceability.

---

## Audit history

| Date       | Commit    | Notes                                                                                                                                                      |
| ---------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2026-05-24 | `0aaff20` | Initial combined audit. 22 items open. 5 plan-drift findings. ISS-001 (page checksum) and ISS-002 (LSM mutex poisoning) closed prior; not carried forward. |
| 2026-05-27 | `0aaff20` | Q-23 sweep added Q-26 (`gc_status` missing observability hook). Phase 6/7 plan claims for `mvcc_put`/`mvcc_delete` and `register_snapshot`/`unregister_snapshot` annotated as NOTE-refactors in `plan.md` (capability exists under different names/structure: `TxnEngine`, `TransactionManager::active_read_timestamps`). |
| 2026-05-27 | (uncommitted) | Q-21 closed: `render_explain` nested arm now wraps inner text with `"Explain\n"` + 2-space indent per line. Regression test `plans_nested_explain_preserves_inner_render` hand-builds nested `LogicalPlan::Explain` (parser rejects nested EXPLAIN syntactically, so SQL surface is unaffected). |
| 2026-05-27 | (uncommitted) | Q-02 closed: `tests/common/{golden_plan,mock_catalog,mock_stats}.rs` + `mod.rs` + `tests/common_smoke.rs` smoke binary. 18 inline tests cover the scaffolding. Design deviations from the plan: (a) `MockCatalog` wraps the real `Catalog<BTreeEngine>` via tempdir rather than introducing a `CatalogRead` trait — avoids refactoring binder/planner/executor signatures right before Phase 12; (b) `MockStats` is a plain struct, not a trait impl — Phase 14 will add the `StatsProvider` trait once the cost model determines its shape; (c) histograms deferred to Phase 14 per same rationale. |
| 2026-05-27 | (uncommitted) | Q-11 closed: `tests/explain_goldens.rs` — 17 goldens covering CREATE TABLE, BEGIN/COMMIT/ROLLBACK, every SELECT shape (bare/projection/filter/limit/multi-col), INSERT (single + multi-row), UPDATE (with/without filter), DELETE (with/without filter), EXPLAIN (wrapping each). Failures pinpoint plan structure changes before Phase 14 starts rewriting. |
| 2026-05-27 | (uncommitted) | Q-04 closed: SSTable writes now go to `<id>.sst.tmp`, fsync, rename to final `<id>.sst`, plus best-effort dir fsync. Manifest `log_add`/`log_del` add `sync_all` after each entry. `Manifest::open` sweeps orphan `*.sst.tmp` files. 6 regression tests in `tests/lsm_atomic_flush_test.rs` cover orphan sweep, durability across drop+reopen, malformed-tmp isolation, repeated-flush leak check. All 583 lib + 14 LSM-adjacent integration tests pass. |
| 2026-05-27 | (uncommitted) | Q-05 closed: `BTreeScanIterator::load_next_page` validates decoded leaf invariants (`keys.len() == values.len()`, tombstone indices in range) and bounds-checks `next_page_id` against `bpm.disk_page_count()` before assigning it. Validation fires before yielding any entries from the current page — defensive, since a corrupt sibling pointer may signal broader corruption. 3 regression tests in `tests/btree_iter_bounds_test.rs`: out-of-range sibling triggers `StorageCorrupted`, `PageId::INVALID` terminator still works, boundary value (`next_page_id == page_count`) is rejected. |
| 2026-05-27 | (uncommitted) | Q-06 closed: `pub trait DiskManager` extracted with 5 methods (`read_page`, `write_page`, `allocate_page`, `page_count`, `file_size`). Old `DiskManager` concrete struct renamed to `FileDiskManager` (unchanged behavior). New `MemoryDiskManager` stores pages as raw `[u8; PAGE_SIZE]` arrays (avoids weakening Page's `#[cfg(test)] Clone`). `BufferPoolManager::new` is now generic over `D: DiskManager + 'static` and boxes internally — zero call-site changes beyond the rename. Mass rename across 32 files via sed. 589 lib + 17 EXPLAIN goldens + 6 LSM atomic flush + 3 btree iter bounds + 28 database integration all pass. |
| 2026-05-27 | (uncommitted) | Q-07 closed: `tests/common/fault_injection.rs` ships `FaultInjectionDiskManager<D>` wrapping any `DiskManager` with `AtomicUsize` counters for read/write/allocate failure injection. Pre-configured before handoff to BPM. 5 unit tests (counter consumption, independence per op-kind, pass-through). `tests/fault_injection_test.rs` adds 5 BPM-level integration tests — proves BPM propagates `Error::Io` rather than silencing dirty-eviction write failures or flush failures. **Scope deviation:** WAL/SSTable I/O uses raw `std::fs::File` directly, not `DiskManager`, so fault injection there requires separate trait extraction. Documented as deferred in the test-file module comment. |
| 2026-05-27 | (uncommitted) | Q-01 + Q-27 closed together. **Q-27 root cause**: a two-layer race in BPM eviction. (a) `handle_cache_hit` pins *after* releasing pt.read, so concurrent eviction can replace frame contents between lookup and pin. (b) `swap_policy` re-registers frames in the new replacer using a stale `pt` snapshot — if it runs between `evict_for_page` (which removed the victim from the old policy's evictable set) and `pt.remove`, the new replacer marks the victim evictable again, letting a second eviction pick it. **Fix**: `evict_page` now holds `pt.write` across `evict_for_page` (pin-count check is now under the same lock that cache-hit threads block on via `pt.read`) and `swap_policy` holds `pt.write` for its entire duration. Both paths serialize at `pt.write`. Tests: 3 active in `tests/policy_swap_stress_test.rs` — marquee storm + isolation + warm-swap. All 583+ lib, 5 concurrency_stress, 7 BPM, 6 btree_concurrent, 1 deadlock_stress, 27 fault_injection, 3 policy_swap pass. This is the same class of bug that defeated the user's BusTub Project 2. |
| 2026-05-27 | (uncommitted) | Q-24 closed: `tests/hermitage_si_test.rs` — 6 tests covering Adya G0/G1a/G1b/G1c, G2-item (OTV), and G2 write skew. First five are PREVENTED under SI; write skew is documented as ALLOWED (would require SSI to prevent — flagged for future work). G0 test accepts either `WriteConflict` (MVCC layer) or `LockTimeout` (2PL fallback) — both correctly prevent dirty writes. |
