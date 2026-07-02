# TPC-C Results — Phase 16

Running log of published TPC-C measurements. **All numbers are RELATIVE**
— for comparing configurations and before/after tuning changes on this
one machine. They are not comparable to published tpmC figures (spec
deviations below) and not absolute performance claims (consumer hardware,
macOS fsync costs).

## Measurement protocol

- **Quiet machine.** No concurrent `cargo` build/test, no other load.
  (Protocol exists for a reason: the first baseline attempt ran while the
  test suite was live — run 1 measured 2803 tpmC, runs 2–3 collapsed to
  ~1600. Discarded.)
- Release build, 3 runs per configuration, **median by tpmC** reported.
- Fixed workload seed; data dir recreated per run.
- Record the commit hash with every table.

## Environment

- Intel Core i7-5557U @ 3.10GHz, 4 logical cores, macOS (Darwin 21.6),
  APFS. APFS fsync ≈ 11ms — commit latency is fsync-dominated; Linux/NVMe
  absolute numbers are a different world (environment item, tracked).
- Harness spec deviations (see `src/bin/tpcc.rs` doc): simplified
  single-column synthetic PKs, `smoke` cardinalities far below spec scale,
  no think times, no remote-warehouse mix. Hence: relative numbers only.

## Baseline — 2026-07-01, commit `23ce79c`

Canonical config: `btree / arc / volcano / rule-based`, 8 warehouses,
8 terminals, 60s, seed 1, pool default.

tpmC across runs: **3246 / 3179 / 3099** (median 3179, spread 4.5%).

Median run detail:

| Txn | count | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|
| NewOrder | 3184 | 69.6 | 139.3 | 180.2 |
| Payment | 2960 | 45.1 | 90.1 | 110.6 |
| OrderStatus | 295 | 81.9 | 204.8 | 245.8 |
| Delivery | 309 | 94.2 | 213.0 | 294.9 |
| StockLevel | 286 | 94.2 | 229.4 | 327.7 |
| **ALL** | **7034** | **57.3** | **147.5** | **204.8** |

- Throughput 117 committed txn/s; abort rate 0.0%.
- fsyncs 3010 ≈ committed/2: group commit is coalescing ~2 commits per
  fsync at 8 terminals. With APFS fsync ≈ 11ms, the WAL sync alone
  accounts for ~33s of the 60s wall-clock budget spread across terminals —
  commit latency is fsync-bound, as expected on this platform.
- Latency percentiles are full BEGIN→COMMIT (including group-commit wait),
  committed transactions only.

## Interchange sweep — engine × exec-model × planner

2026-07-01, commit `23ce79c`; same config/protocol, one 60s run per cell
(single-run — cross-cell gaps below dwarf the ~5% run-to-run spread).

| engine | exec | planner | tpmC | txn/s | p99 ms | fsyncs |
|---|---|---|---|---|---|---|
| btree | volcano | rule-based | 3217 | 119 | 205 | 3106 |
| btree | volcano | selinger | 3214 | 118 | 205 | 3098 |
| btree | push | selinger | 3120 | 115 | 205 | 2945 |
| btree | push | rule-based | 3069 | 113 | 229 | 2849 |
| lsm | push | rule-based | 428 | 16 | 1704 | 930 |
| lsm | volcano | selinger | 426 | 16 | 1769 | 933 |
| lsm | push | selinger | 424 | 16 | 1835 | 927 |
| lsm | volcano | rule-based | 420 | 15 | 1704 | 902 |

Observations:

- **Engine axis dominates: LSM is ~7.6× behind B+Tree** (≈425 vs ≈3200
  tpmC) with p99 ≈ 1.7–1.8s vs ≈205ms. The planner/exec axes are noise in
  comparison. This is the sweep's headline and a profiling target: the
  gap is far larger than an LSM write-path should cost on a write-heavy
  mix (LSM writes are append-cheap), pointing at the read path
  (memtable+SSTable scans for the mix's lookups) rather than writes.
- **Planner axis: dead heat on both engines** (rule-based 3217 vs
  Selinger 3214). Confirms the plan-shape analysis: TPC-C statements are
  single-table or comma-join, so both planners emit identical plans and
  Selinger's per-execute stats reads don't register at fsync-bound
  throughput. The planner axis gets its real test on TPC-H (deferred by
  decision 2026-07-01).
- **Exec-model axis: push trails volcano by ~3–5%** on this DML-heavy
  mix — consistent with Phase 15's finding that push's win is streaming
  scans with LIMIT, which this mix doesn't exercise; DML statements pay
  the executor-bridge overhead.
- All btree cells remain fsync-bound (fsyncs ≈ committed/2).

## Tuning iteration 1

### T16.4 profiling (2026-07-01, 10s `sample` captures mid-run, commit `23ce79c`)

**Btree canonical run** — after the fsync floor (`__fcntl`/F_FULLFSYNC +
group-commit condvar waits, the expected macOS ceiling), the top
user-code symbol is `VecDeque::retain` (525 top-of-stack samples), and
the call tree attributes it to **`ArcReplacer::record_access` called from
`fetch_page_internal` — the cache-HIT path**. ARC's `record_access` does
up to four linear `retain` scans (t1/t2 frame lists at arc.rs:166,169;
b1/b2 ghost lists at arc.rs:150,157) — **O(pool_size) work per page hit,
inside the global replacer mutex**. `__ulock_wait` at 6547 samples shows
the other terminals parked on that mutex. This is the long-documented
"per-hit replacer mutex" bottleneck with its mechanism identified: the
critical section is not just serialized, it is linear in pool size.

**LSM run (the 7.6× sweep gap)** — `__fcntl` drops to 728 (writes are
append-cheap, as predicted); the profile is instead dominated by
`MergeIterator::collect_all` (+ map/btree iterator next) under a wall of
`memmove` (1881) and allocator traffic (~6500 samples): **the LSM read
path materializes entire merged scans per lookup** rather than lazily
merging with early termination. That is the mechanism behind the 425 vs
3200 tpmC gap and the 1.7s p99.

### Candidates (T16.4b)

1. **ARC `record_access` O(n) retains under the replacer mutex.** The
   canonical config's top code bottleneck. Fix shape: O(1) membership /
   positional maps (as Clock/LRU already use) instead of `retain` scans;
   hit-path work should be O(1). Blast radius: `arc.rs` + policy
   conformance tests; Q-35 shuttle gate applies to any replacer change.
2. **LSM `collect_all` read materialization.** The interchange headline
   (7.6×). Fix shape: lazy merge with first-visible-version early exit
   for point reads. Blast radius: `lsm/merge_iterator.rs` + engine reads.
3. **Tuple-decode allocator churn** (both engines; nanov2 clusters).
   Larger refactor (borrowed/arena decode) — Phase 17+ scope.

### The fix, part 1 (T16.5a) — ARC O(log n) hit path

User picked candidates 1 + 2, sequentially. Part 1: `arc.rs` replaces all
four `VecDeque` lists with `OrderedList` (seq-numbered `BTreeMap` order +
`HashMap` membership) — cache-hit move-to-MRU and mid-list removals drop
from O(pool_size) `retain` scans under the replacer mutex to O(log n).
ARC semantics (adaptation order, REPLACE, ghost trims, pinned fallbacks)
unchanged. Gates: ARC unit 17/17, buffer 99/99, config equivalence,
policy swap stress + marquee, full shuttle suite incl. the Q-35 storm.

Before/after (same protocol, quiet machine, 3 runs, medians):

| | baseline `23ce79c` | ARC fix | Δ |
|---|---|---|---|
| tpmC | 3179 | **3360** | **+5.7%** |
| p50 ms | 57.3 | 55.3 | −3.5% |
| p95 ms | 147.5 | 131.1 | **−11%** |
| p99 ms | 204.8 | 180.2 | **−12%** |
| run spread | 4.5% | 0.7% | — |

The contention signature: throughput gain capped by the fsync floor, but
tail latencies and run-to-run variance improve disproportionately (all
three fixed runs landed in identical percentile buckets). Expect the gap
to widen with pool size and terminal count.

### The fix, part 2 (T16.5b) — LSM scan range pushdown

Root cause (sharper than the profile's first read): `LsmTree::scan`
ignored its range until AFTER merging — full memtable clones, every
SSTable read whole from disk, whole-keyspace k-way merge, then a range
filter. `TxnEngine` implements MVCC point reads as scans of a key's
version range, so **every row lookup materialized the entire dataset**
(and degraded as the run grew it — the 1.8s p99). Fix: range-bounded
memtable iteration, SSTable pruning on `[first_key, last_key]` metadata
(no I/O for non-overlapping files), and range-trimmed sources before the
merge. Point `get` was already correct (bloom + binary search); only
`scan` was broken.

Before/after (same protocol; before = sweep cell, after = 3-run median):

| | before | after | Δ |
|---|---|---|---|
| tpmC | ≈425 | **3157** | **≈7.4×** |
| p99 ms | ≈1770 | 246 | **≈7.2×** |
| p50 ms | — | 30.7 | (beats btree's 55.3) |

The engine gap is effectively closed: LSM 3157 vs B+Tree 3360 (−6%),
with complementary latency profiles — LSM wins p50 (no page churn on
writes), B+Tree wins p99. Gates: LSM unit 53/53, cross-engine
differential, engine conformance, LSM goldenscripts, config equivalence.

### Phase 16 outcome

Tuning iteration 1 fixed both profiled bottlenecks: B+Tree **+5.7% tpmC
/ −12% p99** (ARC O(log n) hit path) and LSM **≈7.4×** (scan range
pushdown). Remaining known levers carried forward: the fsync floor
(environment — Linux/NVMe), tuple-decode allocation churn (Phase 17+),
block-level SSTable range reads, O(1) slab lists for ARC T1/T2.
