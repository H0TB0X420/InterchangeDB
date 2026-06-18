# TPC-C Scalability Investigation

Findings from a measure-driven performance investigation of the TPC-C
workload (2026-06). Captured here so a future scalability refactor has the
full picture without re-deriving it. **The headline: it's a multi-front
scaling problem with no single bottleneck, and several leading hypotheses
were falsified by measurement — read the log before optimizing.**

Environment: macOS (APFS, fsync ~11 ms), single node, smoke-scale data.
The absolute numbers are OS/hardware-bound; the *shape* (what scales, what
doesn't, and why) is what generalizes.

**Hardware — read before comparing numbers.** Two machines appear in this doc.
The *development / baseline* box (the "original" column, and all day-to-day
runs) is a **MacBook Pro (Retina, 13-inch, Early 2015): dual-core 3.1 GHz Intel
Core i7 (2 physical / 4 logical), 16 GB DDR3-1867, PCIe SSD** — only **2 cores**,
so it saturates at ~2–4 busy threads. The much faster figures (~168 txn/s, the
"M2 Pro" column) are from a **Mac mini — Apple M2 Pro, 10 cores** (~2–3.5×
faster). A low number on the laptop is **not** a regression; compare the *ratio*
between engines/configs, which is hardware-independent.

Current MacBook Pro baseline (2026-06-15, 8×8, smoke-scale, seed 1, 0% aborts):
**B-tree/arc ≈ 48–50 txn/s** (tpmC ~1.3 k); **LSM ≈ 18 txn/s** (tpmC ~535).
B-tree leads LSM ~2.7×, matching the Mac mini's ~3.2× ratio — a uniformly slower
box, not a regression. **Superseded 2026-06-17: B-tree/arc now ≈ 116 txn/s
(tpmC ~3.2 k) at 8×8 — the lazy-scan lever nearly doubled the full mix via
commit clustering. See "Lazy scan nearly doubles full-mix TPC-C" below.**

**Scaling characterization (2026-06-15, this laptop, warehouses == terminals):**
full-mix B-tree plateaus at ~50 txn/s by 4 terminals and stays flat to 16; LSM
is flat-to-negative (~20–27). But **read-only** (OrderStatus only — no write
locks, no WAL commits) scales ~4.3× (B-tree 89→386, LSM 86→372 over 1→8
terminals). Conclusion on this hardware: **the read path scales; the full-mix
ceiling is the write/commit path** (locks + WAL fsync), consistent with the
"wait-bound write mix" finding below. On 2 cores + a ~11 ms macOS fsync floor
with group commit barely batching (~1.2 fsyncs/commit), the full-mix number is
effectively hardware-capped here.

> **Re-measured 2026-06-06 on an Apple M2 Pro (10 cores, 16 GB, macOS Tahoe
> 26.1).** Magnitudes came in ~2–3.5× the original numbers (faster machine),
> but the *shape* reproduced exactly: B-tree peaks early then declines, LSM
> scales worse than B-tree at every point, 0% aborts throughout. See
> "Re-measurement" below.

---

## Lazy scan nearly doubles full-mix TPC-C (2026-06-17, this laptop)

**Headline: a read-path change lifted full-mix B-tree TPC-C ~1.9× (8×8: ~60 →
~116 txn/s, tpmC ~1650 → ~3180) — a new high score on this box.**
Drift-controlled A/B, HEAD (`dad1876`) vs the working tree, 2 reps each,
back-to-back, same machine, smoke-scale, seed 1, 0% aborts:

| build | throughput | tpmC | fsyncs/commit |
|---|---|---|---|
| HEAD (before) | 59, 61 txn/s | 1622, 1684 | 1.18, 1.16 |
| this session | 112, 119 txn/s | 3048, 3227 | 0.45, 0.43 |

**Why it worked — and why it's surprising.** The change is purely *read-path*
(lazy scans, below), yet it moved the **commit-floored** full mix that this
investigation said read-side levers can't touch. The mechanism is in the fsync
column: **commit clustering**. The full mix is gated by the ~11 ms fsync, and
group commit only batches when commits *overlap in time* — which TPC-C "doesn't
do much" (the §1 finding) *because each transaction first spends ~15 ms on its
own work, spreading the commits out*. The lazy scan cuts that per-transaction
read/scan work, so transactions reach COMMIT sooner and **bunch up**, and group
commit finally engages: batching went **1.17 → 0.44 fsyncs/commit (~2.6× fewer
fsyncs per commit)** and throughput rose in lockstep. So a read-side lever moved
the headline *indirectly*, by feeding the commit path more overlap. This is the
missing half of the "wait-bound write mix" story: the mix is fsync-floored, but
*how close to the floor you run* depends on how tightly commits pack — which the
read path controls. (Caveat: the two builds were ~3 min apart across a rebuild,
not interleaved; but 1.9× is far outside this box's ~20–28% thermal drift, and
the fsync-ratio shift is a mechanism, not just a number.)

### The lever: lazy, streaming scans (root cause was a vestigial trait bound)

`ScanIterator: DoubleEndedIterator` forced eager `Vec` materialization at *two*
layers. Nothing ever calls `next_back()`/`rev()` on an engine scan
(grep-verified), but the bound made `BTreeEngine::scan_range` `collect()` its
whole range "to satisfy DoubleEndedIterator" (its own comment), and `mvcc_scan`
buffer the whole visible range into a `Vec`. Worse, `mvcc_scan`'s probe
(`engine.scan(probe..).next()` — just to peek the first key) collected the
*entire* range first → **O(n) per probe**; a single OrderStatus scan
materialized its range ~3×. Fixed in three increments, full suite green at each
(1286 tests + 9 new `MvccScan` unit tests):
- Relax `ScanIterator` → `Iterator` (the whole suite is the proof nothing used
  reverse).
- `BTreeEngine::scan_range` returns the lazy `BTreeScanIterator` directly — one
  lifetime change (`BTree::scan` returns `'a`, not `'_`, since the iterator
  borrows only `&bpm`, not the temporary tree handle).
- New streaming `mvcc::MvccScan` iterator that *owns* its visibility snapshot
  (so it outlives the `scan_range` call); `mvcc_scan` is now a thin `.collect()`
  wrapper, so every existing caller/test still covers it.

This is the engine-/MVCC-layer analogue of lever #2 (the `EncodedLeaf` in-place
leaf walk): #2 removed the *per-row* leaf-decode allocation; this removes the
*per-scan* outer `Vec` materialization and the O(n) probe, and makes early
termination (LIMIT, first-match) reach the storage layer.

### Read-path diagnostics (where the lever shows directly)

Measured at `--scale big` (cardinalities 100× larger so the read path is
scan-bound — see "Cardinality scale" below):
- **Read-only (OrderStatus, full-consumption scan): 33 → 85 txn/s (2.6×)** —
  removes the O(n) probe + triple-buffering.
- **Push `SELECT … LIMIT 10` over 20k rows: 20 ms → 73 µs (~270×)** —
  `LimitSink.Stop` now propagates through lazy MVCC + lazy BTree all the way to
  the engine, reading ~10 rows not 20k. (Volcano same query 24 → 17 ms: its
  `SeqScan` still eagerly collects — making *it* lazy is the self-referential
  iterator problem, a separate lever. `benches/push_vs_volcano.rs`.)
- **PK point-lookup (`WHERE c_id = $1` over 8000 customers): 148 → 359 txn/s
  (2.4×)** — the PK-lookup lever (`PkLookup` vs `SeqScan+Filter`), A/B'd via
  `--pk-probe`: O(log n) get vs scanning 8000 rows.

### Cardinality scale: `--scale {smoke|big}`

Harness cardinalities are now a runtime flag. `smoke` (default; 2 districts / 10
customers / 20 items / 5 orders per warehouse) is the realistic fsync-bound
full-mix regime and the tpmC baseline. `big` (10 / 100 / 100 / 100) grows
customer/orders/order_line to ~8000 rows so the read-path levers cost enough to
measure — pair with `--read-only` / `--pk-probe`. **Caveat: the full mix is
scan/join-bound at `big`** — StockLevel's `FROM order_line, stock` nested-loop
join is ~O(|order_line| × |stock|), so one StockLevel over 32k×3.2k rows runs
*tens of seconds*; high-concurrency full-mix runs at `big` overshoot the
deadline by minutes (the terminal loop only checks the deadline between txns).
`big` is for read-path diagnostics, `smoke` for tpmC — and that nested-loop
join is itself a future lever (hash join / push the `ol_o_id` filter under it).
The loader also now bulk-loads in one transaction per warehouse (was one fsync
per INSERT → smoke load 6.2 s → 0.48 s), and the id-encoding digit budgets are
asserted at compile time.

---

## Tooling built for this (kept in the repo)

- **`src/bin/tpcc.rs`** — standalone harness. Flags:
  - `--engine btree|lsm` — runs the *same* workload through either
    `StorageEngine` (B+Tree uses the buffer pool; LSM bypasses it).
  - `--policy fifo|lru|lruk|clock|2q|arc` + `--pool-size N` — eviction
    policy + buffer-pool frames (B+Tree only).
  - `--read-only` — diagnostic: runs only OrderStatus (pure MVCC reads, no
    write locks, read-only commit skips the WAL). Isolates read- vs
    write-path scaling.
  - `--pk-probe` — diagnostic (read-only): runs only a single-row PK point
    lookup (`WHERE c_id = $1`) on the customer table. Isolates the PK-lookup
    lever (`PkLookup` vs `SeqScan+Filter`), which `--read-only` can't (its
    reads are non-PK).
  - `--scale smoke|big` — table cardinality preset. `smoke` (default) = the
    fsync-bound full-mix / tpmC regime; `big` = 100× larger read-path tables so
    the scan/lookup levers are measurable. (Full mix is scan/join-bound at
    `big` — use it with `--read-only`/`--pk-probe`.)
  - `--warehouses M --terminals N --duration-secs D --iterations I --seed S`.
  - Prints committed/aborted, per-txn counts, abort rate, **fsync count**,
    throughput, tpmC (NewOrder/min).
- **Profiler**: macOS built-in `sample <pid> <secs> -file out.txt` on the
  running release binary (symbols are present without stripping). The
  "Sort by top of stack" section is the leaf-of-stack breakdown.
- **`WAL fsync_count`** (`Database::wal_fsync_count()`) — observe group-commit
  batching.

Methodology that worked: **profile + differential experiments** (engine
swap, read-only mode, pool-size sweep, policy sweep). Repeatedly, a cheap
experiment killed a plausible structural hypothesis before code was written.

---

## Baseline (8×8, macOS, smoke-scale)

| config | throughput | notes |
|---|---|---|
| 1 terminal | ~46 txn/s | 22 ms/txn |
| 8 terminals | ~53 txn/s | ~150 ms/txn — **no scaling** |
| 16 terminals | ~50 txn/s | declining |
| 32 terminals | ~42 txn/s | declining further |

0% aborts throughout (terminal isolation: per-terminal home warehouse +
disjoint PK ranges). Throughput is flat/declining with concurrency → a
serialization problem.

---

## Re-measurement (2026-06-06, Mac mini — Apple M2 Pro, 10 cores)

Re-ran the harness on a faster box (a **Mac mini**: Apple M2 Pro, 10 cores,
16 GB, macOS Tahoe 26.1) — *not* the development MacBook Pro the "original"
column was measured on. 10 s runs, `warehouses == terminals`, seed 1, arc policy. The
absolute throughput is ~2–3.5× higher (faster CPU/SSD), but the scaling
*shape* is unchanged.

**B-tree (`--engine btree`):**

| config | original | M2 Pro | notes |
|---|---|---|---|
| 1 terminal | ~46 txn/s | **100 txn/s** | ~10 ms/txn |
| 8 terminals | ~53 txn/s | **168 txn/s** | peak — modest scaling (+68%), then declines |
| 16 terminals | ~50 txn/s | **151 txn/s** | declining |
| 32 terminals | ~42 txn/s | **133 txn/s** | declining further |

**LSM (`--engine lsm`):**

| config | original | M2 Pro |
|---|---|---|
| 1×1 | 31 txn/s | **65 txn/s** |
| 8×8 | 21 txn/s | **72 txn/s** |
| 32×32 | 12 txn/s | **54 txn/s** |

0% aborts throughout (one stray abort at LSM 32×32). Caveat: the original
16/32-terminal rows held warehouses=8; this re-run scaled warehouses with
terminals (per the harness's `warehouses >= terminals` isolation rule), so
only the 1T and 8×8 points are strictly apples-to-apples.

**What reproduced:** (1) B-tree shows no real scale-out — it peaks (here at 8
terminals) then declines as concurrency rises, the serialization ceiling the
investigation is about. (2) LSM is ~half B-tree's throughput at every point
and also declines — its read path is still the bottleneck. (3)
Interchangeability: both engines ran the identical SQL/MVCC/WAL stack with
only `--engine` changed. The faster machine surfaced *slightly* more early
scaling (peak at 8 vs near-flat originally) but the same downward slope past
the peak — the structural conclusions all stand.

---

## Investigation log (hypotheses, each resolved by measurement)

### 1. fsync per commit — PARTIALLY, then minor
First baseline looked fsync-serialized (8 terminals ≈ 1 terminal). The WAL
*had* group-commit scaffolding (`Wal::sync_to`, leader/follower + Condvar)
but it never batched: **the fsync ran while holding the writer lock**, so
followers couldn't append (let alone join the batch) during it.

**Fixed (committed):** leader flushes under the writer lock, captures a
dup'd file handle, **releases the writer lock, then fsyncs unlocked** under
a separate `sync_lock`; appends proceed during the fsync and ride the next
batch. Tight commit loop now batches 3.7× (`tests/wal_group_commit_test.rs`).
Also switched `sync_all`→`sync_data` (fdatasync). **But** in TPC-C commits
are temporally spread (per-txn work ~15 ms), so they rarely overlap to
batch — TPC-C improved only ~26% at 8×8. And the profile shows fsync
(`__fcntl`) is only ~2–7% of time anyway: when threads wait for durability
they're *asleep*, not the dominant cost.

### 2. Buffer pool — FALSELY exonerated, then re-implicated
Swapped to the LSM engine (`--engine lsm`), which **bypasses the
`BufferPoolManager` entirely** (memtable + SSTables). Reasoning: if the BPM
were the ceiling, LSM should scale better. **It scaled worse** (1×1=31,
8×8=21, 32×32=12). So "BPM is the bottleneck" looked false. **This
inference was too strong** — LSM being worse only means LSM's own path is
worse, not that the BPM is fine for B-tree. (Bonus: LSM runs the full
MVCC+WAL+SQL stack with zero changes — interchangeability validated. LSM's
own bottleneck is its READ path: `MergeIterator::collect_all` materializing
the whole merged result + `LsmTree::scan` blocking on a condvar.)

### 3. Lock manager global mutex — FALSIFIED
`LockManager { state: Mutex<LockState> }` is a single global mutex taken on
every X-lock (every write). Strong structural suspect for a write-heavy
workload. **Profiling killed it: `LockManager` does not appear in the hot
stacks at all** (6 samples). Glad we didn't shard it. Also: ~350
lock-acquires/sec can't cap a mutex that handles millions/sec.

### 4. Eviction policy — NOT a lever here
Harness defaulted to FIFO. Added `--policy`/`--pool-size`. Sweep at 8×8:
- Large pool (8192 frames/32 MB): all ~58–61 txn/s — **working set fits →
  ~0 evictions → policy irrelevant.**
- Small pool (256 frames/1 MB, forced pressure): fifo 51, clock 52, **arc
  46**, lru 44 — ARC is *slightly worse*. TPC-C here is per-access-cost-
  bound, not miss-bound, so ARC's richer bookkeeping costs more than its
  hit-rate edge buys.

### 5. Get-path allocation — correct but WRONG PATH
Profiler showed `decode_leaf_node` allocating a `Vec` per key + per value
(~400 allocs to decode one leaf). Added `lookup_in_encoded_leaf`
(`page_layout.rs`) — point lookup directly on the encoded page, 1 alloc
instead of ~400; routed `BTree::get`/`contains` through it (committed,
tested, no regression). **But it didn't move TPC-C or read-only.** Two
reasons: (a) **wrong code path** — TPC-C SQL reads go through SCANS
(`SeqScan`→`mvcc_scan`→`BTreeScanIterator`→`decode_leaf_node`), never
`BTree::get` (`PkLookup` is never emitted; MVCC reads scan the version
chain). The hot `decode_leaf_node` allocation is the *scan iterator's*. (b)
**wrong bottleneck class** — the write mix is wait-bound, not CPU-bound.
Lesson: **attribute profiler leaf samples to their caller before
optimizing.**

### 6. BPM replacer latch — REAL, but not a silver bullet
`handle_cache_hit` (every page hit) takes the global `replacer` Mutex
(`record_access` + `set_evictable`). Profiling showed the contended
`raw_mutex::lock_slow` sits under `fetch_page`/`unpin_page`, and ~49k
`swtch_pri` samples were threads spinning on it. **Experiment** (dropped the
replacer lock from `handle_cache_hit` + `unpin`, since `evict_page`
re-checks pin_count under `pt.write` per Q-27): `swtch_pri` 49k→8.5k (spin
~6× lower), throughput +5% (8×8) to **+17% (32×32)** — gain grows with
concurrency, as contention theory predicts. **But** it only exposed the
next layers: re-profile shows **allocation now #1 (~47k, in the SCAN path)**
and the commit/fsync wait (`cvwait` 19k + `fcntl` 6.7k). Reverted — see the
design tension below.

---

## Profiles (8×8, leaf-of-stack)

**B-tree (before any change):**
```
swtch_pri        49199   (spin on contended mutex → BPM replacer)
__ulock_wait     11919   (mutex parking)
nanov2/malloc    ~25000  (allocation — page decode in the scan path)
__psynch_cvwait   4856   (commit/durability wait)
__fcntl           3037   (fsync — minor)
```
Hot subsystems: `btree::tree` 2690, `BufferPoolManager` 2363,
`decode_leaf_node` 1035, `mvcc_scan` 874.

**B-tree (after removing the replacer latch — shows the *next* layer):**
```
__psynch_cvwait  19287   (commit/durability wait — now visible)
__ulock_wait     13330
nanov2/malloc    ~47000  (allocation — now #1, the SCAN path)
swtch_pri         8537   (was 49199 — replacer spin removed)
__fcntl           6679
```

**LSM (8×8):** dominated by `__psynch_cvwait` 41k, from the READ path
(`LsmTree::scan` 1371, `MergeIterator::collect_all` 660) — NOT `sync_to`
(16) or `LockManager` (6).

**Correction (2026-06-15, measured).** The earlier reading — "LSM's killer is
reads" — was too strong. Those `cvwait` samples are readers *parked* behind
the memtable flush, which held the global `inner` lock across its SSTable
write + manifest fsync (two fsyncs + a multi-MB write). Moving the flush
off-lock (the immutable-memtable handoff, now landed) removes that park — and
a drift-controlled A/B showed **+0% full-mix throughput** (LSM 22→25 txn/s,
inside run noise; B-tree control flat). The contention is real but not the
bottleneck: full-mix is **commit/disk-floored**, so removing a read-side stall
just leaves readers waiting on the commit path — only the flush's read *tail
latency* improves (~22 ms → ~0), which the throughput harness doesn't see.

**Unified conclusion:** below the commit/disk floor TPC-C is
**read-path-bound**, differently per engine — B-tree pays in page-decode
allocation + BPM-latch contention, LSM in merge-iterator materialization +
(now removed) the flush-lock read stall.

---

## Ranked levers + design tensions (for the future refactor)

1. **BPM replacer latch on every page hit.** Confirmed +5–17%. The fix —
   take the global lock off the pin/unpin hot path — **conflicts with the
   push-based, pluggable `EvictionPolicy`**: the hot path must `record_access`
   into shared policy state, which is the mutex. Two real forms:
   - *Built-in CLOCK (small):* per-frame atomic reference bit + clock-hand
     eviction; lock-free hot path. **Removes pluggable-policy-governed
     eviction** (`--policy` becomes a no-op, `swap_policy` vestigial,
     policy-specific BPM eviction tests change). Guts the interchangeable-
     replacer feature.
   - *Pull-based refactor (big):* invert `EvictionPolicy` so the hot path
     bumps a lock-free per-frame atomic (e.g. an access-order counter or
     reference bit) and each policy *reads* per-frame state at eviction
     time (eviction is rare → an O(n) scan is fine). Keeps all six policies
     *and* the lock-free path. Multi-day: the trait + all six replacers +
     their tests. **This is the principled path if scalability + the
     interchangeability thesis both matter.**
   - Whichever: keep the Q-27 invariant (`handle_cache_hit` holds `pt.read`
     across the pin; `evict_page` re-checks `pin_count` under `pt.write`).
2. **Scan-path allocation — LANDED (2026-06-15).** `BTreeScanIterator`
   called `decode_leaf_node`, which materialized a `Vec` per key *and* per
   value per leaf (~2N allocs), then cloned each surviving entry *again* into
   the iterator's buffer (~4N allocs/page). `EncodedLeaf` (page_layout.rs) now
   walks the encoded leaf in place — the range-scan analogue of
   `lookup_in_encoded_leaf` — yielding `(&[u8], &[u8])` slices so the scan
   pays only its one owned copy per row (~2N). Drift-controlled A/B on
   `engine_range_scan` (LSM control flat to <1%): **~1.8× faster B-tree range
   scans** (median latency ≈ halved) for any scan spanning more than one leaf;
   ~1.1× for a single leaf; the ratio plateaus with length (a constant
   per-page fraction removed). The iterator's `Item = Result<(Vec, Vec)>` is
   unchanged, so `mvcc_scan` and the executor were untouched. Full zero-copy
   to the executor (a lending iterator holding the page guard) is still
   possible but needs the consumer rewritten — the in-place walk captured the
   allocation win without it.
3. **Commit/durability wait** — the `cvwait` cost. On macOS the fsync floor
   (~11 ms) caps it; group commit only helps when commits overlap, which
   TPC-C doesn't do much. Real unlock: Linux/NVMe (fsync ~11 ms→<1 ms), or
   better batching / commit pipelining.
4. **`committed_txns` unbounded growth** — insert-only until recovery,
   never pruned (memory leak under sustained load). Prune below the
   checkpoint. Touches MVCC visibility — do carefully. (The read *latency*
   from cloning it was already fixed: `committed_txns_read` returns the
   guard instead of deep-cloning; see `TransactionManager`.)
5. **Predicate pushdown / join-key promotion — no logical optimizer yet.**
   *Plan: `docs/plan-predicate-pushdown.md`.*
   Surfaced by the `--scale big` full mix: **StockLevel** (`SELECT
   COUNT(DISTINCT s_i_id) FROM order_line, stock WHERE ol_o_id = $1 AND s_i_id =
   ol_i_id AND s_quantity < $2`) plans as a **cross-product NLJ then Filter**.
   Comma joins bind to `on: None` (`logical.rs`), so the equi-condition
   `s_i_id = ol_i_id` lands in the residual WHERE, and `plan_select` dumps the
   whole conjunction as one `Filter` *on top of* the join (`planner.rs`,
   `if let Some(pred) = residual_filter`). Result: it materializes
   |order_line|×|stock| tuples (~32k×3.2k = 100M at `big`, *tens of seconds*)
   then keeps a handful. The fix is two layers, in order of value:
   - **(a) Predicate pushdown — the real win, currently ABSENT, planner-
     agnostic.** Split the conjunctive `filter` in `plan_select`
     (`src/sql/planner.rs`): route single-table conjuncts to the base scan
     (`ol_o_id = $1` onto order_line → shrinks the outer from 32k to ~5 rows;
     `s_quantity < $2` onto stock) and **promote equi conjuncts to the join
     `on`** (`s_i_id = ol_i_id` becomes the join key). Both planners share this
     — it's structure, not algorithm. Canonically a `LogicalPlan→LogicalPlan`
     pass before physical planning (the optimizer that doesn't exist yet; the
     Cascades home, Phase 17/18). The narrowing also fixes the `outer_card`
     under-estimate (`planner.rs` NOTE at the join loop) that biases the cost
     model.
   - **(b) HashJoin selection — secondary, partly done.** Once (a) makes
     `s_i_id = ol_i_id` the join `on`, `JoinSelection::CostBased` (Selinger)
     *already* picks `HashJoin` via `choose_join_algorithm`
     (`src/sql/join_order.rs`) — today it can't, because `extract_equi_join_keys`
     sees `on: None`. `JoinSelection::Heuristic` says "never HashJoin"
     (`planner.rs`), so it needs one rule: equi-`on` + no usable inner index →
     `HashJoin` instead of `NestedLoopJoin`. The `HashJoin` *operator* already
     exists (`src/execution/join.rs`, built in `build.rs`) — no new operator.

---

## What landed (committed, real wins)

- **WAL group commit** done right (flush-under-lock, fsync-unlocked) +
  `sync_data` + `fsync_count` observability + `tests/wal_group_commit_test.rs`.
- **`committed_txns` read-guard** (no per-read deep clone) and **fdatasync**.
- **`lookup_in_encoded_leaf`** — allocation-free `BTree::get`/`contains`
  (correct + tested; not the TPC-C hot path, but a genuine improvement for
  direct-KV `get`).
- **The harness** (`src/bin/tpcc.rs`) with engine / policy / pool-size /
  read-only knobs — the durable instrument for re-measuring any future
  change against both engines instantly.
- **B-tree scan-path zero-copy** (`EncodedLeaf`) — **~1.8× faster range
  scans** (lever #2 above): an allocation-free in-place leaf walk mirroring
  `lookup_in_encoded_leaf`, no change to the iterator's interface. It also
  **lifts B-tree full-mix TPC-C ~+14%** (5-rep drift-controlled A/B mean, 4/5
  reps up; LSM control flat) — the gain comes from the scan-heavy
  `StockLevel`/`Delivery` txns, *not* the commit-bound NewOrder/Payment
  majority. (Read-only TPC-C stays flat: its only txn, OrderStatus, scans a
  single order's ~10 lines — one leaf — (b)'s weakest case.)
- **Lazy streaming scans** (2026-06-17, uncommitted at time of writing) —
  relaxed the vestigial `ScanIterator: DoubleEndedIterator` bound, so
  `BTreeEngine::scan_range` returns its lazy iterator directly (no eager
  `collect()`, O(n) probe gone) and a new streaming `MvccScan` owns its
  snapshot. **Full-mix B-tree TPC-C ~1.9× (8×8: ~60 → ~116 txn/s) via commit
  clustering** (drift-controlled HEAD-vs-current A/B); read-only scan path 2.6×,
  push `LIMIT` early-stop ~270×, PK point-lookup 2.4×. See the headline section
  at the top. Plus `--scale {smoke|big}` and `--pk-probe` harness knobs and a
  transactional bulk loader.
- **LSM flush off the `inner` lock** (immutable-memtable handoff +
  `flush_lock` + manifest in its own `Mutex`) — the canonical LSM design;
  removes a global lock held across two fsyncs + a multi-MB write and wires up
  the previously-dead `immutable_memtables` read branch. **Full-mix
  throughput: +0 here** (commit/disk-floored, A/B-confirmed); the win is read
  tail-latency during flush + correctness on faster-fsync / more-core hardware.

## What to do first if resuming

Lever #2 (scan-path zero-copy) is **done**. The remaining read-side lever is
#1 — the BPM replacer latch — via the **pull-based `EvictionPolicy` refactor**
(keeps the interchangeability thesis). Refined picture from this session's
A/Bs: the full mix splits into a **commit/disk-floored majority**
(NewOrder/Payment, ~88%, capped by the ~11 ms fsync) and a **scan-bound
minority** (StockLevel/Delivery/OrderStatus, ~12%). Read-side levers (#1, #2)
can't touch the floored majority but *can* move the scan-bound minority — which
is how (b) lifted B-tree full-mix (the +14% in *What landed*). So a read-side
lever moves the headline only as far as the mix has scan-bound headroom; the
floored majority needs the commit path (group commit / faster fsync) or better
hardware (Linux/NVMe). LSM saw no such lift — its full-mix is the floored
majority plus its own commit cost, and (a) is commit-neutral. Pick the lever to
match the metric *and* which part of the mix it bottlenecks. Always re-measure
with a **drift-controlled A/B** (old vs new binary, back-to-back, with a
control): single-run baselines here carry ~20–28% thermal drift, and even a
5-rep mean was needed to pull (b)'s signal out of LSM full-mix's ±35% per-run
swing. Every structural guess in this investigation that *wasn't* measured that
way was wrong.
