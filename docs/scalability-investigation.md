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
box, not a regression.

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

## Tooling built for this (kept in the repo)

- **`src/bin/tpcc.rs`** — standalone harness. Flags:
  - `--engine btree|lsm` — runs the *same* workload through either
    `StorageEngine` (B+Tree uses the buffer pool; LSM bypasses it).
  - `--policy fifo|lru|lruk|clock|2q|arc` + `--pool-size N` — eviction
    policy + buffer-pool frames (B+Tree only).
  - `--read-only` — diagnostic: runs only OrderStatus (pure MVCC reads, no
    write locks, read-only commit skips the WAL). Isolates read- vs
    write-path scaling.
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
