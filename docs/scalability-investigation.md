# TPC-C Scalability Investigation

Findings from a measure-driven performance investigation of the TPC-C
workload (2026-06). Captured here so a future scalability refactor has the
full picture without re-deriving it. **The headline: it's a multi-front
scaling problem with no single bottleneck, and several leading hypotheses
were falsified by measurement — read the log before optimizing.**

Environment: macOS (APFS, fsync ~11 ms), single node, smoke-scale data.
The absolute numbers are OS/hardware-bound; the *shape* (what scales, what
doesn't, and why) is what generalizes.

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
(16) or `LockManager` (6). LSM's killer is reads, not commit or locks.

**Unified conclusion:** TPC-C is **read-path-bound for both engines**
(several reads/txn), differently. B-tree pays in page-decode allocation +
BPM-latch contention; LSM pays in merge-iterator materialization + a
read-path condvar.

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
2. **Scan-path allocation** — once the BPM latch is gone, this is #1
   (~47k samples). `BTreeScanIterator`/`decode_leaf_node` materialize a
   `Vec` per row/leaf. Fix: lazy / zero-copy iteration (yield slices into
   the page guard rather than owned `Vec`s), threaded through `mvcc_scan`
   and the executor. Harder than the get-path fix (multi-row, lifetimes,
   MVCC version chain). The committed `lookup_in_encoded_leaf` is the
   pattern to mirror for the scan path.
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

## What to do first if resuming

Re-profile to confirm nothing drifted, then take lever #1 via the
**pull-based `EvictionPolicy` refactor** (keeps the thesis), then lever #2
(scan-path zero-copy). Expect incremental gains; the absolute ceiling
won't break without Linux/NVMe for the commit path. Always re-measure with
the harness (`--engine`, `--read-only`) after each change — every
structural guess in this investigation that *wasn't* measured was wrong.
