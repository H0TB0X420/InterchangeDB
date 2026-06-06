# Interchangeable Seams

The trait map for InterchangeDB. Each trait is a *seam* — a point where one
implementation can be swapped for another that satisfies the same contract.
This is the living index of what's interchangeable and what each seam should
have behind it.

**Guiding principle:** a trait with one implementation is a hypothesis, not a
proven seam. Every trait here targets **≥2 real implementations**; until it
has them, the abstraction is unvalidated.

Legend: `(built)` exists in code · `(planned)` designed, not yet built ·
`(stretch)` aspirational / research.

Validation status at a glance:
- Validated (2+ impls): `DiskManager`, `EvictionPolicy`, `StorageEngine`
- Hypothesis (1 impl, needs a second): `ConcurrencyControl`, `CommitProtocol`
- Not yet a trait (extract it): `Optimizer`, `CostModel`, `StatsProvider`,
  `ExecutionModel`, `QueryEngine`, `SecondaryIndex`

---

## 1. Disk I/O — `DiskManager`

Raw page read/write/allocate/sync. The lowest seam; the one that makes
deterministic simulation testing possible.

- **FileDiskManager** (built) — single-file, real `fsync` per write.
- **MemoryDiskManager** (built) — in-RAM page array; tests and simulation.
- **FaultInjectionDiskManager** (built) — torn writes, dropped/reordered
  flushes, crash-at-LSN; the engine behind crash-recovery torture tests.
- **IoUringDiskManager** (planned) — Linux `io_uring`, real async + batched
  submission; the principled "async storage" win, kept behind this seam.
- **DirectIoDiskManager** (stretch) — `O_DIRECT`, bypass the OS page cache for
  predictable I/O accounting.

## 2. Buffer pool — `EvictionPolicy`

Chooses which frame to evict. The marquee *runtime*-swappable seam, with warm
state transfer on swap.

- **FIFO** (built) — evict oldest-loaded; baseline.
- **CLOCK** (built) — second-chance approximation of LRU.
- **LRU** (built) — least-recently-used.
- **LRU-K** (built) — evict by backward K-distance; scan-resistant.
- **2Q** (built) — A1in/A1out/Am queues; scan-resistant.
- **ARC** (built) — adaptive balance of recency and frequency.
- **Random / MRU** (stretch) — degenerate baselines for the comparison paper.
- **LIRS** (stretch) — low inter-reference recency set; another research point.

> Note: the **pull-based refactor** (lever #1) changes this trait's *shape* —
> the hot path bumps a lock-free per-frame atomic, and each policy *reads*
> per-frame state at eviction time. All six impls above migrate to the new
> contract; that migration is the work, and it's what keeps the seam *and*
> removes the latch.

## 3. Storage engine — `StorageEngine`

Key/value get/put/delete/scan. Compile-time swap (generic `E: StorageEngine`).

- **BTreeEngine** (built) — B+Tree over the buffer pool; read-optimized.
- **LsmEngine** (built) — LSM-tree; write-optimized, bypasses the buffer pool.
- **InMemoryEngine** (planned) — skiplist/hashmap, no pages, no buffer pool;
  the first rung of the "approach in-memory SOTA" ladder.
- **FractalTreeEngine / Bε-tree** (stretch) — buffered, write-optimized
  B-tree; the novel-structure swap point.

## 4. Secondary indexing — `SecondaryIndex`

Non-primary-key access paths. Currently coupled to the catalog's
`IndexBackend`; extract into its own seam.

- **BTreeIndex** (built, as `IndexBackend::BTree`) — ordered secondary index.
- **HashIndex** (planned) — point-lookup-only, no range support.
- **LsmIndex** (stretch) — write-optimized secondary index.

## 5. Concurrency control — `ConcurrencyControl`

How transactions detect conflicts and provide an isolation level. **Not yet a
trait** — currently hard-wired MVCC. Extract it; this is the seam that makes
the isolation-level and OLTP-speed experiments possible.

- **MVCC + Snapshot Isolation** (built) — current; write skew permitted by
  design.
- **SSI** (planned) — MVCC + rw-antidependency cycle detection; the one
  feature that closes the gap to full serializability.
- **2PL** (planned) — lock-based serializability; the comparison baseline.
- **OCC** (planned) — Silo-style optimistic; avoids shared writes on reads,
  the high-core-count scaling play.
- **PartitionedSerial** (stretch) — H-Store style, one thread per partition,
  no in-partition concurrency control; the TPC-C-by-warehouse play.

## 6. Commit / durability — `CommitProtocol`

How durability is achieved at commit time. Currently fused into the WAL;
extract the policy.

- **GroupCommit** (built) — leader/follower batched fsync; helps when commits
  overlap.
- **EpochCommit** (planned) — Silo-style; persist one epoch at a time,
  amortize fsync across thousands of txns off the critical path.
- **AsyncCommit** (stretch) — acknowledge before durability with bounded loss
  window; for benchmarking the durability/throughput trade.

## 7. Query optimizer — `Optimizer`

Logical → physical plan selection. The four-stage progression is the portfolio
centerpiece; extract the trait so they're swappable and comparable.

- **Heuristic** (built/early) — rule-based rewrites, no cost.
- **SystemR / Selinger** (planned) — dynamic-programming join ordering.
- **Volcano** (planned) — top-down cost-based search.
- **Cascades** (planned) — memo + rules; the differentiator.

## 8. Cost model — `CostModel`

Estimates plan cost for the optimizer. **Not yet a trait.**

- **Heuristic cost** (planned) — simple cardinality/IO formulas.
- **Calibrated cost** (planned) — coefficients tied to measured per-engine
  profiles (addresses the known storage/optimizer cost-coupling leak).
- **Learned cost** (stretch) — trained on `workload_log`; the V3 adaptive
  thesis.

## 9. Statistics — `StatsProvider`

Feeds the cost model. **Not yet a trait.**

- **NoStats** (planned) — fixed guesses; baseline.
- **HistogramStats** (planned) — per-column histograms.
- **SamplingStats** (stretch) — runtime sampling / sketches.

## 10. Execution model — `ExecutionModel`

How operators produce rows. **Not yet a trait.**

- **Volcano / iterator** (built) — row-at-a-time pull (`next()`).
- **Vectorized** (planned) — batch-at-a-time; the OLAP / ClickBench path.
- **Compiled / push-based** (stretch) — codegen per query plan.

## 11. Whole query engine — `QueryEngine`

Coarse seam: SQL string → result set. The substitutability claim at the
largest grain. **Not yet a trait.**

- **NativeEngine** (built) — `parse → bind → plan → execute`.
- **DataFusionEngine** (planned) — mounts IDB storage via `TableProvider`;
  external industrial-strength oracle and columnar baseline (feature-gated).

---

## Build order (dependency-aware)

1. Extract `ConcurrencyControl` from the hard-wired MVCC (unblocks SSI/OCC/2PL).
2. Extract `Optimizer` + `CostModel` + `StatsProvider` together (the query
   triad; needed before the four-optimizer progression).
3. `InMemoryEngine` behind `StorageEngine` (first speed-ladder rung).
4. `EpochCommit` behind `CommitProtocol`.
5. `ExecutionModel` extraction, then `Vectorized`.
6. `QueryEngine` extraction, then `DataFusionEngine`.

Each extraction is only "done" when a second implementation rides the same
trait and a differential harness proves they agree.
