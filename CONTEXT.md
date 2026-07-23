# CONTEXT — shared vocabulary

The terms an agent (or new contributor) needs to drop in warm. Definitions
only; the "why" lives in code comments and the docs listed in
[`docs/README.md`](docs/README.md).

## What this is

**InterchangeDB** — a from-scratch Rust database whose subsystems are
swappable *traits* (`StorageEngine`, `EvictionPolicy`, `DiskManager`,
`Planner`, execution model). Built to study how those choices trade off, and
benchmarked against TPC-C / TPC-H.

## Crates (workspace)

The root is both the `interchangedb` facade crate and the workspace root.

| Crate | Holds |
|-------|-------|
| `idb-core` | Contracts + primitives: `StorageEngine`/`DiskManager` traits, `PageId`, `Error`, the `sync` shim (parking_lot ↔ shuttle). |
| `idb-storage` | Buffer pool, eviction policies, the two storage engines, page/disk formats. |
| `idb-wal` | Write-ahead log, segments, recovery, `SyncMode`. |
| `idb-txn` | Transactions, lock manager, `TransactionManager`. |
| `idb-sql` | Catalog, tables, the SQL pipeline (parse → bind → plan → execute), planners, executors. |
| `interchangedb` (root) | Facade that re-exports the above; `Database`, `Session`, the `tpch`/`tpcc` bins. |
| `testkit` | Dev-dependency: the conformance-matrix registries, workloads, fault injection. |

**Thesis:** `idb-sql`'s `[dependencies]` name only `idb-core` (contracts),
never a storage impl — crate boundaries enforce the seams.

## Storage & buffer pool

- **Storage engines (2):** `BTreeEngine` (B+Tree, variable-length keys/values,
  tombstone deletes) and `LsmEngine` (LSM-tree, memtable + leveled SSTables +
  bloom filters). Both implement `StorageEngine`.
- **Buffer pool (`BufferPoolManager`):** fixed frame pool over a `DiskManager`;
  pages pinned/unpinned, dirty-flushed. `MemoryDiskManager` (RAM) and
  `FileDiskManager` (disk) are the two backends.
- **Eviction policies (6), runtime-swappable via the `EvictionPolicy` trait:**
  `fifo`, `clock`, `lru`, `lru_k`, `two_q` (2Q), `arc`. The replacer mutex is
  the documented per-page-hit hot-path.

## Durability & concurrency

- **WAL:** append record → sync → apply; `SyncMode::Durable` (real fsync) vs
  `NoSync` (tests). Recovery replays from the last checkpoint.
- **Transactions:** strict two-phase locking (2PL) + a lock manager with
  deadlock detection.
- **MVCC:** snapshot isolation over versioned keys; a timestamp oracle issues
  snapshots. **GC** purges versions below the watermark.
- All `Database` methods take `&self`; concurrency is `Arc<Database>`.

## SQL layer

- **Pipeline:** `Session::execute` runs parse → **bind** (name/type
  resolution) → **plan** (logical → physical) → **execute**.
- **Planners (3), the `Planner` enum:** `RuleBased` (heuristic, left-deep,
  FROM-order; default), `Selinger` (System-R DP, cost-based on `ANALYZE`
  stats), `VolcanoMemo` (top-down Cascades-style memo). All share the
  join-algorithm heuristic: INLJ when the inner is indexed on the key,
  HashJoin for an equi-key without an index, else NLJ.
- **Execution models (2), the `ExecModel` enum:** `Volcano` (pull / iterator,
  default) and `Push` (data-driven, native HashAggregate sink).
- **The matrix:** 3 planners × 2 executors = **6 configs**. They are proven to
  produce *identical answers*; only plan shape and speed vary.
- **Indexes:** primary key + secondary (`CREATE INDEX`, with backfill). An
  equality on an indexed column lowers to an `IndexScan`/`PkLookup` seek with
  an MVCC recheck Filter (secondary indexes are unversioned).

## Testing

- **testkit conformance matrix** — write a contract/property *once*, run it
  across every config via registries: `for_each_policy!`, `for_each_disk!`,
  `for_each_engine!`, `for_each_isolation!`. A new impl is one registry line →
  it inherits the whole suite. See [`testkit/README.md`](testkit/README.md).
- **Three shapes:** *conformance* (each config meets a contract), *equivalence*
  (configs produce identical results), *head-to-head bench*.
- **Test tiers:** Default (in-memory + `NoSync`, most tests), Durability (real
  files + fsync, crash/recovery subjects), Conformance matrix (all backends).
- **Gates (mirror CI):** `cargo test --workspace`, `--release`,
  `cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --all`,
  `cargo doc --workspace`.

## Benchmarks

- **TPC-H** — capability ladder H1…H6 (`docs/plan-tpch.md`); validated against
  DuckDB as oracle at SF 0.01. `cargo run --release --bin tpch`.
- **TPC-C** — throughput (tpmC) harness; `cargo run --release --bin tpcc`.

## Conventions

- Priorities: **Safety → Performance → Developer-Experience** (see the local
  `CLAUDE.md`, which is gitignored).
- Names carry units/qualifiers last (`latency_ms_max`); assertions guard
  positive *and* negative space; loops and resources have explicit bounds.
- `ISSUES.md` is the live quality tracker; `docs/stability.md` the testing
  strategy.
