# What production Rust databases do — test setup and build/test times

Research synthesis (2026-07): Materialize, RisingWave, Databend (OLAP);
TiKV, Neon, sled, redb (storage/KV); plus the FoundationDB lineage and the
tooling landscape. Sources cited inline; gathered from engineering blogs,
repos, and primary docs. Companion to `docs/plan-build-times.md`.

## Where InterchangeDB already matches production practice

| Practice | Who does it | Our version |
| --- | --- | --- |
| One merged integration-test binary | RisingWave (`tests/integration/main.rs`), Databend (literally `tests/it/`) | P2's `it` + `stress` harnesses |
| Dev-profile tuning (line tables, optimized deps, proc-macro override) | Bevy pattern; corrode.dev; TiKV's `debuginfo=1` | P1 |
| Model/differential testing vs a `BTreeMap` reference with shrinking | **The dominant correctness technique**: sled (quickcheck vs BTreeMap), redb (stateful fuzzer vs BTreeMap) | `cross_engine_differential` (proptest + BTreeMap oracle), testkit equivalence |
| Workloads as data, goldens over unit tests | Databend shifted to golden-file/SQL-logic tests explicitly "to cut compile time" | goldenscript, testkit `workload` |
| Schedule-exploration DST for shared-memory races | shuttle (AWS) — same lineage as madsim/FDB, right layer for single-node | `bpm_swap_shuttle`, the Q-30/Q-35 discipline |
| Fault injection behind a test-only feature | fail-rs (TiKV-authored), sled `failpoints`, Neon `--features testing` | testkit `FaultInjectionDiskManager`, DST recovery sweep |
| Small-crate/few-deps counter-strategy | redb: single small-dependency crate so build time never becomes a problem | 9 deps, single crate |

Also validated by the research: skipping sccache locally (it requires
`CARGO_INCREMENTAL=0` — hurts the incremental loop; TiKV additionally found
it defeated by proc-macros), skipping linker swaps on macOS (mold rejects
Mach-O; zld deprecated; Apple's `ld_prime` is the fast path), and the
llvm-lines finding that monomorphization — TiKV's #1 cost at 84% of release
LLVM time — is NOT our problem (top codegen is stdlib/sqlparser; biggest
own entry 0.7%).

## The gap that explains the remaining pain

**Production keeps real fsync out of the inner loop, structurally.**
Neon runs most integration tests against `LOCAL_FS`/mock-S3 and reserves
real S3 for a thin tier. redb tests crash/durability by injecting
`io::Error` after an N-operation countdown *in process* — not by hitting a
real disk. sled injects failures around disk ops. FDB mocks the entire
world. Nobody's inner loop waits on a physical disk.

InterchangeDB has the exact seam this requires — `DiskManager` is a
swappable trait, `MemoryDiskManager` exists, and testkit's `for_each_disk!`
matrix already runs the conformance tier against every backend **including
the real file backend**. But 73 of 79 integration suites construct
`FileDiskManager` out of convenience, which is why the `it` harness runs
360 s on this machine's 2015 SATA disk vs 29 s on RAM (measured 12×).

The key coverage argument: because the conformance matrix already exercises
`FileDiskManager` by construction, per-suite convenience use of the real
disk is *redundant* coverage — switching convenience setups to the memory
backend loses nothing the matrix doesn't already hold, while durability
suites (WAL, recovery, torn-page, group-commit, DST, crash-loop) stay on
real disk because the disk is what they test.

## Evaluated and deliberately not adopted (now)

- **Workspace split** — universal in the big systems, but matklad's analysis
  says dependency *shape* beats crate count, monomorphization duplicates
  per-crate (accidentally-quadratic risk), and on 2 physical cores the
  parallelism ceiling is low. Our inner loop is already 7.8 s. The real
  argument for a split is thesis-testing (crate boundary = enforced seam) —
  its own schedule, not a build-times fix.
- **cargo-nextest** — de-facto standard (TiKV, Neon, RisingWave, Databend);
  published 1.4–3.4× run-time wins from process-per-test parallelism. Two
  caveats for us: the wins concentrate in many-binary workspaces (P2 merged
  ours to 2) and in CPU-parallel suites (ours is disk-serial until the tier
  fix lands). Revisit after the backend tiers; its `archive` build-once
  /run-partitioned mode becomes valuable with a CI fleet.
- **sqllogictest-rs** — the shared SQL harness of RisingWave, Databend,
  DataFusion (one `AsyncDB` impl → declarative `.slt` files; DataFusion:
  data-driven tests "do not require a slow recompile/link cycle"; unlocks
  SQLite's 7.2M-query corpus). Compelling for the TPC-H-era SQL surface;
  premature for today's dialect.
- **cargo-hakari, mold/lld, sccache, CI sharding** — CI/Linux/workspace
  levers; none applies to a single-crate macOS local loop today. All noted
  for the eventual Linux benchmark/CI host.

## Key sources
RisingWave DST + compile-time blogs (madsim; sccache/hakari/machete; CI
40→27 min) · Materialize testing guide + CI blogs (testdrive; LTO off for
tests, −94% build; shard-per-core; "link time dominates") · Databend
rustmagazine compile-times post + sqllogictest migration PR #9150 · TiKV
compile-time series ("Rust Compilation Calamity"; generics = 84% of release
LLVM) + fail-rs + nextest-archive CI pattern · sled model-testing series +
simulation guide · redb fuzz_redb.rs (countdown io::Error FuzzerBackend) ·
Neon test_runner README (pytest tiers, RemoteStorageKind) · nexte.st docs
(benchmarks, partitioning, retries) · Rust 1.90 rust-lld announcement ·
Cargo profiles reference · matklad "Fast Rust Builds".
