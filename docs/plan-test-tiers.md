# Plan: test backend tiers — fsync out of the inner loop, structurally

**Status:** plan for review. **Companion:** `docs/plan-build-times.md`
(numbers), `docs/build-times/production-practice.md` (evidence base).

## Why (hygiene and credibility)

The integration suite runs 360 s on a SATA disk and 29 s on a RAM disk —
measured; the entire gap is `fsync`. Production Rust databases do not make
the disk faster; they keep real fsync out of the default tier *by design*
(Neon: `LOCAL_FS`/mock-S3 default, real S3 as a thin tier; redb: crash
tests inject `io::Error` in process; FDB: mock everything). A suite that
pays full durability on every planner test is not more rigorous — it is
slower, machine-dependent, and hides its actual durability tier inside 79
undifferentiated files. Naming the tiers is the hygiene; matching
production practice through a seam the codebase already owns is the
credibility.

## The audit (2026-07)

Every suite classified by the I/O path it actually pays for. Three paths
exist, with different seams:

| I/O path | Cost driver | Seam | Status |
| --- | --- | --- | --- |
| BPM/BTree pages | `DiskManager::write_page` + sync | `DiskManager` trait — `MemoryDiskManager` exists | Ready |
| WAL segments | `sync_data` per commit (`src/wal/writer.rs:164`) | **None — always syncs** | Needs a sync-mode option (T2) |
| LSM SSTables | own `std::fs` writes + sync on flush | None | Measure first; likely acceptable (batched) |

Classification counts (88 suites; full table reproducible via the audit
script — markers: FileDM/MemDM/LSM/DbOpen per file):

- **No I/O (13)** — binder/planner/decode/lock/deadlock suites. No change.
- **Pure FileDM, not durability-subject (28)** — btree_*, buffer_pool_*,
  the join/sort/aggregate operators, planner_* with catalogs,
  goldenscript_bpm/btree, txn_engine, btree_scale (stress). → **T1:
  `MemoryDiskManager`.** No src change; coverage of the file backend is
  held by `disk_manager_conformance` + the `for_each_disk!` matrix, which
  is the argument that makes this a zero-coverage-loss move.
- **FileDM + Database::open, not durability-subject (20 it + 2 stress)** —
  acid (part), sql_e2e, txn_*, mvcc_*, hermitage, session/planner_three_way,
  tpcc_transactions, gc, deterministic, prepared_statement, predicate/push,
  selinger, analyze, concurrency_stress, txn_stress. → **T2: MemoryDiskManager
  + WAL `SyncMode::NoSync`.** Requires the new seam.
- **Durability tier (stays fully real, deliberately, ~12)** — wal_test,
  wal_group_commit, wal_stress, crash_recovery, torn_page_recovery,
  dst_recovery, checkpoint_prune, index_persistence, index_engine_reopen,
  stats_persistence, soak, fault_injection (already testkit-fault-driven);
  plus per-test exceptions: `acid_test::durability_100_crash_cycles`,
  `mvcc_si_conflict_test::concurrent_counter_increments_no_lost_updates`
  (timing-sensitive; real disk is its stable home). **This tier is the
  named, documented home of every fsync-semantics claim the project
  makes.**
- **LSM suites (~14, incl. combos)** — lsm_engine, lsm_atomic_flush,
  goldenscript_lsm, cross_engine_differential, database/catalog matrix
  suites, near_limit, large_scale. → **T3: decide on the timing data.**
  LSM buffers through a 4 MB memtable, so per-op fsync is not its pattern;
  if measurement shows these suites are cheap, they stay untouched and the
  decision is recorded. An LSM I/O seam is NOT in scope.
- **Conformance/matrix + shuttle (6)** — untouched on principle: they are
  the coverage anchor (every disk backend, every engine, every policy).

### Measured per-suite wall time (2026-07, sequential per-suite runs; ~0.4 s
cargo overhead each; single-suite runs lose cross-suite thread parallelism,
so treat as directional)

Top offenders, with their tier assignment:

| Suite | real | cpu (user+sys) | Tier → action |
| --- | --- | --- | --- |
| it::proptest_test | **200.4 s** | 19.6 s | **T2** — WAL+FileDM proptest; ~180 s is fsync wait; the single biggest win |
| stress::large_scale_test | 101.5 s | 15.5 s | T1/T3 — FileDM+LSM (1M cases already ignored) |
| stress::concurrency_stress_test | 91.5 s | 1.8 s | T2 — WAL wait, almost no CPU |
| stress::btree_scale_test | 65.8 s | 3.5 s | T1 — pure FileDM |
| stress::wal_stress_test | 51.7 s | 5.5 s | durability tier — stays |
| it::binder_proptest | 38.5 s | 12.6 s | **no I/O — tier-immune**; post-tier residual, flagged below |
| it::crash_recovery_test | 19.5 s | — | durability tier — stays |
| it::acid_test | 15.5 s | — | T2, with `durability_100_crash_cycles` kept Durable |
| it::deterministic_test | 10.7 s | — | T2 |
| it::config_equivalence | 10.6 s | — | matrix anchor — stays |
| it::mvcc_si_conflict_test | 8.4 s | — | T2 (lost-update test stays real disk) |
| it::cross_engine_differential | 8.1 s | — | T1/T3 (FileDM+LSM) |
| it::btree_scan_test | 7.9 s | — | T1 |
| it::wal_group_commit_test | 6.6 s | — | durability tier — stays |
| it::mvcc_isolation / hermitage / txn_isolation | ~6 s each | — | T2 |
| it::deadlock_test | 5.5 s | — | no I/O — lock-timeout waits, tier-immune |
| ~60 remaining suites | < 3 s each | — | per classification |

Implications baked into the phases:
- **T2 leads with `proptest_test`** — over half the harness in one suite.
- **T3 verdict is already visible**: the pure-LSM suites are cheap
  (lsm_engine 2.3 s, atomic_flush 1.0 s, goldenscript_lsm 0.4 s,
  near_limit 0.7 s) — LSM's memtable batching means no per-op fsync pain.
  Leave untouched; record here.
- **Post-tier residual, named**: binder_proptest (~38 s, pure compute) and
  deadlock_test (~5 s, lock-timeout waits) do not get faster from tiers;
  any change there would alter test logic (case counts, timeouts) and is
  out of scope for this plan.
- The stress harness gains as much as `it` does (btree_scale +
  concurrency_stress ≈ 157 s of convertible wait).

## T2 seam design: `SyncMode` on the WAL

The production pattern (redb `Durability::None`, Postgres `fsync = off`):
keep the entire WAL code path — record encoding, batching, segment
rotation, recovery on clean reopen — and skip only the `sync_data`
syscall.

```rust
/// How the WAL treats the sync syscall on commit. `Durable` is the
/// default everywhere; `NoSync` exists for tests and throwaway
/// environments whose subject is not crash durability. With `NoSync`,
/// recovery after a *process kill* is undefined (buffered data may be
/// lost); recovery after clean shutdown is unchanged.
pub enum SyncMode {
    Durable,
    NoSync,
}
```

- Carried by `Wal`/`WalWriter`; one branch at the two `sync_data` sites
  (`writer.rs:164`, `mod.rs:143`). `wal_fsync_count` still counts *requested*
  syncs? No — it counts issued syscalls, so `NoSync` reads 0; the
  group-commit test (Durable-only, durability tier) is unaffected.
- Constructor: `Database::open_with_sync_mode(data_dir, engine, SyncMode)`;
  `Database::open` delegates with `Durable`. No default-changing, no config
  sprawl; call sites are explicit per CLAUDE.md ("don't rely on defaults").
- Tests for the seam itself (durability tier): `Durable` issues ≥1 fsync
  per commit batch (via `wal_fsync_count`); `NoSync` issues 0 while
  producing identical query results and surviving clean-shutdown reopen.
- Explicitly NOT: an environment variable, a cargo feature, or a global.
  It is a value passed at open, visible at every call site.

## Phases (each: change → gates → phase doc → one commit)

Gates for every phase: `fmt --check` · `clippy --all-targets -- -D warnings`
· `cargo test` · `cargo test --release` · **test-list identity** (aggregated
`--list` unchanged — conversions change setup, never the test set) · timing
table row.

- **T0 — baseline**: per-suite timing table (in flight), committed into
  this doc. Re-measure only the converted suites per phase.
- **T1 — pure-FileDM conversions (28 suites)**: switch setup helpers to
  `MemoryDiskManager`; drop now-unneeded tempdirs. Mechanical, no src
  change, biggest first per the timing table.
- **T2 — the SyncMode seam + WAL-suite conversions (22 suites)**: land the
  seam with its own tests first (separate commit), then convert suites
  (MemDM + `open_with_sync_mode(.., NoSync)`), with the per-test
  durability exceptions kept `Durable` on real disk.
- **T3 — LSM decision**: read the timing table; either record "cheap,
  untouched" in this doc or scope a follow-up. No silent default.
- **T4 — tier documentation + retirement**: README testing section names
  the tiers (default tier: in-memory + NoSync; durability tier: real disk,
  real fsync, enumerated suites; conformance matrix: all backends);
  `scripts/test-fast.sh` deleted (its job no longer exists — the suite is
  fast on any machine); stress harness reviewed against the same tiers.

## Acceptance (the whole plan)

- `cargo test --test it` on the *real disk* lands near the RAM-disk floor
  (~30 s; target < 60 s) — the RAM disk becomes irrelevant.
- The durability tier is enumerable by name and still exercises real
  fsync end-to-end; `for_each_disk!` still covers `FileDiskManager`.
- Test count and pass/ignore sets identical before/after every phase.
- Full-suite wall time (debug gate) drops from ~10 min toward ~3–4 min
  (residual = stress + durability + LSM, all deliberate).

## Explicitly out of scope

An LSM storage seam; changing the `bench` path; cargo-nextest and
sqllogictest-rs adoption (recorded as later candidates in
`production-practice.md`); any change to what a test asserts.
