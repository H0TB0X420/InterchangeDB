# Tier T2 — the WAL SyncMode seam, and the suites that use it

## The change

**Generic.** When a database's commit path syncs on every write, any test
that opens the database pays physical fsync per operation — and
property-based tests multiply that by hundreds of generated cases. The
production pattern (redb `Durability::None`, Postgres `fsync = off` for
test clusters) is a first-class sync-mode option: keep the entire WAL code
path — record encoding, group-commit batching, segment rotation, recovery
on clean reopen — and skip exactly one thing, the sync syscall. The option
must be an explicit value at the open call site (never an env var, cargo
feature, or global), the default must stay durable everywhere, and the
seam must land with contract tests *before* anything uses it.

**This repo.** Two commits:

1. **The seam** (`SyncMode::{Durable, NoSync}` in `src/wal/mod.rs`):
   `Wal::open` delegates to `open_with_sync_mode(.., Durable)`; the two
   syscall sites (`Wal::sync`, `sync_to`'s group-commit leader arm) branch
   on the mode — under `NoSync` the buffer still flushes to the OS and
   `synced_lsn` still advances (committers must never spin waiting for
   durability the mode deliberately doesn't provide), only `sync_data` and
   its `fsync_count` accounting are skipped. `Database::open_with_sync_mode`
   threads it through one private `open_inner`. Contract tests in the
   durability tier: Durable issues syscalls / NoSync issues exactly zero
   with identical results; NoSync-era WAL records replay identically on a
   Durable reopen after clean shutdown.
2. **The conversions**: 20 suites whose subject is not crash durability
   moved to `open_with_sync_mode(.., NoSync)` + `MemoryDiskManager`
   engines, led by `proptest_test` (200 s → 39 s alone); plus
   `large_scale_test`'s BTree half to memory (101.5 s → 5.2 s), a
   T1-class conversion the first sweep missed.

   Remaining `it` time (147 s) decomposes honestly: ~77 s is the two
   compute-bound proptest suites (tier-immune), ~50 s the durability
   tier doing its real-fsync job, ~17 s the matrix anchors. The plan's
   <60 s target undercounted the compute-bound share; the fsync goal
   itself is met — the disk no longer gates any convertible suite.

## Judgment calls (the audit's screen, then human eyes)

- **`acid_test` stays fully Durable+real-disk** — the screen showed its
  crash tests reopen through the shared helper; recovery *is* its subject.
  The plan originally had it converting with one exception; inspection
  reversed that. Whole-file durability tier.
- **`txn_database_test` stays** — 8 opens, 28 crash/reopen markers, and at
  1.2 s conversion buys nothing worth the risk.
- **`proptest_test::lsm_state_survives_reopen`** keeps real files —
  SSTable/manifest survival is a durability property (it also uses
  `LsmTree::open` directly, so the conversion couldn't touch it by
  construction).
- **`mvcc_si_conflict_test::concurrent_counter_increments_no_lost_updates`**
  keeps a dedicated Durable+real-disk setup with a comment: it is
  timing-sensitive under contention (one unproven sum-below-800 observed
  on fast storage) and must not move tiers without shuttle-grade evidence.

## Gains

| Measurement | Baseline | Post-T1 | Post-T2 |
| --- | --- | --- | --- |
| `it::proptest_test` | 200.4 s | 200.4 s | **39.0 s** |
| `it` harness run | 364 s | 315 s | **147.5 s** |
| `stress` harness run | 224 s | 169 s | **90.7 s** |

## Draws

- `NoSync` is a real durability hole by design: a *process kill* (not a
  clean drop) can lose OS-buffered records. That is why it is spelled at
  every call site and why the durability tier never uses it.
- The converted suites no longer exercise the `sync_data` path — that
  coverage now lives exclusively in the durability tier (wal, group-commit,
  crash-recovery, acid, dst, torn-page, soak) plus the seam's own contract
  tests, which is the named, greppable arrangement the tier plan promised.
- One more mode exists. The defense against drift is that `Durable` is the
  default on every public constructor and `NoSync` appears only in test
  setups — a `grep NoSync src/` returning only the seam itself is the
  hygiene check.
