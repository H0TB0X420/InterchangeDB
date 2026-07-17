# Tier T1 — convenience suites move to the in-memory backend

## The change

**Generic.** Tests that construct a real on-disk backend out of setup
convenience — not because the disk is their subject — pay physical fsync
on every operation and quietly blur the question "where is durability
actually tested?" The production pattern (Neon, redb, FDB; see
`production-practice.md`) is a default tier on memory-backed storage and a
small, *named* tier on real disk. The conversion is only safe when a
conformance matrix keeps exercising the real backend independently — then
per-suite real-disk usage is provably redundant coverage.

**This repo.** 30 files switched their `DiskManager` from
`FileDiskManager` to `MemoryDiskManager` (constructor swap + dead
tempdir/path/import removal; stale comments updated). Deliberately NOT
converted, with reasons in place:

- `goldenscript_bpm` — its scripts have a `reopen` command (open the same
  file again); reopen-capable harnesses are durability-tier by design.
- `buffer_pool_integration::test_flush_and_reload` — two real sessions
  over one file; the subject is survival across close/reopen. The file's
  *helper* converted; this test keeps `FileDiskManager` with a comment.
- The durability tier, the testkit matrix suites, and all
  `Database::open` (WAL) suites — the latter are T2's territory.

## Proof nothing changed

- Test-set identity: debug 5 suites 1335/0, release 1333/0 — the same
  totals as before the conversion, zero failures.
- `for_each_disk!` conformance still runs every backend including
  `FileDiskManager`; the file backend's coverage lives there by
  construction, not in convenience setups.
- fmt ✓ · clippy `-D warnings` ✓ (caught three `let_and_return` shapes the
  cleanup introduced, plus one stale doc-comment via the conversion
  script's own assertion — both fixed).

## Gains

| Measurement | Before | After T1 |
| --- | --- | --- |
| `stress` harness run | 224 s | **169 s** |
| `stress::btree_scale_test` | 65.8 s | **1.6 s** (41×) |
| `it::cross_engine_differential` | 8.1 s | 0.5 s |
| `it::btree_scan_test` (test time) | ~7.5 s | 0.06 s |
| `it` harness run | 364 s | 315 s |

The modest `it` delta is expected and predicted by the timing table: the
harness's mass is `proptest_test` (200 s) plus other `Database::open`
suites — all WAL-fsync-bound, all T2. T1's targets were the pure-BPM/BTree
suites, and those dropped 10–100×.

## Draws

- Converted suites no longer touch `FileDiskManager` themselves; anyone
  auditing "what tests the file backend" must know the answer is the
  conformance matrix + the durability tier. (That is the point — but it is
  a convention a newcomer must learn, now written down here and in the
  tier plan.)
- Two reopen-style tests required human judgment to exclude; a purely
  mechanical sweep would have silently converted them into tests of
  nothing. Future test authors adding reopen semantics to a converted
  suite must move it back to real files.
