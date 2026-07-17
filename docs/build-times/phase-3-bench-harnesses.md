# Phase 3 — bench consolidation (and what the inventory actually found)

## The change

**Generic.** The same binary-count multiplier as Phase 2 applies to
`benches/` — but only to targets that share a harness. Criterion benches
merge freely because their result paths are keyed by group and function id
strings in the code, not by binary name: move the file, keep the ids, and
saved baselines plus every doc reference stay valid. Binaries with *custom
mains* do not merge mechanically — combining them means writing an argument
dispatcher, which changes the invocation contract. A consolidation pass
must inventory which is which before believing its own grouping table.

**This repo — the finding first.** The grouping table (inherited from the
discussion doc) assumed 14 criterion benches. The inventory says otherwise:
only 6 are plain criterion binaries. The seven `engine_*` files are
custom-main *experiment harnesses* (CSV/markdown writers behind a
`cargo bench --bench` invocation), and `eviction_policies` has a bespoke
dual-mode main (`-- summary` prints hit-rate/survival tables — referenced
from local notes). Merging any of those is a CLI redesign, which the
session rules reserve for review — **stopped and flagged, not done**.

What did merge, mechanically and id-preserving:

- `engine_bench` ← `btree_bench` + `lsm_bench`
- `sql_bench` ← `push_vs_volcano` + `txn_bench` + `config_matrix`
- unchanged: `bpm_bench`, `eviction_policies`, the 7 `engine_*` harnesses

Per-file edits were exactly: drop the `criterion_main!` line and its
now-unused import; each harness `main.rs` declares the modules and one
`criterion_main!` over their groups. 14 binaries → 11.

## Proof nothing changed

- Benchmark-id identity: `-- --list` before vs after is an **exact diff
  (29 = 29)** — every group and function id byte-identical, so
  `target/criterion/` histories and `benches/RESULTS.md` references remain
  valid.
- No doc or CI invocation referenced the five merged binary names.
- All four gates green.

## Gains

| Measurement | Before P3 | After P3 |
| --- | --- | --- |
| `cargo bench --no-run` after 1-line `src/sql/` edit | 152 s | 182 s wall / same ~460 s user — no link win, wall delta is CPU-parallelism variance (thermal); codegen-bound as Phase 0 predicted |
| Bench binaries linked | 14 | 11 |

Expected to be modest: Phase 0 showed the bench path is codegen-bound
(optimized profile, 316 % CPU), not link-bound, and the three custom-main
harness classes — the majority of the binaries — are out of mechanical
reach. The real payoff is the `--all-targets` pre-commit path and three
fewer relinks per src edit.

## Draws

- **Invocation names changed for five benches:**
  `cargo bench --bench txn_bench` is now
  `cargo bench --bench sql_bench -- <filter>`; same for the other four.
  Criterion name-filters still select the same benchmarks.
- **The 14→3 goal was not reached, deliberately.** Getting to 3 requires
  dispatcher mains for `eviction_policies` + the 7 experiment harnesses —
  a design decision (flag: worth it only if the pre-commit bench build
  time still hurts after this phase; the experiments only build under
  `cargo bench` / `--all-targets`).
- Editing one merged bench file recompiles its harness (2–3 modules) —
  negligible at this size.
