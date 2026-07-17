# Plan: build, test, and bench turnaround

**Status:** plan for review. **Goal:** cut the edit→signal wait that breaks
flow, without changing one line of semantic behavior.

## The three loops being optimized

| Loop | Command today | Bottleneck |
| --- | --- | --- |
| L1 edit→type-check | `cargo check` (whole workspace) | over-broad target set |
| L2 edit→tests runnable | `cargo test` | **88 integration binaries relinked per `src/` edit** |
| L3 pre-commit / bench | `cargo test --release`, `cargo bench`, `clippy --all-targets` | 88 + 14 binaries, full debuginfo |

## Evidence (measured this week, 4-core macOS / ld64)

- One-line `src/` edit → rebuild lib + relink **just 6** test targets ≈ **30 s**
  warm, 212 s cold (measured under `cargo-mutants -j 1` on a quiet machine).
  The full tree links **88** integration binaries + lib/unit targets — the
  same edit through `cargo test --no-run` extrapolates to minutes, almost all
  of it repeated linking of the same 45 k-LOC lib.
- No `[profile.dev]` section exists → full `debug = 2` on everything;
  `target/` is **15 GB**. Debuginfo volume is a first-order ld64 cost.
- Contention poisons everything: 2 parallel build jobs degraded builds 5–8×
  during the mutation campaign. rust-analyzer shares `target/` with terminal
  cargo and stalls builds on the lock.
- Benches are 14 separate `[[bench]]` binaries, each linking criterion + lib.

Every phase below: measure → change → measure → all four gates
(`fmt --check`, `clippy --all-targets -- -D warnings`, `cargo test`,
`cargo test --release`) → one commit. Numbers recorded in the table at the
bottom; a change with no number did not happen. Measurements only on a quiet
machine (no rust-analyzer on the same target dir, nothing else building).

Each phase ends with a short result doc in `docs/build-times/` —
`phase-N-<slug>.md` — describing the change twice (the generic principle,
then what was done in this repo), the measured gains, and any draws. The
table is the scoreboard; the per-phase doc is the explanation a reader
without this session's context needs.

---

## Phase 0 — Baseline (first, quiet machine)

```bash
touch src/sql/binder.rs && time cargo check --lib        # L1 floor
touch src/sql/binder.rs && time cargo test --lib --no-run
touch src/sql/binder.rs && time cargo test --no-run      # THE number (88 links)
touch src/sql/binder.rs && time cargo bench --no-run     # L3 bench path
cargo build --timings                                    # unit count + link share, one cold pass
```

Diagnostic only, recorded for the modularity thesis's build-time price:
`cargo llvm-lines --lib | head -40` (needs `cargo install cargo-llvm-lines`)
— how much `Database<E>` / `Session<E>` / the `for_each_*!` matrix cost in
monomorphization. No action on it this plan.

## Phase 1 — Config-only wins (no code moves)

**1a. Dev profile** in root `Cargo.toml`:

```toml
[profile.dev]
# Full debuginfo dominates ld64 link time; line tables keep panic locations
# and test-failure backtraces legible at a fraction of the cost.
debug = "line-tables-only"

[profile.dev.package."*"]
# Deps compile once, run in every test; optimizing them pays back on every
# proptest/goldenscript run.
opt-level = 2
debug = false

[profile.dev.build-override]
opt-level = 3   # proc macros (serde_derive, thiserror) run at compile time
```

Notes: invalidates the whole cache once — the natural moment to `cargo clean`
and reclaim the 15 GB. Verify with `cargo metadata` the standalone `fuzz/`
workspace is unaffected (it should be; it is a separate workspace).

**1b. rust-analyzer target dir** (per-developer, documented in README under
"Development"): `"rust-analyzer.cargo.targetDir": true` — ends the lock war.
Do this before Phase 0 measurements or they are poisoned.

**1c. Inner-loop commands** documented in README: `cargo check --lib` /
`cargo test --lib` / `cargo clippy --lib` in the loop; `--all-targets` stays
at pre-commit and CI. Sweep `docs/` for `cargo test --test X` idioms while
at it.

## Phase 2 — Collapse 88 test binaries → 2 (the main event)

`tests/it/main.rs` (~79 fast suites as modules) + `tests/stress/main.rs`
(the 9 stress/scale/soak files: `btree_scale`, `concurrency_stress`,
`deadlock_stress`, `large_scale`, `lock_stress`, `policy_swap_stress`,
`soak`, `txn_stress`, `wal_stress`). Files move into the subdirs unchanged;
subdirectory placement ends auto-discovery, two `[[test]]` entries wire the
harnesses. The split keeps the everyday `--test it` lean and slow suites
separately invocable; the link count still drops 88 → 2.

Mechanical rules (the ONLY allowed edits — no test logic changes):

1. File-level `#![cfg(feature = "shuttle")]` → `#[cfg(feature = "shuttle")]`
   on the `mod` line in `main.rs` (exactly one file: `bpm_swap_shuttle.rs`,
   which stays a module of `it`).
2. The four files declaring `mod common;` switch to `use crate::common::…`;
   each harness declares `mod common;` once.
3. proptest regression seeds live NEXT TO their sources —
   `tests/decode_fuzz_test.proptest-regressions` and
   `tests/cross_engine_differential.proptest-regressions` (committed Q-29
   evidence). They move alongside into `tests/it/` and MUST replay after.
   Losing them silently is the worst outcome of this phase.

Acceptance:
- Aggregated `cargo test -- --list` count identical before/after; identical
  pass/fail set.
- Ignore-set identity: today that set is exactly five — the two 1M
  `large_scale_test` cases, `shuttle_q35_replay`, the `dst_recovery_test`
  soak sweep, and `soak_test` (verified by grep 2026-07-17). The marquee is
  NOT ignored; the old Q-30-era quarantine idea was never applied and both
  bugs are closed.
- Seeds replay; `--features shuttle` run green inside the merged binary.
- Known trade, accepted: editing one test file now recompiles its harness
  crate (~10–20 k LOC) instead of one small binary; the src-edit loop wins
  ~44× per harness in exchange.

## Phase 3 — Collapse 14 bench binaries → 3

`bpm_bench` (absorbs `eviction_policies`), `engine_bench` (the 9 `btree`/
`lsm`/`engine_*` files), `sql_bench` (`push_vs_volcano`, `txn_bench`,
`config_matrix`). Criterion results are keyed by group/function id, not
binary name — **preserve every `criterion_group!` name and `bench_function`
id verbatim** so saved baselines and `benches/RESULTS.md` references stay
valid. If a grouping forces a rename, stop and flag. Sweep docs/scripts for
`--bench` invocations. Acceptance: 3 binaries, `--list` identical modulo
binary, table row filled.

## Deliberate non-goals (named, not forgotten)

- **Toolchain bump for rust-lld** (default in 1.90+): Linux-only — buys
  nothing on this Mac (Darwin 21 can't take Xcode 15's newer ld either), only
  CI. Decide after P1–P3 numbers, budgeting the lint/fmt sweep a pin bump
  costs.
- **Workspace split** (`idb-common`/`idb-storage`/…): real value beyond build
  times — a crate boundary is the enforceable form of a trait seam — but
  design-bearing and coupled through `Database<E>`/`Catalog<E>`/`Session<E>`.
  Revisit with P1–P3 results in hand; the `engines/`/`optimizer/` restructure
  already staged the seams.
- **cargo-nextest**: speeds the *run* phase, not builds; revisit only if run
  time is what remains after P2.
- **sccache**: low value for a single-crate local loop.

## Results table (fill as phases land)

Baseline recorded 2026-07-17, quiet 4-core macOS, warm cache, each timed
pass preceded by `touch src/sql/binder.rs`.

| Measurement | Baseline | After P1 | After P2 | After P3 |
| --- | --- | --- | --- | --- |
| `cargo check --lib` after 1-line `src/sql/` edit | 7.8 s | 1.2 s | 1.5 s | |
| `cargo test --lib --no-run` after edit | 10.4 s | 4.2 s | 5.4 s | |
| `cargo test --no-run` after edit | **127 s** | **29.3 s** | **7.8 s** | |
| `cargo bench --no-run` after edit | 150 s | 152 s | 152 s (untouched) | |
| Test binaries linked | 88 | 88 | 2 | 2 |
| Bench binaries linked | 14 | 14 | 14 | 3 |
| `target/` size | 15 GB | 1.6 GB | 1.6 GB | |

P1 gates 2026-07-17: fmt ✓ · clippy ✓ · debug 91 suites 1386/0 ✓ ·
release 91 suites 1384/0 ✓ (2 fewer = the debug-gated should-panic pair).

P2 gates 2026-07-17: fmt ✓ · clippy ✓ · debug 5 suites 1335/0 ✓ ·
release 5 suites 1333/0 ✓ (each exactly 51 below P1 = the de-duplicated
3 × 17 `tests/common` unit-test runs; test-list identity 599 = 599).

**Hypothesis confirmed.** The same one-line edit costs 10.4 s when only the
lib test binary is rebuilt and 127 s for the full test set: ~92 % of the
integration-test build is the other 87 binaries (168 s user / 149 s system —
the system-time share is the linker). Phase 2's ceiling is therefore
~10× on the headline number. One nuance the prior missed: the bench path
(150 s at 316 % CPU, `bench` profile is optimized) is codegen-bound, not
link-bound — Phase 3 still helps but proportionally less there; its main
payoff is the `--all-targets` pre-commit path.
