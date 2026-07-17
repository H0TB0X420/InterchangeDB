# Phase 2 — collapse 88 test binaries into two harnesses

## The change

**Generic.** Every file in a Cargo `tests/` directory is auto-discovered as
its own crate and binary. Each binary statically links the whole library
(plus dev-dependencies), re-monomorphizing every generic seam per binary —
so the *number of test files* silently multiplies the cost of every `src/`
edit. Moving the files into subdirectories (which ends auto-discovery) and
declaring each former file as a `mod` of one explicit `[[test]]` harness
keeps every test identical while linking once. Two harnesses instead of one
keeps a deliberate seam: slow suites stay separately invocable and the
everyday harness recompiles less when a test file is edited.

**This repo.** 88 `tests/*.rs` files became modules of two harnesses:
`tests/it/main.rs` (79 fast suites) and `tests/stress/main.rs` (the 9
stress/scale/soak suites). Mechanical edits, nothing else: the shuttle
file's inner `#![cfg(feature = "shuttle")]` moved onto its `mod`
declaration; four files dropped their per-binary `mod common;` for a single
harness-level one (uses became `crate::common::…`); `common_smoke.rs` kept
its test but lost its reason to exist (the harness compiles `common`
unconditionally now); both committed proptest regression seed files moved
alongside their sources into `tests/it/`. `tests/goldenscripts/` data stayed
put — the macros resolve it manifest-relative.

## Proof nothing changed semantically

- Normalized test-list identity: **599 = 599, zero missing, zero new**.
  (Raw before-count was 650: `tests/common/`'s 17 module-level unit tests
  were compiled into — and ran inside — 4 separate binaries; they now run
  once. 650 − 3×17 = 599.)
- Ignore-set identity: the same 4 (`dst` soak sweep, 2× 1M `large_scale`,
  `soak_test`), plus `shuttle_q35_replay` under `--features shuttle` only —
  same as before.
- proptest seeds replay from their new paths; no new seed files created.
- All four gates green (debug/release totals drop by exactly 51 = the
  de-duplicated common runs).

## Gains

| Measurement | Baseline | After P1 | After P2 | Δ vs baseline |
| --- | --- | --- | --- | --- |
| `cargo test --no-run` after 1-line `src/sql/` edit | 127 s | 29.3 s | **7.8 s** | **16×** |
| `cargo test --lib --no-run` after edit | 10.4 s | 4.2 s | 5.4 s | — |
| Test binaries linked | 88 | 88 | **2** | 44× fewer links |

Test *runtime* also drops slightly: the 17 common scaffolding tests no
longer execute 4×, and 88 process spawns become 2.

## Draws

- **Editing one test file now recompiles its whole harness crate** (the
  79-module `it` crate is ~20 k LOC) instead of one small binary — the
  deliberate trade for the ~44× link reduction on `src/` edits. The
  stress split keeps the worst of it bounded.
- **Test invocation names changed:** `cargo test --test near_limit_sizes_test`
  is now `cargo test --test it near_limit_sizes_test::`. Anything filtering
  by bare test name still works; anything naming a binary needed updating
  (docs swept; CI had none).
- **Tests share two processes instead of 88.** The hazard sweep found no
  process-global state (no env-var mutation, no cwd changes, no ports), so
  this is theoretical today — but a future test that mutates process state
  will interfere with siblings in a way it previously couldn't. The harness
  doc comment says so.
- **One less isolation property:** a test that aborts the process (not
  panics — aborts) would now take its harness's remaining tests with it.
  No such test exists.
