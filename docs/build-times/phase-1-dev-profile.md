# Phase 1 — dev profile and inner-loop hygiene

## The change

**Generic.** Cargo's `dev` profile defaults to full DWARF debuginfo
(`debug = 2`) for everything. Debuginfo is the bulk of what the linker
copies, and on macOS the stock `ld64` linker is slow at it — so every test
binary pays a debuginfo tax on every rebuild, multiplied by the number of
binaries. Two levers cut it without losing what debuginfo is actually used
for day-to-day (legible panic locations and test-failure backtraces):
`debug = "line-tables-only"` keeps file:line info at a fraction of the
volume, and dependencies — which rarely change but execute inside every
test — can be compiled once at `opt-level = 2` with no debuginfo at all.
Separately, an IDE's rust-analyzer sharing `target/` with terminal cargo
serializes them on the build lock; giving the IDE its own target directory
removes stalls that look like slow builds but are lock waits.

**This repo.** Root `Cargo.toml` gained `[profile.dev]
debug = "line-tables-only"`, `[profile.dev.package."*"] opt-level = 2,
debug = false` (the wildcard excludes workspace members, so
`interchangedb`/`testkit` iteration still compiles fast), and
`[profile.dev.build-override] opt-level = 3` for proc macros. README gained
a Development section: the fast loop is `cargo check --lib` /
`cargo test --lib` / `cargo clippy --lib`; full `cargo test`,
`--release`, and `clippy --all-targets -- -D warnings` are pre-commit
gates; plus the rust-analyzer `targetDir` setting. The standalone `fuzz/`
workspace is unaffected (own `[workspace]`, own profiles — verified).

## Gains

Same protocol as the baseline: quiet 4-core Mac, warm cache,
`touch src/sql/binder.rs` before each timed pass.

| Measurement | Before | After P1 | Δ |
| --- | --- | --- | --- |
| `cargo test --no-run` (all 88 binaries) | 127 s | **29.3 s** | **4.3×** |
| `cargo test --lib --no-run` | 10.4 s | 4.2 s | 2.5× |
| `cargo check --lib` | 7.8 s | 1.2 s | 6× (proc-macro build-override) |
| `cargo bench --no-run` | 150 s | 152 s | none (codegen-bound, expected) |
| `target/` size | 15 GB | 1.6 GB | 9× |

The bench path moving 0 % while the test path moved 4.3× is the profile
doing exactly what the diagnosis said: the `bench` profile was already
optimized-no-debuginfo, so the win is all debuginfo linking, all on the
dev/test side.

Incidental catch: running the full release gate for the first time since
2026-07-10 exposed two `should_panic` tests asserting debug-only
`MergeDebug` machinery — they can never pass in release and are now
`#[cfg(debug_assertions)]` (commit `307e2e1`, separate from this phase).

## Draws

- **One-time rebuild:** the profile change invalidates the entire cache.
  Paid here together with a `cargo clean` (freed 32 GiB): cold
  `cargo test --no-run` was 4 m 39 s including the opt-2 dependency build.
- **Debugger fidelity:** line tables give locations, not variable values.
  Step-debugging with full variable inspection needs a temporary
  `debug = 2` (or a custom profile) — accepted because step-debugging is
  rare here; crash-on-corruption plus tests is the working style.
- **Dependency backtraces:** frames inside dependencies have no debuginfo
  at all (`debug = false`). Panics originating inside a dep show mangled
  frames; the first project frame is still precise.
- **Optimized deps can reorder dep-internal behavior** (e.g. timing of a
  parking_lot fast path) — semantically invisible, but stress-test timing
  characteristics in debug runs shift slightly.
