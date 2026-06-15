# Stability & Testing Strategy

A roadmap for turning InterchangeDB from a well-tested course-grade project
into a research artifact with a defensible correctness story. This document
is forward-looking; it complements `ISSUES.md` (the Quality Tracker, which tracks discrete open
items as `Q-NN`) by laying out the *categories* of work and their relative
leverage.

The thesis: stability for a research/portfolio ACID engine decomposes into
four concerns, and our current suite only covers the first well.

1. **Automated regression catching** — does a regression get caught without
   me remembering to look?
2. **Adversarial bug-finding** — does the suite find bugs no example test
   was written for?
3. **Provable ACID claims** — can we *demonstrate* the isolation and
   durability guarantees we advertise, under adversarial conditions?
4. **Visible hygiene** — can a reviewer (interviewer, admissions) see the
   process from the repo alone?

---

## What already exists

So the rest of this document is grounded, not generic. Current assets:

- `proptest` — partial (MVCC encoding, BTreeEngine put/get, GC, txn
  lifecycle). LSM and lock manager have **zero** coverage (`Q-08`).
- `criterion` — extensive (eviction policies, BPM, B+Tree, LSM, txn,
  engine crossover / steady-state / range-scan / amplification / YCSB).
- `goldenscript` — plan and EXPLAIN goldens.
- Soak test (`tests/soak_test.rs`, 30 s, `#[ignore]`).
- Concurrency + policy-swap stress tests; large-scale (25K–100K key) tests.
- `MemoryDiskManager` — an in-memory `DiskManager` impl (a key seam, below).
- `FaultInjectionDiskManager` — a fault-injecting `DiskManager`, wired into
  `tests/fault_injection_test.rs`. **Reality check (Q-32):** today it only
  schedules *I/O errors* (fail the next N read/write/allocate calls with
  `Error::Io`). It does **not** yet do torn writes, dropped/reordered flushes,
  or crash-at-LSN — so pillar D's data-page fault story is partly still to be
  built, not just extended. The WAL side of crash simulation needs no fault
  injector at all: the WAL format is self-describing, so the Q-32 driver
  crashes by truncating the WAL to each record boundary (see pillar D).
- `[lints.clippy] all = "warn"` in `Cargo.toml`.
- `ISSUES.md` (Quality Tracker) Q-NN tracker.

The gap is everything in pillars 2–4, plus the automation floor (pillar 1).

---

## A. Automation floor (CI)

> **Status: DONE — `Q-28` (2026-06-10).** Shipped `rust-toolchain.toml`
> (pin `1.89.0`) and `.github/workflows/ci.yml`: fmt / clippy-deny / test
> (debug + release) / doc jobs on push + PR, with `rust-cache` and
> cancel-in-progress. The nightly `--ignored` cron row below was
> **intentionally not adopted** — per-push only, by request. Required a
> green-baseline cleanup first (whole-repo `cargo fmt`, clippy 33→0,
> rustdoc 14→0); see the `Q-28` audit entry in `ISSUES.md`.

No `.github/workflows/` exists today. This is the cheapest high-value add
and the most visible — the green check is the first thing a reviewer sees.

| Job | Catches | Notes |
| --- | --- | --- |
| `fmt --check` + `clippy -D warnings` | style/lint drift | promote the existing `warn` lints to `deny` in CI only |
| `cargo test` + `cargo test --release` | logic + release-only timing/overflow bugs | release run is where races and overflow-wraparound surface |
| `cargo test -- --ignored` (nightly cron) | soak / large-scale regressions | keep off the per-PR path; too slow |
| `cargo doc -D warnings` | broken doc links / examples | cheap |
| `cargo-audit` + Dependabot | dependency CVEs | the "operational maturity" gap line item |
| `rust-toolchain.toml` pin + `cargo-llvm-cov` | reproducible builds, coverage trend | track coverage, do **not** gate on a % |

Sequencing note: do this first. Everything below rides on a green baseline.

---

## B. Adversarial bug-finding

Highest bug-per-hour. These find the classes of bug example-based tests
structurally cannot.

### B1. Fuzzing (`cargo-fuzz` / libFuzzer)

> **Status: DONE — `Q-29` (2026-06-10).** Both halves shipped: a CI-resident
> proptest suite (`tests/decode_fuzz_test.rs`) over all five decode surfaces,
> and the coverage-guided `cargo-fuzz` scaffold (`fuzz/`, nightly + manual,
> off CI). Together they found **three** real bugs — a `tuple::decode_column`
> cursor overrun, an `SSTableReader::open` short-file `assert!`, and a
> SQL-reachable `DECIMAL`-scale panic (`Q-31`) — all fixed. See the `Q-29`
> audit entries in `ISSUES.md`.

The codebase is full of *decode-arbitrary-bytes* surfaces that already have
round-trip invariants written — textbook fuzz targets, nearly free to wire:

- `types/keyenc` — encode/decode + order-preservation property.
- `types/tuple` — encode/decode + partial `decode_column`.
- `wal::LogRecord::decode`.
- LSM SSTable reader + manifest `replay_line`.
- `parse → bind` (a panic on adversarial SQL input is a bug).

A fuzzer finds malformed-input panics and silent corruption the example
tests miss by construction.

### B2. Mutation testing (`cargo-mutants`)

Measures whether the test suite *actually* catches injected bugs. The
honest-framing payoff: "N% mutation kill rate, here are the survivors and
why" is a far stronger claim than a coverage number, and it feeds the gap
analysis directly. Run it per-module, fix or document survivors.

### B3. Close `Q-08`

Property coverage for LSM (insert/get/scan/tombstone invariants, compaction
preserves the merged view) and the lock manager (no lost locks, no double
release, wait-for graph acyclicity outside deadlock).

---

## C. Concurrency correctness

> **Realized — `Q-30` fixed (2026-06-10).** `shuttle` is adopted for the
> buffer pool via a perf-preserving shim (`src/sync.rs`): prod keeps
> `parking_lot`, `--features shuttle` swaps in shuttle's instrumented
> primitives. The harness (`tests/bpm_swap_shuttle.rs`) **deterministically
> reproduced** the Q-30 eviction-flush race that defeated months of
> inspection, drove the fix, then **confirmed its absence** (PCT 50k + random
> 100k explore clean; the marquee stress went from 3/12 to 25/25 in release).
> It even caught a regression in the *first* fix attempt within milliseconds.
> This is the end-to-end pillar-C loop — find → fix → prove — and it partially
> answers `Q-25`: shuttle adopted where it pays off (the BPM) without the
> codebase-wide `std` migration Q-25 deferred for perf.

Closes `Q-25` (shuttle adoption vs. plan correction).

| Tool | Scope | Trade-off |
| --- | --- | --- |
| `loom` | small lock-free units: BPM frame pin/unpin, catalog `AtomicU32` counters, wait-for graph edits | exhaustive interleaving, but state space explodes past a few atomics |
| `shuttle` | whole `TxnEngine` + `LockManager` under randomized interleavings | scales to our size; the pragmatic Q-25 substitute |

`shuttle` is the right default at this scale. Both yield a seed that
reproduces any failing interleaving — which is the bridge to pillar D.

---

## D. Deterministic Simulation Testing (DST) — the differentiator

The FoundationDB / TigerBeetle approach: run the whole DB on a seeded RNG, a
simulated clock, and a **fault-injecting disk**, then run millions of
randomized scenarios where a single seed reproduces any failure.

It is tractable here because the *crash* mechanism needs no fault injector at
all: the WAL format is self-describing (4-byte length prefix + CRC per record),
so "crash at LSN k" is just truncating the WAL to a record boundary. The
**seeded scenario driver** runs a workload to produce a golden WAL, then sweeps
the failure space (crash at *every* LSN, seeded op streams) and asserts the
invariants below on every recovery.

> **Status: two cuts DONE — `Q-32` (2026-06-12).** (1) `tests/dst_recovery_test.rs`
> implements crash-at-every-LSN via WAL truncation, recovering into a fresh
> in-memory engine and checking against an oracle (committed-iff-Commit-in-cut)
> at every boundary, plus idempotency. Smoke (CI) + `#[ignore]` soak.
> (2) `tests/torn_page_recovery_test.rs` + a new `with_torn_next_node_write`
> capability on `FaultInjectionDiskManager` (which previously only scheduled
> I/O errors — the earlier "torn writes / crash-at-LSN" claim was wrong): tears
> a node-page flush, reopens, and asserts the torn page is detected (corruption
> `Err` or decode panic), never silently accepted. **Remaining:** a
> structurally-valid single-byte corruption to exercise the CRC assert
> specifically (the structural type check currently shadows it), reordered/
> dropped-flush faults, and multi-segment WAL truncation.

```
   ┌─────────────────────────────────────────────┐
   │  Seeded scenario driver (RNG, op generator)  │
   └───────────────────┬─────────────────────────┘
                       │ random txns / crashes
                       ▼
   ┌─────────────────────────────────────────────┐
   │     Database / TxnEngine / WAL / ARIES       │
   └───────────────────┬─────────────────────────┘
                       ▼
   ┌─────────────────────────────────────────────┐
   │  FaultInjectionDiskManager : DiskManager     │
   │   • torn writes (persist first N bytes only) │
   │   • reordered / dropped flushes              │
   │   • partial fsync, latency injection         │
   │   • CRASH at an arbitrary LSN ──┐            │
   └─────────────────────────────────┼───────────┘
                                      ▼
              recover() → assert invariants hold
```

The targeted first cut — **crash at every LSN in a workload, recover, verify**
— is a recovery torture test that exercises the WAL + ARIES + checkpoint
subsystem under exactly the failure modes ARIES exists to handle. This is
both an interview story and a legitimate writing/paper angle ("deterministic
simulation testing of an ACID engine in safe Rust").

Invariants to assert after each recovery:
- Every committed write is present; no aborted write is visible.
- No torn page passes checksum validation undetected.
- Recovery is idempotent (recover twice → identical state).
- The redo/undo passes converge regardless of crash point.

---

## E. Isolation-level proof

> **Status: trait + matrix LANDED — `Q-34` (2026-06-15).** MVCC isolation is now
> behind an `IsolationPolicy` trait (`src/txn/isolation/`); `SnapshotIsolation`
> (default) and `ReadCommitted` are the impls, threaded via
> `Database::open_with_isolation`. The Hermitage anomalies became a **conformance
> matrix**: `testkit::isolation` runs the scenarios per level and
> `tests/isolation_conformance.rs` asserts each level's required anomaly
> spectrum (SI blocks non-repeatable-read + lost-update where RC allows them;
> both block dirty-write, allow write-skew). A new level (SSI) is one
> `for_each_isolation!` line. **Remaining:** SSI impl; the History checker
> (item 2 below); a fuller Hermitage set (G1a/b/c, OTV, PMP).

We claim Snapshot Isolation today, Serializability (SSI) later. The claims
should be *demonstrated*, not asserted.

1. **Hermitage tests (`Q-24`)** — the fixed, named anomaly set
   (G0 / G1a / G1b / G1c / G2-item / G2). Finite, well-specified work; a
   good first step and a direct credibility item.
2. **History checker (the generalization)** — record a concurrent operation
   history, then check it against a reference oracle (Elle-style). This
   generalizes Hermitage from "did we block these six named anomalies" to
   "is every observed history admissible under the level we claim." It
   directly validates the SI → SSI transition that is a stated portfolio
   differentiator.

---

## F. GitHub hygiene & legibility

`ISSUES.md` is good process but invisible to a repo skimmer.

- **Mirror Q-NN into GitHub Issues** with labels (`area:wal`, `area:txn`,
  `area:buffer-pool`, `area:query`, `kind:correctness`, `kind:perf`,
  `kind:test-debt`, `sev:{critical,high,med,low}`) and **Milestones tied to
  phases**. Keep `ISSUES.md` as source of truth if preferred; the issues
  are the shop window.
- **Convert the known defects** (first-updater-wins, abort tracking,
  deadlock victim selection) into issues with a *failing regression test
  first* — the same bugs DST/shuttle would independently rediscover, which
  closes a nice validation loop.
- **Issue + PR templates**; a `DEVELOPMENT.md` even though solo.
- **ADRs (Architecture Decision Records).** Double duty: repo hygiene *and*
  the "defend each subsystem cold / one-page design doc per subsystem"
  preparation. One lightweight ADR per major decision (three-tier
  swappability, B+Tree tombstones, WAL-from-day-one, epoch reclamation,
  SI-by-design write-skew acceptance).

---

## G. Test modularity — the conformance matrix

> **Status: LANDED — `Q-33` (2026-06-14).** Shipped the `testkit` workspace
> crate (dev-dependency of root; source untouched). x-macro registries
> (`for_each_policy!`/`for_each_disk!`/`for_each_engine!`) are the single source
> of truth; per-axis contracts run across every impl
> (`replacer_conformance` ×6, `disk_manager_conformance` ×3,
> `engine_conformance` ×2); `config_equivalence` asserts the interchange thesis
> (same workload → identical state across all 6 policies / both DMs / both
> engines); the Q-32 durability sweep is now parametric (BTree **+** LSM); and
> `benches/config_matrix` measures the same registry head-to-head. A new engine
> inherits the whole suite from one registry line. See the `Q-33` audit entry.

IDB *is* a set of swappable traits: `StorageEngine` (B-tree / LSM / future
fractal), `EvictionPolicy` (6 impls), `DiskManager` (file / memory / fault),
and the `sync` backend (`parking_lot` / `shuttle`). The test architecture should
mirror that: write each trait's **contract once** and run it across **every**
implementation — a conformance matrix — instead of testing each impl ad-hoc.
Two payoffs: a new impl is correct the moment it passes the existing suite, and
"swapping the impl changes performance, never correctness" becomes an *asserted*
invariant rather than a hope. (This is the testing analog of the project's own
thesis — interchangeable parts behind stable interfaces.)

**Where we already do this (the pattern is proven):**

- `StorageEngine` — strong. `tests/cross_engine_differential.rs` (`Q-09`) runs
  the *same* proptest op stream against B-tree and LSM in lockstep and asserts
  observational equivalence; `catalog_table_test.rs` uses generic
  `scenario_*<E: StorageEngine>` functions instantiated per engine.
- `sync` backend — the *same* BPM code runs under `parking_lot` (prod) and
  `shuttle` (model) via the `crate::sync` shim. A config swap, already exercised.
- Performance — more modular than correctness already: `eviction_policies`
  benches all six policies, `engine_crossover` benches both engines.

**Where it's still trait-by-trait (the gap, ironically on our marquee axes):**

- `EvictionPolicy` — **weak.** Each replacer has its own `mod tests`; there is
  **no shared "every policy satisfies the trait contract" suite** (e.g. "a
  frame marked non-evictable is never returned by `evict`", "`record_access`
  makes a frame known", "`remove` clears it everywhere"). `policy_swap_stress`
  exercises all six, but only through the swap storm, not per-policy invariants.
- `DiskManager` — **weak.** Each test picks one; no shared read-after-write /
  allocate-monotonic / persistence suite run across file / memory / fault.
- The new `Q-32` DST sweep and torn-page test, and the `shuttle` models, are
  **hardcoded to B-tree / the BPM.** The *Database-level* crash contract
  (committed survives, uncommitted invisible) is engine-agnostic and should be
  parametric over `StorageEngine` — running the same crash sweep against LSM
  (whose recovery is manifest/SSTable-based — a genuinely different
  implementation of the same contract) is high-value.

**The two reusable shapes:**

1. **Conformance suite** — `fn assert_<trait>_contract(make: impl Fn() -> Impl)`
   encoding the trait's invariants once, called against every constructor.
2. **Metamorphic across configs** — correctness must be *invariant* under
   configuration: "same workload, all six policies → identical final data",
   "same workload, both engines → identical observable results" (the latter
   exists; generalize the former). The differential *is* the test.

Not a new pillar of *bug classes* — a refactor of how the existing pillars are
wired, so coverage multiplies across the matrix instead of accreting per impl.

### Future generators on this seam (`Q-33` follow-ons)

The matrix gives two primitives — a **config registry** and **workloads as
`Vec<Op>` data**. Every remaining testing modality is just a different op-stream
*generator* on that seam. **Step 1 (proptest) landed** (`config_proptest`:
`op_strategy` + registry-driven `all_policies_agree` / `all_engines_agree`;
`cross_engine_differential` kept as the deeper per-op engine check). The other
two, with concrete entailment for when they're picked up:

**2. shuttle × workload — the concurrency dimension.** Replay a workload across
threads under shuttle's scheduler, per config. Entails:
- A feature-gated thread/sync shim in `testkit` mirroring `src/sync.rs`: under
  `--features shuttle`, alias `shuttle::thread::spawn` / `shuttle::sync::Arc`;
  else `std`.
- **Prerequisite (the big one):** convert the txn/lock cone (`TxnEngine`,
  `LockManager`, `TransactionManager`) to `crate::sync` so shuttle instruments
  it — today only the BPM cone is. This *is* leverage #4.
- A `testkit::concurrent` harness: N shuttle threads each replay a slice of a
  workload (or interleaved txns) against one config, with invariants (no lost
  update, the claimed isolation level holds).
- Tests under `#[cfg(feature = "shuttle")]`, run in a separate
  `--features shuttle` CI lane (the sync axis is compile-time, not a registry
  entry). Bounded by design — few threads, short streams (state space explodes).
- Cheap win available now: have `bpm_swap_shuttle` iterate
  `testkit::policy::makers()` to model-check the swap/eviction race across all
  six policies (multiplies runtime — judgment call).

**3. fuzz × matrix — coverage-guided op streams.** A structure-aware
differential fuzzer (the coverage-guided analog of `cross_engine_differential`).
Entails:
- `testkit = { path = ".." }` in `fuzz/Cargo.toml` (fuzz is a separate
  workspace; a path dep works).
- An `arbitrary::Arbitrary` source for `Vec<Op>` (cargo-fuzz uses `arbitrary`,
  not proptest): a fuzz-only `&[u8] -> Vec<Op>` decoder, or
  `#[derive(arbitrary::Arbitrary)]` on `Op` behind a testkit feature.
- A target `fuzz/fuzz_targets/engine_differential.rs`: bytes → `Vec<Op>` → run
  across `for_each_engine!` → `assert_all_equal` (divergence = finding).
  Nightly/manual, corpus persisted, off CI.
- Overlaps proptest heavily; pursue only if libFuzzer's coverage-guidance finds
  op-stream paths proptest's sampling misses. Keep the byte→decoder fuzzers
  as-is (different goal: malformed-input robustness).

---

## Leverage ranking

Sequenced for return on the stated goals (FAANG interviews, PhD apps,
research/learning vehicle):

1. ~~**CI** — a day; unblocks everything.~~ ✅ **DONE** (`Q-28`).
2. ~~**Fuzz the decode surfaces** — cheap; invariants already exist;
   immediate bug yield.~~ ✅ **DONE** (`Q-29`; found 3 bugs → fixed, incl. `Q-31`).
3. **Seeded scenario driver, crash-at-every-LSN torture** — the differentiator.
   ✅ **Two cuts DONE** (`Q-32`): WAL-truncation crash sweep + oracle +
   idempotency (`tests/dst_recovery_test.rs`), and torn-page detection via a new
   `FaultInjectionDiskManager` torn-write capability (`torn_page_recovery_test.rs`).
   **Remaining:** a structurally-valid single-byte corruption to hit the CRC
   assert specifically, reordered/dropped-flush faults, and multi-segment WAL
   truncation.
4. **`shuttle` over the txn/lock cone** (extends `C`/`Q-25` past the BPM) —
   the highest remaining bug-yield. The gnarliest concurrency lives here: the
   ghost-commit window (flagged in `commit_txn` itself), first-updater-wins,
   deadlock victim selection. Model-checking the BPM found `Q-30`; this cone is
   where the next race hides.
5. **`cargo-mutants`** (`B2`) — the *meter*. It injects bugs and checks whether
   the suite kills them, so it tells you both where everything above is blind
   **and** when the suite is good enough to stop (see *When to stop*).
6. **History checker for SI** (`E2`) — validates the headline isolation claim
   ("is every observed history admissible under SI", not just the six named
   anomalies); folds in `Q-24`. The stated research differentiator.
7. ~~**Conformance-matrix testing** (`G`)~~ ✅ **DONE** (`Q-33`) — and
   **GitHub Issues + ADRs** (`F`) — legibility.

Items 3, 4, and 6 are what turn IDB from "a course project that happens to be
well-tested" into "a research artifact with a defensible correctness story"
— precisely the gap the project's own positioning identifies.

---

## When to stop adding testing *features*

Testing infrastructure has diminishing returns, and the goal is confidence
proportional to the project's purpose — a portfolio / research artifact, not a
system with paying users. Three signals say *enough*:

1. **New modalities have stopped finding new bugs.** Fuzzing found 3; `shuttle`
   found `Q-30`. But the DST sweep, the second `shuttle` model, and the
   torn-page test (`Q-32`) found **nothing new** — they guarded already-fixed
   bugs and corroborated behavior. That's the transition from *finding* bugs to
   *preventing regressions*: still valuable, but the marginal bug-yield per new
   test *type* is now low.
2. **Let `cargo-mutants` be the meter, not your gut.** If the kill-rate on the
   core modules is high (≈85 %+), the existing suite already catches injected
   bugs and *more test code has low marginal value* — stop. If it's low, you
   have specific blind spots worth one targeted pass. This replaces "do I feel
   covered?" with a number.
3. **Match investment to purpose.** The ROI here is *a demonstrable correctness
   story*, which rewards **one credible instance of each pillar** far more than
   exhaustive depth in any one. That's nearly in hand.

**The rule:** add exactly the three remaining distinct-dimension items —
`shuttle` over txn/lock (concurrency in the txn layer), one `cargo-mutants` pass
(test-quality measurement), the SI history checker (the isolation claim) — then
**stop adding testing modalities.** Switch to *running* what exists (wire
fuzzers / `shuttle` / DST into a nightly) and only extend the suite when a **new
subsystem lands**. The next subsystem is the query engine (~2026-04); give *it*
the same pillar treatment as it arrives rather than gold-plating the current
surface. Testing infrastructure can quietly become procrastination from
shipping — the stopping point is when a surviving mutant is rarer than the value
of the next feature.

---

## Mapping to existing `Q-NN` items

| This doc | Existing item |
| --- | --- |
| B3 (LSM + lock-manager property tests) | `Q-08` |
| E1 (Hermitage) | `Q-24` |
| C (`shuttle`) | `Q-25` |
| F (convert known defects) | first-updater-wins / abort tracking / deadlock victim |

New work introduced here (CI, fuzzing, DST, history checker, cargo-mutants,
conformance-matrix refactor (`G`), ADRs) should be assigned fresh `Q-NN`
numbers when adopted.
