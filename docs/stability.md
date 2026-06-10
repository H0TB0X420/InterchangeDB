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
- `FaultInjectionDiskManager` — a fault-injecting `DiskManager` (torn writes,
  dropped/reordered flushes, crash-at-LSN), already wired into
  `tests/fault_injection_test.rs` and `tests/crash_recovery_test.rs`. The DST
  mechanism in pillar D **already exists** — pillar D is about extending its
  *coverage*, not building it.
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

It is tractable here *because the mechanism already exists*: `DiskManager` is a
trait, and `FaultInjectionDiskManager` already implements torn writes,
dropped/reordered flushes, and crash-at-LSN — exercised today by
`tests/fault_injection_test.rs` and `tests/crash_recovery_test.rs`. The
remaining work is not building the fault injector; it is wrapping it in a
**seeded scenario driver** that sweeps the failure space (crash at *every* LSN,
randomized op streams) and asserts the invariants below on every recovery.

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

## Leverage ranking

Sequenced for return on the stated goals (FAANG interviews, PhD apps,
research/learning vehicle):

1. ~~**CI** — a day; unblocks everything.~~ ✅ **DONE** (`Q-28`).
2. **Fuzz the decode surfaces** — cheap; invariants already exist;
   immediate bug yield.
3. **Seeded scenario driver over the existing `FaultInjectionDiskManager`**
   (crash-at-every-LSN torture) — the differentiator; the fault injector and
   its tests already exist, so this is coverage extension, not new mechanism.
4. **History checker for SI** — validates the headline isolation claim;
   folds in `Q-24`.
5. **`shuttle`** (closes `Q-25`), then **`cargo-mutants`** (honest
   test-quality story).
6. **GitHub Issues + ADRs** — legibility and interview prep.

Items 3 and 4 are what turn IDB from "a course project that happens to be
well-tested" into "a research artifact with a defensible correctness story"
— precisely the gap the project's own positioning identifies.

---

## Mapping to existing `Q-NN` items

| This doc | Existing item |
| --- | --- |
| B3 (LSM + lock-manager property tests) | `Q-08` |
| E1 (Hermitage) | `Q-24` |
| C (`shuttle`) | `Q-25` |
| F (convert known defects) | first-updater-wins / abort tracking / deadlock victim |

New work introduced here (CI, fuzzing, DST, history checker,
cargo-mutants, ADRs) should be assigned fresh `Q-NN` numbers when adopted.
