# Phase 16 — TPC-C Harness + Tuning Iteration 1: Working Plan

Expands plan.md §Phase 16 into tasks. Charter: first published tpmC +
latency percentiles, the interchange cross-product evidence, and ONE
profile→fix→re-measure tuning iteration. Correctness prerequisites are
done (2026-07-01: review fix plan §0–3, Q-35 closed).

**Already built** (verify, don't rebuild): parametric loader
(`--warehouses/--terminals/--duration-secs`), all 5 transactions as
prepared statements, tpmC reporting, `--sweep` bake-off across
engine × exec-model × planner (`src/bin/tpcc.rs`, 1054 lines).

**Explicitly out of scope**: versioned indexes (own plan:
`docs/plan-versioned-indexes.md`, user schedules it); Phase 17+ planner
work; Linux/NVMe absolute numbers (environment — macOS numbers are
RELATIVE evidence only; say so wherever numbers are published).

---

## T16.1 — `committed_txns` checkpoint-time pruning  [pre-soak gate]

The map is insert-only until recovery (NOTE at `txn/mod.rs:441`); sustained
runs leak memory. Prune entries below the checkpoint timestamp at
checkpoint time — pre-checkpoint versions are resolved by the
assumed-committed heuristic, so their entries are dead weight.

- [ ] 1a. Prune in `checkpoint()` under the txn-manager write path:
      retain `commit_ts >= checkpoint_ts`. Mirror for `aborted_txns`
      (entries whose versions are fully GC'd — check GC's existing
      forget path first; may already cover it).
- [ ] 1b. Tests: visibility across a checkpoint (reader with pre-prune
      snapshot must be unaffected — prune only below the low-water mark,
      i.e. `min(checkpoint_ts, oldest_active_read_ts)`); recovery after
      prune+crash; map size bounded across N checkpoints (soak-shaped
      unit test, not wall-clock).
- [ ] 1c. Run the MVCC/SI/hermitage/recovery suites + a 5-min tpcc soak
      with periodic checkpoints; assert RSS-proxy (map len) plateaus.

Risk: MVCC visibility correctness — this touches the read path's
committed-set semantics. Small diff, careful review, hermitage suite is
the referee.

## T16.2 — Latency percentiles in the harness

- [ ] 2a. Per-transaction-type latency capture (fixed-size reservoir or
      HDR-style buckets — bounded memory, no per-op allocation; bucketed
      log2 histogram is enough and allocation-free).
- [ ] 2b. Report p50/p95/p99 per transaction type + overall, alongside
      tpmC. Wire into both single-run output and the sweep table
      (sweep: p99 column next to tpmC).
- [ ] 2c. Abort/retry accounting surfaced (aborts already counted —
      include rate in the report; a tuning fix that trades tpmC for
      aborts must be visible).

## T16.3 — Baseline measurement (the "first published tpmC")

- [ ] 3a. Pick and record the canonical config: warehouses, terminals,
      duration, engine=btree, exec-model, planner=rule-based (per the
      TPC-H deferral: planner axis re-measured there). Fixed workload
      seed for repeatability.
- [ ] 3b. 3 runs × canonical config; publish median tpmC + percentiles
      in `docs/tpcc-results.md` with hardware/OS caveat and commit hash.
- [ ] 3c. Cross-product sweep (engine × exec-model × policy) published in
      the same doc — the interchange-thesis evidence table.

## T16.4 — Profile → top bottleneck

- [ ] 4a. Profile the canonical run (cargo flamegraph / samply on macOS;
      plus the harness's own stats: BPM hit rates, WAL fsync counts,
      lock waits if cheap to expose).
- [ ] 4b. Write up top-3 candidates with evidence in the results doc.
      Known suspects going in (verify, don't assume):
      - per-hit replacer mutex (`handle_cache_hit` NOTE — documented as
        the dominant write-mix serialization point; fix options in
        `docs/scalability-investigation.md`)
      - MVCC scan materialization (Phase 15 finding: lazy-MVCC-scan is
        the streaming lever)
      - per-execute re-planning of prepared statements (Selinger pays
        stats reads; rule-based planner is cheap — quantify anyway)
- [ ] 4c. Pick ONE (largest measured share, smallest blast radius
      tiebreak) — user signs off before implementation.

## T16.5 — The tuning fix + re-measure

- [ ] 5a. Implement the chosen fix in reviewable increments (design
      sketch first if it touches the BPM: Q-35 discipline — any change
      to eviction/replacer synchronization gets a shuttle model pass
      BEFORE merging, and the storm model must stay green).
- [ ] 5b. Re-run T16.3 (same config, seed, 3 runs); publish before/after
      tpmC + percentiles. Full suite + shuttle suite green.
- [ ] 5c. Milestone: plan.md Phase 16 marked complete; carry the
      remaining bottleneck list forward as Phase 17+ input.

---

## Sequencing & risk

1 → 2 → 3 → 4 → 5 strictly; 1 and 2 are independent (can interleave),
3 depends on both, 4–5 on 3. The phase's variance is in T16.5 — the
replacer-mutex fix, if chosen, is BPM concurrency surgery (the Q-35
family's home turf); budget the shuttle-model time.
