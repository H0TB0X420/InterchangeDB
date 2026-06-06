# TPC-C Harness Fidelity

How to move `src/bin/tpcc.rs` from a flat-out throughput probe toward a
**spec-faithful, self-reported, single-node, unaudited** TPC-C run. The goal
is not to fake an audited result — that requires a TPC-certified auditor, a
full disclosure report, and a priced configuration. The goal is maximum
honest fidelity, labeled honestly.

**Label discipline:** never report a bare `tpmC`. The honest README label is
*"TPC-C-derived, spec-faithful workload; single-node; self-reported;
unaudited."* This is the same discipline as the existing NOPM ≠ tpmC caveat.

**Dual-mode harness.** Keep both, behind `--mode`:
- `flatout` (current) — no keying/think time; max throughput. For profiling,
  regression tracking, and *relative* system comparison. (The performance
  *ratio* between systems is the same with or without think time, so this is
  legitimate for comparison — just not a `tpmC`.)
- `compliant` (new) — full keying/think model, spec mix, NURand, contention.
  Produces an honest `tpmC`.

---

## The reframe: what `compliant` mode actually measures

Adding keying + think time caps each warehouse at ~12.86 tpmC, so total
throughput ≈ warehouses × 12.86. The engine is barely worked unless warehouse
count is large. Therefore `compliant` mode is **not a throughput drag race —
it is a latency-under-concurrency test**:

> How many warehouses can the engine host before the 90th-percentile response
> time exceeds 5 s? Reported `tpmC` ≈ (sustained warehouses) × 12.86 ×
> (achieved fraction).

The binding constraint shifts from per-op speed to memory footprint and tail
latency. This is a genuinely different test from `flatout` profiling, and it
is the honest path to "as close to audited as possible."

---

## Tier 1 — Workload fidelity

### 1.1 The RTE is a restructure, not a tweak

Each terminal becomes a state machine that is mostly asleep:
`keying delay → submit → response → think delay → repeat`.

**Recommended: event-driven scheduler, not thread-per-terminal.** A min-heap
keyed by each terminal's next-wake timestamp; pop the earliest, run its
transaction on a worker pool, reschedule at `now + response + think`. No idle
threads, low scheduler noise, and seed-deterministic firing order (fits the
DST/goldenscript ethos). Async, if used at all, belongs *here* in the load
generator driving idle terminals — never in the engine.

Spec keying / think-time minimums (think time is exponential with the listed
mean, truncated at 10× mean):

| Transaction  | Keying (s) | Think mean (s) |
| ------------ | ---------- | -------------- |
| New-Order    | 18         | 12             |
| Payment      | 3          | 12             |
| Order-Status | 2          | 10             |
| Delivery     | 2          | 5              |
| Stock-Level  | 2          | 5              |

### 1.2 Terminal / warehouse binding

10 terminals per warehouse, one per district (D_ID 1–10), each bound to its
home W_ID/D_ID. Drop the independent `--terminals` knob; derive it as
`warehouses × 10`.

### 1.3 Transaction mix

Per-terminal weighted selection yielding the spec ratios: equal New-Order and
Payment, plus one Delivery, one Order-Status, and one Stock-Level per ten
New-Orders (≈ 45 / 43 / 4 / 4 / 4).

### 1.4 NURand — the fidelity lynchpin

The current disjoint-PK-range scheme deliberately removes skew (hence the 0%
abort artifact). Replace it with the spec's non-uniform generator
`NURand(A, x, y)`:

- Customer ID: `A = 1023`, range `[1, 3000]`
- Item ID: `A = 8191`, range `[1, 100000]`
- Customer last name: `A = 255`

The run's `C` constant is constrained relative to the load's so the run cannot
trivially hit only the loaded-hot keys. This skew is what stresses version
chains and the buffer pool the way audited runs do. Easy to get subtly wrong —
verify the `C` constraint.

### 1.5 Built-in contention (restores the realistic abort signature)

- **Per-district row** is contended by construction: every New-Order bumps
  `D_NEXT_O_ID`; every Payment updates `D_YTD`. The unavoidable hotspot.
- **By-last-name customer selection** (60% of Payment / Order-Status)
  exercises the non-PK / secondary-index access path.
- **Remote traffic**: 1% of New-Order order-lines use a remote supply
  warehouse; 15% of Payments are remote (home 85% / remote 15%). Requires
  ≥ 2 warehouses to exist at all.
- **1% deliberate rollback**: New-Order with an invalid item ID forces an
  abort, exercising the rollback path under load.

Faithful signature: a small, non-zero abort rate and realistic tail latency —
not 0%.

### 1.6 Measurement methodology

- Per-transaction-type latency histograms (reuse `benches/common`'s
  `LatencyHistogram`).
- Response-time gate: 90th-percentile ≤ 5 s (Stock-Level ≤ 20 s).
- Ramp-up (stagger terminal starts / warm caches), then a documented
  steady-state measurement window; checkpoint at least every 30 minutes;
  report sustained throughput, not peak.

---

## Tier 2 — The ACID battery

One-shot verifications, not measured under load. Put them behind `--acid` or in
`tests/tpcc_acid.rs`. This tier is where credibility actually lives, and it
fuses with the `STABILITY.md` DST work.

### 2.1 Atomicity

Commit a New-Order, verify all rows present. Abort one, verify zero trace
across W / D / C / O / OL / Stock.

### 2.2 Consistency (the twelve conditions)

Cheap, high-value aggregate checks — run **before and after** a measured run:

- `W_YTD = Σ D_YTD` (per warehouse)
- `D_NEXT_O_ID − 1 = max(O_ID) = max(NO_O_ID)` (per district)
- New-Order IDs contiguous (`max − min + 1 = row count`, per district)
- `Σ O_OL_CNT = |order-line rows|` (per district)

These are exactly the invariants a fuzz/DST run should assert — the fusion
point with `STABILITY.md`.

### 2.3 Isolation

The spec's constructed interleavings target the classic anomalies — dirty
read, non-repeatable read, phantom — which **Snapshot Isolation already
prevents**, so IDB under SI should pass the battery. TPC-C's isolation tests
do not include a write-skew scenario that SI fails, so the known SI gap is not
exercised here.

- Verify each test empirically (good goldenscript / deterministic-harness use).
- Document: *"passes TPC-C's isolation tests under SI; write skew remains
  permitted by design and motivates SSI."* The battery becomes the acceptance
  test that justifies the SSI roadmap item.

### 2.4 Durability

Maps directly onto `FaultInjectionDiskManager` + ARIES recovery.

- **Power loss / memory loss** — faithfully testable: crash at a random LSN,
  restart, recover, verify every committed txn is present and no uncommitted
  one leaked.
- **Durable-media failure** — needs storage redundancy (RAID/replication);
  out of scope for a single-node engine. Scope it out honestly.

### 2.5 The highest-credibility artifact

Combine the pieces you already have: run a real TPC-C workload, **crash
mid-run via fault injection, recover, then check the twelve consistency
conditions.** If they hold, you have demonstrated atomicity, durability, *and*
consistency in one shot under the actual benchmark workload. This is assembly,
not new invention, and it is more convincing than any `tpmC` number.

---

## Tier 3 — Honestly out of scope

State these in the README so the label stays honest; do not fake them:
`$/tpmC` pricing, three-year maintenance cost, 60-day storage sizing, a
certified auditor + full disclosure report, and 8-hour sustainability.

---

## Build order

1. **Event-driven RTE + NURand + 10:1 terminal binding** (Tier 1) — the
   biggest structural change; everything rides on a faithful workload.
2. **Consistency-condition checker** (Tier 2.2) — cheap, and it immediately
   validates that the Tier 1 population is correct.
3. **Crash-recovery-under-TPC-C artifact** (Tier 2.5) — wire fault injection
   into the harness run, then assert the consistency conditions on recovery.
4. Isolation battery (2.3) and atomicity/durability tests (2.1, 2.4) as
   standalone `--acid` checks.

Fidelity-bearing decisions to own personally: the NURand generator (and its
`C`-constant constraint) and the per-terminal transaction-mix weighting. The
event-driven scheduler skeleton and the consistency-checker queries are
boilerplate.
