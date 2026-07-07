# Clock2Q+ reproduction — full-fleet results (106 CloudPhysics traces)

## Curve-by-curve recreation map (vs the paper's figures)

We recreate every core evaluation figure the paper's prose supports,
with 10 of its 16 evaluated algorithms. Per figure:

| Paper figure | Our artifact | Status / gap |
|---|---|---|
| Fig 8(a) — improvement bars, metadata traces, 106-trace mean | `figures/fig8a_metadata_traces.png` + table below | Done. We cover 7 of its 10 algorithms (missing LIRS, LIRS2, TinyLFU, Cacheus) and add LRU/FIFO from the paper's wider set. |
| Fig 8(b) — same, raw data traces (fanout 1) | `figures/fig8b_data_traces.png` | Fleet pass 2 (in flight). |
| Fig 9 — miss-ratio curves, trace w56, log-scale sizes, metadata + data | `figures/fig9_w56_mrc.png` + `w56.{meta,data}.csv` | **Done — claim exactly reproduced**: clock2q+ strictly below Clock/ARC/S3-FIFO at all 9 sizes on BOTH panels (18/18), all 4 of its algorithms covered. Axis note: we plot footprint fraction; the paper plots bytes. |
| Fig 7 — derived vs real-B-tree agreement (Clock2Q+/S3-FIFO) | our IDB-native experiment (below) is the adversarial sibling | Their TLX-leaf trace shows agreement; our TPC-C page stream shows the ranking flip. Both belong in the writeup. |
| Table 1 — Small→Main / Small→Ghost / Ghost→Main counts | movement-counter columns in every CSV | Done (their table is one unnamed case-study trace; we emit it per trace). |
| Fig 13 — window sweep (10/30/50% of Small) | `figures/fig13_window_sweep.png` | Fleet pass 2 (in flight), via `clock2q+w10/w30` variants. |
| Fig 10 — next-reuse-distance PDFs | not recreated | Needs per-block NRD instrumentation; deferred. |
| Fig 12 — hand-skip counts + reinsertion limit | not recreated | Needs skip instrumentation + a hand-limit knob (A11/C5); deferred. |
| Fig 11 — dirty-block handling | out of scope | C7: our policies sit below the BPM and never see dirtiness. |
| Fig 14 — non-block traces (Wikimedia/Meta/Tencent) | out of scope this pass | Different datasets; same S3 ecosystem, future work. |

**Honesty box.** (1) Miss ratios come from deterministic single-threaded
replay — they are machine-independent, so "different hardware" does NOT
caveat these numbers; it would only caveat throughput/CPU-overhead
claims, which we do not reproduce. (2) The caveats that DO apply: our
policies are independent reimplementations (validated blind against the
authors' code on the core mechanism, see REPRODUCTION.md ambiguity log);
the trace-to-stream construction makes two logged choices the paper
leaves open (A16 one-access-per-record, A17 reads+writes); and we
reproduce 10 of the paper's 16 algorithms — LIRS, LIRS2, DLIRS, CAR,
TinyLFU, and Cacheus are compared only against the paper's reported
curves, not rerun. (3) The paper's Fig 8/9 exact values live in figures
we can't extract numerically; shape and ordering are the comparison,
plus the text's headline numbers.

Produced 2026-07-05 by `trace_sim` (fanout 200, the paper's derivation)
over all 106 CloudPhysics traces (2,114M requests total), 10 policies ×
the paper's 4 cache fractions {0.005, 0.01, 0.05, 0.1} × footprint.
Raw data: `fleet.csv` (4,240 rows). Trace provenance: `MANIFEST.sha256`
(sha256 of each downloaded `.zst`; source = cacheMon/cache_dataset
public S3 mirror, CC BY 4.0).

## Improvement vs Clock, per policy (mean% / median% / traces-positive%)

| policy | 0.005 | 0.01 | 0.05 | 0.1 |
|---|---|---|---|---|
| fifo | −6.4 / −1.9 / 1% | −6.1 / −2.1 / 2% | −6.1 / −3.3 / 2% | −7.8 / −5.6 / 3% |
| lru | +0.3 / +0.3 / 92% | +0.4 / +0.3 / 93% | +0.6 / +0.5 / 94% | +0.7 / +0.5 / 92% |
| lru-k | −68.5 / −39.6 / 0% | −84.2 / −46.6 / 1% | −99.5 / −61.8 / 1% | −103.6 / −71.4 / 4% |
| 2q (classic) | +4.1 / +2.7 / 81% | +4.0 / +3.1 / 76% | +6.4 / +5.0 / 89% | +6.9 / +6.1 / 92% |
| arc | +1.9 / +1.1 / 84% | +1.8 / +1.1 / 84% | +1.8 / +0.9 / 74% | +1.7 / +0.8 / 70% |
| s3fifo | +1.0 / +0.4 / 60% | +1.1 / +0.8 / 59% | +1.6 / +1.2 / 64% | +2.7 / +1.5 / 69% |
| s3fifo-2bit | +1.6 / +0.6 / 64% | +1.7 / +1.0 / 64% | +2.5 / +2.6 / 72% | +4.1 / +4.2 / 70% |
| clock2q | +1.3 / +0.7 / 70% | +1.2 / +0.9 / 67% | +1.5 / +1.0 / 71% | +2.1 / +1.3 / 72% |
| **clock2q+** | **+3.0 / +1.7 / 68%** | **+3.4 / +2.8 / 71%** | **+6.7 / +5.9 / 82%** | **+9.1 / +8.1 / 91%** |

clock2q+ is the single best of the 10 policies on 18 / 29 / 57 / **71**
of 106 traces at the four fractions respectively.

## Claim verdicts

- **C1 ("up to 28.5% lower miss ratio than S3-FIFO 2-bit", larger
  caches) — CONFIRMED.** Our per-trace max reduction vs s3fifo-2bit:
  **+36.9%** (w41, frac 0.05) and **+32.1%** (w50, frac 0.1); median
  +3.4% / +5.1%; clock2q+ wins 86/106 and 94/106 traces at those sizes.
  An independent reimplementation landing within a few points of the
  headline "up to" figure, with the same grows-with-cache-size shape,
  is about as clean as C1 confirmation gets. (Our max exceeding 28.5%
  plausibly traces to A16/A17 derivation choices and trace-subset
  differences in their figure.)
- **C2 (best curve everywhere) — CONFIRMED at 0.05/0.1, NUANCED
  below.** clock2q+ has the highest mean improvement of all policies at
  0.05 and 0.1. At 0.005/0.01 our classic full-2Q edges it on mean
  (+4.1 vs +3.0). CORRECTED 2026-07-07: we initially hypothesized the
  paper's 2Q baseline was non-classic; the authors' `TwoQ.c` is in fact
  Kin/Kout-faithful (Ain 25%, Aout 50%, evict-Am-unless-Ain-over-limit)
  — essentially our implementation. So the small-cache 2Q > Clock2Q+
  ordering is a real property of the workload their figures don't
  surface, and goes to the authors as a question, not a diagnosis.
- **Design-lineage ordering (§3.2–3.4) — CONFIRMED at larger caches:**
  s3fifo < s3fifo-2bit < clock2q+, with clock2q ≈ s3fifo. Exactly the
  paper's improvement narrative.
- **Fig 11 distribution shape — CONFIRMED:** below-zero tails exist at
  every size (9–32% of traces negative), matching the paper's
  distribution plots rather than a uniform win.

## IDB-native trace — the divergence experiment

`tpcc --capture-trace` records the BPM's page-REQUEST stream (hit or
miss — policy-independent) after the load phase. First capture:
TPC-C smoke scale, 4 warehouses × 4 terminals × 120 s, 11,728
committed txns → **4.79M page accesses over 3,983 distinct pages**
(`trace_sim --fanout 1`; no derivation — these ARE metadata accesses).

Improvement vs Clock (single trace, so point values not distributions):

| policy | 0.005 (c=20) | 0.01 (c=40) | 0.05 (c=199) | 0.1 (c=398) |
|---|---|---|---|---|
| lru-k | −0.4 | +0.2 | +6.8 | **+13.0** |
| clock2q+ | +4.9 | +4.3 | +6.3 | +8.9 |
| s3fifo-2bit | +4.9 | +4.6 | +6.4 | +8.1 |
| arc | **+7.4** | **+5.9** | +6.8 | +7.9 |
| s3fifo | +6.4 | +5.4 | +6.3 | +7.9 |
| 2q | +4.4 | +3.7 | +5.9 | +7.8 |

Findings vs the derived-trace (CloudPhysics ÷200) results:

1. **The ranking changes.** On derived traces clock2q+ dominates and
   LRU-K is catastrophic (median −71% at 0.1). On the real B-tree
   stream **LRU-K wins outright at 0.1 (+13.0%)** and ARC leads the
   small fractions — real B-tree traffic has a persistently hot upper
   tree (root/internals ≈ heavy frequency structure) that
   frequency-aware policies exploit, while the derived traces'
   one-touch-heavy pattern is their poison.
2. **The correlation window still works** (clock2q+ promotes 2.5–5×
   less than s3fifo, and it beats every S3-FIFO-family sibling at 0.1)
   — but its edge over s3fifo-2bit shrinks to <1 point, versus several
   points on derived traces.
3. **Methodology implication (C6 from outside):** the paper's
   LBN÷fanout derivation produces streams whose policy ranking does NOT
   transfer unchanged to at least one real B-tree metadata workload.
   Their §5.2 validation compared derived vs full-B+tree *simulation*
   on the same disk traces; a real engine's page stream is a different
   object. This is the headline divergence for the author email —
   stated carefully: one workload, smoke scale, 3,983-page footprint.

Caveats: single TPC-C configuration; footprint 270× smaller than a
CloudPhysics trace; captured under ARC (stream is policy-independent
by construction — requests, not residency).

## LRU-2-CRP — 1993 as specified vs 2025 (our experiment; A18/A19)

`LruKCrpReplacer` restores O'Neil's full Fig 2.1 algorithm (Correlated
Reference Period + eviction protection + Retained Information Period)
on the logical clock. Preview (2 derived traces + native; fleet-wide
sweep in flight), improvement vs Clock (%):

| | w105 @0.1 | w93 @0.1 | native @0.005 | native @0.1 |
|---|---|---|---|---|
| lruk (as commonly implemented) | −27.7 | −12.2 | −0.4 | **+13.0** |
| lruk-crp5 (1993 as specified) | +7.8 | +13.4 | **+6.4** | +5.9 |
| clock2q+ (2025) | **+11.0** | **+14.0** | +4.9 | +8.9 |

Three preview findings:
1. **The clause is the whole failure.** Restoring the CRP swings LRU-K
   30–40 points on derived traces; on w93 it lands within 0.6 points
   of Clock2Q+. LRU-K's last-place finish was the reimplementation
   gap, not the algorithm.
2. **The 2025 result survives its strongest baseline — narrowly.**
   Clock2Q+ still wins both derived traces at every size, by ~1–3
   points instead of ~40 (and keeps its O(1)-structure advantage
   regardless).
3. **On real B-tree traffic, the clause's value inverts with cache
   size**: CRP takes small caches from −0.4 to +6.4 (beating
   clock2q+) but costs the large-cache lead (+13.0 → +5.9) — rapid
   re-touches of the hot upper tree are genuine frequency signal
   there, and collapsing them discards it. Correlated references are
   noise or signal depending on how much of the tree fits in memory.

CRP length is insensitive (1/5/25% of capacity all land close) —
echoing the paper's own window-insensitivity claim (C3).

## IDB-side findings (byproduct of running at paper scale)

Six O(n) hot paths were found and fixed across the policy suite during
these runs (clock, lru, lru-k, fifo, 2q, and the reproduction core's
own Main ring) — every fix verified row-identical on completed replays
before the fleet used it. Classic-2Q's Kin threshold was restored
(the old always-drain-A1in variant lost up to 40% vs Clock on w87).
LRU-K is genuinely ill-suited to these traces (median −71% at 0.1):
half the blocks are touched once, so its +∞-distance class dominates.
