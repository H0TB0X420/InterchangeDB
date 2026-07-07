# Clock2Q+ Reproduction

**Paper:** *Clock2Q+: A Simple and Efficient Replacement Algorithm for
Metadata Cache in VMware vSAN.* arXiv 2511.21958 (Nov 2025).
First author: Yiyan Zhai (CMU). Senior author: Juncheng Yang
(S3-FIFO / SIEVE / libCacheSim).

**Why this repo:** it is a B-tree metadata/buffer-cache paper evaluated
only on *derived* synthetic metadata traces (data-trace LBN ÷ fanout,
fanout = 200). InterchangeDB has a real B-tree producing real metadata
access patterns — after the validation gate we can run the policy on
evidence the authors could not produce.

**Algorithm (confirmed against the paper, 2026-07-04 read):** S3-FIFO
base — Small FIFO at **10% of cache size**, Main Clock at **90%**, Ghost
FIFO of evicted-block ids at **50% of total cache size** (S3-FIFO uses
100%) — plus the paper's innovation: a **correlation window = 50% of the
Small FIFO (5% of total cache)** at the insertion side, inside which
hits do NOT set the reference bit. Small-FIFO eviction: ref set →
promote to Main Clock; ref clear → id goes to Ghost. Ghost hit on a
later miss → insert directly to Main. Main evicts by standard clock
sweep. (Design lineage in the paper: 2Q → Clock2Q → Clock2Q+, §3.1–3.4.)

---

## Claims table

Skeleton — populated during the two-read pass (read 2 is adversarial).
Every empirical claim in the paper gets a row.

| # | Claim | Paper § | Supporting experiment | What we must observe to confirm |
|---|---|---|---|---|
| C1 | Up to **28.5% lower miss ratio** than S3-FIFO(2-bit) on derived metadata traces | §5.3 | CloudPhysics-derived metadata traces, 4 cache sizes | Our Clock2Q+ vs our/reported S3-FIFO at the same sizes shows comparable relative gap |
| C2 | Best miss-ratio curve **across the entire 1%–100% cache-size range** (strictly below all baselines) | §5.3, Fig 9 | full MRC sweep | Our MRC lies below FIFO/Clock/LRU/ARC/2Q at all tested sizes |
| C3 | Miss ratio **insensitive to window size** (10/30/50% of Small all improve; larger slightly better at small caches) | §5.6, Fig 13 | window sweep | Reproduce the 3-point sweep; same insensitivity shape |
| C4 | Ghost at **50% of cache ≈ 100%** miss ratio, at half the ghost memory | §5.6 | ghost sweep | 50% vs 100% curves within noise |
| C5 | Clock hand sweep: default mean **<2 skipped blocks**/eviction; hard limit 10 costs only slight miss-ratio increase | §5.5.2 | hand-limit sweep {10,100,1000,∞} | Instrument skip counts; reproduce both findings |
| C6 | Metadata-trace derivation (LBN÷fanout, fanout 200) is faithful: **<0.01% difference** vs full B+ tree simulation | §5.2 | derivation validation | Sanity-check our derivation tool the same way (bonus: against our REAL B-tree) |
| C7 | Skipping dirty pages at eviction (vs migrating them) has **negligible** miss-ratio impact | §5.5.1 | dirty-handling comparison | Out of scope v1 (policy below BPM never sees dirtiness) — cite, don't reproduce |
| C8 | Block-movement counts (Table 1): Clock2Q+ promotes Small→Main **4.3× less** than S3-FIFO (20,762 vs 88,529), demotes more to Ghost (186,959 vs 126,897), rescues slightly more from Ghost (33,068 vs 29,909) | Table 1 | movement counters | Instrument the same three counters in our impl — a strong, figure-free validation instrument |
| — | Setup facts: CloudPhysics = 106 traces / 7 days / 2,114M requests / 82 TB; cache sizes {0.005, 0.01, 0.05, 0.1}×footprint; simulator libCacheSim; metric `(MR_Clock − MR_algo)/MR_Clock` | §5.1 | — | our runner reports the same metric at the same sizes |
| — | Unverifiable-as-stated (qualitative, no numbers): "low CPU overhead", "lock contention never observed as bottleneck", production resizing costs | §4 | none given | Not reproduction targets; note in writeup |

## Section → file → test traceability

Populated as code lands. Format is fixed so authors can verify quickly.

All code in `src/buffer/replacer/clock_two_q_plus.rs`; tests are that
file's `mod tests` unless noted.

| Paper § | Rule / mechanism | Code | Test |
|---|---|---|---|
| §3.3 | Small-FIFO insertion, ref clear | `record_access` INSERT arm | `new_blocks_enter_small_with_ref_clear` |
| §2.2, Fig 5 | Correlation-window ref-bit suppression | `in_correlation_window` + HIT arm | `in_window_hit_does_not_set_ref`, `post_window_hit_sets_ref`, `window_boundary_is_strict` |
| §3.3/3.4 | Small→Main promotion (ref set) / Small→Ghost demotion (ref clear) | `evict_from_small` | `small_eviction_promotes_referenced_and_ghosts_unreferenced` |
| §3.4 | Main clock sweep (second chance; Main evictions not ghosted) | `evict_from_main` | `main_clock_second_chance` |
| §3.4 | Ghost-hit reinsertion → Main | `record_access` ghost arm | `ghost_hit_inserts_to_main` |
| §3.4, A7 | Ghost sizing (50% of total) + FIFO trim | `ghost_push` | `ghost_trims_to_capacity_fifo` |
| §3.3 | S3-FIFO(1-bit) baseline = window 0, ghost 100% | `Clock2QPlusReplacer::s3_fifo` | `s3fifo_sets_ref_on_first_rehit` |
| §3.3 | S3-FIFO 2-bit: promotion needs two re-references (A10) | `Clock2QPlusReplacer::s3_fifo_2bit` (`promote_threshold: 2`) | `s3fifo_2bit_requires_two_rehits_to_promote` |
| §3.2 | Clock2Q predecessor: 2Q fractions (Small 25%), clock Main, ghost 50%, no window | `Clock2QPlusReplacer::clock_two_q` | `clock2q_uses_2q_fractions_and_no_window` |
| Table 1 | Movement counters (C8) | `MovementStats` | counter asserts in the eviction/ghost tests |
| — (BPM contract) | Never `None` while an evictable frame exists (incl. the promote-into-searched-queue hole) | two-round `evict` | `lone_refset_small_entry_is_still_evictable`, `bpm_pattern_never_starves_*`, `tests/config_equivalence.rs` |

## Trace acquisition

The paper's dataset is the 106 one-week CloudPhysics vscsi traces
(Waldspurger et al., FAST '15). Two public sources:

1. **SNIA IOTTA** (canonical): browser click-through license, cookies
   required — not scriptable.
2. **cacheMon/cache_dataset S3 mirror** (Yang lab — the paper's own
   group; CC BY 4.0, no click-through): `s3://cache-datasets/
   cache_dataset_oracleGeneral/2015_cloudphysics/w01..w106.oracleGeneral.bin.zst`,
   106 objects, ~9.0 GB compressed total. **We use this.**

Fetch: `docs/reproductions/clock2q-plus/fetch_traces.sh [w01 ...]` →
`data/traces/` (git-ignored), sha256s appended to
`data/traces/MANIFEST.sha256`.

Format verification chain (2026-07-04): libCacheSim bundles the same
sample trace in three formats (`cloudPhysicsIO.{vscsi,oracleGeneral.bin,txt}`,
113,872 records). Our vscsi parser's LBN stream matches the `.txt` ground
truth record-for-record; the oracleGeneral `obj_id` stream equals the
vscsi LBN stream record-for-record; and `trace_sim` produces identical
miss ratios and movement counters from either binary format. So the S3
mirror's oracleGeneral files carry the raw vscsi LBNs the paper's
derivation starts from.

## Validation gate (must pass before IDB-native runs)

- [x] CloudPhysics traces acquired (public S3 mirror; SNIA IOTTA is the
      click-through alternative) and checksummed — w105/w87/w93 fetched
      first; full 106-trace set is one `fetch_traces.sh` invocation away.
- [x] Metadata-trace derivation tool matches the paper's method
      (LBN ÷ fanout, fanout = 200) — `trace_sim --fanout`, unit-tested;
      format verified against libCacheSim's three-format sample twin.
- [x] Miss-ratio curves for Clock2Q+ AND the baselines across the
      paper's cache-size points — full 106-trace fleet completed
      2026-07-05, zero failures (`results/fleet.csv`, 4,240 rows).
- [x] Curves match the paper's claims (see `results/README.md`):
      C1 confirmed (max +36.9%/+32.1% vs s3fifo-2bit at 0.05/0.1 vs the
      paper's "up to 28.5%", same grows-with-cache shape); lineage
      ordering confirmed; Fig-11 below-zero tail confirmed. One
      documented divergence: classic full-2Q (Kin-faithful) beats
      clock2q+ on MEAN at the two smallest sizes — the paper's 2Q
      baseline appears weaker than Johnson & Shasha's; flag to authors.

Only then: IDB-native B-tree metadata traces (captured from the real
BPM under TPC-C and YCSB-style workloads) — the novel contribution for
the author email.

## Ambiguity log

Decisions we had to make where the paper underspecifies. Seeded from the
first planning read; every entry gets a rationale and, where possible, a
sensitivity check.

| # | Ambiguity | Our choice | Rationale / sensitivity |
|---|---|---|---|
| A1 | Ref-bit semantics exactly AT the correlation-window boundary (is the boundary entry inside or outside the window?) | **RESOLVED (2026-07-07, authors' code): in-window iff insertions-since < window (strict)** — exactly our provisional choice. `Clock2QPlus.c` (libCacheSim, landed 2026-05-04, consulted only AFTER our fleet results): `n_obj_admit_to_fifo - insertion_time >= corr_window_size` ⇒ counted. | Blind convergence: A1, A2, and A13 all match the authors' implementation. |
| A2 | Window position: insertion end or eviction end of the Small FIFO | **RESOLVED (read, 2026-07-04): insertion end.** The paper's Fig 5 says a block "still within" the window measured "from the head" — i.e. blocks BEGIN in the window and age out of it; the paper's head = insertion side. Matches the mechanism (correlated re-touches arrive just after insertion, §2.2's m₄,m₄ example). | Settled; no sensitivity needed. |
| A3 | Small FIFO so small the window rounds to 0 or the whole queue | Clamp window length to `[0, small_len]`; degenerate ends = plain S3-FIFO / never-set-ref respectively | Paper silent (their caches are large). Unit-test both ends. |
| A4 | Dirty-block handling | Out of scope v1 — IDB's BPM handles dirty frames above the policy. NOTE: the paper's own answer is "skip dirty pages when selecting eviction candidates" (§5.5.1) and measures the impact as negligible (C7) | Cite in writeup; no reproduction needed. |
| A5 | Ghost ∩ resident tie-breaking | Enforce invariant *ghost ∩ resident = ∅* (proptest); remove from ghost on reinsertion | Mirrors ARC's B-list discipline already in-tree. |
| A6 | Ghost fingerprint: full PageId vs hashed | Full PageId (u32) — no collisions at our scale; paper's fingerprint is a space optimization | Note for authors; zero behavioral divergence. |
| A7 | Ghost sizing basis | **RESOLVED (read): 50% of TOTAL cache size** (not 50% of Main). S3-FIFO baseline = 100% of cache size. **NEW (2026-07-07): the authors' own `Clock2QPlus.c` defaults ghost to 90%** — a paper-vs-code discrepancy on their side (likely inherited from S3-FIFO's default). We follow the PAPER (50%). Their C4 finding (50% ≈ 100%) bounds the impact; author-email question. | Handoff said "50% instead of 100%" ambiguously; paper §3.4 pins it. |
| A8 | Ghost-hit insertion position within Main Clock (at hand? behind hand?) and initial ref-bit value | OPEN — propose: insert behind the hand (maximal residency before first sweep), ref clear | Paper says only "directly promoted to Main." Ask authors; sensitivity-check at validation if curves diverge. |
| A9 | Small-FIFO eviction trigger (evict-from-Small when it exceeds its 10% target vs other condition) | OPEN — propose: on insertion overflow, evict from Small if `small_len ≥ small_cap`, else from Main (S3-FIFO discipline) | Text describes per-queue eviction rules but not the selector; S3-FIFO precedent is the natural reading. |
| A10 | S3-FIFO 2-bit variant's exact rules | **RESOLVED (read, 2026-07-04):** §3.3 — "Blocks in S3-FIFO 2-bit need to be re-referenced at least twice before they can be moved to the Main Clock." Small-side saturating counter, promotion threshold 2. Implemented as `promote_threshold` on the shared core; both variants registered. | The window (clock2q+) and the 2-bit threshold are two competing answers to correlated references — the head-to-head IS the paper's headline (C1). |
| A11 | Clock-hand movement limit in the HEADLINE results: production uses limit 10 (§5.5.2), but were the §5.3 miss-ratio curves run with limit 10 or ∞ in libCacheSim? | Assume ∞ for validation curves (simulator default), expose the limit as a field, reproduce the §5.5.2 sweep separately | The §5.5.2 finding (limit 10 ≈ ∞) says it barely matters, but pin it for exactness. |
| A12 | Predecessor "Clock2Q" is cited only as **patents** (no academic paper) | We reproduce Clock2Q+ only; the intermediate design exists in this paper's §3.2 narrative alone | No action; noted for the writeup's lineage section. |
| A13 | Window "distance from head" measured how, exactly, in an implementation with mid-queue removals? | Insertion-sequence arithmetic: `age = newest_seq − seq(entry)`, in-window iff `age < window_entries`. Exact under pure FIFO motion; over-estimates age (conservatively shrinks the window for older peers) after rare mid-queue removals (page deletes, promotions). **CONFIRMED (2026-07-07): the authors' code uses the same admission-counter arithmetic**, not positional counting. | O(1) on the hit path — the alternative (positional counting) re-creates the O(n)-under-mutex ARC bug our own profiling found. C3's insensitivity finding says the error is noise. |
| A14 | Eviction control flow: the paper never states it as a loop, nor whether the Small scan continues after a promotion; and ineligible entries (their "dirty", our "pinned") interact with candidate selection | Skip ineligible entries in place; continue the Small scan past promotions; cross-queue fallback when the preferred queue yields nothing; `evict` runs TWO bounded rounds (a round-1 promotion can move the only candidate into an already-searched queue — found by our conformance differential at pool=4) | **Paper cross-check (2026-07-04):** the fallback IS the paper's own §4.1.3 behavior ("if all scanned entries are dirty… checks the Main Clock"); the loop-until-freed shape is S3-FIFO's (SOSP '23) definition that this paper inherits implicitly — our two rounds are that loop with an explicit, provably sufficient bound. Divergence: §5.5.2's "forcibly evicted" backstop after the reinsertion limit is inapplicable to pins (BPM contract: a pinned frame is NEVER evictable); for ref-bit reinsertion our 2×len sweep bound is equivalent to it. Behavior on unpinned workloads (all validation traces) is identical to the paper's. |
| A15 | §4.1.3 claims skipping a dirty page is "equivalent to reinserting [it] at the head" — but for the correlation window those differ: reinsert-at-head puts an old block back INSIDE the window (re-suppressing its ref bit); skip-in-place preserves its age | Skip in place (age preserved) | A real underspecification interacting with the paper's own novelty; the equivalence claim holds only if the window is position-based AND reinsertion resets position — the paper doesn't say which. Flag prominently in the author email. Out of scope for validation (traces have no dirty/pinned blocks). |
| A16 | Derivation input granularity: a vscsi record has an LBN *and a request length* — does each record contribute ONE metadata access (its LBN ÷ fanout) or one per data block it spans? | One access per record (`trace_sim` reads only the LBN field) | §5.2 describes the conversion as per-LBN ("convert an LBN in the data trace… by dividing"); no mention of expanding by length. Multi-block requests hitting one B+ tree leaf is also exactly the correlated-reference story (§2.2) — expansion would manufacture correlation the window then hides, biasing toward the paper's conclusion; the conservative reading avoids that. Sensitivity check possible later (parse `len`, expand, compare). |
| A17 | Are writes included, or reads only? | Both (cmd field not filtered) | §5.1/§5.2 never filter by op; vSAN's metadata lookup happens for reads and writes alike. The libCacheSim oracleGeneral conversion (the authors' own toolchain) also keeps both — its records carry no op field at all, so the paper's simulator *could not* have filtered by op when consuming these files. |
| A18 | *(LRU-2-CRP experiment, ours)* O'Neil's CRP/RIP are wall-clock timeouts; trace replay has no wall clock | Both measured on the logical access clock: CRP in accesses, RIP as a retained-entry count | Mirrors how Clock2Q+'s window is measured in insertions — keeps CRP-vs-window apples-to-apples. O'Neil §2.1.1 himself floats non-clock variants (transaction boundaries), so logical time is within the paper's spirit. `crp = 0` defined as protection-off (discrete clock freezes between accesses; a strict `> 0` would shield the just-touched frame) — making crp = 0 exactly plain LRU-K + RIP. |
| A19 | *(LRU-2-CRP experiment, ours)* CRP/RIP lengths are unspecified tuning parameters; and Fig 2.1 never handles "every candidate inside its CRP" | Sweep CRP ∈ {1%, 5%, 25%} of capacity; RIP fixed at capacity (ghost-100% analog). When all evictable frames are protected, protection yields (evict by K-distance ignoring CRP) — NOTE'd in code | The fallback is a BPM-safety deviation (a None with unpinned frames starves the pool — the A14 lesson); O'Neil's buffers dwarf his period so the case never arises for him. |

## Status

- [x] Target verified (no released code), planning read done (from
      handoff session).
- [x] Read 1 (2026-07-04, via arXiv HTML) — claims table filled from
      the text; A2/A7 resolved, A8–A10 opened.
- [x] Read 2 (adversarial, 2026-07-04): references all verified real and
      correctly attributed (2Q/VLDB'94, S3-FIFO/SOSP'23, LIRS/SIGMETRICS'02,
      Clock-Pro/ATC'05, CAR/FAST'04, ARC/FAST'03, SHARDS-CloudPhysics/FAST'15,
      SIEVE/NSDI'24, TinyLFU/TOS'17 — contrast DynamicAdaptiveClimb).
      Table 1 internally consistent with the thesis (fewer promotions,
      more demotions, more ghost rescues; totals differ because miss
      counts differ — expected). New A11/A12 opened; C8 added as a
      figure-free validation instrument. Residual: exact per-figure
      curve values need the PDF figures at validation time (HTML text
      carries only headline numbers).
- [x] Policy lineage implemented from one parameterized core
      (2026-07-04): `clock2q+`, `clock2q`, `s3fifo`, `s3fifo-2bit` — all
      enrolled in the conformance matrix (10-policy correctness-
      neutrality differential green). Baseline coverage: 10 of the
      paper's 16 evaluated algorithms (ours: Clock, ARC, 2Q, LRU, FIFO,
      LRU-K + the four above); LIRS/DLIRS/CAR/CART/Clock-Pro/TinyLFU/
      LeCaR/SIEVE are deliberately NOT reimplemented — they are
      libCacheSim stock baselines; we compare against the paper's
      reported curves for those.
- [x] Scaffolding complete (2026-07-04): `src/bin/trace_sim.rs` —
      vscsi v1/v2 + oracleGeneral + txt parsers (each verified against
      libCacheSim's three-format sample twin), LBN÷fanout derivation,
      pure-cache replay (no pins — libCacheSim conditions) over any
      registered policy, CSV with miss ratio, the paper's
      `(MR_Clock−MR_algo)/MR_Clock` Improvement, and Table-1 movement
      counters (now surfaced through `EvictionPolicy::movement_stats`).
      Trace acquisition scripted (`fetch_traces.sh`) + checksummed.
      A16/A17 opened (derivation granularity; reads+writes).
- [x] Paper-scale replay exposed three IDB implementation defects
      (2026-07-04) — the reproduction's first material payoff:
      1. `ClockReplacer::evict` was O(n) (`Vec::remove` + full index-map
         fixup). Rewritten as an intrusive circular ring (all ops O(1));
         victim sequences verified bit-identical on 3×3.4M-access
         replays; ~68 min → ~25 s for 12 replays.
      2. `LruKReplacer::evict` was an O(n) max-scan on wall-clock
         timestamps (nondeterministic under `Instant` ties). Rewritten
         on a logical access clock + `BTreeSet` victim order (O(log n),
         deterministic); miss counts match old impl to 5 significant
         figures (deltas = the old impl's own tie nondeterminism).
      3. `TwoQReplacer` was NOT classic 2Q: no Kin threshold (evicted
         A1in-first unconditionally → Am swallowed the cache → new pages
         got zero residency), ghost at 100% of capacity (paper: Kout
         50%), O(n) `retain` on every Am hit. Fixed to Johnson & Shasha
         full-2Q (Kin 25%, Kout 50%, `OrderedList` queues): w87 went
         from −40% vs Clock to −3.6%…+4.4%, and fixed `2q` now sits
         beside `clock2q` — the neighbor the paper's lineage predicts.
- [x] First real data points (w105/w87/w93, paper fractions): clock2q+
      +11.0%/+2.8%/+14.0% vs Clock at 0.1×footprint; w87 negative at
      smaller sizes (the paper's own Fig 11 below-zero tail); Small→Main
      promotion suppression 20–34× vs s3fifo (Table-1 signature, C8).
- [ ] Full 106-trace fleet sweep (in flight 2026-07-04: 10 policies ×
      4 fractions, fetch→sha256→replay→delete pipeline).
- [ ] Implementation sessions 1–4 (see `implementation-plan.md`).
- [ ] Validation gate.
- [x] IDB-native divergence runs — first capture + replay 2026-07-05
      (`results/tpcc_btree_native.csv`, analysis in `results/README.md`).
      Instrumentation: BPM page-request recorder (`start_trace_capture`
      / `take_trace_capture`, AtomicBool-gated, bounded, off-path) +
      `tpcc --capture-trace` + trace_sim `.pages` format. Headline: the
      derived-trace policy ranking does NOT transfer — LRU-K flips from
      catastrophic to best-at-0.1 (+13.0%), ARC leads small fractions,
      clock2q+ still best of its family but its window edge shrinks
      to <1 point over s3fifo-2bit.
- [x] Post-hoc diff against the authors' code (2026-07-07). CORRECTION
      to our earlier status: a reference implementation HAS existed —
      `Clock2QPlus.c` landed in libCacheSim 2026-05-04, before this
      project started; our "no released code" check missed it (we
      searched for a standalone release, not the simulator tree). We
      did not consult it until after the fleet results. The diff:
      window mechanics (A1 boundary, A2 orientation, A13 arithmetic)
      match our blind choices exactly; promotion threshold and
      ghost-hit-to-Main match; no hand limit (A11 assumption
      confirmed); their ghost defaults to 90% vs the paper's 50% (A7
      note — author question). Their `TwoQ.c` is Kin-faithful classic
      2Q, retracting our "weak 2Q baseline" hypothesis — the
      small-cache 2Q > Clock2Q+ ordering in our fleet data stands as a
      workload property, reframed as an author question.
- [x] 1993 provenance verified against the SIGMOD '93 text (O'Neil
      §2.1.1): the Correlated Reference Period collapses correlated
      references (history not credited, page protected from eviction
      during the period), motivating example is B-tree leaves under
      concurrent transactions, and O'Neil credits Robinson &
      Devarakonda 1990 ("Factoring out Locality"). The correlation
      window's lineage is 1990 → 1993 → 2025.
- [ ] Curve-recreation pass: reproduce each paper figure with our 10
      of their 16 policies (see results/, in progress 2026-07-07).
      Landed so far: Fig 8(a); Fig 9 EXACT (clock2q+ strictly below at
      all 18 points on w56, both panels). In flight: Fig 8(b) data-trace
      fleet + Fig 13 window sweep.
- [ ] QUEUED — LRU-2-CRP experiment (ours, not the paper's): implement
      O'Neil's Correlated Reference Period faithfully (history
      collapsing + eviction protection + Retained Information Period,
      Fig 2.1 of the 1993 paper) on the logical clock, sweep CRP
      lengths, run fleet + native. Tests whether the 2025 result beats
      the 1993 literature AS SPECIFIED rather than as commonly
      implemented — closes the loop the writeup's footnote opens. Two
      new ambiguities to log: logical-time CRP (O'Neil's is wall-clock)
      and CRP/RIP length choices.
- [ ] Writeup + author email.
