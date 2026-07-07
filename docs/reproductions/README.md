# Paper Reproductions — Pipeline & Target Queue

Turn recent (6–24 month) DB/systems papers — preferably without released
code — into faithful implementations on InterchangeDB's harness, send
findings to authors (PhD-student first author preferred), and publish a
writeup per paper regardless of author response.

> Naming rule: internal phase numbers never appear in these docs, the
> writeups, or author emails — this directory's contents may go public.

## Per-paper process (the gate sequence)

1. **Two full reads before any code.** First for the idea; second
   adversarial — list every parameter, constant, workload assumption,
   hardware detail; check tables for internal consistency and references
   for existence.
2. **Claims table.** Every empirical claim: claim | paper § | supporting
   experiment | what we must observe to confirm. This is the writeup
   skeleton.
3. **Section-to-code traceability.** Doc comments cite paper sections
   (`/// Implements the ghost-promotion rule from §4.2`). Each paper's
   `REPRODUCTION.md` maps section → file → test so an author can verify
   the implementation in ten minutes.
4. **Validate, then diverge.** Reproduce the paper's exact
   workload/parameters and match their numbers (or document why not) —
   the validation gate. Only after it passes, run the technique on
   IDB-native workloads. Divergences after the gate are findings;
   before it, they are probably our bugs.
5. **Ambiguity log.** Every underspecified detail where we chose,
   recorded with rationale. Half the writeup's value to authors.
6. **Property tests on the paper's invariants.** Any theorem/invariant
   becomes a proptest.

Contact protocol: email the student first author; lead with concrete
numbers, divergences prominently; link the REPRODUCTION.md.

## Target queue (status as of 2026-07)

| # | Paper | Tier | Status |
|---|---|---|---|
| 1 | **Clock2Q+** (arXiv 2511.21958, Zhai; Yang group) | 1 — eviction | **ACTIVE — first target.** Verified no code released. See `clock2q-plus/`. |
| 2 | DynamicAdaptiveClimb (arXiv 2511.21235) | 1 — eviction | Demoted to *critical-reproduction* candidate: adversarial read found impossible IPC figures, suspicious rank-order uniformity, fabricated-looking references, CACHEUS/libCacheSim misattributions. Algorithm trivially implementable; do the "algorithm real, numbers not" writeup AFTER Clock2Q+. |
| 3 | ScaleEvict (DaMoN '25 Best Paper) | 1 — eviction | Unverified code status; RDMA context heavy but eviction logic may reproduce single-node. |
| 4 | 3L-Cache (FAST '25) | 1 — eviction | Likely has artifact; use as harness calibration run (reproduce WITH their code as ground truth). |
| 5 | LeCaR / CACHEUS | 1 — meta | Not reproductions — feature candidates: meta-policy over IDB's six policies. |
| 6 | CorrBound (SIGMOD '26) | 2 — cardinality | Bounds-based, no training pipeline; start when optimizer work resumes. |
| 7 | ZeroCard (arXiv 2510.07983) | 2 — cardinality | Pluggable estimator claim; verify code status. |
| 8 | JOB-Complex (arXiv 2507.07471) | 2 — benchmark | Run against IDB's cost-based planners; low effort. |
| 9 | "Still Asking: How Good Are Query Optimizers, Really?" (Leis, PVLDB 18) | 2 — methodology | North-star for the planner-comparison harness's external validation. |
| 10 | Cache is King / cache_ext eBPF (arXiv 2502.02750) | 3 — standalone | Linux-box side project; kernel-eBPF vs userspace-trait swap-architecture writeup angle. |
| 11 | ScaleCache | 3 — standalone | Per-core buffer pools; relates to the BPM shuttle work. |

## Writing outputs

- One blog post per reproduction (validation story + divergences +
  ambiguity log).
- Capstone once several Tier-1 papers land: "N eviction papers, one
  harness" — the cross-policy comparison table.
