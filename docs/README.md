# Documentation map

What to read, and when. Start with the two orientation files, then reach for a
reference doc only when its topic is your task.

## Start here

- [`../CONTEXT.md`](../CONTEXT.md) — **shared vocabulary.** The engines,
  policies, seams, planners/executors, benchmark terms. Read first.
- [`../README.md`](../README.md) — project entry point, dev-loop commands, test
  tiers, pre-commit gates.

## Active reference (read when…)

| Doc | Read when… |
|-----|------------|
| [`plan-tpch.md`](plan-tpch.md) | Working on TPC-H (H1–H6): the capability ladder, per-phase result notes, open levers. |
| [`stability.md`](stability.md) | Reasoning about the testing/verification strategy (deterministic concurrency, fault injection). Complements `../ISSUES.md`. |
| [`seams.md`](seams.md) | Touching a swappable boundary (engine/policy/disk) and deciding static vs `dyn`. |
| [`plan-versioned-indexes.md`](plan-versioned-indexes.md) | Picking up the (unscheduled) versioned-secondary-index design. |
| [`tpch-timings.md`](tpch-timings.md) | Wanting query-shape timings. **Stale** — pre-H6; needs a re-time. |
| [`../ISSUES.md`](../ISSUES.md) | Checking or adding an open quality item — the live tracker. |

## Component docs (co-located with what they describe)

- [`../testkit/README.md`](../testkit/README.md) — the conformance-matrix
  crate; **read before writing tests that touch a swappable axis.**
- [`../tests/slt/README.md`](../tests/slt/README.md) — sqllogictest corpus.
- [`../fuzz/README.md`](../fuzz/README.md) — cargo-fuzz targets.
- [`../benches/RESULTS.md`](../benches/RESULTS.md) — benchmark results.
- [`architecture.svg`](architecture.svg) — layer diagram.

## Archived (git history)

Completed-phase plans, one-off review logs, and superseded investigations were
removed in the "docs clean up" commit — their durable takeaways live in the
code and in the session memory index. Retrieve any of them with:

```bash
git log --diff-filter=D --name-only -- 'docs/*.md' plan.md   # find the delete
git show <commit>^:docs/<name>.md                            # print old content
```

Removed: `plan.md` *(was gitignored/local)*, the completed `plan-*.md`
(predicate-pushdown, test-tiers, build-times, workspace-split, phase16-tpcc),
`phase_17_plan.md`, the `build-times/` sub-tree, the optimizer/executor review
logs (`optimizer-review.md`, `exec-optimizer-review.md`, `review-fix-plan.md`),
the Phase-16 TPC-C results (`tpcc-results.md`, `tpcc_fidelity.md`), and the
superseded `scalability-investigation.md`.
