# Plan: TPC-H capability

**Status:** decisions settled 2026-07-18 (DATE type, H5 validation, H3b
included, Push native sink) — ready to plan H1 line-by-line.
**Goal:** execute all 22 TPC-H queries correctly on generated data at small
scale factors, through every planner and both execution models.
**Non-goals (recorded, not forgotten):** competitive analytical performance,
subquery decorrelation, sort-based aggregation, PK-less tables (SQLite-corpus
concern only — all eight TPC-H tables declare primary keys, two composite,
and composite PKs already run end-to-end under TPC-C).

## Verified capability matrix (2026-07, file:line evidence)

Have:
- Composite PKs (binder `table_pk` path; TPC-C exercises them end-to-end).
- Column-list INSERT with NULL-fill (`binder.rs:479`); `INTEGER` alias
  (`binder.rs:1272`). Proven by `tests/slt/insert.slt` against select1.test's
  own statements.
- Joins: NLJ / Hash / INLJ / merge; three planners; two execution models.
- Ungrouped aggregates: COUNT(*) / COUNT / COUNT(DISTINCT) / SUM / MIN /
  MAX / AVG over *column indices* (`AggregateSpec`, `logical.rs:141`).
- Expression arithmetic with 3VL compilation (`expr.rs:35` — Literal, Column,
  Parameter, BinaryOp ± × ÷); predicates And/Or/Not over six comparators.
- ORDER BY, LIMIT, Decimal, `Value::Timestamp(i64 µs)`.

Missing (each is a phase below):
- GROUP BY / HAVING — `logical.rs:61-62` documents the designed slot
  ("lands alongside `HashAggregate`'s grouped path"); binder rejects it.
- Expressions in projections (`projection: Vec<usize>` — bare indices) and
  inside aggregates (`AggregateSpec::Sum(usize)`).
- DATE literals + interval arithmetic; EXTRACT.
- BETWEEN, IN-list, LIKE, CASE, IS [NOT] NULL.
- LEFT OUTER JOIN (binder accepts Inner/Cross only, `binder.rs:381-383`).
- Subqueries: scalar, IN, EXISTS; correlated forms.

## Query → feature demand (which rungs unlock which queries)

- H1+H2+H3 alone: Q1, Q3, Q5, Q6, Q7, Q8, Q9, Q10, Q12, Q14, Q19 (11 queries).
- H3b (outer join): Q13.
- H4a (uncorrelated scalar/IN subqueries): Q11, Q15 (view rewritten), Q16, Q18.
- H4b (correlated / EXISTS): Q2, Q4, Q17, Q20, Q21, Q22.

## Phases

### H1 — grouped aggregation (GROUP BY + HAVING)

The designed slot: `LogicalPlan::Select` already documents "projection and
aggregates both non-empty = GROUP BY semantics"; `HashAggregate` grows its
grouped path (hash on key columns → per-group accumulators). HAVING binds as
a predicate over the aggregate's output row — a spine Filter above
HashAggregate, nothing new at execution. Binder lifts its rejection.

Touch points beyond binder/executor: `SpineParts` gains group keys
(`normalize.rs:89`), Selinger's `ColumnRemap` walks them like it walks
`order_by`/`aggregates` today (`selinger.rs:420-464`), memo emit's aggregate
remap (T17-A.4) extends to group keys. Search spaces unchanged — see
"Per-planner impact".

Gates: `group_by.slt` (grouped results, HAVING, NULL groups, empty input);
three-way planner parity on grouped queries; exec-model equivalence;
EXPLAIN golden for the grouped tree.

**H1 executed (2026-07-18).** Change, generic: grouped aggregation and
HAVING land as a spine feature — one IR field (`having`), one coordinate
rule (aggregate-output space for ORDER BY/HAVING when aggregates exist),
one lowering site, one aggregation kernel shared by both execution
models. Repo-specific: binder accepts GROUP BY with set-equality
validation and keys-first ordering (GROUP BY/HAVING/DISTINCT were
previously *silently dropped* — now bound or loudly rejected);
`HashAggregate` gained the designed `group_by` path (`HashMap` buckets,
`MAX_GROUP_COUNT = 2^20` assert, canonical NULL-first key-order
emission); the spine reuses `projection` as group keys with no trailing
Projection; Selinger skips the order_by remap when aggregated (`having`
is remap-invariant); memo carries both through `SpineParts` untouched
(props already guarded §7.5); Push runs grouped aggregation as a native
accumulate-on-push/emit-on-finish sink sharing the pull kernel. Gains:
the aggregation shape of 21/22 TPC-H queries now plans and executes
through all three planners (three-way parity extended with 4 grouped
queries incl. joined grouping) and both exec models (byte-identical,
order included); tests 1314 → 1317 plus in-place corpus growth; grouped
EXPLAIN tree pinned in `group_by.slt`. Draws (recorded limits): HAVING/
ORDER BY aggregates must appear in the SELECT list; group keys must be
projected, before the aggregates; no DISTINCT; group keys are plain
columns until H2/H3; whole-table aggregation stays on Push's delegated
bridge (nothing to stream).

### H2 — expressions in projections and aggregates

IR change: `projection: Vec<usize>` → projection items that may be
`Expression`; `AggregateSpec` variants take `Expression` instead of `usize`
(Q1: `sum(l_extendedprice * (1 - l_discount))`). Evaluation already exists
(`Expression::compile`); this is plumbing, not a new evaluator.

The real cost is column-reference tracking: every walker that reads or
remaps column indices — `referenced_columns`, `shift_predicate`,
`ColumnRemap`, normalize's spine handling, memo emit's remap — must walk
Expression trees. Rule: extend ONE shared helper in `column_map` (visit /
remap columns inside an `Expression`) and route all three planners through
it; no per-planner walkers.

Gates: Q1 shape end-to-end (dates stubbed as Timestamp literals until H3);
parity + equivalence suites extended; workload-log/plan serde still round-trips.

### H3 — scalar and predicate surface

- DATE: **decided 2026-07-18** — new `Value::Date(i32 days)` with its own
  `ColumnType`, order-preserving key encoding, and display. TPC-H compares
  dates to dates; a distinct type keeps encoding and rendering honest
  (rejected: reusing `Timestamp` — midnight-µs display lies, 4 wasted key
  bytes per component).
- Date literals + `INTERVAL` arithmetic: **fold at bind time** — every TPC-H
  date expression is literal-only (`date '1994-01-01' + interval '1' year`),
  so planners see a plain `Compare` with a literal. Zero planner impact.
- BETWEEN → `And(Gte, Lte)`; IN-list → Or-chain of equalities: binder
  desugarings, no IR change.
- LIKE: new predicate form + evaluator; selectivity default (O10-style
  constant; prefix patterns cheaper than contains) so cost-based planners
  keep sane cardinalities.
- CASE: new `Expression` variant (Q8, Q12, Q14 use it *inside* SUM — lands
  on H2's expression plumbing).
- EXTRACT(YEAR): expression function over Date (Q7, Q8, Q9).
- H3b: LEFT OUTER JOIN (Q13) — binder + NLJ/hash outer variants; excluded
  from join reordering (see below).

Gates: `scalar.slt`; Q1, Q3, Q5, Q6, Q10, Q12, Q14, Q19 end-to-end on a
hand-checked micro-dataset; Q13 after H3b.

### H4 — subqueries, staged

- H4a-scalar: uncorrelated scalar subquery — plan and run the inner query
  first, splice its result as a literal into the outer plan. All planners
  see a literal; identical everywhere.
- H4a-semi: uncorrelated IN / EXISTS → hash semi/anti-join operator.
- H4b: correlated subqueries (Q2, Q17, Q21…) → Apply-style operator
  (re-bind outer columns per row). Correctness-first; decorrelation is a
  recorded deferred lever.

Gates: per-stage query unlocks (see demand map); parity suite runs the
unlocked queries across all planners (identical *results*; plan shapes may
legitimately differ only in the join core).

### H5 — the harness

`bin/tpch` in the TPC-C driver's mold: deterministic seeded dbgen-style
generator (SF parameter), the 22 queries, answer validation. Validation
(**decided 2026-07-18**): two layers — exact hand-computed answers on a
fixed micro-dataset (slt-style; catches logic bugs precisely), plus
committed expected outputs generated once offline via a trusted engine
(DuckDB/SQLite) at SF 0.01 (volume-dependent bugs; the oracle is a
one-time offline tool, never a dependency). Spec answer set at SF1 is
deferred to the Linux/NVMe era. Per-planner and per-exec-model sweep
flags from day one.

## Per-planner impact of the new expressions and operators

Architecture fact that drives everything: all three planners share one
lowering (`plan_inner`) and one statement spine — residual Filter →
HashAggregate → Sort → Limit above the join core. Rule-based lowers the
textual plan directly. Selinger rewrites `LogicalPlan → LogicalPlan`
(left-deep DP reorder + `ColumnRemap`) then calls the *same* lowering with
cost-based join-algorithm choice. The memo planner splits the query into an
optimizable join core and a **fixed spine** (`normalize.rs` D3) and only
searches the core; unsupported shapes bail to the shared lowering (D8).

Consequences per rung:

- **GROUP BY / HAVING / expressions / CASE / EXTRACT / date arithmetic:
  spine-only.** No planner's search space changes; plans differ across
  planners exactly as much as they do today (join core only). The work each
  planner owes is mechanical: remap/walk the new column references
  (Selinger `ColumnRemap`, memo emit remap) through the shared column_map
  helper.
- **BETWEEN / IN-list: invisible.** Desugared before planning into
  And/Or trees all planners already handle; existing conjunct
  decomposition classifies them into per-relation local predicates.
- **LIKE: evaluation identical everywhere; costing differs.** Rule-based
  ignores selectivity by design. Selinger and memo route it through
  `estimate_predicate_selectivity` — without a LIKE rule their cardinality
  estimates silently degrade, so H3 ships the default constant with the
  operator, not after it.
- **Outer join and semi/anti-join (H3b/H4a): first real divergence.**
  Selinger's join graph edges and memo's normalize accept only single
  `col = col` INNER equi-joins; outer/semi joins won't become edges.
  Selinger keeps them in textual position (reorders around them at best);
  memo D8-bails the whole query to the shared lowering. Honest consequence:
  outer/semi/correlated queries initially get rule-based-shaped plans on
  every planner. Recorded levers: semi/outer edges in the DP and memo
  (valid-reordering rules), then decorrelation.
- **Uncorrelated scalar subqueries: invisible** (spliced literals).
- **Execution models:** `PhysOp` stays model-neutral; Volcano gets native
  operators. Push implements grouped HashAggregate as a **native push sink**
  (**decided 2026-07-18**: accumulate on push, emit on close — the shape
  push models are best at; the bridge-delegate shortcut was rejected so
  Push stays a real alternative on aggregate queries). Equivalence suite
  gates it.

## Deviations from industry practice (revisit triggers)

Three conscious deviations ride along; each has a named trigger — revisit
if correctness or performance isn't there, not before:

1. **Query-block logical IR** (flat `Select`, Postgres/SQLite school) vs
   algebra tree (Calcite/DuckDB/Cascades school). Trigger: H4b — if
   correlated-subquery or decorrelation work fights the flat block,
   that's the moment an algebra tree earns its cost, not earlier.
2. **PK required, no synthesized hidden key.** Industry synthesizes
   (InnoDB `GEN_CLUST_INDEX`, CockroachDB hidden `rowid`, SQLite rowid);
   our hard requirement is a real dialect gap, not a design position.
   Trigger: SQLite-corpus adoption (its last blocker after H1+H3).
3. **Max-groups assert, no spill.** Production standard is degrade
   (spill or sort-based fallback), never crash. Trigger: the bound fires
   on a legitimate workload, or grouped-aggregate memory/perf lags —
   standard middle step is sort-based grouping (the recorded 17-B order
   lever); true spill is warehouse-scale scope we likely never need.

## Deferred, recorded

- Hidden rowid / PK-less tables — SQLite-corpus-only; revisit when H1+H3
  land (at that point it is the corpus's last blocker).
- Decorrelation; semi/outer join edges in DP + memo; sort-based grouped
  aggregation exploiting 17-B order properties; GROUP BY pushdown.
- TPC-H performance work (Linux/NVMe environment question included).

## Risks

- H2's IR change ripples through serde (plans in the workload log), memo
  fixtures, and EXPLAIN goldens — expect golden churn; the test-count
  arithmetic must balance.
- `HashAggregate` grouped state is unbounded per distinct key — bound it
  explicitly (spill is out of scope; assert a max-groups limit).
- Q19's OR-heavy predicate stresses selectivity estimation of Or-chains;
  verify the estimator handles disjunction before H3 gates.
