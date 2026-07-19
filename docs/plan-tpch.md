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

**H2 staged (deviation note).** H2 executes in two steps: **H2a** —
aggregates over expressions (the load-bearing TPC-H piece; every Q1-class
query) — and **H2b** — computed projections (row-level and
post-aggregate, e.g. Q14's `100.00 * sum(..)/sum(..)`), still open.
Recorded here per the plan-deviation rule; the split bounds the IR blast
radius per increment.

**H2a executed (2026-07-18).** Change, generic: aggregate arguments are
expression trees — `AggregateSpec`/`AggregateFn` carry `Expression`, a
static `Expression::column_type()` inference mirrors the runtime
arithmetic exactly (Int pairs; Decimal scale algebra: Mul sums scales,
Add/Sub/Div require equal scales), args compile once per operator and
both execution models share the fold kernel; bind-time integer-literal
alignment (to Int32 peers, and to the Decimal peer's scale for
Add/Sub/Div) closes the promote-to-scale-0 NULL landmines (`1 - l_discount`,
`price / 2`, `c_val * 2`). Remap rides the one shared walker
(`ColumnRemap::apply_expression`) for Selinger and memo alike.
Review findings (Sonnet pass) fixed before commit: (1) CONFIRMED — Div
inference originally claimed Mul-like any-scale validity while the
runtime requires equal scales → `SUM(price / 2)` silently all-NULL, its
unit test asserting the bug; fixed both sides (inference mirrors, binder
aligns Div literals). (2) The expression-remap parity guard used a
2-relation join whose order-symmetric costs tie → the strictly-cheaper
gate never rewrote → identity remap guarded nothing; replaced with a
3-relation worst-textual-order query, rewrite verified to fire (arg
columns remap (2,5)→(7,4)). (3) EXPLAIN columns now render `c{i}` so
column-vs-literal is unambiguous in labels. Gains: Q1's full aggregate
shape (scales 2/4/6) runs grouped through all three planners and both
exec models, hand-computed slt expectations independently recomputed in
review; tests 1317 → 1324. Draws: aggregate labels duplicate between IR
and operator explain (pre-existing pattern, recorded, unfixed);
Int32-column × Int64-column arithmetic errs loudly rather than aligning
(no consumer); Decimal Mul scale growth unbounded in inference
(runtime-asserted; scope ends at 6 here).

**H2b executed (2026-07-18).** Change, generic: computed projections land
as a display layer — one new IR field (`select_list: Vec<Expression>`),
one new physical op (`Compute`, above Sort / below Limit), one coordinate
rule reused verbatim (input space when unaggregated, aggregate-output
space otherwise — remapped through the shared `apply_expression` walker
IFF unaggregated, cloned through when aggregated, exactly like
`order_by`). An EMPTY `select_list` is the identity display, so every
pre-H2b query keeps its exact plan shape (the zero-churn invariant — all
prior EXPLAIN goldens passed unmodified). Repo-specific: the binder reworks
`bind_select_items` into aggregate-extraction form (two passes over the
items — pass 1 collects group keys, pass 2 binds each item into output
coordinates, deduping aggregates by `PartialEq`), lifting H1's
keys-before-aggregates restriction (`SELECT COUNT(*), region` now works);
a bare column inside a computed grouped item must be a group key; identity
display elides to empty (`is_identity_select_list`). The `Compute`
operator (Volcano) and native `ComputeSink` (Push) compile the exprs once
and share `build_compute_schema`, which infers the output type via
`Expression::column_type` and refuses an untypeable display with the same
loud "cannot infer" both models raise identically. NO literal alignment in
output space: the left-associative `100.00 * SUM(x) / SUM(y)` is scale4 ÷
scale2 → uninferable (recorded as an error contract; the working form
divides first). Gains: Q14/Q8/Q17's post-aggregate arithmetic shape plans
and runs through all three planners (corpus += input-space remap and
output-space no-remap cases) and both exec models (grouped Q14 proven
byte-identical); tests 1324 → 1328. Review findings (Sonnet pass) fixed
before commit: (1) CONFIRMED — pass 1's `as_bare_column` didn't unwrap
parenthesized expressions, so `SELECT (region), COUNT(*) … GROUP BY
region` false-errored when the parens were the key's sole occurrence;
Nested-recursion arm added + repro pinned. (2) Compute passthrough
columns hardcoded nullable instead of inheriting — latent metadata trap,
now per-arm. (3) The aggregate-dedup shape (`SUM(x), SUM(x)*2` → one
spec, two refs) was traced-correct but untested — pinned with values +
EXPLAIN golden. Review also proved the elision check structurally unable
to fire on a non-identity list and independently recomputed the decimal
expectations. Draws (recorded limits): a group key referenced only
inside an expression (never projected bare) still errs (H1's "keys must
be projected" holds); left-associative Decimal division is uninferable
pending the division-semantics fix noted under H3.

### H3 — scalar and predicate surface

**H3.1 executed (2026-07-18).** Change, generic: Decimal division became
true rounded division at result scale `max(s1, s2)` (round half away
from zero, everything checked, error → NULL), replacing the
equal-scale-only rule — `div_keeping_scale` deleted (single caller).
Inference mirrors; `align_target` dropped its Div special case (proven
value-preserving for any operand scale in review); the four temporary
error contracts flipped positive — including TPC-H Q14's verbatim
left-associative `100.00 * sum(..) / sum(..)` → 45.0000, hand-computed
and independently recomputed. `AvgDecimal`'s finalize was derived (not
assumed) identical to routing SUM/COUNT through `Decimal::div`. New
parity property test: an op×type grid asserts `eval_binary_op` result
types equal `column_type` inference — the H2a divergence class is now
regression-guarded as a class. Review findings fixed before commit:
(1) CONFIRMED pre-existing member of that same class — inference's Mul
arm had no `MAX_SCALE` guard, so `DECIMAL(18,10) × DECIMAL(18,10)`
inferred scale 20 while the runtime NULLed every row; guarded, and the
grid gained a scale-10 entry because its old scale-≤4 values made the
failing region structurally unreachable. (2) The rescale-overflow test
was exercising a different overflow branch than its name claimed —
renamed honestly; `rescale_mantissa`'s check documented as
defense-in-depth (unreachable from valid Decimals; serde doesn't
re-validate). (3) `docs/optimizer-review.md` O4 status corrected to
FIXED. Gains: tests 1328 → 1331; Q14 runs verbatim. Draws: none new;
Decimal Mul at extreme scale pairs now errs loudly at bind/build instead
of silently NULLing at runtime.

**H3.2 executed (2026-07-18).** `Value::Date(i32 days)` + `ColumnType::Date`
(appended last in both enums — bincode tag stability pinned by a
raw-bytes test, old catalogs decode unchanged); dependency-free civil
calendar (`types/civil.rs`, Hinnant algorithms, anchors hand-verified in
review incl. era/negative-day arithmetic); strict `DATE 'YYYY-MM-DD'`
literals; bind-time INTERVAL folding (DAY exact; MONTH/YEAR clamped —
subtraction is negate-then-fold, proven for the borrow cases; Q1's
`- INTERVAL '90' DAY` folds to `DATE '1998-09-02'`, EXPLAIN-pinned);
`Expression::ExtractYear` (YEAR only, bind-time Date-typed, total at
runtime); key encoding mirrors Int32 with cross-epoch order property
tests; Date columns joined the int-histogram ANALYZE/selectivity path
(~4 mechanical lines). Review found ZERO functional defects — five
coverage/DX findings fixed pre-commit: the cargo-fuzz ColumnType mirror
had silently missed Date (manual-sync warning added), Date
histogram/ANALYZE now asserted end-to-end (an overclaiming test comment
honestied), the WHERE-side fold tests gained boundary-straddling rows
(±1-day regressions now flip result sets), join-key property grid +
Date, and the fold error split (non-Date operand vs non-literal date).
Checking the fuzz crate also surfaced pre-existing breakage: two fuzz
targets still imported `index::lsm` paths dead since the workspace
split — repaired in a separate commit. Gains: tests 1345 → 1347 (+14
from the phase itself); every TPC-H date predicate shape (Q1's window,
range scans, EXTRACT in projections/aggregate args) now runs. Draws:
expression GROUP BY keys (Q7/Q8/Q9's `EXTRACT` grouping) remain a
recorded separate increment; INTERVAL only folds against literal dates.

- DATE: **decided 2026-07-18** — new `Value::Date(i32 days)` with its own
  `ColumnType`, order-preserving key encoding, and display. TPC-H compares
  dates to dates; a distinct type keeps encoding and rendering honest
  (rejected: reusing `Timestamp` — midnight-µs display lies, 4 wasted key
  bytes per component).
**H3.3 executed (2026-07-19).** BETWEEN and IN-list land as pure binder
desugars (3VL-exact: the classic `NOT IN (…, NULL)` → zero rows is
pinned; the Or-chain estimator already did inclusion–exclusion, closing
the recorded Q19 risk); LIKE as `Predicate::Like` with a compile-once,
bounded %-segment matcher (Unicode-correct by construction — chars, not
bytes; overlap-guarded anchors; every adversarial case hand-executed in
review) and documented selectivity constants (literal-first-char prefix
0.1 / contains 0.25); searched CASE as `Expression::Case` (3VL
fall-through, bind-time integer-literal branch coercion for the
Q12/Q14 `ELSE 0` shape, scale-based Decimal unification); IS [NOT] NULL
(never UNKNOWN by design). Review: matcher and NOT-IN traces fully
held; three findings fixed pre-commit — CASE typed `ELSE NULL`
differently from omitted ELSE (NULL-literal branches now skip typing,
both spellings identical; the fix also caught the mixed
`THEN 0 ELSE NULL` coercion bail), IN-lists gained an explicit
`MAX_IN_LIST_ITEMS = 1000` bound (Or-chain depth = recursion depth in
compile/remap/drop — the flatten_conjuncts lesson applied), and `'_x%'`
reclassified to the contains bucket. Recorded levers: per-row
`Vec<char>` alloc in LIKE (ASCII fast path), IS NULL true null-fraction
selectivity (needs row count beside ColumnStats). Tests 1347 → 1360.
Q12/Q14/Q19 predicate shapes complete.

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
- Decimal division semantics (**decided 2026-07-18** — the Q14 fix):
  replace the equal-scale-only `div_keeping_scale` runtime rule with true
  division at result scale `max(s1, s2)` (rescale operands, integer
  division with round-half-up, checked overflow → NULL as today), so
  Q14's verbatim left-associative `100.00 * sum(..) / sum(..)`
  (scale 4 ÷ scale 2) is legal. Inference's Div arm mirrors:
  `Some(Decimal{p, max(s1, s2)})`, no equal-scale bail. Ripples to
  update, all recorded in H2a/H2b as temporary: `align_target` drops Div
  (no longer needed), `column_type_decimal_rules`' mismatched-Div → Some,
  `aggregate_expr.slt`'s uninferable-div contract and H2b's
  left-associative Q14 error contract both flip to positive tests with
  hand-computed values. Audit `AvgDecimal`'s finalize division for
  consistency with the new rule.
- H3b: LEFT OUTER JOIN (Q13) — binder + NLJ/hash outer variants; excluded
  from join reordering (see below).

**H3b executed (2026-07-19).** `JoinKind{Inner, LeftOuter}` through
binder/IR/planner/executors: NLJ-outer (full ON per pair, pad unmatched
left with right-width NULLs) and hash-outer (hash the single equi
conjunct, evaluate residual ON per candidate — Q13's verbatim compound
`ON c_custkey = o_custkey AND o_comment NOT LIKE …` runs at hash speed);
INLJ/Merge structurally never lower for outer. The two semantic rules
are law and test-pinned where they visibly differ: ON filters the match
while WHERE filters the padded result (R1), and right-side WHERE
conjuncts never push below the null-padding (R2 — the Phase C.2
pushdown gate keys on join kind per iteration; the outer right leaf is
always an unfiltered scan, EXPLAIN-pinned). Cost-based planners bail
explicitly on any non-Inner join — previously only incidental; a bare
equi outer ON would have become a reorderable edge. Outer cardinality
floored at the left input (a LOJ emits ≥ every left row). Review: zero
correctness findings (compound-ON splitting, NULL-key padding,
bail-before-routing order, left-side pushdown validity all held);
coverage gaps fixed pre-commit — mixed Inner+LeftOuter chains both
directions (incl. a padded row flowing through a later inner join, and
per-join pushdown gating pinned in one EXPLAIN), and the COUNT(*) = 1
vs COUNT(o_id) = 0 distinction on padded rows (Q13's exact risk).
Tests 1360 → 1362. Q13's join layer complete; its derived-table shell
is H4a. Draws: right leaf of an outer join takes no index paths
(recorded lever); RIGHT/FULL OUTER remain loud rejections.

Gates: `scalar.slt`; Q1, Q3, Q5, Q6, Q10, Q12, Q14, Q19 end-to-end on a
hand-checked micro-dataset; Q13 after H3b.

**Scope discovery (2026-07-19, recorded).** Q7/Q8/Q9/Q13/Q22 use
derived tables (FROM-subqueries) — verbatim, their `l_year`-style
grouping keys are plain columns OF the derived table, so expression
GROUP BY keys are NOT on the verbatim critical path (they'd only unlock
flattened rewrites). Reorder: the expression-group-keys increment is
parked (general SQL value, corpus-era); H3b (outer join) runs next;
derived tables are promoted to H4's first stage — uncorrelated by
construction, they unlock Q7/Q8/Q9 outright, Q13 with H3b, and Q22's
inner shells. Design note for H4: derived tables break the
"leaf = catalog table" assumption in all three planners — likely
landing shape is plan-inner-query → materialize → anonymous table
source at the executor layer with D8-style planner fallbacks first,
optimization across the boundary recorded as a lever.

**H4a executed (2026-07-19).** Derived tables land per the recorded
shape: step-0 pre-req relocated aggregate output-type/name promotion to
the IR (`AggregateSpec::{output_type, output_name}` +
`select_output_schema`; executor delegates — single source; gated alone
at 1362/0 before feature work); alias capture incl. Q13's column-list
form (`AS c_orders (c_custkey, c_count)`); `DerivedTable` on Select +
`PhysOp::DerivedScan` leaf materializing once per statement
(`MAX_DERIVED_ROWS 2^20`, `MAX_DERIVED_DEPTH 4`); derived aliases
shadow catalog names; Selinger/memo bail explicitly; no pushdown into
subplans and no index paths (levers); same-snapshot execution verified
(one engine handle threads the whole statement). **Q13 runs FULL
VERBATIM** (sole note: ORDER BY output-alias unsupported —
`ORDER BY COUNT(*)` equivalent used; recorded). Review: HIGH finding
fixed pre-commit — duplicate relation names in one FROM were never
rejected anywhere (binder first-match vs planner last-insert-wins →
`[2,2]` for `[1,2]`); the guard at the `Scope::push` choke point also
fixed the PRE-EXISTING `FROM t AS a, u AS a` silent misresolution.
Derived schemas are now outer-join-nullability-aware (mirrors the
executor's rule); the subplan-joins-never-reorder scope limit is
documented as a lever. Tests 1362 → 1369. Q7/Q8/Q9/Q13 unlocked —
12/22 verbatim-capable. Draws: derived cardinality is the un-ANALYZEd
default; ORDER BY aliases; subplan reordering lever.

### H4 — subqueries, staged

**H4b executed (2026-07-19).** Uncorrelated subqueries: scalar (bind to
a dedicated slot, session pre-executes through the statement's engine
handle — 0 rows → NULL, >1 → loud — then splices; planners see
literals) and IN/NOT IN/EXISTS (`Predicate::InSubquery` — the constant
set materializes once per statement into a null-aware hash set; no new
join operators, no reorder implications; EXISTS keys on nonempty with a
bind-time LIMIT 1 early-exit). Q18's prerequisite landed: HAVING/ORDER
BY-only aggregates compute-then-project-out (H1's projected-only
restriction lifted; position math append-stable). Correlated references
reject loudly naming the column. Review found and fixes closed three
substantive defects pre-commit: (1) CONFIRMED — scalar slots shared
`Expression::Parameter`'s namespace with user `$n`, so a prepared
statement mixing both silently substituted the user's value into the
subquery slot (traced repro); fixed with a dedicated
`Expression::SubqueryResult` namespace + its own substitution walker.
(2) CONFIRMED — `eval_in` returned UNKNOWN for a NULL probe against an
EMPTY set where SQL's quantified-comparison semantics say vacuous
FALSE/TRUE (the unit test had pinned the wrong behavior; Postgres
cross-checked); fixed nonempty-first. (3) The in-subquery row cap was a
release-crashing assert on user-triggerable volume — now a returned
error per the MAX_IN_LIST_ITEMS policy. Also: qualified-typo errors no
longer misreport as "correlated". Tests 1369 → 1380. **Q11, Q16, Q18
verbatim; Q15 as-derived (CREATE VIEW recorded gap). 16/22.** Draws:
subqueries inside derived tables reject loudly (lever); IN-subquery
selectivity is a documented constant; statement-global subquery indices
ride a session→executor seam rather than PhysOp fields.

- H4a-scalar: uncorrelated scalar subquery — plan and run the inner query
  first, splice its result as a literal into the outer plan. All planners
  see a literal; identical everywhere.
- H4a-semi: uncorrelated IN / EXISTS → hash semi/anti-join operator.
- H4b: correlated subqueries (Q2, Q17, Q21…) → Apply-style operator
  (re-bind outer columns per row). Correctness-first; decorrelation is a
  recorded deferred lever.

**H4c executed (2026-07-19).** Correlated subqueries land as per-outer-row
apply: the binder's correlation flip binds enclosing-scope references to
positional `Expression::OuterRef(k)` inside inner templates, with the
outer tuple-global indices in `outer_cols` vecs ON the outer variants
(`Predicate::CorrelatedExists`, `Expression::CorrelatedScalar`) — the
indirection that makes join-reorder remapping touch one small vec and
never walk into subplans. Session-built evaluators capture the
statement's engine handle (one MVCC snapshot for every row) and
plan/execute the substituted template per outer row (rule-based inner
planning, plan-per-row: recorded correctness-first shape; caching,
decorrelation, planner-threading = levers). Infallible filter closures
report faults through a shared cell checked before any row is emitted —
proven leak-free in review for both execution models. Correlated IN and
multi-level correlation reject loudly (no target query needs them);
CorrelatedScalar is direct-compare-operand only. The implementing
agent's internal review caught nested-subqueries-inside-correlated-
templates executing with empty sets (now a loud bind rejection); the
independent review confirmed zero correctness defects and yielded three
fixes: (1) the cost-tie lesson's THIRD recurrence — now a STANDING
RULE: any reorder-sensitive guard query must prove the rewrite fires
(3-relation big-first shape + an EXPLAIN leaf-order assertion; the new
guard shows RuleBased [c,b,a] vs Selinger [a,b,c] with outer_cols 6→0
remapped and results identical); (2) fault-cell gating became
per-predicate (`contains_correlated`) so plain Filters in correlated
statements skip the mutex; (3) the new invariant-violation paths crash
(`unreachable!`) instead of silently NULLing in release — incl. the
scalar analog found adjacent to the specced sites. Tests 1380 → 1390.
**Q2, Q4, Q17, Q20, Q21 verbatim (Q21 modulo the recorded ORDER BY
alias form, as Q13); Q22 correlated-core gated — 21/22.** Q22's last
mile is H4d: SUBSTRING + subquery machinery inside DerivedScan subplans
(its NOT EXISTS correlates within the derived table's own scope).
Second prompt-injection sighting in a subagent tool stream this phase
(fake system-reminder requesting hidden info / expanded git
permissions) — recognized, ignored, reported.

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
