# Clock2Q+ — Implementation Plan

Division of labor (decided 2026-07-03: the standard CLAUDE.md flow
applies here too — **Claude writes all code, the user reviews and owns
every line**). The four sessions below remain the reviewable increments:
Claude plans each line-by-line, implements it, and explains it; the user
reviews before the next session starts.

Everything below is written against the tree as of commit `d38fe43`.

---

## 0. The trait being implemented (verbatim contract)

`src/buffer/replacer/traits.rs` — `EvictionPolicy`:

- `fn name(&self) -> &'static str` → `"clock2q+"`.
- `fn record_access(&mut self, frame_id: FrameId, page_id: PageId)` —
  called on every fetch/create. Frame already tracked ⇒ HIT path;
  untracked ⇒ INSERT path (this is where the ghost check lives).
- `fn set_evictable(&mut self, frame_id: FrameId, evictable: bool)` —
  **must honor the Q-35 contract documented on the trait**:
  `set_evictable(_, true)` for an untracked frame is a NO-OP
  (`evictable ⊆ tracked`). Pattern-match the guard in `fifo.rs`.
- `fn evict(&mut self) -> Option<FrameId>` — select + REMOVE the victim
  from all tracking; only evictable frames; `None` if none.
- `fn evict_for_page(&mut self, incoming: PageId) -> Option<FrameId>` —
  default delegates to `evict()`; Clock2Q+ keeps the default (its ghost
  logic runs in `record_access`, unlike ARC's adapt-before-REPLACE).
- `fn remove(&mut self, frame_id: FrameId)` — page deleted; purge from
  every structure (do NOT ghost it — deletion is not eviction).
- `fn size(&self) -> usize` — evictable count.
- `fn export_state / import_state(PolicyState)` — warm-swap. Export:
  score Small entries below Main entries (colder = lower), ordering
  within each by queue position — mirror `fifo.rs`/`arc.rs` exports.
  Import: `pending_page_scores` pattern (score > threshold ⇒ insert to
  Main on first access) — mirror `arc.rs`.

Also required reading before session 1: the BPM call-sites that define
when these methods fire — `src/buffer/buffer_pool_manager.rs`
(`handle_cache_hit`, `handle_cache_miss` publish + loser paths,
`evict_page` phases 1/3, `unpin_page_internal`, `swap_policy`
re-registration). The Q-35 history lives in those comments.

## 1. Files touched

| File | Change | Who |
|---|---|---|
| `src/buffer/replacer/clock_two_q_plus.rs` | NEW — the policy (~250 lines + tests) | user |
| `src/buffer/replacer/mod.rs` | `pub mod clock_two_q_plus;` + `pub use clock_two_q_plus::Clock2QPlusReplacer;` (alongside lines 25–31) | user (1 line each) |
| `testkit/src/policy.rs` | ctor `pub fn clock2q_plus() -> Box<dyn EvictionPolicy>` (pattern-match existing ctors) | user |
| `testkit/src/matrix.rs` | one line in `for_each_policy!` (line ~16–25): `$cb!(clock2q_plus, $crate::policy::clock2q_plus);` — this alone enrolls the policy in the conformance suite, the correctness-neutrality differential, and every policy bench | user |
| `src/bin/tpcc.rs::make_policy` (line ~215–225) | `"clock2q+" => Box::new(Clock2QPlusReplacer::new(capacity)),` | user |
| `benches/eviction_policies.rs` | add the policy to the bench set (pattern-match) | user |
| `tools/` or `benches/` trace runner + derivation tool | NEW | Claude |
| proptest invariants file | NEW `tests/clock2q_plus_invariants.rs` | Claude scaffolds, user reviews |

## 2. Session 1 — queue skeleton

Goal: struct + construction + registration compile; no behavior yet
beyond plain-FIFO placeholder so the conformance suite can run.

```rust
pub struct Clock2QPlusReplacer {
    // Small FIFO: insertion order, newest at back. Reuse the OrderedList
    // pattern from arc.rs (seq-BTreeMap + membership HashMap) — it
    // provides O(log n) mid-list removal AND, critically, the per-entry
    // insertion seq the correlation window needs (A2: window measured
    // from the newest end via seq arithmetic).
    small: OrderedList<FrameId>,        // consider promoting OrderedList out of arc.rs into a shared module — decide at review
    /// Ref bits for Small entries (set by post-window hits only).
    small_ref: HashSet<FrameId>,
    // Main: clock ring. VecDeque<FrameId> + hand index + ref bits, OR
    // reuse ClockReplacer's internal layout as the pattern (read
    // clock.rs first; do not import it — copy the shape).
    main: /* clock ring */,
    main_ref: HashSet<FrameId>,
    /// Ghost: evicted-page fingerprints, FIFO, capacity = ghost_cap.
    ghost: OrderedList<PageId>,
    /// Capacities (CONFIRMED by the read): small_cap = 10% of c (min 1),
    /// ghost_cap = 50% of c (of TOTAL cache — A7), window = 50% of
    /// small_cap (= 5% of c). Struct fields, not constants, so the C3/C4
    /// sensitivity sweeps can vary them.
    c: usize, small_cap: usize, ghost_cap: usize,
    window_frac_percent: u8,            // 50 default; sweepable {10,30,50}
    frame_to_page: HashMap<FrameId, PageId>,
    evictable: HashSet<FrameId>,
    pending_page_scores: HashMap<PageId, u64>,
}
```

Assertions (TigerStyle, ≥2/function): constructor asserts
`c > 0`, `small_cap ≥ 1`, `small_cap ≤ c`; every mutation method
`debug_assert!`s the disjointness invariant (a frame in exactly one of
small/main; ghost holds only non-resident pages) — factor an
`fn check_invariants(&self)` called under `debug_assertions`.

Gate: `cargo test -p testkit` conformance suite green with the
placeholder (plain FIFO semantics), policy selectable in tpcc via
`--policy clock2q+`.

## 3. Session 2 — correlation-window rule (the paper's novelty)

`record_access` HIT path for a frame in Small:
- Compute window membership from insertion seqs: entry is *in window*
  iff its seq is in the newest `window_frac_percent` of the span
  `[oldest_seq, newest_seq]` currently in Small (A1/A2 choices — see
  REPRODUCTION.md ambiguity log; encode A3's clamp).
- In window ⇒ do NOT set ref bit. Past window ⇒ `small_ref.insert`.
- Hits in Main ⇒ set main ref bit (standard clock).
- Hits NEVER move entries between queues (S3-FIFO discipline —
  promotion happens only at eviction time).

Every branch gets a unit test: in-window hit leaves ref clear;
past-window hit sets it; boundary entry per A1; degenerate small queue
per A3; Main hit sets main ref.

## 4. Session 3 — eviction / promotion path

`evict()` (returns victim + fully de-tracks it):
1. Prefer evicting from Small when `small.len() ≥ small_cap` (confirm
   trigger condition against paper — S3-FIFO evicts from Small when
   Small is over its target, else from Main; record as ambiguity if
   the paper differs).
2. **Small eviction scan** from oldest: skip non-evictable frames
   (pinned — BPM contract); for the first evictable candidate:
   ref bit set ⇒ *promote to Main* (insert at hand, ref clear) and
   continue scanning Small for a victim; ref clear ⇒ victim: de-track,
   push its PageId to Ghost (trim ghost to `ghost_cap` FIFO-wise),
   return.
3. **Main clock sweep** from the hand: skip non-evictable; ref set ⇒
   clear ref, advance; ref clear ⇒ victim: de-track, ghost it, return.
   Bound the sweep to 2×len (all-ref case degrades to FIFO — assert
   the loop bound explicitly).
4. Cross-queue fallback both directions (the ARC pinned-list lesson —
   see `test_arc_falls_back_when_chosen_list_pinned`): if the preferred
   queue has no evictable frame, try the other before returning `None`.

Unit tests: promotion on ref-set; ghost receipt on ref-clear; ghost trim
at cap; pinned-skip; both fallbacks; the 2×len sweep bound.

## 5. Session 4 — ghost-hit insertion + invariants

`record_access` INSERT path (untracked frame):
- `pending_page_scores` (warm import) hot ⇒ Main, done.
- Ghost hit ⇒ remove from ghost, insert to **Main** (confirm insertion
  point at hand-position vs MRU against paper — ambiguity if unstated),
  ref clear.
- Otherwise ⇒ insert to Small (newest end), ref clear.

Then Claude lands `tests/clock2q_plus_invariants.rs` (proptest over
op sequences, pattern: `tests/config_proptest.rs`):
- capacity accounting: `small.len() + main.len() ≤ c` at all times;
- disjointness: no frame in two queues; ghost ∩ resident pages = ∅ (A5);
- ghost bounded by `ghost_cap`;
- `evictable ⊆ tracked` (the Q-35 contract) under arbitrary
  set_evictable interleavings;
- sanity: on a pure-recency trace, hit ratio ≥ plain FIFO's.

Gate for sessions 2–4 complete: full conformance matrix + swap-stress
marquee + the invariants proptest green; `cargo bench eviction_policies`
runs with the new policy.

## 6. Claude scaffolding tasks (complete fully, in parallel with reads)

- [ ] Trace acquisition: documented steps + script for CloudPhysics
      traces via SNIA IOTTA (license permitting; if click-through walls
      block scripting, write the manual steps + a checksum manifest).
- [ ] Derivation tool: `LBN ÷ fanout (200)` metadata-trace transform as
      a small standalone binary or bench-common helper; unit-tested on
      a synthetic fixture.
- [ ] Trace-driven policy runner: given a trace file + policy name +
      cache size, replay accesses through any `EvictionPolicy` (via the
      testkit ctor registry) and report hit/miss ratio. Lives beside
      `benches/eviction_policies.rs` (pattern-match its harness usage);
      output CSV suitable for the paper's miss-ratio-curve axes.
- [ ] IDB-native trace capture: a feature-gated hook (or reuse of BPM
      stats) that logs `(page_id)` access streams from a live BPM run —
      needed for the post-gate divergence experiments. Design sketch to
      user before building (touches the BPM hot path — Q-35 territory;
      keep it to an opt-in ring buffer flushed off the hot path).
- [ ] S3-FIFO baseline policy? The paper's primary comparator. DECISION
      NEEDED at read time: implement plain S3-FIFO as a 7th…8th policy
      for the validation curves (small marginal cost once Clock2Q+
      exists — it is Clock2Q+ minus the window with 100% ghost), or
      validate against the paper's reported S3-FIFO numbers only.
      Recommend implementing it; record either way.

## 7. Open items blocking session 1

1. **The paper reads** (pipeline steps 1–2) — fills the claims table and
   confirms: exact small/ghost fractions, window fraction, Small
   eviction trigger, ghost-hit insertion point. Reads happen BEFORE
   session 1 per pipeline; the ambiguity log already carries our
   provisional choices so sessions aren't blocked on author replies.
2. Whether `OrderedList` gets promoted out of `arc.rs` into a shared
   `replacer/ordered_list.rs` (recommended — second consumer now
   exists; decide at session-1 review).
