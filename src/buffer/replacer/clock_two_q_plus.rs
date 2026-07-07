//! Clock2Q+ (and S3-FIFO) page replacement — paper reproduction.
//!
//! Reproduces *Clock2Q+: A Simple and Efficient Replacement Algorithm for
//! Metadata Cache in VMware vSAN* (arXiv 2511.21958; Zhai et al.).
//! Traceability, claims table, and the ambiguity log live in
//! `docs/reproductions/clock2q-plus/REPRODUCTION.md` — every `A#`/`C#`
//! referenced below is an entry there.
//!
//! Structure (paper §3.4): a **Small FIFO** (10% of capacity) receives all
//! new blocks; a **Main Clock** (the rest) holds promoted blocks; a
//! **Ghost FIFO** (ids only, 50% of total capacity — A7) remembers blocks
//! demoted from Small so a genuine re-access can be recognized.
//!
//! The paper's novelty (§2.2, Fig 5): the **correlation window** — the
//! newest 50% of the Small FIFO (5% of total capacity). Hits inside the
//! window do NOT set the reference bit, so *correlated references*
//! (bursts of re-touches just after insertion, e.g. one B+ tree leaf
//! serving several adjacent logical reads) don't masquerade as reuse.
//! A block earns promotion only by being re-accessed AFTER aging past
//! the window.
//!
//! One parameterized core yields four registered policies — the paper's
//! own design lineage (§3.2–3.4):
//! - `clock2q+`    — small 10%, ghost 50%, window 50% of Small (§3.4).
//! - `clock2q`     — small 25% (2Q fractions), ghost 50%, no window (§3.2).
//! - `s3fifo`      — small 10%, ghost 100%, no window, promote at 1 (§3.3).
//! - `s3fifo-2bit` — as s3fifo but promotion needs TWO re-references
//!   (§3.3, A10 resolved) — the paper's strongest baseline (C1).
//!
//! The window and the 2-bit threshold are two competing answers to the
//! correlated-reference problem; the reproduction compares them head-on.
//!
//! Movement counters (`MovementStats`) mirror the paper's Table 1 — the
//! figure-free validation instrument (C8).

use std::collections::{HashMap, HashSet};

use crate::common::{FrameId, PageId};

use super::ordered_list::OrderedList;
use super::{EvictionPolicy, MovementStats, PolicyState};

/// One entry in the Main Clock ring — intrusive circular links (a sole
/// entry links to itself), the same O(1) layout as `clock.rs`. This
/// file briefly copied clock.rs's ORIGINAL Vec + index-map pattern; its
/// O(n) insert-and-reindex was 53% of the w86 trace-replay profile
/// (104K-frame Main), so both files now share the ring design.
#[derive(Debug, Clone)]
struct MainEntry {
    ref_bit: bool,
    prev: FrameId,
    next: FrameId,
}

/// Clock2Q+ / S3-FIFO replacement policy (see module docs).
pub struct Clock2QPlusReplacer {
    /// `"clock2q+"` or `"s3fifo"` — fixed at construction.
    policy_name: &'static str,

    /// Small FIFO: newest at the back. Insertion seqs drive the O(1)
    /// correlation-window test (A13: seq arithmetic over-estimates age
    /// after rare mid-list removals — conservative, documented).
    small: OrderedList<FrameId>,
    /// Per-entry access counters for Small entries, bumped only by
    /// post-window hits, saturating at 3 (2 bits — §3.3's "S3-FIFO
    /// 2-bit" and the 1-bit configs share this representation).
    small_freq: HashMap<FrameId, u8>,

    /// Main Clock: frame state + intrusive ring links, keyed by frame.
    main: HashMap<FrameId, MainEntry>,
    /// The frame the Main sweep examines next. `None` iff Main is empty.
    hand: Option<FrameId>,

    /// Ghost FIFO of demoted block ids. Full `PageId`s, not hashed
    /// fingerprints (A6). Invariant: ghost ∩ resident = ∅ (A5).
    ghost: OrderedList<PageId>,

    /// Capacities (paper §3.4): small = 10% of `c` (min 1); ghost = 50%
    /// of TOTAL capacity for clock2q+ / 100% for s3fifo (A7); window in
    /// ENTRIES = window% of `small_cap` (0 disables — s3fifo).
    small_cap: usize,
    ghost_cap: usize,
    window_entries: u64,
    /// Post-window accesses required for Small→Main promotion: 1 for
    /// clock2q+/clock2q/s3fifo, 2 for s3fifo-2bit ("re-referenced at
    /// least twice before they can be moved to the Main Clock", §3.3 —
    /// A10 resolved).
    promote_threshold: u8,

    frame_to_page: HashMap<FrameId, PageId>,
    evictable: HashSet<FrameId>,
    pending_page_scores: HashMap<PageId, u64>,

    stats: MovementStats,
}

impl Clock2QPlusReplacer {
    /// Paper-default Clock2Q+ (§3.4): small 10%, ghost 50%, window 50%.
    pub fn new(capacity: usize) -> Self {
        Self::with_fractions(capacity, 10, 50, 50, 1, "clock2q+")
    }

    /// S3-FIFO (1-bit) baseline (§3.3): ghost 100%, no window.
    pub fn s3_fifo(capacity: usize) -> Self {
        Self::with_fractions(capacity, 10, 100, 0, 1, "s3fifo")
    }

    /// S3-FIFO 2-bit baseline (§3.3): promotion needs TWO re-references
    /// — the paper's strongest baseline (the 28.5% headline, C1).
    pub fn s3_fifo_2bit(capacity: usize) -> Self {
        Self::with_fractions(capacity, 10, 100, 0, 2, "s3fifo-2bit")
    }

    /// Clock2Q predecessor (§3.2): 2Q fractions (Small 25%), clock Main,
    /// ghost 50%, no window.
    pub fn clock_two_q(capacity: usize) -> Self {
        Self::with_fractions(capacity, 25, 50, 0, 1, "clock2q")
    }

    /// Sweepable constructor for the C3/C4 sensitivity experiments.
    /// `small_pct`/`ghost_pct` are % of total capacity; `window_pct` is
    /// % of the Small FIFO's capacity.
    pub fn with_fractions(
        capacity: usize,
        small_pct: usize,
        ghost_pct: usize,
        window_pct: usize,
        promote_threshold: u8,
        policy_name: &'static str,
    ) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        assert!((1..=100).contains(&small_pct), "small_pct out of range");
        assert!(window_pct <= 100, "window_pct out of range");
        assert!(
            (1..=3).contains(&promote_threshold),
            "promote_threshold must fit the 2-bit counter"
        );
        let small_cap = (capacity * small_pct / 100).max(1);
        let ghost_cap = (capacity * ghost_pct / 100).max(1);
        // A3: clamp — a window that rounds to 0 disables suppression
        // (plain S3-FIFO behavior at tiny capacities).
        let window_entries = (small_cap * window_pct / 100) as u64;
        Self {
            policy_name,
            small: OrderedList::new(),
            small_freq: HashMap::new(),
            main: HashMap::new(),
            hand: None,
            ghost: OrderedList::new(),
            small_cap,
            ghost_cap,
            window_entries,
            promote_threshold,
            frame_to_page: HashMap::new(),
            evictable: HashSet::new(),
            pending_page_scores: HashMap::new(),
            stats: MovementStats::default(),
        }
    }

    /// Correlation-window membership (Fig 5): a Small entry is in-window
    /// while fewer than `window_entries` blocks are newer than it —
    /// i.e. within window-size blocks of the insertion side ("head" in
    /// the paper's orientation, A2). Age via seq arithmetic (A13);
    /// boundary is strict-< (A1: window = strictly-newer region).
    fn in_correlation_window(&self, frame_id: FrameId) -> bool {
        if self.window_entries == 0 {
            return false;
        }
        let Some(seq) = self.small.seq(&frame_id) else {
            return false;
        };
        // next_seq - 1 is the newest live seq under pure FIFO motion.
        let age = (self.small.next_seq() - 1).saturating_sub(seq);
        age < self.window_entries
    }

    /// Insert a frame into the Main Clock behind the hand (A8 choice:
    /// maximal residency before the first sweep), ref bit clear.
    fn insert_main(&mut self, frame_id: FrameId) {
        debug_assert!(!self.main.contains_key(&frame_id), "frame already in Main");
        let (prev, next) = match self.hand {
            // Splice just before the hand: the sweep reaches it last.
            Some(hand_id) => (self.main[&hand_id].prev, hand_id),
            // First frame: self-linked ring.
            None => (frame_id, frame_id),
        };
        self.main.insert(
            frame_id,
            MainEntry {
                ref_bit: false,
                prev,
                next,
            },
        );
        if prev == frame_id {
            self.hand = Some(frame_id);
        } else {
            self.main.get_mut(&prev).expect("ring: prev missing").next = frame_id;
            self.main.get_mut(&next).expect("ring: next missing").prev = frame_id;
        }
    }

    /// Remove a frame from the Main ring; the hand moves to the
    /// successor if it pointed at the removed frame.
    fn remove_main(&mut self, frame_id: FrameId) -> bool {
        let Some(entry) = self.main.remove(&frame_id) else {
            return false;
        };
        if entry.next == frame_id {
            // Sole entry: it must self-link, and the ring becomes empty.
            assert_eq!(entry.prev, frame_id, "sole ring entry must self-link");
            self.hand = None;
        } else {
            self.main
                .get_mut(&entry.prev)
                .expect("ring: prev of removed frame missing")
                .next = entry.next;
            self.main
                .get_mut(&entry.next)
                .expect("ring: next of removed frame missing")
                .prev = entry.prev;
            if self.hand == Some(frame_id) {
                self.hand = Some(entry.next);
            }
        }
        true
    }

    /// Demote a Small entry's page id to the Ghost FIFO, trimming to
    /// capacity (paper §3.4: ghost is plain FIFO).
    fn ghost_push(&mut self, page_id: PageId) {
        self.ghost.push_back(page_id);
        while self.ghost.len() > self.ghost_cap {
            self.ghost.pop_front();
        }
    }

    /// Fully de-track an evicted frame (either queue).
    fn forget(&mut self, frame_id: FrameId) {
        self.small.remove(&frame_id);
        self.small_freq.remove(&frame_id);
        self.remove_main(frame_id);
        self.evictable.remove(&frame_id);
        self.frame_to_page.remove(&frame_id);
    }

    /// Evict from the Small FIFO, applying S3-FIFO's promote-or-demote
    /// rule oldest-first (§3.3/§3.4): ref set → promote to Main
    /// (promotion is bookkeeping, so pinned frames promote too);
    /// ref clear + evictable → victim (id to Ghost); ref clear + pinned
    /// → skip. Returns the victim, or None if Small has no candidate.
    fn evict_from_small(&mut self) -> Option<FrameId> {
        // Cursor scan in insertion order: promotions/demotions mutate
        // `small` mid-scan, so each step re-queries the oldest entry
        // past the last-visited seq (O(log n)/step, no snapshot — the
        // old per-eviction Vec snapshot was O(|Small|) and dominated
        // large-cache replay profiles alongside the Vec insert_main).
        let step_limit = self.small.len();
        let mut steps = 0;
        let mut cursor: u64 = 0;
        while let Some((seq, frame_id)) = self.small.first_at_or_after(cursor) {
            cursor = seq + 1; // seqs are unique: no entry is revisited
            steps += 1;
            assert!(steps <= step_limit, "Small scan visits each entry once");
            let freq = self.small_freq.get(&frame_id).copied().unwrap_or(0);
            if freq >= self.promote_threshold {
                self.small_freq.remove(&frame_id);
                // C8: Small → Main promotion.
                self.small.remove(&frame_id);
                self.insert_main(frame_id);
                self.stats.small_to_main += 1;
                continue;
            }
            if self.evictable.contains(&frame_id) {
                // C8: Small → Ghost demotion.
                let page = self.frame_to_page.get(&frame_id).copied();
                self.forget(frame_id);
                if let Some(pid) = page {
                    self.ghost_push(pid);
                }
                self.stats.small_to_ghost += 1;
                return Some(frame_id);
            }
            // Pinned and unreferenced: skip (the BPM contract — never
            // return a pinned frame; the paper's production skip, §5.5.2).
        }
        None
    }

    /// Standard clock sweep over Main (paper §3.4; layout per clock.rs):
    /// ref set → clear + advance; ref clear + evictable → victim;
    /// pinned → skip. Bounded to 2×len (all-ref degrades to FIFO).
    /// Main evictions do NOT enter the ghost (§3.4: only Small
    /// demotions are ghosted).
    fn evict_from_main(&mut self) -> Option<FrameId> {
        let bound = self.main.len() * 2;
        for _ in 0..bound {
            let hand_id = self.hand?; // None iff Main is empty
            let entry = self
                .main
                .get_mut(&hand_id)
                .expect("hand frame must be tracked in Main");
            let next = entry.next;
            if !self.evictable.contains(&hand_id) {
                self.hand = Some(next);
                continue;
            }
            if entry.ref_bit {
                entry.ref_bit = false;
                self.hand = Some(next);
                continue;
            }
            // forget() -> remove_main() leaves the hand on the successor.
            self.forget(hand_id);
            return Some(hand_id);
        }
        None
    }
}

impl EvictionPolicy for Clock2QPlusReplacer {
    fn name(&self) -> &'static str {
        self.policy_name
    }

    fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
        // HIT in Main: standard clock ref bit.
        if let Some(entry) = self.main.get_mut(&frame_id) {
            entry.ref_bit = true;
            self.frame_to_page.insert(frame_id, page_id);
            return;
        }
        // HIT in Small: THE paper rule (Fig 5) — in-window hits are
        // correlated references (§2.2) and must NOT set the ref bit.
        if self.small.contains(&frame_id) {
            if !self.in_correlation_window(frame_id) {
                let freq = self.small_freq.entry(frame_id).or_insert(0);
                *freq = freq.saturating_add(1).min(3); // 2-bit cap (§3.3)
            }
            self.frame_to_page.insert(frame_id, page_id);
            return;
        }

        // INSERT path (frame untracked).
        self.frame_to_page.insert(frame_id, page_id);
        let imported_hot = self
            .pending_page_scores
            .remove(&page_id)
            .map(|s| s > 0)
            .unwrap_or(false);
        if self.ghost.remove(&page_id) {
            // Ghost hit (§3.4): re-access after demotion is real reuse —
            // straight to Main (C8 counter).
            self.insert_main(frame_id);
            self.stats.ghost_to_main += 1;
        } else if imported_hot {
            // Warm-swap import: previously-hot pages go to Main.
            self.insert_main(frame_id);
        } else {
            // New block: Small FIFO, ref clear (§3.3 discipline).
            self.small.push_back(frame_id);
            debug_assert!(!self.small_freq.contains_key(&frame_id));
        }
    }

    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        // Q-35 contract: `evictable ⊆ tracked` — see FifoReplacer.
        let tracked = self.small.contains(&frame_id) || self.main.contains_key(&frame_id);
        if evictable && !tracked {
            return;
        }
        if evictable {
            self.evictable.insert(frame_id);
        } else {
            self.evictable.remove(&frame_id);
        }
    }

    fn evict(&mut self) -> Option<FrameId> {
        if self.evictable.is_empty() {
            return None;
        }
        // A9 choice: evict from Small when it is at/over its target,
        // else from Main — with cross-queue fallback both ways. The
        // fallback is the paper's own §4.1.3 rule ("if all scanned
        // entries are dirty … checks the Main Clock"), with pinned
        // frames playing the ineligible-entry role dirty blocks play
        // there (see REPRODUCTION.md A14).
        //
        // TWO rounds, provably sufficient: the Small scan PROMOTES
        // ref-set entries into Main, so round 1 can move the only
        // evictable candidate into a queue it already searched (found
        // by the correctness-neutrality differential at POOL=4, where
        // latch-crabbing pins everything except one ref-set Small
        // entry). After round 1, Small holds no ref bits (all promoted)
        // and Main's sweep has cleared evictable entries' refs — so if
        // any evictable frame exists, round 2 must find it. This is
        // S3-FIFO's repeat-until-freed eviction loop (inherited
        // implicitly by the paper) with an explicit termination bound.
        for _ in 0..2 {
            let victim = if self.small.len() >= self.small_cap {
                self.evict_from_small().or_else(|| self.evict_from_main())
            } else {
                self.evict_from_main().or_else(|| self.evict_from_small())
            };
            if victim.is_some() {
                return victim;
            }
        }
        None
    }

    fn remove(&mut self, frame_id: FrameId) {
        // Page deletion is not an eviction: no ghost entry (a ghosted
        // deleted page would fake a "reuse" if its id is reallocated).
        self.forget(frame_id);
    }

    fn size(&self) -> usize {
        self.evictable.len()
    }

    fn export_state(&self) -> PolicyState {
        let mut state = PolicyState::new(self.name());
        let mut score = 1u64;
        // Small entries: colder (lower scores), oldest first.
        for &frame_id in self.small.iter() {
            if let Some(&page_id) = self.frame_to_page.get(&frame_id) {
                state.hot_pages.push((page_id, score));
                score += 1;
            }
        }
        // Main entries: hotter — same +1000 gap convention as ARC.
        // One ring walk in sweep order from the hand (next victim
        // candidate first). NOTE: the old Vec layout exported in index
        // order, an equally arbitrary rotation — only relative
        // Small-below-Main scoring is contractual.
        score += 1000;
        if let Some(hand_id) = self.hand {
            let mut cursor = hand_id;
            for _ in 0..self.main.len() {
                if let Some(&page_id) = self.frame_to_page.get(&cursor) {
                    state.hot_pages.push((page_id, score));
                    score += 1;
                }
                cursor = self.main[&cursor].next;
            }
            assert_eq!(cursor, hand_id, "ring walk must return to the hand");
        }
        state
    }

    fn import_state(&mut self, state: &PolicyState) {
        self.pending_page_scores.clear();
        for &(page_id, score) in &state.hot_pages {
            self.pending_page_scores.insert(page_id, score);
        }
    }

    /// Table-1 movement counters (C8).
    fn movement_stats(&self) -> Option<MovementStats> {
        Some(self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fid(i: usize) -> FrameId {
        FrameId::new(i)
    }
    fn pid(i: u32) -> PageId {
        PageId::new(i)
    }

    /// capacity 40 → small_cap 4, window 2 entries, ghost 20.
    fn small_policy() -> Clock2QPlusReplacer {
        Clock2QPlusReplacer::new(40)
    }

    #[test]
    fn new_blocks_enter_small_with_ref_clear() {
        let mut p = small_policy();
        p.record_access(fid(0), pid(100));
        assert!(p.small.contains(&fid(0)));
        assert!(!p.small_freq.contains_key(&fid(0)));
        assert!(p.main.is_empty());
    }

    // Fig 5: a hit while still within window-size blocks of the
    // insertion side must NOT set the ref bit.
    #[test]
    fn in_window_hit_does_not_set_ref() {
        let mut p = small_policy(); // window = 2
        p.record_access(fid(0), pid(100));
        p.record_access(fid(1), pid(101)); // fid0 age 1, fid1 age 0 — both in window
        p.record_access(fid(0), pid(100)); // correlated re-touch
        assert!(
            !p.small_freq.contains_key(&fid(0)),
            "in-window hit must not count (correlated reference, §2.2)"
        );
    }

    #[test]
    fn post_window_hit_sets_ref() {
        let mut p = small_policy(); // window = 2
        p.record_access(fid(0), pid(100));
        p.record_access(fid(1), pid(101));
        p.record_access(fid(2), pid(102)); // fid0 now age 2 — past the window
        p.record_access(fid(0), pid(100)); // genuine reuse
        assert_eq!(
            p.small_freq.get(&fid(0)),
            Some(&1),
            "post-window hit must count"
        );
    }

    // A1: boundary is strict — age == window_entries is OUTSIDE.
    #[test]
    fn window_boundary_is_strict() {
        let p = small_policy(); // window = 2
        let mut p2 = p;
        p2.record_access(fid(0), pid(100)); // age 0: in
        assert!(p2.in_correlation_window(fid(0)));
        p2.record_access(fid(1), pid(101)); // fid0 age 1: in
        assert!(p2.in_correlation_window(fid(0)));
        p2.record_access(fid(2), pid(102)); // fid0 age 2 == window: out
        assert!(!p2.in_correlation_window(fid(0)));
    }

    // s3fifo-2bit (§3.3): ONE re-reference is not enough to promote;
    // two are. The counter is small-side (A10).
    #[test]
    fn s3fifo_2bit_requires_two_rehits_to_promote() {
        let mut p = Clock2QPlusReplacer::s3_fifo_2bit(40); // small_cap 4
        for i in 0..4 {
            p.record_access(fid(i), pid(100 + i as u32));
            p.set_evictable(fid(i), true);
        }
        p.record_access(fid(0), pid(100)); // 1st re-hit: freq 1 < 2
        let v = p.evict().unwrap();
        assert_eq!(v, fid(0), "one re-hit must NOT save fid0 under 2-bit");

        let mut p = Clock2QPlusReplacer::s3_fifo_2bit(40);
        for i in 0..4 {
            p.record_access(fid(i), pid(100 + i as u32));
            p.set_evictable(fid(i), true);
        }
        p.record_access(fid(0), pid(100)); // freq 1
        p.record_access(fid(0), pid(100)); // freq 2
        let v = p.evict().unwrap();
        assert_ne!(v, fid(0), "two re-hits must promote fid0");
        assert!(p.main.contains_key(&fid(0)));
    }

    // clock2q (§3.2): 2Q fractions, no window — first re-hit counts.
    #[test]
    fn clock2q_uses_2q_fractions_and_no_window() {
        let p = Clock2QPlusReplacer::clock_two_q(40);
        assert_eq!(p.name(), "clock2q");
        assert_eq!(p.small_cap, 10); // 25% of 40
        assert_eq!(p.ghost_cap, 20); // 50% of 40
        assert_eq!(p.window_entries, 0);
    }

    // s3fifo config: window 0 ⇒ every Small hit sets the ref bit.
    #[test]
    fn s3fifo_sets_ref_on_first_rehit() {
        let mut p = Clock2QPlusReplacer::s3_fifo(40);
        p.record_access(fid(0), pid(100));
        p.record_access(fid(0), pid(100));
        assert_eq!(p.small_freq.get(&fid(0)), Some(&1));
        assert_eq!(p.name(), "s3fifo");
    }

    // §3.3/3.4 eviction: ref set → promote to Main; ref clear → Ghost.
    #[test]
    fn small_eviction_promotes_referenced_and_ghosts_unreferenced() {
        let mut p = small_policy(); // small_cap 4, window 2
        for i in 0..4 {
            p.record_access(fid(i), pid(100 + i as u32));
            p.set_evictable(fid(i), true);
        }
        // fid0 is past the window (age 3): genuine reuse → ref set.
        p.record_access(fid(0), pid(100));
        assert_eq!(p.small_freq.get(&fid(0)), Some(&1));

        let victim = p.evict().expect("small has evictable frames");
        // fid0 promoted (not evicted); fid1 (oldest unreferenced) evicted.
        assert_eq!(victim, fid(1));
        assert!(p.main.contains_key(&fid(0)), "ref-set frame promoted");
        assert!(p.ghost.contains(&pid(101)), "victim's page ghosted");
        let s = p.movement_stats().unwrap();
        assert_eq!(
            (s.small_to_main, s.small_to_ghost, s.ghost_to_main),
            (1, 1, 0)
        );
    }

    // §3.4 ghost hit: re-access after demotion goes straight to Main.
    #[test]
    fn ghost_hit_inserts_to_main() {
        let mut p = small_policy();
        for i in 0..4 {
            p.record_access(fid(i), pid(100 + i as u32));
            p.set_evictable(fid(i), true);
        }
        let victim = p.evict().unwrap(); // fid0 (oldest, no refs anywhere)
        assert_eq!(victim, fid(0));
        assert!(p.ghost.contains(&pid(100)));

        // Page 100 returns in a fresh frame: ghost hit → Main directly.
        p.record_access(fid(9), pid(100));
        assert!(p.main.contains_key(&fid(9)));
        assert!(!p.ghost.contains(&pid(100)), "A5: ghost ∩ resident = ∅");
        assert_eq!(p.movement_stats().unwrap().ghost_to_main, 1);
    }

    #[test]
    fn ghost_trims_to_capacity_fifo() {
        let mut p = Clock2QPlusReplacer::with_fractions(20, 10, 10, 0, 1, "s3fifo");
        // ghost_cap = 2. Demote three unreferenced blocks.
        for i in 0..3 {
            p.record_access(fid(i), pid(100 + i as u32));
            p.set_evictable(fid(i), true);
            // small_cap = 2, so inserting drives Small over target.
        }
        while p.evict().is_some() {}
        assert!(p.ghost.len() <= 2, "ghost must trim to cap");
        // Oldest ghost id (100) trimmed first (FIFO).
        assert!(!p.ghost.contains(&pid(100)));
    }

    // Main clock: ref grants exactly one second chance.
    #[test]
    fn main_clock_second_chance() {
        let mut p = small_policy();
        // Drive two frames into Main via ghost hits.
        for i in 0..2 {
            p.record_access(fid(i), pid(200 + i as u32));
            p.set_evictable(fid(i), true);
        }
        while p.evict().is_some() {}
        p.record_access(fid(10), pid(200)); // ghost hit → Main
        p.record_access(fid(11), pid(201)); // ghost hit → Main
        p.set_evictable(fid(10), true);
        p.set_evictable(fid(11), true);
        p.record_access(fid(10), pid(200)); // Main hit: ref set

        // Sweep: fid10 gets a second chance; a victim must still emerge.
        let victim = p.evict().expect("main has evictable frames");
        assert_eq!(victim, fid(11), "unreferenced main entry evicted first");
        assert!(
            p.main.contains_key(&fid(10)),
            "referenced entry survived the sweep"
        );
    }

    // The ARC lesson: preferred queue pinned ⇒ fall back, never None
    // while an evictable frame exists.
    #[test]
    fn falls_back_across_queues_when_preferred_is_pinned() {
        let mut p = small_policy(); // small_cap 4
        for i in 0..4 {
            p.record_access(fid(i), pid(100 + i as u32));
            p.set_evictable(fid(i), true);
        }
        let v = p.evict().unwrap(); // ghost one block
        let ghosted_pid = pid(100 + v.0 as u32);
        assert!(p.ghost.contains(&ghosted_pid));
        p.record_access(fid(9), ghosted_pid); // → Main
        p.set_evictable(fid(9), true);
        // Pin everything in Small.
        for i in 0..4 {
            p.set_evictable(fid(i), false);
        }
        // Small is at/over cap → preferred queue is Small → all pinned →
        // must fall back to Main and evict fid9.
        assert_eq!(p.evict(), Some(fid(9)));
    }

    // Q-35 contract: set_evictable(true) on an untracked frame no-ops.
    #[test]
    fn untracked_set_evictable_is_noop() {
        let mut p = small_policy();
        p.set_evictable(fid(7), true);
        assert_eq!(p.size(), 0);
        assert_eq!(p.evict(), None);
    }

    #[test]
    fn remove_purges_without_ghosting() {
        let mut p = small_policy();
        p.record_access(fid(0), pid(100));
        p.set_evictable(fid(0), true);
        p.remove(fid(0));
        assert_eq!(p.size(), 0);
        assert!(!p.ghost.contains(&pid(100)), "deletion is not eviction");
        assert_eq!(p.evict(), None);
    }

    /// Simulate the BPM's exact call pattern over a small frame pool:
    /// miss → evict-if-full → record_access(new) + set_evictable(false)
    /// → guard drop sets evictable(true); hit → record_access +
    /// set_evictable(false) → true. The policy must ALWAYS produce a
    /// victim when every frame is unpinned — this is the exact contract
    /// the correctness-neutrality differential exercises.
    fn bpm_pattern(mut p: Clock2QPlusReplacer, frames: usize, rounds: usize) {
        let mut resident: HashMap<u32, usize> = HashMap::new(); // page -> frame
        let mut free: Vec<usize> = (0..frames).collect();
        let mut rng: u64 = 0x9E3779B97F4A7C15;
        for step in 0..rounds {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let page = (rng >> 33) as u32 % 24;
            if let Some(&frame) = resident.get(&page) {
                p.record_access(fid(frame), pid(page));
                p.set_evictable(fid(frame), false);
                p.set_evictable(fid(frame), true);
                continue;
            }
            let frame = if let Some(f) = free.pop() {
                f
            } else {
                // Latch-crabbing dimension: pin a rotating subset of
                // residents during the eviction, like a live B-tree
                // descent would. At least one frame stays evictable.
                let pinned: Vec<usize> = resident
                    .values()
                    .copied()
                    .filter(|f| (f + step) % 3 == 0)
                    .take(frames - 1)
                    .collect();
                for &f in &pinned {
                    p.set_evictable(fid(f), false);
                }
                let victim = p.evict().unwrap_or_else(|| {
                    panic!(
                        "NoFreeFrames at step {}: an evictable frame exists, evict() must succeed",
                        step
                    )
                });
                assert!(
                    !pinned.contains(&victim.0),
                    "evicted a pinned frame at step {}",
                    step
                );
                for &f in &pinned {
                    p.set_evictable(fid(f), true);
                }
                let (&vp, _) = resident
                    .iter()
                    .find(|(_, &f)| f == victim.0)
                    .expect("victim must be a resident frame");
                resident.remove(&vp);
                victim.0
            };
            resident.insert(page, frame);
            p.record_access(fid(frame), pid(page));
            p.set_evictable(fid(frame), false);
            p.set_evictable(fid(frame), true);
        }
    }

    // Regression for the differential's POOL=4 failure: the ONLY
    // evictable frame is in Small WITH its ref bit set; everything else
    // is pinned. Round 1 promotes it to Main after Main was already
    // searched — a single-round evict() returned None (= NoFreeFrames
    // in the BPM). The two-round evict must return it.
    #[test]
    fn lone_refset_small_entry_is_still_evictable() {
        let mut p = small_policy(); // small_cap 4, window 2
        for i in 0..4 {
            p.record_access(fid(i), pid(100 + i as u32));
        }
        // fid0 aged past the window: genuine reuse → ref set.
        p.record_access(fid(0), pid(100));
        assert_eq!(p.small_freq.get(&fid(0)), Some(&1));
        // Only fid0 is evictable; the rest stay pinned.
        p.set_evictable(fid(0), true);

        let victim = p.evict();
        assert_eq!(
            victim,
            Some(fid(0)),
            "the lone evictable frame must be evicted even when round 1 \
             promotes it into an already-searched queue"
        );
        assert_eq!(p.size(), 0);
    }

    #[test]
    fn bpm_pattern_never_starves_clock2q_plus() {
        bpm_pattern(Clock2QPlusReplacer::new(64), 8, 4_000);
    }

    #[test]
    fn bpm_pattern_never_starves_s3fifo() {
        bpm_pattern(Clock2QPlusReplacer::s3_fifo(64), 8, 4_000);
    }

    #[test]
    fn export_scores_main_above_small() {
        let mut p = small_policy();
        p.record_access(fid(0), pid(100)); // Small
        for i in 1..5 {
            p.record_access(fid(i), pid(100 + i as u32));
            p.set_evictable(fid(i), true);
        }
        let v = p.evict().unwrap();
        let gpid = pid(100 + v.0 as u32);
        p.record_access(fid(9), gpid); // Main via ghost
        let state = p.export_state();
        let small_score = state
            .hot_pages
            .iter()
            .find(|(pg, _)| *pg == pid(100))
            .map(|(_, s)| *s)
            .unwrap();
        let main_score = state
            .hot_pages
            .iter()
            .find(|(pg, _)| *pg == gpid)
            .map(|(_, s)| *s)
            .unwrap();
        assert!(main_score > small_score);
    }
}
