//! LRU-K with the Correlated Reference Period — the 1993 algorithm as
//! specified, not as commonly implemented.
//!
//! Our own experiment (not part of the Clock2Q+ paper's evaluation):
//! O'Neil, O'Neil & Weikum's SIGMOD '93 LRU-K paper anticipated the
//! correlated-references problem that Clock2Q+ (arXiv 2511.21958)
//! solves with its correlation window — and prescribed a fix that
//! modern reimplementations (including our `lru_k.rs`) drop. This file
//! restores the full Figure 2.1 algorithm so the 2025 result can be
//! measured against the 1993 literature done right. Ambiguity entries
//! A18/A19 in `docs/reproductions/clock2q-plus/REPRODUCTION.md` cover
//! the two adaptation choices.
//!
//! The three mechanisms of the 1993 paper (§2.1.1–2.1.3):
//! 1. **Correlated Reference Period (CRP)**: a re-reference within
//!    `crp` of `LAST(p)` is correlated — it updates `LAST(p)` only,
//!    never the K-distance history. When an UNcorrelated reference
//!    arrives, the whole correlated span collapses to zero width:
//!    `HIST(p,i) := HIST(p,i-1) + (LAST(p) - HIST(p,1))`, then
//!    `HIST(p,1) := t` ("we do not want to penalize or credit the page
//!    for that").
//! 2. **Eviction protection**: only frames with `t - LAST(q) > crp`
//!    are eligible victims. A page gets its correlated burst in peace.
//! 3. **Retained Information Period (RIP, §2.1.2)**: history is kept
//!    for evicted PAGES (bounded FIFO), so a page returning promptly
//!    resumes its history instead of restarting as a stranger. This is
//!    the ghost list, in 1993.
//!
//! A18: O'Neil's CRP/RIP are wall-clock timeouts; replay has no wall
//! clock, so both are measured on the logical access clock (CRP in
//! accesses, RIP in retained entries) — mirroring how Clock2Q+'s
//! window is measured in insertions, which keeps the comparison
//! apples-to-apples. A19: lengths are tuning parameters in the paper;
//! we sweep CRP and fix RIP at cache capacity (the ghost-100% analog).

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use crate::common::{FrameId, PageId};

use super::ordered_list::OrderedList;
use super::{EvictionPolicy, PolicyState};

/// Reference history of a PAGE (not a frame): survives eviction via
/// the retained store, per §2.1.2.
#[derive(Debug, Clone)]
struct PageHistory {
    /// Logical times of the K most recent UNcorrelated references,
    /// newest at back, correlated spans collapsed. Max length K.
    hist: VecDeque<u64>,

    /// Logical time of the most recent reference, correlated or not
    /// (O'Neil's `LAST(p)`).
    last: u64,
}

impl PageHistory {
    fn new(seq: u64, k: usize) -> Self {
        let mut hist = VecDeque::with_capacity(k);
        hist.push_back(seq);
        Self { hist, last: seq }
    }

    /// Record a reference at `seq` (Fig 2.1 update rules).
    fn record(&mut self, seq: u64, k: usize, crp: u64) {
        assert!(seq >= self.last, "logical clock must be monotonic");
        if seq - self.last > crp {
            // Uncorrelated: collapse the correlated span to zero width,
            // then start a new history period at `seq`.
            let correlated_span = self.last - self.hist.back().copied().unwrap_or(self.last);
            for time in self.hist.iter_mut() {
                *time += correlated_span;
            }
            self.hist.push_back(seq);
            while self.hist.len() > k {
                self.hist.pop_front();
            }
        }
        // Correlated (or first): LAST always advances.
        self.last = seq;
    }

    /// Logical time of the Kth most recent uncorrelated reference.
    /// None if fewer than K (backward K-distance = +∞).
    fn kth_recent_seq(&self, k: usize) -> Option<u64> {
        if self.hist.len() < k {
            None
        } else {
            Some(self.hist[self.hist.len() - k])
        }
    }

    /// Eviction sort key — minimum evicts first. Class 0 = +∞
    /// K-distance (FIFO by first known reference); class 1 = finite,
    /// oldest Kth-recent first. Same shape as `lru_k.rs`.
    fn victim_key(&self, k: usize, frame_id: FrameId) -> (u8, u64, usize) {
        match self.kth_recent_seq(k) {
            None => (0, self.hist.front().copied().unwrap_or(0), frame_id.0),
            Some(kth_seq) => (1, kth_seq, frame_id.0),
        }
    }
}

/// LRU-K with Correlated Reference Period + Retained Information
/// Period — O'Neil et al. 1993, Figure 2.1, on a logical clock.
pub struct LruKCrpReplacer {
    /// The K parameter (number of uncorrelated references tracked).
    k: usize,

    /// Correlated Reference Period, in logical accesses.
    crp: u64,

    /// Logical access clock; incremented on every `record_access`.
    access_clock: u64,

    /// The page each tracked frame holds.
    frame_to_page: HashMap<FrameId, PageId>,

    /// History for RESIDENT pages, keyed by page (§2.1.2: history is a
    /// page property; frames are just its current address).
    history: HashMap<PageId, PageHistory>,

    /// BPM pin state: frames whose pin count is 0.
    evictable: HashSet<FrameId>,

    /// Frames still within their CRP, ordered by `LAST` (each access
    /// re-appends, so front = longest-untouched = first to expire).
    protected: OrderedList<FrameId>,

    /// CRP-expired evictable frames ordered by victim key — `evict`
    /// pops the first, O(log n), after draining expirations.
    victim_order: BTreeSet<(u8, u64, usize)>,

    /// Retained Information Period store: histories of evicted pages,
    /// FIFO by eviction time, trimmed to `retained_cap`.
    retained: OrderedList<PageId>,
    retained_hist: HashMap<PageId, PageHistory>,
    retained_cap: usize,

    /// Imported page scores waiting for frame assignment (warm swap).
    pending_page_scores: HashMap<PageId, u64>,
}

impl LruKCrpReplacer {
    /// `crp` in logical accesses; `retained_cap` bounds the RIP store.
    pub fn new(k: usize, crp: u64, retained_cap: usize) -> Self {
        assert!(k >= 1, "K must be at least 1");
        assert!(retained_cap >= 1, "RIP store needs capacity");
        Self {
            k,
            crp,
            access_clock: 0,
            frame_to_page: HashMap::new(),
            history: HashMap::new(),
            evictable: HashSet::new(),
            protected: OrderedList::new(),
            victim_order: BTreeSet::new(),
            retained: OrderedList::new(),
            retained_hist: HashMap::new(),
            retained_cap,
            pending_page_scores: HashMap::new(),
        }
    }

    /// A frame is CRP-expired when its page's last reference is more
    /// than `crp` accesses old. `crp == 0` disables protection outright
    /// (the logical clock freezes between accesses, so a strict `> 0`
    /// would still shield the just-touched frame at eviction time —
    /// with protection AND collapsing both inert, crp = 0 is exactly
    /// plain LRU-K + RIP).
    fn is_expired(&self, frame_id: FrameId) -> bool {
        if self.crp == 0 {
            return true;
        }
        let page_id = self.frame_to_page[&frame_id];
        self.access_clock - self.history[&page_id].last > self.crp
    }

    fn victim_key_of(&self, frame_id: FrameId) -> (u8, u64, usize) {
        let page_id = self.frame_to_page[&frame_id];
        self.history[&page_id].victim_key(self.k, frame_id)
    }

    /// Move CRP-expired frames out of `protected`; evictable ones enter
    /// the victim order. Amortized O(log n): each protected entry is
    /// drained at most once per access that re-protects it.
    fn drain_expired(&mut self) {
        let limit = self.protected.len();
        for _ in 0..limit {
            let Some((_, frame_id)) = self.protected.first_at_or_after(0) else {
                break;
            };
            if !self.is_expired(frame_id) {
                break; // front = first to expire; the rest are newer
            }
            let removed = self.protected.remove(&frame_id);
            assert!(removed, "peeked frame must be in protected");
            if self.evictable.contains(&frame_id) {
                self.victim_order.insert(self.victim_key_of(frame_id));
            }
        }
    }

    /// De-track a frame and retain its page's history (RIP), unless
    /// `retain` is false (page deletion: history is garbage — mirrors
    /// the "deletion is not eviction" ghost rule in clock2q+).
    fn forget(&mut self, frame_id: FrameId, retain: bool) {
        let page_id = self
            .frame_to_page
            .remove(&frame_id)
            .expect("forgotten frame must be tracked");
        let page_history = self
            .history
            .remove(&page_id)
            .expect("tracked frame's page must have history");
        self.evictable.remove(&frame_id);
        self.protected.remove(&frame_id);
        self.victim_order
            .remove(&page_history.victim_key(self.k, frame_id));

        if retain {
            self.retained.push_back(page_id);
            self.retained_hist.insert(page_id, page_history);
            while self.retained.len() > self.retained_cap {
                let evicted = self.retained.pop_front().expect("over-cap RIP non-empty");
                self.retained_hist.remove(&evicted);
            }
        }
        assert_eq!(self.retained.len(), self.retained_hist.len());
    }
}

impl EvictionPolicy for LruKCrpReplacer {
    fn name(&self) -> &'static str {
        "lruk-crp"
    }

    fn record_access(&mut self, frame_id: FrameId, page_id: PageId) {
        self.access_clock += 1;
        let seq = self.access_clock;

        if let Some(&resident_page) = self.frame_to_page.get(&frame_id) {
            // Existing frame: keys change — pull out of both orders,
            // update, re-file as protected (it was just touched).
            assert_eq!(resident_page, page_id, "frame/page binding is stable");
            self.victim_order.remove(&self.victim_key_of(frame_id));
            self.history
                .get_mut(&page_id)
                .expect("resident page has history")
                .record(seq, self.k, self.crp);
        } else {
            // New frame. RIP first (§2.1.2): a returning page resumes
            // its retained history — this reference is uncorrelated by
            // construction (it was evicted more than a burst ago).
            let mut page_history = if self.retained.remove(&page_id) {
                self.retained_hist
                    .remove(&page_id)
                    .expect("RIP order and store agree")
            } else {
                let mut fresh = PageHistory::new(seq, self.k);
                if let Some(score) = self.pending_page_scores.remove(&page_id) {
                    if score > 0 {
                        // Warm import: seed K references at now (hot).
                        fresh.hist.clear();
                        for _ in 0..self.k {
                            fresh.hist.push_back(seq);
                        }
                    }
                }
                self.frame_to_page.insert(frame_id, page_id);
                self.history.insert(page_id, fresh);
                self.protected.push_back(frame_id);
                return;
            };
            page_history.record(seq, self.k, self.crp);
            self.frame_to_page.insert(frame_id, page_id);
            self.history.insert(page_id, page_history);
        }
        self.protected.push_back(frame_id); // (re-)enter the CRP window
    }

    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        // Q-35 contract: `evictable ⊆ tracked` — see FifoReplacer.
        if !self.frame_to_page.contains_key(&frame_id) {
            return;
        }
        if evictable {
            self.evictable.insert(frame_id);
            if !self.protected.contains(&frame_id) {
                self.victim_order.insert(self.victim_key_of(frame_id));
            }
        } else {
            self.evictable.remove(&frame_id);
            self.victim_order.remove(&self.victim_key_of(frame_id));
        }
    }

    fn evict(&mut self) -> Option<FrameId> {
        self.drain_expired();
        if let Some(&(_, _, frame_index)) = self.victim_order.iter().next() {
            let frame_id = FrameId::new(frame_index);
            self.forget(frame_id, true); // removes the victim key too
            return Some(frame_id);
        }

        // NOTE (deviation for BPM safety): O'Neil's Fig 2.1 never
        // handles "every candidate is inside its CRP" — his buffers
        // dwarf the period. A policy returning None here would starve
        // the BPM (NoFreeFrames with unpinned frames — the A14 lesson),
        // so protection yields: choose by K-distance among ALL
        // evictable frames, ignoring the CRP. Bounded by protected.len.
        let victim = self
            .protected
            .iter()
            .filter(|frame_id| self.evictable.contains(frame_id))
            .map(|&frame_id| self.victim_key_of(frame_id))
            .min()?;
        let frame_id = FrameId::new(victim.2);
        self.forget(frame_id, true);
        Some(frame_id)
    }

    fn remove(&mut self, frame_id: FrameId) {
        if self.frame_to_page.contains_key(&frame_id) {
            self.forget(frame_id, false); // deletion is not eviction
        }
    }

    fn size(&self) -> usize {
        // BPM semantics: pin-based evictability. CRP protection is
        // policy-internal and must not hide frames from the contract.
        self.evictable.len()
    }

    fn export_state(&self) -> PolicyState {
        let mut state = PolicyState::new(self.name());
        for &page_id in self.frame_to_page.values() {
            let score = match self.history[&page_id].kth_recent_seq(self.k) {
                None => 0,
                Some(kth_seq) => kth_seq + 1, // finite distance = hotter
            };
            state.hot_pages.push((page_id, score));
        }
        state
    }

    fn import_state(&mut self, state: &PolicyState) {
        self.pending_page_scores.clear();
        for &(page_id, score) in &state.hot_pages {
            self.pending_page_scores.insert(page_id, score);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fid(index: usize) -> FrameId {
        FrameId::new(index)
    }
    fn pid(id: u32) -> PageId {
        PageId::new(id)
    }

    /// Access `frame/page` once and mark it evictable.
    fn touch(policy: &mut LruKCrpReplacer, frame: usize, page: u32) {
        policy.record_access(fid(frame), pid(page));
        policy.set_evictable(fid(frame), true);
    }

    // §2.1.1: a burst inside the CRP is ONE reference — the page never
    // earns a finite K-distance and evicts before a genuinely
    // twice-referenced page.
    #[test]
    fn crp_burst_does_not_build_history() {
        let mut policy = LruKCrpReplacer::new(2, 10, 8);
        // Page 100: 4 rapid touches (all within crp=10) = 1 reference.
        for _ in 0..4 {
            touch(&mut policy, 0, 100);
        }
        // Page 101: two touches separated by > crp accesses.
        touch(&mut policy, 1, 101);
        for spacer in 0..12 {
            touch(&mut policy, 2 + spacer, 200 + spacer as u32);
        }
        touch(&mut policy, 1, 101); // uncorrelated re-reference
                                    // Age everything past the CRP so protection is not a factor.
        for spacer in 0..12 {
            touch(&mut policy, 20 + spacer, 300 + spacer as u32);
        }

        // Burst page (+∞ distance, oldest) must evict before page 101
        // (finite distance).
        let mut evicted_100_before_101 = None;
        for _ in 0..30 {
            match policy.evict() {
                Some(f) if f == fid(0) => {
                    evicted_100_before_101 = Some(true);
                    break;
                }
                Some(f) if f == fid(1) => {
                    evicted_100_before_101 = Some(false);
                    break;
                }
                Some(_) => continue,
                None => break,
            }
        }
        assert_eq!(evicted_100_before_101, Some(true));
    }

    // §2.1.1 protection: a frame inside its CRP is not chosen while an
    // expired candidate exists.
    #[test]
    fn crp_protects_recent_frames() {
        let mut policy = LruKCrpReplacer::new(2, 5, 8);
        touch(&mut policy, 0, 100);
        // Age frame 0 past the CRP.
        for spacer in 0..7 {
            touch(&mut policy, 1 + spacer, 200 + spacer as u32);
        }
        // Frame 9 is brand new (inside CRP) when we evict.
        touch(&mut policy, 9, 999);
        let victim = policy.evict().unwrap();
        assert_ne!(victim, fid(9), "within-CRP frame must be protected");
    }

    // BPM-safety fallback: all evictable frames inside CRP still yield
    // a victim (never starve the pool).
    #[test]
    fn all_protected_still_evicts() {
        let mut policy = LruKCrpReplacer::new(2, 1_000, 8);
        touch(&mut policy, 0, 100);
        touch(&mut policy, 1, 101);
        assert!(policy.evict().is_some(), "protection must yield to safety");
    }

    // §2.1.2 RIP: an evicted page returning within the retained window
    // resumes its history — its earlier uncorrelated reference still
    // counts toward K.
    #[test]
    fn rip_resumes_history_across_eviction() {
        let mut policy = LruKCrpReplacer::new(2, 2, 8);
        touch(&mut policy, 0, 100);
        // Age + evict page 100 (history retained).
        for spacer in 0..4 {
            touch(&mut policy, 1 + spacer, 200 + spacer as u32);
        }
        loop {
            match policy.evict() {
                Some(f) if f == fid(0) => break,
                Some(_) => continue,
                None => panic!("frame 0 never evicted"),
            }
        }
        // Page 100 returns in a NEW frame: retained reference + this one
        // = finite K-distance (K=2).
        policy.record_access(fid(9), pid(100));
        assert_eq!(
            policy.history[&pid(100)].hist.len(),
            2,
            "RIP must resume history: prior reference + return = K"
        );
    }

    // RIP store is bounded FIFO.
    #[test]
    fn rip_store_trims_to_capacity() {
        let mut policy = LruKCrpReplacer::new(2, 0, 2);
        for i in 0..4 {
            touch(&mut policy, i, 100 + i as u32);
        }
        for _ in 0..4 {
            policy.evict().unwrap();
        }
        assert_eq!(policy.retained_hist.len(), 2, "RIP capped at 2");
        assert!(
            !policy.retained_hist.contains_key(&pid(100)),
            "oldest trimmed"
        );
        assert!(policy.retained_hist.contains_key(&pid(103)));
    }

    // crp = 0 degenerates to plain LRU-K ordering (+ RIP): the
    // canonical scan-resistance sequence from lru_k.rs still holds.
    #[test]
    fn crp_zero_matches_plain_lruk_ordering() {
        let mut policy = LruKCrpReplacer::new(2, 0, 8);
        // Hot page: two spaced references.
        touch(&mut policy, 0, 100);
        touch(&mut policy, 1, 101);
        policy.record_access(fid(0), pid(100));
        touch(&mut policy, 2, 102);

        // One-touch pages 101/102 (+∞) evict first, FIFO; hot page last.
        assert_eq!(policy.evict(), Some(fid(1)));
        assert_eq!(policy.evict(), Some(fid(2)));
        assert_eq!(policy.evict(), Some(fid(0)));
    }

    // Q-35 contract: stale evictability for an untracked frame is a
    // no-op.
    #[test]
    fn untracked_set_evictable_is_noop() {
        let mut policy = LruKCrpReplacer::new(2, 5, 8);
        policy.set_evictable(fid(7), true);
        assert_eq!(policy.size(), 0);
        assert_eq!(policy.evict(), None);
    }
}
