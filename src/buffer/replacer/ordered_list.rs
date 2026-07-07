//! `OrderedList` — LRU-ordered collection shared by replacer policies.
//!
//! Extracted from `arc.rs` when Clock2Q+ became the second consumer.
//! Front = LRU/oldest, back = MRU/newest; O(log n) mid-list removal.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

/// LRU-ordered list with O(log n) mid-list removal — sequence-numbered
/// `BTreeMap` (order) + `HashMap` (membership).
///
/// T16.5 tuning fix: the previous `VecDeque` representation made ARC's
/// cache-HIT path (`record_access` move-to-MRU) an O(pool_size)
/// `retain` scan inside the global replacer mutex — the top user-code
/// symbol in the TPC-C profile, with the other terminals parked on the
/// mutex behind it. Iteration order (front = LRU, back = MRU) and every
/// ARC semantic are unchanged; only the complexity class is.
/// NOTE (perf): frames are dense, so T1/T2 could go O(1) via a
/// slab-backed intrusive list; ghosts are sparse PageIds and could not.
/// One uniform O(log n) structure is the KISS choice until a profile
/// says the BTreeMap constants matter.
pub(crate) struct OrderedList<K> {
    /// Insertion order: ascending seq = LRU → MRU.
    by_seq: BTreeMap<u64, K>,
    /// Membership + reverse index for O(log n) removal.
    seq_of: HashMap<K, u64>,
    /// Monotonic per-list counter; u64 cannot realistically wrap.
    next_seq: u64,
}

impl<K: Copy + Eq + Hash> OrderedList<K> {
    pub(crate) fn new() -> Self {
        Self {
            by_seq: BTreeMap::new(),
            seq_of: HashMap::new(),
            next_seq: 0,
        }
    }

    /// Append at the MRU end. A key already present moves to MRU.
    pub(crate) fn push_back(&mut self, key: K) {
        if let Some(seq) = self.seq_of.remove(&key) {
            self.by_seq.remove(&seq);
        }
        self.by_seq.insert(self.next_seq, key);
        self.seq_of.insert(key, self.next_seq);
        self.next_seq += 1;
    }

    /// Remove `key` if present; true when it was.
    pub(crate) fn remove(&mut self, key: &K) -> bool {
        match self.seq_of.remove(key) {
            Some(seq) => {
                self.by_seq.remove(&seq);
                true
            }
            None => false,
        }
    }

    /// Remove and return the LRU entry.
    pub(crate) fn pop_front(&mut self) -> Option<K> {
        let (_, key) = self.by_seq.pop_first()?;
        self.seq_of.remove(&key);
        Some(key)
    }

    pub(crate) fn contains(&self, key: &K) -> bool {
        self.seq_of.contains_key(key)
    }

    pub(crate) fn len(&self) -> usize {
        self.seq_of.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.seq_of.is_empty()
    }

    /// LRU → MRU iteration.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &K> {
        self.by_seq.values()
    }

    /// The LRU entry, if any. Test-only observer.
    #[cfg(test)]
    pub(crate) fn front(&self) -> Option<&K> {
        self.by_seq.first_key_value().map(|(_, k)| k)
    }

    /// The MRU entry, if any. Test-only observer.
    #[cfg(test)]
    pub(crate) fn back(&self) -> Option<&K> {
        self.by_seq.last_key_value().map(|(_, k)| k)
    }

    /// Insertion sequence of `key`, if present. Monotonic per list —
    /// callers use seq arithmetic for O(1) age/position tests (the
    /// Clock2Q+ correlation window).
    pub(crate) fn seq(&self, key: &K) -> Option<u64> {
        self.seq_of.get(key).copied()
    }

    /// Oldest entry with seq >= `seq_from`, as `(seq, key)`.
    ///
    /// Cursor primitive for scans that mutate the list between steps
    /// (the Clock2Q+ Small eviction scan): re-querying from
    /// `last_seq + 1` is O(log n) and immune to removals — no snapshot
    /// allocation.
    pub(crate) fn first_at_or_after(&self, seq_from: u64) -> Option<(u64, K)> {
        self.by_seq
            .range(seq_from..)
            .next()
            .map(|(&seq, &key)| (seq, key))
    }

    /// The next sequence number to be assigned (= 1 + newest live seq
    /// when non-empty; gaps possible after mid-list removals).
    pub(crate) fn next_seq(&self) -> u64 {
        self.next_seq
    }
}
