//! K-way merge iterator over sorted sources.
//!
//! Sources are ordered by priority (index 0 = highest priority / newest).
//! When multiple sources have the same key, the highest-priority source wins
//! and duplicates from lower-priority sources are skipped. Tombstones are
//! included in the output — the caller decides whether to filter them.

use crate::common::error::Result;
use super::Entry;

/// K-way merge iterator. Sources must each yield entries in sorted key order.
///
/// Priority: source at index 0 has highest priority (newest data). When two
/// sources have the same key, the entry from the higher-priority source is
/// emitted and the lower-priority duplicate is discarded.
pub struct MergeIterator {
    /// Each source is a Vec of `(key, Option<value>)` with a current index.
    sources: Vec<Source>,
}

struct Source {
    entries: Vec<Entry>,
    pos: usize,
}

impl Source {
    fn peek(&self) -> Option<&Entry> {
        self.entries.get(self.pos)
    }

    fn advance(&mut self) {
        self.pos += 1;
    }
}

impl MergeIterator {
    /// Create a merge iterator from a list of sorted entry vectors.
    ///
    /// `sources[0]` has highest priority (newest data).
    pub fn new(sources: Vec<Vec<Entry>>) -> Self {
        let sources = sources
            .into_iter()
            .map(|entries| Source { entries, pos: 0 })
            .collect();
        Self { sources }
    }

    /// Collect all merged entries into a Vec.
    ///
    /// Deduplicates keys across sources, keeping the entry from the
    /// highest-priority source. Tombstones are included in the output.
    pub fn collect_all(mut self) -> Result<Vec<Entry>> {
        let mut result = Vec::new();

        loop {
            // Find the source with the smallest current key.
            // On tie, prefer lower index (higher priority).
            let mut min_key: Option<&[u8]> = None;
            let mut min_idx: Option<usize> = None;

            for (i, source) in self.sources.iter().enumerate() {
                if let Some((key, _)) = source.peek() {
                    match min_key {
                        None => {
                            min_key = Some(key.as_slice());
                            min_idx = Some(i);
                        }
                        Some(current_min) => {
                            if key.as_slice() < current_min {
                                min_key = Some(key.as_slice());
                                min_idx = Some(i);
                            }
                            // Equal keys: lower index wins (already set).
                        }
                    }
                }
            }

            let Some(winner_idx) = min_idx else {
                break; // All sources exhausted.
            };

            // Take the winning entry.
            let winner_key = min_key.unwrap().to_vec();
            let entry = self.sources[winner_idx].peek().unwrap().clone();
            self.sources[winner_idx].advance();

            // Skip duplicates of this key from all other sources.
            for (i, source) in self.sources.iter_mut().enumerate() {
                if i == winner_idx {
                    continue;
                }
                while let Some((k, _)) = source.peek() {
                    if k.as_slice() == winner_key.as_slice() {
                        source.advance();
                    } else {
                        break;
                    }
                }
            }

            result.push(entry);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sources() {
        let iter = MergeIterator::new(vec![]);
        let result = iter.collect_all().unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn single_source() {
        let source = vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), Some(b"2".to_vec())),
        ];
        let iter = MergeIterator::new(vec![source]);
        let result = iter.collect_all().unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, b"a");
        assert_eq!(result[1].0, b"b");
    }

    #[test]
    fn merge_two_disjoint() {
        let s1 = vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"c".to_vec(), Some(b"3".to_vec())),
        ];
        let s2 = vec![
            (b"b".to_vec(), Some(b"2".to_vec())),
            (b"d".to_vec(), Some(b"4".to_vec())),
        ];
        let iter = MergeIterator::new(vec![s1, s2]);
        let result = iter.collect_all().unwrap();
        let keys: Vec<&[u8]> = result.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"a".as_slice(), b"b", b"c", b"d"]);
    }

    #[test]
    fn dedup_higher_priority_wins() {
        // Source 0 (newer) has key "b" = "new".
        // Source 1 (older) has key "b" = "old".
        let s0 = vec![(b"b".to_vec(), Some(b"new".to_vec()))];
        let s1 = vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), Some(b"old".to_vec())),
            (b"c".to_vec(), Some(b"3".to_vec())),
        ];
        let iter = MergeIterator::new(vec![s0, s1]);
        let result = iter.collect_all().unwrap();

        assert_eq!(result.len(), 3);
        // Key "b" should have the value from s0 (newer).
        assert_eq!(result[1].0, b"b");
        assert_eq!(result[1].1.as_ref().unwrap(), b"new");
    }

    #[test]
    fn tombstone_preserved() {
        let s0 = vec![(b"a".to_vec(), None)]; // Tombstone.
        let s1 = vec![(b"a".to_vec(), Some(b"alive".to_vec()))];
        let iter = MergeIterator::new(vec![s0, s1]);
        let result = iter.collect_all().unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, b"a");
        assert!(result[0].1.is_none(), "tombstone should win");
    }

    #[test]
    fn three_sources() {
        let s0 = vec![(b"c".to_vec(), Some(b"C0".to_vec()))];
        let s1 = vec![
            (b"a".to_vec(), Some(b"A1".to_vec())),
            (b"c".to_vec(), Some(b"C1".to_vec())),
        ];
        let s2 = vec![
            (b"b".to_vec(), Some(b"B2".to_vec())),
            (b"c".to_vec(), Some(b"C2".to_vec())),
            (b"d".to_vec(), Some(b"D2".to_vec())),
        ];
        let iter = MergeIterator::new(vec![s0, s1, s2]);
        let result = iter.collect_all().unwrap();

        assert_eq!(result.len(), 4); // a, b, c, d
        assert_eq!(result[2].1.as_ref().unwrap(), b"C0"); // c from s0 wins.
    }
}
