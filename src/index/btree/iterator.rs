//! B+Tree range scan iterator.
//!
//! Lazy, page-at-a-time buffered iterator over leaf entries.
//! Fetches one leaf page, yields its entries, then advances via
//! `next_page_id`. Does not hold page guards across `next()` calls —
//! each page is fetched, decoded into owned data, and released.

use std::ops::Bound;

use crate::buffer::BufferPoolManager;
use crate::common::{Error, PageId, Result};

use super::node::NodeType;
use super::page_layout::EncodedLeaf;
use crate::storage::page::PageHeader;

/// Safety bound on maximum pages visited during a single scan.
const MAX_SCAN_PAGES: usize = 10_000;

/// Page-at-a-time buffered iterator over B+Tree leaf entries.
///
/// Yields `Result<(Vec<u8>, Vec<u8>)>` — key-value pairs in sorted order.
/// Filters by the provided range bounds, skipping tombstoned entries.
pub struct BTreeScanIterator<'a> {
    bpm: &'a BufferPoolManager,
    /// Page ID of the next leaf to load. INVALID = no more pages.
    current_page_id: PageId,
    /// Buffered entries from the current leaf, reversed for efficient pop().
    buffer: Vec<(Vec<u8>, Vec<u8>)>,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    /// Start bound filtering applies only to the first page loaded.
    first_page: bool,
    /// Number of pages visited so far.
    pages_visited: usize,
    /// Terminal state: error occurred or scan exhausted.
    done: bool,
}

impl<'a> BTreeScanIterator<'a> {
    /// Create a new scan iterator.
    ///
    /// `start_page_id` is the first leaf page to read (found by the caller
    /// via `find_leaf()` or `get_first_leaf()`). Pass `PageId::INVALID` for
    /// an empty tree.
    pub fn new(
        bpm: &'a BufferPoolManager,
        start_page_id: PageId,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> Self {
        let done = start_page_id == PageId::INVALID;
        Self {
            bpm,
            current_page_id: start_page_id,
            buffer: Vec::new(),
            start_bound,
            end_bound,
            first_page: true,
            pages_visited: 0,
            done,
        }
    }

    /// Load the next leaf page into the buffer.
    ///
    /// Fetches `current_page_id`, decodes the leaf, filters entries by
    /// bounds, fills `buffer` (reversed), and advances `current_page_id`.
    /// Returns error on corruption or BPM failure.
    fn load_next_page(&mut self) -> Result<()> {
        debug_assert!(self.current_page_id != PageId::INVALID);
        debug_assert!(!self.done);

        // Safety bound: prevent infinite scans.
        if self.pages_visited >= MAX_SCAN_PAGES {
            self.done = true;
            return Err(Error::StorageCorrupted(format!(
                "Scan exceeded {} pages",
                MAX_SCAN_PAGES
            )));
        }

        let guard = self.bpm.fetch_page_read(self.current_page_id)?;
        let data = guard.as_slice();

        // Validate node type — return error, not assert (could be BPM issue).
        let node_type = NodeType::from_u8(data[PageHeader::SIZE]).ok_or_else(|| {
            Error::StorageCorrupted(format!(
                "Invalid node type at page {}",
                self.current_page_id.0
            ))
        })?;

        if node_type != NodeType::Leaf {
            return Err(Error::StorageCorrupted(format!(
                "Expected leaf at page {}, got internal",
                self.current_page_id.0
            )));
        }

        // Walk the encoded leaf in place: no per-entry decode allocation, just
        // the one owned copy of each surviving entry pushed into the buffer.
        // The `leaf` borrow points into the page guard, so it is scoped to this
        // block — which yields the owned results — and ends before the guard is
        // dropped below.
        let (mut entries, next_page_id, hit_end_bound) = {
            let leaf = EncodedLeaf::parse(data);
            let next_page_id = leaf.next_page_id;

            // Validate before trusting the page. A page that still passes the
            // checksum (a writer bug) could carry an out-of-range tombstone
            // index or a bogus sibling pointer; without these checks we'd skip
            // the wrong entry or follow next_page_id off the end of the BPM.
            // (Key and value counts can't disagree when walked in lockstep, so
            // the decode path's keys==values check is now structural.)
            if !leaf.tombstones_in_range() {
                self.done = true;
                return Err(Error::StorageCorrupted(format!(
                    "Leaf {} has an out-of-range tombstone index",
                    self.current_page_id.0
                )));
            }
            let page_count = self.bpm.disk_page_count();
            if next_page_id != PageId::INVALID && next_page_id.0 >= page_count {
                self.done = true;
                return Err(Error::StorageCorrupted(format!(
                    "Leaf {} next_page_id={} out of range (page_count={})",
                    self.current_page_id.0, next_page_id.0, page_count
                )));
            }

            // Collect live entries, filtering by bounds.
            let mut entries = Vec::new();
            let mut hit_end_bound = false;

            for (key, value) in leaf.live_entries() {
                // Start bound: skip entries before start key (first page only).
                if self.first_page {
                    let skip = match &self.start_bound {
                        Bound::Included(start) => key < start.as_slice(),
                        Bound::Excluded(start) => key <= start.as_slice(),
                        Bound::Unbounded => false,
                    };
                    if skip {
                        continue;
                    }
                }

                // End bound: stop when entries pass end key.
                let past_end = match &self.end_bound {
                    Bound::Included(end) => key > end.as_slice(),
                    Bound::Excluded(end) => key >= end.as_slice(),
                    Bound::Unbounded => false,
                };
                if past_end {
                    hit_end_bound = true;
                    break;
                }

                entries.push((key.to_vec(), value.to_vec()));
            }

            (entries, next_page_id, hit_end_bound)
        };

        // Drop the page guard before updating state (the `leaf` borrow ended
        // with the block above).
        drop(guard);

        self.first_page = false;
        self.pages_visited += 1;

        // If we hit the end bound, no more pages needed.
        if hit_end_bound {
            self.current_page_id = PageId::INVALID;
        } else {
            self.current_page_id = next_page_id;
        }

        // Reverse so pop() yields entries in sorted order.
        entries.reverse();
        self.buffer = entries;

        Ok(())
    }
}

impl<'a> Iterator for BTreeScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Step 1: yield from buffer.
            if let Some(entry) = self.buffer.pop() {
                return Some(Ok(entry));
            }

            // Step 2: check terminal state.
            if self.done {
                return None;
            }

            // Step 3: check if exhausted (no more pages).
            if self.current_page_id == PageId::INVALID {
                self.done = true;
                return None;
            }

            // Step 4: load next page.
            match self.load_next_page() {
                Ok(()) => {
                    // Loop back to step 1 (handles empty pages — all tombstoned).
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}
