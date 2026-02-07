//! Latch crabbing context for B+Tree operations.
//!
//! Holds write latches acquired during top-down traversal. The write_set
//! is ordered closest-to-root first: `[closest_to_root, ..., parent_of_leaf]`.
//! When a "safe" node is found (won't split/merge), all ancestors are released.

use crate::buffer::PageWriteGuard;

/// Maximum write set depth — matches MAX_TREE_HEIGHT.
const MAX_WRITE_SET_DEPTH: usize = 20;

/// Holds write latches accumulated during a top-down traversal.
///
/// Used by insert and delete to implement pessimistic latch crabbing:
/// write-latch each node on the way down, release ancestors when a
/// "safe" node is encountered.
pub struct Context<'a> {
    /// Write latch on the header page. Held until a safe node is found
    /// or until a new root is created.
    pub header_guard: Option<PageWriteGuard<'a>>,
    /// Write latches on internal nodes, ordered root-to-leaf.
    /// `[closest_to_root, ..., parent_of_leaf]`
    pub write_set: Vec<PageWriteGuard<'a>>,
}

impl<'a> Context<'a> {
    /// Create an empty context.
    pub fn new() -> Self {
        Self {
            header_guard: None,
            write_set: Vec::with_capacity(MAX_WRITE_SET_DEPTH),
        }
    }

    /// Release all ancestor latches (header + write_set).
    ///
    /// Called when a "safe" node is encountered — one that can absorb
    /// any structural change from below without propagating up.
    pub fn release_ancestors(&mut self) {
        self.header_guard.take();
        self.write_set.clear();
    }

    /// Push a write guard onto the write set.
    pub fn push(&mut self, guard: PageWriteGuard<'a>) {
        debug_assert!(
            self.write_set.len() < MAX_WRITE_SET_DEPTH,
            "Write set exceeded max tree height"
        );
        self.write_set.push(guard);
    }

    /// Pop the deepest ancestor (last element) from the write set.
    ///
    /// Used during split propagation / rebalancing to get the parent guard.
    pub fn pop(&mut self) -> Option<PageWriteGuard<'a>> {
        self.write_set.pop()
    }
}
