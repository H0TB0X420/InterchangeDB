//! B+Tree implementation.
//!
//! Core tree operations: get, insert, delete, scan.

use std::ops::{Bound, RangeBounds};

use crate::buffer::{BufferPoolManager, PageReadGuard, PageWriteGuard};
use crate::common::{PageId, Result, Error};

use super::context::Context;

use super::iterator::BTreeScanIterator;

use super::node::{NodeType, LeafNode, InternalNode};
use super::page_layout::{
    decode_leaf_node, decode_internal_node,
    encode_leaf_node, encode_internal_node,
    calculate_leaf_max_size, calculate_internal_max_size,
};
use super::BTreeHeaderPage;

// =============================================================================
// Constants
// =============================================================================

/// Maximum tree height. 20 supports petabytes with realistic fanout, and
/// handles pathological small-size cases (leaf_max=2, internal_max=3) up
/// to ~1M keys where height ≈ log_2(n).
const MAX_TREE_HEIGHT: usize = 20;

/// Default average key size for max_size calculation.
const DEFAULT_AVG_KEY_SIZE: usize = 32;

/// Default average value size for max_size calculation.
const DEFAULT_AVG_VALUE_SIZE: usize = 64;

// =============================================================================
// Types
// =============================================================================

/// Result of splitting a node.
struct SplitResult {
    /// Page ID of the new (right) node.
    new_page_id: PageId,
    /// Separator key to insert into parent.
    separator_key: Vec<u8>,
}

// =============================================================================
// BTree
// =============================================================================

/// B+Tree index structure.
///
/// Provides key-value storage with O(log n) operations.
pub struct BTree<'a> {
    bpm: &'a BufferPoolManager,
    header_page_id: PageId,
    leaf_max_size: u16,
    internal_max_size: u16,
    /// Maximum tombstones per leaf. 0 = no tombstones (physical remove on delete).
    max_tombstones: usize,
}

impl<'a> BTree<'a> {
    /// Create a new B+Tree handle with default max sizes.
    ///
    /// The header_page_id should point to an existing header page.
    pub fn new(bpm: &'a BufferPoolManager, header_page_id: PageId) -> Self {
        Self {
            bpm,
            header_page_id,
            leaf_max_size: calculate_leaf_max_size(DEFAULT_AVG_KEY_SIZE, DEFAULT_AVG_VALUE_SIZE),
            internal_max_size: calculate_internal_max_size(DEFAULT_AVG_KEY_SIZE),
            max_tombstones: 0,
        }
    }

    /// Create a new B+Tree handle with explicit max sizes.
    ///
    /// `max_tombstones = 0` means no tombstones (physical remove on delete).
    /// Used for testing with small node sizes to force splits and merges.
    pub fn with_sizes(
        bpm: &'a BufferPoolManager,
        header_page_id: PageId,
        leaf_max_size: u16,
        internal_max_size: u16,
    ) -> Self {
        debug_assert!(leaf_max_size >= 2, "leaf_max_size must be at least 2");
        debug_assert!(internal_max_size >= 3, "internal_max_size must be at least 3");
        Self { bpm, header_page_id, leaf_max_size, internal_max_size, max_tombstones: 0 }
    }

    /// Create a new B+Tree handle with explicit max sizes and tombstone limit.
    pub fn with_tombstones(
        bpm: &'a BufferPoolManager,
        header_page_id: PageId,
        leaf_max_size: u16,
        internal_max_size: u16,
        max_tombstones: usize,
    ) -> Self {
        debug_assert!(leaf_max_size >= 2, "leaf_max_size must be at least 2");
        debug_assert!(internal_max_size >= 3, "internal_max_size must be at least 3");
        Self { bpm, header_page_id, leaf_max_size, internal_max_size, max_tombstones }
    }

    /// Get the header page ID.
    pub fn header_page_id(&self) -> PageId {
        self.header_page_id
    }

    // =========================================================================
    // Search Operations
    // =========================================================================

    /// Look up a value by key.
    ///
    /// Returns `Some(value)` if key exists and is not tombstoned.
    /// Returns `None` if key doesn't exist or is tombstoned.
    /// Uses latch coupling: holds parent read latch while acquiring child.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = match self.find_leaf_read(key)? {
            Some(g) => g,
            None => return Ok(None),
        };

        let leaf = decode_leaf_node(guard.as_slice());

        match leaf.lookup(key) {
            Some(index) => Ok(Some(leaf.value_at(index).to_vec())),
            None => Ok(None),
        }
    }

    /// Check if a key exists (not tombstoned).
    /// Uses latch coupling: holds parent read latch while acquiring child.
    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        let guard = match self.find_leaf_read(key)? {
            Some(g) => g,
            None => return Ok(false),
        };

        let leaf = decode_leaf_node(guard.as_slice());

        Ok(leaf.lookup(key).is_some())
    }

    /// Check if the tree is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.get_root_page_id()?.is_none())
    }

    // =========================================================================
    // Insert Operations
    // =========================================================================

    /// Insert a key-value pair using pessimistic latch crabbing.
    ///
    /// Returns `true` if inserted, `false` if key already exists.
    /// Write-latches are held top-down and released early when a "safe" node
    /// (one that won't split) is encountered.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        let mut ctx = Context::new();

        // Write-latch header to read root and protect against concurrent root changes.
        let header_guard = self.bpm.fetch_page_write(self.header_page_id)?;
        let header = BTreeHeaderPage::decode(header_guard.as_slice());

        // Empty tree: create first leaf as root.
        if header.is_empty() {
            return self.insert_into_empty_tree(key, value, header_guard);
        }

        let root_page_id = header.root_page_id;
        ctx.header_guard = Some(header_guard);

        // Traverse from root to leaf with write latches.
        let mut current_guard = self.bpm.fetch_page_write(root_page_id)?;

        for _depth in 0..MAX_TREE_HEIGHT {
            let data = current_guard.as_slice();
            let node_type = NodeType::from_u8(data[0])
                .ok_or_else(|| Error::StorageCorrupted(
                    format!("Invalid node type at page {}", current_guard.page_id().0)
                ))?;

            match node_type {
                NodeType::Leaf => {
                    return self.insert_into_leaf(key, value, current_guard, &mut ctx);
                }
                NodeType::Internal => {
                    let internal = decode_internal_node(data);

                    // If this node is safe for insert, release all ancestors.
                    if internal.is_safe_for_insert() {
                        ctx.release_ancestors();
                    }

                    let child_index = internal.find_child_index(key);
                    let child_id = internal.child_at(child_index);

                    // Latch coupling: acquire child write latch before releasing parent.
                    let child_guard = self.bpm.fetch_page_write(child_id)?;
                    ctx.push(current_guard);
                    current_guard = child_guard;
                }
            }
        }

        Err(Error::StorageCorrupted(
            format!("Tree exceeds max height {}", MAX_TREE_HEIGHT)
        ))
    }

    /// Insert into an empty tree — creates first leaf as root.
    ///
    /// Takes the already-held header write guard.
    fn insert_into_empty_tree(
        &self,
        key: &[u8],
        value: &[u8],
        mut header_guard: PageWriteGuard<'a>,
    ) -> Result<bool> {
        // Create new leaf page.
        let leaf_page = self.bpm.new_page()?;
        let leaf_page_id = leaf_page.page_id();

        {
            let mut guard = leaf_page;
            let mut leaf = LeafNode::new(self.leaf_max_size);
            leaf.insert(key.to_vec(), value.to_vec());

            debug_assert_eq!(leaf.physical_size(), 1);
            encode_leaf_node(&leaf, guard.as_mut_slice());
        }

        // Update header to point to new root (guard already held).
        let mut header = BTreeHeaderPage::decode(header_guard.as_slice());
        debug_assert!(header.is_empty());
        header.root_page_id = leaf_page_id;
        header.encode(header_guard.as_mut_slice());

        Ok(true)
    }

    /// Insert a key-value pair into the leaf, handling splits if needed.
    ///
    /// Takes the already-held leaf write guard and the traversal context
    /// containing ancestor write guards.
    fn insert_into_leaf(
        &self,
        key: &[u8],
        value: &[u8],
        mut leaf_guard: PageWriteGuard<'a>,
        ctx: &mut Context<'a>,
    ) -> Result<bool> {
        let leaf_page_id = leaf_guard.page_id();
        let mut leaf = decode_leaf_node(leaf_guard.as_slice());

        // Check for duplicate.
        if leaf.lookup(key).is_some() {
            return Ok(false);
        }

        leaf.insert(key.to_vec(), value.to_vec());

        // No split needed — release ancestors and write back.
        if !leaf.needs_split() {
            ctx.release_ancestors();
            encode_leaf_node(&leaf, leaf_guard.as_mut_slice());
            return Ok(true);
        }

        // Split needed — split the leaf using the held guard.
        let split = self.split_leaf_ctx(&mut leaf, leaf_page_id, &mut leaf_guard)?;

        // Propagate split up through held ancestor guards.
        self.propagate_split_up_ctx(ctx, split)
    }

    /// Split a leaf node using a held write guard.
    ///
    /// Modifies `leaf` in place (keeps left half), creates a new right leaf,
    /// and writes both. Returns split info for parent insertion.
    fn split_leaf_ctx(
        &self,
        leaf: &mut LeafNode,
        leaf_page_id: PageId,
        leaf_guard: &mut PageWriteGuard<'a>,
    ) -> Result<SplitResult> {
        let mid = leaf.physical_size() / 2;

        debug_assert!(mid > 0);
        debug_assert!(mid < leaf.physical_size());

        // Create new right leaf.
        let new_page = self.bpm.new_page()?;
        let new_page_id = new_page.page_id();

        let mut right_leaf = LeafNode::new(leaf.header.max_size);

        // Move upper half to right leaf.
        right_leaf.keys = leaf.keys.split_off(mid);
        right_leaf.values = leaf.values.split_off(mid);
        right_leaf.header.size = right_leaf.keys.len() as u16;
        leaf.header.size = leaf.keys.len() as u16;

        // Partition tombstones.
        let mut left_tombstones = Vec::new();
        let mut right_tombstones = Vec::new();
        for &tomb in &leaf.tombstones {
            if (tomb as usize) < mid {
                left_tombstones.push(tomb);
            } else {
                right_tombstones.push(tomb - mid as u16);
            }
        }
        leaf.tombstones = left_tombstones;
        right_leaf.tombstones = right_tombstones;

        // Update sibling pointers.
        let old_next = leaf.next_page_id;
        leaf.next_page_id = new_page_id;
        right_leaf.prev_page_id = leaf_page_id;
        right_leaf.next_page_id = old_next;

        // Update old next's prev pointer if it exists.
        if old_next != PageId::INVALID {
            let mut next_guard = self.bpm.fetch_page_write(old_next)?;
            let mut next_leaf = decode_leaf_node(next_guard.as_slice());
            next_leaf.prev_page_id = new_page_id;
            encode_leaf_node(&next_leaf, next_guard.as_mut_slice());
        }

        // Separator is first key of right leaf.
        let separator = right_leaf.keys[0].clone();

        debug_assert!(!leaf.keys.is_empty());
        debug_assert!(leaf.keys.last().unwrap().as_slice() < separator.as_slice());

        // Write both leaves.
        encode_leaf_node(leaf, leaf_guard.as_mut_slice());
        {
            let mut new_guard = new_page;
            encode_leaf_node(&right_leaf, new_guard.as_mut_slice());
        }

        Ok(SplitResult {
            new_page_id,
            separator_key: separator,
        })
    }

    /// Split an internal node using a held write guard.
    ///
    /// Modifies `internal` in place (keeps left half), creates a new right
    /// internal, writes both. Returns split info for parent insertion.
    fn split_internal_ctx(
        &self,
        internal: &mut InternalNode,
        parent_guard: &mut PageWriteGuard<'a>,
    ) -> Result<SplitResult> {
        let mid = internal.keys.len() / 2;

        debug_assert!(mid > 0);
        debug_assert!(mid < internal.keys.len() - 1);

        let separator = internal.keys[mid].clone();

        // Create new right internal.
        let new_page = self.bpm.new_page()?;
        let new_page_id = new_page.page_id();

        let mut right_internal = InternalNode::new(internal.header.max_size);

        // Right gets keys[mid..] and children[mid..].
        right_internal.keys = internal.keys.split_off(mid);
        right_internal.children = internal.children.split_off(mid);
        right_internal.header.size = right_internal.keys.len() as u16;
        internal.header.size = internal.keys.len() as u16;

        debug_assert!(!internal.children.is_empty());
        debug_assert!(!right_internal.children.is_empty());
        debug_assert_eq!(internal.keys.len(), internal.children.len());
        debug_assert_eq!(right_internal.keys.len(), right_internal.children.len());

        // Write both.
        encode_internal_node(internal, parent_guard.as_mut_slice());
        {
            let mut new_guard = new_page;
            encode_internal_node(&right_internal, new_guard.as_mut_slice());
        }

        Ok(SplitResult {
            new_page_id,
            separator_key: separator,
        })
    }

    /// Propagate a split up through held ancestor guards.
    ///
    /// Pops parent guards from the context write_set, inserts the separator,
    /// and continues splitting upward if needed.
    fn propagate_split_up_ctx(
        &self,
        ctx: &mut Context<'a>,
        mut split: SplitResult,
    ) -> Result<bool> {
        loop {
            match ctx.pop() {
                Some(mut parent_guard) => {
                    // Insert separator into parent using held guard.
                    let mut internal = decode_internal_node(parent_guard.as_slice());
                    let old_root_id = parent_guard.page_id();
                    internal.insert_separator(split.separator_key, split.new_page_id);

                    let needs_split = internal.keys.len() as u16 > internal.header.max_size;

                    if !needs_split {
                        // Parent absorbed the separator. Done.
                        encode_internal_node(&internal, parent_guard.as_mut_slice());
                        return Ok(true);
                    }

                    // Parent needs split too.
                    split = self.split_internal_ctx(&mut internal, &mut parent_guard)?;

                    // If write_set is now empty and header_guard is held,
                    // this was the root — need new root.
                    if ctx.write_set.is_empty() {
                        return self.create_new_root_ctx(ctx, old_root_id, split);
                    }
                }
                None => {
                    // No more parents in write_set. If header_guard is held,
                    // the split reached the root level.
                    let header_guard = ctx.header_guard.take()
                        .ok_or_else(|| Error::StorageCorrupted(
                            "Split propagation lost header guard".into()
                        ))?;

                    // We need the old root ID from the header.
                    let header = BTreeHeaderPage::decode(header_guard.as_slice());
                    let old_root_id = header.root_page_id;
                    ctx.header_guard = Some(header_guard);

                    return self.create_new_root_ctx(ctx, old_root_id, split);
                }
            }
        }
    }

    /// Create a new root after the old root split.
    ///
    /// Uses the held header guard from the context.
    fn create_new_root_ctx(
        &self,
        ctx: &mut Context<'a>,
        old_root_id: PageId,
        split: SplitResult,
    ) -> Result<bool> {
        let new_root_page = self.bpm.new_page()?;
        let new_root_id = new_root_page.page_id();

        {
            let mut guard = new_root_page;
            let mut new_root = InternalNode::new(self.internal_max_size);
            new_root.init_with_child(old_root_id);
            new_root.insert_separator(split.separator_key, split.new_page_id);

            debug_assert_eq!(new_root.children.len(), 2);
            encode_internal_node(&new_root, guard.as_mut_slice());
        }

        // Update header using held guard.
        let mut header_guard = ctx.header_guard.take()
            .ok_or_else(|| Error::StorageCorrupted(
                "create_new_root_ctx: no header guard".into()
            ))?;
        let mut header = BTreeHeaderPage::decode(header_guard.as_slice());

        debug_assert_eq!(header.root_page_id, old_root_id);
        header.root_page_id = new_root_id;
        header.encode(header_guard.as_mut_slice());

        Ok(true)
    }

    // =========================================================================
    // Delete Operations
    // =========================================================================

    /// Delete a key using pessimistic latch crabbing.
    ///
    /// Returns `true` if key was found and deleted, `false` if not found.
    /// Write-latches are held top-down and released early when a "safe" node
    /// (one that won't underflow) is encountered.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let mut ctx = Context::new();

        // Write-latch header to read root and protect against concurrent root changes.
        let header_guard = self.bpm.fetch_page_write(self.header_page_id)?;
        let header = BTreeHeaderPage::decode(header_guard.as_slice());

        if header.is_empty() {
            return Ok(false);
        }

        let root_page_id = header.root_page_id;
        ctx.header_guard = Some(header_guard);

        // Traverse from root to leaf with write latches.
        let mut current_guard = self.bpm.fetch_page_write(root_page_id)?;

        for _depth in 0..MAX_TREE_HEIGHT {
            let data = current_guard.as_slice();
            let node_type = NodeType::from_u8(data[0])
                .ok_or_else(|| Error::StorageCorrupted(
                    format!("Invalid node type at page {}", current_guard.page_id().0)
                ))?;

            match node_type {
                NodeType::Leaf => {
                    return self.delete_from_leaf(key, current_guard, &mut ctx);
                }
                NodeType::Internal => {
                    let internal = decode_internal_node(data);

                    // If this node is safe for delete, release all ancestors.
                    if internal.is_safe_for_delete() && internal.children.len() > 1 {
                        ctx.release_ancestors();
                    }

                    let child_index = internal.find_child_index(key);
                    let child_id = internal.child_at(child_index);

                    let child_guard = self.bpm.fetch_page_write(child_id)?;
                    ctx.push(current_guard);
                    current_guard = child_guard;
                }
            }
        }

        Err(Error::StorageCorrupted(
            format!("Tree exceeds max height {}", MAX_TREE_HEIGHT)
        ))
    }

    /// Delete a key from the leaf, handling underflow and rebalancing.
    fn delete_from_leaf(
        &self,
        key: &[u8],
        mut leaf_guard: PageWriteGuard<'a>,
        ctx: &mut Context<'a>,
    ) -> Result<bool> {
        let mut leaf = decode_leaf_node(leaf_guard.as_slice());

        let deleted = leaf.delete(key, self.max_tombstones);
        if !deleted {
            return Ok(false);
        }

        let underflowed = leaf.has_underflowed();
        encode_leaf_node(&leaf, leaf_guard.as_mut_slice());

        // Root leaf (write_set empty) is allowed to underflow.
        if ctx.write_set.is_empty() {
            // Check if root leaf is now physically empty.
            if leaf.physical_size() == 0 {
                if let Some(mut header_guard) = ctx.header_guard.take() {
                    let mut header = BTreeHeaderPage::decode(header_guard.as_slice());
                    header.root_page_id = PageId::INVALID;
                    header.encode(header_guard.as_mut_slice());
                }
            }
            return Ok(true);
        }

        if !underflowed {
            ctx.release_ancestors();
            return Ok(true);
        }

        // Underflow — need to rebalance. Drop the leaf guard first since
        // rebalancing acquires fresh guards on sibling pages.
        let leaf_page_id = leaf_guard.page_id();
        drop(leaf_guard);

        self.rebalance_leaf_ctx(leaf_page_id, ctx)?;

        // Release any remaining ancestor guards from write_set.
        // These are no longer needed after rebalancing is complete, and
        // holding them would violate top-down latch ordering if we tried
        // to acquire any new locks.
        ctx.release_ancestors();

        Ok(true)
    }

    // =========================================================================
    // Leaf Rebalancing
    // =========================================================================

    /// Rebalance an underflowed leaf. Pops parent from context write_set.
    ///
    /// Sibling guards are acquired fresh from BPM — safe because the parent
    /// write latch prevents concurrent sibling access.
    fn rebalance_leaf_ctx(
        &self,
        leaf_page_id: PageId,
        ctx: &mut Context<'a>,
    ) -> Result<()> {
        let mut parent_guard = ctx.pop()
            .ok_or_else(|| Error::StorageCorrupted(
                "rebalance_leaf_ctx: no parent in write_set".into()
            ))?;

        let parent = decode_internal_node(parent_guard.as_slice());
        let child_index = parent.find_child_position(leaf_page_id)
            .ok_or_else(|| Error::StorageCorrupted(
                "Leaf not found in parent".into()
            ))?;

        // Try redistribute from left sibling.
        if child_index > 0 {
            let left_page_id = parent.child_at(child_index - 1);
            let left_leaf = self.read_leaf(left_page_id)?;
            if left_leaf.is_safe_for_delete() {
                return self.redistribute_leaf_from_left_ctx(
                    &mut parent_guard, child_index, left_page_id, leaf_page_id,
                );
            }
        }

        // Try redistribute from right sibling.
        if child_index < parent.children.len() - 1 {
            let right_page_id = parent.child_at(child_index + 1);
            let right_leaf = self.read_leaf(right_page_id)?;
            if right_leaf.is_safe_for_delete() {
                return self.redistribute_leaf_from_right_ctx(
                    &mut parent_guard, child_index, leaf_page_id, right_page_id,
                );
            }
        }

        let has_left = child_index > 0;
        let has_right = child_index < parent.children.len() - 1;

        // No siblings (parent has single child) — can't merge at leaf level.
        if !has_left && !has_right {
            let leaf_empty = {
                let mut guard = self.bpm.fetch_page_write(leaf_page_id)?;
                let mut leaf = decode_leaf_node(guard.as_slice());
                leaf.compact();
                let empty = leaf.physical_size() == 0;
                encode_leaf_node(&leaf, guard.as_mut_slice());
                empty
            };

            if leaf_empty {
                return self.remove_empty_subtree_ctx(
                    leaf_page_id, parent_guard, ctx,
                );
            }

            // Leaf has entries but no siblings. Rebalance parent.
            return self.rebalance_internal_ctx(parent_guard, ctx);
        }

        // Merge with a sibling. Parent guard is used to update parent.
        if has_left {
            let left_page_id = parent.child_at(child_index - 1);
            self.merge_leaves_ctx(
                &mut parent_guard, child_index, left_page_id, leaf_page_id,
            )?;
        } else {
            let right_page_id = parent.child_at(child_index + 1);
            self.merge_leaves_ctx(
                &mut parent_guard, child_index + 1, leaf_page_id, right_page_id,
            )?;
        }

        // Check if parent needs rebalancing.
        self.rebalance_internal_ctx(parent_guard, ctx)
    }

    /// Steal the last physical entry from the left sibling. Uses held parent guard.
    fn redistribute_leaf_from_left_ctx(
        &self,
        parent_guard: &mut PageWriteGuard<'a>,
        child_index: usize,
        left_page_id: PageId,
        leaf_page_id: PageId,
    ) -> Result<()> {
        let (stolen_key, stolen_value, was_tombstoned) = {
            let mut guard = self.bpm.fetch_page_write(left_page_id)?;
            let mut left = decode_leaf_node(guard.as_slice());

            debug_assert!(!left.keys.is_empty());
            let last = left.keys.len() - 1;
            let was_tomb = left.is_tombstone(last);
            let key = left.keys.remove(last);
            let value = left.values.remove(last);
            if was_tomb {
                left.tombstones.retain(|&t| t as usize != last);
            }
            left.header.size = left.keys.len() as u16;

            encode_leaf_node(&left, guard.as_mut_slice());
            (key, value, was_tomb)
        };

        let new_separator = {
            let mut guard = self.bpm.fetch_page_write(leaf_page_id)?;
            let mut leaf = decode_leaf_node(guard.as_slice());

            for tomb in &mut leaf.tombstones {
                *tomb += 1;
            }
            leaf.keys.insert(0, stolen_key);
            leaf.values.insert(0, stolen_value);
            if was_tombstoned {
                leaf.tombstones.push(0);
            }
            leaf.header.size = leaf.keys.len() as u16;

            while leaf.tombstones.len() > self.max_tombstones {
                leaf.process_oldest_tombstone();
            }

            let separator = leaf.keys[0].clone();
            encode_leaf_node(&leaf, guard.as_mut_slice());
            separator
        };

        // Update parent separator using held guard.
        let mut parent = decode_internal_node(parent_guard.as_slice());
        parent.set_key_at(child_index, new_separator);
        encode_internal_node(&parent, parent_guard.as_mut_slice());

        Ok(())
    }

    /// Steal the first physical entry from the right sibling. Uses held parent guard.
    fn redistribute_leaf_from_right_ctx(
        &self,
        parent_guard: &mut PageWriteGuard<'a>,
        child_index: usize,
        leaf_page_id: PageId,
        right_page_id: PageId,
    ) -> Result<()> {
        let (stolen_key, stolen_value, was_tombstoned, new_separator) = {
            let mut guard = self.bpm.fetch_page_write(right_page_id)?;
            let mut right = decode_leaf_node(guard.as_slice());

            debug_assert!(!right.keys.is_empty());
            let was_tomb = right.is_tombstone(0);
            let key = right.keys.remove(0);
            let value = right.values.remove(0);
            let mut new_tombs = Vec::new();
            for &t in &right.tombstones {
                if (t as usize) > 0 {
                    new_tombs.push(t - 1);
                }
            }
            right.tombstones = new_tombs;
            right.header.size = right.keys.len() as u16;

            debug_assert!(!right.keys.is_empty());
            let separator = right.keys[0].clone();
            encode_leaf_node(&right, guard.as_mut_slice());
            (key, value, was_tomb, separator)
        };

        {
            let mut guard = self.bpm.fetch_page_write(leaf_page_id)?;
            let mut leaf = decode_leaf_node(guard.as_slice());

            let new_index = leaf.keys.len();
            leaf.keys.push(stolen_key);
            leaf.values.push(stolen_value);
            if was_tombstoned {
                leaf.tombstones.push(new_index as u16);
            }
            leaf.header.size = leaf.keys.len() as u16;

            while leaf.tombstones.len() > self.max_tombstones {
                leaf.process_oldest_tombstone();
            }

            encode_leaf_node(&leaf, guard.as_mut_slice());
        }

        // Update parent separator using held guard.
        let mut parent = decode_internal_node(parent_guard.as_slice());
        parent.set_key_at(child_index + 1, new_separator);
        encode_internal_node(&parent, parent_guard.as_mut_slice());

        Ok(())
    }

    /// Merge right leaf into left leaf. Uses held parent guard.
    fn merge_leaves_ctx(
        &self,
        parent_guard: &mut PageWriteGuard<'a>,
        right_child_index: usize,
        left_page_id: PageId,
        right_page_id: PageId,
    ) -> Result<()> {
        let (right_keys, right_values, right_tombstones, right_next) = {
            let guard = self.bpm.fetch_page_read(right_page_id)?;
            let right = decode_leaf_node(guard.as_slice());
            (right.keys, right.values, right.tombstones, right.next_page_id)
        };

        {
            let mut guard = self.bpm.fetch_page_write(left_page_id)?;
            let mut left = decode_leaf_node(guard.as_slice());

            let left_physical = left.keys.len();

            for &t in &right_tombstones {
                left.tombstones.push(t + left_physical as u16);
            }

            left.keys.extend(right_keys);
            left.values.extend(right_values);
            left.header.size = left.keys.len() as u16;
            left.next_page_id = right_next;

            while left.tombstones.len() > self.max_tombstones {
                left.process_oldest_tombstone();
            }

            encode_leaf_node(&left, guard.as_mut_slice());
        }

        if right_next != PageId::INVALID {
            let mut guard = self.bpm.fetch_page_write(right_next)?;
            let mut next_leaf = decode_leaf_node(guard.as_slice());
            next_leaf.prev_page_id = left_page_id;
            encode_leaf_node(&next_leaf, guard.as_mut_slice());
        }

        // Remove separator and right child from parent using held guard.
        let mut parent = decode_internal_node(parent_guard.as_slice());
        parent.remove_at(right_child_index);
        encode_internal_node(&parent, parent_guard.as_mut_slice());

        Ok(())
    }

    // =========================================================================
    // Internal Node Rebalancing
    // =========================================================================

    /// Check if an internal node needs rebalancing after a child merge.
    ///
    /// Takes the held guard on the node. Pops the next parent from context.
    fn rebalance_internal_ctx(
        &self,
        node_guard: PageWriteGuard<'a>,
        ctx: &mut Context<'a>,
    ) -> Result<()> {
        let node_page_id = node_guard.page_id();
        let node = decode_internal_node(node_guard.as_slice());

        // Check if this is the topmost held node (write_set is empty).
        let is_topmost = ctx.write_set.is_empty();

        if is_topmost {
            // Two cases: we hold the header guard (this IS the root) or
            // we released ancestors earlier (this was the safe node).
            if ctx.header_guard.is_some() {
                // This is the actual root.
                if node.children.len() == 1 {
                    let new_root_id = node.child_at(0);
                    drop(node_guard);
                    return self.shrink_root_ctx(ctx, node_page_id, new_root_id);
                }
                // Root is allowed to underflow.
                return Ok(());
            } else {
                // Ancestors were released at a safe node. This node was
                // safe for delete, meaning it can absorb one child removal.
                // No further propagation is possible or needed.
                return Ok(());
            }
        }

        // Non-root: check for underflow.
        if node.size() >= node.min_size() && node.children.len() > 1 {
            return Ok(());
        }

        // Need parent from write_set.
        let mut parent_guard = ctx.pop()
            .ok_or_else(|| Error::StorageCorrupted(
                "rebalance_internal_ctx: no parent in write_set".into()
            ))?;

        let parent = decode_internal_node(parent_guard.as_slice());
        let child_index = parent.find_child_position(node_page_id)
            .ok_or_else(|| Error::StorageCorrupted(
                "Internal node not found in parent".into()
            ))?;

        // Try redistribute from left sibling.
        if child_index > 0 {
            let left_page_id = parent.child_at(child_index - 1);
            let left = self.read_internal(left_page_id)?;
            if left.is_safe_for_delete() {
                drop(node);
                drop(node_guard);
                return self.redistribute_internal_from_left_ctx(
                    &mut parent_guard, child_index, left_page_id, node_page_id,
                );
            }
        }

        // Try redistribute from right sibling.
        if child_index < parent.children.len() - 1 {
            let right_page_id = parent.child_at(child_index + 1);
            let right = self.read_internal(right_page_id)?;
            if right.is_safe_for_delete() {
                drop(node);
                drop(node_guard);
                return self.redistribute_internal_from_right_ctx(
                    &mut parent_guard, child_index, node_page_id, right_page_id,
                );
            }
        }

        // No siblings — parent also has single child (degenerate chain).
        let has_left = child_index > 0;
        let has_right = child_index < parent.children.len() - 1;
        if !has_left && !has_right {
            drop(node);
            drop(node_guard);
            return self.rebalance_internal_ctx(parent_guard, ctx);
        }

        // Cannot redistribute — must merge.
        drop(node);
        drop(node_guard);

        if has_left {
            let left_page_id = parent.child_at(child_index - 1);
            self.merge_internals_ctx(
                &mut parent_guard, child_index, left_page_id, node_page_id,
            )?;
        } else {
            let right_page_id = parent.child_at(child_index + 1);
            self.merge_internals_ctx(
                &mut parent_guard, child_index + 1, node_page_id, right_page_id,
            )?;
        }

        // Recurse up to check parent.
        self.rebalance_internal_ctx(parent_guard, ctx)
    }

    /// Steal from left internal sibling. Uses held parent guard.
    fn redistribute_internal_from_left_ctx(
        &self,
        parent_guard: &mut PageWriteGuard<'a>,
        child_index: usize,
        left_page_id: PageId,
        node_page_id: PageId,
    ) -> Result<()> {
        let parent = decode_internal_node(parent_guard.as_slice());
        let old_separator = parent.key_at(child_index).to_vec();
        drop(parent);

        let (new_separator, moved_child) = {
            let mut guard = self.bpm.fetch_page_write(left_page_id)?;
            let mut left = decode_internal_node(guard.as_slice());

            let last = left.keys.len() - 1;
            let key = left.keys.remove(last);
            let child = left.children.remove(last);
            left.header.size = left.keys.len() as u16;

            encode_internal_node(&left, guard.as_mut_slice());
            (key, child)
        };

        {
            let mut guard = self.bpm.fetch_page_write(node_page_id)?;
            let mut node = decode_internal_node(guard.as_slice());

            node.keys.insert(1, old_separator);
            node.children.insert(0, moved_child);
            node.header.size = node.keys.len() as u16;

            encode_internal_node(&node, guard.as_mut_slice());
        }

        // Update parent separator using held guard.
        let mut parent = decode_internal_node(parent_guard.as_slice());
        parent.set_key_at(child_index, new_separator);
        encode_internal_node(&parent, parent_guard.as_mut_slice());

        Ok(())
    }

    /// Steal from right internal sibling. Uses held parent guard.
    fn redistribute_internal_from_right_ctx(
        &self,
        parent_guard: &mut PageWriteGuard<'a>,
        child_index: usize,
        node_page_id: PageId,
        right_page_id: PageId,
    ) -> Result<()> {
        let parent = decode_internal_node(parent_guard.as_slice());
        let old_separator = parent.key_at(child_index + 1).to_vec();
        drop(parent);

        let (new_separator, moved_child) = {
            let mut guard = self.bpm.fetch_page_write(right_page_id)?;
            let mut right = decode_internal_node(guard.as_slice());

            let key = right.keys.remove(1);
            let child = right.children.remove(0);
            right.header.size = right.keys.len() as u16;

            encode_internal_node(&right, guard.as_mut_slice());
            (key, child)
        };

        {
            let mut guard = self.bpm.fetch_page_write(node_page_id)?;
            let mut node = decode_internal_node(guard.as_slice());

            node.keys.push(old_separator);
            node.children.push(moved_child);
            node.header.size = node.keys.len() as u16;

            encode_internal_node(&node, guard.as_mut_slice());
        }

        // Update parent separator using held guard.
        let mut parent = decode_internal_node(parent_guard.as_slice());
        parent.set_key_at(child_index + 1, new_separator);
        encode_internal_node(&parent, parent_guard.as_mut_slice());

        Ok(())
    }

    /// Merge right internal node into left. Uses held parent guard.
    fn merge_internals_ctx(
        &self,
        parent_guard: &mut PageWriteGuard<'a>,
        right_child_index: usize,
        left_page_id: PageId,
        right_page_id: PageId,
    ) -> Result<()> {
        let parent = decode_internal_node(parent_guard.as_slice());
        let separator = parent.key_at(right_child_index).to_vec();
        drop(parent);

        let (right_keys, right_children) = {
            let guard = self.bpm.fetch_page_read(right_page_id)?;
            let right = decode_internal_node(guard.as_slice());
            let keys: Vec<Vec<u8>> = right.keys[1..].to_vec();
            let children: Vec<PageId> = right.children.clone();
            (keys, children)
        };

        {
            let mut guard = self.bpm.fetch_page_write(left_page_id)?;
            let mut left = decode_internal_node(guard.as_slice());

            left.keys.push(separator);
            left.keys.extend(right_keys);
            left.children.extend(right_children);
            left.header.size = left.keys.len() as u16;

            encode_internal_node(&left, guard.as_mut_slice());
        }

        // Remove separator and right child from parent using held guard.
        let mut parent = decode_internal_node(parent_guard.as_slice());
        parent.remove_at(right_child_index);
        encode_internal_node(&parent, parent_guard.as_mut_slice());

        Ok(())
    }

    /// Remove an empty subtree using held context guards.
    fn remove_empty_subtree_ctx(
        &self,
        leaf_page_id: PageId,
        mut node_guard: PageWriteGuard<'a>,
        ctx: &mut Context<'a>,
    ) -> Result<()> {
        let mut child_to_remove = leaf_page_id;

        loop {
            let node = decode_internal_node(node_guard.as_slice());

            if node.children.len() > 1 {
                // This ancestor has multiple children. Remove the degenerate one.
                let child_index = node.find_child_position(child_to_remove)
                    .ok_or_else(|| Error::StorageCorrupted(
                        "Degenerate child not found in parent".into()
                    ))?;

                drop(node);
                let mut n = decode_internal_node(node_guard.as_slice());
                n.remove_at(child_index);
                encode_internal_node(&n, node_guard.as_mut_slice());

                return self.rebalance_internal_ctx(node_guard, ctx);
            }

            // Node has single child. Check if this is the root.
            child_to_remove = node_guard.page_id();
            drop(node);

            match ctx.pop() {
                Some(parent_guard) => {
                    // Continue up with the parent guard.
                    drop(node_guard);
                    node_guard = parent_guard;
                }
                None => {
                    // Reached the root — tree is empty.
                    drop(node_guard);
                    if let Some(mut header_guard) = ctx.header_guard.take() {
                        let mut header = BTreeHeaderPage::decode(header_guard.as_slice());
                        header.root_page_id = PageId::INVALID;
                        header.encode(header_guard.as_mut_slice());
                    }
                    return Ok(());
                }
            }
        }
    }

    /// Shrink tree when root has only one child. Uses held header guard.
    fn shrink_root_ctx(
        &self,
        ctx: &mut Context<'a>,
        old_root_id: PageId,
        new_root_id: PageId,
    ) -> Result<()> {
        if let Some(mut header_guard) = ctx.header_guard.take() {
            let mut header = BTreeHeaderPage::decode(header_guard.as_slice());
            debug_assert_eq!(header.root_page_id, old_root_id);
            header.root_page_id = new_root_id;
            header.encode(header_guard.as_mut_slice());
        } else {
            // Header was already released (safe node found earlier).
            // Re-acquire header for root update.
            let mut header_guard = self.bpm.fetch_page_write(self.header_page_id)?;
            let mut header = BTreeHeaderPage::decode(header_guard.as_slice());
            debug_assert_eq!(header.root_page_id, old_root_id);
            header.root_page_id = new_root_id;
            header.encode(header_guard.as_mut_slice());
        }
        Ok(())
    }

    // =========================================================================
    // Scan Operations (Task 3.2.5)
    // =========================================================================

    /// Create a lazy range scan iterator over the B+Tree.
    ///
    /// Supports all `RangeBounds` variants: `..`, `start..`, `..end`,
    /// `start..end`, `start..=end`, `..=end`.
    ///
    /// The iterator fetches one leaf page at a time, yielding entries in
    /// sorted order. Pages are not held across `next()` calls.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<BTreeScanIterator<'_>> {
        let start_bound = match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        // Determine the start page based on the start bound.
        let start_page_id = match &start_bound {
            Bound::Unbounded => {
                match self.get_first_leaf()? {
                    Some(id) => id,
                    None => PageId::INVALID,
                }
            }
            Bound::Included(key) | Bound::Excluded(key) => {
                match self.find_leaf(key)? {
                    Some(id) => id,
                    None => PageId::INVALID,
                }
            }
        };

        Ok(BTreeScanIterator::new(self.bpm, start_page_id, start_bound, end_bound))
    }

    /// Get the first leaf page (leftmost). Uses latch coupling.
    pub fn get_first_leaf(&self) -> Result<Option<PageId>> {
        let root_page_id = match self.get_root_page_id()? {
            Some(id) => id,
            None => return Ok(None),
        };

        let mut current_guard = self.bpm.fetch_page_read(root_page_id)?;

        for _depth in 0..MAX_TREE_HEIGHT {
            let data = current_guard.as_slice();

            let node_type = NodeType::from_u8(data[0])
                .ok_or_else(|| Error::StorageCorrupted(
                    format!("Invalid node type at page {}", current_guard.page_id().0)
                ))?;

            match node_type {
                NodeType::Leaf => return Ok(Some(current_guard.page_id())),
                NodeType::Internal => {
                    let internal = decode_internal_node(data);
                    let child_id = internal.child_at(0);

                    let child_guard = self.bpm.fetch_page_read(child_id)?;
                    drop(current_guard);
                    current_guard = child_guard;
                }
            }
        }

        Err(Error::StorageCorrupted(
            format!("Tree exceeds max height {}", MAX_TREE_HEIGHT)
        ))
    }

    /// Get the last leaf page (rightmost). Uses latch coupling.
    pub fn get_last_leaf(&self) -> Result<Option<PageId>> {
        let root_page_id = match self.get_root_page_id()? {
            Some(id) => id,
            None => return Ok(None),
        };

        let mut current_guard = self.bpm.fetch_page_read(root_page_id)?;

        for _depth in 0..MAX_TREE_HEIGHT {
            let data = current_guard.as_slice();

            let node_type = NodeType::from_u8(data[0])
                .ok_or_else(|| Error::StorageCorrupted(
                    format!("Invalid node type at page {}", current_guard.page_id().0)
                ))?;

            match node_type {
                NodeType::Leaf => return Ok(Some(current_guard.page_id())),
                NodeType::Internal => {
                    let internal = decode_internal_node(data);
                    let child_id = internal.child_at(internal.children.len() - 1);

                    let child_guard = self.bpm.fetch_page_read(child_id)?;
                    drop(current_guard);
                    current_guard = child_guard;
                }
            }
        }

        Err(Error::StorageCorrupted(
            format!("Tree exceeds max height {}", MAX_TREE_HEIGHT)
        ))
    }

    /// Collect all live (non-tombstoned) key-value pairs in sorted order.
    ///
    /// Convenience wrapper around `scan(..)` that eagerly collects results.
    pub fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan(..)?.collect::<Result<Vec<_>>>()
    }

    /// Collect all live key-value pairs starting from the given key (inclusive).
    ///
    /// Convenience wrapper around `scan(start_key..)` that eagerly collects results.
    pub fn scan_from(&self, start_key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan(start_key.to_vec()..)?.collect::<Result<Vec<_>>>()
    }

    /// Count the number of leaf pages in the tree.
    pub fn count_leaves(&self) -> Result<usize> {
        let first = match self.get_first_leaf()? {
            Some(id) => id,
            None => return Ok(0),
        };

        let mut current = first;
        let mut count = 0;
        let max_leaves = 10_000;

        while current != PageId::INVALID && count < max_leaves {
            let leaf = self.read_leaf(current)?;
            count += 1;
            current = leaf.next_page_id;
        }

        Ok(count)
    }

    // =========================================================================
    // Internal Helpers
    // =========================================================================

    /// Get the root page ID from the header.
    fn get_root_page_id(&self) -> Result<Option<PageId>> {
        let guard = self.bpm.fetch_page_read(self.header_page_id)?;
        let header = BTreeHeaderPage::decode(guard.as_slice());

        if header.is_empty() {
            Ok(None)
        } else {
            Ok(Some(header.root_page_id))
        }
    }

    /// Find the leaf page that should contain the given key.
    ///
    /// Returns the PageId only (no held guard). Used by scan() which does
    /// its own page-at-a-time reads.
    fn find_leaf(&self, key: &[u8]) -> Result<Option<PageId>> {
        match self.find_leaf_read(key)? {
            Some(guard) => Ok(Some(guard.page_id())),
            None => Ok(None),
        }
    }

    /// Find the leaf page for the given key with latch coupling.
    ///
    /// Returns a held read guard on the leaf page. The traversal holds the
    /// parent read latch while acquiring the child read latch, then releases
    /// the parent — eliminating the race window between release and acquire.
    fn find_leaf_read(&self, key: &[u8]) -> Result<Option<PageReadGuard<'a>>> {
        let root_page_id = match self.get_root_page_id()? {
            Some(id) => id,
            None => return Ok(None),
        };

        let mut current_guard = self.bpm.fetch_page_read(root_page_id)?;

        for _depth in 0..MAX_TREE_HEIGHT {
            let data = current_guard.as_slice();

            let node_type = NodeType::from_u8(data[0])
                .ok_or_else(|| Error::StorageCorrupted(
                    format!("Invalid node type {} at page {}", data[0], current_guard.page_id().0)
                ))?;

            match node_type {
                NodeType::Leaf => return Ok(Some(current_guard)),
                NodeType::Internal => {
                    let internal = decode_internal_node(data);
                    let child_index = internal.find_child_index(key);
                    let child_id = internal.child_at(child_index);

                    // Latch coupling: acquire child before releasing parent.
                    let child_guard = self.bpm.fetch_page_read(child_id)?;
                    drop(current_guard); // Release parent.
                    current_guard = child_guard;
                }
            }
        }

        Err(Error::StorageCorrupted(
            format!("Tree exceeds max height {}", MAX_TREE_HEIGHT)
        ))
    }

    /// Read a leaf node from a page.
    pub fn read_leaf(&self, page_id: PageId) -> Result<LeafNode> {
        let guard = self.bpm.fetch_page_read(page_id)?;
        let data = guard.as_slice();

        let node_type = NodeType::from_u8(data[0])
            .ok_or_else(|| Error::StorageCorrupted(
                format!("Invalid node type at page {}", page_id.0)
            ))?;

        if node_type != NodeType::Leaf {
            return Err(Error::StorageCorrupted(
                format!("Expected leaf at page {}", page_id.0)
            ));
        }

        Ok(decode_leaf_node(data))
    }

    /// Read an internal node from a page.
    pub fn read_internal(&self, page_id: PageId) -> Result<InternalNode> {
        let guard = self.bpm.fetch_page_read(page_id)?;
        let data = guard.as_slice();

        let node_type = NodeType::from_u8(data[0])
            .ok_or_else(|| Error::StorageCorrupted(
                format!("Invalid node type at page {}", page_id.0)
            ))?;

        if node_type != NodeType::Internal {
            return Err(Error::StorageCorrupted(
                format!("Expected internal node at page {}", page_id.0)
            ));
        }

        Ok(decode_internal_node(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::BufferPoolManager;
    use crate::storage::DiskManager;
    use super::super::page_layout::encode_internal_node;
    use tempfile::tempdir;

    fn setup_bpm() -> (BufferPoolManager, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let dm = DiskManager::create(&path).unwrap();
        let bpm = BufferPoolManager::new(10, dm);
        (bpm, dir)
    }

    fn create_empty_tree(bpm: &BufferPoolManager) -> PageId {
        let header_page = bpm.new_page().unwrap();
        let header_page_id = header_page.page_id();
        {
            let mut guard = header_page;
            let header = BTreeHeaderPage::new();
            header.encode(guard.as_mut_slice());
        }
        header_page_id
    }

    #[test]
    fn test_empty_tree() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        assert!(tree.is_empty().unwrap());
        assert_eq!(tree.get(b"key").unwrap(), None);
        assert!(!tree.contains(b"key").unwrap());
        assert_eq!(tree.get_first_leaf().unwrap(), None);
    }

    #[test]
    fn test_insert_into_empty_tree() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        assert!(tree.insert(b"apple", b"red").unwrap());
        assert!(!tree.is_empty().unwrap());
        assert_eq!(tree.get(b"apple").unwrap(), Some(b"red".to_vec()));
    }

    #[test]
    fn test_insert_multiple() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        assert!(tree.insert(b"banana", b"yellow").unwrap());
        assert!(tree.insert(b"apple", b"red").unwrap());
        assert!(tree.insert(b"cherry", b"red").unwrap());

        assert_eq!(tree.get(b"apple").unwrap(), Some(b"red".to_vec()));
        assert_eq!(tree.get(b"banana").unwrap(), Some(b"yellow".to_vec()));
        assert_eq!(tree.get(b"cherry").unwrap(), Some(b"red".to_vec()));
        assert_eq!(tree.get(b"date").unwrap(), None);
    }

    #[test]
    fn test_insert_duplicate() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        assert!(tree.insert(b"apple", b"red").unwrap());
        assert!(!tree.insert(b"apple", b"green").unwrap()); // Duplicate
        assert_eq!(tree.get(b"apple").unwrap(), Some(b"red".to_vec())); // Original value
    }

    #[test]
    fn test_delete() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        tree.insert(b"apple", b"red").unwrap();
        tree.insert(b"banana", b"yellow").unwrap();

        assert!(tree.delete(b"apple").unwrap());
        assert_eq!(tree.get(b"apple").unwrap(), None);
        assert_eq!(tree.get(b"banana").unwrap(), Some(b"yellow".to_vec()));

        assert!(!tree.delete(b"apple").unwrap()); // Already deleted
        assert!(!tree.delete(b"cherry").unwrap()); // Never existed
    }

    #[test]
    fn test_contains() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        tree.insert(b"apple", b"red").unwrap();

        assert!(tree.contains(b"apple").unwrap());
        assert!(!tree.contains(b"banana").unwrap());

        tree.delete(b"apple").unwrap();
        assert!(!tree.contains(b"apple").unwrap());
    }

    #[test]
    fn test_two_level_tree_search() {
        let (bpm, _dir) = setup_bpm();

        // Manually construct a two-level tree
        let header_id = create_empty_tree(&bpm);

        // Create left leaf
        let left_leaf = bpm.new_page().unwrap();
        let left_id = left_leaf.page_id();
        {
            let mut guard = left_leaf;
            let mut leaf = LeafNode::new(100);
            leaf.insert(b"apple".to_vec(), b"red".to_vec());
            encode_leaf_node(&leaf, guard.as_mut_slice());
        }

        // Create right leaf
        let right_leaf = bpm.new_page().unwrap();
        let right_id = right_leaf.page_id();
        {
            let mut guard = right_leaf;
            let mut leaf = LeafNode::new(100);
            leaf.insert(b"banana".to_vec(), b"yellow".to_vec());
            encode_leaf_node(&leaf, guard.as_mut_slice());
        }

        // Create root internal
        let root = bpm.new_page().unwrap();
        let root_id = root.page_id();
        {
            let mut guard = root;
            let mut internal = InternalNode::new(100);
            internal.init_with_child(left_id);
            internal.insert_separator(b"banana".to_vec(), right_id);
            encode_internal_node(&internal, guard.as_mut_slice());
        }

        // Update header
        {
            let mut guard = bpm.fetch_page_write(header_id).unwrap();
            let mut header = BTreeHeaderPage::decode(guard.as_slice());
            header.root_page_id = root_id;
            header.encode(guard.as_mut_slice());
        }

        let tree = BTree::new(&bpm, header_id);

        assert_eq!(tree.get(b"apple").unwrap(), Some(b"red".to_vec()));
        assert_eq!(tree.get(b"banana").unwrap(), Some(b"yellow".to_vec()));
        assert_eq!(tree.get(b"cherry").unwrap(), None);

        assert_eq!(tree.get_first_leaf().unwrap(), Some(left_id));
        assert_eq!(tree.get_last_leaf().unwrap(), Some(right_id));
    }

    #[test]
    fn test_leaf_split() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        // Insert enough entries to trigger a leaf split.
        // With default avg sizes, leaf max_size is around 40.
        // Insert 50 entries to ensure split.
        for i in 0..50u32 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            assert!(tree.insert(key.as_bytes(), value.as_bytes()).unwrap());
        }

        // Verify all entries are retrievable
        for i in 0..50u32 {
            let key = format!("key{:04}", i);
            let value = format!("value{:04}", i);
            assert_eq!(
                tree.get(key.as_bytes()).unwrap(),
                Some(value.into_bytes()),
                "Failed to retrieve key{:04}", i
            );
        }

        // Tree should no longer have leaf as root (internal node created)
        let root_id = tree.get_root_page_id().unwrap().unwrap();
        let guard = bpm.fetch_page_read(root_id).unwrap();
        let node_type = NodeType::from_u8(guard.as_slice()[0]).unwrap();
        assert_eq!(node_type, NodeType::Internal, "Root should be internal after split");
    }

    #[test]
    fn test_multiple_splits() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        // Insert many entries to trigger multiple splits
        let count = 200;
        for i in 0..count {
            let key = format!("k{:05}", i);
            let value = format!("v{:05}", i);
            tree.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Verify all entries
        for i in 0..count {
            let key = format!("k{:05}", i);
            let value = format!("v{:05}", i);
            assert_eq!(
                tree.get(key.as_bytes()).unwrap(),
                Some(value.into_bytes()),
                "Failed at key k{:05}", i
            );
        }

        // Verify duplicates rejected
        for i in 0..10 {
            let key = format!("k{:05}", i);
            assert!(!tree.insert(key.as_bytes(), b"new").unwrap());
        }
    }

    #[test]
    fn test_split_maintains_order() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        // Insert in random-ish order to test sorting
        let keys = [50, 25, 75, 10, 40, 60, 90, 5, 15, 30, 45, 55, 70, 80, 95];
        for &k in &keys {
            let key = format!("{:03}", k);
            tree.insert(key.as_bytes(), key.as_bytes()).unwrap();
        }

        // Verify all retrievable
        for &k in &keys {
            let key = format!("{:03}", k);
            assert_eq!(tree.get(key.as_bytes()).unwrap(), Some(key.into_bytes()));
        }
    }

    #[test]
    fn test_split_with_sibling_pointers() {
        let (bpm, _dir) = setup_bpm();
        let header_id = create_empty_tree(&bpm);
        let tree = BTree::new(&bpm, header_id);

        // Insert enough to create multiple leaves
        for i in 0..100u32 {
            let key = format!("{:04}", i);
            tree.insert(key.as_bytes(), key.as_bytes()).unwrap();
        }

        // Walk leaf chain from first to last
        let first_leaf_id = tree.get_first_leaf().unwrap().unwrap();
        let last_leaf_id = tree.get_last_leaf().unwrap().unwrap();

        let mut current = first_leaf_id;
        let mut leaf_count = 0;
        let max_leaves = 20; // Safety bound

        while current != PageId::INVALID && leaf_count < max_leaves {
            let leaf = tree.read_leaf(current).unwrap();
            leaf_count += 1;

            if current == last_leaf_id {
                assert_eq!(leaf.next_page_id, PageId::INVALID);
                break;
            }

            assert_ne!(leaf.next_page_id, PageId::INVALID);
            current = leaf.next_page_id;
        }

        assert!(leaf_count >= 2, "Should have multiple leaves after inserts");
        assert!(leaf_count < max_leaves, "Leaf chain too long or infinite");
    }
}
