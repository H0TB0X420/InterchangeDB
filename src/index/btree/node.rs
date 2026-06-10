//! B+Tree node types.
//!
//! Defines the in-memory representation of internal and leaf nodes.
//! These are deserialized from pages for manipulation, then serialized back.

use crate::common::PageId;

/// Maximum number of tombstones per leaf node.
///
/// When the tombstone buffer is full, the oldest tombstone is physically
/// removed (entries shifted) before adding a new one.
pub const MAX_TOMBSTONES: usize = 8;

/// Internal node header size in bytes.
/// Layout: PageHeader(13) + node_type(1) + size(2) + max_size(2) + reserved(7) = 25 bytes
pub const INTERNAL_HEADER_SIZE: usize = 25;

/// Leaf node header size in bytes.
/// Layout: PageHeader(13) + node_type(1) + size(2) + max_size(2) + next_page_id(4) +
///         prev_page_id(4) + tombstone_count(2) + reserved(5) = 33 bytes
pub const LEAF_HEADER_SIZE: usize = 33;

/// Node type discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    /// Internal (non-leaf) node containing keys and child pointers.
    Internal = 0,
    /// Leaf node containing keys and values.
    Leaf = 1,
}

impl NodeType {
    /// Convert from u8.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(NodeType::Internal),
            1 => Some(NodeType::Leaf),
            _ => None,
        }
    }
}

/// Common node header fields.
#[derive(Debug, Clone)]
pub struct NodeHeader {
    /// Type of this node (Internal or Leaf).
    pub node_type: NodeType,
    /// Current number of keys in this node.
    pub size: u16,
    /// Maximum number of keys this node can hold.
    pub max_size: u16,
}

impl NodeHeader {
    /// Create a new node header.
    pub fn new(node_type: NodeType, max_size: u16) -> Self {
        Self {
            node_type,
            size: 0,
            max_size,
        }
    }

    /// Check if this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        self.node_type == NodeType::Leaf
    }

    /// Get minimum size (for underflow detection).
    /// min_size = floor(max_size / 2)
    pub fn min_size(&self) -> u16 {
        self.max_size / 2
    }

    /// Check if node is at minimum size (would underflow on delete).
    pub fn at_min_size(&self) -> bool {
        self.size <= self.min_size()
    }

    /// Check if node would overflow on insert.
    pub fn would_overflow(&self) -> bool {
        self.size >= self.max_size
    }

    /// Check if node is "safe" for latch crabbing (won't split on insert).
    pub fn is_safe_for_insert(&self) -> bool {
        self.size < self.max_size - 1
    }

    /// Check if node is "safe" for latch crabbing (won't merge on delete).
    pub fn is_safe_for_delete(&self) -> bool {
        self.size > self.min_size()
    }
}

/// Internal (non-leaf) B+tree node.
///
/// Contains n keys and n+1 child pointers. The first key (index 0) is
/// considered invalid/unused to maintain the n keys, n+1 children invariant.
///
/// Routing: For key K, follow `child[i]` where `keys[i] <= K < keys[i+1]`.
#[derive(Debug, Clone)]
pub struct InternalNode {
    /// Node header.
    pub header: NodeHeader,
    /// Keys (index 0 is unused/invalid).
    pub keys: Vec<Vec<u8>>,
    /// Child page IDs. children.len() == keys.len() for our representation.
    /// `children[i]` is the child for keys <= `keys[i]`.
    pub children: Vec<PageId>,
}

impl InternalNode {
    /// Create a new empty internal node.
    pub fn new(max_size: u16) -> Self {
        Self {
            header: NodeHeader::new(NodeType::Internal, max_size),
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Initialize with first child (when creating root after first split).
    pub fn init_with_child(&mut self, child: PageId) {
        self.keys.push(Vec::new()); // Invalid key at index 0
        self.children.push(child);
        self.header.size = 1;
    }

    /// Get the number of children.
    pub fn num_children(&self) -> usize {
        self.children.len()
    }

    /// Get key at index (index 0 is invalid).
    pub fn key_at(&self, index: usize) -> &[u8] {
        &self.keys[index]
    }

    /// Get child page ID at index.
    pub fn child_at(&self, index: usize) -> PageId {
        self.children[index]
    }

    /// Find which child to follow for the given key.
    ///
    /// Uses binary search to find the largest index i where `keys[i] <= key`.
    /// Returns the index of the child to follow.
    pub fn find_child_index(&self, key: &[u8]) -> usize {
        if self.children.is_empty() {
            return 0;
        }

        // Binary search: find the rightmost key <= search key
        // Skip index 0 (invalid key)
        let mut left = 1;
        let mut right = self.keys.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if self.keys[mid].as_slice() <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // left is now the first index where key[left] > search key
        // We want the child at left - 1, but bounded to valid range
        left.saturating_sub(1)
    }

    /// Find the insertion index for a new separator key.
    ///
    /// Returns the index where the key should be inserted to maintain sorted order.
    pub fn find_insert_index(&self, key: &[u8]) -> usize {
        // Skip index 0 (always invalid)
        let mut index = 1;
        while index < self.keys.len() && self.keys[index].as_slice() < key {
            index += 1;
        }
        index
    }

    /// Insert a separator key and its associated child page.
    ///
    /// The new_child is the page to the right of the separator.
    pub fn insert_separator(&mut self, key: Vec<u8>, new_child: PageId) {
        let index = self.find_insert_index(&key);
        self.keys.insert(index, key);
        self.children.insert(index, new_child);
        self.header.size += 1;
    }

    /// Remove the entry at the given index.
    ///
    /// When removing `key[i]`, also removes `children[i]` (the child to the left
    /// of the separator, which was merged).
    pub fn remove_at(&mut self, index: usize) {
        if index < self.keys.len() {
            self.keys.remove(index);
        }
        if index < self.children.len() {
            self.children.remove(index);
        }
        self.header.size = self.header.size.saturating_sub(1);
    }

    /// Set key at index.
    pub fn set_key_at(&mut self, index: usize, key: Vec<u8>) {
        self.keys[index] = key;
    }

    /// Set child at index.
    pub fn set_child_at(&mut self, index: usize, child: PageId) {
        self.children[index] = child;
    }

    /// Find the position of a child page ID in the children array.
    ///
    /// Used during rebalancing to locate a node within its parent.
    pub fn find_child_position(&self, child: PageId) -> Option<usize> {
        self.children.iter().position(|&c| c == child)
    }

    /// Check if this node is safe for insert (won't split).
    pub fn is_safe_for_insert(&self) -> bool {
        self.header.is_safe_for_insert()
    }

    /// Check if this node is safe for delete (won't underflow).
    pub fn is_safe_for_delete(&self) -> bool {
        self.header.is_safe_for_delete()
    }

    /// Get the current size (number of keys, including the invalid one at 0).
    pub fn size(&self) -> u16 {
        self.header.size
    }

    /// Get the minimum allowed size.
    pub fn min_size(&self) -> u16 {
        self.header.min_size()
    }
}

/// Leaf B+tree node.
///
/// Contains key-value pairs in sorted order, plus a linked list pointer
/// to the next leaf for range scans.
#[derive(Debug, Clone)]
pub struct LeafNode {
    /// Node header.
    pub header: NodeHeader,
    /// Next leaf page ID for forward scans.
    pub next_page_id: PageId,
    /// Previous leaf page ID for backward scans.
    pub prev_page_id: PageId,
    /// Keys in sorted order.
    pub keys: Vec<Vec<u8>>,
    /// Values corresponding to keys.
    pub values: Vec<Vec<u8>>,
    /// Indices of tombstoned (deleted but not removed) entries.
    /// Stored as indices into keys/values arrays.
    pub tombstones: Vec<u16>,
}

impl LeafNode {
    /// Create a new empty leaf node.
    pub fn new(max_size: u16) -> Self {
        Self {
            header: NodeHeader::new(NodeType::Leaf, max_size),
            next_page_id: PageId::INVALID,
            prev_page_id: PageId::INVALID,
            keys: Vec::new(),
            values: Vec::new(),
            tombstones: Vec::new(),
        }
    }

    /// Get the physical size (including tombstoned entries).
    pub fn physical_size(&self) -> usize {
        self.keys.len()
    }

    /// Get the logical size (excluding tombstoned entries).
    pub fn logical_size(&self) -> usize {
        self.keys.len() - self.tombstones.len()
    }

    /// Get key at index.
    pub fn key_at(&self, index: usize) -> &[u8] {
        &self.keys[index]
    }

    /// Get value at index.
    pub fn value_at(&self, index: usize) -> &[u8] {
        &self.values[index]
    }

    /// Check if entry at index is tombstoned.
    pub fn is_tombstone(&self, index: usize) -> bool {
        self.tombstones.contains(&(index as u16))
    }

    /// Lookup key, respecting tombstones.
    ///
    /// Returns Some(index) if key found and not tombstoned, None otherwise.
    pub fn lookup(&self, key: &[u8]) -> Option<usize> {
        self.lookup_physical(key)
            .filter(|&idx| !self.is_tombstone(idx))
    }

    /// Lookup key ignoring tombstones (physical lookup).
    ///
    /// Returns Some(index) if key physically exists, None otherwise.
    pub fn lookup_physical(&self, key: &[u8]) -> Option<usize> {
        self.keys.binary_search_by(|k| k.as_slice().cmp(key)).ok()
    }

    /// Find insertion index for a key (where it would go to maintain sorted order).
    pub fn find_insert_index(&self, key: &[u8]) -> usize {
        match self.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(index) => index,  // Key exists, would overwrite
            Err(index) => index, // Key doesn't exist, insert here
        }
    }

    /// Insert a key-value pair at the correct position.
    ///
    /// If key already exists (physically), returns false.
    /// If key exists but is tombstoned, revives it with the new value.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> bool {
        if let Some(index) = self.lookup_physical(&key) {
            if self.is_tombstone(index) {
                // Revive tombstoned entry
                self.values[index] = value;
                self.remove_tombstone_at_index(index);
                return true;
            }
            // Duplicate key
            return false;
        }

        let index = self.find_insert_index(&key);

        // Adjust tombstone indices for entries that shift
        for tomb in &mut self.tombstones {
            if *tomb as usize >= index {
                *tomb += 1;
            }
        }

        self.keys.insert(index, key);
        self.values.insert(index, value);
        self.header.size += 1;
        true
    }

    /// Delete a key.
    ///
    /// When `max_tombstones == 0`, physically removes the entry immediately.
    /// When `max_tombstones > 0`, adds a tombstone marker. If the tombstone
    /// buffer is full, evicts the oldest tombstone (physically removing it)
    /// before adding the new one.
    ///
    /// Returns true if key was found and deleted, false if not found.
    pub fn delete(&mut self, key: &[u8], max_tombstones: usize) -> bool {
        let index = match self.lookup(key) {
            Some(idx) => idx,
            None => return false, // Not found or already tombstoned
        };

        if max_tombstones == 0 {
            // Physically remove immediately — no tombstone.
            self.keys.remove(index);
            self.values.remove(index);
            self.header.size = self.keys.len() as u16;
            // Adjust any stale tombstone indices (defensive).
            self.tombstones.retain(|&t| (t as usize) != index);
            for tomb in &mut self.tombstones {
                if *tomb as usize > index {
                    *tomb -= 1;
                }
            }
            return true;
        }

        // Tombstone-based deletion.
        if self.tombstones.len() >= max_tombstones {
            // Evict oldest tombstone (physically remove it)
            self.process_oldest_tombstone();
            // Recalculate index since array shifted
            if let Some(new_index) = self.lookup(key) {
                self.add_tombstone(new_index);
            }
        } else {
            self.add_tombstone(index);
        }

        true
    }

    /// Add a tombstone for the entry at index.
    fn add_tombstone(&mut self, index: usize) {
        if !self.is_tombstone(index) {
            self.tombstones.push(index as u16);
        }
    }

    /// Remove tombstone for the entry at index.
    fn remove_tombstone_at_index(&mut self, index: usize) {
        self.tombstones.retain(|&t| t as usize != index);
    }

    /// Process (physically remove) the oldest tombstone.
    pub fn process_oldest_tombstone(&mut self) {
        if self.tombstones.is_empty() {
            return;
        }

        // Remove the first (oldest) tombstone
        let oldest_index = self.tombstones.remove(0) as usize;

        // Physically remove the entry
        self.keys.remove(oldest_index);
        self.values.remove(oldest_index);
        self.header.size = self.header.size.saturating_sub(1);

        // Adjust remaining tombstone indices
        for tomb in &mut self.tombstones {
            if *tomb as usize > oldest_index {
                *tomb -= 1;
            }
        }
    }

    /// Process all tombstones (compact the node).
    pub fn compact(&mut self) {
        if self.tombstones.is_empty() {
            return;
        }

        // Sort tombstones in descending order to remove from end first
        self.tombstones.sort_by(|a, b| b.cmp(a));

        for &tomb_index in &self.tombstones.clone() {
            let idx = tomb_index as usize;
            if idx < self.keys.len() {
                self.keys.remove(idx);
                self.values.remove(idx);
            }
        }

        self.header.size = self.keys.len() as u16;
        self.tombstones.clear();
    }

    /// Get tombstone count.
    pub fn tombstone_count(&self) -> usize {
        self.tombstones.len()
    }

    /// Check if this node is safe for insert (won't split).
    pub fn is_safe_for_insert(&self) -> bool {
        // Use physical size since that's what determines page space
        (self.physical_size() as u16) < self.header.max_size - 1
    }

    /// Check if node needs to be split (overflowed past max size).
    ///
    /// Called after insert. A leaf can hold max_size entries; split only
    /// when the (max_size + 1)th entry is inserted.
    pub fn needs_split(&self) -> bool {
        self.physical_size() as u16 > self.header.max_size
    }

    /// Check if this node is safe for delete (won't underflow).
    pub fn is_safe_for_delete(&self) -> bool {
        self.logical_size() as u16 > self.header.min_size()
    }

    /// Get minimum size (for underflow checks, uses logical size).
    pub fn min_size(&self) -> u16 {
        self.header.min_size()
    }

    /// Get current logical size as u16.
    pub fn size(&self) -> u16 {
        self.logical_size() as u16
    }

    /// Check if node has underflowed (physical size below minimum).
    ///
    /// Uses physical_size so that tombstoned entries (which still occupy space)
    /// prevent unnecessary rebalancing. Only when tombstone eviction physically
    /// removes an entry can underflow occur.
    pub fn has_underflowed(&self) -> bool {
        (self.physical_size() as u16) < self.min_size()
    }

    /// Set value at index (for updates).
    pub fn set_value_at(&mut self, index: usize, value: Vec<u8>) {
        self.values[index] = value;
    }

    /// Get all non-tombstoned key-value pairs.
    pub fn live_entries(&self) -> Vec<(&[u8], &[u8])> {
        self.keys
            .iter()
            .zip(self.values.iter())
            .enumerate()
            .filter(|(i, _)| !self.is_tombstone(*i))
            .map(|(_, (k, v))| (k.as_slice(), v.as_slice()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // NodeHeader tests
    // ========================================================================

    #[test]
    fn test_node_header_new() {
        let header = NodeHeader::new(NodeType::Internal, 10);
        assert_eq!(header.node_type, NodeType::Internal);
        assert_eq!(header.size, 0);
        assert_eq!(header.max_size, 10);
    }

    #[test]
    fn test_node_header_min_size() {
        let header = NodeHeader::new(NodeType::Leaf, 10);
        assert_eq!(header.min_size(), 5);

        let header = NodeHeader::new(NodeType::Leaf, 11);
        assert_eq!(header.min_size(), 5); // floor(11/2)
    }

    #[test]
    fn test_node_header_is_safe() {
        let mut header = NodeHeader::new(NodeType::Internal, 10);
        header.size = 5;
        assert!(header.is_safe_for_insert()); // 5 < 10 - 1 = 9
        assert!(!header.is_safe_for_delete()); // 5 == 5

        header.size = 9;
        assert!(!header.is_safe_for_insert()); // 9 >= 9
        assert!(header.is_safe_for_delete()); // 9 > 5
    }

    // ========================================================================
    // InternalNode tests
    // ========================================================================

    #[test]
    fn test_internal_node_new() {
        let node = InternalNode::new(10);
        assert_eq!(node.header.node_type, NodeType::Internal);
        assert_eq!(node.header.max_size, 10);
        assert!(node.keys.is_empty());
        assert!(node.children.is_empty());
    }

    #[test]
    fn test_internal_node_init_with_child() {
        let mut node = InternalNode::new(10);
        node.init_with_child(PageId::new(42));

        assert_eq!(node.header.size, 1);
        assert_eq!(node.keys.len(), 1);
        assert_eq!(node.children.len(), 1);
        assert_eq!(node.child_at(0), PageId::new(42));
    }

    #[test]
    fn test_internal_node_find_child_index() {
        let mut node = InternalNode::new(10);
        node.keys = vec![
            vec![],   // index 0: invalid
            vec![5],  // index 1: separator
            vec![10], // index 2: separator
            vec![15], // index 3: separator
        ];
        node.children = vec![
            PageId::new(0), // < 5
            PageId::new(1), // 5 <= x < 10
            PageId::new(2), // 10 <= x < 15
            PageId::new(3), // >= 15
        ];
        node.header.size = 4;

        // Keys less than 5 go to child 0
        assert_eq!(node.find_child_index(&[3]), 0);
        assert_eq!(node.find_child_index(&[4]), 0);

        // Keys 5-9 go to child 1
        assert_eq!(node.find_child_index(&[5]), 1);
        assert_eq!(node.find_child_index(&[9]), 1);

        // Keys 10-14 go to child 2
        assert_eq!(node.find_child_index(&[10]), 2);
        assert_eq!(node.find_child_index(&[14]), 2);

        // Keys >= 15 go to child 3
        assert_eq!(node.find_child_index(&[15]), 3);
        assert_eq!(node.find_child_index(&[100]), 3);
    }

    #[test]
    fn test_internal_node_insert_separator() {
        let mut node = InternalNode::new(10);
        node.init_with_child(PageId::new(0));

        // Insert separator 10, pointing to child 1
        node.insert_separator(vec![10], PageId::new(1));

        assert_eq!(node.header.size, 2);
        assert_eq!(node.keys.len(), 2);
        assert_eq!(node.children.len(), 2);
        assert_eq!(node.key_at(1), &[10]);
        assert_eq!(node.child_at(1), PageId::new(1));

        // Insert separator 5 (should go before 10)
        node.insert_separator(vec![5], PageId::new(2));

        assert_eq!(node.header.size, 3);
        assert_eq!(node.key_at(1), &[5]);
        assert_eq!(node.key_at(2), &[10]);
    }

    // ========================================================================
    // LeafNode tests
    // ========================================================================

    #[test]
    fn test_leaf_node_new() {
        let node = LeafNode::new(10);
        assert_eq!(node.header.node_type, NodeType::Leaf);
        assert_eq!(node.header.max_size, 10);
        assert_eq!(node.next_page_id, PageId::INVALID);
        assert_eq!(node.prev_page_id, PageId::INVALID);
        assert!(node.keys.is_empty());
        assert!(node.values.is_empty());
        assert!(node.tombstones.is_empty());
    }

    #[test]
    fn test_leaf_node_insert() {
        let mut node = LeafNode::new(10);

        assert!(node.insert(vec![5], vec![50]));
        assert!(node.insert(vec![3], vec![30]));
        assert!(node.insert(vec![7], vec![70]));

        // Keys should be sorted
        assert_eq!(node.key_at(0), &[3]);
        assert_eq!(node.key_at(1), &[5]);
        assert_eq!(node.key_at(2), &[7]);

        // Values should correspond
        assert_eq!(node.value_at(0), &[30]);
        assert_eq!(node.value_at(1), &[50]);
        assert_eq!(node.value_at(2), &[70]);
    }

    #[test]
    fn test_leaf_node_insert_duplicate() {
        let mut node = LeafNode::new(10);

        assert!(node.insert(vec![5], vec![50]));
        assert!(!node.insert(vec![5], vec![55])); // Duplicate

        assert_eq!(node.physical_size(), 1);
        assert_eq!(node.value_at(0), &[50]); // Original value unchanged
    }

    #[test]
    fn test_leaf_node_lookup() {
        let mut node = LeafNode::new(10);
        node.insert(vec![3], vec![30]);
        node.insert(vec![5], vec![50]);
        node.insert(vec![7], vec![70]);

        assert_eq!(node.lookup(&[5]), Some(1));
        assert_eq!(node.lookup(&[6]), None);
    }

    #[test]
    fn test_leaf_node_delete_tombstone() {
        let mut node = LeafNode::new(10);
        node.insert(vec![3], vec![30]);
        node.insert(vec![5], vec![50]);
        node.insert(vec![7], vec![70]);

        assert!(node.delete(&[5], MAX_TOMBSTONES));

        // Logical size decreases, physical stays same
        assert_eq!(node.logical_size(), 2);
        assert_eq!(node.physical_size(), 3);
        assert!(node.is_tombstone(1));

        // Lookup should not find tombstoned key
        assert_eq!(node.lookup(&[5]), None);
        // But physical lookup should
        assert_eq!(node.lookup_physical(&[5]), Some(1));
    }

    #[test]
    fn test_leaf_node_revive_tombstoned() {
        let mut node = LeafNode::new(10);
        node.insert(vec![5], vec![50]);
        node.delete(&[5], MAX_TOMBSTONES);

        // Insert same key - should revive
        assert!(node.insert(vec![5], vec![55]));

        assert_eq!(node.physical_size(), 1);
        assert_eq!(node.tombstone_count(), 0);
        assert_eq!(node.value_at(0), &[55]); // New value
    }

    #[test]
    fn test_leaf_node_compact() {
        let mut node = LeafNode::new(10);
        node.insert(vec![1], vec![10]);
        node.insert(vec![2], vec![20]);
        node.insert(vec![3], vec![30]);
        node.insert(vec![4], vec![40]);

        node.delete(&[2], MAX_TOMBSTONES);
        node.delete(&[4], MAX_TOMBSTONES);

        assert_eq!(node.physical_size(), 4);
        assert_eq!(node.logical_size(), 2);

        node.compact();

        assert_eq!(node.physical_size(), 2);
        assert_eq!(node.logical_size(), 2);
        assert_eq!(node.tombstone_count(), 0);

        // Remaining keys should be 1 and 3
        assert_eq!(node.key_at(0), &[1]);
        assert_eq!(node.key_at(1), &[3]);
    }

    #[test]
    fn test_leaf_node_tombstone_buffer_overflow() {
        let mut node = LeafNode::new(20);

        // Insert more than MAX_TOMBSTONES entries
        for i in 0..(MAX_TOMBSTONES + 3) {
            node.insert(vec![i as u8], vec![i as u8 * 10]);
        }

        // Delete MAX_TOMBSTONES entries
        for i in 0..MAX_TOMBSTONES {
            assert!(node.delete(&[i as u8], MAX_TOMBSTONES));
        }

        assert_eq!(node.tombstone_count(), MAX_TOMBSTONES);

        // Delete one more - should evict oldest tombstone
        assert!(node.delete(&[MAX_TOMBSTONES as u8], MAX_TOMBSTONES));

        // Tombstone count stays at MAX
        assert!(node.tombstone_count() <= MAX_TOMBSTONES);
    }

    #[test]
    fn test_leaf_node_live_entries() {
        let mut node = LeafNode::new(10);
        node.insert(vec![1], vec![10]);
        node.insert(vec![2], vec![20]);
        node.insert(vec![3], vec![30]);
        node.delete(&[2], MAX_TOMBSTONES);

        let live = node.live_entries();
        assert_eq!(live.len(), 2);
        assert_eq!(live[0], (&[1u8][..], &[10u8][..]));
        assert_eq!(live[1], (&[3u8][..], &[30u8][..]));
    }

    #[test]
    fn test_leaf_node_sizes() {
        let mut node = LeafNode::new(10);
        node.insert(vec![1], vec![10]);
        node.insert(vec![2], vec![20]);
        node.insert(vec![3], vec![30]);
        node.delete(&[2], MAX_TOMBSTONES);

        assert_eq!(node.size(), 2); // logical
        assert_eq!(node.physical_size(), 3);
        assert_eq!(node.logical_size(), 2);
        assert_eq!(node.min_size(), 5); // floor(10/2)
        assert!(node.has_underflowed()); // 2 < 5
    }
}
