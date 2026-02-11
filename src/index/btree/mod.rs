//! B+Tree index implementation.
//!
//! A full-featured B+tree with:
//! - Variable-length keys and values
//! - Concurrency via latch crabbing (optimistic + pessimistic paths)
//! - Full rebalancing (merge/redistribute on underflow)
//! - Tombstone-based deletion with configurable buffer
//! - Range scans via leaf linked list
//!
//! ## Architecture
//!
//! ```text
//!                    ┌─────────────────┐
//!                    │  Header Page    │
//!                    │  (root_page_id) │
//!                    └────────┬────────┘
//!                             │
//!                    ┌────────▼────────┐
//!                    │  Internal Node  │
//!                    │ [k1|k2|k3|...]  │
//!                    │ [p0|p1|p2|p3]   │
//!                    └─┬──┬──┬──┬──────┘
//!              ┌───────┘  │  │  └───────┐
//!              ▼          ▼  ▼          ▼
//!         ┌────────┐  ┌────────┐  ┌────────┐
//!         │  Leaf  │──│  Leaf  │──│  Leaf  │──► (linked list)
//!         │ [k,v]  │  │ [k,v]  │  │ [k,v]  │
//!         └────────┘  └────────┘  └────────┘
//! ```
//!
//! ## Concurrency Model
//!
//! Uses latch crabbing (lock coupling):
//! 1. Acquire lock on child before releasing parent
//! 2. If child is "safe" (won't split/merge), release all ancestors
//! 3. Optimistic path: read latches down, upgrade at leaf
//! 4. Pessimistic path: write latches with early release when safe
//!
//! ## Page Layout
//!
//! ```text
//! Internal Node:
//! ┌──────────────────────────────────────────────────┐
//! │ Header (12 bytes)                                │
//! │ ├─ node_type: u8                                 │
//! │ ├─ size: u16                                     │
//! │ ├─ max_size: u16                                 │
//! │ ├─ _reserved: [u8; 7]                            │
//! ├──────────────────────────────────────────────────┤
//! │ Keys: [len:u16 | key_bytes...]...                │
//! ├──────────────────────────────────────────────────┤
//! │ Children: [PageId; size+1]                       │
//! └──────────────────────────────────────────────────┘
//!
//! Leaf Node:
//! ┌──────────────────────────────────────────────────┐
//! │ Header (20 bytes)                                │
//! │ ├─ node_type: u8                                 │
//! │ ├─ size: u16                                     │
//! │ ├─ max_size: u16                                 │
//! │ ├─ next_page_id: u32                             │
//! │ ├─ prev_page_id: u32                             │
//! │ ├─ tombstone_count: u16                          │
//! │ ├─ _reserved: [u8; 5]                            │
//! ├──────────────────────────────────────────────────┤
//! │ Tombstone indices: [u16; tombstone_count]        │
//! ├──────────────────────────────────────────────────┤
//! │ Entries: [key_len:u16 | key | val_len:u16 | val] │
//! └──────────────────────────────────────────────────┘
//! ```
//!
//! ## References
//!
//! - Comer, "The Ubiquitous B-Tree" (1979)
//! - BusTub CMU 15-445 B+Tree implementation

pub mod engine;
mod context;
mod iterator;
mod node;
mod page_layout;
mod tree;

pub use node::{
    InternalNode, LeafNode, NodeHeader, NodeType,
    INTERNAL_HEADER_SIZE, LEAF_HEADER_SIZE, MAX_TOMBSTONES,
};
pub use page_layout::{
    decode_internal_node, decode_leaf_node,
    encode_internal_node, encode_leaf_node,
};
pub use iterator::BTreeScanIterator;
pub use engine::BTreeEngine;
pub use tree::BTree;

use crate::common::PageId;

/// B+Tree header page - stores only the root page ID.
///
/// This is a separate page to enable safe concurrent root changes.
/// When the root splits, we can update this atomically while holding
/// only the header page latch.
#[derive(Debug, Clone, Copy)]
pub struct BTreeHeaderPage {
    /// Page ID of the root node, or INVALID if tree is empty.
    pub root_page_id: PageId,
}

impl BTreeHeaderPage {
    /// Size of the header page data in bytes.
    pub const SIZE: usize = 4;

    /// Create a new header page with no root (empty tree).
    pub fn new() -> Self {
        Self {
            root_page_id: PageId::INVALID,
        }
    }

    /// Check if the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.root_page_id == PageId::INVALID
    }

    /// Encode header page to bytes.
    pub fn encode(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.root_page_id.0.to_le_bytes());
    }

    /// Decode header page from bytes.
    pub fn decode(buf: &[u8]) -> Self {
        let root_page_id = PageId::new(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]));
        Self { root_page_id }
    }
}

impl Default for BTreeHeaderPage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_page_new() {
        let header = BTreeHeaderPage::new();
        assert!(header.is_empty());
        assert_eq!(header.root_page_id, PageId::INVALID);
    }

    #[test]
    fn test_header_page_encode_decode() {
        let mut header = BTreeHeaderPage::new();
        header.root_page_id = PageId::new(42);

        let mut buf = [0u8; BTreeHeaderPage::SIZE];
        header.encode(&mut buf);

        let decoded = BTreeHeaderPage::decode(&buf);
        assert_eq!(decoded.root_page_id, PageId::new(42));
        assert!(!decoded.is_empty());
    }

    #[test]
    fn test_header_page_empty_roundtrip() {
        let header = BTreeHeaderPage::new();
        let mut buf = [0u8; BTreeHeaderPage::SIZE];
        header.encode(&mut buf);

        let decoded = BTreeHeaderPage::decode(&buf);
        assert!(decoded.is_empty());
    }
}
