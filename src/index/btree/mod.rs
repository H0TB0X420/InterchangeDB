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
//! Every page starts with a 13-byte `PageHeader` prefix (page_type, checksum, LSN).
//!
//! ```text
//! Internal Node (25-byte header):
//! ┌──────────────────────────────────────────────────┐
//! │ PageHeader (13 bytes)                            │
//! │ ├─ page_type: u8 (BTreeInternal=2)               │
//! │ ├─ checksum: u32                                 │
//! │ ├─ lsn: u64                                      │
//! ├──────────────────────────────────────────────────┤
//! │ Node Header (12 bytes)                           │
//! │ ├─ node_type: u8                                 │
//! │ ├─ size: u16                                     │
//! │ ├─ max_size: u16                                 │
//! │ ├─ _reserved: [u8; 7]                            │
//! ├──────────────────────────────────────────────────┤
//! │ Entries: [key_len:u16 | key | child:u32]...      │
//! └──────────────────────────────────────────────────┘
//!
//! Leaf Node (33-byte header):
//! ┌──────────────────────────────────────────────────┐
//! │ PageHeader (13 bytes)                            │
//! │ ├─ page_type: u8 (BTreeLeaf=3)                   │
//! │ ├─ checksum: u32                                 │
//! │ ├─ lsn: u64                                      │
//! ├──────────────────────────────────────────────────┤
//! │ Node Header (20 bytes)                           │
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
use crate::storage::page::{PageHeader, PageType};

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
    /// Size of the header page data in bytes (PageHeader + root_page_id).
    pub const SIZE: usize = PageHeader::SIZE + 4;

    /// Offset of root_page_id within the page (after PageHeader prefix).
    const ROOT_OFFSET: usize = PageHeader::SIZE;

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
        let page_header = PageHeader::new(PageType::Data);
        page_header.write_to(buf);
        buf[Self::ROOT_OFFSET..Self::ROOT_OFFSET + 4]
            .copy_from_slice(&self.root_page_id.0.to_le_bytes());

        // Compute and store checksum if this is a full page buffer.
        if buf.len() >= crate::common::config::PAGE_SIZE {
            let checksum = PageHeader::compute_checksum(buf);
            buf[PageHeader::OFFSET_CHECKSUM..PageHeader::OFFSET_CHECKSUM + 4]
                .copy_from_slice(&checksum.to_le_bytes());
        }
    }

    /// Decode header page from bytes.
    pub fn decode(buf: &[u8]) -> Self {
        // Verify checksum if this is a full page buffer with a stored checksum.
        if buf.len() >= crate::common::config::PAGE_SIZE {
            let stored_checksum = u32::from_le_bytes([
                buf[PageHeader::OFFSET_CHECKSUM],
                buf[PageHeader::OFFSET_CHECKSUM + 1],
                buf[PageHeader::OFFSET_CHECKSUM + 2],
                buf[PageHeader::OFFSET_CHECKSUM + 3],
            ]);
            if stored_checksum != 0 {
                let computed = PageHeader::compute_checksum(buf);
                assert_eq!(
                    stored_checksum, computed,
                    "header page checksum mismatch"
                );
            }
        }

        let root_page_id = PageId::new(u32::from_le_bytes([
            buf[Self::ROOT_OFFSET],
            buf[Self::ROOT_OFFSET + 1],
            buf[Self::ROOT_OFFSET + 2],
            buf[Self::ROOT_OFFSET + 3],
        ]));
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

        let mut buf = vec![0u8; BTreeHeaderPage::SIZE];
        header.encode(&mut buf);

        let decoded = BTreeHeaderPage::decode(&buf);
        assert_eq!(decoded.root_page_id, PageId::new(42));
        assert!(!decoded.is_empty());
    }

    #[test]
    fn test_header_page_empty_roundtrip() {
        let header = BTreeHeaderPage::new();
        let mut buf = vec![0u8; BTreeHeaderPage::SIZE];
        header.encode(&mut buf);

        let decoded = BTreeHeaderPage::decode(&buf);
        assert!(decoded.is_empty());
    }
}
