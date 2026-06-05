//! B+Tree node serialization to/from page bytes.
//!
//! Handles encoding nodes to fit within a 4KB page and decoding them back.
//! Every page starts with a 13-byte [`PageHeader`] prefix (page_type, checksum, LSN).
//!
//! ## Internal Node Layout
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//! 0       13    PageHeader (page_type=BTreeInternal, checksum, lsn)
//! 13      1     node_type (0 = Internal)
//! 14      2     size (number of entries)
//! 16      2     max_size
//! 18      7     reserved
//! 25      var   entries: [key_len:u16, key_bytes..., child:u32]...
//! ```
//!
//! ## Leaf Node Layout
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//! 0       13    PageHeader (page_type=BTreeLeaf, checksum, lsn)
//! 13      1     node_type (1 = Leaf)
//! 14      2     size (number of entries)
//! 16      2     max_size
//! 18      4     next_page_id
//! 22      4     prev_page_id
//! 26      2     tombstone_count
//! 28      5     reserved
//! 33      var   tombstones: [u16; tombstone_count]
//! var     var   entries: [key_len:u16, key_bytes..., val_len:u16, val_bytes...]...
//! ```

use crate::common::config::PAGE_SIZE;
use crate::common::PageId;
use crate::storage::page::{PageHeader, PageType};

use super::node::{
    InternalNode, LeafNode, NodeType,
    INTERNAL_HEADER_SIZE, LEAF_HEADER_SIZE, MAX_TOMBSTONES,
};

/// Encode an internal node to a page buffer.
///
/// Returns the number of bytes written.
pub fn encode_internal_node(node: &InternalNode, buf: &mut [u8]) -> usize {
    assert!(buf.len() >= PAGE_SIZE);

    // Write PageHeader prefix.
    let page_header = PageHeader::new(PageType::BTreeInternal);
    page_header.write_to(buf);

    let mut offset = PageHeader::SIZE;

    // Node header
    buf[offset] = NodeType::Internal as u8;
    offset += 1;

    buf[offset..offset + 2].copy_from_slice(&node.header.size.to_le_bytes());
    offset += 2;

    buf[offset..offset + 2].copy_from_slice(&node.header.max_size.to_le_bytes());
    offset += 2;

    // Reserved (7 bytes)
    buf[offset..offset + 7].fill(0);
    offset = INTERNAL_HEADER_SIZE;

    // Entries: key_len, key, child_page_id
    for i in 0..node.keys.len() {
        let key = &node.keys[i];
        let child = node.children[i];

        // Key length (u16)
        let key_len = key.len() as u16;
        buf[offset..offset + 2].copy_from_slice(&key_len.to_le_bytes());
        offset += 2;

        // Key bytes
        buf[offset..offset + key.len()].copy_from_slice(key);
        offset += key.len();

        // Child page ID (u32)
        buf[offset..offset + 4].copy_from_slice(&child.0.to_le_bytes());
        offset += 4;
    }

    // Compute and store checksum over the full page buffer.
    let checksum = PageHeader::compute_checksum(buf);
    buf[PageHeader::OFFSET_CHECKSUM..PageHeader::OFFSET_CHECKSUM + 4]
        .copy_from_slice(&checksum.to_le_bytes());

    offset
}

/// Decode an internal node from a page buffer.
pub fn decode_internal_node(buf: &[u8]) -> InternalNode {
    // Verify page checksum if one has been stored (non-zero).
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
            "internal node page checksum mismatch"
        );
    }

    // Skip PageHeader prefix.
    let mut offset = PageHeader::SIZE;

    // Verify node type
    let node_type = buf[offset];
    assert_eq!(node_type, NodeType::Internal as u8);
    offset += 1;

    // Size
    let size = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
    offset += 2;

    // Max size
    let max_size = u16::from_le_bytes([buf[offset], buf[offset + 1]]);

    // Skip reserved, jump to entries
    let mut offset = INTERNAL_HEADER_SIZE;

    // Read entries
    let mut keys = Vec::with_capacity(size as usize);
    let mut children = Vec::with_capacity(size as usize);

    for _ in 0..size {
        // Key length
        let key_len = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        // Key bytes
        let key = buf[offset..offset + key_len].to_vec();
        offset += key_len;

        // Child page ID
        let child = PageId::new(u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]));
        offset += 4;

        keys.push(key);
        children.push(child);
    }

    let mut node = InternalNode::new(max_size);
    node.header.size = size;
    node.keys = keys;
    node.children = children;
    node
}

/// Point lookup directly on an encoded leaf page — without decoding it.
///
/// `decode_leaf_node` materializes the whole leaf (a `Vec` per key AND per
/// value — up to ~400 heap allocations for a full leaf) just so the caller
/// can read one value. This walks the encoded entries in place and returns
/// the matching value as a slice into `buf`, so the only allocation is the
/// caller's final copy of the one value it wants. Keys are stored sorted,
/// so the scan early-exits once it passes the target.
///
/// Returns `None` if the key is absent or tombstoned — identical semantics
/// to `decode_leaf_node(buf).lookup(key)`. The page checksum is still
/// verified, matching the decode path's safety check.
pub fn lookup_in_encoded_leaf<'a>(buf: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    // Verify page checksum (parity with `decode_leaf_node`).
    let stored_checksum = u32::from_le_bytes([
        buf[PageHeader::OFFSET_CHECKSUM],
        buf[PageHeader::OFFSET_CHECKSUM + 1],
        buf[PageHeader::OFFSET_CHECKSUM + 2],
        buf[PageHeader::OFFSET_CHECKSUM + 3],
    ]);
    if stored_checksum != 0 {
        assert_eq!(
            stored_checksum,
            PageHeader::compute_checksum(buf),
            "leaf node page checksum mismatch"
        );
    }
    debug_assert_eq!(buf[PageHeader::SIZE], NodeType::Leaf as u8);

    // `size` follows the 1-byte node_type; `tombstone_count` follows
    // size + max_size + next + prev (see `decode_leaf_node`).
    let size = u16::from_le_bytes([buf[PageHeader::SIZE + 1], buf[PageHeader::SIZE + 2]]) as usize;
    let tombstone_count_offset = PageHeader::SIZE + 1 + 2 + 2 + 4 + 4;
    let tombstone_count =
        u16::from_le_bytes([buf[tombstone_count_offset], buf[tombstone_count_offset + 1]]) as usize;

    let tombstones_offset = LEAF_HEADER_SIZE;
    let mut offset = tombstones_offset + tombstone_count * 2; // entries follow the tombstone indices

    for physical_index in 0..size {
        let key_len = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
        let key_start = offset + 2;
        let entry_key = &buf[key_start..key_start + key_len];
        let val_len_offset = key_start + key_len;
        let val_len = u16::from_le_bytes([buf[val_len_offset], buf[val_len_offset + 1]]) as usize;
        let val_start = val_len_offset + 2;

        match entry_key.cmp(key) {
            std::cmp::Ordering::Equal => {
                if is_tombstoned(buf, tombstones_offset, tombstone_count, physical_index) {
                    return None; // physically present but deleted
                }
                return Some(&buf[val_start..val_start + val_len]);
            }
            // Keys are sorted ascending — once we pass the target it's absent.
            std::cmp::Ordering::Greater => return None,
            std::cmp::Ordering::Less => offset = val_start + val_len,
        }
    }
    None
}

/// Whether `physical_index` is in the leaf's tombstone-index region. Scans
/// in place; the region is small (usually empty → no work).
fn is_tombstoned(
    buf: &[u8],
    tombstones_offset: usize,
    tombstone_count: usize,
    physical_index: usize,
) -> bool {
    let target = physical_index as u16;
    (0..tombstone_count).any(|i| {
        let off = tombstones_offset + i * 2;
        u16::from_le_bytes([buf[off], buf[off + 1]]) == target
    })
}

/// Encode a leaf node to a page buffer.
///
/// Returns the number of bytes written.
pub fn encode_leaf_node(node: &LeafNode, buf: &mut [u8]) -> usize {
    assert!(buf.len() >= PAGE_SIZE);

    // Write PageHeader prefix.
    let page_header = PageHeader::new(PageType::BTreeLeaf);
    page_header.write_to(buf);

    let mut offset = PageHeader::SIZE;

    // Node header
    buf[offset] = NodeType::Leaf as u8;
    offset += 1;

    // Size (physical size for encoding)
    let size = node.keys.len() as u16;
    buf[offset..offset + 2].copy_from_slice(&size.to_le_bytes());
    offset += 2;

    buf[offset..offset + 2].copy_from_slice(&node.header.max_size.to_le_bytes());
    offset += 2;

    buf[offset..offset + 4].copy_from_slice(&node.next_page_id.0.to_le_bytes());
    offset += 4;

    buf[offset..offset + 4].copy_from_slice(&node.prev_page_id.0.to_le_bytes());
    offset += 4;

    // Tombstone count
    let tombstone_count = node.tombstones.len().min(MAX_TOMBSTONES) as u16;
    buf[offset..offset + 2].copy_from_slice(&tombstone_count.to_le_bytes());
    offset += 2;

    // Reserved (5 bytes)
    buf[offset..offset + 5].fill(0);
    offset = LEAF_HEADER_SIZE;

    // Tombstone indices
    for i in 0..tombstone_count as usize {
        buf[offset..offset + 2].copy_from_slice(&node.tombstones[i].to_le_bytes());
        offset += 2;
    }

    // Entries: key_len, key, val_len, val
    for i in 0..node.keys.len() {
        let key = &node.keys[i];
        let value = &node.values[i];

        // Key length (u16)
        let key_len = key.len() as u16;
        buf[offset..offset + 2].copy_from_slice(&key_len.to_le_bytes());
        offset += 2;

        // Key bytes
        buf[offset..offset + key.len()].copy_from_slice(key);
        offset += key.len();

        // Value length (u16)
        let val_len = value.len() as u16;
        buf[offset..offset + 2].copy_from_slice(&val_len.to_le_bytes());
        offset += 2;

        // Value bytes
        buf[offset..offset + value.len()].copy_from_slice(value);
        offset += value.len();
    }

    // Compute and store checksum over the full page buffer.
    let checksum = PageHeader::compute_checksum(buf);
    buf[PageHeader::OFFSET_CHECKSUM..PageHeader::OFFSET_CHECKSUM + 4]
        .copy_from_slice(&checksum.to_le_bytes());

    offset
}

/// Decode a leaf node from a page buffer.
pub fn decode_leaf_node(buf: &[u8]) -> LeafNode {
    // Verify page checksum if one has been stored (non-zero).
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
            "leaf node page checksum mismatch"
        );
    }

    // Skip PageHeader prefix.
    let mut offset = PageHeader::SIZE;

    // Verify node type
    let node_type = buf[offset];
    assert_eq!(node_type, NodeType::Leaf as u8);
    offset += 1;

    // Size
    let size = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
    offset += 2;

    // Max size
    let max_size = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
    offset += 2;

    // Next page ID
    let next_page_id = PageId::new(u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]));
    offset += 4;

    // Prev page ID
    let prev_page_id = PageId::new(u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]));
    offset += 4;

    // Tombstone count
    let tombstone_count = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;

    // Skip reserved, jump to tombstone indices
    let mut offset = LEAF_HEADER_SIZE;

    // Read tombstones
    let mut tombstones = Vec::with_capacity(tombstone_count);
    for _ in 0..tombstone_count {
        let idx = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
        tombstones.push(idx);
        offset += 2;
    }

    // Read entries
    let mut keys = Vec::with_capacity(size as usize);
    let mut values = Vec::with_capacity(size as usize);

    for _ in 0..size {
        // Key length
        let key_len = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        // Key bytes
        let key = buf[offset..offset + key_len].to_vec();
        offset += key_len;

        // Value length
        let val_len = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
        offset += 2;

        // Value bytes
        let value = buf[offset..offset + val_len].to_vec();
        offset += val_len;

        keys.push(key);
        values.push(value);
    }

    let mut node = LeafNode::new(max_size);
    node.header.size = size;
    node.next_page_id = next_page_id;
    node.prev_page_id = prev_page_id;
    node.keys = keys;
    node.values = values;
    node.tombstones = tombstones;
    node
}

/// Calculate maximum entries for a leaf node given average key and value sizes.
///
/// Used to determine `max_size` when creating nodes.
pub fn calculate_leaf_max_size(avg_key_size: usize, avg_value_size: usize) -> u16 {
    // Available space = PAGE_SIZE - header - max tombstones
    let tombstone_space = MAX_TOMBSTONES * 2; // Each tombstone is u16
    let available = PAGE_SIZE - LEAF_HEADER_SIZE - tombstone_space;

    // Each entry uses: key_len(2) + key + val_len(2) + val
    let entry_size = 2 + avg_key_size + 2 + avg_value_size;

    (available / entry_size) as u16
}

/// Calculate maximum entries for an internal node given average key size.
pub fn calculate_internal_max_size(avg_key_size: usize) -> u16 {
    let available = PAGE_SIZE - INTERNAL_HEADER_SIZE;

    // Each entry uses: key_len(2) + key + child_page_id(4)
    let entry_size = 2 + avg_key_size + 4;

    (available / entry_size) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_node_roundtrip() {
        let mut node = InternalNode::new(100);
        node.init_with_child(PageId::new(10));
        node.insert_separator(vec![5, 6, 7], PageId::new(20));
        node.insert_separator(vec![10, 11], PageId::new(30));

        let mut buf = vec![0u8; PAGE_SIZE];
        let written = encode_internal_node(&node, &mut buf);
        assert!(written > INTERNAL_HEADER_SIZE);

        let decoded = decode_internal_node(&buf);

        assert_eq!(decoded.header.size, node.header.size);
        assert_eq!(decoded.header.max_size, node.header.max_size);
        assert_eq!(decoded.keys.len(), node.keys.len());
        assert_eq!(decoded.children.len(), node.children.len());

        for i in 0..node.keys.len() {
            assert_eq!(decoded.keys[i], node.keys[i]);
            assert_eq!(decoded.children[i], node.children[i]);
        }
    }

    #[test]
    fn test_leaf_node_roundtrip() {
        let mut node = LeafNode::new(100);
        node.insert(vec![1, 2], vec![10, 20, 30]);
        node.insert(vec![3, 4, 5], vec![40, 50]);
        node.insert(vec![6], vec![60, 70, 80, 90]);
        node.next_page_id = PageId::new(42);
        node.prev_page_id = PageId::new(41);

        // Add a tombstone
        node.delete(&[3, 4, 5], 8);

        let mut buf = vec![0u8; PAGE_SIZE];
        let written = encode_leaf_node(&node, &mut buf);
        assert!(written > LEAF_HEADER_SIZE);

        let decoded = decode_leaf_node(&buf);

        assert_eq!(decoded.header.max_size, node.header.max_size);
        assert_eq!(decoded.next_page_id, node.next_page_id);
        assert_eq!(decoded.prev_page_id, node.prev_page_id);
        assert_eq!(decoded.keys.len(), node.keys.len());
        assert_eq!(decoded.values.len(), node.values.len());
        assert_eq!(decoded.tombstones.len(), node.tombstones.len());

        for i in 0..node.keys.len() {
            assert_eq!(decoded.keys[i], node.keys[i]);
            assert_eq!(decoded.values[i], node.values[i]);
        }

        for i in 0..node.tombstones.len() {
            assert_eq!(decoded.tombstones[i], node.tombstones[i]);
        }
    }

    #[test]
    fn lookup_in_encoded_leaf_matches_decode() {
        // The fast in-place lookup must return exactly what
        // `decode_leaf_node(buf).lookup(key)` would — including tombstone
        // semantics and absent keys before/between/after the entries.
        let mut node = LeafNode::new(100);
        node.insert(vec![1, 2], vec![10, 20, 30]);
        node.insert(vec![3, 4, 5], vec![40, 50]);
        node.insert(vec![6], vec![60, 70, 80, 90]);
        node.delete(&[3, 4, 5], 8); // tombstone the middle key

        let mut buf = vec![0u8; PAGE_SIZE];
        encode_leaf_node(&node, &mut buf);

        let expected = |k: &[u8]| -> Option<Vec<u8>> {
            let leaf = decode_leaf_node(&buf);
            leaf.lookup(k).map(|i| leaf.value_at(i).to_vec())
        };

        for probe in [
            &[1u8, 2][..], // present, first
            &[6][..],      // present, last
            &[3, 4, 5][..], // physically present but tombstoned → None
            &[0][..],      // before all keys
            &[2][..],      // between keys (absent)
            &[9][..],      // after all keys
        ] {
            assert_eq!(
                lookup_in_encoded_leaf(&buf, probe).map(|v| v.to_vec()),
                expected(probe),
                "mismatch for {:?}",
                probe
            );
        }
    }

    #[test]
    fn lookup_in_encoded_leaf_empty() {
        let node = LeafNode::new(100);
        let mut buf = vec![0u8; PAGE_SIZE];
        encode_leaf_node(&node, &mut buf);
        assert_eq!(lookup_in_encoded_leaf(&buf, &[1]), None);
    }

    #[test]
    fn test_calculate_leaf_max_size() {
        // With 8-byte keys and 8-byte values
        let max_size = calculate_leaf_max_size(8, 8);

        // Each entry: 2 + 8 + 2 + 8 = 20 bytes
        // Available: 4096 - 33 - 16 = 4047 bytes
        // Expected: 4047 / 20 = 202 entries
        assert!(max_size > 100);
        assert!(max_size < 300);
    }

    #[test]
    fn test_calculate_internal_max_size() {
        // With 8-byte keys
        let max_size = calculate_internal_max_size(8);

        // Each entry: 2 + 8 + 4 = 14 bytes
        // Available: 4096 - 25 = 4071 bytes
        // Expected: 4071 / 14 = 290 entries
        assert!(max_size > 200);
        assert!(max_size < 400);
    }

    #[test]
    fn test_empty_internal_node_roundtrip() {
        let node = InternalNode::new(50);

        let mut buf = vec![0u8; PAGE_SIZE];
        encode_internal_node(&node, &mut buf);

        let decoded = decode_internal_node(&buf);

        assert_eq!(decoded.header.size, 0);
        assert_eq!(decoded.header.max_size, 50);
        assert!(decoded.keys.is_empty());
        assert!(decoded.children.is_empty());
    }

    #[test]
    fn test_empty_leaf_node_roundtrip() {
        let node = LeafNode::new(50);

        let mut buf = vec![0u8; PAGE_SIZE];
        encode_leaf_node(&node, &mut buf);

        let decoded = decode_leaf_node(&buf);

        assert_eq!(decoded.header.size, 0);
        assert_eq!(decoded.header.max_size, 50);
        assert!(decoded.keys.is_empty());
        assert!(decoded.values.is_empty());
        assert!(decoded.tombstones.is_empty());
    }

    #[test]
    fn test_leaf_with_many_tombstones() {
        let mut node = LeafNode::new(100);

        // Insert many entries
        for i in 0..20u8 {
            node.insert(vec![i], vec![i * 10]);
        }

        // Delete several (creating tombstones)
        for i in 0..8u8 {
            node.delete(&[i], 8);
        }

        assert_eq!(node.tombstone_count(), 8);

        let mut buf = vec![0u8; PAGE_SIZE];
        encode_leaf_node(&node, &mut buf);

        let decoded = decode_leaf_node(&buf);

        assert_eq!(decoded.tombstone_count(), 8);
        assert_eq!(decoded.keys.len(), 20);

        // Verify tombstoned entries are marked correctly
        for i in 0..8usize {
            assert!(decoded.is_tombstone(i));
        }
        for i in 8..20usize {
            assert!(!decoded.is_tombstone(i));
        }
    }
}
