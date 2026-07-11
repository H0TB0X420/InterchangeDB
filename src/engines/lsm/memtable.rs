//! In-memory sorted map for the LSM-tree.
//!
//! The memtable is a `BTreeMap` that stores key-value pairs in sorted order.
//! Values are `Option<Vec<u8>>` where `None` represents a tombstone (deletion marker).
//! Size tracking is approximate — it exists to trigger flush at the right time.

use std::collections::BTreeMap;
use std::ops::RangeBounds;

/// Per-entry overhead estimate for BTreeMap node bookkeeping.
const ENTRY_OVERHEAD_BYTES: usize = 32;

/// In-memory sorted map backed by a `BTreeMap`.
///
/// Entries map `Vec<u8>` keys to `Option<Vec<u8>>` values, where `None`
/// indicates a tombstone. Size tracking is approximate and used only
/// to decide when the memtable should be flushed to an SSTable.
pub struct Memtable {
    entries: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    size_bytes: usize,
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable {
    /// Create an empty memtable.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    /// Insert or overwrite a key-value pair.
    ///
    /// If the key already exists, the old value is replaced and the size
    /// estimate is adjusted accordingly.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let new_value_size = value.len();

        if let Some(old) = self.entries.insert(key.clone(), Some(value)) {
            // Overwrite — adjust for the old value's size.
            let old_size = old.as_ref().map_or(0, |v| v.len());
            // Remove old value size, add new value size.
            self.size_bytes = self.size_bytes - old_size + new_value_size;
        } else {
            // New entry — add key + value + overhead.
            self.size_bytes += key.len() + new_value_size + ENTRY_OVERHEAD_BYTES;
        }
    }

    /// Insert a tombstone for the given key.
    ///
    /// If the key already has a value, the size estimate is reduced by
    /// the old value's size. A tombstone is always written regardless.
    pub fn delete(&mut self, key: Vec<u8>) {
        if let Some(old) = self.entries.insert(key.clone(), None) {
            // Overwrite — remove old value's size contribution.
            let old_size = old.as_ref().map_or(0, |v| v.len());
            self.size_bytes -= old_size;
        } else {
            // New entry (tombstone) — key + overhead, no value.
            self.size_bytes += key.len() + ENTRY_OVERHEAD_BYTES;
        }
    }

    /// Look up a key. Returns `Some(&Some(value))` for live entries,
    /// `Some(&None)` for tombstones, or `None` if the key is absent.
    pub fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.entries.get(key)
    }

    /// Iterate over all entries in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Option<Vec<u8>>)> {
        self.entries.iter()
    }

    /// Iterate over entries in the given key range.
    pub fn range<R: RangeBounds<Vec<u8>>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (&Vec<u8>, &Option<Vec<u8>>)> {
        // Turbofish pins the search type: `Vec<u8>: Borrow<T>` admits both
        // `T = Vec<u8>` and `T = [u8]`, so inference alone is ambiguous.
        self.entries.range::<Vec<u8>, R>(range)
    }

    /// Number of entries (including tombstones).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the memtable has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Approximate size in bytes of all entries.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Consume the memtable and return the underlying sorted map.
    ///
    /// Used when freezing the memtable for flush to an SSTable.
    pub fn freeze(self) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
        self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get() {
        let mut mt = Memtable::new();
        mt.put(b"hello".to_vec(), b"world".to_vec());

        let result = mt.get(b"hello");
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref().unwrap(), b"world");
    }

    #[test]
    fn get_missing() {
        let mt = Memtable::new();
        assert!(mt.get(b"missing").is_none());
    }

    #[test]
    fn overwrite() {
        let mut mt = Memtable::new();
        mt.put(b"key".to_vec(), b"value1".to_vec());
        mt.put(b"key".to_vec(), b"value2".to_vec());

        assert_eq!(mt.len(), 1);
        assert_eq!(mt.get(b"key").unwrap().as_ref().unwrap(), b"value2");
    }

    #[test]
    fn tombstone() {
        let mut mt = Memtable::new();
        mt.put(b"key".to_vec(), b"value".to_vec());
        mt.delete(b"key".to_vec());

        // Key exists but value is None (tombstone).
        let result = mt.get(b"key");
        assert!(result.is_some());
        assert!(result.unwrap().is_none());
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn delete_nonexistent() {
        let mut mt = Memtable::new();
        mt.delete(b"ghost".to_vec());

        // Tombstone inserted for a key that never existed.
        assert_eq!(mt.len(), 1);
        assert!(mt.get(b"ghost").unwrap().is_none());
    }

    #[test]
    fn iter_sorted() {
        let mut mt = Memtable::new();
        mt.put(b"charlie".to_vec(), b"3".to_vec());
        mt.put(b"alice".to_vec(), b"1".to_vec());
        mt.put(b"bob".to_vec(), b"2".to_vec());

        let keys: Vec<&[u8]> = mt.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![b"alice".as_slice(), b"bob", b"charlie"]);
    }

    #[test]
    fn range_query() {
        let mut mt = Memtable::new();
        for i in 0u8..10 {
            mt.put(vec![i], vec![i * 10]);
        }

        let range_entries: Vec<_> = mt.range(vec![3]..vec![7]).collect();
        assert_eq!(range_entries.len(), 4); // keys 3, 4, 5, 6
        assert_eq!(range_entries[0].0, &vec![3u8]);
        assert_eq!(range_entries[3].0, &vec![6u8]);
    }

    #[test]
    fn size_tracking() {
        let mut mt = Memtable::new();
        assert_eq!(mt.size_bytes(), 0);

        // Insert: key(3) + value(5) + overhead(32) = 40.
        mt.put(b"foo".to_vec(), b"hello".to_vec());
        assert_eq!(mt.size_bytes(), 3 + 5 + ENTRY_OVERHEAD_BYTES);

        // Overwrite with shorter value: size decreases by 5, increases by 2.
        mt.put(b"foo".to_vec(), b"hi".to_vec());
        assert_eq!(mt.size_bytes(), 3 + 2 + ENTRY_OVERHEAD_BYTES);

        // Delete: value size drops to 0.
        mt.delete(b"foo".to_vec());
        assert_eq!(mt.size_bytes(), 3 + ENTRY_OVERHEAD_BYTES);
    }

    #[test]
    fn size_tracking_delete_then_put() {
        let mut mt = Memtable::new();

        // Delete first (tombstone): key(3) + overhead(32) = 35.
        mt.delete(b"bar".to_vec());
        assert_eq!(mt.size_bytes(), 3 + ENTRY_OVERHEAD_BYTES);

        // Now put overwrites the tombstone: old value size was 0, new is 4.
        mt.put(b"bar".to_vec(), b"data".to_vec());
        assert_eq!(mt.size_bytes(), 3 + 4 + ENTRY_OVERHEAD_BYTES);
    }

    #[test]
    fn is_empty_and_len() {
        let mut mt = Memtable::new();
        assert!(mt.is_empty());
        assert_eq!(mt.len(), 0);

        mt.put(b"a".to_vec(), b"1".to_vec());
        assert!(!mt.is_empty());
        assert_eq!(mt.len(), 1);

        mt.delete(b"b".to_vec());
        assert_eq!(mt.len(), 2); // Tombstone counts as an entry.
    }

    #[test]
    fn freeze() {
        let mut mt = Memtable::new();
        mt.put(b"x".to_vec(), b"1".to_vec());
        mt.put(b"y".to_vec(), b"2".to_vec());
        mt.delete(b"z".to_vec());

        let map = mt.freeze();
        assert_eq!(map.len(), 3);
        assert_eq!(map.get(b"x".as_slice()).unwrap(), &Some(b"1".to_vec()));
        assert_eq!(map.get(b"y".as_slice()).unwrap(), &Some(b"2".to_vec()));
        assert_eq!(map.get(b"z".as_slice()).unwrap(), &None);
    }
}
