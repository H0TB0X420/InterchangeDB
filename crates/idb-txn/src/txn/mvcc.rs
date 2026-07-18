//! MVCC key/value encoding, visibility logic, and snapshot reads.
//!
//! ## Key Encoding
//!
//! `key_len:u32_be || user_key || inverted_ts:u64_be`
//!
//! The length prefix groups all versions of the same user key together.
//! The inverted timestamp (u64::MAX - ts) makes newer versions sort first
//! lexicographically, so a forward scan yields newest-to-oldest.
//!
//! ## Value Encoding
//!
//! `tag:u8 || txn_id:u64_le || [data_len:u32_le || data]`
//!
//! Tag 1 = live value, tag 0 = tombstone (deletion marker).
//! The txn_id identifies the writer for visibility checks.

use std::collections::{HashMap, HashSet};

use crate::common::{Error, Result};
use crate::storage::{ScanIterator, StorageEngine};
use crate::txn::{Snapshot, Timestamp, TxnId};

// ---------------------------------------------------------------------------
// MVCC key encoding
// ---------------------------------------------------------------------------

/// Encode a user key + timestamp into an MVCC storage key.
///
/// Inverted timestamp (u64::MAX - ts) makes newer versions sort first.
pub fn encode_mvcc_key(user_key: &[u8], ts: Timestamp) -> Vec<u8> {
    assert!(user_key.len() <= u32::MAX as usize, "user key too large");
    let inverted_ts = u64::MAX - ts.0;

    let mut encoded = Vec::with_capacity(4 + user_key.len() + 8);
    encoded.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
    encoded.extend_from_slice(user_key);
    encoded.extend_from_slice(&inverted_ts.to_be_bytes());
    encoded
}

/// Decode an MVCC storage key into (user_key, timestamp).
pub fn decode_mvcc_key(encoded: &[u8]) -> Result<(Vec<u8>, Timestamp)> {
    if encoded.len() < 12 {
        return Err(Error::StorageCorrupted("mvcc key too short".into()));
    }
    let key_len = u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize;
    if encoded.len() != 4 + key_len + 8 {
        return Err(Error::StorageCorrupted("mvcc key length mismatch".into()));
    }
    let user_key = encoded[4..4 + key_len].to_vec();
    let inverted_ts = u64::from_be_bytes(encoded[4 + key_len..4 + key_len + 8].try_into().unwrap());
    Ok((user_key, Timestamp(u64::MAX - inverted_ts)))
}

/// Encode the lower bound for scanning all versions of a user key.
/// Uses Timestamp(u64::MAX) which inverts to 0 — sorts first among all
/// versions of this key.
pub fn encode_mvcc_key_start(user_key: &[u8]) -> Vec<u8> {
    encode_mvcc_key(user_key, Timestamp(u64::MAX))
}

/// Encode the upper bound (inclusive) for scanning all versions of a user key.
/// Uses Timestamp(0) which inverts to u64::MAX — sorts last among all
/// versions of this key.
pub fn encode_mvcc_key_end(user_key: &[u8]) -> Vec<u8> {
    encode_mvcc_key(user_key, Timestamp(0))
}

// ---------------------------------------------------------------------------
// MVCC value encoding
// ---------------------------------------------------------------------------

/// An MVCC value: either a live value or a tombstone.
///
/// Stores the txn_id of the writer so visibility checks can determine
/// whether this version was written by a committed transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccValue {
    /// Live value written by txn_id.
    Value { txn_id: TxnId, data: Vec<u8> },
    /// Deletion marker written by txn_id.
    Tombstone { txn_id: TxnId },
}

/// Encode an MvccValue to bytes.
///
/// Layout: tag:u8 || txn_id:u64_le || [data_len:u32_le || data]
/// Tag 1 = Value, tag 0 = Tombstone.
pub fn encode_mvcc_value(val: &MvccValue) -> Vec<u8> {
    match val {
        MvccValue::Value { txn_id, data } => {
            let mut buf = Vec::with_capacity(1 + 8 + 4 + data.len());
            buf.push(1);
            buf.extend_from_slice(&txn_id.0.to_le_bytes());
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
            buf
        }
        MvccValue::Tombstone { txn_id } => {
            let mut buf = Vec::with_capacity(1 + 8);
            buf.push(0);
            buf.extend_from_slice(&txn_id.0.to_le_bytes());
            buf
        }
    }
}

/// Decode an MvccValue from bytes.
pub fn decode_mvcc_value(encoded: &[u8]) -> Result<MvccValue> {
    if encoded.is_empty() {
        return Err(Error::StorageCorrupted("empty mvcc value".into()));
    }
    match encoded[0] {
        1 => {
            if encoded.len() < 13 {
                return Err(Error::StorageCorrupted(
                    "mvcc value too short for Value".into(),
                ));
            }
            let txn_id = TxnId(u64::from_le_bytes(encoded[1..9].try_into().unwrap()));
            let data_len = u32::from_le_bytes(encoded[9..13].try_into().unwrap()) as usize;
            if encoded.len() != 13 + data_len {
                return Err(Error::StorageCorrupted(
                    "mvcc value data length mismatch".into(),
                ));
            }
            let data = encoded[13..13 + data_len].to_vec();
            Ok(MvccValue::Value { txn_id, data })
        }
        0 => {
            if encoded.len() < 9 {
                return Err(Error::StorageCorrupted(
                    "mvcc value too short for Tombstone".into(),
                ));
            }
            let txn_id = TxnId(u64::from_le_bytes(encoded[1..9].try_into().unwrap()));
            Ok(MvccValue::Tombstone { txn_id })
        }
        tag => Err(Error::StorageCorrupted(format!(
            "invalid mvcc value tag: {}",
            tag
        ))),
    }
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

/// Determine if a version is visible to a transaction's snapshot.
///
/// Standard SI: a version is visible iff the writer committed at-or-before
/// our snapshot's read_ts. The `active_txns` field on the snapshot is not
/// consulted — `committed_txns` is authoritative, and a writer's presence
/// there with `commit_ts <= read_ts` is sufficient and necessary.
///
/// Rules (evaluated in order):
/// 1. Own writes are always visible.
/// 2. Pre-checkpoint versions are assumed committed (their Commit record
///    may have been in a truncated WAL segment).
/// 3. If the writer never committed (not in committed_txns, post-checkpoint
///    or known-uncommitted), invisible.
/// 4. If the writer committed after our snapshot, invisible.
/// 5. Otherwise, visible.
// Visibility requires all 7 parameters for correctness — no clean way to reduce.
#[allow(clippy::too_many_arguments)]
pub fn is_visible(
    version_txn_id: TxnId,
    version_ts: Timestamp,
    my_txn_id: TxnId,
    snapshot: &Snapshot,
    committed_txns: &HashMap<TxnId, Timestamp>,
    checkpoint_ts: Timestamp,
    known_uncommitted: &HashSet<TxnId>,
) -> bool {
    if version_txn_id == my_txn_id {
        return true;
    }

    let commit_ts = match committed_txns.get(&version_txn_id) {
        Some(ts) => *ts,
        None => {
            if version_ts <= checkpoint_ts && !known_uncommitted.contains(&version_txn_id) {
                version_ts
            } else {
                return false;
            }
        }
    };

    commit_ts <= snapshot.read_ts
}

// ---------------------------------------------------------------------------
// MVCC read path
// ---------------------------------------------------------------------------

/// Find the newest visible version of a user key.
///
/// Scans the engine for all versions of the key (newest first due to
/// inverted timestamp encoding). Returns the first visible version's data,
/// or None if the key doesn't exist or is tombstoned.
pub fn mvcc_get<E: StorageEngine>(
    engine: &E,
    user_key: &[u8],
    visible: &dyn Fn(TxnId, Timestamp) -> bool,
) -> Result<Option<Vec<u8>>> {
    let start = encode_mvcc_key_start(user_key);
    let end = encode_mvcc_key_end(user_key);

    // Scan all versions of this key, newest first.
    let iter = engine.scan(start..=end);

    for result in iter {
        let (encoded_key, encoded_value) = result?;

        // Decode and verify this is still the same user key.
        let (found_key, version_ts) = decode_mvcc_key(&encoded_key)?;
        if found_key != user_key {
            break;
        }

        // Decode value to get txn_id and data/tombstone.
        let mvcc_val = decode_mvcc_value(&encoded_value)?;

        let version_txn_id = match &mvcc_val {
            MvccValue::Value { txn_id, .. } => *txn_id,
            MvccValue::Tombstone { txn_id } => *txn_id,
        };

        // Check visibility.
        if !visible(version_txn_id, version_ts) {
            continue;
        }

        // First visible version found.
        return match mvcc_val {
            MvccValue::Value { data, .. } => Ok(Some(data)),
            MvccValue::Tombstone { .. } => Ok(None),
        };
    }

    // No visible version exists for this key.
    Ok(None)
}

/// Smallest user_key of length `l` that is >= `start`.
///
/// Returns `None` when no such key exists (e.g., `start` is all-`0xFF` and
/// `l < start.len()`).
fn min_user_key_at_length_ge(start: &[u8], l: usize) -> Option<Vec<u8>> {
    if l >= start.len() {
        // Pad with zeros: result equals `start` for the first start.len()
        // bytes, then has extra zeros, making it strictly greater than
        // `start` (longer with matching prefix wins lex).
        let mut uk = Vec::with_capacity(l);
        uk.extend_from_slice(start);
        uk.resize(l, 0);
        Some(uk)
    } else {
        // Need uk[0..l] > start[0..l] lex (strictly, because uk shorter than
        // start with uk == start[0..l] makes uk < start). Smallest such is
        // lex-increment of start[0..l].
        let mut uk = start[..l].to_vec();
        let mut carry = true;
        for byte in uk.iter_mut().rev() {
            if !carry {
                break;
            }
            if *byte == 0xFF {
                *byte = 0;
            } else {
                *byte += 1;
                carry = false;
            }
        }
        if carry {
            None
        } else {
            Some(uk)
        }
    }
}

/// Largest user_key of length `l` that is <= `end`.
///
/// Returns `None` when no such key exists (e.g., `end` is all-zero and
/// `l > end.len()`).
fn max_user_key_at_length_le(end: &[u8], l: usize) -> Option<Vec<u8>> {
    if l <= end.len() {
        // Take the first l bytes of `end`. Result is <= end either via
        // shorter-with-prefix-match (l < end.len) or equal (l == end.len).
        Some(end[..l].to_vec())
    } else {
        // l > end.len: any length-l uk has uk[0..end.len] vs end. Same
        // prefix means uk > end (longer wins). So we need uk[0..end.len]
        // strictly < end. Largest such is lex-decrement of end, then pad
        // with 0xFF up to length l.
        let mut prefix = end.to_vec();
        let mut borrow = true;
        for byte in prefix.iter_mut().rev() {
            if !borrow {
                break;
            }
            if *byte == 0 {
                *byte = 0xFF;
            } else {
                *byte -= 1;
                borrow = false;
            }
        }
        if borrow {
            None
        } else {
            prefix.resize(l, 0xFF);
            Some(prefix)
        }
    }
}

/// Lazy, streaming scan over a user-key range, yielding the newest visible
/// version of each key as it goes — the iterator behind [`mvcc_scan`].
///
/// Iterates length-buckets of the MVCC keyspace using probe-then-scan: each
/// `engine.scan(probe..).next()` is an O(log n) seek that returns the next
/// occupied bucket's first entry. From its length-prefix we compute the
/// sub-range of that bucket that intersects `[start_key, end_key]` and scan
/// it directly, then advance the probe past the bucket.
///
/// For fixed-length-PK tables (TPC-C warehouse/district/customer) this is one
/// seek + one targeted scan. For variable-length PKs it's K seeks where K =
/// number of distinct row-key lengths present in the engine.
///
/// Streaming (vs the previous `Vec`-buffered version) matters because the
/// underlying engine scan is now lazy too: a consumer that stops early (LIMIT,
/// first-match) stops the storage read, and a full scan never holds the whole
/// range resident. The visibility decision `F` is **owned** so the iterator
/// can outlive the call that built it.
///
/// `start_key` and `end_key` define an INCLUSIVE user-key range.
pub struct MvccScan<'a, E: StorageEngine, F: Fn(TxnId, Timestamp) -> bool> {
    engine: &'a E,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    visible: F,
    /// Lowest MVCC key the next bucket-probe seeks from. Starts at the lowest
    /// possible key (length-prefix 0, empty user_key, ts inverted to 0).
    probe_pos: Vec<u8>,
    /// Active bucket's version scan, if one is in progress.
    inner: Option<Box<dyn ScanIterator + 'a>>,
    /// Dedup state within the active bucket: the user key currently being
    /// resolved and whether its newest version has already been decided.
    current_user_key: Option<Vec<u8>>,
    found_visible_for_current: bool,
    /// The active bucket is the last possible one (probe length overflowed).
    last_bucket: bool,
    /// Terminal state — an error surfaced or the keyspace is exhausted.
    done: bool,
}

impl<'a, E: StorageEngine, F: Fn(TxnId, Timestamp) -> bool> MvccScan<'a, E, F> {
    pub fn new(engine: &'a E, start_key: Vec<u8>, end_key: Vec<u8>, visible: F) -> Self {
        Self {
            engine,
            start_key,
            end_key,
            visible,
            probe_pos: vec![0; 4 + 8],
            inner: None,
            current_user_key: None,
            found_visible_for_current: false,
            last_bucket: false,
            done: false,
        }
    }

    /// Find the next occupied length-bucket at/after `probe_pos` and arm
    /// `inner` with its intersection against `[start_key, end_key]`. Skips
    /// buckets that don't intersect. Returns `Ok(true)` once a bucket scan is
    /// armed, `Ok(false)` when the keyspace is exhausted. Advances `probe_pos`
    /// past each bucket it inspects and sets `last_bucket` on u32 overflow.
    fn arm_next_bucket(&mut self) -> Result<bool> {
        loop {
            let probe_key = {
                let mut iter = self.engine.scan(self.probe_pos.clone()..);
                match iter.next() {
                    None => return Ok(false),
                    Some(Err(e)) => return Err(e),
                    Some(Ok((k, _))) => k,
                }
            };

            // The first 4 bytes of a well-formed MVCC key are the user_key
            // length prefix; anything shorter isn't an MVCC entry.
            if probe_key.len() < 4 {
                return Ok(false);
            }
            let bucket_len =
                u32::from_be_bytes([probe_key[0], probe_key[1], probe_key[2], probe_key[3]])
                    as usize;

            // Validate MVCC encoding. When the engine is shared between
            // TxnEngine (MVCC entries) and Catalog (raw entries), a non-MVCC
            // entry's first 4 bytes are arbitrary — interpreting them as a
            // length yields huge values. MVCC entries always satisfy
            // `total_len == 4 + bucket_len + 8`. Use u64 to avoid usize
            // overflow when `bucket_len` is huge.
            let expected_total = 4u64 + bucket_len as u64 + 8u64;
            if probe_key.len() as u64 != expected_total {
                // Crossed into a non-MVCC region of the keyspace — done.
                return Ok(false);
            }

            // Advance the probe to the next length bucket now. If
            // `bucket_len + 1` overflows u32, this is the last possible bucket.
            match (bucket_len as u32).checked_add(1) {
                Some(n) => self.probe_pos = n.to_be_bytes().to_vec(),
                None => self.last_bucket = true,
            }

            // Compute the bucket's intersection with [start_key, end_key].
            let bucket_min = min_user_key_at_length_ge(&self.start_key, bucket_len);
            let bucket_max = max_user_key_at_length_le(&self.end_key, bucket_len);
            if let (Some(min_uk), Some(max_uk)) = (bucket_min, bucket_max) {
                if min_uk <= max_uk {
                    let scan_start = encode_mvcc_key_start(&min_uk);
                    let scan_end = encode_mvcc_key_end(&max_uk);
                    self.inner = Some(self.engine.scan(scan_start..=scan_end));
                    self.current_user_key = None;
                    self.found_visible_for_current = false;
                    return Ok(true);
                }
            }

            // Empty intersection: this bucket contributes nothing. Stop if it
            // was the last one, otherwise probe the next bucket.
            if self.last_bucket {
                return Ok(false);
            }
        }
    }
}

impl<'a, E: StorageEngine, F: Fn(TxnId, Timestamp) -> bool> Iterator for MvccScan<'a, E, F> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Pull one entry from the active bucket scan, if any. The
            // `self.inner` borrow is released as the match expression yields
            // the owned `Option`, so the body below can touch other fields.
            let pulled = match self.inner.as_mut() {
                Some(inner) => inner.next(),
                None => None,
            };

            match pulled {
                Some(Ok((encoded_key, encoded_value))) => {
                    let (user_key, version_ts) = match decode_mvcc_key(&encoded_key) {
                        Ok(v) => v,
                        Err(e) => {
                            self.done = true;
                            self.inner = None;
                            return Some(Err(e));
                        }
                    };

                    // New user key → reset the newest-version decision.
                    if self.current_user_key.as_ref() != Some(&user_key) {
                        self.current_user_key = Some(user_key.clone());
                        self.found_visible_for_current = false;
                    }
                    if self.found_visible_for_current {
                        continue;
                    }

                    let mvcc_val = match decode_mvcc_value(&encoded_value) {
                        Ok(v) => v,
                        Err(e) => {
                            self.done = true;
                            self.inner = None;
                            return Some(Err(e));
                        }
                    };
                    let version_txn_id = match &mvcc_val {
                        MvccValue::Value { txn_id, .. } => *txn_id,
                        MvccValue::Tombstone { txn_id } => *txn_id,
                    };
                    if !(self.visible)(version_txn_id, version_ts) {
                        continue;
                    }

                    // First visible version of this key wins. A live value is
                    // emitted; a tombstone means "deleted" — emit nothing but
                    // still skip the key's older versions.
                    self.found_visible_for_current = true;
                    if let MvccValue::Value { data, .. } = mvcc_val {
                        return Some(Ok((user_key, data)));
                    }
                }
                Some(Err(e)) => {
                    self.done = true;
                    self.inner = None;
                    return Some(Err(e));
                }
                None => {
                    // `pulled == None` either because there's no active bucket
                    // or because the active one is exhausted.
                    if self.inner.is_some() {
                        self.inner = None;
                        if self.last_bucket {
                            self.done = true;
                        }
                    }
                    if self.done {
                        return None;
                    }
                    match self.arm_next_bucket() {
                        Ok(true) => continue,
                        Ok(false) => {
                            self.done = true;
                            return None;
                        }
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e));
                        }
                    }
                }
            }
        }
    }
}

/// Scan a range of user keys, returning the newest visible version of each as
/// an eagerly-collected `Vec`. Thin wrapper over [`MvccScan`] — every caller
/// and test of `mvcc_scan` exercises the same streaming iterator, just
/// collected. Streaming callers use `MvccScan` directly.
///
/// `start_key` and `end_key` define an INCLUSIVE user-key range.
pub fn mvcc_scan<E: StorageEngine>(
    engine: &E,
    start_key: &[u8],
    end_key: &[u8],
    visible: &dyn Fn(TxnId, Timestamp) -> bool,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    MvccScan::new(engine, start_key.to_vec(), end_key.to_vec(), visible).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::buffer::BufferPoolManager;
    use crate::engines::btree::BTreeEngine;
    use crate::storage::FileDiskManager;
    use tempfile::TempDir;

    // ---- min_user_key_at_length_ge ----

    #[test]
    fn min_uk_equal_length_returns_start_padded_to_self() {
        // L == len(start): result is start itself (pad-with-zero of zero bytes).
        assert_eq!(
            min_user_key_at_length_ge(&[0, 0, 0, 1], 4),
            Some(vec![0, 0, 0, 1])
        );
    }

    #[test]
    fn min_uk_longer_pads_with_zeros() {
        // L > len(start): pad with zeros; result > start (longer with matching prefix).
        assert_eq!(
            min_user_key_at_length_ge(&[0, 0, 0, 1], 6),
            Some(vec![0, 0, 0, 1, 0, 0])
        );
    }

    #[test]
    fn min_uk_shorter_lex_increments_prefix() {
        // L < len(start): smallest length-L uk > start[..L] = start[..L] incremented.
        assert_eq!(
            min_user_key_at_length_ge(&[0, 0, 0, 1], 3),
            Some(vec![0, 0, 1])
        );
        assert_eq!(
            min_user_key_at_length_ge(&[0, 0, 0, 1], 2),
            Some(vec![0, 1])
        );
    }

    #[test]
    fn min_uk_shorter_with_carry() {
        // Lex-inc with carry across multiple bytes.
        assert_eq!(
            min_user_key_at_length_ge(&[0, 0xFF, 0xFF, 1], 3),
            Some(vec![1, 0, 0])
        );
    }

    #[test]
    fn min_uk_returns_none_when_overflow() {
        // start prefix is all 0xFF at shorter length: no length-L uk can exceed it.
        assert_eq!(
            min_user_key_at_length_ge(&[0xFF, 0xFF, 0xFF, 0xFF], 3),
            None
        );
    }

    // ---- max_user_key_at_length_le ----

    #[test]
    fn max_uk_equal_length_returns_end_itself() {
        assert_eq!(
            max_user_key_at_length_le(&[0, 0, 0, 2], 4),
            Some(vec![0, 0, 0, 2])
        );
    }

    #[test]
    fn max_uk_shorter_takes_prefix_of_end() {
        // L < len(end): the first L bytes of end. result <= end via
        // shorter-with-matching-prefix.
        assert_eq!(
            max_user_key_at_length_le(&[0, 0, 0, 2], 3),
            Some(vec![0, 0, 0])
        );
    }

    #[test]
    fn max_uk_longer_decrements_end_then_pads_max() {
        // L > len(end): need uk[..end.len] < end. Lex-dec end, pad with 0xFF.
        assert_eq!(
            max_user_key_at_length_le(&[0, 0, 0, 2], 6),
            Some(vec![0, 0, 0, 1, 0xFF, 0xFF])
        );
    }

    #[test]
    fn max_uk_longer_with_borrow() {
        // Lex-dec across borrow.
        assert_eq!(
            max_user_key_at_length_le(&[0, 0, 1, 0], 6),
            Some(vec![0, 0, 0, 0xFF, 0xFF, 0xFF])
        );
    }

    #[test]
    fn max_uk_returns_none_when_underflow() {
        // end is all-zero at its length and we need a strictly smaller prefix.
        assert_eq!(max_user_key_at_length_le(&[0, 0, 0, 0], 6), None);
    }

    // ---- intersection viability ----

    #[test]
    fn bucket_at_table_id_length_overlaps_range() {
        // The case from the operator_tree e2e bug: start=[0,0,0,1], end=[0,0,0,2],
        // table rows live in length-9 bucket (4-byte table_id + 5-byte int32 PK).
        let min = min_user_key_at_length_ge(&[0, 0, 0, 1], 9).unwrap();
        let max = max_user_key_at_length_le(&[0, 0, 0, 2], 9).unwrap();
        assert_eq!(min, vec![0, 0, 0, 1, 0, 0, 0, 0, 0]);
        assert_eq!(max, vec![0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
        assert!(min <= max, "bucket should be non-empty for this range");
    }

    // ---- MvccScan (streaming scan + visibility/dedup state machine) ----
    //
    // These drive `MvccScan` directly against a real `BTreeEngine` holding
    // raw MVCC-encoded entries. They lock down the parts the iterator
    // refactor could break: newest-visible-wins dedup per key, tombstone
    // omission, falling through an invisible newest to an older visible
    // version, multi-length-bucket ordering, range filtering, and incremental
    // (early-terminable) yielding.

    fn fresh_engine() -> (BTreeEngine, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let dm = FileDiskManager::create(dir.path().join("t.db")).unwrap();
        let bpm = BufferPoolManager::new(256, dm);
        (BTreeEngine::new(bpm).unwrap(), dir)
    }

    /// Write one live version of `user_key` at `ts`, authored by `txn`.
    fn put_version(engine: &BTreeEngine, user_key: &[u8], ts: u64, txn: u64, data: &[u8]) {
        let key = encode_mvcc_key(user_key, Timestamp(ts));
        let val = encode_mvcc_value(&MvccValue::Value {
            txn_id: TxnId(txn),
            data: data.to_vec(),
        });
        engine.put(&key, &val).unwrap();
    }

    /// Write one tombstone version of `user_key` at `ts`, authored by `txn`.
    fn put_tombstone(engine: &BTreeEngine, user_key: &[u8], ts: u64, txn: u64) {
        let key = encode_mvcc_key(user_key, Timestamp(ts));
        let val = encode_mvcc_value(&MvccValue::Tombstone { txn_id: TxnId(txn) });
        engine.put(&key, &val).unwrap();
    }

    /// Full user-key range `[empty, 0xFF*32]` — the same bounds `scan_range`
    /// uses for an unbounded scan; covers any test key of length <= 32.
    fn full_range() -> (Vec<u8>, Vec<u8>) {
        (vec![], vec![0xFF; 32])
    }

    /// Collect an `MvccScan` over the whole keyspace with the given visibility.
    fn scan_all<F: Fn(TxnId, Timestamp) -> bool>(
        engine: &BTreeEngine,
        visible: F,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (start, end) = full_range();
        MvccScan::new(engine, start, end, visible)
            .collect::<Result<Vec<_>>>()
            .unwrap()
    }

    #[test]
    fn newest_visible_version_wins() {
        // Two visible versions of "a": the newer (ts=2) must shadow the older.
        let (engine, _dir) = fresh_engine();
        put_version(&engine, b"a", 1, 1, b"v1");
        put_version(&engine, b"a", 2, 2, b"v2");

        let rows = scan_all(&engine, |_txn, _ts| true);
        assert_eq!(rows, vec![(b"a".to_vec(), b"v2".to_vec())]);
    }

    #[test]
    fn invisible_newest_falls_through_to_older_visible() {
        // Newest version (txn 2) is invisible; the scan must surface the older
        // visible version (txn 1) instead of skipping the key entirely.
        let (engine, _dir) = fresh_engine();
        put_version(&engine, b"a", 1, 1, b"v1");
        put_version(&engine, b"a", 2, 2, b"v2");

        let rows = scan_all(&engine, |txn, _ts| txn != TxnId(2));
        assert_eq!(rows, vec![(b"a".to_vec(), b"v1".to_vec())]);
    }

    #[test]
    fn tombstone_as_newest_visible_omits_key() {
        // Newest visible version is a delete marker → the key is absent, even
        // though an older live version exists.
        let (engine, _dir) = fresh_engine();
        put_version(&engine, b"a", 1, 1, b"v1");
        put_tombstone(&engine, b"a", 2, 2);

        let rows = scan_all(&engine, |_txn, _ts| true);
        assert!(
            rows.is_empty(),
            "tombstoned key should not appear: {:?}",
            rows
        );
    }

    #[test]
    fn no_visible_version_omits_key() {
        // Every version of "a" is invisible → key absent; "b" still shows.
        let (engine, _dir) = fresh_engine();
        put_version(&engine, b"a", 1, 9, b"hidden");
        put_version(&engine, b"b", 1, 1, b"shown");

        let rows = scan_all(&engine, |txn, _ts| txn != TxnId(9));
        assert_eq!(rows, vec![(b"b".to_vec(), b"shown".to_vec())]);
    }

    #[test]
    fn yields_keys_across_length_buckets_in_bucket_order() {
        // Keys of different lengths live in different length-buckets. The scan
        // visits buckets by ascending length, so order is by length first,
        // then lexicographic within a length — exercising the probe-then-scan
        // bucket walk. "z" (len 1) precedes "aa" (len 2) despite 'z' > 'a'.
        let (engine, _dir) = fresh_engine();
        put_version(&engine, b"z", 1, 1, b"1");
        put_version(&engine, b"aa", 1, 1, b"2");
        put_version(&engine, b"mmm", 1, 1, b"3");

        let rows = scan_all(&engine, |_txn, _ts| true);
        let keys: Vec<Vec<u8>> = rows.into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"z".to_vec(), b"aa".to_vec(), b"mmm".to_vec()]);
    }

    #[test]
    fn range_bounds_are_inclusive_and_exclude_outside() {
        // Scan [b, d] inclusive over a..e: yields b, c, d only.
        let (engine, _dir) = fresh_engine();
        for k in [b"a", b"b", b"c", b"d", b"e"] {
            put_version(&engine, k, 1, 1, b"x");
        }
        let rows = MvccScan::new(&engine, b"b".to_vec(), b"d".to_vec(), |_t, _s| true)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let keys: Vec<Vec<u8>> = rows.into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
    }

    #[test]
    fn empty_engine_yields_nothing() {
        let (engine, _dir) = fresh_engine();
        assert!(scan_all(&engine, |_t, _s| true).is_empty());
    }

    #[test]
    fn yields_incrementally_so_consumers_can_stop_early() {
        // The scan is a real iterator: `take(2)` pulls exactly the first two
        // keys and stops, never visiting the rest. This is what lets a LIMIT
        // or first-match consumer halt the storage read.
        let (engine, _dir) = fresh_engine();
        for k in [b"a", b"b", b"c", b"d"] {
            put_version(&engine, k, 1, 1, b"x");
        }
        let (start, end) = full_range();
        let first_two: Vec<Vec<u8>> = MvccScan::new(&engine, start, end, |_t, _s| true)
            .take(2)
            .map(|r| r.unwrap().0)
            .collect();
        assert_eq!(first_two, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn streaming_matches_a_mixed_dataset_reference() {
        // End-to-end equivalence on a dataset mixing multiple versions,
        // tombstones, partial visibility, and multiple length buckets.
        let (engine, _dir) = fresh_engine();
        put_version(&engine, b"a", 1, 1, b"a-old");
        put_version(&engine, b"a", 3, 3, b"a-new"); // newest visible wins
        put_version(&engine, b"b", 2, 2, b"b-only");
        put_version(&engine, b"c", 1, 1, b"c-old");
        put_tombstone(&engine, b"c", 4, 4); // c deleted
        put_version(&engine, b"dd", 1, 1, b"dd-len2"); // separate bucket

        let rows = scan_all(&engine, |_txn, _ts| true);
        assert_eq!(
            rows,
            vec![
                (b"a".to_vec(), b"a-new".to_vec()),
                (b"b".to_vec(), b"b-only".to_vec()),
                // c omitted (tombstoned)
                (b"dd".to_vec(), b"dd-len2".to_vec()),
            ]
        );
    }
}
