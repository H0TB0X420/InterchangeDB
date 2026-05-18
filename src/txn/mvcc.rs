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
use crate::storage::StorageEngine;
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
    let key_len =
        u32::from_be_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]) as usize;
    if encoded.len() != 4 + key_len + 8 {
        return Err(Error::StorageCorrupted("mvcc key length mismatch".into()));
    }
    let user_key = encoded[4..4 + key_len].to_vec();
    let inverted_ts =
        u64::from_be_bytes(encoded[4 + key_len..4 + key_len + 8].try_into().unwrap());
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
            let txn_id = TxnId(u64::from_le_bytes(
                encoded[1..9].try_into().unwrap(),
            ));
            let data_len =
                u32::from_le_bytes(encoded[9..13].try_into().unwrap()) as usize;
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
            let txn_id = TxnId(u64::from_le_bytes(
                encoded[1..9].try_into().unwrap(),
            ));
            Ok(MvccValue::Tombstone { txn_id })
        }
        tag => Err(Error::StorageCorrupted(
            format!("invalid mvcc value tag: {}", tag),
        )),
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
            if version_ts <= checkpoint_ts
                && !known_uncommitted.contains(&version_txn_id)
            {
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
    my_txn_id: TxnId,
    snapshot: &Snapshot,
    committed_txns: &HashMap<TxnId, Timestamp>,
    checkpoint_ts: Timestamp,
    known_uncommitted: &HashSet<TxnId>,
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
        if !is_visible(version_txn_id, version_ts, my_txn_id, snapshot, committed_txns, checkpoint_ts, known_uncommitted) {
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

/// Scan a range of user keys, returning the newest visible version of each.
///
/// Iterates length-buckets of the MVCC keyspace using probe-then-scan: each
/// `engine.scan(probe..).next()` is an O(log n) seek that returns the next
/// occupied bucket's first entry. From its length-prefix we compute the
/// sub-range of that bucket that intersects `[start_key, end_key]` and scan
/// it directly. After processing, advance the probe past the bucket.
///
/// For fixed-length-PK tables (TPC-C warehouse/district/customer) this is
/// one seek + one targeted scan. For variable-length PKs it's K seeks where
/// K = number of distinct row-key lengths actually present in the engine.
///
/// `start_key` and `end_key` define an INCLUSIVE user-key range.
///
/// NOTE (perf): still buffers into a `Vec` rather than streaming. The Vec
/// is the next bottleneck for analytical workloads — Phase 11 streaming
/// refactor.
pub fn mvcc_scan<E: StorageEngine>(
    engine: &E,
    start_key: &[u8],
    end_key: &[u8],
    my_txn_id: TxnId,
    snapshot: &Snapshot,
    committed_txns: &HashMap<TxnId, Timestamp>,
    checkpoint_ts: Timestamp,
    known_uncommitted: &HashSet<TxnId>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    // Probe starts at the lowest possible MVCC key: length-prefix 0,
    // empty user_key, ts=u64::MAX (inverted to 0). Equivalent to
    // `[0,0,0,0, 0*8]`.
    let mut probe_pos: Vec<u8> = vec![0; 4 + 8];

    loop {
        let probe_key = {
            let mut iter = engine.scan(probe_pos.clone()..);
            match iter.next() {
                None => break,
                Some(Err(e)) => return Err(e),
                Some(Ok((k, _))) => k,
            }
        };

        // The first 4 bytes of any well-formed MVCC key are the user_key
        // length prefix. Anything shorter is a non-MVCC engine entry —
        // shouldn't happen in practice, but guard against it.
        if probe_key.len() < 4 {
            break;
        }
        let bucket_len = u32::from_be_bytes([
            probe_key[0],
            probe_key[1],
            probe_key[2],
            probe_key[3],
        ]) as usize;

        // Compute the bucket's intersection with [start_key, end_key].
        let bucket_min = min_user_key_at_length_ge(start_key, bucket_len);
        let bucket_max = max_user_key_at_length_le(end_key, bucket_len);
        if let (Some(min_uk), Some(max_uk)) = (bucket_min, bucket_max) {
            if min_uk <= max_uk {
                let scan_start = encode_mvcc_key_start(&min_uk);
                let scan_end = encode_mvcc_key_end(&max_uk);

                let mut current_user_key: Option<Vec<u8>> = None;
                let mut found_visible_for_current = false;

                for result in engine.scan(scan_start..=scan_end) {
                    let (encoded_key, encoded_value) = result?;
                    let (user_key, version_ts) = decode_mvcc_key(&encoded_key)?;

                    if current_user_key.as_ref() != Some(&user_key) {
                        current_user_key = Some(user_key.clone());
                        found_visible_for_current = false;
                    }

                    if found_visible_for_current {
                        continue;
                    }

                    let mvcc_val = decode_mvcc_value(&encoded_value)?;
                    let version_txn_id = match &mvcc_val {
                        MvccValue::Value { txn_id, .. } => *txn_id,
                        MvccValue::Tombstone { txn_id } => *txn_id,
                    };

                    if !is_visible(
                        version_txn_id,
                        version_ts,
                        my_txn_id,
                        snapshot,
                        committed_txns,
                        checkpoint_ts,
                        known_uncommitted,
                    ) {
                        continue;
                    }

                    found_visible_for_current = true;
                    if let MvccValue::Value { data, .. } = mvcc_val {
                        results.push((user_key, data));
                    }
                }
            }
        }

        // Advance probe past bucket `bucket_len`: seek to start of the
        // next length bucket. If `bucket_len + 1` overflows u32, we've
        // covered every possible bucket — terminate.
        let next_len = match (bucket_len as u32).checked_add(1) {
            Some(n) => n,
            None => break,
        };
        probe_pos = next_len.to_be_bytes().to_vec();
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(min_user_key_at_length_ge(&[0xFF, 0xFF, 0xFF, 0xFF], 3), None);
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
}

