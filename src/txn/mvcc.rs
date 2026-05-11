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

/// Scan a range of user keys, returning the newest visible version of each.
///
/// Iterates over MVCC-encoded keys in the engine, groups by user key,
/// and emits only the first (newest) visible version per user key.
/// Tombstoned keys are excluded from results.
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
    let scan_start = encode_mvcc_key_start(start_key);
    let scan_end = encode_mvcc_key_end(end_key);

    let iter = engine.scan(scan_start..=scan_end);

    let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut current_user_key: Option<Vec<u8>> = None;
    let mut found_visible_for_current = false;

    for result in iter {
        let (encoded_key, encoded_value) = result?;
        let (user_key, version_ts) = decode_mvcc_key(&encoded_key)?;

        // Moved to a new user key — reset tracking.
        if current_user_key.as_ref() != Some(&user_key) {
            current_user_key = Some(user_key.clone());
            found_visible_for_current = false;
        }

        // Already found the newest visible version for this key — skip older versions.
        if found_visible_for_current {
            continue;
        }

        let mvcc_val = decode_mvcc_value(&encoded_value)?;
        let version_txn_id = match &mvcc_val {
            MvccValue::Value { txn_id, .. } => *txn_id,
            MvccValue::Tombstone { txn_id } => *txn_id,
        };

        if !is_visible(version_txn_id, version_ts, my_txn_id, snapshot, committed_txns, checkpoint_ts, known_uncommitted) {
            continue;
        }

        // First visible version for this user key.
        found_visible_for_current = true;
        if let MvccValue::Value { data, .. } = mvcc_val {
            results.push((user_key, data));
        }
        // If tombstone: key is deleted, don't emit. But mark found so we skip older versions.
    }

    Ok(results)
}

