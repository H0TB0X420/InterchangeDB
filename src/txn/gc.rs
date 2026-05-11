//! Garbage collection for MVCC versions.
//!
//! Scans the engine for old versions that no active snapshot can see,
//! and physically removes them. Without GC, the engine grows without bound.
//!
//! ## Algorithm
//!
//! 1. Compute low-water mark = min(active snapshot read_ts values).
//! 2. Full scan of engine, group versions by user key.
//! 3. For each key's versions (newest first):
//!    - Keep anything above the watermark.
//!    - Keep the newest committed version below the watermark (the "current" value).
//!    - Remove everything else below the watermark.
//! 4. Batch-delete removed versions after scan completes.

use std::collections::{HashMap, HashSet};

use crate::common::Result;
use crate::storage::StorageEngine;
use crate::txn::mvcc::{decode_mvcc_key, decode_mvcc_value, MvccValue};
use crate::txn::{Timestamp, TxnId};

/// Statistics from a garbage collection run.
#[derive(Debug, Clone, Default)]
pub struct GcStats {
    /// Total MVCC versions scanned.
    pub versions_scanned: u64,
    /// Versions physically removed.
    pub versions_removed: u64,
    /// Tombstones physically removed.
    pub tombstones_removed: u64,
    /// Distinct user keys processed.
    pub keys_processed: u64,
}

/// Run garbage collection on the engine.
///
/// Removes MVCC versions that are no longer visible to any active or future
/// snapshot. Keeps the newest committed version per key below the watermark
/// as the "current" value.
///
/// `committed_txns` maps txn_id -> commit_ts for visibility determination.
/// `checkpoint_ts` is the watermark for pre-recovery assumed-committed versions.
pub fn gc_collect<E: StorageEngine>(
    engine: &E,
    low_water_mark: Timestamp,
    committed_txns: &HashMap<TxnId, Timestamp>,
    checkpoint_ts: Timestamp,
    non_committed: &HashSet<TxnId>,
) -> Result<GcStats> {
    let mut stats = GcStats::default();
    let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();

    // Phase 1: Scan all versions, identify reclaimable ones.
    // Collect into Vec first — can't mutate engine during scan.
    let all_entries: Vec<(Vec<u8>, Vec<u8>)> = engine
        .scan(..)
        .filter_map(|r| r.ok())
        .collect();

    let mut current_user_key: Option<Vec<u8>> = None;
    let mut found_keeper = false;

    for (encoded_key, encoded_value) in &all_entries {
        // Decode MVCC key. Skip entries that don't decode (non-MVCC data).
        let (user_key, version_ts) = match decode_mvcc_key(encoded_key) {
            Ok(pair) => pair,
            Err(_) => continue,
        };

        stats.versions_scanned += 1;

        // New user key — reset tracking.
        if current_user_key.as_ref() != Some(&user_key) {
            current_user_key = Some(user_key.clone());
            found_keeper = false;
            stats.keys_processed += 1;
        }

        // Above watermark — always keep (could be visible to active snapshots).
        if version_ts >= low_water_mark {
            continue;
        }

        // Below watermark. Determine if this version is committed.
        let mvcc_val = match decode_mvcc_value(encoded_value) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let version_txn_id = match &mvcc_val {
            MvccValue::Value { txn_id, .. } => *txn_id,
            MvccValue::Tombstone { txn_id } => *txn_id,
        };

        // Pre-checkpoint heuristic ("assume committed") is overridden by
        // definitive evidence the writer aborted or never committed.
        let is_committed = !non_committed.contains(&version_txn_id)
            && (committed_txns.contains_key(&version_txn_id)
                || version_ts <= checkpoint_ts);

        // Uncommitted/aborted version below watermark — always remove.
        if !is_committed {
            keys_to_delete.push(encoded_key.clone());
            match &mvcc_val {
                MvccValue::Tombstone { .. } => stats.tombstones_removed += 1,
                _ => stats.versions_removed += 1,
            }
            continue;
        }

        // Committed version below watermark.
        if !found_keeper {
            // This is the newest committed version below watermark — keep it.
            found_keeper = true;

            // Special case: if the keeper is a tombstone and it's the last
            // version for this key (no older versions will follow since we
            // process newest-first and this is the first below WM), we could
            // remove it. But we can't know if older versions follow without
            // lookahead. Mark it as keeper for now — a second GC pass would
            // clean it up.
            continue;
        }

        // Older committed version below watermark — superseded by the keeper.
        keys_to_delete.push(encoded_key.clone());
        match &mvcc_val {
            MvccValue::Tombstone { .. } => stats.tombstones_removed += 1,
            _ => stats.versions_removed += 1,
        }
    }

    // Phase 2: Batch-delete reclaimable versions.
    for key in &keys_to_delete {
        engine.delete(key)?;
    }

    Ok(stats)
}
