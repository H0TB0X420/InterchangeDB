//! Crash recovery — replays WAL records into the storage engine.
//!
//! Three-phase recovery with MVCC awareness:
//! 1. Analysis: scan forward, classify transactions (committed vs aborted vs uncommitted).
//! 2. Redo: replay committed txn operations and auto-commit operations.
//! 3. Under MVCC, uncommitted versions are NOT undone — they stay in the engine
//!    but are invisible (their txn_id never appears in committed_txns).
//!    GC in Phase 7 physically removes them.
//!
//! Recovery is idempotent — replaying the same WAL twice produces the
//! same engine state.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::common::Result;
use crate::storage::StorageEngine;
use crate::wal::{LogPayload, LogRecord, Lsn, WalReader};

/// Statistics from a recovery run.
#[derive(Debug, Clone)]
pub struct RecoveryStats {
    /// Total records scanned (including skipped types).
    pub records_scanned: u64,
    /// Records actually replayed into the engine.
    pub records_redone: u64,
    /// Number of committed transactions found.
    pub txns_committed: u64,
    /// Wall-clock duration of recovery.
    pub duration: std::time::Duration,
    /// Committed transactions map: txn_id -> commit_ts.
    /// Passed to TransactionManager for MVCC visibility checks.
    pub committed_txns: HashMap<u64, u64>,
    /// Oracle timestamp from the last checkpoint record.
    /// Used to restore the oracle on reopen so timestamps don't collide.
    pub checkpoint_oracle_ts: u64,
    /// Transactions that have WAL records but no Commit or Abort record.
    /// These are genuinely uncommitted — their versions must be invisible.
    pub uncommitted_txns: HashSet<u64>,
}

/// Replay WAL records into the engine, starting from `last_checkpoint_lsn`.
///
/// Returns recovery stats including the committed_txns map needed by MVCC.
pub fn recover<E: StorageEngine>(
    reader: &WalReader,
    engine: &mut E,
    last_checkpoint_lsn: Lsn,
) -> Result<RecoveryStats> {
    let start = Instant::now();

    let start_lsn = if last_checkpoint_lsn.is_valid() {
        last_checkpoint_lsn
    } else {
        Lsn::new(0)
    };

    let records: Vec<LogRecord> = reader
        .scan_forward(start_lsn)
        .collect::<Result<Vec<_>>>()?;

    let records_scanned = records.len() as u64;

    // Phase 1: Analysis — who committed, who aborted, last checkpoint oracle_ts.
    let (committed_map, aborted, checkpoint_oracle_ts) = analyze_transactions(&records);

    // Phase 2: Redo — replay committed operations.
    let committed_set: HashSet<u64> = committed_map.keys().copied().collect();
    let records_redone = redo_committed(engine, &records, &committed_set)?;

    // Phase 3: Identify uncommitted transactions (Begin but no Commit/Abort).
    // Their versions are in the engine but must be invisible.
    let mut seen_txns: HashSet<u64> = HashSet::new();
    for record in &records {
        if record.txn_id != 0 {
            seen_txns.insert(record.txn_id);
        }
    }
    let uncommitted_txns: HashSet<u64> = seen_txns
        .into_iter()
        .filter(|id| !committed_set.contains(id))
        .filter(|id| !aborted.contains(id))
        .collect();

    Ok(RecoveryStats {
        records_scanned,
        records_redone,
        txns_committed: committed_map.len() as u64,
        duration: start.elapsed(),
        committed_txns: committed_map,
        checkpoint_oracle_ts,
        uncommitted_txns,
    })
}

/// Phase 1: Forward scan to classify transactions.
/// Returns (committed: txn_id -> commit_ts, aborted: set, last checkpoint oracle_ts).
fn analyze_transactions(records: &[LogRecord]) -> (HashMap<u64, u64>, HashSet<u64>, u64) {
    let mut committed = HashMap::new();
    let mut aborted = HashSet::new();
    let mut checkpoint_oracle_ts: u64 = 0;

    for record in records {
        match &record.payload {
            LogPayload::Commit { commit_ts } => {
                committed.insert(record.txn_id, *commit_ts);
            }
            LogPayload::Abort => {
                aborted.insert(record.txn_id);
            }
            LogPayload::Checkpoint { oracle_ts, .. } => {
                checkpoint_oracle_ts = *oracle_ts;
            }
            _ => {}
        }
    }

    (committed, aborted, checkpoint_oracle_ts)
}

/// Phase 2: Replay committed transaction writes and auto-commit operations.
fn redo_committed<E: StorageEngine>(
    engine: &mut E,
    records: &[LogRecord],
    committed: &HashSet<u64>,
) -> Result<u64> {
    let mut redone: u64 = 0;

    for record in records {
        match &record.payload {
            // Auto-commit put (legacy format, txn_id == 0).
            LogPayload::Put { key, value } => {
                engine.put(key, value)?;
                redone += 1;
            }
            // Auto-commit delete (legacy format, txn_id == 0).
            LogPayload::Delete { key } => {
                engine.delete(key)?;
                redone += 1;
            }
            // Transactional put (MVCC-encoded key+value) — redo only if committed.
            LogPayload::TxnPut { key, value, .. } => {
                if committed.contains(&record.txn_id) {
                    engine.put(key, value)?;
                    redone += 1;
                }
            }
            // Transactional delete — redo only if committed.
            LogPayload::TxnDelete { key, .. } => {
                if committed.contains(&record.txn_id) {
                    engine.delete(key)?;
                    redone += 1;
                }
            }
            _ => {}
        }
    }

    Ok(redone)
}
