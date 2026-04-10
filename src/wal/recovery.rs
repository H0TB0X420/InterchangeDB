//! Crash recovery — replays WAL records into the storage engine.
//!
//! In auto-commit mode, every fully-decoded WAL record is considered
//! committed. Recovery is a simple forward scan: replay every `Put` and
//! `Delete` into the engine. `Begin`/`Commit`/`Abort`/`Checkpoint`
//! records are skipped.
//!
//! Recovery is idempotent — replaying the same WAL twice produces the
//! same engine state, because `put` overwrites and `delete` on a
//! non-existent key is a no-op.

use std::time::Instant;
use std::collections::HashSet;

use crate::common::Result;
use crate::storage::StorageEngine;
use crate::wal::{LogPayload, LogRecord, Lsn, WalReader};

/// Statistics from a recovery run.
#[derive(Debug, Clone)]
pub struct RecoveryStats {
    /// Total records scanned (including skipped types).
    pub records_scanned: u64,
    /// Records actually replayed into the engine (Put + Delete).
    pub records_redone: u64,
    pub records_undone: u64,
    pub txns_committed: u64,
    pub txns_undone: u64,
    /// Wall-clock duration of recovery.
    pub duration: std::time::Duration,
}

/// Replay WAL records into the engine, starting from `last_checkpoint_lsn`.
///
/// This is the core recovery algorithm for auto-commit mode:
/// 1. Forward scan from the last checkpoint (or beginning if none).
/// 2. Replay every `Put`/`Delete` into the engine.
/// 3. Skip `Begin`/`Commit`/`Abort`/`Checkpoint`.
///
/// Returns statistics about the recovery process.
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
    let (committed, aborted) = analyze_transactions(&records);
    let records_redone = redo_committed(engine, &records, &committed)?;
    let (records_undone, txns_undone) = undo_uncommitted(engine, &records, &committed,&aborted)?;

  
    Ok(RecoveryStats {
        records_scanned,
        records_redone,
        records_undone,
        txns_committed: committed.len() as u64,
        txns_undone,
        duration: start.elapsed(),
    })
}

fn analyze_transactions(records: &[LogRecord]) -> (HashSet<u64>, HashSet<u64>) {
    let mut committed = HashSet::new();
    let mut aborted = HashSet::new();

    for record in records {
        match &record.payload {
            LogPayload::Commit {..} => {
                committed.insert(record.txn_id);
            }
            LogPayload::Abort => {
                aborted.insert(record.txn_id);
            }
            _ => {}
        }
    }
    
    (committed, aborted)
}

fn redo_committed<E: StorageEngine>(
    engine: &mut E,
    records: &[LogRecord],
    committed: &HashSet<u64>,
    ) -> Result<u64> {
    let mut redone: u64 = 0;

    for record in records {
        match &record.payload {
            LogPayload::Put {key, value} => {
                engine.put(key, value)?;
                redone += 1;
            }
            LogPayload::Delete {key} => {
                engine.delete(key)?;
                redone += 1;
            }
            LogPayload::TxnPut { key, value, .. } => {
                if committed.contains(&record.txn_id) {
                    engine.put(key, value)?;
                    redone += 1;
                }
            }
            LogPayload::TxnDelete {key, ..} => {
                if committed.contains(&record.txn_id){
                    engine.delete(key)?;
                    redone += 1;
                }
            }
            _ => {}
        }
    }

    Ok(redone)
}

fn undo_uncommitted<E: StorageEngine>(
    engine: &mut E,
    records: &[LogRecord],
    committed: &HashSet<u64>,
    aborted: &HashSet<u64>,
    ) -> Result<(u64, u64)> {
        
    let mut uncommitted = HashSet::new();
    for record in records {
        if record.txn_id != 0 
            && !committed.contains(&record.txn_id)
            && !aborted.contains(&record.txn_id)
            {
                uncommitted.insert(record.txn_id);
            }
    }

    if uncommitted.is_empty() {
        return Ok((0,0))
    }

    let mut undone: u64 = 0;

    for record in records.iter().rev() {
        if !uncommitted.contains(&record.txn_id) {
            continue;
        }

        match &record.payload {
            LogPayload::TxnPut { key, old_value, .. } => {
                match old_value {
                    Some(ov) => engine.put(key, ov)?,
                    None => { let _ = engine.delete(key); }
                }
                undone += 1;
            }
            LogPayload::TxnDelete { key, old_value } => {
                if let Some(ov) = old_value {
                    engine.put(key, ov)?;
                }
                undone += 1;
            }
            _ => {}
        }
    }
    let txns_undone = uncommitted.len() as u64;
    Ok((undone, txns_undone))
}
