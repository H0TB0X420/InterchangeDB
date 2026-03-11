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

use crate::common::Result;
use crate::storage::StorageEngine;
use crate::wal::{LogPayload, Lsn, WalReader};

/// Statistics from a recovery run.
#[derive(Debug, Clone)]
pub struct RecoveryStats {
    /// Total records scanned (including skipped types).
    pub records_scanned: u64,
    /// Records actually replayed into the engine (Put + Delete).
    pub records_redone: u64,
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

    let mut records_scanned: u64 = 0;
    let mut records_redone: u64 = 0;

    for result in reader.scan_forward(start_lsn) {
        let record = result?;
        records_scanned += 1;

        match record.payload {
            LogPayload::Put { ref key, ref value } => {
                engine.put(key, value)?;
                records_redone += 1;
            }
            LogPayload::Delete { ref key } => {
                engine.delete(key)?;
                records_redone += 1;
            }
            // Skip non-data records in auto-commit mode.
            LogPayload::Begin
            | LogPayload::Commit { .. }
            | LogPayload::Abort
            | LogPayload::Checkpoint { .. } => {}
        }
    }

    Ok(RecoveryStats {
        records_scanned,
        records_redone,
        duration: start.elapsed(),
    })
}
