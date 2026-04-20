//! Database struct with compile-time engine selection.
//!
//! `Database<E>` is the primary user-facing type. It wraps a `StorageEngine`
//! and optionally a WAL for crash recovery. Engine selection is a compile-time
//! decision via generics — zero-cost abstraction with no vtable overhead.
//!
//! ## Usage
//!
//! ```ignore
//! // Without WAL (all existing tests unchanged):
//! let db = Database::new(engine);
//!
//! // With WAL (durability + crash recovery):
//! let db = Database::open(data_dir, engine)?;
//! ```
//!
//! ## Type Aliases
//!
//! - `BTreeDatabase` = `Database<BTreeEngine>`
//! - `LsmDatabase` = `Database<LsmEngine>`

use std::ops::RangeBounds;
use std::path::Path;

use crate::buffer::BufferPoolManager;
use crate::common::Error;
use crate::common::Result;
use crate::index::btree::BTreeEngine;
use crate::index::lsm::LsmEngine;
use crate::storage::{ScanIterator, StorageEngine, StorageStatus};
use crate::txn::gc::{self, GcStats};
use crate::txn::lock_manager::LockMode;
use crate::txn::mvcc::{self, MvccValue};
use crate::txn::{Snapshot, TransactionManager, TxnId, TxnMode};
use crate::wal::{LogRecord, Wal};

/// A database instance parameterized by storage engine.
///
/// When `wal` is `Some`, every mutation is logged before being applied
/// to the engine (write-ahead discipline). When `None`, the database
/// operates without durability guarantees (backward compatible).
pub struct Database<E: StorageEngine> {
    engine: E,
    wal: Option<Wal>,
    txn_manager: Option<TransactionManager>,
}

/// B-tree backed database (point-lookup optimized).
pub type BTreeDatabase = Database<BTreeEngine>;

/// LSM-tree backed database (write-optimized).
pub type LsmDatabase = Database<LsmEngine>;

// ---------------------------------------------------------------------------
// Generic API (all engines)
// ---------------------------------------------------------------------------

impl<E: StorageEngine> Database<E> {
    /// Create a database wrapping the given storage engine (no WAL).
    ///
    /// This preserves backward compatibility — all existing tests continue
    /// to work unchanged.
    pub fn new(engine: E) -> Self {
        Self {
            engine,
            wal: None,
            txn_manager: None,
        }
    }

    /// Open a WAL-enabled database.
    ///
    /// Creates or resumes the WAL in `data_dir/wal/`, runs crash recovery
    /// if needed, and returns the ready database.
    pub fn open(data_dir: &Path, mut engine: E) -> Result<Self> {
        let wal_dir = data_dir.join("wal");
        let wal = Wal::open(&wal_dir)?;

        // Run recovery: replay WAL records into the engine.
        let reader = wal.reader()?;
        let stats =
            crate::wal::recovery::recover(&reader, &mut engine, wal.last_checkpoint_lsn())?;

        // Seed the transaction manager with committed_txns from recovery.
        // This is required for MVCC visibility — without it, recovered
        // versions would be invisible (no txn_id in committed_txns).
        let mut txn_mgr = TransactionManager::new();
        let recovered: std::collections::HashMap<TxnId, crate::txn::Timestamp> = stats
            .committed_txns
            .into_iter()
            .map(|(id, ts)| (TxnId::new(id), crate::txn::Timestamp::new(ts)))
            .collect();
        txn_mgr.load_committed_txns(recovered);

        // Load uncommitted txn_ids so visibility doesn't assume they're committed.
        let uncommitted: std::collections::HashSet<TxnId> = stats
            .uncommitted_txns
            .into_iter()
            .map(TxnId::new)
            .collect();
        txn_mgr.load_uncommitted_txns(uncommitted);

        // Restore oracle timestamp from checkpoint. This ensures new timestamps
        // don't collide with pre-existing versions after WAL truncation.
        // Only raise the checkpoint_ts — never lower it (load_committed_txns may
        // have set it higher from post-checkpoint commit records).
        if stats.checkpoint_oracle_ts > 0 {
            let ckpt_ts = crate::txn::Timestamp(stats.checkpoint_oracle_ts);
            if ckpt_ts > txn_mgr.checkpoint_ts() {
                txn_mgr.set_checkpoint_ts(ckpt_ts);
            }
            while txn_mgr.ts_oracle_peek().0 <= stats.checkpoint_oracle_ts {
                txn_mgr.advance_oracle();
            }
        }

        Ok(Self {
            engine,
            wal: Some(wal),
            txn_manager: Some(txn_mgr),
        })
    }

    /// Get the engine name (e.g., "btree", "lsm").
    pub fn engine_name(&self) -> &'static str {
        self.engine.name()
    }

    /// Retrieve a value by key.
    ///
    /// If MVCC is enabled (WAL mode), creates a temporary snapshot at the
    /// latest timestamp and reads through the MVCC layer. Otherwise: direct.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(ref txn_mgr) = self.txn_manager {
            // Use the higher of oracle peek and checkpoint_ts as read_ts.
            // After reopen, oracle may be low but checkpoint_ts covers old data.
            let oracle_ts = txn_mgr.ts_oracle_peek();
            let ckpt_ts = txn_mgr.checkpoint_ts();
            let read_ts = if ckpt_ts > oracle_ts { ckpt_ts } else { oracle_ts };
            let snapshot = Snapshot {
                read_ts,
                active_txns: std::collections::HashSet::new(),
            };
            let committed = txn_mgr.committed_txns();
            let uncommitted = txn_mgr.uncommitted_txns();
            mvcc::mvcc_get(&self.engine, key, TxnId::AUTO_COMMIT, &snapshot, committed, ckpt_ts, uncommitted)
        } else {
            self.engine.get(key)
        }
    }

    /// Insert or update a key-value pair.
    ///
    /// If MVCC is enabled, wraps in an implicit transaction (begin→put→commit).
    /// Otherwise: direct apply.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.wal.is_some() {
            let txn = self.begin_txn(TxnMode::ReadWrite)?;
            self.txn_put(txn, key, value)?;
            self.commit_txn(txn)?;
            Ok(())
        } else {
            self.engine.put(key, value)
        }
    }

    /// Delete a key.
    ///
    /// If MVCC is enabled, wraps in an implicit transaction (begin→delete→commit).
    /// Otherwise: direct apply.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if self.wal.is_some() {
            let txn = self.begin_txn(TxnMode::ReadWrite)?;
            self.txn_delete(txn, key)?;
            self.commit_txn(txn)?;
            Ok(())
        } else {
            self.engine.delete(key)
        }
    }

    /// Scan a range of keys in sorted order.
    ///
    /// Under MVCC, returns the newest visible version of each user key.
    /// Without MVCC (no WAL), delegates directly to the engine.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Box<dyn ScanIterator + '_> {
        if self.txn_manager.is_none() {
            return self.engine.scan(range);
        }

        // MVCC path: collect visible versions into a Vec, wrap in iterator.
        let txn_mgr = self.txn_manager.as_ref().unwrap();
        let oracle_ts = txn_mgr.ts_oracle_peek();
        let ckpt_ts = txn_mgr.checkpoint_ts();
        let read_ts = if ckpt_ts > oracle_ts { ckpt_ts } else { oracle_ts };
        let snapshot = Snapshot {
            read_ts,
            active_txns: std::collections::HashSet::new(),
        };
        let committed = txn_mgr.committed_txns();

        let start = match range.start_bound() {
            std::ops::Bound::Included(k) => k.clone(),
            std::ops::Bound::Excluded(k) => {
                let mut next = k.clone();
                next.push(0);
                next
            }
            std::ops::Bound::Unbounded => vec![],
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(k) | std::ops::Bound::Excluded(k) => k.clone(),
            std::ops::Bound::Unbounded => vec![0xFF; 32],
        };

        let uncommitted = txn_mgr.uncommitted_txns();
        let results = mvcc::mvcc_scan(
            &self.engine, &start, &end,
            TxnId::AUTO_COMMIT, &snapshot, committed, txn_mgr.checkpoint_ts(), uncommitted,
        );

        match results {
            Ok(pairs) => Box::new(pairs.into_iter().map(Ok)),
            Err(e) => Box::new(std::iter::once(Err(e))),
        }
    }

    /// Get engine statistics.
    pub fn status(&self) -> StorageStatus {
        self.engine.status()
    }

    /// Flush buffered writes to durable storage.
    ///
    /// If WAL is enabled, syncs the WAL after flushing the engine.
    pub fn flush(&mut self) -> Result<()> {
        self.engine.flush()?;
        if let Some(ref mut wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Export all data as an iterator of key-value pairs.
    pub fn export_data(&self) -> Box<dyn ScanIterator + '_> {
        self.engine.export_data()
    }

    /// Import data from an iterator. Used for cross-engine transfer.
    pub fn import_data(
        &mut self,
        data: &mut dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
    ) -> Result<()> {
        self.engine.import_data(data)
    }

    /// Access the underlying storage engine.
    pub fn engine(&self) -> &E {
        &self.engine
    }

    /// Access the underlying storage engine mutably.
    pub fn engine_mut(&mut self) -> &mut E {
        &mut self.engine
    }

    /// Check if WAL is enabled.
    pub fn has_wal(&self) -> bool {
        self.wal.is_some()
    }

    /// Create a checkpoint: flush engine, write checkpoint record, truncate old segments.
    ///
    /// 1. Flush the engine to durable storage.
    /// 2. Append a `Checkpoint` record to the WAL.
    /// 3. Sync the WAL.
    /// 4. Delete old WAL segments that precede the checkpoint.
    ///
    /// No-op if WAL is not enabled.
    pub fn checkpoint(&mut self) -> Result<()> {
        let Some(ref mut wal) = self.wal else {
            return Ok(());
        };

        // Step 1: Flush engine data to disk.
        self.engine.flush()?;

        let active_txn_ids = self
            .txn_manager
            .as_ref()
            .map(|mgr| mgr.active_txn_ids())
            .unwrap_or_default();
        let oracle_ts = self
            .txn_manager
            .as_ref()
            .map(|mgr| mgr.ts_oracle_peek().0)
            .unwrap_or(0);
        let mut record = LogRecord::checkpoint(active_txn_ids, oracle_ts);
        let checkpoint_lsn = wal.append(&mut record)?;

        // Step 3: Sync WAL.
        wal.sync()?;

        // Step 4: Truncate old segments.
        wal.truncate_before(checkpoint_lsn)?;

        // Update cached checkpoint LSN.
        wal.last_checkpoint_lsn = checkpoint_lsn;

        // Update MVCC checkpoint watermark.
        if let Some(ref mut txn_mgr) = self.txn_manager {
            txn_mgr.set_checkpoint_ts(crate::txn::Timestamp(oracle_ts));
        }

        Ok(())
    }

    /// Run garbage collection — remove old MVCC versions no longer visible.
    ///
    /// Computes the low-water mark from active snapshots, scans the engine
    /// for reclaimable versions, and physically deletes them.
    pub fn gc(&mut self) -> Result<GcStats> {
        let txn_mgr = self.txn_manager.as_ref().ok_or(Error::TxnNotSupported)?;

        let active_timestamps = txn_mgr.active_read_timestamps();
        let low_water_mark = active_timestamps.iter().min().copied()
            .unwrap_or(txn_mgr.ts_oracle_peek());

        let committed = txn_mgr.committed_txns().clone();
        let checkpoint_ts = txn_mgr.checkpoint_ts();

        gc::gc_collect(&mut self.engine, low_water_mark, &committed, checkpoint_ts)
    }

    pub fn begin_txn(&mut self, mode: TxnMode) -> Result<TxnId> {
        let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;

        let txn_id_raw = txn_mgr.next_txn_id_peek();

        let mut record = LogRecord::begin(txn_id_raw);
        let start_lsn = wal.append(&mut record)?;
        wal.sync()?;

        let txn_id = txn_mgr.begin(mode, start_lsn)?;
        assert_eq!(txn_id.0, txn_id_raw);

        Ok(txn_id)
    }

    /// Commit a transaction — assign commit_ts, make writes visible.
    pub fn commit_txn(&mut self, txn_id: TxnId) -> Result<()> {
        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        let prev_lsn = txn_mgr.last_lsn(txn_id)?;

        // Assign commit_ts from oracle — this is when our writes become visible.
        let commit_ts = txn_mgr.assign_commit_ts(txn_id)?;

        let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
        let mut record = LogRecord::commit(txn_id.0, prev_lsn, commit_ts.0);
        wal.append(&mut record)?;
        wal.sync()?;

        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        txn_mgr.lock_manager().release_all(txn_id);
        txn_mgr.commit(txn_id)?;

        Ok(())
    }

    /// Read a value within a transaction using snapshot isolation (no locks).
    pub fn txn_get(&mut self, txn_id: TxnId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let txn_mgr = self.txn_manager.as_ref().ok_or(Error::TxnNotSupported)?;
        let snapshot = txn_mgr.snapshot(txn_id)?;
        let committed = txn_mgr.committed_txns();
        let uncommitted = txn_mgr.uncommitted_txns();
        mvcc::mvcc_get(&self.engine, key, txn_id, snapshot, committed, txn_mgr.checkpoint_ts(), uncommitted)
    }

    /// Put a key-value pair within a transaction. Creates a new MVCC version.
    pub fn txn_put(&mut self, txn_id: TxnId, key: &[u8], value: &[u8]) -> Result<()> {
        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        if txn_mgr.mode(txn_id)? == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly(txn_id.0));
        }
        txn_mgr.lock_manager().acquire(txn_id, key, LockMode::Exclusive)?;

        let begin_ts = self.txn_manager.as_ref().unwrap().begin_ts(txn_id)?;
        let mvcc_key = mvcc::encode_mvcc_key(key, begin_ts);
        let mvcc_val = mvcc::encode_mvcc_value(&MvccValue::Value {
            txn_id,
            data: value.to_vec(),
        });

        let prev_lsn = self.txn_manager.as_ref().unwrap().last_lsn(txn_id)?;
        let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
        let mut record = LogRecord::txn_put(
            txn_id.0, prev_lsn, mvcc_key.clone(), mvcc_val.clone(), None,
        );
        let lsn = wal.append(&mut record)?;
        wal.sync()?;

        self.txn_manager.as_mut().unwrap().update_last_lsn(txn_id, lsn)?;
        self.engine.put(&mvcc_key, &mvcc_val)
    }

    /// Delete a key within a transaction. Writes a tombstone MVCC version.
    pub fn txn_delete(&mut self, txn_id: TxnId, key: &[u8]) -> Result<()> {
        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        if txn_mgr.mode(txn_id)? == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly(txn_id.0));
        }
        txn_mgr.lock_manager().acquire(txn_id, key, LockMode::Exclusive)?;

        let begin_ts = self.txn_manager.as_ref().unwrap().begin_ts(txn_id)?;
        let mvcc_key = mvcc::encode_mvcc_key(key, begin_ts);
        let mvcc_val = mvcc::encode_mvcc_value(&MvccValue::Tombstone { txn_id });

        let prev_lsn = self.txn_manager.as_ref().unwrap().last_lsn(txn_id)?;
        let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
        let mut record = LogRecord::txn_put(
            txn_id.0, prev_lsn, mvcc_key.clone(), mvcc_val.clone(), None,
        );
        let lsn = wal.append(&mut record)?;
        wal.sync()?;

        self.txn_manager.as_mut().unwrap().update_last_lsn(txn_id, lsn)?;
        self.engine.put(&mvcc_key, &mvcc_val)
    }

    /// Abort a transaction — no undo needed under MVCC.
    ///
    /// Aborted versions stay in the engine but are invisible (txn_id never
    /// appears in committed_txns). GC in Phase 7 removes them.
    pub fn txn_abort(&mut self, txn_id: TxnId) -> Result<()> {
        let txn_mgr = self.txn_manager.as_ref().ok_or(Error::TxnNotSupported)?;
        let prev_lsn = txn_mgr.last_lsn(txn_id)?;

        let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
        let mut record = LogRecord::abort(txn_id.0, prev_lsn);
        wal.append(&mut record)?;
        wal.sync()?;

        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        txn_mgr.lock_manager().release_all(txn_id);
        txn_mgr.abort(txn_id)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// B-tree convenience constructor
// ---------------------------------------------------------------------------

impl Database<BTreeEngine> {
    /// Create a B-tree backed database.
    pub fn btree(bpm: BufferPoolManager) -> Result<Self> {
        Ok(Self::new(BTreeEngine::new(bpm)?))
    }
}

// ---------------------------------------------------------------------------
// LSM-tree convenience constructor
// ---------------------------------------------------------------------------

impl Database<LsmEngine> {
    /// Create an LSM-tree backed database.
    pub fn lsm(data_dir: &Path) -> Result<Self> {
        Ok(Self::new(LsmEngine::new(data_dir)?))
    }
}
