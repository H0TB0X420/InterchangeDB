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
use std::sync::Arc;

use crate::buffer::BufferPoolManager;
use crate::common::Error;
use crate::common::Result;
use crate::index::btree::BTreeEngine;
use crate::index::lsm::LsmEngine;
use crate::storage::{ScanIterator, StorageEngine, StorageStatus};
use crate::txn::engine::TxnEngine;
use crate::txn::gc::{self, GcStats};
use crate::txn::mvcc::{self, MvccValue};
use crate::txn::{TransactionManager, TxnId, TxnMode};
use crate::wal::{LogRecord, Wal};

/// A database instance parameterized by storage engine.
///
/// When `wal` is `Some`, every mutation is logged before being applied
/// to the engine (write-ahead discipline). When `None`, the database
/// operates without durability guarantees (backward compatible).
///
/// Fields are `Arc` so transaction-scoped `TxnEngine<E>` handles can be
/// constructed cheaply (one atomic ref-count bump per shared piece) without
/// borrowing from `&self`. This keeps the shim through `TxnEngine` free
/// of lifetime gymnastics.
pub struct Database<E: StorageEngine> {
    engine: Arc<E>,
    wal: Option<Arc<Wal>>,
    txn_manager: Option<Arc<TransactionManager>>,
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
            engine: Arc::new(engine),
            wal: None,
            txn_manager: None,
        }
    }

    /// Open a WAL-enabled database.
    ///
    /// Creates or resumes the WAL in `data_dir/wal/`, runs crash recovery
    /// if needed, and returns the ready database.
    pub fn open(data_dir: &Path, engine: E) -> Result<Self> {
        let wal_dir = data_dir.join("wal");
        let wal = Wal::open(&wal_dir)?;

        // Run recovery: replay WAL records into the engine.
        let reader = wal.reader()?;
        let stats = crate::wal::recovery::recover(&reader, &engine, wal.last_checkpoint_lsn())?;

        // Seed the transaction manager with committed_txns from recovery.
        let txn_mgr = TransactionManager::new();
        let recovered: std::collections::HashMap<TxnId, crate::txn::Timestamp> = stats
            .committed_txns
            .into_iter()
            .map(|(id, ts)| (TxnId::new(id), crate::txn::Timestamp::new(ts)))
            .collect();
        txn_mgr.load_committed_txns(recovered);

        // Load uncommitted txn_ids so visibility doesn't assume they're committed.
        let uncommitted: std::collections::HashSet<TxnId> =
            stats.uncommitted_txns.into_iter().map(TxnId::new).collect();
        txn_mgr.load_uncommitted_txns(uncommitted);

        // Advance the txn-id counter past every id the engine could still
        // hold MVCC versions for. Without this, a fresh transaction could
        // be assigned an id that collides with a leftover uncommitted txn
        // from a prior crash; a later commit on that reused id would
        // retroactively make the leftover versions visible.
        txn_mgr.advance_next_txn_id_past(stats.max_txn_id);

        // Restore oracle timestamp from checkpoint.
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
            engine: Arc::new(engine),
            wal: Some(Arc::new(wal)),
            txn_manager: Some(Arc::new(txn_mgr)),
        })
    }

    /// Get the engine name (e.g., "btree", "lsm").
    pub fn engine_name(&self) -> &'static str {
        self.engine.name()
    }

    /// Retrieve a value by key.
    ///
    /// If MVCC is enabled (WAL mode), reads through a `TxnEngine` handle
    /// bound to `AUTO_COMMIT` so the visibility/snapshot logic lives in one
    /// place. Otherwise: direct engine get.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.txn_manager.is_some() {
            let handle = self.txn_engine_handle(TxnId::AUTO_COMMIT)?;
            handle.get(key)
        } else {
            self.engine.get(key)
        }
    }

    /// Insert or update a key-value pair.
    ///
    /// If MVCC is enabled, wraps in an implicit transaction (begin→put→commit).
    /// Otherwise: direct apply.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
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
    pub fn delete(&self, key: &[u8]) -> Result<()> {
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
    /// Under MVCC, returns the newest visible version of each user key
    /// via a fresh `TxnEngine` handle bound to `AUTO_COMMIT`. The handle's
    /// streaming iterator is collected into an owned `Vec` here so the
    /// returned iterator outlives the local handle. Cost: one extra Vec on
    /// this specific path. Operators bypass `Database::scan` entirely —
    /// they call `TxnEngine::scan` directly through their own handle, so
    /// they pay the single-Vec cost from `mvcc_scan` only.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Box<dyn ScanIterator + '_> {
        if self.txn_manager.is_none() {
            return self.engine.scan(range);
        }
        let handle = match self.txn_engine_handle(TxnId::AUTO_COMMIT) {
            Ok(h) => h,
            Err(e) => return Box::new(std::iter::once(Err(e))),
        };
        let collected: Result<Vec<_>> = handle.scan(range).collect();
        match collected {
            Ok(v) => Box::new(v.into_iter().map(Ok)),
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
    pub fn flush(&self) -> Result<()> {
        self.engine.flush()?;
        if let Some(ref wal) = self.wal {
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
        &self,
        data: &mut dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
    ) -> Result<()> {
        self.engine.import_data(data)
    }

    /// Access the underlying storage engine.
    pub fn engine(&self) -> &E {
        &self.engine
    }

    /// Check if WAL is enabled.
    pub fn has_wal(&self) -> bool {
        self.wal.is_some()
    }

    /// Create a checkpoint: flush engine, write checkpoint record, truncate old segments.
    ///
    /// No-op if WAL is not enabled.
    pub fn checkpoint(&self) -> Result<()> {
        let Some(ref wal) = self.wal else {
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
        let next_txn_id = self
            .txn_manager
            .as_ref()
            .map(|mgr| mgr.next_txn_id_peek())
            .unwrap_or(1);
        let mut record = LogRecord::checkpoint(active_txn_ids, oracle_ts, next_txn_id);
        let checkpoint_lsn = wal.append(&mut record)?;

        // Step 3: Sync WAL.
        wal.sync()?;

        // Step 4: Truncate old segments.
        wal.truncate_before(checkpoint_lsn)?;

        // Update cached checkpoint LSN.
        wal.set_last_checkpoint_lsn(checkpoint_lsn);

        // Update MVCC checkpoint watermark.
        if let Some(ref txn_mgr) = self.txn_manager {
            txn_mgr.set_checkpoint_ts(crate::txn::Timestamp(oracle_ts));
        }

        Ok(())
    }

    /// Run garbage collection — remove old MVCC versions no longer visible.
    pub fn gc(&self) -> Result<GcStats> {
        let txn_mgr = self.txn_manager.as_ref().ok_or(Error::TxnNotSupported)?;

        let active_timestamps = txn_mgr.active_read_timestamps();
        let low_water_mark = active_timestamps
            .iter()
            .min()
            .copied()
            .unwrap_or(txn_mgr.ts_oracle_peek());

        let committed = txn_mgr.committed_txns();
        let checkpoint_ts = txn_mgr.checkpoint_ts();
        let aborted_snapshot = txn_mgr.aborted_txns();
        let non_committed = txn_mgr.known_not_committed();

        let stats = gc::gc_collect(
            &*self.engine,
            low_water_mark,
            &committed,
            checkpoint_ts,
            &non_committed,
        )?;

        // Step 2.D: forget any snapshotted aborted txn whose versions are
        // fully gone. We re-scan the engine and intersect against the
        // snapshot; anything not seen is safe to remove from the in-memory
        // set. Aborts that occurred during gc_collect aren't in the snapshot,
        // so they stay tracked.
        //
        // NOTE (perf): this is a second full engine scan after gc_collect's
        // own pass. Could be folded into gc_collect to halve scan work —
        // gc_collect would return the safely-forgettable set alongside its
        // stats. Phase-13 maintenance cleanup.
        let mut still_present: std::collections::HashSet<TxnId> = std::collections::HashSet::new();
        for result in self.engine.scan(..) {
            let (encoded_key, encoded_value) = result?;
            if mvcc::decode_mvcc_key(&encoded_key).is_err() {
                continue;
            }
            let Ok(val) = mvcc::decode_mvcc_value(&encoded_value) else {
                continue;
            };
            let writer = match val {
                MvccValue::Value { txn_id, .. } => txn_id,
                MvccValue::Tombstone { txn_id } => txn_id,
            };
            if aborted_snapshot.contains(&writer) {
                still_present.insert(writer);
            }
        }
        let safe_to_forget: std::collections::HashSet<TxnId> = aborted_snapshot
            .difference(&still_present)
            .copied()
            .collect();
        txn_mgr.remove_aborted_txns(&safe_to_forget);

        Ok(stats)
    }

    /// Snapshot current GC state without running a collection. Cheap —
    /// no engine scan; just reads `TransactionManager` fields. Returns
    /// `Error::TxnNotSupported` for databases opened without a txn manager
    /// (i.e. `Database::new()` rather than `Database::open()`).
    pub fn gc_status(&self) -> Result<gc::GcStatus> {
        let txn_mgr = self.txn_manager.as_ref().ok_or(Error::TxnNotSupported)?;
        let active = txn_mgr.active_read_timestamps();
        let oldest_active_read_ts = active.iter().min().copied();
        let current_timestamp = txn_mgr.ts_oracle_peek();
        let low_water_mark = oldest_active_read_ts.unwrap_or(current_timestamp);
        Ok(gc::GcStatus {
            low_water_mark,
            oldest_active_read_ts,
            active_snapshot_count: active.len(),
            committed_txns_tracked: txn_mgr.committed_txns_len(),
            aborted_txns_tracked: txn_mgr.aborted_txns().len(),
            current_timestamp,
        })
    }

    pub fn begin_txn(&self, mode: TxnMode) -> Result<TxnId> {
        let txn_mgr = self.txn_manager.as_ref().ok_or(Error::TxnNotSupported)?;

        // Allocate ID atomically first (avoids race between peek and begin).
        let txn_id = txn_mgr.begin(mode, crate::wal::Lsn::INVALID)?;

        if mode == TxnMode::ReadWrite {
            // Write WAL Begin record with the actual assigned ID.
            let wal = self.wal.as_ref().ok_or(Error::TxnNotSupported)?;
            let mut record = LogRecord::begin(txn_id.0);
            let begin_lsn = wal.append(&mut record)?;
            // Update last_lsn so subsequent records chain correctly.
            txn_mgr.update_last_lsn(txn_id, begin_lsn)?;
        }

        Ok(txn_id)
    }

    /// Commit a transaction — assign commit_ts, make writes visible.
    ///
    /// NOTE (correctness, narrow window): `assign_commit_ts` atomically bumps
    /// the oracle, inserts into `committed_txns`, and removes the txn from
    /// `active_txns`. Between that point and `wal.sync_to(commit_lsn)` below,
    /// in-process readers can already see this txn as committed. On a crash
    /// in that window, the WAL doesn't have the Commit record so recovery
    /// marks the txn uncommitted — any reader that acted on the in-memory
    /// "ghost commit" gets a post-recovery inconsistency. In practice the
    /// window is microseconds and a real WAL-sync failure usually aborts the
    /// process anyway. Phase-13 fix: two-phase commit-marking that holds
    /// txns in a "preparing" state until WAL sync returns.
    pub fn commit_txn(&self, txn_id: TxnId) -> Result<()> {
        let txn_mgr = self.txn_manager.as_ref().ok_or(Error::TxnNotSupported)?;
        let prev_lsn = txn_mgr.last_lsn(txn_id)?;
        let is_read_write = txn_mgr.mode(txn_id)? == TxnMode::ReadWrite;

        // Assign commit_ts from oracle — this is when our writes become visible.
        let commit_ts = txn_mgr.assign_commit_ts(txn_id)?;

        if is_read_write {
            // Group commit: append the Commit record, then wait for durability.
            // sync_to batches multiple threads' fsyncs into one.
            let wal = self.wal.as_ref().ok_or(Error::TxnNotSupported)?;
            let mut record = LogRecord::commit(txn_id.0, prev_lsn, commit_ts.0);
            let commit_lsn = wal.append(&mut record)?;
            wal.sync_to(commit_lsn)?;
        }

        txn_mgr.lock_manager().release_all(txn_id);
        txn_mgr.commit(txn_id)?;

        Ok(())
    }

    /// Borrow the underlying engine `Arc` (used by callers that need to
    /// hand the same engine to a peer like `Catalog`). Cheap — caller
    /// clones the Arc.
    pub fn engine_arc(&self) -> &Arc<E> {
        &self.engine
    }

    /// Number of WAL fsync syscalls issued so far (0 if WAL-disabled).
    /// Group commit keeps this well below the commit count under
    /// concurrency; used by benchmarks/tests to observe batching.
    pub fn wal_fsync_count(&self) -> u64 {
        self.wal.as_ref().map(|w| w.fsync_count()).unwrap_or(0)
    }

    /// Construct a per-call `TxnEngine` handle bound to `txn_id`. Cheap —
    /// three `Arc` clones + the `TxnId`. All MVCC/locking ops route through
    /// this handle so the visibility/conflict logic lives in one place.
    pub fn txn_engine_handle(&self, txn_id: TxnId) -> Result<TxnEngine<E>> {
        let txn_mgr = self.txn_manager.as_ref().ok_or(Error::TxnNotSupported)?;
        let wal = self.wal.as_ref().ok_or(Error::TxnNotSupported)?;
        Ok(TxnEngine::new(
            self.engine.clone(),
            txn_mgr.clone(),
            wal.clone(),
            txn_id,
        ))
    }

    /// Read a value within a transaction using snapshot isolation (no locks).
    pub fn txn_get(&self, txn_id: TxnId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let handle = self.txn_engine_handle(txn_id)?;
        handle.get(key)
    }

    /// Put a key-value pair within a transaction. Creates a new MVCC version.
    pub fn txn_put(&self, txn_id: TxnId, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = self.txn_engine_handle(txn_id)?;
        handle.lock_for_write(key)?;
        handle.put(key, value)
    }

    /// Delete a key within a transaction. Writes a tombstone MVCC version.
    pub fn txn_delete(&self, txn_id: TxnId, key: &[u8]) -> Result<()> {
        let handle = self.txn_engine_handle(txn_id)?;
        handle.lock_for_write(key)?;
        handle.delete(key)
    }

    /// Abort a transaction — no undo needed under MVCC.
    pub fn txn_abort(&self, txn_id: TxnId) -> Result<()> {
        let txn_mgr = self.txn_manager.as_ref().ok_or(Error::TxnNotSupported)?;
        let prev_lsn = txn_mgr.last_lsn(txn_id)?;

        let wal = self.wal.as_ref().ok_or(Error::TxnNotSupported)?;
        let mut record = LogRecord::abort(txn_id.0, prev_lsn);
        let abort_lsn = wal.append(&mut record)?;
        wal.sync_to(abort_lsn)?;

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
