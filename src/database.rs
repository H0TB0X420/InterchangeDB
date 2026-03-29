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
use crate::common::Result;
use crate::index::btree::BTreeEngine;
use crate::index::lsm::LsmEngine;
use crate::storage::{ScanIterator, StorageEngine, StorageStatus};
use crate::wal::{LogRecord, Wal, LogPayload};
use crate::common::Error;
use crate::txn::{TransactionManager, TxnId, TxnMode};
use crate::txn::lock_manager::LockMode;

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
        Self { engine, wal: None , txn_manager: None}
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
        let _stats = crate::wal::recovery::recover(
            &reader,
            &mut engine,
            wal.last_checkpoint_lsn(),
        )?;

        Ok(Self {
            engine,
            wal: Some(wal),
            txn_manager: Some(TransactionManager::new()),
        })
    }

    /// Get the engine name (e.g., "btree", "lsm").
    pub fn engine_name(&self) -> &'static str {
        self.engine.name()
    }

    /// Retrieve a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.engine.get(key)
    }

    /// Insert or update a key-value pair.
    ///
    /// If WAL is enabled: log → sync → apply. Otherwise: direct apply.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(ref mut wal) = self.wal {
            let mut record = LogRecord::put(key.to_vec(), value.to_vec());
            wal.append(&mut record)?;
            wal.sync()?;
        }
        self.engine.put(key, value)
    }

    /// Delete a key.
    ///
    /// If WAL is enabled: log → sync → apply. Otherwise: direct apply.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if let Some(ref mut wal) = self.wal {
            let mut record = LogRecord::delete(key.to_vec());
            wal.append(&mut record)?;
            wal.sync()?;
        }
        self.engine.delete(key)
    }

    /// Scan a range of keys in sorted order.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Box<dyn ScanIterator + '_> {
        self.engine.scan(range)
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

        let active_txn_ids = self.txn_manager
            .as_ref()
            .map(|mgr| mgr.active_txn_ids())
            .unwrap_or_default();
        let mut record = LogRecord::checkpoint(active_txn_ids);
        let checkpoint_lsn = wal.append(&mut record)?;

        // Step 3: Sync WAL.
        wal.sync()?;

        // Step 4: Truncate old segments.
        wal.truncate_before(checkpoint_lsn)?;

        // Update cached checkpoint LSN.
        wal.last_checkpoint_lsn = checkpoint_lsn;

        Ok(())
    }

    pub fn begin_txn(&mut self, mode:TxnMode) -> Result<TxnId> {
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

    pub fn commit_txn(&mut self, txn_id: TxnId) -> Result<()> {
        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        let prev_lsn = txn_mgr.last_lsn(txn_id)?;

        let commit_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
        let mut record = LogRecord::commit(txn_id.0, prev_lsn, commit_ts);
        wal.append(&mut record)?;
        wal.sync()?;
        
        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        txn_mgr.lock_manager().release_all(txn_id);
        txn_mgr.commit(txn_id)?;

        Ok(())
    }

    pub fn txn_get(&mut self, txn_id: TxnId, key: &[u8]) -> 
Result<Option<Vec<u8>>> {
                
        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        txn_mgr.lock_manager().acquire(txn_id, key, LockMode::Shared)?;
        self.engine.get(key)
    }

    pub fn txn_put(&mut self, txn_id: TxnId, key: &[u8], value: &[u8]) -> Result<()>
    {
        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        if txn_mgr.mode(txn_id)? == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly(txn_id.0));
        }
        
        txn_mgr.lock_manager().acquire(txn_id, key, LockMode::Exclusive)?;
        
        let old_value = self.engine.get(key)?;
        let prev_lsn = self.txn_manager.as_ref().unwrap().last_lsn(txn_id)?;
        let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
        let mut record = LogRecord::txn_put(
            txn_id.0, prev_lsn, key.to_vec(), value.to_vec(), old_value,
        );
        let lsn = wal.append(&mut record);
        wal.sync()?;

        self.txn_manager.as_mut().unwrap().update_last_lsn(txn_id, lsn?)?;
         self.engine.put(key, value)
    }

    pub fn txn_delete(&mut self, txn_id: TxnId, key: &[u8]) -> Result<()> {
         let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        if txn_mgr.mode(txn_id)? == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly(txn_id.0));
        }
        
        txn_mgr.lock_manager().acquire(txn_id, key, LockMode::Exclusive)?;
        
        let old_value = self.engine.get(key)?;
        let prev_lsn = self.txn_manager.as_ref().unwrap().last_lsn(txn_id)?;
        let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
        let mut record = LogRecord::txn_delete(
            txn_id.0, prev_lsn, key.to_vec(), old_value,
        );
        let lsn = wal.append(&mut record);
        wal.sync()?;

        self.txn_manager.as_mut().unwrap().update_last_lsn(txn_id, lsn?)?;
        self.engine.delete(key)        
    }

    pub fn txn_abort(&mut self, txn_id: TxnId) -> Result<()> {
        let txn_mgr = self.txn_manager.as_mut().ok_or(Error::TxnNotSupported)?;
        let last_lsn = txn_mgr.last_lsn(txn_id)?;

        if last_lsn.is_valid() {    
            let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
            let reader = wal.reader()?;

            let txn_records: Vec<_> = reader
                .scan_forward(crate::wal::Lsn::new(0))
                .filter_map(|r| r.ok())
                .filter(|r| r.txn_id == txn_id.0)
                .collect(); //need to add to wal for O(1) random access 
                            //This is v ineffcient in comparison

            for record in txn_records.iter().rev() {
                match &record.payload {
                    LogPayload::TxnPut {key, old_value, .. } => {
                        match old_value {
                            Some(ov) => self.engine.put(key, ov)?,
                            None => {let _ = self.engine(); }
                        }
                    }
                    LogPayload::TxnDelete {key, old_value} => {
                        if let Some(ov) = old_value {
                            self.engine.put(key, ov)?;
                        }
                    }
                    _ => {}
                }
            }
        }

        let prev_lsn = self.txn_manager.as_ref().unwrap().last_lsn(txn_id)?;
        let wal = self.wal.as_mut().ok_or(Error::TxnNotSupported)?;
        let mut record = LogRecord::abort(txn_id.0, prev_lsn);
        let _ = wal.append(&mut record);
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
