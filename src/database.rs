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
use crate::wal::{LogRecord, Wal};

/// A database instance parameterized by storage engine.
///
/// When `wal` is `Some`, every mutation is logged before being applied
/// to the engine (write-ahead discipline). When `None`, the database
/// operates without durability guarantees (backward compatible).
pub struct Database<E: StorageEngine> {
    engine: E,
    wal: Option<Wal>,
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
        Self { engine, wal: None }
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

        // Step 2: Append checkpoint record (empty active_txn_ids for auto-commit).
        let mut record = LogRecord::checkpoint(vec![]);
        let checkpoint_lsn = wal.append(&mut record)?;

        // Step 3: Sync WAL.
        wal.sync()?;

        // Step 4: Truncate old segments.
        wal.truncate_before(checkpoint_lsn)?;

        // Update cached checkpoint LSN.
        wal.last_checkpoint_lsn = checkpoint_lsn;

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
