//! Write-Ahead Logging — durability and crash recovery.
//!
//! The WAL enforces write-ahead discipline: every mutation is logged
//! before being applied to the storage engine. On crash, the log is
//! replayed to reach a consistent state.
//!
//! ## Modules
//!
//! - [`lsn`] — Log Sequence Number type
//! - [`record`] — Binary record format with CRC32 integrity
//! - [`segment`] — Segment file utilities
//! - [`writer`] — WAL writer (append + sync)
//! - [`reader`] — WAL reader (forward scan)

pub mod lsn;
pub mod reader;
pub mod record;
pub mod recovery;
pub mod segment;
pub mod writer;

pub use lsn::Lsn;
pub use reader::WalReader;
pub use record::{LogPayload, LogRecord, LogRecordType};
pub use writer::WalWriter;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Condvar, Mutex};

use crate::common::Result;

/// WAL facade — combines writer and reader with checkpoint tracking.
///
/// Supports group commit: multiple threads append records concurrently,
/// then a single fsync makes all pending records durable at once.
/// Waiting threads sleep on a Condvar until the sync leader wakes them.
pub struct Wal {
    writer: Mutex<WalWriter>,
    wal_dir: PathBuf,
    last_checkpoint_lsn_raw: AtomicU64,
    /// Highest LSN that has been fsync'd to disk.
    synced_lsn: AtomicU64,
    /// Condvar + Mutex for sleeping waiters. The Mutex holds no meaningful
    /// state — it exists only to pair with the Condvar.
    sync_waiters: Mutex<()>,
    sync_notify: Condvar,
}

impl Wal {
    /// Open or create a WAL in the given directory.
    ///
    /// If the directory already contains segments, resumes from the last state.
    /// Scans for the last checkpoint LSN for recovery purposes.
    pub fn open(wal_dir: &Path) -> Result<Self> {
        let writer = WalWriter::open(wal_dir)?;

        // Scan for the last checkpoint LSN.
        let last_checkpoint_lsn = Self::find_last_checkpoint(wal_dir)?;

        Ok(Self {
            writer: Mutex::new(writer),
            wal_dir: wal_dir.to_path_buf(),
            last_checkpoint_lsn_raw: AtomicU64::new(last_checkpoint_lsn.0),
            synced_lsn: AtomicU64::new(0),
            sync_waiters: Mutex::new(()),
            sync_notify: Condvar::new(),
        })
    }

    /// Append a record and return its assigned LSN.
    pub fn append(&self, record: &mut LogRecord) -> Result<Lsn> {
        self.writer.lock().append(record)
    }

    /// Flush + fsync the WAL to disk. Syncs all buffered records.
    pub fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        writer.sync()?;
        // Update synced_lsn to cover everything written so far.
        let new_synced = writer.next_lsn().0.saturating_sub(1);
        self.synced_lsn.store(new_synced, Ordering::Release);
        drop(writer);
        // Wake all threads waiting for durability.
        self.sync_notify.notify_all();
        Ok(())
    }

    /// Group commit: wait until `target_lsn` is durable on disk.
    ///
    /// If another thread already synced past our LSN, returns immediately.
    /// Otherwise, becomes the sync leader (acquires writer lock, fsyncs,
    /// wakes all waiters). Threads that arrive while a sync is in progress
    /// sleep on the Condvar and get woken when the sync completes.
    pub fn sync_to(&self, target_lsn: Lsn) -> Result<()> {
        // Fast path: already synced past our LSN.
        if self.synced_lsn.load(Ordering::Acquire) >= target_lsn.0 {
            return Ok(());
        }

        // Try to become sync leader by acquiring writer lock.
        // If we get it, we fsync. If not, we sleep on the Condvar.
        if let Some(mut writer) = self.writer.try_lock() {
            // Double-check after acquiring lock (another leader may have synced).
            if self.synced_lsn.load(Ordering::Acquire) >= target_lsn.0 {
                return Ok(());
            }

            // We're the sync leader — flush everything buffered.
            writer.sync()?;
            let new_synced = writer.next_lsn().0.saturating_sub(1);
            self.synced_lsn.store(new_synced, Ordering::Release);
            drop(writer);

            // Wake all sleeping waiters.
            self.sync_notify.notify_all();
            return Ok(());
        }

        // Another thread holds the writer (likely syncing). Sleep until woken.
        let mut guard = self.sync_waiters.lock();
        loop {
            if self.synced_lsn.load(Ordering::Acquire) >= target_lsn.0 {
                return Ok(());
            }
            // Sleep until a sync leader wakes us (bounded by one fsync duration).
            self.sync_notify.wait(&mut guard);
        }
    }

    /// Create a reader for this WAL's segments.
    pub fn reader(&self) -> Result<WalReader> {
        WalReader::open(&self.wal_dir)
    }

    /// The next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        self.writer.lock().next_lsn()
    }

    /// The LSN of the last checkpoint record, or `Lsn::INVALID` if none.
    pub fn last_checkpoint_lsn(&self) -> Lsn {
        Lsn::new(self.last_checkpoint_lsn_raw.load(Ordering::Relaxed))
    }

    /// Update the cached checkpoint LSN (called after checkpoint writes).
    pub fn set_last_checkpoint_lsn(&self, lsn: Lsn) {
        self.last_checkpoint_lsn_raw.store(lsn.0, Ordering::Relaxed);
    }

    /// Delete segments whose records all have LSN < the given threshold.
    ///
    /// Never deletes the active (last) segment. Returns the number of
    /// deleted segments.
    pub fn truncate_before(&self, lsn: Lsn) -> Result<u64> {
        let segments = segment::list_segments(&self.wal_dir)?;
        if segments.len() <= 1 {
            return Ok(0);
        }

        let mut deleted: u64 = 0;

        // Never delete the last segment (it's the active one).
        for (_, ref path) in &segments[..segments.len() - 1] {
            // Scan the segment to find its highest LSN.
            let data = std::fs::read(path)?;
            let mut highest_lsn = Lsn::INVALID;
            let mut offset = 0;
            while offset < data.len() {
                match LogRecord::decode(&data[offset..]) {
                    Ok((record, consumed)) => {
                        highest_lsn = record.lsn;
                        offset += consumed;
                    }
                    Err(_) => break,
                }
            }

            // If all records in this segment are before the checkpoint, delete it.
            if highest_lsn.is_valid() && highest_lsn < lsn {
                std::fs::remove_file(path)?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    /// Scan all segments to find the last checkpoint LSN.
    fn find_last_checkpoint(wal_dir: &Path) -> Result<Lsn> {
        let reader = WalReader::open(wal_dir)?;
        if reader.is_empty() {
            return Ok(Lsn::INVALID);
        }

        let mut last_checkpoint = Lsn::INVALID;
        for result in reader.scan_forward(Lsn::new(0)) {
            match result {
                Ok(record) => {
                    if record.record_type() == LogRecordType::Checkpoint {
                        last_checkpoint = record.lsn;
                    }
                }
                Err(_) => break, // Truncated tail.
            }
        }

        Ok(last_checkpoint)
    }
}
