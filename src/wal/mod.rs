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
    /// Highest LSN that has been fsync'd to disk. Advanced with `fetch_max`
    /// so it never regresses (a concurrent checkpoint `sync` and a commit
    /// batch can publish out of order).
    synced_lsn: AtomicU64,
    /// Group-commit leadership token, *separate* from `writer`. Only its
    /// holder performs the fsync — and it flushes the buffer then releases
    /// the writer lock *before* fsyncing, so appends proceed during the
    /// fsync and accumulate into the next batch.
    sync_lock: Mutex<()>,
    /// Count of actual fsync syscalls. With group commit this is well below
    /// the commit count under concurrency; used to observe/verify batching.
    fsync_count: AtomicU64,
    /// Mutex + Condvar for sleeping followers. The Mutex holds no
    /// meaningful state — it pairs with the Condvar.
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
            sync_lock: Mutex::new(()),
            fsync_count: AtomicU64::new(0),
            sync_waiters: Mutex::new(()),
            sync_notify: Condvar::new(),
        })
    }

    /// Append a record and return its assigned LSN.
    pub fn append(&self, record: &mut LogRecord) -> Result<Lsn> {
        self.writer.lock().append(record)
    }

    /// Flush + fsync the WAL to disk. Syncs all buffered records. Used by
    /// the single-threaded paths (checkpoint, `Database` shutdown); commit
    /// durability goes through `sync_to`'s group commit.
    pub fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        writer.sync()?;
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        // Cover everything written so far. `fetch_max` so a concurrent
        // group-commit batch can't be clobbered backwards.
        let new_synced = writer.next_lsn().0.saturating_sub(1);
        self.synced_lsn.fetch_max(new_synced, Ordering::AcqRel);
        drop(writer);
        // Wake all threads waiting for durability.
        self.sync_notify.notify_all();
        Ok(())
    }

    /// Group commit: return once `target_lsn` is durable on disk.
    ///
    /// A committer either (a) finds its LSN already durable, (b) becomes
    /// the sync leader for a batch, or (c) sleeps briefly while another
    /// leader fsyncs and then re-checks / leads the next batch.
    ///
    /// The batching happens because the leader **releases the writer lock
    /// before fsyncing** (flush under the lock, fsync on a cloned handle
    /// without it). So while a leader fsyncs, other committers append their
    /// records into the buffer; the next leader's single flush+fsync makes
    /// all of them durable at once.
    pub fn sync_to(&self, target_lsn: Lsn) -> Result<()> {
        loop {
            // (a) Already durable.
            if self.synced_lsn.load(Ordering::Acquire) >= target_lsn.0 {
                return Ok(());
            }

            // (b) Try to lead a sync batch. Leadership is a dedicated lock,
            // distinct from the writer lock.
            if let Some(_leader) = self.sync_lock.try_lock() {
                // A prior batch may have just covered us.
                if self.synced_lsn.load(Ordering::Acquire) >= target_lsn.0 {
                    return Ok(());
                }
                // Flush under the writer lock, capturing what we'll fsync,
                // then drop the writer lock so appends resume during fsync.
                let (flushed_lsn, file) = {
                    let mut writer = self.writer.lock();
                    writer.flush_buffer()?;
                    let flushed = writer.next_lsn().0.saturating_sub(1);
                    let file = writer.active_file_clone()?;
                    (flushed, file)
                };
                // fsync — no writer lock held. We appended `target_lsn`
                // before calling sync_to, so `flushed_lsn >= target_lsn`:
                // leading always makes our own commit durable.
                file.sync_data()?;
                self.fsync_count.fetch_add(1, Ordering::Relaxed);
                self.synced_lsn.fetch_max(flushed_lsn, Ordering::AcqRel);
                self.sync_notify.notify_all();
                return Ok(());
            }

            // (c) A leader is mid-fsync. Sleep briefly, then loop to
            // re-check or lead the next batch. The short timeout guarantees
            // liveness even if a wake-up is missed (a lone late committer
            // re-checks and leads its own batch rather than stalling).
            let mut guard = self.sync_waiters.lock();
            if self.synced_lsn.load(Ordering::Acquire) < target_lsn.0 {
                let _ = self
                    .sync_notify
                    .wait_for(&mut guard, std::time::Duration::from_millis(1));
            }
        }
    }

    /// Number of fsync syscalls issued so far. Under group commit this
    /// sits well below the commit count when commits overlap.
    pub fn fsync_count(&self) -> u64 {
        self.fsync_count.load(Ordering::Relaxed)
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
