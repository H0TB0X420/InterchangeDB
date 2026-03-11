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

use crate::common::Result;

/// WAL facade — combines writer and reader with checkpoint tracking.
///
/// This is the main entry point for WAL operations. It owns the writer
/// and provides reader access for recovery.
pub struct Wal {
    writer: WalWriter,
    wal_dir: PathBuf,
    pub(crate) last_checkpoint_lsn: Lsn,
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
            writer,
            wal_dir: wal_dir.to_path_buf(),
            last_checkpoint_lsn,
        })
    }

    /// Append a record and return its assigned LSN.
    pub fn append(&mut self, record: &mut LogRecord) -> Result<Lsn> {
        self.writer.append(record)
    }

    /// Flush + fsync the WAL to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.sync()
    }

    /// Create a reader for this WAL's segments.
    pub fn reader(&self) -> Result<WalReader> {
        WalReader::open(&self.wal_dir)
    }

    /// The next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        self.writer.next_lsn()
    }

    /// The LSN of the last checkpoint record, or `Lsn::INVALID` if none.
    pub fn last_checkpoint_lsn(&self) -> Lsn {
        self.last_checkpoint_lsn
    }

    /// Delete segments whose records all have LSN < the given threshold.
    ///
    /// Never deletes the active (last) segment. Returns the number of
    /// deleted segments.
    pub fn truncate_before(&mut self, lsn: Lsn) -> Result<u64> {
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
