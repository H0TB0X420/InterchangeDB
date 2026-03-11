//! WAL reader — scans log records across segments for recovery.
//!
//! The reader lazily iterates through segment files, decoding records
//! one at a time. Truncated tails on the *last* segment are treated as
//! the crash boundary (not an error). Truncation on earlier segments
//! is corruption.

use std::path::{Path, PathBuf};

use crate::common::{Error, Result};
use crate::wal::segment;
use crate::wal::{LogRecord, Lsn};

/// Upper bound on records during recovery to prevent infinite loops.
const MAX_RECOVERY_RECORDS: u64 = 100_000_000;

/// WAL reader — opens a WAL directory and iterates records.
pub struct WalReader {
    segments: Vec<(u64, PathBuf)>,
}

impl WalReader {
    /// Open a WAL directory for reading.
    pub fn open(wal_dir: &Path) -> Result<Self> {
        let segments = segment::list_segments(wal_dir)?;
        Ok(Self { segments })
    }

    /// Return true if there are no segments (empty WAL).
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Iterate records forward from `start_lsn`.
    ///
    /// Records with LSN < start_lsn are skipped. Pass `Lsn::new(0)` to
    /// scan from the beginning.
    pub fn scan_forward(&self, start_lsn: Lsn) -> WalForwardIter<'_> {
        WalForwardIter {
            segments: &self.segments,
            segment_index: 0,
            segment_data: Vec::new(),
            offset: 0,
            segment_loaded: false,
            start_lsn,
            records_yielded: 0,
            done: false,
        }
    }
}

/// Forward iterator over WAL records, reading segments lazily.
pub struct WalForwardIter<'a> {
    segments: &'a [(u64, PathBuf)],
    segment_index: usize,
    segment_data: Vec<u8>,
    offset: usize,
    segment_loaded: bool,
    start_lsn: Lsn,
    records_yielded: u64,
    done: bool,
}

impl<'a> Iterator for WalForwardIter<'a> {
    type Item = Result<LogRecord>;

    fn next(&mut self) -> Option<Result<LogRecord>> {
        if self.done {
            return None;
        }

        // Bound check.
        if self.records_yielded >= MAX_RECOVERY_RECORDS {
            self.done = true;
            return Some(Err(Error::StorageCorrupted(format!(
                "WAL recovery exceeded {} records",
                MAX_RECOVERY_RECORDS
            ))));
        }

        loop {
            // Load the next segment if needed.
            if !self.segment_loaded {
                if self.segment_index >= self.segments.len() {
                    self.done = true;
                    return None;
                }

                let (_, ref path) = self.segments[self.segment_index];
                match std::fs::read(path) {
                    Ok(data) => {
                        self.segment_data = data;
                        self.offset = 0;
                        self.segment_loaded = true;
                    }
                    Err(e) => {
                        self.done = true;
                        return Some(Err(Error::Io(e)));
                    }
                }
            }

            // Try to decode a record from the current segment.
            if self.offset < self.segment_data.len() {
                let is_last_segment = self.segment_index == self.segments.len() - 1;

                match LogRecord::decode(&self.segment_data[self.offset..]) {
                    Ok((record, consumed)) => {
                        self.offset += consumed;

                        // Skip records below start_lsn.
                        if record.lsn < self.start_lsn {
                            continue;
                        }

                        self.records_yielded += 1;
                        return Some(Ok(record));
                    }
                    Err(e) => {
                        if is_last_segment {
                            // Truncated tail on last segment = crash boundary.
                            self.done = true;
                            return None;
                        } else {
                            // Corruption in a non-last segment.
                            self.done = true;
                            return Some(Err(Error::StorageCorrupted(format!(
                                "WAL corruption in segment {}: {}",
                                self.segments[self.segment_index].0, e
                            ))));
                        }
                    }
                }
            }

            // Current segment exhausted — advance to the next.
            self.segment_index += 1;
            self.segment_loaded = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::record::LogPayload;
    use crate::wal::writer::WalWriter;

    /// Helper: write N put records, sync, return the wal_dir path.
    fn write_records(dir: &Path, count: usize) -> PathBuf {
        let wal_dir = dir.join("wal");
        let mut writer = WalWriter::create(&wal_dir).unwrap();
        for i in 0..count {
            let mut record = LogRecord::put(
                format!("key{:04}", i).into_bytes(),
                format!("val{:04}", i).into_bytes(),
            );
            writer.append(&mut record).unwrap();
        }
        writer.sync().unwrap();
        wal_dir
    }

    #[test]
    fn forward_scan_all() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = write_records(dir.path(), 10);

        let reader = WalReader::open(&wal_dir).unwrap();
        assert!(!reader.is_empty());

        let records: Vec<_> = reader
            .scan_forward(Lsn::new(0))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(records.len(), 10);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.lsn, Lsn::new(i as u64));
        }
    }

    #[test]
    fn skip_by_start_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = write_records(dir.path(), 10);

        let reader = WalReader::open(&wal_dir).unwrap();
        let records: Vec<_> = reader
            .scan_forward(Lsn::new(5))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(records.len(), 5);
        assert_eq!(records[0].lsn, Lsn::new(5));
        assert_eq!(records[4].lsn, Lsn::new(9));
    }

    #[test]
    fn empty_wal() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        // Don't create any files.

        let reader = WalReader::open(&wal_dir).unwrap();
        assert!(reader.is_empty());

        let records: Vec<_> = reader
            .scan_forward(Lsn::new(0))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn truncated_tail_last_segment() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = write_records(dir.path(), 5);

        // Truncate the last segment: remove a few bytes from the end.
        let segments = segment::list_segments(&wal_dir).unwrap();
        let (_, path) = &segments[0];
        let data = std::fs::read(path).unwrap();
        let truncated = &data[..data.len() - 10];
        std::fs::write(path, truncated).unwrap();

        let reader = WalReader::open(&wal_dir).unwrap();
        let records: Vec<_> = reader
            .scan_forward(Lsn::new(0))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // Should get 4 complete records (the 5th is truncated).
        assert_eq!(records.len(), 4);
    }

    #[test]
    fn multi_segment_scan() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let mut writer = WalWriter::create(&wal_dir).unwrap();
            writer.set_max_segment_size(100);

            for i in 0..20 {
                let mut record = LogRecord::put(
                    format!("k{:04}", i).into_bytes(),
                    b"v".to_vec(),
                );
                writer.append(&mut record).unwrap();
            }
            writer.sync().unwrap();
        }

        let segments = segment::list_segments(&wal_dir).unwrap();
        assert!(segments.len() > 1);

        let reader = WalReader::open(&wal_dir).unwrap();
        let records: Vec<_> = reader
            .scan_forward(Lsn::new(0))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(records.len(), 20);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.lsn, Lsn::new(i as u64));
            if let LogPayload::Put { ref key, .. } = record.payload {
                assert_eq!(key, &format!("k{:04}", i).into_bytes());
            } else {
                panic!("Expected Put record");
            }
        }
    }

    #[test]
    fn multi_segment_skip_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        {
            let mut writer = WalWriter::create(&wal_dir).unwrap();
            writer.set_max_segment_size(100);

            for i in 0..20 {
                let mut record = LogRecord::put(
                    format!("k{:04}", i).into_bytes(),
                    b"v".to_vec(),
                );
                writer.append(&mut record).unwrap();
            }
            writer.sync().unwrap();
        }

        let reader = WalReader::open(&wal_dir).unwrap();
        let records: Vec<_> = reader
            .scan_forward(Lsn::new(15))
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(records.len(), 5);
        assert_eq!(records[0].lsn, Lsn::new(15));
    }
}
