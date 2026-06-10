//! WAL writer — appends log records to segment files with fsync durability.
//!
//! The writer manages segment rotation: when the active segment exceeds
//! `max_segment_size`, it is closed and a new segment is opened.
//! LSNs are assigned monotonically starting from 0.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::common::Result;
use crate::wal::segment::{self, DEFAULT_MAX_SEGMENT_SIZE, MAX_SEGMENTS};
use crate::wal::{LogRecord, Lsn};

/// WAL writer — appends records to the active segment file.
pub struct WalWriter {
    wal_dir: PathBuf,
    active_writer: BufWriter<File>,
    active_segment_id: u64,
    active_segment_size: u64,
    next_lsn: Lsn,
    max_segment_size: u64,
}

impl WalWriter {
    /// Create a new WAL from scratch in an empty directory.
    ///
    /// The directory is created if it does not exist. Panics if the directory
    /// already contains segment files.
    pub fn create(wal_dir: &std::path::Path) -> Result<Self> {
        std::fs::create_dir_all(wal_dir)?;

        let existing = segment::list_segments(wal_dir)?;
        assert!(
            existing.is_empty(),
            "WalWriter::create called on non-empty WAL directory"
        );

        let segment_id = 0u64;
        let path = wal_dir.join(segment::segment_filename(segment_id));
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            wal_dir: wal_dir.to_path_buf(),
            active_writer: writer,
            active_segment_id: segment_id,
            active_segment_size: 0,
            next_lsn: Lsn::new(0),
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
        })
    }

    /// Resume an existing WAL from disk.
    ///
    /// Scans the last segment to find the highest LSN so that new records
    /// continue the sequence. If no segments exist, creates a fresh WAL.
    pub fn open(wal_dir: &std::path::Path) -> Result<Self> {
        std::fs::create_dir_all(wal_dir)?;

        let segments = segment::list_segments(wal_dir)?;
        if segments.is_empty() {
            return Self::create(wal_dir);
        }

        // Find the highest LSN by scanning the last segment.
        let (last_id, last_path) = segments.last().unwrap();
        let last_data = std::fs::read(last_path)?;
        let segment_size = last_data.len() as u64;

        let mut highest_lsn: Option<Lsn> = None;
        let mut offset = 0;
        while offset < last_data.len() {
            match LogRecord::decode(&last_data[offset..]) {
                Ok((record, consumed)) => {
                    highest_lsn = Some(record.lsn);
                    offset += consumed;
                }
                Err(_) => {
                    // Truncated tail — stop scanning.
                    break;
                }
            }
        }

        // Also scan earlier segments if the last segment was empty.
        let next_lsn = match highest_lsn {
            Some(lsn) => Lsn::new(lsn.0 + 1),
            None => {
                // Scan backwards through segments to find any LSN.
                let mut found_lsn = None;
                for (_, seg_path) in segments.iter().rev().skip(1) {
                    let data = std::fs::read(seg_path)?;
                    let mut off = 0;
                    while off < data.len() {
                        match LogRecord::decode(&data[off..]) {
                            Ok((record, consumed)) => {
                                found_lsn = Some(record.lsn);
                                off += consumed;
                            }
                            Err(_) => break,
                        }
                    }
                    if found_lsn.is_some() {
                        break;
                    }
                }
                match found_lsn {
                    Some(lsn) => Lsn::new(lsn.0 + 1),
                    None => Lsn::new(0),
                }
            }
        };

        // Open the last segment for append.
        let file = OpenOptions::new().append(true).open(last_path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            wal_dir: wal_dir.to_path_buf(),
            active_writer: writer,
            active_segment_id: *last_id,
            active_segment_size: segment_size,
            next_lsn,
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
        })
    }

    /// Append a record to the WAL, assigning it the next LSN.
    ///
    /// The record's `lsn` field is set by this method. Returns the assigned LSN.
    /// The record is buffered — call `sync()` to ensure durability.
    pub fn append(&mut self, record: &mut LogRecord) -> Result<Lsn> {
        // Assign LSN.
        let lsn = self.next_lsn;
        record.lsn = lsn;
        self.next_lsn = Lsn::new(lsn.0 + 1);

        let encoded = record.encode();
        let record_size = encoded.len() as u64;

        // Rotate if this record would exceed the segment size limit.
        // Always allow at least one record per segment.
        if self.active_segment_size > 0
            && self.active_segment_size + record_size > self.max_segment_size
        {
            self.rotate_segment()?;
        }

        self.active_writer.write_all(&encoded)?;
        self.active_segment_size += record_size;

        Ok(lsn)
    }

    /// Flush the buffer and fsync the active segment to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.active_writer.flush()?;
        // `sync_data` (fdatasync on Linux) over `sync_all` (fsync): the WAL
        // only needs the appended bytes durable, not file mtime/atime.
        // POSIX fdatasync still flushes the size metadata required to read
        // the new bytes back, so durability is preserved. ~2x cheaper on
        // Linux ext4/NVMe; no worse elsewhere.
        self.active_writer.get_ref().sync_data()?;
        Ok(())
    }

    /// Push the in-memory buffer to the OS file *without* fsync. Pairs with
    /// a later `sync_data` on a cloned handle (see `active_file_clone`) so
    /// the fsync can run without the writer lock held — the basis of group
    /// commit. Returns once the bytes are in the OS page cache.
    pub fn flush_buffer(&mut self) -> Result<()> {
        self.active_writer.flush()?;
        Ok(())
    }

    /// Dup the active segment's file handle for an unlocked fsync. Called
    /// per-sync so it always targets the *current* segment across rotation.
    /// The clone is used only for `sync_data` (never written through), so
    /// it sharing the file offset with the writer is harmless.
    pub fn active_file_clone(&self) -> Result<File> {
        Ok(self.active_writer.get_ref().try_clone()?)
    }

    /// Close the active segment and open the next one.
    fn rotate_segment(&mut self) -> Result<()> {
        // Flush current segment.
        self.active_writer.flush()?;
        // fdatasync over fsync — see `sync()`.
        self.active_writer.get_ref().sync_data()?;

        // Open next segment.
        let next_id = self.active_segment_id + 1;
        assert!(
            next_id < MAX_SEGMENTS,
            "WAL segment ID {} exceeds MAX_SEGMENTS {}",
            next_id,
            MAX_SEGMENTS
        );

        let path = self.wal_dir.join(segment::segment_filename(next_id));
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);

        self.active_writer = writer;
        self.active_segment_id = next_id;
        self.active_segment_size = 0;

        Ok(())
    }

    /// The next LSN that will be assigned.
    pub fn next_lsn(&self) -> Lsn {
        self.next_lsn
    }

    /// The WAL directory path.
    pub fn wal_dir(&self) -> &std::path::Path {
        &self.wal_dir
    }

    /// Set the maximum segment size (for testing).
    #[cfg(test)]
    pub fn set_max_segment_size(&mut self, size: u64) {
        self.max_segment_size = size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::record::LogPayload;

    #[test]
    fn create_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = WalWriter::create(&wal_dir).unwrap();

        let mut r1 = LogRecord::put(b"key1".to_vec(), b"val1".to_vec());
        let lsn1 = writer.append(&mut r1).unwrap();
        assert_eq!(lsn1, Lsn::new(0));
        assert_eq!(r1.lsn, Lsn::new(0));

        let mut r2 = LogRecord::delete(b"key2".to_vec());
        let lsn2 = writer.append(&mut r2).unwrap();
        assert_eq!(lsn2, Lsn::new(1));

        writer.sync().unwrap();

        // Verify the segment file exists and has data.
        let segments = segment::list_segments(&wal_dir).unwrap();
        assert_eq!(segments.len(), 1);
        assert!(std::fs::metadata(&segments[0].1).unwrap().len() > 0);
    }

    #[test]
    fn lsn_monotonicity() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut writer = WalWriter::create(&wal_dir).unwrap();

        let mut prev_lsn = None;
        for i in 0..100 {
            let mut record = LogRecord::put(
                format!("key{}", i).into_bytes(),
                format!("val{}", i).into_bytes(),
            );
            let lsn = writer.append(&mut record).unwrap();

            if let Some(prev) = prev_lsn {
                assert!(lsn > prev, "LSN must be monotonically increasing");
            }
            prev_lsn = Some(lsn);
        }
        writer.sync().unwrap();
    }

    #[test]
    fn segment_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        let mut writer = WalWriter::create(&wal_dir).unwrap();

        // Set a tiny segment size to force rotation.
        writer.set_max_segment_size(100);

        // Write enough records to cause multiple rotations.
        for i in 0..20 {
            let mut record = LogRecord::put(
                format!("key{:04}", i).into_bytes(),
                format!("value{:04}", i).into_bytes(),
            );
            writer.append(&mut record).unwrap();
        }
        writer.sync().unwrap();

        let segments = segment::list_segments(&wal_dir).unwrap();
        assert!(
            segments.len() > 1,
            "Expected multiple segments, got {}",
            segments.len()
        );

        // Segment IDs should be consecutive.
        for (i, (id, _)) in segments.iter().enumerate() {
            assert_eq!(*id, i as u64);
        }
    }

    #[test]
    fn reopen_continues_lsn() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write some records.
        {
            let mut writer = WalWriter::create(&wal_dir).unwrap();
            for _ in 0..5 {
                let mut record = LogRecord::put(b"k".to_vec(), b"v".to_vec());
                writer.append(&mut record).unwrap();
            }
            writer.sync().unwrap();
        }

        // Reopen and verify LSN continues.
        {
            let mut writer = WalWriter::open(&wal_dir).unwrap();
            assert_eq!(writer.next_lsn(), Lsn::new(5));

            let mut record = LogRecord::put(b"k2".to_vec(), b"v2".to_vec());
            let lsn = writer.append(&mut record).unwrap();
            assert_eq!(lsn, Lsn::new(5));
            writer.sync().unwrap();
        }
    }

    #[test]
    fn append_then_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = WalWriter::create(&wal_dir).unwrap();

        let mut r1 = LogRecord::put(b"hello".to_vec(), b"world".to_vec());
        writer.append(&mut r1).unwrap();

        let mut r2 = LogRecord::delete(b"goodbye".to_vec());
        writer.append(&mut r2).unwrap();

        writer.sync().unwrap();

        // Read the segment file and decode records.
        let segments = segment::list_segments(&wal_dir).unwrap();
        let data = std::fs::read(&segments[0].1).unwrap();

        let (decoded1, consumed) = LogRecord::decode(&data).unwrap();
        assert_eq!(decoded1.lsn, Lsn::new(0));
        assert_eq!(
            decoded1.payload,
            LogPayload::Put {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
            }
        );

        let (decoded2, _) = LogRecord::decode(&data[consumed..]).unwrap();
        assert_eq!(decoded2.lsn, Lsn::new(1));
        assert_eq!(
            decoded2.payload,
            LogPayload::Delete {
                key: b"goodbye".to_vec(),
            }
        );
    }

    #[test]
    fn multi_segment_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = dir.path().join("wal");

        // Write across multiple segments.
        {
            let mut writer = WalWriter::create(&wal_dir).unwrap();
            writer.set_max_segment_size(100);

            for i in 0..20 {
                let mut record = LogRecord::put(format!("key{:04}", i).into_bytes(), b"v".to_vec());
                writer.append(&mut record).unwrap();
            }
            writer.sync().unwrap();

            let segments = segment::list_segments(&wal_dir).unwrap();
            assert!(segments.len() > 1);
        }

        // Reopen and continue.
        {
            let mut writer = WalWriter::open(&wal_dir).unwrap();
            assert_eq!(writer.next_lsn(), Lsn::new(20));

            let mut record = LogRecord::put(b"new".to_vec(), b"v".to_vec());
            let lsn = writer.append(&mut record).unwrap();
            assert_eq!(lsn, Lsn::new(20));
            writer.sync().unwrap();
        }
    }
}
