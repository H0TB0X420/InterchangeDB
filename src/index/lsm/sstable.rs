//! SSTable (Sorted String Table) format: write, read, footer, and index.
//!
//! Binary format (low to high offset):
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ Data Block 0..N-1    (each up to 4096B) │
//! │   Entries: key_len:u16 | key |          │
//! │            value_len:u16 | value         │
//! │   (0xFFFF value_len = tombstone)        │
//! │   entry_count:u16 (last 2 bytes)        │
//! ├─────────────────────────────────────────┤
//! │ Bloom Filter Block (variable size)      │
//! ├─────────────────────────────────────────┤
//! │ Index Block (variable size)             │
//! │   Per block: first_key_len:u16 |        │
//! │              first_key | offset:u32     │
//! │   entry_count:u32 (last 4 bytes)        │
//! ├─────────────────────────────────────────┤
//! │ Footer (32 bytes, fixed at EOF)         │
//! │   magic:u32 (0x01545353 = "SST\x01")   │
//! │   block_count:u32                       │
//! │   index_offset:u64, index_size:u32      │
//! │   bloom_offset:u32, bloom_size:u32      │
//! │   checksum:u32 (CRC32 of first 28B)    │
//! └─────────────────────────────────────────┘
//! ```

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::common::error::{Error, Result};
use super::Entry;

/// Magic number: "SST\x01" in little-endian.
const MAGIC: u32 = 0x01545353;

/// Data block size in bytes.
const BLOCK_SIZE: usize = 4096;

/// Usable data capacity per block (block size minus 2-byte entry count).
const BLOCK_DATA_CAPACITY: usize = BLOCK_SIZE - 2;

/// Sentinel value_len indicating a tombstone.
const TOMBSTONE_SENTINEL: u16 = 0xFFFF;

/// Footer size in bytes.
pub const FOOTER_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// SSTableMeta — lightweight metadata kept in memory
// ---------------------------------------------------------------------------

/// In-memory metadata for an SSTable file.
#[derive(Debug, Clone)]
pub struct SSTableMeta {
    pub id: u64,
    pub path: PathBuf,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub file_size: u64,
    pub entry_count: u64,
}

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------

/// 32-byte footer at the end of every SSTable file.
///
/// On-disk layout (all little-endian):
///   [0..4]   magic:u32
///   [4..8]   block_count:u32
///   [8..16]  index_offset:u64
///   [16..20] index_size:u32
///   [20..24] bloom_offset:u32
///   [24..28] bloom_size:u32
///   [28..32] checksum:u32 (CRC32 of bytes [0..28])
#[derive(Debug, Clone, Copy)]
struct Footer {
    magic: u32,
    block_count: u32,
    index_offset: u64,
    index_size: u32,
    /// Stored as u32 on disk. Sufficient for files up to 4GB.
    bloom_offset: u64,
    bloom_size: u32,
    #[allow(dead_code)]
    checksum: u32,
}

impl Footer {
    /// Encode footer to 32 bytes.
    ///
    /// Layout: magic:u32 | block_count:u32 | index_offset:u64 | index_size:u32
    ///         | bloom_offset:u32 | bloom_size:u32 | checksum:u32 = 32 bytes.
    fn encode(&self) -> [u8; FOOTER_SIZE] {
        assert!(
            self.bloom_offset <= u32::MAX as u64,
            "bloom_offset {0} exceeds u32::MAX — SSTable too large",
            self.bloom_offset
        );

        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.block_count.to_le_bytes());
        buf[8..16].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[16..20].copy_from_slice(&self.index_size.to_le_bytes());
        buf[20..24].copy_from_slice(&(self.bloom_offset as u32).to_le_bytes());
        buf[24..28].copy_from_slice(&self.bloom_size.to_le_bytes());

        // Compute CRC32 over first 28 bytes, store in last 4.
        let crc = crc32fast::hash(&buf[..28]);
        buf[28..32].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode footer from 32 bytes. Validates magic and checksum.
    fn decode(buf: &[u8; FOOTER_SIZE]) -> Result<Self> {
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::StorageCorrupted(format!(
                "SSTable bad magic: expected {MAGIC:#010x}, got {magic:#010x}"
            )));
        }

        let stored_checksum = u32::from_le_bytes(buf[28..32].try_into().unwrap());
        let computed_checksum = crc32fast::hash(&buf[..28]);
        if stored_checksum != computed_checksum {
            return Err(Error::StorageCorrupted(format!(
                "SSTable footer checksum mismatch: stored {stored_checksum:#010x}, \
                 computed {computed_checksum:#010x}"
            )));
        }

        Ok(Footer {
            magic,
            block_count: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            index_offset: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            index_size: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            bloom_offset: u32::from_le_bytes(buf[20..24].try_into().unwrap()) as u64,
            bloom_size: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            checksum: stored_checksum,
        })
    }
}

// ---------------------------------------------------------------------------
// Index entry — maps a block's first key to its file offset
// ---------------------------------------------------------------------------

/// One entry in the SSTable index block.
#[derive(Debug, Clone)]
pub(crate) struct IndexEntry {
    pub first_key: Vec<u8>,
    pub block_offset: u32,
}

// ---------------------------------------------------------------------------
// write_sstable — flush sorted entries to a new SSTable file
// ---------------------------------------------------------------------------

/// Write sorted entries to a new SSTable file.
///
/// `entries` must yield `(key, Option<value>)` in sorted key order.
/// `None` values are written as tombstones.
///
/// Returns metadata about the written SSTable, or an error.
/// Returns `None` if the iterator was empty (no file created).
pub fn write_sstable(
    path: &Path,
    id: u64,
    entries: impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
) -> Result<Option<SSTableMeta>> {
    // Write to a sibling .sst.tmp path, fsync, then atomically rename to
    // `path`. Guarantees on crash: either `path` exists as a complete,
    // fsync'd SSTable or it doesn't exist at all. Recovery never sees a
    // half-written final file. Orphan `.sst.tmp` files left by a crash are
    // swept by `Manifest::open` on the next startup.
    let tmp_path = path.with_extension("sst.tmp");
    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::new(file);

    let mut block_buf: Vec<u8> = Vec::with_capacity(BLOCK_SIZE);
    let mut block_entry_count: u16 = 0;
    let mut block_first_key: Option<Vec<u8>> = None;
    let mut index_entries: Vec<IndexEntry> = Vec::new();
    let mut current_block_offset: u32 = 0;
    let mut total_entry_count: u64 = 0;
    let mut first_key: Option<Vec<u8>> = None;
    let mut last_key: Option<Vec<u8>> = None;
    let mut all_keys: Vec<Vec<u8>> = Vec::new();

    for (key, value) in entries {
        // Calculate entry size: key_len(2) + key + value_len(2) + value.
        let entry_size = 2 + key.len() + 2 + value.as_ref().map_or(0, |v| v.len());

        // Would this entry overflow the current block?
        if !block_buf.is_empty() && block_buf.len() + entry_size > BLOCK_DATA_CAPACITY {
            // Flush current block.
            flush_block(
                &mut writer,
                &mut block_buf,
                block_entry_count,
                &mut index_entries,
                block_first_key.take().unwrap(),
                current_block_offset,
            )?;
            current_block_offset += BLOCK_SIZE as u32;
            block_entry_count = 0;
        }

        // Track first key of the block.
        if block_first_key.is_none() {
            block_first_key = Some(key.clone());
        }

        // Track global first/last key.
        if first_key.is_none() {
            first_key = Some(key.clone());
        }
        last_key = Some(key.clone());

        // Collect key for bloom filter (built later in subtask 3.4.3).
        all_keys.push(key.clone());

        // Write entry into block buffer.
        let key_len = key.len() as u16;
        block_buf.extend_from_slice(&key_len.to_le_bytes());
        block_buf.extend_from_slice(&key);

        match &value {
            Some(v) => {
                let value_len = v.len() as u16;
                block_buf.extend_from_slice(&value_len.to_le_bytes());
                block_buf.extend_from_slice(v);
            }
            None => {
                block_buf.extend_from_slice(&TOMBSTONE_SENTINEL.to_le_bytes());
            }
        }

        block_entry_count += 1;
        total_entry_count += 1;
    }

    // If no entries were written, remove the empty tmp file and return None.
    if total_entry_count == 0 {
        drop(writer);
        let _ = std::fs::remove_file(&tmp_path);
        return Ok(None);
    }

    // Flush the last block.
    assert!(block_first_key.is_some(), "last block must have a first key");
    flush_block(
        &mut writer,
        &mut block_buf,
        block_entry_count,
        &mut index_entries,
        block_first_key.take().unwrap(),
        current_block_offset,
    )?;

    let block_count = index_entries.len() as u32;

    // --- Write bloom filter ---
    let bloom_offset: u32 = current_block_offset + BLOCK_SIZE as u32;
    let key_refs: Vec<&[u8]> = all_keys.iter().map(|k| k.as_slice()).collect();
    let bloom = super::bloom::BloomFilter::build(&key_refs);
    let bloom_bytes = bloom.encode();
    let bloom_size = bloom_bytes.len() as u32;
    writer.write_all(&bloom_bytes)?;

    // --- Write index block ---
    let index_offset = bloom_offset as u64 + bloom_size as u64;
    let index_start = writer.stream_position()?;
    assert_eq!(
        index_start, index_offset,
        "index offset mismatch: expected {index_offset}, got {index_start}"
    );

    for entry in &index_entries {
        let key_len = entry.first_key.len() as u16;
        writer.write_all(&key_len.to_le_bytes())?;
        writer.write_all(&entry.first_key)?;
        writer.write_all(&entry.block_offset.to_le_bytes())?;
    }
    writer.write_all(&block_count.to_le_bytes())?;

    let index_end = writer.stream_position()?;
    let index_size = (index_end - index_start) as u32;

    // --- Write footer ---
    let footer = Footer {
        magic: MAGIC,
        block_count,
        index_offset,
        index_size,
        bloom_offset: bloom_offset as u64,
        bloom_size,
        checksum: 0, // Placeholder — encode() computes it.
    };
    writer.write_all(&footer.encode())?;
    writer.flush()?;

    let file_size = writer.stream_position()?;

    // Recover the File from BufWriter so we can fsync. Drop before rename
    // so no handle survives the rename on platforms that care.
    let file = writer.into_inner().map_err(|e| e.into_error())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp_path, path)?;

    // Best-effort: fsync the parent directory so the rename itself is
    // durable on POSIX. Failure here doesn't roll back — the file is
    // already at its final path with valid, fsync'd contents.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(Some(SSTableMeta {
        id,
        path: path.to_path_buf(),
        first_key: first_key.unwrap(),
        last_key: last_key.unwrap(),
        file_size,
        entry_count: total_entry_count,
    }))
}

/// Flush a data block: pad to BLOCK_SIZE, write entry count in last 2 bytes.
fn flush_block(
    writer: &mut BufWriter<File>,
    block_buf: &mut Vec<u8>,
    entry_count: u16,
    index_entries: &mut Vec<IndexEntry>,
    first_key: Vec<u8>,
    block_offset: u32,
) -> io::Result<()> {
    assert!(
        block_buf.len() <= BLOCK_DATA_CAPACITY,
        "block data overflow: {} > {BLOCK_DATA_CAPACITY}",
        block_buf.len()
    );

    // Record index entry for this block.
    index_entries.push(IndexEntry {
        first_key,
        block_offset,
    });

    // Pad to BLOCK_SIZE - 2, then write entry count.
    let padding = BLOCK_DATA_CAPACITY - block_buf.len();
    block_buf.resize(BLOCK_DATA_CAPACITY, 0);
    let _ = padding; // Suppress unused variable.
    block_buf.extend_from_slice(&entry_count.to_le_bytes());

    assert_eq!(block_buf.len(), BLOCK_SIZE);
    writer.write_all(block_buf)?;
    block_buf.clear();

    Ok(())
}

// ---------------------------------------------------------------------------
// SSTableReader — read an SSTable from disk
// ---------------------------------------------------------------------------

/// Reader for an SSTable file. Loads footer + index on open.
pub struct SSTableReader {
    file: BufReader<File>,
    footer: Footer,
    index: Vec<IndexEntry>,
    pub meta: SSTableMeta,
    #[allow(dead_code)]
    bloom: Option<super::bloom::BloomFilter>,
}

impl SSTableReader {
    /// Open an SSTable file, read its footer and index block.
    pub fn open(path: &Path, id: u64) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let file_size = file.seek(SeekFrom::End(0))?;

        assert!(
            file_size >= FOOTER_SIZE as u64,
            "SSTable file too small: {file_size} < {FOOTER_SIZE}"
        );

        // Read footer.
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)?;
        let footer = Footer::decode(&footer_buf)?;

        // Read index block.
        file.seek(SeekFrom::Start(footer.index_offset))?;
        let mut index_buf = vec![0u8; footer.index_size as usize];
        file.read_exact(&mut index_buf)?;

        let index = decode_index_block(&index_buf, footer.block_count)?;

        // Read bloom filter (if present).
        let bloom = if footer.bloom_size > 0 {
            file.seek(SeekFrom::Start(footer.bloom_offset))?;
            let mut bloom_buf = vec![0u8; footer.bloom_size as usize];
            file.read_exact(&mut bloom_buf)?;
            Some(super::bloom::BloomFilter::decode(&bloom_buf)?)
        } else {
            None
        };

        // Derive first_key and last_key from index.
        let first_key = index
            .first()
            .map(|e| e.first_key.clone())
            .unwrap_or_default();

        // For last_key we need to read the last block and find its last entry.
        let last_key = if let Some(last_index) = index.last() {
            read_last_key_in_block(&mut file, last_index.block_offset as u64)?
        } else {
            Vec::new()
        };

        // Count entries across all blocks.
        let entry_count = count_all_entries(&mut file, &index)?;

        let meta = SSTableMeta {
            id,
            path: path.to_path_buf(),
            first_key,
            last_key,
            file_size,
            entry_count,
        };

        Ok(Self {
            file,
            footer,
            index,
            meta,
            bloom,
        })
    }

    /// Point lookup. Returns:
    /// - `Ok(Some(Some(value)))` — key found with a value.
    /// - `Ok(Some(None))` — key found as tombstone.
    /// - `Ok(None)` — key not in this SSTable.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        // Check bloom filter first (if available).
        if let Some(ref bloom) = self.bloom {
            if !bloom.may_contain(key) {
                return Ok(None);
            }
        }

        // Key range check.
        if key < self.meta.first_key.as_slice() || key > self.meta.last_key.as_slice() {
            return Ok(None);
        }

        // Binary search the index to find the right block.
        let block_idx = self.find_block_for_key(key);

        // Read and scan that block.
        let block_offset = self.index[block_idx].block_offset as u64;
        let entries = self.read_block(block_offset)?;

        for (k, v) in entries {
            if k == key {
                return Ok(Some(v));
            }
            // Entries are sorted; if we've passed the key, it's not here.
            if k.as_slice() > key {
                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Check if the SSTable might contain a key (bloom + range check).
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if key < self.meta.first_key.as_slice() || key > self.meta.last_key.as_slice() {
            return false;
        }
        if let Some(ref bloom) = self.bloom {
            return bloom.may_contain(key);
        }
        true
    }

    /// Number of data blocks.
    pub fn block_count(&self) -> u32 {
        self.footer.block_count
    }

    /// Binary search the index to find which block could contain `key`.
    ///
    /// Returns the index of the last block whose first_key <= key.
    fn find_block_for_key(&self, key: &[u8]) -> usize {
        // Binary search: find the rightmost block where first_key <= key.
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.index[mid].first_key.as_slice() <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // lo is the first block where first_key > key, so the target is lo - 1.
        assert!(lo > 0, "key is before the first block's first key");
        lo - 1
    }

    /// Read all entries from a single data block at the given offset.
    fn read_block(
        &mut self,
        offset: u64,
    ) -> Result<Vec<Entry>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; BLOCK_SIZE];
        self.file.read_exact(&mut buf)?;

        let entry_count =
            u16::from_le_bytes(buf[BLOCK_DATA_CAPACITY..BLOCK_SIZE].try_into().unwrap());
        decode_block_entries(&buf[..BLOCK_DATA_CAPACITY], entry_count)
    }

    /// Read all entries in this SSTable in sorted order.
    pub fn iter(&mut self) -> Result<Vec<Entry>> {
        let offsets: Vec<u64> = self.index.iter().map(|e| e.block_offset as u64).collect();
        let mut all_entries = Vec::new();
        for offset in offsets {
            let entries = self.read_block(offset)?;
            all_entries.extend(entries);
        }
        Ok(all_entries)
    }
}

// ---------------------------------------------------------------------------
// Block and index decoding helpers
// ---------------------------------------------------------------------------

/// Decode entries from a data block's raw bytes.
fn decode_block_entries(
    data: &[u8],
    entry_count: u16,
) -> Result<Vec<Entry>> {
    let mut entries = Vec::with_capacity(entry_count as usize);
    let mut pos = 0;

    for _ in 0..entry_count {
        if pos + 2 > data.len() {
            return Err(Error::StorageCorrupted("block entry truncated (key_len)".into()));
        }
        let key_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        if pos + key_len > data.len() {
            return Err(Error::StorageCorrupted("block entry truncated (key)".into()));
        }
        let key = data[pos..pos + key_len].to_vec();
        pos += key_len;

        if pos + 2 > data.len() {
            return Err(Error::StorageCorrupted("block entry truncated (value_len)".into()));
        }
        let value_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap());
        pos += 2;

        if value_len == TOMBSTONE_SENTINEL {
            entries.push((key, None));
        } else {
            let vlen = value_len as usize;
            if pos + vlen > data.len() {
                return Err(Error::StorageCorrupted("block entry truncated (value)".into()));
            }
            let value = data[pos..pos + vlen].to_vec();
            pos += vlen;
            entries.push((key, Some(value)));
        }
    }

    Ok(entries)
}

/// Decode the index block.
fn decode_index_block(data: &[u8], expected_count: u32) -> Result<Vec<IndexEntry>> {
    if data.len() < 4 {
        return Err(Error::StorageCorrupted("index block too small".into()));
    }

    // Last 4 bytes = entry count.
    let count_bytes = &data[data.len() - 4..];
    let count = u32::from_le_bytes(count_bytes.try_into().unwrap());
    assert_eq!(
        count, expected_count,
        "index block count {count} != footer block_count {expected_count}"
    );

    let mut entries = Vec::with_capacity(count as usize);
    let mut pos = 0;
    let payload = &data[..data.len() - 4];

    for _ in 0..count {
        if pos + 2 > payload.len() {
            return Err(Error::StorageCorrupted("index entry truncated (key_len)".into()));
        }
        let key_len = u16::from_le_bytes(payload[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;

        if pos + key_len > payload.len() {
            return Err(Error::StorageCorrupted("index entry truncated (key)".into()));
        }
        let first_key = payload[pos..pos + key_len].to_vec();
        pos += key_len;

        if pos + 4 > payload.len() {
            return Err(Error::StorageCorrupted("index entry truncated (offset)".into()));
        }
        let block_offset = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap());
        pos += 4;

        entries.push(IndexEntry {
            first_key,
            block_offset,
        });
    }

    Ok(entries)
}

/// Read the last key in a data block at the given offset.
fn read_last_key_in_block(file: &mut BufReader<File>, offset: u64) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = [0u8; BLOCK_SIZE];
    file.read_exact(&mut buf)?;

    let entry_count =
        u16::from_le_bytes(buf[BLOCK_DATA_CAPACITY..BLOCK_SIZE].try_into().unwrap());
    let entries = decode_block_entries(&buf[..BLOCK_DATA_CAPACITY], entry_count)?;

    assert!(!entries.is_empty(), "block has no entries");
    Ok(entries.last().unwrap().0.clone())
}

/// Count total entries across all blocks.
fn count_all_entries(
    file: &mut BufReader<File>,
    index: &[IndexEntry],
) -> Result<u64> {
    let mut total = 0u64;
    for entry in index {
        file.seek(SeekFrom::Start(entry.block_offset as u64))?;
        let mut count_buf = [0u8; 2];
        // Entry count is at offset BLOCK_DATA_CAPACITY within the block.
        file.seek(SeekFrom::Start(
            entry.block_offset as u64 + BLOCK_DATA_CAPACITY as u64,
        ))?;
        file.read_exact(&mut count_buf)?;
        let count = u16::from_le_bytes(count_buf);
        total += count as u64;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_dir() -> TempDir {
        TempDir::new().expect("failed to create temp dir")
    }

    #[test]
    fn footer_encode_decode_roundtrip() {
        let footer = Footer {
            magic: MAGIC,
            block_count: 5,
            index_offset: 20480,
            index_size: 128,
            bloom_offset: 20480,
            bloom_size: 0,
            checksum: 0,
        };
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();

        assert_eq!(decoded.magic, MAGIC);
        assert_eq!(decoded.block_count, 5);
        assert_eq!(decoded.index_offset, 20480);
        assert_eq!(decoded.index_size, 128);
    }

    #[test]
    fn footer_bad_magic() {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_le_bytes());
        let result = Footer::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn footer_bad_checksum() {
        let footer = Footer {
            magic: MAGIC,
            block_count: 1,
            index_offset: 4096,
            index_size: 10,
            bloom_offset: 4096,
            bloom_size: 0,
            checksum: 0,
        };
        let mut encoded = footer.encode();
        // Corrupt one byte in the middle.
        encoded[10] ^= 0xFF;
        let result = Footer::decode(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn write_and_read_single_block() {
        let dir = test_dir();
        let path = dir.path().join("000001.sst");

        let entries = vec![
            (b"alice".to_vec(), Some(b"100".to_vec())),
            (b"bob".to_vec(), Some(b"200".to_vec())),
            (b"charlie".to_vec(), Some(b"300".to_vec())),
        ];

        let meta = write_sstable(&path, 1, entries.into_iter())
            .unwrap()
            .unwrap();
        assert_eq!(meta.id, 1);
        assert_eq!(meta.first_key, b"alice");
        assert_eq!(meta.last_key, b"charlie");
        assert_eq!(meta.entry_count, 3);

        let mut reader = SSTableReader::open(&path, 1).unwrap();
        assert_eq!(reader.block_count(), 1);

        // Point lookups.
        assert_eq!(
            reader.get(b"alice").unwrap(),
            Some(Some(b"100".to_vec()))
        );
        assert_eq!(
            reader.get(b"bob").unwrap(),
            Some(Some(b"200".to_vec()))
        );
        assert_eq!(
            reader.get(b"charlie").unwrap(),
            Some(Some(b"300".to_vec()))
        );
        assert_eq!(reader.get(b"dave").unwrap(), None);
        assert_eq!(reader.get(b"aaa").unwrap(), None);
    }

    #[test]
    fn write_and_read_multi_block() {
        let dir = test_dir();
        let path = dir.path().join("000002.sst");

        // Generate enough entries to span multiple blocks.
        // Each entry: key_len(2) + key(10) + value_len(2) + value(100) = 114 bytes.
        // Block capacity = 4094 bytes. ~35 entries per block.
        let mut entries = Vec::new();
        for i in 0u32..200 {
            let key = format!("key_{:06}", i).into_bytes();
            let value = vec![b'v'; 100];
            entries.push((key, Some(value)));
        }

        let meta = write_sstable(&path, 2, entries.into_iter())
            .unwrap()
            .unwrap();
        assert_eq!(meta.entry_count, 200);

        let mut reader = SSTableReader::open(&path, 2).unwrap();
        assert!(reader.block_count() > 1, "expected multiple blocks");

        // Spot-check lookups.
        let result = reader.get(b"key_000000").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().unwrap().len(), 100);

        let result = reader.get(b"key_000199").unwrap();
        assert!(result.is_some());

        assert_eq!(reader.get(b"key_000200").unwrap(), None);
    }

    #[test]
    fn tombstone_encoding() {
        let dir = test_dir();
        let path = dir.path().join("000003.sst");

        let entries = vec![
            (b"alive".to_vec(), Some(b"yes".to_vec())),
            (b"dead".to_vec(), None), // Tombstone.
            (b"zombie".to_vec(), Some(b"no".to_vec())),
        ];

        let meta = write_sstable(&path, 3, entries.into_iter())
            .unwrap()
            .unwrap();
        assert_eq!(meta.entry_count, 3);

        let mut reader = SSTableReader::open(&path, 3).unwrap();
        assert_eq!(
            reader.get(b"alive").unwrap(),
            Some(Some(b"yes".to_vec()))
        );
        assert_eq!(reader.get(b"dead").unwrap(), Some(None)); // Tombstone.
        assert_eq!(
            reader.get(b"zombie").unwrap(),
            Some(Some(b"no".to_vec()))
        );
    }

    #[test]
    fn iterator_forward() {
        let dir = test_dir();
        let path = dir.path().join("000004.sst");

        let entries = vec![
            (b"a".to_vec(), Some(b"1".to_vec())),
            (b"b".to_vec(), None),
            (b"c".to_vec(), Some(b"3".to_vec())),
        ];

        write_sstable(&path, 4, entries.into_iter()).unwrap();
        let mut reader = SSTableReader::open(&path, 4).unwrap();

        let all = reader.iter().unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0], (b"a".to_vec(), Some(b"1".to_vec())));
        assert_eq!(all[1], (b"b".to_vec(), None));
        assert_eq!(all[2], (b"c".to_vec(), Some(b"3".to_vec())));
    }

    #[test]
    fn empty_sstable() {
        let dir = test_dir();
        let path = dir.path().join("000005.sst");

        let result = write_sstable(&path, 5, std::iter::empty()).unwrap();
        assert!(result.is_none());
        assert!(!path.exists(), "empty SSTable should not create a file");
    }

    #[test]
    fn bloom_filter_persists_across_reopen() {
        let dir = test_dir();
        let path = dir.path().join("000007.sst");

        // Write SSTable with enough keys to make the bloom filter meaningful.
        let mut entries = Vec::new();
        for i in 0u32..100 {
            let key = format!("key_{i:04}").into_bytes();
            entries.push((key, Some(b"val".to_vec())));
        }
        write_sstable(&path, 7, entries.into_iter()).unwrap();

        // Re-open from disk — bloom filter should load correctly.
        let reader = SSTableReader::open(&path, 7).unwrap();
        assert!(
            reader.bloom.is_some(),
            "bloom filter should be loaded on re-open"
        );

        // Bloom should accept inserted keys.
        assert!(reader.may_contain(b"key_0000"));
        assert!(reader.may_contain(b"key_0050"));
        assert!(reader.may_contain(b"key_0099"));

        // Bloom should reject keys outside the range (may have FPs, but
        // definitely-absent keys should return false most of the time).
        let mut rejected = 0;
        for i in 200u32..300 {
            let key = format!("key_{i:04}").into_bytes();
            if !reader.may_contain(&key) {
                rejected += 1;
            }
        }
        assert!(
            rejected > 80,
            "bloom filter should reject most non-existent keys, but only rejected {rejected}/100"
        );
    }

    #[test]
    fn footer_bloom_fields_roundtrip() {
        // Verify bloom_offset and bloom_size survive encode → decode.
        let footer = Footer {
            magic: MAGIC,
            block_count: 3,
            index_offset: 12500,
            index_size: 200,
            bloom_offset: 12288,
            bloom_size: 212,
            checksum: 0,
        };
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();

        assert_eq!(decoded.bloom_offset, 12288);
        assert_eq!(decoded.bloom_size, 212);
        assert_eq!(decoded.index_offset, 12500);
        assert_eq!(decoded.index_size, 200);
    }

    #[test]
    fn large_entries() {
        let dir = test_dir();
        let path = dir.path().join("000006.sst");

        // Single entry that nearly fills a block.
        // key_len(2) + key(100) + value_len(2) + value(3900) = 4004 < 4094 ✓
        let key = vec![b'k'; 100];
        let value = vec![b'v'; 3900];
        let entries = vec![(key.clone(), Some(value.clone()))];

        let meta = write_sstable(&path, 6, entries.into_iter())
            .unwrap()
            .unwrap();
        assert_eq!(meta.entry_count, 1);

        let mut reader = SSTableReader::open(&path, 6).unwrap();
        let result = reader.get(&key).unwrap().unwrap().unwrap();
        assert_eq!(result, value);
    }
}
