//! Manifest file for tracking SSTable level assignments.
//!
//! The manifest is an append-only text file. Each line is one of:
//! - `ADD level sst_id first_key_hex last_key_hex file_size entry_count`
//! - `DEL sst_id`
//!
//! On startup, the manifest is replayed to reconstruct the `LevelState`.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::common::error::{Error, Result};
use crate::index::lsm::config::MAX_LEVEL_COUNT;
use crate::index::lsm::sstable::SSTableMeta;

/// Encode bytes as lowercase hex string.
fn hex_encode(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for byte in data {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// Decode a hex string to bytes.
fn hex_decode(s: &str) -> std::result::Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("odd-length hex string".into());
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        let byte = u8::from_str_radix(&s[i..i + 2], 16)
            .map_err(|e| format!("invalid hex at offset {i}: {e}"))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

/// Tracks which SSTables belong to which levels.
#[derive(Clone)]
pub struct LevelState {
    /// levels[0] = L0 SSTables, levels[1] = L1, etc.
    /// L0 may have overlapping key ranges; L1+ are non-overlapping.
    pub levels: Vec<Vec<SSTableMeta>>,
    /// Next SSTable ID to assign.
    pub next_sst_id: u64,
}

impl Default for LevelState {
    fn default() -> Self {
        Self::new()
    }
}

impl LevelState {
    /// Create an empty level state.
    pub fn new() -> Self {
        Self {
            levels: vec![Vec::new(); MAX_LEVEL_COUNT],
            next_sst_id: 1,
        }
    }

    /// Allocate the next SSTable ID.
    pub fn next_id(&mut self) -> u64 {
        let id = self.next_sst_id;
        self.next_sst_id += 1;
        id
    }

    /// Total disk size across all levels.
    pub fn total_disk_size(&self) -> u64 {
        self.levels
            .iter()
            .flat_map(|level| level.iter())
            .map(|meta| meta.file_size)
            .sum()
    }

    /// Size of a specific level in bytes.
    pub fn level_size(&self, level: usize) -> u64 {
        assert!(level < MAX_LEVEL_COUNT, "level {level} >= {MAX_LEVEL_COUNT}");
        self.levels[level].iter().map(|m| m.file_size).sum()
    }
}

/// Append-only manifest file.
pub struct Manifest {
    path: PathBuf,
    file: File,
}

impl Manifest {
    /// Create or open a manifest file. If the file exists, replay it to
    /// reconstruct state. If not, create a new empty manifest.
    pub fn open(path: &Path, sst_dir: &Path) -> Result<(Self, LevelState)> {
        let mut state = LevelState::new();

        if path.exists() {
            // Replay existing manifest.
            let reader = BufReader::new(File::open(path)?);
            for line in reader.lines() {
                let line = line?;
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                replay_line(line, &mut state, sst_dir)?;
            }
        }

        // Open for append.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok((Self { path: path.to_path_buf(), file }, state))
    }

    /// Record that an SSTable was added to a level.
    pub fn log_add(&mut self, level: usize, meta: &SSTableMeta) -> Result<()> {
        let first_hex = hex_encode(&meta.first_key);
        let last_hex = hex_encode(&meta.last_key);
        let line = format!(
            "ADD {} {} {} {} {} {}\n",
            level, meta.id, first_hex, last_hex, meta.file_size, meta.entry_count
        );
        self.file.write_all(line.as_bytes())?;
        self.file.flush()?;
        Ok(())
    }

    /// Record that an SSTable was deleted.
    pub fn log_del(&mut self, sst_id: u64) -> Result<()> {
        let line = format!("DEL {}\n", sst_id);
        self.file.write_all(line.as_bytes())?;
        self.file.flush()?;
        Ok(())
    }

    /// Path to the manifest file.
    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Replay a single manifest line into the level state.
fn replay_line(line: &str, state: &mut LevelState, sst_dir: &Path) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    match parts.first() {
        Some(&"ADD") => {
            if parts.len() != 7 {
                return Err(Error::StorageCorrupted(format!(
                    "manifest ADD line has {} fields, expected 7: {line}",
                    parts.len()
                )));
            }
            let level: usize = parts[1]
                .parse()
                .map_err(|_| Error::StorageCorrupted(format!("bad level: {}", parts[1])))?;
            let sst_id: u64 = parts[2]
                .parse()
                .map_err(|_| Error::StorageCorrupted(format!("bad sst_id: {}", parts[2])))?;
            let first_key = hex_decode(parts[3])
                .map_err(|e| Error::StorageCorrupted(format!("bad first_key hex: {e}")))?;
            let last_key = hex_decode(parts[4])
                .map_err(|e| Error::StorageCorrupted(format!("bad last_key hex: {e}")))?;
            let file_size: u64 = parts[5]
                .parse()
                .map_err(|_| Error::StorageCorrupted(format!("bad file_size: {}", parts[5])))?;
            let entry_count: u64 = parts[6]
                .parse()
                .map_err(|_| {
                    Error::StorageCorrupted(format!("bad entry_count: {}", parts[6]))
                })?;

            assert!(
                level < MAX_LEVEL_COUNT,
                "manifest ADD level {level} >= {MAX_LEVEL_COUNT}"
            );

            let path = sst_dir.join(format!("{sst_id:06}.sst"));
            let meta = SSTableMeta {
                id: sst_id,
                path,
                first_key,
                last_key,
                file_size,
                entry_count,
            };

            state.levels[level].push(meta);

            // Track the maximum SSTable ID seen.
            if sst_id >= state.next_sst_id {
                state.next_sst_id = sst_id + 1;
            }
        }

        Some(&"DEL") => {
            if parts.len() != 2 {
                return Err(Error::StorageCorrupted(format!(
                    "manifest DEL line has {} fields, expected 2: {line}",
                    parts.len()
                )));
            }
            let sst_id: u64 = parts[1]
                .parse()
                .map_err(|_| Error::StorageCorrupted(format!("bad sst_id: {}", parts[1])))?;

            // Remove the SSTable from whichever level it's in.
            for level in &mut state.levels {
                level.retain(|m| m.id != sst_id);
            }
        }

        _ => {
            return Err(Error::StorageCorrupted(format!(
                "unknown manifest command: {line}"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, PathBuf, PathBuf) {
        let dir = TempDir::new().unwrap();
        let manifest_path = dir.path().join("MANIFEST");
        let sst_dir = dir.path().join("sst");
        std::fs::create_dir_all(&sst_dir).unwrap();
        (dir, manifest_path, sst_dir)
    }

    #[test]
    fn create_new_manifest() {
        let (_dir, manifest_path, sst_dir) = setup();
        let (_manifest, state) = Manifest::open(&manifest_path, &sst_dir).unwrap();
        assert_eq!(state.next_sst_id, 1);
        for level in &state.levels {
            assert!(level.is_empty());
        }
    }

    #[test]
    fn add_and_replay() {
        let (_dir, manifest_path, sst_dir) = setup();

        // Write some entries.
        {
            let (mut manifest, _state) = Manifest::open(&manifest_path, &sst_dir).unwrap();
            let meta = SSTableMeta {
                id: 1,
                path: sst_dir.join("000001.sst"),
                first_key: b"alice".to_vec(),
                last_key: b"charlie".to_vec(),
                file_size: 4096,
                entry_count: 10,
            };
            manifest.log_add(0, &meta).unwrap();

            let meta2 = SSTableMeta {
                id: 2,
                path: sst_dir.join("000002.sst"),
                first_key: b"dave".to_vec(),
                last_key: b"frank".to_vec(),
                file_size: 8192,
                entry_count: 20,
            };
            manifest.log_add(1, &meta2).unwrap();
        }

        // Reopen and verify replay.
        let (_manifest, state) = Manifest::open(&manifest_path, &sst_dir).unwrap();
        assert_eq!(state.next_sst_id, 3);
        assert_eq!(state.levels[0].len(), 1);
        assert_eq!(state.levels[0][0].id, 1);
        assert_eq!(state.levels[1].len(), 1);
        assert_eq!(state.levels[1][0].id, 2);
    }

    #[test]
    fn add_and_delete() {
        let (_dir, manifest_path, sst_dir) = setup();

        {
            let (mut manifest, _state) = Manifest::open(&manifest_path, &sst_dir).unwrap();
            let meta = SSTableMeta {
                id: 1,
                path: sst_dir.join("000001.sst"),
                first_key: b"a".to_vec(),
                last_key: b"z".to_vec(),
                file_size: 4096,
                entry_count: 5,
            };
            manifest.log_add(0, &meta).unwrap();
            manifest.log_del(1).unwrap();
        }

        let (_manifest, state) = Manifest::open(&manifest_path, &sst_dir).unwrap();
        // SSTable 1 was added then deleted — should not appear.
        assert!(state.levels[0].is_empty());
        assert_eq!(state.next_sst_id, 2);
    }

    #[test]
    fn total_disk_size() {
        let state = LevelState {
            levels: vec![
                vec![SSTableMeta {
                    id: 1,
                    path: PathBuf::from("a.sst"),
                    first_key: vec![],
                    last_key: vec![],
                    file_size: 1000,
                    entry_count: 0,
                }],
                vec![SSTableMeta {
                    id: 2,
                    path: PathBuf::from("b.sst"),
                    first_key: vec![],
                    last_key: vec![],
                    file_size: 2000,
                    entry_count: 0,
                }],
            ],
            next_sst_id: 3,
        };
        assert_eq!(state.total_disk_size(), 3000);
    }
}
