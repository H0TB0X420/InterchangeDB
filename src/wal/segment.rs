//! WAL segment file utilities.
//!
//! Each WAL segment is a numbered file (`00000000.wal`, `00000001.wal`, ...)
//! containing a sequence of log records. Segments are rotated when they
//! exceed `DEFAULT_MAX_SEGMENT_SIZE`.

use std::path::{Path, PathBuf};

use crate::common::Result;

/// Default maximum segment size: 16MB.
pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

/// Upper bound on segment IDs to prevent unbounded growth.
pub const MAX_SEGMENTS: u64 = 100_000;

/// Format a segment filename from its ID.
///
/// Segments are zero-padded to 8 digits: `00000000.wal`, `00000042.wal`.
pub fn segment_filename(id: u64) -> String {
    format!("{:08}.wal", id)
}

/// Parse a segment ID from a filename, returning None if not a valid segment name.
fn parse_segment_id(filename: &str) -> Option<u64> {
    let stem = filename.strip_suffix(".wal")?;
    if stem.len() != 8 {
        return None;
    }
    stem.parse::<u64>().ok()
}

/// List all WAL segment files in a directory, sorted by segment ID ascending.
///
/// Returns `(segment_id, path)` pairs. Only files matching the `XXXXXXXX.wal`
/// pattern are included.
pub fn list_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    if !dir.exists() {
        return Ok(vec![]);
    }

    let mut segments = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
            if let Some(id) = parse_segment_id(filename) {
                segments.push((id, path));
            }
        }
    }

    segments.sort_by_key(|(id, _)| *id);
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_formatting() {
        assert_eq!(segment_filename(0), "00000000.wal");
        assert_eq!(segment_filename(1), "00000001.wal");
        assert_eq!(segment_filename(42), "00000042.wal");
        assert_eq!(segment_filename(99_999_999), "99999999.wal");
    }

    #[test]
    fn parse_valid() {
        assert_eq!(parse_segment_id("00000000.wal"), Some(0));
        assert_eq!(parse_segment_id("00000042.wal"), Some(42));
        assert_eq!(parse_segment_id("99999999.wal"), Some(99_999_999));
    }

    #[test]
    fn parse_invalid() {
        assert_eq!(parse_segment_id("not_a_segment.wal"), None);
        assert_eq!(parse_segment_id("00000000.log"), None);
        assert_eq!(parse_segment_id("0000000.wal"), None); // Too short.
        assert_eq!(parse_segment_id("000000000.wal"), None); // Too long.
        assert_eq!(parse_segment_id(""), None);
    }

    #[test]
    fn list_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let segments = list_segments(dir.path()).unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn list_nonexistent_dir() {
        let segments = list_segments(Path::new("/nonexistent/path/wal")).unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn list_with_segments() {
        let dir = tempfile::tempdir().unwrap();

        // Create some segment files and a non-segment file.
        std::fs::write(dir.path().join("00000002.wal"), b"").unwrap();
        std::fs::write(dir.path().join("00000000.wal"), b"").unwrap();
        std::fs::write(dir.path().join("00000001.wal"), b"").unwrap();
        std::fs::write(dir.path().join("not_a_segment.txt"), b"").unwrap();

        let segments = list_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 3);
        // Sorted by ID.
        assert_eq!(segments[0].0, 0);
        assert_eq!(segments[1].0, 1);
        assert_eq!(segments[2].0, 2);
    }
}
