//! Append-only journal of executed SQL statements.
//!
//! One JSON object per line, newline-terminated:
//!   `{"id":1,"ts_ms":1734567890123,"sql":"CREATE TABLE t (id INT PRIMARY KEY)"}`
//!
//! Phase 17 (offline replan loop) and Phase 18 (Cascades training) will
//! consume this file to replay workloads. Phase 11's contract is write-
//! only — no reader API, no rotation, no truncation.
//!
//! ## What's logged
//!
//! Every statement passed to `Session::execute`, **before** parsing.
//! Malformed SQL is logged too — the workload log captures intent, not
//! just successful runs.
//!
//! ## What's NOT logged (Phase 11)
//!
//! - Result rows or side-effects.
//! - Bound parameters (no prepared statements yet — Phase 13).
//! - Session id (single-session-per-log assumption — Phase 17 will
//!   namespace this).
//! - Restart-safe sequencing. `next_id` resets to 1 on every `open()`.
//!   Phase 17 will scan-on-open if cross-restart uniqueness matters.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;

use crate::common::{Error, Result};

#[derive(Serialize)]
struct LogEntry<'a> {
    id: u64,
    ts_ms: u128,
    sql: &'a str,
}

pub struct WorkloadLog {
    file: Mutex<File>,
    next_id: AtomicU64,
}

impl WorkloadLog {
    /// Open (or create) a log file in append mode. Multiple sessions
    /// holding `Arc<WorkloadLog>` write-serialize through the inner
    /// mutex; one line per `append`.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(Error::Io)?;
        Ok(Self {
            file: Mutex::new(file),
            next_id: AtomicU64::new(1),
        })
    }

    /// Append one statement. Returns the assigned id. Each call performs
    /// one `write_all` + one `flush` while holding the inner mutex; the
    /// id is allocated atomically outside the lock so concurrent appenders
    /// don't all serialize on id assignment.
    pub fn append(&self, sql: &str) -> Result<u64> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let entry = LogEntry { id, ts_ms, sql };
        let line = serde_json::to_string(&entry)
            .map_err(|e| Error::SqlParse(format!("workload log serialize: {}", e)))?;
        let mut file = self.file.lock();
        writeln!(file, "{}", line).map_err(Error::Io)?;
        file.flush().map_err(Error::Io)?;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};

    fn read_lines(path: &Path) -> Vec<String> {
        let f = File::open(path).unwrap();
        BufReader::new(f).lines().map(|l| l.unwrap()).collect()
    }

    #[test]
    fn append_writes_one_line_per_call() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wl.jsonl");
        let log = WorkloadLog::open(&path).unwrap();

        assert_eq!(log.append("SELECT 1").unwrap(), 1);
        assert_eq!(log.append("SELECT 2").unwrap(), 2);
        assert_eq!(log.append("SELECT 3").unwrap(), 3);

        let lines = read_lines(&path);
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("\"id\":1"));
        assert!(lines[0].contains("\"sql\":\"SELECT 1\""));
        assert!(lines[2].contains("\"sql\":\"SELECT 3\""));
    }

    #[test]
    fn entries_are_valid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wl.jsonl");
        let log = WorkloadLog::open(&path).unwrap();
        log.append("UPDATE t SET x = 1").unwrap();

        let lines = read_lines(&path);
        let parsed: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(parsed["id"], 1);
        assert_eq!(parsed["sql"], "UPDATE t SET x = 1");
        assert!(parsed["ts_ms"].as_u64().unwrap() > 0);
    }

    #[test]
    fn opening_existing_file_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wl.jsonl");
        {
            let log = WorkloadLog::open(&path).unwrap();
            log.append("FIRST").unwrap();
        }
        {
            let log = WorkloadLog::open(&path).unwrap();
            // NOTE (Phase 17): next_id resets — id collision across reopens
            // is acceptable for V1, will be fixed when the reader needs it.
            log.append("SECOND").unwrap();
        }
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("FIRST"));
        assert!(lines[1].contains("SECOND"));
    }

    #[test]
    fn concurrent_appends_serialize_correctly() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wl.jsonl");
        let log = Arc::new(WorkloadLog::open(&path).unwrap());

        let handles: Vec<_> = (0..8)
            .map(|i| {
                let log = log.clone();
                thread::spawn(move || {
                    for j in 0..25 {
                        log.append(&format!("stmt t{} n{}", i, j)).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let lines = read_lines(&path);
        assert_eq!(lines.len(), 200, "expected 8×25 = 200 entries");
        // Every id 1..=200 must appear exactly once.
        let mut ids: Vec<u64> = lines
            .iter()
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).unwrap()["id"]
                    .as_u64()
                    .unwrap()
            })
            .collect();
        ids.sort();
        assert_eq!(ids, (1u64..=200).collect::<Vec<_>>());
    }
}
