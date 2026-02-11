//! Goldenscript test runner for the B-tree storage engine.
//!
//! Exercises the `StorageEngine` trait interface via plain text scripts.
//! Each script file in `tests/goldenscripts/btree/` becomes a separate test.

use std::error::Error;
use std::fmt::Write as _;

use goldenscript::{Command, Runner};
use tempfile::TempDir;
use test_each_file::test_each_path;

use interchangedb::buffer::BufferPoolManager;
use interchangedb::index::btree::BTreeEngine;
use interchangedb::storage::engine::StorageEngine;
use interchangedb::storage::DiskManager;

// Auto-discover all scripts in tests/goldenscripts/btree/.
test_each_path! { in "tests/goldenscripts/btree" as btree => test_goldenscript }

fn test_goldenscript(path: &std::path::Path) {
    let mut runner = BTreeRunner::new();
    goldenscript::run(&mut runner, path).expect("goldenscript failed");
}

/// Runner that owns a `BTreeEngine` backed by a temp directory.
struct BTreeRunner {
    engine: BTreeEngine,
    _dir: TempDir,
}

impl BTreeRunner {
    fn new() -> Self {
        let dir = TempDir::new().expect("failed to create temp dir");
        let path = dir.path().join("test.db");
        let dm = DiskManager::create(&path).expect("failed to create disk manager");
        let bpm = BufferPoolManager::new(256, dm);
        let engine = BTreeEngine::new(bpm).expect("failed to create BTreeEngine");
        Self { engine, _dir: dir }
    }
}

impl Runner for BTreeRunner {
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            // put KEY VALUE — insert or overwrite a key-value pair.
            "put" => {
                let mut args = command.consume_args();
                let key = &args.next_pos().ok_or("put: key not given")?.value;
                let value = &args.next_pos().ok_or("put: value not given")?.value;
                args.reject_rest()?;
                self.engine.put(key.as_bytes(), value.as_bytes())?;
            }

            // get KEY — retrieve the value for a key.
            "get" => {
                let mut args = command.consume_args();
                let key = &args.next_pos().ok_or("get: key not given")?.value;
                args.reject_rest()?;
                match self.engine.get(key.as_bytes())? {
                    Some(value) => {
                        let value_str = String::from_utf8_lossy(&value);
                        writeln!(output, "{key} => {value_str}")?;
                    }
                    None => {
                        writeln!(output, "{key} => None")?;
                    }
                }
            }

            // delete KEY — remove a key.
            "delete" => {
                let mut args = command.consume_args();
                let key = &args.next_pos().ok_or("delete: key not given")?.value;
                args.reject_rest()?;
                self.engine.delete(key.as_bytes())?;
            }

            // scan [START [END]] — range scan. No args = full scan,
            // one arg = from START, two args = [START, END) exclusive end.
            "scan" => {
                let mut args = command.consume_args();
                let start = args.next_pos().map(|a| a.value.as_bytes().to_vec());
                let end = args.next_pos().map(|a| a.value.as_bytes().to_vec());
                args.reject_rest()?;

                let iter: Box<dyn Iterator<Item = _>> = match (start, end) {
                    (None, None) => Box::new(self.engine.scan(..)),
                    (Some(s), None) => Box::new(self.engine.scan(s..)),
                    (Some(s), Some(e)) => Box::new(self.engine.scan(s..e)),
                    (None, Some(_)) => return Err("scan: end without start".into()),
                };

                for result in iter {
                    let (key, value) = result?;
                    let key_str = String::from_utf8_lossy(&key);
                    let value_str = String::from_utf8_lossy(&value);
                    writeln!(output, "{key_str} => {value_str}")?;
                }
            }

            // status — print deterministic engine statistics.
            "status" => {
                let args = command.consume_args();
                args.reject_rest()?;
                let status = self.engine.status();
                writeln!(output, "keys: {}", status.keys)?;
                writeln!(output, "size: {}", status.size)?;
                writeln!(output, "tombstones: {}", status.tombstones)?;
            }

            // flush — flush dirty pages to disk.
            "flush" => {
                let args = command.consume_args();
                args.reject_rest()?;
                self.engine.flush()?;
            }

            name => return Err(format!("unknown command '{name}'").into()),
        }
        Ok(output)
    }
}
