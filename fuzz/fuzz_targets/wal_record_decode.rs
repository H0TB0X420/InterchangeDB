#![no_main]
//! Fuzz `wal::LogRecord::decode` — the WAL recovery path decodes records
//! straight off disk, so arbitrary/torn bytes must return Ok/Err, never panic.

use libfuzzer_sys::fuzz_target;

use interchangedb::wal::LogRecord;

fuzz_target!(|data: &[u8]| {
    let _ = LogRecord::decode(data);
});
