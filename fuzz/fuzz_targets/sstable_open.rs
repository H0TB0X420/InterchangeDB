#![no_main]
//! Fuzz `SSTableReader::open` on a file of arbitrary bytes. `open` is the
//! recovery path for SSTable files of unknown provenance (a crash mid-write
//! leaves a short/torn file), so it must reject garbage with an error, never
//! panic. Q-29 caught a too-small-file `assert!` here.

use std::io::Write;

use libfuzzer_sys::fuzz_target;

use interchangedb::engines::lsm::sstable::SSTableReader;

fuzz_target!(|data: &[u8]| {
    let mut file = tempfile::NamedTempFile::new().expect("create temp sst file");
    file.write_all(data).expect("write fuzz bytes");
    file.flush().expect("flush");
    let _ = SSTableReader::open(file.path(), 1);
});
