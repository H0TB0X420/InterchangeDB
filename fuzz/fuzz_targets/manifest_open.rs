#![no_main]
//! Fuzz `Manifest::open` on a file of arbitrary bytes. This drives the
//! private `replay_line` parser through its public entry point — a corrupt
//! or partially-written manifest must produce an error, never a panic.

use libfuzzer_sys::fuzz_target;

use interchangedb::engines::lsm::manifest::Manifest;

fuzz_target!(|data: &[u8]| {
    let dir = tempfile::tempdir().expect("create temp dir");
    let manifest_path = dir.path().join("MANIFEST");
    let sst_dir = dir.path().join("sst");
    std::fs::create_dir_all(&sst_dir).expect("create sst dir");
    std::fs::write(&manifest_path, data).expect("write fuzz bytes");
    let _ = Manifest::open(&manifest_path, &sst_dir);
});
