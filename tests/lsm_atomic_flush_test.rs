//! Q-04 regression tests: LSM flush must be crash-atomic at the SSTable
//! level. An SSTable file at its final path is either complete-and-fsync'd
//! or absent — no half-written files for recovery to misinterpret.
//!
//! These tests don't simulate a real kernel crash; they exercise the
//! invariants the atomicity machinery is supposed to preserve:
//! - `*.sst.tmp` orphans are swept at LSM open time.
//! - Successful flushes leave the final path and no tmp residue.
//! - Data is durable across drop-and-reopen cycles.
//! - A malformed `.tmp` orphan doesn't break open (it's swept, not parsed).

use interchangedb::index::lsm::LsmTree;
use std::fs;
use tempfile::TempDir;

/// Open an LSM tree in a fresh tempdir. Returns the tree plus the
/// tempdir's `sst/` subdirectory path for test inspection.
fn open_fresh() -> (LsmTree, TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let tree = LsmTree::open(dir.path()).unwrap();
    let sst_dir = dir.path().join("sst");
    (tree, dir, sst_dir)
}

#[test]
fn flush_leaves_no_tmp_residue_in_sst_dir() {
    let (tree, _dir, sst_dir) = open_fresh();
    tree.put(b"k1".to_vec(), b"v1".to_vec()).unwrap();
    tree.put(b"k2".to_vec(), b"v2".to_vec()).unwrap();
    tree.flush_memtable().unwrap();

    // Sweep the sst_dir for any .tmp file. Atomic rename means none survive.
    let tmp_files: Vec<_> = fs::read_dir(&sst_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("tmp"))
        .collect();
    assert!(
        tmp_files.is_empty(),
        "found tmp residue after successful flush: {:?}",
        tmp_files.iter().map(|e| e.path()).collect::<Vec<_>>()
    );

    // And at least one final-path .sst file must exist.
    let sst_files: Vec<_> = fs::read_dir(&sst_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
        .collect();
    assert_eq!(sst_files.len(), 1, "expected exactly one SSTable file");
}

#[test]
fn data_survives_drop_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    {
        let tree = LsmTree::open(&data_dir).unwrap();
        tree.put(b"alpha".to_vec(), b"1".to_vec()).unwrap();
        tree.put(b"beta".to_vec(), b"2".to_vec()).unwrap();
        tree.flush_memtable().unwrap();
        // Drop the tree without explicit shutdown — simulates abrupt exit
        // after a successful flush.
    }

    let tree2 = LsmTree::open(&data_dir).unwrap();
    assert_eq!(tree2.get(b"alpha").unwrap(), Some(b"1".to_vec()));
    assert_eq!(tree2.get(b"beta").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn orphan_tmp_file_swept_at_open() {
    // First open: creates the data_dir/sst/ layout.
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    {
        let _tree = LsmTree::open(&data_dir).unwrap();
    }
    let sst_dir = data_dir.join("sst");

    // Plant an orphan .sst.tmp file as if a crash interrupted a flush
    // mid-write. The content is intentional garbage — recovery must NOT
    // try to parse it.
    let orphan = sst_dir.join("999999.sst.tmp");
    fs::write(&orphan, b"this is not a valid SSTable footer").unwrap();
    assert!(orphan.exists(), "test setup failed: orphan not planted");

    // Reopening must sweep the orphan and succeed.
    let tree = LsmTree::open(&data_dir).unwrap();
    assert!(!orphan.exists(), "orphan .sst.tmp survived open(): {:?}", orphan);

    // Tree must still function — the sweep didn't break anything.
    tree.put(b"k".to_vec(), b"v".to_vec()).unwrap();
    assert_eq!(tree.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn malformed_tmp_does_not_corrupt_existing_data() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    // Write and flush real data first.
    {
        let tree = LsmTree::open(&data_dir).unwrap();
        tree.put(b"keep".to_vec(), b"me".to_vec()).unwrap();
        tree.flush_memtable().unwrap();
    }

    // Plant garbage tmp file alongside the real SSTable.
    let sst_dir = data_dir.join("sst");
    let garbage = sst_dir.join("888888.sst.tmp");
    fs::write(&garbage, vec![0xff; 4096]).unwrap();

    // Reopen — orphan swept, real data intact.
    let tree = LsmTree::open(&data_dir).unwrap();
    assert!(!garbage.exists(), "orphan survived open");
    assert_eq!(tree.get(b"keep").unwrap(), Some(b"me".to_vec()));
}

#[test]
fn repeated_flushes_never_accumulate_tmp_files() {
    let (tree, _dir, sst_dir) = open_fresh();
    for i in 0..10u32 {
        let k = format!("k{:03}", i).into_bytes();
        tree.put(k, vec![i as u8; 64]).unwrap();
        tree.flush_memtable().unwrap();
    }

    let tmp_count = fs::read_dir(&sst_dir)
        .unwrap()
        .flatten()
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("tmp"))
        .count();
    assert_eq!(tmp_count, 0, "tmp files leaked across repeated flushes");
}

#[test]
fn manifest_entry_survives_drop_and_reopen() {
    // The Q-04 fix added sync_all to manifest.log_add. Without it, a drop
    // immediately after a flush could lose the manifest entry, orphaning
    // the (atomically-renamed) SSTable. With it, both the SSTable and its
    // manifest entry are durable.
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    {
        let tree = LsmTree::open(&data_dir).unwrap();
        tree.put(b"durable_key".to_vec(), b"durable_value".to_vec()).unwrap();
        tree.flush_memtable().unwrap();
    }
    // No graceful shutdown — the flush's manifest write must be self-sufficient.
    let tree = LsmTree::open(&data_dir).unwrap();
    assert_eq!(
        tree.get(b"durable_key").unwrap(),
        Some(b"durable_value".to_vec())
    );
}
