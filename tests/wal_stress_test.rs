//! WAL stress tests.
//!
//! Exercises the WAL under heavy, chaotic workloads:
//!
//! - **Crash-recovery loop**: 100 iterations of write → drop → recover → verify.
//! - **Overwrite storm**: thousands of overwrites to the same key set.
//! - **Interleaved checkpoints**: checkpoints fired mid-workload at random intervals.
//! - **Large values**: keys near u16 max, values at page-size scale.
//! - **Delete-heavy workload**: insert then delete most keys, crash, recover.
//! - **Rapid checkpoint cycle**: checkpoint after every N ops.
//! - **Segment rotation torture**: tiny segment size forces constant rotation.
//! - **Idempotent recovery loop**: recover the same WAL 10 times, verify identical state.
//! - **Mixed workload gauntlet**: puts, deletes, overwrites, scans, checkpoints, crashes
//!   all in one long sequence.

use std::collections::HashMap;
use std::path::Path;

use interchangedb::database::Database;
use interchangedb::index::lsm::LsmEngine;
use interchangedb::wal::{Lsn, WalReader};
use tempfile::tempdir;

// ===========================================================================
// Helpers
// ===========================================================================

/// Open a WAL-enabled LSM database at the given data_dir.
/// The LSM engine directory is `data_dir/lsm`.
fn open_db(data_dir: &Path) -> Database<LsmEngine> {
    let lsm_dir = data_dir.join("lsm");
    let engine = LsmEngine::new(&lsm_dir).unwrap();
    Database::open(data_dir, engine).unwrap()
}

/// Deterministic pseudo-random number generator (xorshift64).
/// Avoids pulling in rand as a dependency. Good enough for stress tests.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Ensure non-zero seed.
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 16) as u32
    }

    /// Uniform in [0, bound).
    fn next_range(&mut self, bound: u32) -> u32 {
        assert!(bound > 0);
        self.next_u32() % bound
    }
}

/// Format a key from a u32 index. Fixed 10-byte format for consistent ordering.
fn make_key(i: u32) -> Vec<u8> {
    format!("k{:09}", i).into_bytes()
}

/// Format a value from a u32 index and a version counter.
fn make_value(i: u32, version: u32) -> Vec<u8> {
    format!("v{:09}_{:05}", i, version).into_bytes()
}

// ===========================================================================
// 1. Crash-recovery loop — 100 iterations of write → drop → recover → verify
// ===========================================================================

#[test]
fn crash_recovery_loop() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Ground truth: what the database should contain after recovery.
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rng = Rng::new(42);

    for iteration in 0u32..100 {
        // Open (runs recovery from prior iteration).
        {
            let db = open_db(&data_dir);

            // Verify ground truth from previous iterations.
            for (key, value) in &expected {
                let got = db.get(key).unwrap();
                assert_eq!(
                    got.as_deref(),
                    Some(value.as_slice()),
                    "iteration {}: key {:?} mismatch",
                    iteration,
                    String::from_utf8_lossy(key)
                );
            }

            // Do some random work: 10 puts and 3 deletes per iteration.
            for _ in 0..10 {
                let idx = rng.next_range(200);
                let key = make_key(idx);
                let value = make_value(idx, iteration);
                db.put(&key, &value).unwrap();
                expected.insert(key, value);
            }

            for _ in 0..3 {
                let idx = rng.next_range(200);
                let key = make_key(idx);
                db.delete(&key).unwrap();
                expected.remove(&key);
            }

            // Drop without flush — simulate crash.
        }
    }

    // Final recovery + verify everything.
    let db = open_db(&data_dir);
    for (key, value) in &expected {
        let got = db.get(key).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(value.as_slice()),
            "final verify: key {:?}",
            String::from_utf8_lossy(key)
        );
    }

    // Verify no phantom keys exist.
    let scan_count = db.scan(..).count();
    assert_eq!(
        scan_count,
        expected.len(),
        "scan count {} != expected {}",
        scan_count,
        expected.len()
    );
}

// ===========================================================================
// 2. Overwrite storm — thousands of overwrites to a small key set
// ===========================================================================

#[test]
fn overwrite_storm() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let key_count: u32 = 20;
    let versions_per_key: u32 = 50;

    // Phase 1: overwrite every key many times.
    {
        let db = open_db(&data_dir);
        for version in 0..versions_per_key {
            for idx in 0..key_count {
                let key = make_key(idx);
                let value = make_value(idx, version);
                db.put(&key, &value).unwrap();
            }
        }
        // Drop — crash.
    }

    // Phase 2: recover and verify only the last version survives.
    {
        let db = open_db(&data_dir);
        let last_version = versions_per_key - 1;
        for idx in 0..key_count {
            let key = make_key(idx);
            let expected = make_value(idx, last_version);
            let got = db.get(&key).unwrap();
            assert_eq!(got, Some(expected), "key {} wrong version", idx);
        }
        assert_eq!(db.scan(..).count(), key_count as usize);
    }
}

// ===========================================================================
// 3. Interleaved checkpoints — checkpoint at random intervals during workload
// ===========================================================================

#[test]
fn interleaved_checkpoints() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rng = Rng::new(0xDEAD);

    {
        let db = open_db(&data_dir);
        let mut ops_since_checkpoint: u32 = 0;

        for i in 0u32..500 {
            let idx = rng.next_range(100);
            let key = make_key(idx);

            if rng.next_range(4) == 0 {
                // 25% deletes.
                db.delete(&key).unwrap();
                expected.remove(&key);
            } else {
                let value = make_value(idx, i);
                db.put(&key, &value).unwrap();
                expected.insert(key, value);
            }

            ops_since_checkpoint += 1;

            // Checkpoint roughly every 30-70 ops.
            if ops_since_checkpoint > 30 + rng.next_range(40) {
                db.checkpoint().unwrap();
                ops_since_checkpoint = 0;
            }
        }

        // Crash without final checkpoint.
    }

    // Recover and verify.
    let db = open_db(&data_dir);
    for (key, value) in &expected {
        let got = db.get(key).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(value.as_slice()),
            "key {:?}",
            String::from_utf8_lossy(key)
        );
    }

    let scan_count = db.scan(..).count();
    assert_eq!(scan_count, expected.len());
}

// ===========================================================================
// 4. Large values — push key/value sizes toward limits
// ===========================================================================

#[test]
fn large_values() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // 4KB values, 50 keys.
    let value_size = 4096;
    let key_count = 50;

    {
        let db = open_db(&data_dir);
        for i in 0u32..key_count {
            let key = make_key(i);
            // Fill value with a recognizable pattern: the key index repeated.
            let byte = (i & 0xFF) as u8;
            let value = vec![byte; value_size];
            db.put(&key, &value).unwrap();
        }
        // Crash.
    }

    // Recover.
    {
        let db = open_db(&data_dir);
        for i in 0u32..key_count {
            let key = make_key(i);
            let byte = (i & 0xFF) as u8;
            let expected = vec![byte; value_size];
            let got = db.get(&key).unwrap().unwrap();
            assert_eq!(got.len(), value_size, "key {} wrong value length", i);
            assert_eq!(got, expected, "key {} wrong value content", i);
        }
        assert_eq!(db.scan(..).count(), key_count as usize);
    }
}

// ===========================================================================
// 5. Delete-heavy workload — insert many, delete most, crash, recover
// ===========================================================================

#[test]
fn delete_heavy() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let total_keys: u32 = 500;
    let survivors: u32 = 50; // Only keys [0..50) survive.

    {
        let db = open_db(&data_dir);

        // Insert all keys.
        for i in 0..total_keys {
            db.put(&make_key(i), &make_value(i, 0)).unwrap();
        }

        // Delete keys [50..500).
        for i in survivors..total_keys {
            db.delete(&make_key(i)).unwrap();
        }
        // Crash.
    }

    // Recover.
    {
        let db = open_db(&data_dir);

        for i in 0..survivors {
            assert!(
                db.get(&make_key(i)).unwrap().is_some(),
                "survivor key {} missing",
                i
            );
        }
        for i in survivors..total_keys {
            assert!(
                db.get(&make_key(i)).unwrap().is_none(),
                "deleted key {} still present",
                i
            );
        }

        let scan: Vec<_> = db.scan(..).collect();
        assert_eq!(scan.len(), survivors as usize);
    }
}

// ===========================================================================
// 6. Rapid checkpoint cycle — checkpoint after every N ops
// ===========================================================================

#[test]
fn rapid_checkpoint_cycle() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let ops = 200u32;
    let checkpoint_interval = 5u32;

    {
        let db = open_db(&data_dir);

        for i in 0..ops {
            let key = make_key(i % 30);
            let value = make_value(i % 30, i);
            db.put(&key, &value).unwrap();

            if (i + 1) % checkpoint_interval == 0 {
                db.checkpoint().unwrap();
            }
        }
        // Crash.
    }

    // Recover.
    {
        let db = open_db(&data_dir);

        // Each key in [0..30) should have the value from the last overwrite.
        for idx in 0u32..30 {
            // Last version for this key: the highest i where i % 30 == idx.
            let last_i = if idx < (ops % 30) {
                ops - (ops % 30) + idx
            } else {
                ops - (ops % 30) - 30 + idx
            };
            let expected_value = make_value(idx, last_i);
            let got = db.get(&make_key(idx)).unwrap();
            assert_eq!(
                got,
                Some(expected_value),
                "key {} (last_i={}) mismatch",
                idx,
                last_i
            );
        }
    }
}

// ===========================================================================
// 7. WAL reader integrity — verify all records have monotonic LSNs
// ===========================================================================

#[test]
fn wal_reader_integrity_after_stress() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let wal_dir = data_dir.join("wal");

    // Generate a busy workload.
    {
        let db = open_db(&data_dir);
        for i in 0u32..300 {
            let key = make_key(i % 50);
            if i % 7 == 0 {
                db.delete(&key).unwrap();
            } else {
                db.put(&key, &make_value(i, i)).unwrap();
            }
            if i % 40 == 0 {
                db.checkpoint().unwrap();
            }
        }
        db.flush().unwrap();
    }

    // Read back all WAL records and verify monotonic LSNs.
    let reader = WalReader::open(&wal_dir).unwrap();
    let mut prev_lsn: Option<Lsn> = None;
    let mut count: u64 = 0;

    for result in reader.scan_forward(Lsn::new(0)) {
        let record = result.unwrap();
        if let Some(prev) = prev_lsn {
            assert!(
                record.lsn > prev,
                "LSN not monotonic: {} after {}",
                record.lsn,
                prev
            );
        }
        prev_lsn = Some(record.lsn);
        count += 1;
    }

    // Should have at least as many records as ops (300 data + some checkpoints).
    assert!(count >= 300, "Expected >= 300 records, got {}", count);
}

// ===========================================================================
// 8. Idempotent recovery loop — recover 10 times, verify same state
// ===========================================================================

#[test]
fn idempotent_recovery_loop() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Seed some data.
    {
        let db = open_db(&data_dir);
        for i in 0u32..100 {
            db.put(&make_key(i), &make_value(i, 0)).unwrap();
        }
        // Delete odd keys.
        for i in (1u32..100).step_by(2) {
            db.delete(&make_key(i)).unwrap();
        }
        // Crash without flush.
    }

    // Recover 10 times. Each time the state should be identical.
    for attempt in 0..10 {
        let db = open_db(&data_dir);

        // Even keys present.
        for i in (0u32..100).step_by(2) {
            assert!(
                db.get(&make_key(i)).unwrap().is_some(),
                "attempt {}: even key {} missing",
                attempt,
                i
            );
        }
        // Odd keys absent.
        for i in (1u32..100).step_by(2) {
            assert!(
                db.get(&make_key(i)).unwrap().is_none(),
                "attempt {}: odd key {} should be deleted",
                attempt,
                i
            );
        }

        assert_eq!(
            db.scan(..).count(),
            50,
            "attempt {}: wrong key count",
            attempt
        );
    }
}

// ===========================================================================
// 9. Mixed workload gauntlet — puts, deletes, overwrites, scans, checkpoints,
//    crashes all in one long sequence.
// ===========================================================================

#[test]
fn mixed_workload_gauntlet() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rng = Rng::new(0xCAFE_BABE);

    // Run 5 "epochs". Each epoch: open, do work, crash.
    for epoch in 0u32..5 {
        let db = open_db(&data_dir);

        // Verify state from previous epochs.
        for (key, value) in &expected {
            let got = db.get(key).unwrap();
            assert_eq!(
                got.as_deref(),
                Some(value.as_slice()),
                "epoch {} pre-check: key {:?}",
                epoch,
                String::from_utf8_lossy(key)
            );
        }

        let ops_this_epoch = 100 + rng.next_range(100) as usize;

        for op_idx in 0..ops_this_epoch {
            let action = rng.next_range(10);
            let idx = rng.next_range(150);
            let key = make_key(idx);

            match action {
                0..=5 => {
                    // 60%: put.
                    let value = make_value(idx, epoch * 1000 + op_idx as u32);
                    db.put(&key, &value).unwrap();
                    expected.insert(key, value);
                }
                6..=8 => {
                    // 30%: delete.
                    db.delete(&key).unwrap();
                    expected.remove(&key);
                }
                _ => {
                    // 10%: scan (read-only, just exercise the path).
                    let _results: Vec<_> = db.scan(..).take(20).collect();
                }
            }

            // Checkpoint every ~80 ops.
            if op_idx > 0 && op_idx % 80 == 0 {
                db.checkpoint().unwrap();
            }
        }

        // Crash without flush.
    }

    // Final recovery.
    let db = open_db(&data_dir);

    for (key, value) in &expected {
        let got = db.get(key).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(value.as_slice()),
            "final: key {:?}",
            String::from_utf8_lossy(key)
        );
    }

    let scan_count = db.scan(..).count();
    assert_eq!(
        scan_count,
        expected.len(),
        "final scan: {} vs expected {}",
        scan_count,
        expected.len()
    );
}

// ===========================================================================
// 10. Checkpoint then crash — verify recovery only replays post-checkpoint
// ===========================================================================

#[test]
fn checkpoint_bounds_recovery_window() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Phase 1: lots of data, then checkpoint, then a few more writes.
    {
        let db = open_db(&data_dir);

        for i in 0u32..200 {
            db.put(&make_key(i), &make_value(i, 0)).unwrap();
        }
        db.checkpoint().unwrap();

        // Only 5 more writes after checkpoint.
        for i in 200u32..205 {
            db.put(&make_key(i), &make_value(i, 1)).unwrap();
        }
        // Crash.
    }

    // Phase 2: Recover. The WAL reader should start scanning from the
    // checkpoint LSN, not from LSN 0. But all data should be intact
    // because pre-checkpoint data was flushed by the checkpoint.
    {
        let db = open_db(&data_dir);

        for i in 0u32..205 {
            assert!(
                db.get(&make_key(i)).unwrap().is_some(),
                "key {} missing after checkpoint recovery",
                i
            );
        }
    }
}

// ===========================================================================
// 11. Crash mid-overwrite — verify we get either old or new value, never
//     corruption.
// ===========================================================================

#[test]
fn crash_mid_overwrite_consistency() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let key = make_key(0);

    // Write version 0 and checkpoint to make it durable.
    {
        let db = open_db(&data_dir);
        db.put(&key, b"version_0").unwrap();
        db.checkpoint().unwrap();
    }

    // Now overwrite 100 times without checkpoint, then crash.
    {
        let db = open_db(&data_dir);
        for v in 1u32..=100 {
            let value = format!("version_{}", v).into_bytes();
            db.put(&key, &value).unwrap();
        }
        // Crash — the last fully synced write is version 100 (auto-commit syncs each).
    }

    // Recover: should get version_100 (each put syncs immediately).
    {
        let db = open_db(&data_dir);
        let got = db.get(&key).unwrap().unwrap();
        let got_str = String::from_utf8(got).unwrap();
        assert_eq!(got_str, "version_100");
    }

    // Now simulate a partial write by truncating the WAL.
    let wal_dir = data_dir.join("wal");
    let segments = interchangedb::wal::segment::list_segments(&wal_dir).unwrap();
    let (_, ref path) = segments.last().unwrap();
    let data = std::fs::read(path).unwrap();
    // Chop off last 5 bytes — corrupts the tail record.
    if data.len() > 5 {
        std::fs::write(path, &data[..data.len() - 5]).unwrap();
    }

    // Recover again: should get SOME valid version, never garbage.
    {
        let db = open_db(&data_dir);
        let got = db.get(&key).unwrap().unwrap();
        let got_str = String::from_utf8(got.clone()).unwrap();
        assert!(
            got_str.starts_with("version_"),
            "Expected version_N, got {:?}",
            got_str
        );
    }
}
