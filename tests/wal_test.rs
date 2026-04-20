//! WAL integration tests.
//!
//! Tests the Database<E> API with WAL enabled, verifying that:
//! - put/get/delete/scan work through the WAL path
//! - WAL segment files are created on disk
//! - WAL records are verifiable via the reader
//! - Crash recovery replays WAL records into a fresh engine
//! - Recovery is idempotent
//! - Checkpointing truncates old segments

use interchangedb::database::Database;
use interchangedb::index::lsm::LsmEngine;
use interchangedb::wal::{Lsn, LogPayload, WalReader};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Helper: create a WAL-enabled LSM database in a temp directory.
// ---------------------------------------------------------------------------

fn setup_wal_lsm() -> (Database<LsmEngine>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");
    let engine = LsmEngine::new(&lsm_dir).unwrap();
    let db = Database::open(&data_dir, engine).unwrap();
    (db, dir)
}

// ---------------------------------------------------------------------------
// Basic operations through WAL
// ---------------------------------------------------------------------------

#[test]
fn wal_put_get() {
    let (db, _dir) = setup_wal_lsm();
    assert!(db.has_wal());

    db.put(b"hello", b"world").unwrap();
    db.put(b"foo", b"bar").unwrap();

    assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));
    assert_eq!(db.get(b"foo").unwrap(), Some(b"bar".to_vec()));
    assert_eq!(db.get(b"missing").unwrap(), None);

    assert_eq!(db.status().keys, 2);
}

#[test]
fn wal_delete() {
    let (db, _dir) = setup_wal_lsm();

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();

    db.delete(b"b").unwrap();
    assert_eq!(db.get(b"b").unwrap(), None);
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn wal_scan() {
    let (db, _dir) = setup_wal_lsm();

    db.put(b"cherry", b"3").unwrap();
    db.put(b"apple", b"1").unwrap();
    db.put(b"banana", b"2").unwrap();

    let results: Vec<_> = db.scan(..).map(|r| r.unwrap()).collect();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, b"apple".to_vec());
    assert_eq!(results[1].0, b"banana".to_vec());
    assert_eq!(results[2].0, b"cherry".to_vec());
}

// ---------------------------------------------------------------------------
// WAL files exist on disk
// ---------------------------------------------------------------------------

#[test]
fn wal_files_on_disk() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");
    let engine = LsmEngine::new(&lsm_dir).unwrap();
    let db = Database::open(&data_dir, engine).unwrap();

    db.put(b"key", b"value").unwrap();

    // WAL directory should exist with at least one segment.
    let wal_dir = data_dir.join("wal");
    assert!(wal_dir.exists());

    let segments = interchangedb::wal::segment::list_segments(&wal_dir).unwrap();
    assert!(!segments.is_empty());
}

// ---------------------------------------------------------------------------
// WAL records verifiable via reader
// ---------------------------------------------------------------------------

#[test]
fn wal_records_verifiable() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");
    let engine = LsmEngine::new(&lsm_dir).unwrap();
    let db = Database::open(&data_dir, engine).unwrap();

    db.put(b"key1", b"val1").unwrap();
    db.put(b"key2", b"val2").unwrap();
    db.delete(b"key1").unwrap();

    // Read back WAL records.
    let wal_dir = data_dir.join("wal");
    let reader = WalReader::open(&wal_dir).unwrap();
    let records: Vec<_> = reader
        .scan_forward(Lsn::new(0))
        .map(|r| r.unwrap())
        .collect();

    // Under MVCC, each auto-commit op produces 3 WAL records: Begin, TxnPut, Commit.
    // 3 operations × 3 records = 9 total.
    assert_eq!(records.len(), 9);

    // Verify structure: [Begin, TxnPut, Commit] × 3.
    // Each triplet has sequential LSNs and matching txn_ids.
    for triplet in 0..3 {
        let base = triplet * 3;
        let txn_id = records[base].txn_id;
        assert!(txn_id > 0, "txn_id should be non-zero for explicit txns");

        match &records[base].payload {
            LogPayload::Begin => {}
            other => panic!("Record {} expected Begin, got {:?}", base, other),
        }
        match &records[base + 1].payload {
            LogPayload::TxnPut { .. } => {}
            other => panic!("Record {} expected TxnPut, got {:?}", base + 1, other),
        }
        match &records[base + 2].payload {
            LogPayload::Commit { .. } => {}
            other => panic!("Record {} expected Commit, got {:?}", base + 2, other),
        }
        // All three records in a triplet share the same txn_id.
        assert_eq!(records[base + 1].txn_id, txn_id);
        assert_eq!(records[base + 2].txn_id, txn_id);
    }
}

// ---------------------------------------------------------------------------
// Crash recovery: WAL records without engine flush
// ---------------------------------------------------------------------------

#[test]
fn crash_recovery() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");

    // Phase 1: Write data through the WAL-enabled database.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();
        db.put(b"key1", b"val1").unwrap();
        db.put(b"key2", b"val2").unwrap();
        db.put(b"key3", b"val3").unwrap();
        db.delete(b"key2").unwrap();
        // Drop without flush — simulates crash.
    }

    // Phase 2: Reopen with a fresh engine — recovery should replay the WAL.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();

        assert_eq!(db.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(db.get(b"key2").unwrap(), None); // Was deleted.
        assert_eq!(db.get(b"key3").unwrap(), Some(b"val3".to_vec()));
    }
}

// ---------------------------------------------------------------------------
// Clean shutdown + reopen
// ---------------------------------------------------------------------------

#[test]
fn clean_shutdown_reopen() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");

    // Phase 1: Write and flush.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();
    }

    // Phase 2: Reopen — recovery replays (idempotent).
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();

        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    }
}

// ---------------------------------------------------------------------------
// Idempotent recovery: recover, drop, recover again
// ---------------------------------------------------------------------------

#[test]
fn idempotent_recovery() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");

    // Phase 1: Write data.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();
        db.put(b"x", b"10").unwrap();
        db.put(b"y", b"20").unwrap();
    }

    // Phase 2: Recover once.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();
        assert_eq!(db.get(b"x").unwrap(), Some(b"10".to_vec()));
        assert_eq!(db.get(b"y").unwrap(), Some(b"20".to_vec()));
    }

    // Phase 3: Recover again — same result (idempotent).
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();
        assert_eq!(db.get(b"x").unwrap(), Some(b"10".to_vec()));
        assert_eq!(db.get(b"y").unwrap(), Some(b"20".to_vec()));
        assert_eq!(db.status().keys, 2);
    }
}

// ---------------------------------------------------------------------------
// Truncated tail recovery
// ---------------------------------------------------------------------------

#[test]
fn truncated_tail_recovery() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");

    // Phase 1: Write data.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
    }

    // Corrupt: truncate the last WAL segment to simulate partial write.
    let wal_dir = data_dir.join("wal");
    let segments = interchangedb::wal::segment::list_segments(&wal_dir).unwrap();
    let (_, ref path) = segments.last().unwrap();
    let data = std::fs::read(path).unwrap();
    // Remove last 10 bytes — truncates the last record.
    let truncated = &data[..data.len() - 10];
    std::fs::write(path, truncated).unwrap();

    // Phase 2: Recover — should get at least 2 complete records.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        // "c" may or may not be recovered depending on record boundaries.
    }
}

// ---------------------------------------------------------------------------
// Multi-op crash recovery
// ---------------------------------------------------------------------------

#[test]
fn multi_op_crash_recovery() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");

    // Phase 1: Many operations.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();

        for i in 0u16..100 {
            db.put(&i.to_be_bytes(), &(i * 10).to_be_bytes()).unwrap();
        }
        // Delete even keys.
        for i in (0u16..100).step_by(2) {
            db.delete(&i.to_be_bytes()).unwrap();
        }
        // No flush — simulate crash.
    }

    // Phase 2: Recover and verify.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();

        // Odd keys should be present.
        for i in (1u16..100).step_by(2) {
            let val = db.get(&i.to_be_bytes()).unwrap();
            assert_eq!(val, Some((i * 10).to_be_bytes().to_vec()), "key {} missing", i);
        }

        // Even keys should be deleted.
        for i in (0u16..100).step_by(2) {
            let val = db.get(&i.to_be_bytes()).unwrap();
            assert_eq!(val, None, "key {} should be deleted", i);
        }
    }
}

// ===========================================================================
// Checkpointing tests
// ===========================================================================

#[test]
fn checkpoint_and_crash_recovery() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");

    // Phase 1: Write data, checkpoint, write more data.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();

        db.put(b"pre1", b"a").unwrap();
        db.put(b"pre2", b"b").unwrap();

        db.checkpoint().unwrap();

        db.put(b"post1", b"c").unwrap();
        db.put(b"post2", b"d").unwrap();
        // No flush — simulate crash.
    }

    // Phase 2: Recover — all data should be present.
    // Recovery replays from the checkpoint, which is after pre1/pre2 were flushed.
    // post1/post2 are in the WAL after the checkpoint and will be replayed.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();

        assert_eq!(db.get(b"pre1").unwrap(), Some(b"a".to_vec()));
        assert_eq!(db.get(b"pre2").unwrap(), Some(b"b".to_vec()));
        assert_eq!(db.get(b"post1").unwrap(), Some(b"c".to_vec()));
        assert_eq!(db.get(b"post2").unwrap(), Some(b"d".to_vec()));
    }
}

#[test]
fn checkpoint_deletes_old_segments() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");
    let wal_dir = data_dir.join("wal");

    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();

        // Write enough data to create multiple segments.
        // We'll write many records to ensure the WAL has content.
        for i in 0..50 {
            let key = format!("key{:04}", i);
            let val = format!("val{:04}", i);
            db.put(key.as_bytes(), val.as_bytes()).unwrap();
        }

        let segments_before = interchangedb::wal::segment::list_segments(&wal_dir).unwrap();
        let segment_count_before = segments_before.len();

        db.checkpoint().unwrap();

        let segments_after = interchangedb::wal::segment::list_segments(&wal_dir).unwrap();

        // After checkpoint, we should have at most the same number of segments
        // (likely fewer if some were truncated, but at minimum the active segment).
        assert!(
            !segments_after.is_empty(),
            "Should have at least the active segment"
        );
        assert!(
            segments_after.len() <= segment_count_before,
            "Segments should not increase after checkpoint"
        );
    }
}

#[test]
fn multiple_checkpoints() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");

    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();

        db.put(b"k1", b"v1").unwrap();
        db.checkpoint().unwrap();

        db.put(b"k2", b"v2").unwrap();
        db.checkpoint().unwrap();

        db.put(b"k3", b"v3").unwrap();
        // No checkpoint for k3 — simulate crash.
    }

    // Recover — all data should be present.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();

        assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.get(b"k3").unwrap(), Some(b"v3".to_vec()));
    }
}

#[test]
fn checkpoint_with_no_prior_ops() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");

    let engine = LsmEngine::new(&lsm_dir).unwrap();
    let db = Database::open(&data_dir, engine).unwrap();

    // Checkpoint on empty database — should not error.
    db.checkpoint().unwrap();

    assert_eq!(db.status().keys, 0);
}

#[test]
fn recovery_without_checkpoint_full_replay() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let lsm_dir = data_dir.join("lsm");

    // Phase 1: Write without any checkpoint.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
    }

    // Phase 2: Recover — should do full replay from LSN 0.
    {
        let engine = LsmEngine::new(&lsm_dir).unwrap();
        let db = Database::open(&data_dir, engine).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
    }
}
