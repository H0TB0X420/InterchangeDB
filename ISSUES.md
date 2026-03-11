# Issue Tracker

Identified during Phase 4 (WAL) verification audit. Prioritized by severity.

---

## ISS-001: Page Checksum Verification Not Wired Up — RESOLVED

**Priority**: High
**Component**: Storage / B-Tree
**Status**: Fixed

All B-tree pages now include a 13-byte `PageHeader` prefix (page_type, checksum, lsn) before node-specific data. Header size constants updated: internal 12→25, leaf 20→33. CRC32 checksums computed at encode time and verified at decode time (guarded by `checksum != 0` for backward compatibility with pre-checksum pages). All 11 `data[0]` node-type reads updated to `data[PageHeader::SIZE]`.

**Files modified**: `node.rs`, `page_layout.rs`, `mod.rs`, `tree.rs`, `iterator.rs`

---

## ISS-002: LSM Mutex Unwrap on Reader Map — RESOLVED

**Priority**: Medium
**Component**: LSM-Tree
**Status**: Fixed

Switched `std::sync::Mutex` → `parking_lot::Mutex` in `mod.rs` (3 sites) and `compaction.rs` (4 sites). Removed `.unwrap()` from all `.lock()` calls. `parking_lot::Mutex` doesn't poison on thread panic.

**Files modified**: `src/index/lsm/mod.rs`, `src/index/lsm/compaction.rs`

---

## ISS-003: LSM Crash Mid-Flush Data Loss

**Priority**: Medium
**Component**: LSM-Tree

If the process crashes while `LsmTree::flush_memtable()` is writing an SSTable, the SSTable may be partially written but the manifest may or may not have been updated. On recovery, this could leave orphaned partial SSTable files or a manifest pointing to a corrupt SSTable.

**Fix**: Use a write-ahead approach for SSTable creation: write SSTable to a temp file, fsync, then atomically rename to final path before updating the manifest. On recovery, clean up any temp files.

**Files**: `src/index/lsm/mod.rs`, `src/index/lsm/sstable.rs`

---

## ISS-004: No Fault Injection for I/O Errors

**Priority**: Medium
**Component**: Testing Infrastructure

No tests verify behavior under I/O failures (disk full, read errors, write errors). The `DiskManager` uses real file I/O with no injection points. Critical paths like WAL sync, BPM flush, and SSTable writes are untested under failure conditions.

**Fix**: Add a `DiskManager` wrapper or trait that can inject failures at specific points. Write tests for: WAL append during disk-full, BPM flush failure, SSTable write failure mid-flush.

**Files**: `src/storage/disk_manager.rs`, new test files

---

## ISS-005: B-Tree Iterator Doesn't Detect Corruption

**Priority**: Low
**Component**: B-Tree

`BTreeScanIterator` follows `next_page_id` links between leaf nodes. If a page is corrupted and contains a bogus `next_page_id`, the iterator will silently follow it, potentially reading garbage data or panicking on an out-of-bounds page.

**Fix**: Add bounds checking on `next_page_id` (must be < page_count) and validate leaf node structure after decode.

**Files**: `src/index/btree/iterator.rs`, `src/index/btree/page_layout.rs`

---

## ISS-006: Property-Based / Fuzzing Tests

**Priority**: Low
**Component**: Testing

No property-based tests exist. The B-tree and LSM-tree would benefit from randomized testing that verifies invariants (sorted order, no data loss, scan consistency) across thousands of random operation sequences.

**Fix**: Add `proptest` or `quickcheck` tests that generate random put/delete/scan sequences and verify invariants after each operation.

**Files**: New test files

---

## ISS-007: Near-Limit Key/Value Sizes

**Priority**: Low
**Component**: B-Tree / LSM-Tree

No tests exercise keys or values near the maximum sizes that fit in a single page. For the B-tree, a key larger than ~4060 bytes (PAGE_SIZE - leaf header) would fail to encode. For the LSM-tree WAL records, key/value sizes are capped at u16::MAX (65535 bytes). Edge cases around these limits are untested.

**Fix**: Add tests with keys/values at exactly the maximum allowed size, one byte over, and at page boundary sizes.

**Files**: New test files

---

## ISS-008: Large-Scale Integration Tests

**Priority**: Low
**Component**: Testing

Current scale tests use 5,000-10,000 keys. No tests exercise the system at 100K+ keys to verify performance doesn't degrade non-linearly, memory usage stays bounded, and the BPM eviction works correctly under sustained pressure.

**Fix**: Add benchmarks/tests at 100K and 1M key scale with bounded pool sizes.

**Files**: New test/bench files
