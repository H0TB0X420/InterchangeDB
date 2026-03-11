//! LSM-tree configuration constants.

/// Default memtable size threshold before flushing to an SSTable (4 MB).
pub const DEFAULT_MEMTABLE_SIZE_BYTES: usize = 4 * 1024 * 1024;

/// Maximum number of L0 SSTables before triggering compaction.
pub const MAX_L0_SSTABLE_COUNT: usize = 4;

/// Size multiplier between adjacent levels (L1 is 10x L0's total, etc.).
pub const LEVEL_SIZE_RATIO: usize = 10;

/// Maximum number of levels (L0 through L6).
pub const MAX_LEVEL_COUNT: usize = 7;

/// Target SSTable output size during compaction (~4 MB).
pub const TARGET_SSTABLE_SIZE_BYTES: usize = 4 * 1024 * 1024;
