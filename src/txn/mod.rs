//! Transaction management — lifecycle tracking for active transactions.
//!
//! The `TransactionManager` assigns unique transaction IDs, tracks active
//! transactions, and coordinates begin/commit/abort with the WAL.
//!
//! ## Design
//!
//! Each transaction gets a monotonically increasing `TxnId`. The manager
//! maintains a map of active transactions and their state. WAL integration
//! uses the existing `LogRecord` format which already supports `txn_id`,
//! `prev_lsn` chains, and Begin/Commit/Abort record types.
//!
//! ## Prev-LSN Chain
//!
//! Each WAL record for a transaction stores the LSN of the previous record
//! for that same transaction. This creates a backward chain used by abort
//! to walk and undo writes without scanning the entire WAL.
//!
//! ```text
//! Begin(txn=1, prev=INVALID) → Put(txn=1, prev=lsn0) → Put(txn=1, prev=lsn1) → Commit(txn=1, prev=lsn2)
//! ```

pub mod gc;
pub mod lock_manager;
pub mod mvcc;

use std::collections::HashMap;
use std::fmt;

use crate::common::{Error, Result};
use crate::wal::Lsn;
pub use lock_manager::{LockManager, LockMode};
use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Timestamp — monotonic counter for MVCC version ordering
// ---------------------------------------------------------------------------

/// Monotonic timestamp for MVCC version ordering.
///
/// Assigned by `TimestampOracle`. Each transaction gets a `begin_ts` at start
/// and a `commit_ts` at commit. Visibility rules use these to determine which
/// versions a snapshot can see.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const INVALID: Timestamp = Timestamp(u64::MAX);
    pub const ZERO: Timestamp = Timestamp(0);

    pub fn new(value: u64) -> Self {
        Timestamp(value)
    }

    pub fn is_valid(&self) -> bool {
        *self != Self::INVALID
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "Ts(INVALID)")
        } else {
            write!(f, "Ts({})", self.0)
        }
    }
}

// ---------------------------------------------------------------------------
// TimestampOracle — atomic monotonic timestamp generator
// ---------------------------------------------------------------------------

/// Assigns globally unique, monotonically increasing timestamps.
///
/// Thread-safe via atomic operations. Used by TransactionManager to assign
/// begin_ts and commit_ts to transactions.
pub struct TimestampOracle {
    next_ts: AtomicU64,
}

impl Default for TimestampOracle {
    fn default() -> Self {
        Self::new()
    }
}

impl TimestampOracle {
    pub fn new() -> Self {
        Self {
            next_ts: AtomicU64::new(1),
        }
    }

    /// Assign the next timestamp. Never returns the same value twice.
    pub fn next(&self) -> Timestamp {
        let ts = self.next_ts.fetch_add(1, Ordering::SeqCst);
        assert!(ts < u64::MAX, "timestamp overflow");
        Timestamp(ts)
    }

    /// Peek at the current value without consuming it.
    pub fn peek(&self) -> Timestamp {
        Timestamp(self.next_ts.load(Ordering::SeqCst))
    }
}

// ---------------------------------------------------------------------------
// Snapshot — frozen view of the database at transaction begin time
// ---------------------------------------------------------------------------

/// Captures what a transaction can see at the moment it begins.
///
/// A version is visible if its writer committed before `read_ts` AND
/// was not in `active_txns` when the snapshot was taken.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Versions committed at or before this timestamp are candidates for visibility.
    pub read_ts: Timestamp,
    /// Transactions that were active (uncommitted) when this snapshot was taken.
    /// Their writes are invisible even if they commit with commit_ts <= read_ts.
    pub active_txns: HashSet<TxnId>,
}
// ---------------------------------------------------------------------------
// TxnId — newtype over u64, follows PageId/Lsn pattern
// ---------------------------------------------------------------------------

/// Transaction identifier — monotonically increasing, never reused.
///
/// `TxnId(0)` is reserved for auto-commit operations (no explicit transaction).
/// `INVALID` (u64::MAX) is the sentinel for "no transaction".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TxnId(pub u64);

impl TxnId {
    /// Sentinel value representing "no transaction" or uninitialized state.
    pub const INVALID: TxnId = TxnId(u64::MAX);

    /// Reserved ID for auto-commit operations (no explicit transaction).
    pub const AUTO_COMMIT: TxnId = TxnId(0);

    /// Create a new TxnId.
    #[inline]
    pub fn new(id: u64) -> Self {
        TxnId(id)
    }

    /// Check if this transaction ID is valid (not the sentinel value).
    #[inline]
    pub fn is_valid(&self) -> bool {
        *self != Self::INVALID
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "Txn(INVALID)")
        } else {
            write!(f, "Txn({})", self.0)
        }
    }
}

// ---------------------------------------------------------------------------
// TxnState / TxnMode
// ---------------------------------------------------------------------------

/// Transaction lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// Transaction is active and accepting operations.
    Active,
    /// Transaction has been committed — all writes are durable.
    Committed,
    /// Transaction has been aborted — all writes have been undone.
    Aborted,
}

/// Transaction access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnMode {
    /// Read-only — acquires only shared locks, never modifies data.
    ReadOnly,
    /// Read-write — can acquire exclusive locks and modify data.
    ReadWrite,
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

/// Per-transaction state tracked by the manager.
///
/// The `start_lsn` and `last_lsn` fields enable WAL-based undo: on abort,
/// walk backward from `last_lsn` via `prev_lsn` chains to undo each write.
pub struct Transaction {
    /// Unique transaction identifier.
    pub id: TxnId,
    /// Current lifecycle state.
    pub state: TxnState,
    /// Access mode (read-only vs read-write).
    pub mode: TxnMode,
    /// LSN of the Begin WAL record.
    pub start_lsn: Lsn,
    /// LSN of the most recent WAL record for this transaction.
    /// Updated after each append. Used as `prev_lsn` for the next record.
    pub last_lsn: Lsn,

    pub begin_ts: Timestamp,
    pub commit_ts: Option<Timestamp>,
    pub snapshot: Snapshot,
}

impl Transaction {
    /// Create a new active transaction.
    fn new(
        id: TxnId,
        mode: TxnMode,
        start_lsn: Lsn,
        begin_ts: Timestamp,
        snapshot: Snapshot,
    ) -> Self {
        Self {
            id,
            state: TxnState::Active,
            mode,
            start_lsn,
            last_lsn: start_lsn,
            begin_ts,
            commit_ts: None,
            snapshot,
        }
    }
}

// ---------------------------------------------------------------------------
// TransactionManager
// ---------------------------------------------------------------------------

/// Maximum number of concurrent active transactions.
///
/// Bounded to prevent unbounded memory growth from leaked transactions.
const MAX_ACTIVE_TRANSACTIONS: usize = 65536;

/// Manages transaction lifecycle: begin, commit, abort.
///
/// All methods take `&self`. Internal state uses Mutex/RwLock for concurrency.
/// The lock manager and timestamp oracle are already thread-safe.
pub struct TransactionManager {
    /// Next transaction ID to assign. Starts at 1 (0 = auto-commit).
    next_txn_id: AtomicU64,
    /// All currently active (uncommitted, non-aborted) transactions.
    active_txns: Mutex<HashMap<TxnId, Transaction>>,
    /// Key-level lock manager for transaction isolation.
    lock_manager: LockManager,
    ts_oracle: TimestampOracle,
    /// Committed txns map: txn_id -> commit_ts. Readers frequent, writers rare.
    committed_txns: RwLock<HashMap<TxnId, Timestamp>>,
    /// Transactions known to be uncommitted (Begin but no Commit/Abort in WAL).
    uncommitted_txns: RwLock<HashSet<TxnId>>,
    /// Watermark: versions written before this timestamp are assumed committed.
    checkpoint_ts: AtomicU64,
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionManager {
    /// Create a new transaction manager.
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            active_txns: Mutex::new(HashMap::new()),
            lock_manager: LockManager::new(),
            ts_oracle: TimestampOracle::new(),
            committed_txns: RwLock::new(HashMap::new()),
            uncommitted_txns: RwLock::new(HashSet::new()),
            checkpoint_ts: AtomicU64::new(Timestamp::ZERO.0),
        }
    }

    /// Access the lock manager (for Database to acquire/release locks).
    pub fn lock_manager(&self) -> &LockManager {
        &self.lock_manager
    }

    /// Begin a new transaction, returning its assigned ID.
    pub fn begin(&self, mode: TxnMode, start_lsn: Lsn) -> Result<TxnId> {
        let mut active = self.active_txns.lock();
        if active.len() >= MAX_ACTIVE_TRANSACTIONS {
            return Err(Error::TxnLimit(MAX_ACTIVE_TRANSACTIONS));
        }

        let id = TxnId::new(self.next_txn_id.fetch_add(1, Ordering::SeqCst));

        let begin_ts = self.ts_oracle.next();
        let snapshot = Snapshot {
            read_ts: begin_ts,
            active_txns: active.keys().copied().collect(),
        };

        let txn = Transaction::new(id, mode, start_lsn, begin_ts, snapshot);
        active.insert(id, txn);

        Ok(id)
    }

    /// Mark a transaction as committed and remove from active set.
    pub fn commit(&self, txn_id: TxnId) -> Result<()> {
        let mut active = self.active_txns.lock();
        let txn = active
            .get_mut(&txn_id)
            .ok_or(Error::TxnNotActive(txn_id.0))?;
        txn.state = TxnState::Committed;
        active.remove(&txn_id);
        Ok(())
    }

    /// Mark a transaction as aborted and remove from active set.
    pub fn abort(&self, txn_id: TxnId) -> Result<()> {
        let mut active = self.active_txns.lock();
        let txn = active
            .get_mut(&txn_id)
            .ok_or(Error::TxnNotActive(txn_id.0))?;
        txn.state = TxnState::Aborted;
        active.remove(&txn_id);
        Ok(())
    }

    /// Update the last LSN for a transaction after a WAL append.
    pub fn update_last_lsn(&self, txn_id: TxnId, lsn: Lsn) -> Result<()> {
        let mut active = self.active_txns.lock();
        let txn = active
            .get_mut(&txn_id)
            .ok_or(Error::TxnNotActive(txn_id.0))?;
        txn.last_lsn = lsn;
        Ok(())
    }

    /// Get the last LSN for a transaction (for prev_lsn chain).
    pub fn last_lsn(&self, txn_id: TxnId) -> Result<Lsn> {
        let active = self.active_txns.lock();
        let txn = active
            .get(&txn_id)
            .ok_or(Error::TxnNotActive(txn_id.0))?;
        Ok(txn.last_lsn)
    }

    /// Get the mode of a transaction.
    pub fn mode(&self, txn_id: TxnId) -> Result<TxnMode> {
        let active = self.active_txns.lock();
        let txn = active
            .get(&txn_id)
            .ok_or(Error::TxnNotActive(txn_id.0))?;
        Ok(txn.mode)
    }

    /// Get all active transaction IDs — used by checkpoint.
    pub fn active_txn_ids(&self) -> Vec<u64> {
        self.active_txns.lock().keys().map(|id| id.0).collect()
    }

    /// Number of currently active transactions.
    pub fn active_count(&self) -> usize {
        self.active_txns.lock().len()
    }

    /// Get all active snapshot read_ts values (for GC watermark computation).
    pub fn active_read_timestamps(&self) -> Vec<Timestamp> {
        self.active_txns.lock().values().map(|txn| txn.begin_ts).collect()
    }

    /// Peek at the next transaction ID without consuming it.
    pub fn next_txn_id_peek(&self) -> u64 {
        self.next_txn_id.load(Ordering::SeqCst)
    }

    /// Get a transaction's snapshot (for MVCC reads). Returns a clone.
    pub fn snapshot(&self, txn_id: TxnId) -> Result<Snapshot> {
        let active = self.active_txns.lock();
        active
            .get(&txn_id)
            .map(|txn| txn.snapshot.clone())
            .ok_or(Error::TxnNotActive(txn_id.0))
    }

    /// Get a transaction's begin timestamp.
    pub fn begin_ts(&self, txn_id: TxnId) -> Result<Timestamp> {
        let active = self.active_txns.lock();
        active
            .get(&txn_id)
            .map(|txn| txn.begin_ts)
            .ok_or(Error::TxnNotActive(txn_id.0))
    }

    /// Access the committed transactions map (for visibility checks).
    /// Returns a clone to avoid holding the RwLock across engine operations.
    pub fn committed_txns(&self) -> HashMap<TxnId, Timestamp> {
        self.committed_txns.read().clone()
    }

    /// Access the known-uncommitted transactions set (for visibility checks).
    pub fn uncommitted_txns(&self) -> HashSet<TxnId> {
        self.uncommitted_txns.read().clone()
    }

    /// Load uncommitted txn_ids from recovery.
    pub fn load_uncommitted_txns(&self, txns: HashSet<TxnId>) {
        *self.uncommitted_txns.write() = txns;
    }

    /// Advance the txn-id counter past `max_seen` so future `begin()` calls
    /// can never reuse an id that already appears in the engine. Recovery
    /// calls this with the highest txn_id seen across committed, aborted,
    /// and uncommitted transactions (and the persisted checkpoint mark).
    /// Reusing an id would let a fresh commit retroactively make leftover
    /// MVCC versions from a prior crash visible.
    pub fn advance_next_txn_id_past(&self, max_seen: u64) {
        let _ = self.next_txn_id.fetch_max(max_seen + 1, Ordering::SeqCst);
    }

    /// Assign a commit timestamp and record it. Called during commit.
    pub fn assign_commit_ts(&self, txn_id: TxnId) -> Result<Timestamp> {
        let mut active = self.active_txns.lock();
        let txn = active
            .get_mut(&txn_id)
            .ok_or(Error::TxnNotActive(txn_id.0))?;
        let commit_ts = self.ts_oracle.next();
        txn.commit_ts = Some(commit_ts);
        self.committed_txns.write().insert(txn_id, commit_ts);
        Ok(commit_ts)
    }

    /// Peek at the oracle's current timestamp (for auto-commit snapshots).
    pub fn ts_oracle_peek(&self) -> Timestamp {
        self.ts_oracle.peek()
    }

    /// Get the checkpoint watermark timestamp.
    pub fn checkpoint_ts(&self) -> Timestamp {
        Timestamp(self.checkpoint_ts.load(Ordering::Relaxed))
    }

    /// Update checkpoint watermark (called after successful checkpoint).
    pub fn set_checkpoint_ts(&self, ts: Timestamp) {
        self.checkpoint_ts.store(ts.0, Ordering::Relaxed);
    }

    /// Advance the oracle by one tick. Used during recovery to skip past
    /// persisted timestamps so new timestamps don't collide.
    pub fn advance_oracle(&self) {
        self.ts_oracle.next();
    }

    /// Seed the committed_txns map from recovery.
    /// Also advances the oracle past recovered timestamps.
    pub fn load_committed_txns(&self, recovered: HashMap<TxnId, Timestamp>) {
        let mut max_ts: u64 = 0;
        let mut max_txn_id: u64 = 0;
        for (&txn_id, &ts) in &recovered {
            if ts.0 > max_ts {
                max_ts = ts.0;
            }
            if txn_id.0 > max_txn_id {
                max_txn_id = txn_id.0;
            }
        }
        *self.committed_txns.write() = recovered;
        // Advance oracle past any recovered timestamp so new timestamps don't collide.
        while self.ts_oracle.peek().0 <= max_ts {
            self.ts_oracle.next();
        }
        // Advance txn_id counter past any recovered txn.
        // Use compare-and-swap to only raise it, never lower.
        let _ = self.next_txn_id.fetch_max(max_txn_id + 1, Ordering::SeqCst);
        // Set checkpoint watermark so pre-recovery versions are visible.
        self.checkpoint_ts.store(max_ts, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_id_new_and_valid() {
        let id = TxnId::new(42);
        assert_eq!(id.0, 42);
        assert!(id.is_valid());
    }

    #[test]
    fn txn_id_invalid() {
        assert!(!TxnId::INVALID.is_valid());
        assert_eq!(TxnId::INVALID.0, u64::MAX);
    }

    #[test]
    fn txn_id_auto_commit() {
        assert_eq!(TxnId::AUTO_COMMIT.0, 0);
        assert!(TxnId::AUTO_COMMIT.is_valid());
    }

    #[test]
    fn txn_id_ordering() {
        assert!(TxnId::new(1) < TxnId::new(2));
        assert!(TxnId::new(5) > TxnId::new(3));
        assert_eq!(TxnId::new(7), TxnId::new(7));
    }

    #[test]
    fn txn_id_display() {
        assert_eq!(format!("{}", TxnId::new(42)), "Txn(42)");
        assert_eq!(format!("{}", TxnId::INVALID), "Txn(INVALID)");
    }

    #[test]
    fn txn_id_hash_and_eq() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(TxnId::new(1));
        set.insert(TxnId::new(2));
        set.insert(TxnId::new(1));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn begin_assigns_unique_increasing_ids() {
        let mgr = TransactionManager::new();

        let id1 = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        let id2 = mgr.begin(TxnMode::ReadWrite, Lsn::new(1)).unwrap();
        let id3 = mgr.begin(TxnMode::ReadOnly, Lsn::new(2)).unwrap();

        assert_eq!(id1, TxnId::new(1));
        assert_eq!(id2, TxnId::new(2));
        assert_eq!(id3, TxnId::new(3));
        assert!(id1 < id2);
        assert!(id2 < id3);
    }

    #[test]
    fn commit_removes_from_active_set() {
        let mgr = TransactionManager::new();

        let id = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        assert_eq!(mgr.active_count(), 1);

        mgr.commit(id).unwrap();
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn abort_removes_from_active_set() {
        let mgr = TransactionManager::new();

        let id = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        assert_eq!(mgr.active_count(), 1);

        mgr.abort(id).unwrap();
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn operations_after_commit_return_error() {
        let mgr = TransactionManager::new();

        let id = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        mgr.commit(id).unwrap();

        // All operations on a committed txn should fail.
        assert!(mgr.commit(id).is_err());
        assert!(mgr.abort(id).is_err());
        assert!(mgr.update_last_lsn(id, Lsn::new(5)).is_err());
        assert!(mgr.last_lsn(id).is_err());
        assert!(mgr.mode(id).is_err());
    }

    #[test]
    fn operations_after_abort_return_error() {
        let mgr = TransactionManager::new();

        let id = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        mgr.abort(id).unwrap();

        assert!(mgr.commit(id).is_err());
        assert!(mgr.abort(id).is_err());
        assert!(mgr.update_last_lsn(id, Lsn::new(5)).is_err());
    }

    #[test]
    fn multiple_concurrent_transactions() {
        let mgr = TransactionManager::new();

        let id1 = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        let id2 = mgr.begin(TxnMode::ReadOnly, Lsn::new(1)).unwrap();
        let id3 = mgr.begin(TxnMode::ReadWrite, Lsn::new(2)).unwrap();

        assert_eq!(mgr.active_count(), 3);

        // Commit one, abort another — third still active.
        mgr.commit(id1).unwrap();
        assert_eq!(mgr.active_count(), 2);

        mgr.abort(id3).unwrap();
        assert_eq!(mgr.active_count(), 1);

        // id2 still active.
        assert_eq!(mgr.mode(id2).unwrap(), TxnMode::ReadOnly);
        assert_eq!(mgr.last_lsn(id2).unwrap(), Lsn::new(1));
    }

    #[test]
    fn active_txn_ids_for_checkpoint() {
        let mgr = TransactionManager::new();

        let id1 = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        let _id2 = mgr.begin(TxnMode::ReadOnly, Lsn::new(1)).unwrap();
        let id3 = mgr.begin(TxnMode::ReadWrite, Lsn::new(2)).unwrap();

        mgr.commit(id1).unwrap();

        let mut active = mgr.active_txn_ids();
        active.sort();
        assert_eq!(active, vec![2, 3]);

        mgr.abort(id3).unwrap();
        let active = mgr.active_txn_ids();
        assert_eq!(active, vec![2]);
    }

    #[test]
    fn update_and_read_last_lsn() {
        let mgr = TransactionManager::new();

        let id = mgr.begin(TxnMode::ReadWrite, Lsn::new(10)).unwrap();
        assert_eq!(mgr.last_lsn(id).unwrap(), Lsn::new(10));

        mgr.update_last_lsn(id, Lsn::new(15)).unwrap();
        assert_eq!(mgr.last_lsn(id).unwrap(), Lsn::new(15));

        mgr.update_last_lsn(id, Lsn::new(22)).unwrap();
        assert_eq!(mgr.last_lsn(id).unwrap(), Lsn::new(22));
    }

    #[test]
    fn begin_respects_mode() {
        let mgr = TransactionManager::new();

        let rw = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        let ro = mgr.begin(TxnMode::ReadOnly, Lsn::new(1)).unwrap();

        assert_eq!(mgr.mode(rw).unwrap(), TxnMode::ReadWrite);
        assert_eq!(mgr.mode(ro).unwrap(), TxnMode::ReadOnly);
    }

    #[test]
    fn unknown_txn_id_returns_error() {
        let mgr = TransactionManager::new();
        assert!(mgr.last_lsn(TxnId::new(999)).is_err());
        assert!(mgr.mode(TxnId::new(999)).is_err());
    }
}
