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

pub mod lock_manager;

use std::collections::HashMap;
use std::fmt;

use crate::common::{Error, Result};
use crate::wal::Lsn;

pub use lock_manager::{LockManager, LockMode};

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
}

impl Transaction {
    /// Create a new active transaction.
    fn new(id: TxnId, mode: TxnMode, start_lsn: Lsn) -> Self {
        Self {
            id,
            state: TxnState::Active,
            mode,
            start_lsn,
            last_lsn: start_lsn,
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
/// Assigns monotonically increasing IDs starting from 1 (0 is reserved
/// for auto-commit). Tracks all active transactions and their WAL state.
pub struct TransactionManager {
    /// Next transaction ID to assign. Starts at 1 (0 = auto-commit).
    next_txn_id: u64,
    /// All currently active (uncommitted, non-aborted) transactions.
    active_txns: HashMap<TxnId, Transaction>,
    /// Key-level lock manager for transaction isolation.
    lock_manager: LockManager,
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
            next_txn_id: 1,
            active_txns: HashMap::new(),
            lock_manager: LockManager::new(),
        }
    }

    /// Access the lock manager (for Database to acquire/release locks).
    pub fn lock_manager(&self) -> &LockManager {
        &self.lock_manager
    }

    /// Begin a new transaction, returning its assigned ID.
    ///
    /// The caller is responsible for appending the Begin WAL record and
    /// passing the resulting LSN back via the `start_lsn` parameter.
    /// This two-step approach keeps WAL ownership in `Database<E>`.
    pub fn begin(&mut self, mode: TxnMode, start_lsn: Lsn) -> Result<TxnId> {
        if self.active_txns.len() >= MAX_ACTIVE_TRANSACTIONS {
            return Err(Error::TxnLimit(MAX_ACTIVE_TRANSACTIONS));
        }

        let id = TxnId::new(self.next_txn_id);
        self.next_txn_id += 1;

        let txn = Transaction::new(id, mode, start_lsn);
        self.active_txns.insert(id, txn);

        Ok(id)
    }

    /// Mark a transaction as committed and remove from active set.
    ///
    /// The caller is responsible for writing the Commit WAL record and
    /// syncing before calling this. This method only updates bookkeeping.
    pub fn commit(&mut self, txn_id: TxnId) -> Result<()> {
        let txn = self.get_active_mut(txn_id)?;
        txn.state = TxnState::Committed;
        self.active_txns.remove(&txn_id);
        Ok(())
    }

    /// Mark a transaction as aborted and remove from active set.
    ///
    /// The caller is responsible for undoing writes (via prev_lsn chain)
    /// and writing the Abort WAL record before calling this.
    pub fn abort(&mut self, txn_id: TxnId) -> Result<()> {
        let txn = self.get_active_mut(txn_id)?;
        txn.state = TxnState::Aborted;
        self.active_txns.remove(&txn_id);
        Ok(())
    }

    /// Update the last LSN for a transaction after a WAL append.
    ///
    /// Called after each Put/Delete WAL record is appended, so the next
    /// record in the chain has the correct `prev_lsn`.
    pub fn update_last_lsn(&mut self, txn_id: TxnId, lsn: Lsn) -> Result<()> {
        let txn = self.get_active_mut(txn_id)?;
        txn.last_lsn = lsn;
        Ok(())
    }

    /// Get the last LSN for a transaction (for prev_lsn chain).
    pub fn last_lsn(&self, txn_id: TxnId) -> Result<Lsn> {
        let txn = self.get_active(txn_id)?;
        Ok(txn.last_lsn)
    }

    /// Get the mode of a transaction.
    pub fn mode(&self, txn_id: TxnId) -> Result<TxnMode> {
        let txn = self.get_active(txn_id)?;
        Ok(txn.mode)
    }

    /// Get all active transaction IDs — used by checkpoint.
    pub fn active_txn_ids(&self) -> Vec<u64> {
        self.active_txns.keys().map(|id| id.0).collect()
    }

    /// Number of currently active transactions.
    pub fn active_count(&self) -> usize {
        self.active_txns.len()
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Get a reference to an active transaction, or error.
    fn get_active(&self, txn_id: TxnId) -> Result<&Transaction> {
        self.active_txns
            .get(&txn_id)
            .ok_or(Error::TxnNotActive(txn_id.0))
    }

    /// Get a mutable reference to an active transaction, or error.
    fn get_active_mut(&mut self, txn_id: TxnId) -> Result<&mut Transaction> {
        self.active_txns
            .get_mut(&txn_id)
            .ok_or(Error::TxnNotActive(txn_id.0))
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
        let mut mgr = TransactionManager::new();

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
        let mut mgr = TransactionManager::new();

        let id = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        assert_eq!(mgr.active_count(), 1);

        mgr.commit(id).unwrap();
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn abort_removes_from_active_set() {
        let mut mgr = TransactionManager::new();

        let id = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        assert_eq!(mgr.active_count(), 1);

        mgr.abort(id).unwrap();
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn operations_after_commit_return_error() {
        let mut mgr = TransactionManager::new();

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
        let mut mgr = TransactionManager::new();

        let id = mgr.begin(TxnMode::ReadWrite, Lsn::new(0)).unwrap();
        mgr.abort(id).unwrap();

        assert!(mgr.commit(id).is_err());
        assert!(mgr.abort(id).is_err());
        assert!(mgr.update_last_lsn(id, Lsn::new(5)).is_err());
    }

    #[test]
    fn multiple_concurrent_transactions() {
        let mut mgr = TransactionManager::new();

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
        let mut mgr = TransactionManager::new();

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
        let mut mgr = TransactionManager::new();

        let id = mgr.begin(TxnMode::ReadWrite, Lsn::new(10)).unwrap();
        assert_eq!(mgr.last_lsn(id).unwrap(), Lsn::new(10));

        mgr.update_last_lsn(id, Lsn::new(15)).unwrap();
        assert_eq!(mgr.last_lsn(id).unwrap(), Lsn::new(15));

        mgr.update_last_lsn(id, Lsn::new(22)).unwrap();
        assert_eq!(mgr.last_lsn(id).unwrap(), Lsn::new(22));
    }

    #[test]
    fn begin_respects_mode() {
        let mut mgr = TransactionManager::new();

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
