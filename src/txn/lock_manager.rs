//! Key-level lock manager for transaction isolation.
//!
//! Provides shared (S) and exclusive (X) locks with FIFO wait queues.
//! Transactions acquire S locks for reads and X locks for writes.
//! Lock upgrades (S→X) are supported when the txn is the sole holder.
//!
//! ## Compatibility Matrix
//!
//! | Request \ Held | None | Shared | Exclusive |
//! |---|---|---|---|
//! | Shared | Grant | Grant | Wait |
//! | Exclusive | Grant | Wait | Wait |
//!
//! ## Fairness
//!
//! Requests are queued in FIFO order. New requests that are compatible
//! with the current mode are only granted immediately if there are no
//! waiters ahead of them. This prevents starvation of exclusive requests
//! by a continuous stream of shared requests.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use super::TxnId;
use crate::common::{Error, Result};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default timeout for blocking lock acquisition.
const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum entries in the lock table — bounded to prevent unbounded growth.
const MAX_LOCK_TABLE_ENTRIES: usize = 1_000_000;

// ---------------------------------------------------------------------------
// LockMode
// ---------------------------------------------------------------------------

/// Lock mode: shared (readers) or exclusive (writers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Multiple transactions can hold shared locks simultaneously.
    Shared,
    /// Only one transaction can hold an exclusive lock.
    Exclusive,
}

impl LockMode {
    /// Check if two lock modes are compatible (can be held simultaneously).
    fn is_compatible(self, other: Self) -> bool {
        matches!((self, other), (LockMode::Shared, LockMode::Shared))
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// A pending lock request in the wait queue.
struct LockRequest {
    txn_id: TxnId,
    mode: LockMode,
    /// Shared flag: set to true by the granter, read by the waiter.
    granted: Arc<AtomicBool>,
}

/// Per-key lock state: who holds it, who's waiting.
struct LockEntry {
    /// Current granted lock mode. None if no holders.
    granted_mode: Option<LockMode>,
    /// Transactions currently holding this lock.
    holders: HashSet<TxnId>,
    /// FIFO queue of pending requests.
    wait_queue: VecDeque<LockRequest>,
}

impl LockEntry {
    fn new() -> Self {
        Self {
            granted_mode: None,
            holders: HashSet::new(),
            wait_queue: VecDeque::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.holders.is_empty() && self.wait_queue.is_empty()
    }
}

/// Internal state protected by the mutex.
struct LockState {
    /// Key → lock entry.
    lock_table: HashMap<Vec<u8>, LockEntry>,
    /// TxnId → set of keys held by that transaction (reverse index).
    txn_locks: HashMap<TxnId, HashSet<Vec<u8>>>,
}

impl LockState {
    fn new() -> Self {
        Self {
            lock_table: HashMap::new(),
            txn_locks: HashMap::new(),
        }
    }

    /// Record that txn_id now holds a lock on key.
    fn record_hold(&mut self, txn_id: TxnId, key: &[u8]) {
        self.txn_locks
            .entry(txn_id)
            .or_default()
            .insert(key.to_vec());
    }
}

/// Result of trying to grant a lock immediately (no waiting).
enum GrantResult {
    /// Lock was granted — txn added to holders.
    Granted,
    /// Txn already holds a compatible or stronger lock — no-op.
    AlreadyHeld,
    /// Lock cannot be granted now — must wait.
    MustWait,
}

// ---------------------------------------------------------------------------
// LockManager
// ---------------------------------------------------------------------------

/// Key-level lock manager with shared/exclusive modes and FIFO wait queues.
///
/// Thread-safe: all methods take `&self` and synchronize internally via
/// a single `parking_lot::Mutex`. Blocked threads wait on a `Condvar`
/// and are woken when any lock is released.
pub struct LockManager {
    state: Mutex<LockState>,
    condvar: Condvar,
    lock_timeout: Duration,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    /// Create a lock manager with default timeout (5 seconds).
    pub fn new() -> Self {
        Self {
            state: Mutex::new(LockState::new()),
            condvar: Condvar::new(),
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
        }
    }

    /// Create a lock manager with a custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            state: Mutex::new(LockState::new()),
            condvar: Condvar::new(),
            lock_timeout: timeout,
        }
    }

    /// Acquire a lock on a key for a transaction.
    ///
    /// If the lock can be granted immediately (no conflict), returns at once.
    /// If there's a conflict, blocks until the lock is granted or timeout.
    ///
    /// # Reentrant behavior
    /// - Already hold X → any request is a no-op (X subsumes S).
    /// - Already hold S, request S → no-op.
    /// - Already hold S, request X → upgrade if sole holder, else wait.
    ///
    /// # Errors
    /// - `LockTimeout` if blocked beyond the configured timeout.
    pub fn acquire(&self, txn_id: TxnId, key: &[u8], mode: LockMode) -> Result<()> {
        let key_vec = key.to_vec();
        let mut state = self.state.lock();

        // Ensure lock entry exists for this key.
        if !state.lock_table.contains_key(&key_vec) {
            if state.lock_table.len() >= MAX_LOCK_TABLE_ENTRIES {
                return Err(Error::StorageCorrupted(
                    "Lock table entry limit reached".into(),
                ));
            }
            state.lock_table.insert(key_vec.clone(), LockEntry::new());
        }

        // Try immediate grant.
        match Self::try_grant_immediate(&mut state, &key_vec, txn_id, mode) {
            GrantResult::Granted | GrantResult::AlreadyHeld => return Ok(()),
            GrantResult::MustWait => {}
        }

        // Cannot grant immediately — add to wait queue and block.
        let granted_flag = Arc::new(AtomicBool::new(false));
        {
            let entry = state.lock_table.get_mut(&key_vec).unwrap();
            entry.wait_queue.push_back(LockRequest {
                txn_id,
                mode,
                granted: granted_flag.clone(),
            });
        }

        {
            let graph = Self::build_wait_for_graph(&state);
            if let Some(cycle) = Self::detect_cycle(&graph, txn_id) {
                let _victim = Self::choose_victim(&cycle);
                if let Some(entry) = state.lock_table.get_mut(&key_vec) {
                    entry
                        .wait_queue
                        .retain(|r| !Arc::ptr_eq(&r.granted, &granted_flag));
                    if entry.is_empty() {
                        state.lock_table.remove(&key_vec);
                    }
                }
                return Err(Error::Deadlock(txn_id.0));
            }
        }

        // Block until granted or timeout.
        let deadline = Instant::now() + self.lock_timeout;
        loop {
            if granted_flag.load(Ordering::Acquire) {
                // Lock was granted by a releasing thread.
                return Ok(());
            }

            let now = Instant::now();
            if now >= deadline {
                // Timeout — remove our request from the wait queue.
                if let Some(entry) = state.lock_table.get_mut(&key_vec) {
                    entry
                        .wait_queue
                        .retain(|r| !Arc::ptr_eq(&r.granted, &granted_flag));
                    if entry.is_empty() {
                        state.lock_table.remove(&key_vec);
                    }
                }
                return Err(Error::LockTimeout);
            }

            // Release mutex and sleep until notified or deadline.
            self.condvar.wait_until(&mut state, deadline);
        }
    }

    /// Release a specific lock held by a transaction.
    ///
    /// If the txn doesn't hold the lock, this is a no-op.
    /// After releasing, eligible waiters in the queue are granted.
    pub fn release(&self, txn_id: TxnId, key: &[u8]) {
        let mut state = self.state.lock();

        // Remove from reverse index.
        if let Some(keys) = state.txn_locks.get_mut(&txn_id) {
            keys.remove(key);
            if keys.is_empty() {
                state.txn_locks.remove(&txn_id);
            }
        }

        let woke_any = Self::release_key(&mut state, txn_id, key);
        if woke_any {
            self.condvar.notify_all();
        }
    }

    /// Release all locks held by a transaction.
    ///
    /// Called during commit and abort to free all held locks at once.
    pub fn release_all(&self, txn_id: TxnId) {
        let mut state = self.state.lock();

        // Take all keys from reverse index.
        let keys: Vec<Vec<u8>> = state
            .txn_locks
            .remove(&txn_id)
            .map(|set| set.into_iter().collect())
            .unwrap_or_default();

        let mut woke_any = false;
        for key in &keys {
            woke_any |= Self::release_key(&mut state, txn_id, key);
        }

        if woke_any {
            self.condvar.notify_all();
        }
    }

    /// List all keys currently locked by a transaction.
    pub fn held_by(&self, txn_id: TxnId) -> Vec<Vec<u8>> {
        let state = self.state.lock();
        state
            .txn_locks
            .get(&txn_id)
            .map(|keys| {
                let mut sorted: Vec<Vec<u8>> = keys.iter().cloned().collect();
                sorted.sort();
                sorted
            })
            .unwrap_or_default()
    }

    /// Number of keys currently in the lock table (for diagnostics).
    pub fn lock_count(&self) -> usize {
        self.state.lock().lock_table.len()
    }

    // -----------------------------------------------------------------------
    // Private: immediate grant logic
    // -----------------------------------------------------------------------

    /// Try to grant the lock without blocking. Returns the outcome.
    fn try_grant_immediate(
        state: &mut LockState,
        key: &[u8],
        txn_id: TxnId,
        mode: LockMode,
    ) -> GrantResult {
        let entry = state.lock_table.get_mut(key).unwrap();

        // Case 1: No holders → grant unconditionally.
        if entry.holders.is_empty() {
            entry.holders.insert(txn_id);
            entry.granted_mode = Some(mode);
            state.record_hold(txn_id, key);
            return GrantResult::Granted;
        }

        // Case 2: Reentrant — txn already holds this lock.
        if entry.holders.contains(&txn_id) {
            let current_mode = entry.granted_mode.unwrap();
            match (current_mode, mode) {
                // Already hold X → subsumes any request.
                (LockMode::Exclusive, _) => return GrantResult::AlreadyHeld,
                // Already hold S, request S → already satisfied.
                (LockMode::Shared, LockMode::Shared) => return GrantResult::AlreadyHeld,
                // Upgrade: S → X when sole holder.
                (LockMode::Shared, LockMode::Exclusive) => {
                    if entry.holders.len() == 1 {
                        entry.granted_mode = Some(LockMode::Exclusive);
                        return GrantResult::Granted;
                    }
                    // Other S holders exist → must wait for them to release.
                    return GrantResult::MustWait;
                }
            }
        }

        // Case 3: Not a holder. Grant S immediately if current mode is S
        // AND no waiters ahead (FIFO fairness prevents S starvation of X).
        if entry.wait_queue.is_empty() {
            let current_mode = entry.granted_mode.unwrap();
            if mode.is_compatible(current_mode) {
                entry.holders.insert(txn_id);
                state.record_hold(txn_id, key);
                return GrantResult::Granted;
            }
        }

        GrantResult::MustWait
    }

    // -----------------------------------------------------------------------
    // Private: release and waiter granting
    // -----------------------------------------------------------------------

    /// Remove txn from a key's holders and try to grant waiters.
    /// Returns true if any waiter was granted (caller should notify condvar).
    fn release_key(state: &mut LockState, txn_id: TxnId, key: &[u8]) -> bool {
        let Some(entry) = state.lock_table.get_mut(key) else {
            return false;
        };

        if !entry.holders.remove(&txn_id) {
            return false;
        }

        // If no holders left, clear granted mode.
        if entry.holders.is_empty() {
            entry.granted_mode = None;
        }

        // Try to grant pending waiters.
        let woke_any = Self::try_grant_waiters(state, key);

        // Clean up empty entries to prevent table growth.
        if let Some(entry) = state.lock_table.get(key) {
            if entry.is_empty() {
                state.lock_table.remove(key);
            }
        }

        woke_any
    }

    /// Walk the wait queue front-to-back, granting compatible requests.
    /// Returns true if any request was granted.
    ///
    /// Two-phase approach to satisfy the borrow checker: first mutate the
    /// lock entry (which borrows lock_table), then update the reverse index
    /// (which borrows txn_locks) after dropping the entry borrow.
    fn try_grant_waiters(state: &mut LockState, key: &[u8]) -> bool {
        // Phase 1: Grant waiters and collect new holders for reverse index.
        let mut new_holders: Vec<TxnId> = Vec::new();
        let mut granted_any = false;
        {
            let Some(entry) = state.lock_table.get_mut(key) else {
                return false;
            };

            while let Some(front) = entry.wait_queue.front() {
                let is_upgrade = entry.holders.contains(&front.txn_id);

                let can_grant = if entry.holders.is_empty() {
                    true
                } else if is_upgrade {
                    entry.holders.len() == 1
                } else {
                    let current_mode = entry.granted_mode.unwrap();
                    front.mode.is_compatible(current_mode)
                };

                if !can_grant {
                    break;
                }

                let request = entry.wait_queue.pop_front().unwrap();

                if !is_upgrade {
                    entry.holders.insert(request.txn_id);
                    new_holders.push(request.txn_id);
                }

                entry.granted_mode = Some(if entry.holders.len() == 1 {
                    request.mode
                } else {
                    LockMode::Shared
                });

                request.granted.store(true, Ordering::Release);
                granted_any = true;

                if request.mode == LockMode::Exclusive {
                    break;
                }
            }
        }
        // Phase 2: Update reverse index (entry borrow is dropped).
        for txn_id in new_holders {
            state.record_hold(txn_id, key);
        }

        granted_any
    }
    /// Deadlock detection
    /// Build a wait-for graph from current lock state
    /// HOW: For each key with waiters, add an edge from each waiter to each holder. When the waiter's
    /// requested mode is incompatible with the granted mode. Edges represent "this waiter is blocked
    /// by this hold"
    /// WHY: A cycle means deadlock. No txn in the cycle can make progress.
    fn build_wait_for_graph(state: &LockState) -> HashMap<TxnId, HashSet<TxnId>> {
        let mut graph: HashMap<TxnId, HashSet<TxnId>> = HashMap::new();
        for entry in state.lock_table.values() {
            if entry.wait_queue.is_empty() {
                continue;
            }

            let granted_mode = match entry.granted_mode {
                Some(mode) => mode,
                None => continue,
            };

            for request in &entry.wait_queue {
                if request.mode.is_compatible(granted_mode) {
                    continue;
                }

                for &holder_id in &entry.holders {
                    if holder_id == request.txn_id {
                        continue;
                    }
                    graph.entry(request.txn_id).or_default().insert(holder_id);
                }
            }
        }
        graph
    }
    /// Detect if start_txn is part of a cycle
    /// HOW: BFS from start_txn neighbors. If any path leads back to strat_txn, there is a cycle.
    /// WHY: Only the newly queued waiter can complete a new cycle.
    ///
    fn detect_cycle(
        graph: &HashMap<TxnId, HashSet<TxnId>>,
        start_txn: TxnId,
    ) -> Option<Vec<TxnId>> {
        let start_neighbors = graph.get(&start_txn)?;

        let mut parent: HashMap<TxnId, TxnId> = HashMap::new();
        let mut queue: VecDeque<TxnId> = VecDeque::new();

        for &neighbor in start_neighbors {
            parent.insert(neighbor, start_txn);
            queue.push_back(neighbor);
        }

        let mut iterations = 0usize;
        const MAX_BFS_ITERATIONS: usize = 65536;

        while let Some(current) = queue.pop_front() {
            iterations += 1;
            assert!(
                iterations <= MAX_BFS_ITERATIONS,
                "BFS exceeded max iterations in deadlock detection"
            );

            let neighbors = match graph.get(&current) {
                Some(n) => n,
                None => continue,
            };

            for &neighbor in neighbors {
                if neighbor == start_txn {
                    let mut cycle = Vec::new();
                    let mut node = current;
                    while node != start_txn {
                        cycle.push(node);
                        node = parent[&node];
                    }
                    cycle.push(start_txn);
                    return Some(cycle);
                }
                if let std::collections::hash_map::Entry::Vacant(e) = parent.entry(neighbor) {
                    e.insert(current);
                    queue.push_back(neighbor);
                }
            }
        }
        None
    }

    ///Choose the deadlock victim from a cycle. The youngest transaction.
    ///WHY: The youngest transaction has done the least work, so aborting wastes the fewest resources.
    fn choose_victim(cycle: &[TxnId]) -> TxnId {
        assert!(!cycle.is_empty(), "Cycle must not be empty");
        *cycle.iter().max().unwrap()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    /// Small timeout for tests that expect blocking.
    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    /// Small sleep to let a spawned thread reach the condvar wait.
    const SETTLE_TIME: Duration = Duration::from_millis(50);

    fn lm() -> Arc<LockManager> {
        Arc::new(LockManager::with_timeout(TEST_TIMEOUT))
    }

    // -------------------------------------------------------------------
    // Non-blocking tests (single thread)
    // -------------------------------------------------------------------

    #[test]
    fn acquire_shared_unlocked() {
        let lm = LockManager::new();
        lm.acquire(TxnId::new(1), b"key", LockMode::Shared).unwrap();
        assert_eq!(lm.held_by(TxnId::new(1)), vec![b"key".to_vec()]);
    }

    #[test]
    fn acquire_exclusive_unlocked() {
        let lm = LockManager::new();
        lm.acquire(TxnId::new(1), b"key", LockMode::Exclusive)
            .unwrap();
        assert_eq!(lm.held_by(TxnId::new(1)), vec![b"key".to_vec()]);
    }

    #[test]
    fn acquire_shared_shared() {
        let lm = LockManager::new();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);
        lm.acquire(t1, b"key", LockMode::Shared).unwrap();
        lm.acquire(t2, b"key", LockMode::Shared).unwrap();
        assert_eq!(lm.held_by(t1), vec![b"key".to_vec()]);
        assert_eq!(lm.held_by(t2), vec![b"key".to_vec()]);
    }

    #[test]
    fn lock_upgrade_sole_holder() {
        let lm = LockManager::new();
        let t1 = TxnId::new(1);
        lm.acquire(t1, b"key", LockMode::Shared).unwrap();
        // Sole S holder → upgrade to X immediately.
        lm.acquire(t1, b"key", LockMode::Exclusive).unwrap();
        assert_eq!(lm.held_by(t1), vec![b"key".to_vec()]);
        assert_eq!(lm.lock_count(), 1);
    }

    #[test]
    fn reentrant_same_mode() {
        let lm = LockManager::new();
        let t1 = TxnId::new(1);

        // S then S → no-op.
        lm.acquire(t1, b"key", LockMode::Shared).unwrap();
        lm.acquire(t1, b"key", LockMode::Shared).unwrap();

        // X then X → no-op.
        lm.acquire(t1, b"k2", LockMode::Exclusive).unwrap();
        lm.acquire(t1, b"k2", LockMode::Exclusive).unwrap();

        // X then S → no-op (X subsumes S).
        lm.acquire(t1, b"k2", LockMode::Shared).unwrap();
    }

    #[test]
    fn release_all_frees_all_locks() {
        let lm = LockManager::new();
        let t1 = TxnId::new(1);
        lm.acquire(t1, b"a", LockMode::Exclusive).unwrap();
        lm.acquire(t1, b"b", LockMode::Shared).unwrap();
        lm.acquire(t1, b"c", LockMode::Exclusive).unwrap();

        assert_eq!(lm.held_by(t1).len(), 3);
        assert_eq!(lm.lock_count(), 3);

        lm.release_all(t1);

        assert!(lm.held_by(t1).is_empty());
        assert_eq!(lm.lock_count(), 0);
    }

    #[test]
    fn held_by_returns_sorted_keys() {
        let lm = LockManager::new();
        let t1 = TxnId::new(1);
        lm.acquire(t1, b"cherry", LockMode::Shared).unwrap();
        lm.acquire(t1, b"apple", LockMode::Exclusive).unwrap();
        lm.acquire(t1, b"banana", LockMode::Shared).unwrap();

        let keys = lm.held_by(t1);
        assert_eq!(
            keys,
            vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()]
        );
    }

    #[test]
    fn lock_entry_cleanup() {
        let lm = LockManager::new();
        let t1 = TxnId::new(1);

        lm.acquire(t1, b"key", LockMode::Exclusive).unwrap();
        assert_eq!(lm.lock_count(), 1);

        lm.release(t1, b"key");
        assert_eq!(lm.lock_count(), 0);
    }

    #[test]
    fn release_nonexistent_is_noop() {
        let lm = LockManager::new();
        // Releasing a lock that was never acquired should not panic.
        lm.release(TxnId::new(1), b"key");
        lm.release_all(TxnId::new(1));
    }

    // -------------------------------------------------------------------
    // Blocking tests (multi-thread)
    // -------------------------------------------------------------------

    #[test]
    fn shared_blocks_exclusive() {
        // T1 holds S, T2 requests X → blocks until T1 releases.
        let lm = lm();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"key", LockMode::Shared).unwrap();

        let lm2 = lm.clone();
        let handle = thread::spawn(move || lm2.acquire(t2, b"key", LockMode::Exclusive));

        thread::sleep(SETTLE_TIME);
        // T2 should be waiting — verify it doesn't hold the lock yet.
        assert!(lm.held_by(t2).is_empty());

        lm.release(t1, b"key");
        handle.join().unwrap().unwrap();

        // T2 now holds X.
        assert_eq!(lm.held_by(t2), vec![b"key".to_vec()]);
    }

    #[test]
    fn exclusive_blocks_shared() {
        // T1 holds X, T2 requests S → blocks until T1 releases.
        let lm = lm();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"key", LockMode::Exclusive).unwrap();

        let lm2 = lm.clone();
        let handle = thread::spawn(move || lm2.acquire(t2, b"key", LockMode::Shared));

        thread::sleep(SETTLE_TIME);
        assert!(lm.held_by(t2).is_empty());

        lm.release(t1, b"key");
        handle.join().unwrap().unwrap();

        assert_eq!(lm.held_by(t2), vec![b"key".to_vec()]);
    }

    #[test]
    fn exclusive_blocks_exclusive() {
        // T1 holds X, T2 requests X → blocks until T1 releases.
        let lm = lm();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"key", LockMode::Exclusive).unwrap();

        let lm2 = lm.clone();
        let handle = thread::spawn(move || lm2.acquire(t2, b"key", LockMode::Exclusive));

        thread::sleep(SETTLE_TIME);
        assert!(lm.held_by(t2).is_empty());

        lm.release(t1, b"key");
        handle.join().unwrap().unwrap();

        assert_eq!(lm.held_by(t2), vec![b"key".to_vec()]);
    }

    #[test]
    fn lock_upgrade_multiple_holders() {
        // T1 and T2 hold S. T1 upgrades to X → waits for T2 to release S.
        let lm = lm();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"key", LockMode::Shared).unwrap();
        lm.acquire(t2, b"key", LockMode::Shared).unwrap();

        let lm2 = lm.clone();
        let handle = thread::spawn(move || {
            // T1 upgrade: must wait for T2 to release.
            lm2.acquire(t1, b"key", LockMode::Exclusive)
        });

        thread::sleep(SETTLE_TIME);

        // T2 releases S → T1 upgrade should complete.
        lm.release(t2, b"key");
        handle.join().unwrap().unwrap();

        // T1 now holds X exclusively.
        assert_eq!(lm.held_by(t1), vec![b"key".to_vec()]);
        assert!(lm.held_by(t2).is_empty());
    }

    #[test]
    fn release_wakes_waiter() {
        // T1 holds X, T2 waits for S. Release T1 → T2 gets S.
        let lm = lm();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"key", LockMode::Exclusive).unwrap();

        let lm2 = lm.clone();
        let handle = thread::spawn(move || lm2.acquire(t2, b"key", LockMode::Shared));

        thread::sleep(SETTLE_TIME);
        lm.release(t1, b"key");
        handle.join().unwrap().unwrap();

        assert_eq!(lm.held_by(t2), vec![b"key".to_vec()]);
        assert!(lm.held_by(t1).is_empty());
    }

    #[test]
    fn release_wakes_multiple_shared() {
        // T1 holds X; T2, T3, T4 all wait for S.
        // Release X → all three get S simultaneously.
        let lm = lm();
        let t1 = TxnId::new(1);

        lm.acquire(t1, b"key", LockMode::Exclusive).unwrap();

        let mut handles = vec![];
        for i in 2..=4 {
            let lm2 = lm.clone();
            let tid = TxnId::new(i);
            handles.push(thread::spawn(move || {
                lm2.acquire(tid, b"key", LockMode::Shared)
            }));
        }

        thread::sleep(SETTLE_TIME);
        lm.release(t1, b"key");

        for h in handles {
            h.join().unwrap().unwrap();
        }

        // All three hold S.
        for i in 2..=4 {
            assert_eq!(lm.held_by(TxnId::new(i)), vec![b"key".to_vec()]);
        }
    }

    #[test]
    fn lock_timeout() {
        // T1 holds X indefinitely. T2 times out waiting for X.
        let lm = Arc::new(LockManager::with_timeout(Duration::from_millis(100)));
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"key", LockMode::Exclusive).unwrap();

        let lm2 = lm.clone();
        let handle = thread::spawn(move || lm2.acquire(t2, b"key", LockMode::Exclusive));

        let result = handle.join().unwrap();
        assert!(matches!(result, Err(Error::LockTimeout)));

        // T1 still holds the lock.
        assert_eq!(lm.held_by(t1), vec![b"key".to_vec()]);
        // T2's request was removed from the wait queue.
        assert!(lm.held_by(t2).is_empty());
    }

    #[test]
    fn fairness_x_not_starved_by_late_s() {
        // T1 holds S. T2 waits for X. T3 requests S.
        // T3 should NOT jump ahead of T2 (FIFO fairness).
        let lm = lm();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);
        let t3 = TxnId::new(3);

        lm.acquire(t1, b"key", LockMode::Shared).unwrap();

        // T2 waits for X.
        let lm2 = lm.clone();
        let h2 = thread::spawn(move || lm2.acquire(t2, b"key", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        // T3 requests S — should NOT be granted because T2(X) is ahead in queue.
        let lm3 = lm.clone();
        let h3 = thread::spawn(move || lm3.acquire(t3, b"key", LockMode::Shared));
        thread::sleep(SETTLE_TIME);

        // Neither T2 nor T3 should hold the lock yet.
        assert!(lm.held_by(t2).is_empty());
        assert!(lm.held_by(t3).is_empty());

        // Release T1 → T2 gets X (front of queue). T3 still waits.
        lm.release(t1, b"key");
        h2.join().unwrap().unwrap();
        assert_eq!(lm.held_by(t2), vec![b"key".to_vec()]);

        // Release T2 → T3 gets S.
        lm.release(t2, b"key");
        h3.join().unwrap().unwrap();
        assert_eq!(lm.held_by(t3), vec![b"key".to_vec()]);
    }

    // -------------------------------------------------------------------
    // Deadlock detection tests
    // -------------------------------------------------------------------

    fn lm_deadlock() -> Arc<LockManager> {
        Arc::new(LockManager::with_timeout(Duration::from_secs(5)))
    }

    #[test]
    fn deadlock_two_txns() {
        // Classic A-B / B-A deadlock. T1 holds A, T2 holds B.
        // T1 requests B (blocks). T2 requests A (detects cycle, gets Deadlock).
        let lm = lm_deadlock();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"A", LockMode::Exclusive).unwrap();
        lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();

        // T1 requests B — blocks because T2 holds B.
        let lm2 = lm.clone();
        let t1_handle = thread::spawn(move || lm2.acquire(t1, b"B", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        // T2 requests A — detects cycle: T2→T1(A), T1→T2(B). T2 is victim.
        let result = lm.acquire(t2, b"A", LockMode::Exclusive);
        assert!(matches!(result, Err(Error::Deadlock(_))));

        // Release T2's lock on B so T1 can proceed.
        lm.release(t2, b"B");
        let t1_result = t1_handle.join().unwrap();
        assert!(t1_result.is_ok());
    }

    #[test]
    fn deadlock_three_txns() {
        // Three-way cycle: T1→T2→T3→T1.
        // T1 holds A, T2 holds B, T3 holds C.
        // T1 requests B (blocks). T2 requests C (blocks). T3 requests A (deadlock).
        let lm = lm_deadlock();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);
        let t3 = TxnId::new(3);

        lm.acquire(t1, b"A", LockMode::Exclusive).unwrap();
        lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();
        lm.acquire(t3, b"C", LockMode::Exclusive).unwrap();

        // T1 requests B — blocks (T2 holds B).
        let lm_t1 = lm.clone();
        let t1_handle = thread::spawn(move || lm_t1.acquire(t1, b"B", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        // T2 requests C — blocks (T3 holds C).
        let lm_t2 = lm.clone();
        let t2_handle = thread::spawn(move || lm_t2.acquire(t2, b"C", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        // T3 requests A — detects cycle T3→T1→T2→T3, returns Deadlock.
        let result = lm.acquire(t3, b"A", LockMode::Exclusive);
        assert!(matches!(result, Err(Error::Deadlock(_))));

        // Unblock the chain: T3 releases C → T2 gets C.
        lm.release(t3, b"C");
        let t2_result = t2_handle.join().unwrap();
        assert!(t2_result.is_ok());

        // T2 now holds B (original) and C (just acquired).
        // Release T2's B → T1 gets B.
        lm.release(t2, b"B");
        let t1_result = t1_handle.join().unwrap();
        assert!(t1_result.is_ok());
    }

    #[test]
    fn no_false_positive_chain() {
        // Linear chain T1→T2→T3 with no back edge. No deadlock.
        // T3 holds C, T2 holds B. T2 requests C (blocks). T1 requests B (blocks).
        // No cycle — both complete after releases.
        let lm = lm_deadlock();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);
        let t3 = TxnId::new(3);

        lm.acquire(t3, b"C", LockMode::Exclusive).unwrap();
        lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();

        // T2 requests C — blocks on T3.
        let lm_t2 = lm.clone();
        let t2_handle = thread::spawn(move || lm_t2.acquire(t2, b"C", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        // T1 requests B — blocks on T2. Chain: T1→T2→T3. No cycle.
        let lm_t1 = lm.clone();
        let t1_handle = thread::spawn(move || lm_t1.acquire(t1, b"B", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        // Unblock: T3 releases C → T2 gets C. T2 releases B → T1 gets B.
        lm.release(t3, b"C");
        t2_handle.join().unwrap().unwrap();

        lm.release(t2, b"B");
        t1_handle.join().unwrap().unwrap();

        // Both completed without Deadlock.
        assert!(!lm.held_by(t1).is_empty());
        assert!(!lm.held_by(t2).is_empty());
    }

    #[test]
    fn no_false_positive_diamond() {
        // T2 holds B, T3 holds C. T1 requests B — blocks on T2.
        // No cycle (T2 and T3 don't wait for T1). Release B → T1 gets it.
        let lm = lm_deadlock();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);
        let t3 = TxnId::new(3);

        lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();
        lm.acquire(t3, b"C", LockMode::Exclusive).unwrap();

        // T1 requests B — blocks on T2. Graph: T1→T2. No cycle.
        let lm_t1 = lm.clone();
        let t1_handle = thread::spawn(move || lm_t1.acquire(t1, b"B", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        // Release T2's B → T1 gets it. No deadlock.
        lm.release(t2, b"B");
        t1_handle.join().unwrap().unwrap();
    }

    #[test]
    fn victim_is_youngest() {
        // T1(id=1) holds A, T2(id=5) holds B. T1 requests B (blocks).
        // T2 requests A → deadlock. T2(id=5) is youngest and current requester.
        // Assert the Deadlock payload is 5.
        let lm = lm_deadlock();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(5);

        lm.acquire(t1, b"A", LockMode::Exclusive).unwrap();
        lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();

        let lm2 = lm.clone();
        let t1_handle = thread::spawn(move || lm2.acquire(t1, b"B", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        let result = lm.acquire(t2, b"A", LockMode::Exclusive);
        assert!(matches!(result, Err(Error::Deadlock(5))));

        lm.release(t2, b"B");
        t1_handle.join().unwrap().unwrap();
    }

    #[test]
    fn victim_receives_deadlock_error() {
        // Verify the exact error variant and payload via explicit match.
        let lm = lm_deadlock();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"A", LockMode::Exclusive).unwrap();
        lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();

        let lm2 = lm.clone();
        let _t1_handle = thread::spawn(move || lm2.acquire(t1, b"B", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        let result = lm.acquire(t2, b"A", LockMode::Exclusive);

        match result {
            Err(Error::Deadlock(victim_id)) => {
                assert_eq!(victim_id, 2, "Victim should be the current requester");
            }
            other => panic!("Expected Deadlock error, got: {:?}", other),
        }

        lm.release(t2, b"B");
        _t1_handle.join().unwrap().unwrap();
    }

    #[test]
    fn non_victim_eventually_granted() {
        // After victim gets Deadlock and releases all locks, the blocked
        // non-victim's acquire succeeds.
        let lm = lm_deadlock();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"A", LockMode::Exclusive).unwrap();
        lm.acquire(t2, b"B", LockMode::Exclusive).unwrap();

        // T1 requests B — blocks.
        let lm2 = lm.clone();
        let t1_handle = thread::spawn(move || lm2.acquire(t1, b"B", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        // T2 requests A — deadlock, T2 is victim.
        let result = lm.acquire(t2, b"A", LockMode::Exclusive);
        assert!(matches!(result, Err(Error::Deadlock(_))));

        // T2 (victim) releases all locks — simulating abort cleanup.
        lm.release_all(t2);

        // T1 should now acquire B (T2 released it).
        let t1_result = t1_handle.join().unwrap();
        assert!(t1_result.is_ok(), "Non-victim T1 should get the lock");
        assert_eq!(lm.held_by(t1), vec![b"A".to_vec(), b"B".to_vec()]);
    }

    #[test]
    fn deadlock_after_partial_release() {
        // T1 holds A+B. T1 releases A. T2 acquires A and C.
        // T1 requests C (blocks). T2 requests B → cycle → Deadlock.
        let lm = lm_deadlock();
        let t1 = TxnId::new(1);
        let t2 = TxnId::new(2);

        lm.acquire(t1, b"A", LockMode::Exclusive).unwrap();
        lm.acquire(t1, b"B", LockMode::Exclusive).unwrap();

        // T1 releases A. T2 acquires A and C.
        lm.release(t1, b"A");
        lm.acquire(t2, b"A", LockMode::Exclusive).unwrap();
        lm.acquire(t2, b"C", LockMode::Exclusive).unwrap();

        // T1 requests C — blocks (T2 holds C).
        let lm2 = lm.clone();
        let t1_handle = thread::spawn(move || lm2.acquire(t1, b"C", LockMode::Exclusive));
        thread::sleep(SETTLE_TIME);

        // T2 requests B — deadlock: T2→T1(via B), T1→T2(via C).
        let result = lm.acquire(t2, b"B", LockMode::Exclusive);
        assert!(matches!(result, Err(Error::Deadlock(_))));

        // Cleanup: release T2's C so T1 completes.
        lm.release(t2, b"C");
        t1_handle.join().unwrap().unwrap();
    }
}
