//! `TxnEngine<E>` — MVCC + locking + WAL wrapper around any `StorageEngine`.
//!
//! Implements `StorageEngine` itself, so any consumer written against the
//! trait (notably `Table<E, L>` and the Phase 10 executor operators) works
//! against either the raw engine or a TxnEngine without code changes.
//!
//! Each handle binds one transaction context. `Database` constructs a fresh
//! handle per `txn_*` call (cheap: three `Arc` clones + a `TxnId`).
//!
//! Steps wired in this module:
//!   1.A  struct + constructor                   ← this commit
//!   1.B  `impl StorageEngine`                   ← next
//!   1.C  override `lock_for_write` (X-lock)
//!   1.D  thread snapshot/txn through ops
//!   2.*  relocate MVCC bodies in from `Database`

use std::sync::Arc;

use crate::common::{Error, Result};
use crate::storage::{ScanIterator, StorageEngine, StorageStatus};
use crate::txn::mvcc::{self, MvccValue};
use crate::txn::{LockMode, Snapshot, Timestamp, TransactionManager, TxnId, TxnMode};
use crate::wal::{LogRecord, Wal};

/// One transaction's view of the storage engine.
pub struct TxnEngine<E: StorageEngine> {
    engine: Arc<E>,
    txn_mgr: Arc<TransactionManager>,
    wal: Arc<Wal>,
    txn_id: TxnId,
}

impl<E: StorageEngine> TxnEngine<E> {
    pub fn new(
        engine: Arc<E>,
        txn_mgr: Arc<TransactionManager>,
        wal: Arc<Wal>,
        txn_id: TxnId,
    ) -> Self {
        Self {
            engine,
            txn_mgr,
            wal,
            txn_id,
        }
    }

    /// Direct access to the wrapped engine. Bypasses MVCC. Reserved for GC,
    /// which must process every version regardless of visibility.
    pub fn inner(&self) -> &Arc<E> {
        &self.engine
    }

    /// Snapshot for this handle's bound txn. Explicit txns reuse the
    /// snapshot captured at `begin()`. `AUTO_COMMIT` constructs a fresh
    /// snapshot from the current oracle (matches `Database::get` semantics
    /// for auto-commit reads).
    fn snapshot_for_read(&self) -> Result<Snapshot> {
        if self.txn_id == TxnId::AUTO_COMMIT {
            let oracle_ts = self.txn_mgr.ts_oracle_peek();
            let ckpt_ts = self.txn_mgr.checkpoint_ts();
            let read_ts = if ckpt_ts > oracle_ts { ckpt_ts } else { oracle_ts };
            Ok(Snapshot { read_ts })
        } else {
            self.txn_mgr.snapshot(self.txn_id)
        }
    }

    /// SI first-committer-wins check. Returns the writer of any committed
    /// version whose `commit_ts > my_begin_ts`. Caller must hold the X-lock
    /// on `user_key` so no concurrent writer can race the scan.
    fn find_conflicting_committed_version(
        &self,
        user_key: &[u8],
        my_begin_ts: Timestamp,
    ) -> Result<Option<TxnId>> {
        let committed = self.txn_mgr.committed_txns();
        let start = mvcc::encode_mvcc_key_start(user_key);
        let end = mvcc::encode_mvcc_key_end(user_key);

        for result in self.engine.scan(start..=end) {
            let (encoded_key, encoded_value) = result?;
            let (found_key, _version_ts) = mvcc::decode_mvcc_key(&encoded_key)?;
            if found_key != user_key {
                break;
            }
            let mvcc_val = mvcc::decode_mvcc_value(&encoded_value)?;
            let version_txn_id = match &mvcc_val {
                MvccValue::Value { txn_id, .. } => *txn_id,
                MvccValue::Tombstone { txn_id } => *txn_id,
            };
            if let Some(commit_ts) = committed.get(&version_txn_id) {
                if *commit_ts > my_begin_ts {
                    return Ok(Some(version_txn_id));
                }
            }
        }
        Ok(None)
    }
}

// Step 1.B: trait impl with pure pass-through. MVCC overlays in Step 2.
impl<E: StorageEngine> StorageEngine for TxnEngine<E> {
    fn name(&self) -> &'static str {
        self.engine.name()
    }

    /// Acquire an X-lock on `key` for this handle's bound transaction.
    /// Closes the get-then-put TOCTOU window in `Table::insert / update_* /
    /// delete_by_pk` — those callers already invoke `lock_for_write` before
    /// the read, so swapping a `TxnEngine` in for a raw engine makes them
    /// transactional with no other code change.
    fn lock_for_write(&self, key: &[u8]) -> Result<()> {
        self.txn_mgr
            .lock_manager()
            .acquire(self.txn_id, key, LockMode::Exclusive)
    }

    /// MVCC get: newest visible version of `key` for this handle's bound txn.
    /// Honors the SI snapshot, the known-not-committed set (aborts +
    /// recovery-loaded uncommitted), and the pre-checkpoint heuristic.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snapshot = self.snapshot_for_read()?;
        let committed = self.txn_mgr.committed_txns();
        let non_committed = self.txn_mgr.known_not_committed();
        mvcc::mvcc_get(
            &*self.engine,
            key,
            self.txn_id,
            &snapshot,
            &committed,
            self.txn_mgr.checkpoint_ts(),
            &non_committed,
        )
    }

    /// MVCC put: writes a new `Value` version under the bound txn.
    /// X-lock must be held by the caller — `Table` callers do this via the
    /// `lock_for_write` step in their get-then-put pair; `Database::txn_put`
    /// does it explicitly in its shim. No internal re-acquire (saves a
    /// hash-table hit on the hot Table path).
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.txn_mgr.mode(self.txn_id)? == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly(self.txn_id.0));
        }
        let begin_ts = self.txn_mgr.begin_ts(self.txn_id)?;
        if let Some(writer) = self.find_conflicting_committed_version(key, begin_ts)? {
            return Err(Error::WriteConflict { writer: writer.0 });
        }
        let mvcc_key = mvcc::encode_mvcc_key(key, begin_ts);
        let mvcc_val = mvcc::encode_mvcc_value(&MvccValue::Value {
            txn_id: self.txn_id,
            data: value.to_vec(),
        });
        let prev_lsn = self.txn_mgr.last_lsn(self.txn_id)?;
        let mut record = LogRecord::txn_put(
            self.txn_id.0, prev_lsn, mvcc_key.clone(), mvcc_val.clone(), None,
        );
        let lsn = self.wal.append(&mut record)?;
        self.txn_mgr.update_last_lsn(self.txn_id, lsn)?;
        self.engine.put(&mvcc_key, &mvcc_val)
    }

    /// MVCC delete: writes a `Tombstone` version under the bound txn.
    /// Same locking contract as `put` — caller holds the X-lock.
    fn delete(&self, key: &[u8]) -> Result<()> {
        if self.txn_mgr.mode(self.txn_id)? == TxnMode::ReadOnly {
            return Err(Error::TxnReadOnly(self.txn_id.0));
        }
        let begin_ts = self.txn_mgr.begin_ts(self.txn_id)?;
        if let Some(writer) = self.find_conflicting_committed_version(key, begin_ts)? {
            return Err(Error::WriteConflict { writer: writer.0 });
        }
        let mvcc_key = mvcc::encode_mvcc_key(key, begin_ts);
        let mvcc_val = mvcc::encode_mvcc_value(&MvccValue::Tombstone {
            txn_id: self.txn_id,
        });
        let prev_lsn = self.txn_mgr.last_lsn(self.txn_id)?;
        let mut record = LogRecord::txn_put(
            self.txn_id.0, prev_lsn, mvcc_key.clone(), mvcc_val.clone(), None,
        );
        let lsn = self.wal.append(&mut record)?;
        self.txn_mgr.update_last_lsn(self.txn_id, lsn)?;
        self.engine.put(&mvcc_key, &mvcc_val)
    }

    /// MVCC scan: newest visible version per user-key across the given
    /// bound pair. Buffers via `mvcc_scan`'s `Vec` return and re-emits as
    /// a streaming iterator. Streaming-native refactor is a Phase-11 perf
    /// todo — for TPC-C table sizes the buffer is fine.
    fn scan_range(
        &self,
        start_bound: std::ops::Bound<Vec<u8>>,
        end_bound: std::ops::Bound<Vec<u8>>,
    ) -> Box<dyn ScanIterator + '_> {
        let snapshot = match self.snapshot_for_read() {
            Ok(s) => s,
            Err(e) => return Box::new(std::iter::once(Err(e))),
        };
        let committed = self.txn_mgr.committed_txns();
        let non_committed = self.txn_mgr.known_not_committed();

        let start = match &start_bound {
            std::ops::Bound::Included(k) => k.clone(),
            std::ops::Bound::Excluded(k) => {
                let mut next = k.clone();
                next.push(0);
                next
            }
            std::ops::Bound::Unbounded => vec![],
        };
        let end = match &end_bound {
            std::ops::Bound::Included(k) | std::ops::Bound::Excluded(k) => k.clone(),
            std::ops::Bound::Unbounded => vec![0xFF; 32],
        };

        match mvcc::mvcc_scan(
            &*self.engine,
            &start,
            &end,
            self.txn_id,
            &snapshot,
            &committed,
            self.txn_mgr.checkpoint_ts(),
            &non_committed,
        ) {
            Ok(pairs) => Box::new(pairs.into_iter().map(Ok)),
            Err(e) => Box::new(std::iter::once(Err(e))),
        }
    }

    fn status(&self) -> StorageStatus {
        self.engine.status()
    }

    fn flush(&self) -> Result<()> {
        self.engine.flush()
    }
}
