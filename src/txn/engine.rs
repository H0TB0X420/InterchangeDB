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
use crate::txn::isolation::{VersionRef, VisibilityView};
use crate::txn::mvcc::{self, MvccValue};
use crate::txn::{LockMode, Timestamp, TransactionManager, TxnId, TxnMode};
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

    /// All existing versions of `user_key` in the store as `(txn_id, ts)`
    /// pairs. The isolation policy's `on_write` consults these for its conflict
    /// rule — the engine scan (generic over `E`) stays here, the decision lives
    /// in the policy. Caller must hold the X-lock so no writer races the scan.
    fn scan_versions(&self, user_key: &[u8]) -> Result<Vec<VersionRef>> {
        let start = mvcc::encode_mvcc_key_start(user_key);
        let end = mvcc::encode_mvcc_key_end(user_key);

        let mut versions = Vec::new();
        for result in self.engine.scan(start..=end) {
            let (encoded_key, encoded_value) = result?;
            let (found_key, version_ts) = mvcc::decode_mvcc_key(&encoded_key)?;
            if found_key != user_key {
                break;
            }
            let mvcc_val = mvcc::decode_mvcc_value(&encoded_value)?;
            let txn_id = match &mvcc_val {
                MvccValue::Value { txn_id, .. } => *txn_id,
                MvccValue::Tombstone { txn_id } => *txn_id,
            };
            versions.push(VersionRef {
                txn_id,
                ts: version_ts,
            });
        }
        Ok(versions)
    }

    /// Run the isolation policy's per-write check for `user_key`. Scans the
    /// key's versions and hands them to `on_write` along with the read-side
    /// view; the policy decides whether to reject (e.g. SI first-committer-wins).
    fn check_write(&self, user_key: &[u8], begin_ts: Timestamp) -> Result<()> {
        let versions = self.scan_versions(user_key)?;
        let committed = self.txn_mgr.committed_txns_read();
        let known = self.txn_mgr.known_not_committed();
        let view = VisibilityView {
            committed: &committed,
            known_uncommitted: &known,
            checkpoint_ts: self.txn_mgr.checkpoint_ts(),
        };
        self.txn_mgr
            .policy()
            .on_write(self.txn_id, &versions, begin_ts, &view)
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

    /// MVCC get: newest version of `key` visible under the active isolation
    /// policy for this handle's bound txn. The policy supplies the read view and
    /// the visibility predicate; the read-side state (committed set, known-
    /// uncommitted, checkpoint watermark) is acquired once and lent via the view.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let policy = self.txn_mgr.policy();
        let snapshot = policy.read_view(self.txn_id, &self.txn_mgr)?;
        let committed = self.txn_mgr.committed_txns_read();
        let known = self.txn_mgr.known_not_committed();
        let view = VisibilityView {
            committed: &committed,
            known_uncommitted: &known,
            checkpoint_ts: self.txn_mgr.checkpoint_ts(),
        };
        let visible = |txn_id: TxnId, ts: Timestamp| {
            policy.visible(VersionRef { txn_id, ts }, &snapshot, self.txn_id, &view)
        };
        mvcc::mvcc_get(&*self.engine, key, &visible)
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
        self.check_write(key, begin_ts)?;
        let mvcc_key = mvcc::encode_mvcc_key(key, begin_ts);
        let mvcc_val = mvcc::encode_mvcc_value(&MvccValue::Value {
            txn_id: self.txn_id,
            data: value.to_vec(),
        });
        let prev_lsn = self.txn_mgr.last_lsn(self.txn_id)?;
        let mut record = LogRecord::txn_put(
            self.txn_id.0,
            prev_lsn,
            mvcc_key.clone(),
            mvcc_val.clone(),
            None,
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
        self.check_write(key, begin_ts)?;
        let mvcc_key = mvcc::encode_mvcc_key(key, begin_ts);
        let mvcc_val = mvcc::encode_mvcc_value(&MvccValue::Tombstone {
            txn_id: self.txn_id,
        });
        let prev_lsn = self.txn_mgr.last_lsn(self.txn_id)?;
        let mut record = LogRecord::txn_put(
            self.txn_id.0,
            prev_lsn,
            mvcc_key.clone(),
            mvcc_val.clone(),
            None,
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
        let policy = self.txn_mgr.policy();
        let snapshot = match policy.read_view(self.txn_id, &self.txn_mgr) {
            Ok(s) => s,
            Err(e) => return Box::new(std::iter::once(Err(e))),
        };
        // Clone committed/known so the read lock isn't held during the scan.
        let committed = self.txn_mgr.committed_txns();
        let known = self.txn_mgr.known_not_committed();
        let view = VisibilityView {
            committed: &committed,
            known_uncommitted: &known,
            checkpoint_ts: self.txn_mgr.checkpoint_ts(),
        };

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

        let visible = |txn_id: TxnId, ts: Timestamp| {
            policy.visible(VersionRef { txn_id, ts }, &snapshot, self.txn_id, &view)
        };
        match mvcc::mvcc_scan(&*self.engine, &start, &end, &visible) {
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
