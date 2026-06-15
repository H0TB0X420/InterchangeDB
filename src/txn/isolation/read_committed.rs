//! Read Committed — each read sees the latest committed state via a *fresh*
//! snapshot, and writes rely on the lock manager (no first-committer-wins).
//! Relative to Snapshot Isolation this permits non-repeatable reads, read skew,
//! and lost updates — exactly the anomalies the conformance matrix asserts it
//! allows while SI blocks them.

use crate::common::Result;
use crate::txn::{mvcc, Snapshot, Timestamp, TransactionManager, TxnId};

use super::{IsolationPolicy, VersionRef, VisibilityView};

/// Read Committed: fresh-per-read view, lock-based write safety.
#[derive(Debug, Default)]
pub struct ReadCommitted;

impl IsolationPolicy for ReadCommitted {
    fn name(&self) -> &'static str {
        "read-committed"
    }

    fn read_view(&self, _txn: TxnId, mgr: &TransactionManager) -> Result<Snapshot> {
        // Fresh snapshot on every read: see everything committed so far. This is
        // the same view SI gives auto-commit reads — just applied to every txn,
        // which is what makes reads non-repeatable.
        let oracle_ts = mgr.ts_oracle_peek();
        let ckpt_ts = mgr.checkpoint_ts();
        let read_ts = if ckpt_ts > oracle_ts {
            ckpt_ts
        } else {
            oracle_ts
        };
        Ok(Snapshot { read_ts })
    }

    fn visible(
        &self,
        version: VersionRef,
        snapshot: &Snapshot,
        txn: TxnId,
        view: &VisibilityView<'_>,
    ) -> bool {
        // Same committed-and-before-read_ts predicate as SI; only the read_ts
        // (fresh per read) differs.
        mvcc::is_visible(
            version.txn_id,
            version.ts,
            txn,
            snapshot,
            view.committed,
            view.checkpoint_ts,
            view.known_uncommitted,
        )
    }

    fn on_write(
        &self,
        _txn: TxnId,
        _versions: &[VersionRef],
        _begin_ts: Timestamp,
        _view: &VisibilityView<'_>,
    ) -> Result<()> {
        // No first-committer-wins: write-write safety is the lock manager's job
        // (commit-duration X-locks acquired by `Database::txn_put`).
        Ok(())
    }

    fn validate_commit(&self, txn: TxnId, mgr: &TransactionManager) -> Result<Timestamp> {
        mgr.assign_commit_ts(txn)
    }
}
