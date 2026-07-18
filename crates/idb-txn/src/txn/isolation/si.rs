//! Snapshot Isolation — the current (and default) concurrency-control protocol.
//! Reads see a static snapshot taken at `begin`; writes use first-committer-wins.
//! Holds the SI-specific decisions extracted from `mvcc`/`engine`; the version
//! store, oracle, committed-set, and locks remain collaborators it consults.

use crate::common::{Error, Result};
use crate::txn::{mvcc, Snapshot, Timestamp, TransactionManager, TxnId};

use super::{IsolationPolicy, VersionRef, VisibilityView};

/// Snapshot Isolation (PostgreSQL-style): snapshot-at-begin reads,
/// first-committer-wins writes. Permits write-skew (G2-item) by design.
#[derive(Debug, Default)]
pub struct SnapshotIsolation;

impl IsolationPolicy for SnapshotIsolation {
    fn name(&self) -> &'static str {
        "si"
    }

    fn read_view(&self, txn: TxnId, mgr: &TransactionManager) -> Result<Snapshot> {
        if txn == TxnId::AUTO_COMMIT {
            // Auto-commit reads see the latest committed state: max(oracle, checkpoint).
            let oracle_ts = mgr.ts_oracle_peek();
            let ckpt_ts = mgr.checkpoint_ts();
            let read_ts = if ckpt_ts > oracle_ts {
                ckpt_ts
            } else {
                oracle_ts
            };
            Ok(Snapshot { read_ts })
        } else {
            mgr.snapshot(txn)
        }
    }

    fn visible(
        &self,
        version: VersionRef,
        snapshot: &Snapshot,
        txn: TxnId,
        view: &VisibilityView<'_>,
    ) -> bool {
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
        versions: &[VersionRef],
        begin_ts: Timestamp,
        view: &VisibilityView<'_>,
    ) -> Result<()> {
        // First-committer-wins: reject if any committed version of this key
        // committed after our snapshot began.
        for v in versions {
            if let Some(commit_ts) = view.committed.get(&v.txn_id) {
                if *commit_ts > begin_ts {
                    return Err(Error::WriteConflict { writer: v.txn_id.0 });
                }
            }
        }
        Ok(())
    }

    fn validate_commit(&self, txn: TxnId, mgr: &TransactionManager) -> Result<Timestamp> {
        mgr.assign_commit_ts(txn)
    }
}
