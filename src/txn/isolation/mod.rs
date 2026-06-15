//! Isolation protocol (Q-34, stability.md pillar E): the concurrency-control
//! decisions that define an isolation level — read view, version visibility,
//! write-conflict, and commit validation. The version store, WAL, timestamp
//! oracle, committed-set, and lock manager are collaborators an impl is *handed*,
//! never state it owns. Swapping the impl swaps the isolation level (Snapshot
//! Isolation today; Read Committed / SSI later).
//!
//! Behind a trait so the conformance matrix (`testkit::isolation`) can test each
//! level against the Hermitage anomaly spectrum its level is required to block.

use std::collections::{HashMap, HashSet};

use crate::common::Result;
use crate::txn::{Snapshot, Timestamp, TransactionManager, TxnId};

pub mod read_committed;
pub mod si;
pub use read_committed::ReadCommitted;
pub use si::SnapshotIsolation;

/// A version's identity for a visibility or write-conflict decision.
#[derive(Debug, Clone, Copy)]
pub struct VersionRef {
    pub txn_id: TxnId,
    pub ts: Timestamp,
}

/// Pre-acquired read-side state for a single operation.
///
/// The caller acquires the `committed_txns` guard and the `known_uncommitted`
/// set **once** and passes references here, so a per-version `visible()` call in
/// a scan loop never re-locks. Borrowed only for the duration of one operation.
pub struct VisibilityView<'a> {
    /// Committed transactions → their commit timestamps.
    pub committed: &'a HashMap<TxnId, Timestamp>,
    /// Transactions known to have *not* committed (aborts + recovery-loaded
    /// unresolved). A version written by one of these is never visible.
    pub known_uncommitted: &'a HashSet<TxnId>,
    /// Versions at or before this timestamp are assumed committed when their
    /// writer is absent from `committed` (the Commit record may have been
    /// truncated by a checkpoint). A recovery concern, level-independent.
    pub checkpoint_ts: Timestamp,
}

/// The concurrency-control protocol that defines an isolation level.
///
/// `dyn`-dispatched — a runtime config, so the matrix can swap it. Impls hold no
/// mutable state of their own; all shared state arrives through the parameters.
pub trait IsolationPolicy: Send + Sync {
    /// Stable name, e.g. `"si"`, `"read-committed"`.
    fn name(&self) -> &'static str;

    /// The read view for a transaction's read. Snapshot Isolation returns the
    /// static snapshot captured at `begin`; Read Committed returns a fresh
    /// snapshot on each call.
    fn read_view(&self, txn: TxnId, mgr: &TransactionManager) -> Result<Snapshot>;

    /// Whether `version` is visible to a reader holding `snapshot` within `txn`.
    fn visible(
        &self,
        version: VersionRef,
        snapshot: &Snapshot,
        txn: TxnId,
        view: &VisibilityView<'_>,
    ) -> bool;

    /// Per-write check. `versions` are the existing versions of the key being
    /// written, pre-scanned by the caller (which owns the generic engine) so
    /// this trait stays object-safe. May reject with `WriteConflict`. SI does
    /// first-committer-wins; Read Committed relies on the lock manager and
    /// returns `Ok`.
    fn on_write(
        &self,
        txn: TxnId,
        versions: &[VersionRef],
        begin_ts: Timestamp,
        view: &VisibilityView<'_>,
    ) -> Result<()>;

    /// Validate at commit and produce the commit timestamp. SI / Read Committed
    /// simply assign from the oracle; SSI will validate first and may reject.
    fn validate_commit(&self, txn: TxnId, mgr: &TransactionManager) -> Result<Timestamp>;
}
