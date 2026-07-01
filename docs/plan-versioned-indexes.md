# Versioned Secondary Indexes — Design

Status: DESIGN (no code). Closes the unversioned-index cluster found in the
2026-07-01 executor/optimizer review (`docs/review-fix-plan.md` §0):
E1/O12's remaining false-negative half plus abort and crash divergence.

---

## 1. Problem

Secondary-index engines are raw `Arc<dyn StorageEngine>` handles
(`catalog/manager.rs`), while table reads/writes go through the MVCC
`TxnEngine`. `Table::put_index_entries` / `delete_index_entries` mutate the
index **physically and immediately** — unversioned, un-WAL'd, un-undoable.
Three legs, one root cause:

| Leg | Trigger | Effect |
|---|---|---|
| **Snapshot false negative** | Reader's snapshot predates a committed indexed-column update | Index entry for the old value is physically gone → a row the snapshot says matches is unreachable via the index. Transient, needs concurrency. |
| **Abort divergence** | `BEGIN; UPDATE <indexed col>; ROLLBACK` — no concurrency needed | Old entry deleted, new entry orphaned; the committed row is unreachable by its (only) value **forever**. |
| **Crash divergence** | Any crash | Table replays from WAL; indexes are whatever their BPMs happened to flush (index engines are never WAL'd and not checkpoint-flushed — `database.rs` flushes only `self.engine`). |

The 2026-07-01 recheck Filter kills false *positives* only. False negatives
are unfixable by rechecking — you cannot recheck what the scan never found.
Pinned by `snapshot_reader_false_negative_by_old_value_is_a_known_gap`
(`tests/index_mvcc_recheck_test.rs`), which must flip when this lands.

## 2. Options considered

- **A — Versioned index entries (TxnEngine-style wrapping). CHOSEN.**
  Index writes become MVCC versions + WAL records; index scans become
  snapshot reads. Closes all three legs with one mechanism — the same one
  the table already uses. Cost: WAL format extension, recovery routing, GC
  over index engines, per-txn handle wiring.
- **B — Abort-time index undo (side-log).** Per-txn in-memory log of index
  ops, reversed on ROLLBACK. Closes only the abort leg: the undo log dies
  with the process (crash leg unfixed) and readers still see physically
  mutated entries (snapshot leg unfixed). Rejected as the primary fix;
  strictly dominated by A.
- **C — Rebuild indexes on recovery.** On `open_persistent`, scan each
  table and rebuild every index. Closes the crash leg only, at O(data)
  per crash; abort and snapshot legs remain. Rejected, but noted: it is
  the correct *repair* tool for indexes that predate this design.

## 3. Design (Option A)

### 3.1 Write path — `TxnIndexEngine`

A thin MVCC wrapper for index engines, parallel to `TxnEngine` but built
for `Arc<dyn StorageEngine>` (avoids threading generics through
`IndexHandle`) and carrying its `IndexId`:

- `put(key, [])` → encode MVCC key `(index_key, begin_ts)`, value
  `MvccValue::Value { txn_id, data: [] }`, append `TxnIndexPut` WAL record,
  write to the raw index engine.
- `delete(key)` → MVCC tombstone + `TxnIndexDelete` record.
- `scan_range` → the same `MvccScan` visibility filtering `TxnEngine` uses,
  bound to the transaction's snapshot.

`Catalog::indexes_for_table` keeps returning raw handles (DDL, tests,
rebuild tooling). The executor's `resolve_table` (`execution/build.rs`)
wraps each `IndexHandle.engine` in a `TxnIndexEngine` bound to the same
`TxnId` as the table's engine handle, so one transaction sees one snapshot
across table and indexes.

**No new locks.** An index key is `(secondary cols ++ PK)` — unique per
row — so the row's existing X-lock already serializes all writers of any
given index key. **No index-level `check_write`**: first-committer-wins
conflicts are detected on the row; running the policy again per index key
would re-detect the same conflict at best (skip it — perf, documented).

**Aborts need zero work** — versions become invisible, exactly like table
versions. This deletes the abort-divergence leg by construction.

### 3.2 WAL — two new record types

`TxnIndexPut = 9`, `TxnIndexDelete = 10`. Payload = existing
`TxnPut`/`TxnDelete` layout prefixed with `index_id: u64`. New types rather
than trailing-optional fields on existing types: decode stays unambiguous,
and old logs (which cannot contain type 9/10) replay unchanged. Old-value
capture is unnecessary (`old_value: None` always — MVCC needs no undo
images; kept only for layout symmetry).

Checkpointing: `Database::checkpoint` must `flush()` every index engine
before writing the Checkpoint record (today it flushes only the main
engine) — otherwise truncated segments could strip index redo records whose
effects were never flushed.

### 3.3 Recovery — two-stage handoff

Constraint: `Database::open` runs recovery before the `Catalog` exists, and
only the catalog knows how to instantiate index engines. Restructuring
ownership (Database builds index engines) was rejected — it inverts the
catalog's responsibility for `__sys_indexes`.

Instead: recovery's redo phase routes by record type. Main-engine records
replay as today. `TxnIndexPut/Delete` records for **committed** txns are
retained as `RecoveryStats.pending_index_redo: Vec<(IndexId, RedoOp)>`.
`Catalog::open_persistent` gains an attach step: after instantiating index
engines, it drains the pending redo into them (`Database` exposes
`take_pending_index_redo()`; the existing construction order —
`Database::open` then `Catalog::open_persistent` — makes the sequencing a
non-issue, and a `debug_assert` that the pending list is empty on first
query catches misuse).

Idempotency: identical to table redo (MVCC keys are versioned; re-putting
the same version is a no-op overwrite).

### 3.4 GC — same vacuum, more engines

`gc_collect` is already generic over `StorageEngine` and scans the whole
keyspace. Run it per index engine with the same watermark/committed view as
the main pass. The GC driver (Database) needs the index-engine list — the
same provider hook the recovery handoff introduces (catalog registers its
engines back onto the Database after attach).

### 3.5 What IndexScan/INLJ change

Nothing structural — they already read through `IndexHandle.engine`; the
handle's engine becomes the txn-bound wrapper. The recheck Filter (E1) and
INLJ recheck **stay**: they are still the guard against lossy access paths
and cost one comparison per candidate row. `IndexScan`'s "skip invisible
rows via `get_by_pk`" logic also stays (an entry's row version can be
GC-racing); it just stops being load-bearing for correctness.

## 4. Costs

- Indexed-table writes: one WAL record + one MVCC version per index entry
  mutation (~2× WAL bytes on a table with one index). Group commit already
  batches the fsync; no additional syncs.
- Index scans: MVCC decode per entry (same cost profile the table scan
  already pays). GC keeps version chains short.
- Measure with the tpcc harness before/after (Stock-Level + NewOrder are
  the index-heavy paths).

## 5. Test plan

1. **Flip the pinned gap**: `snapshot_reader_false_negative_by_old_value…`
   asserts the row IS found via the old value at the old snapshot.
2. **Abort divergence**: `BEGIN; UPDATE indexed; ROLLBACK` → index reads by
   the old value still find the row; by the new value find nothing.
3. **Crash divergence**: extend `crash_recovery_test` / DST — kill after
   commit of an indexed-column update, reopen, read via index both values.
4. **GC**: index version chains shrink under `gc_collect`; reads at live
   snapshots unaffected.
5. **Equivalence**: the `{NLJ, Hash, INLJ} × {Volcano, Push}` differential
   and the MVCC recheck suite must stay green throughout.

## 6. Phasing

- **P-A**: WAL record types + `TxnIndexEngine` + executor wiring (write
  path versioned; abort leg + snapshot leg close). Tests 1, 2, 5.
- **P-B**: recovery handoff + checkpoint index flush (crash leg closes).
  Test 3.
- **P-C**: GC over index engines. Test 4.
- **P-D**: tpcc before/after measurement; recheck-Filter cost revisit.

Open question for implementation time: whether `TxnIndexEngine` shares code
with `TxnEngine` via a common MVCC helper module (preferred if the borrow
shapes allow) or duplicates the ~80-line put/delete/scan bodies with a
`NOTE` tying them together.
