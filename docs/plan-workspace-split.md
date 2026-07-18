# Plan: workspace split — crate boundaries as enforced seams

**Status:** pass 1 in execution. **Why:** a trait seam inside one crate is
a convention; a crate boundary is checked by the compiler. If `idb-sql`
compiles without the storage impls in its dependency list, the seam is
real; where it can't, the seam was fiction and we learned something. Build
times are NOT the motivation (that problem was solved cheaper —
`docs/plan-build-times.md`); this is architecture verification.

## The measured dependency graph (2026-07)

Production-only module adjacency (`#[cfg(test)]` stripped — the raw scan
is misleading: nearly all `sql/execution → buffer/engines` edges were
in-module unit tests):

```
sync      -> (none)                     types  -> common
common    -> types                      storage-> common
buffer    -> common, sync, storage      engines-> common, storage, buffer
wal       -> common, storage            txn    -> common, storage, wal
layout    -> catalog(TableId), common, storage, types
catalog   -> types, common, layout, storage, ENGINES, table, BUFFER
table     -> catalog, common, layout, storage, types
sql       -> common, catalog, types, table, storage, execution(Tuple)
execution -> common, types, catalog, storage, layout, table, sql(IR)
database  -> txn, wal, common, engines, buffer, storage
session   -> sql, catalog, txn, types, common, database, execution, layout
```

## Knots found, and their verdicts

1. **common ↔ types** (`Error::TypeMismatch` embeds `ColumnType`): both
   are vocabulary → cohabit in one foundation crate. Not worth fighting.
2. **sql ↔ execution** (`sql/ir/expr.rs` imports `Tuple`, a `Vec<Value>`
   alias defined in execution): shared data alias belongs in the
   foundation. `Tuple` moves to `idb-core::types`; the edge becomes
   execution → sql only (execution consuming the IR, the right direction).
3. **catalog → engines/buffer — THE finding**: `catalog/manager.rs`
   constructs concrete `BTreeEngine`/`LsmEngine`/`BufferPoolManager` for
   secondary indexes. The catalog is doing storage-factory work, so
   `sql → catalog → engines` holds transitively and the sql/storage seam
   is fiction at exactly this point (and nowhere else). Fix — an injected
   index-engine factory — is a real refactor: **pass 2's gate.**
4. **layout → catalog** is only `TableId`: vocabulary misplaced in
   catalog → moves to the foundation in pass 2.
5. **catalog ↔ table** (`IndexHandle` defined in table, returned by
   catalog): home assignment decided in pass 2.

## Pass 1 (this execution) — extract what is already clean

```
crates/idb-core     common + types + sync (+ Tuple)   (~3.2k LOC)
crates/idb-storage  storage + buffer + engines        (~14k LOC)
crates/idb-wal      wal                               (~2.4k LOC)
crates/idb-txn      txn                               (~3.4k LOC)
interchangedb       (root facade) layout, catalog, table, execution,
                    sql, database, session, gc, bin — re-exports keep
                    every public path identical; tests/benches unchanged
```

Mechanics that keep churn near zero:
- Each extracted crate's `lib.rs` declares its modules under the same
  names AND re-exports its dependencies' modules (`pub use idb_core::{common,
  sync, types};`), so `crate::common::…` paths inside moved code resolve
  unchanged.
- The root facade `pub use`s each crate's modules, so `interchangedb::…`
  public paths and root-internal `crate::…` references are untouched.
- The compiler enumerates the real work: `pub(crate)` items now crossing
  crate lines must become `pub` (each is a conscious API decision), and
  the `shuttle` feature forwards per-crate.
- One extraction per commit, full gates each (fmt, clippy `-D warnings`,
  test, test --release, test-list identity).

## Pass 1 executed (2026-07) — results

Commits: W1 `5c9acff` (idb-core) · W2 `72bf383` (idb-storage) · W3
`80d9f0f` (idb-wal + idb-txn). Gates green at each step; final workspace
totals 1344/0 debug, 1342/0 release across 15 suites — conserved exactly
through every move (`--workspace` is now the gate invocation, which also
picked up testkit's 5 unit tests the old single-package gate never ran).

**What the compiler enforced, concretely:** every extraction compiled with
zero production errors thanks to the dependency-module shims — the only
demanded changes were one conscious `pub(crate) → pub` escalation
(`div_i128_round_half_away`, cross-crate consumer named at the site), five
doc examples repointed from the facade to their defining crates, and
`#[cfg(test)]`-gated shims for idb-txn's engine-constructing unit tests.
Test arithmetic balanced at every step (731 = 645+86; 645 = 413+232;
413 = 304+48+61).

**Gains:** the four layers' dependency directions are now machine-checked
— `idb-storage` cannot grow a WAL/SQL dependency without a Cargo.toml
change reviewed as such; an `sql/` edit rebuilds only the root crate
(~23k LOC of storage/wal/txn no longer recompile; inner loop measured
unchanged at 7.7 s). Git recorded the whole split as ~100% renames.

**Draws:** four more Cargo.tomls with duplicated dependency versions
(workspace-level `[workspace.dependencies]` is the fix if drift appears);
the shim re-exports mean `idb_storage::common` exists as a public path
(harmless alias, but an extra way to name the same module); gates must
remember `--workspace`.

## Pass 2 (gated, not this execution)

Fix knot 3 (index-engine factory out of catalog), knots 4/5 (TableId,
IndexHandle homes), then extract `idb-sql` (sql + catalog + table +
layout + execution?) depending only on idb-core + storage *traits*. The
compile succeeding IS the thesis test. `database`/`session` stay in the
facade — they are the composition root and legitimately see everything.

## Pass 2 executed (2026-07) — the thesis holds

Commits: 2a `682c6d6` (knots) · 2b (this commit, `idb-sql`).

**Knot fixes (2a):** `StorageEngine` trait family + `TableId`/`IndexId`/
`IndexBackend` + the `IndexEngineOpener` signature moved to `idb-core`
(contract layer); the catalog's index-engine factory inverted to an
injected opener whose default lives in `idb-storage` and is wired
explicitly at every `open_persistent` call site; `IndexHandle` rehomed to
catalog (table→catalog now one-way).

**The extraction (2b):** `crates/idb-sql` = layout + catalog + table +
sql + execution. Its `[dependencies]`: **idb-core + externals only — no
idb-storage, no idb-wal, no idb-txn.** That Cargo.toml IS the thesis
proof: the SQL side compiles against contracts alone. Engine-constructing
unit tests use dev-dependencies (test-only edges, honestly separate).

**Discovered en route:** ANALYZE's implementation lived in the session;
its stats computation is planner-side machinery and moved to
`sql::optimizer::stats::analyze_table` (session now only supplies the
MVCC-scoped scan handle and wraps the result). The memo G3 gate + dense-
star tests were rewritten to run natively in `idb-sql` — they never
needed a Session, only stats and a catalog. A dev-dependency cycle back
to the facade was tried first and abandoned: Cargo permits it, but the
crate compiles twice and its types don't unify across the two builds —
recorded so nobody retries it.

Final shape: facade = `database` + `session` + `bin` + re-exports; five
crates below it; every public path unchanged through both passes.

## Non-goals

Renaming public API, cargo-hakari (revisit once multi-crate feature drift
actually appears), moving testkit, splitting engines from buffer (their
generics are hot; matklad's monomorphization-duplication warning applies).
