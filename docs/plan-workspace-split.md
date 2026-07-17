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

## Pass 2 (gated, not this execution)

Fix knot 3 (index-engine factory out of catalog), knots 4/5 (TableId,
IndexHandle homes), then extract `idb-sql` (sql + catalog + table +
layout + execution?) depending only on idb-core + storage *traits*. The
compile succeeding IS the thesis test. `database`/`session` stay in the
facade — they are the composition root and legitimately see everything.

## Non-goals

Renaming public API, cargo-hakari (revisit once multi-crate feature drift
actually appears), moving testkit, splitting engines from buffer (their
generics are hot; matklad's monomorphization-duplication warning applies).
