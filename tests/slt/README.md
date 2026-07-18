# sqllogictest corpus

SQL correctness tests as data. Each `.slt` file runs through
`tests/it/slt.rs` against a full `Session` on the default tier (memory
engine + WAL `NoSync`); one `#[test]` per file. Adding or editing a SQL
test here recompiles nothing.

Format: sqllogictest (`statement ok` / `statement error <regex>` /
`query <types>` + `----` + expected rows). Conventions from the harness:
`NULL` for SQL NULL, `(empty)` for empty strings, decimals with an
explicit point at their stored scale, EXPLAIN output one trimmed line per
row (pins operator trees exactly).

Provenance: converted from `sql_order_by_test.rs`, `sql_aggregate_test.rs`,
and the single-session half of `sql_e2e_test.rs` — assertion-for-assertion,
several strengthened (full-row expectations, exact EXPLAIN trees).

Deliberately still Rust:
- `sql_e2e_test.rs` (slimmed): snapshot isolation and write-conflict need
  two sessions; workload-log capture asserts on the filesystem.
- `sql_join_test.rs`: its INLJ setup needs `catalog.create_index` — the
  SQL surface has no CREATE INDEX yet. Convert when it does.

Later: the same corpus can run against every engine config (implement the
harness over `Database<E>` generically), and SQLite's cross-verified
sqllogictest corpus becomes usable at TPC-H time.
