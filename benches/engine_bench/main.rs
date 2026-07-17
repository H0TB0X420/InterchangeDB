//! `engine_bench`: criterion micro-benches for the storage engines
//! (build-times P3). Modules keep their `criterion_group!` and every
//! benchmark id verbatim — criterion result paths are keyed by group and
//! function id, not binary name, so saved baselines and benches/RESULTS.md
//! references stay valid.

use criterion::criterion_main;

mod btree_bench;
mod lsm_bench;

criterion_main!(btree_bench::benches, lsm_bench::benches);
