//! `sql_bench`: criterion benches for the SQL/txn layer (build-times P3).
//! Same id-preservation contract as `engine_bench`.

use criterion::criterion_main;

mod config_matrix;
mod push_vs_volcano;
mod txn_bench;

criterion_main!(
    config_matrix::benches,
    push_vs_volcano::benches,
    txn_bench::benches
);
