//! The pull-based operator implementations. Each file holds one operator
//! (plus its inline tests); the `Executor` trait they implement and the
//! flat re-exports live one level up in `execution`.

pub mod compute;
pub mod delete;
pub mod filter;
pub mod hash_aggregate;
pub mod index_scan;
pub mod insert;
pub mod join;
pub mod limit;
pub mod pk_lookup;
pub mod projection;
pub mod seq_scan;
pub mod sort;
pub mod update;
