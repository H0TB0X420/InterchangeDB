//! Shared test/bench support for `interchangedb` (Q-33, stability.md pillar G).
//!
//! The database is a set of swappable traits — `StorageEngine`,
//! `EvictionPolicy`, `DiskManager`, the `sync` backend. This crate lets the
//! test suite and the benchmarks exploit that: declare each axis's
//! configurations **once** here, then run the same conformance contract, the
//! same equivalence differential, and the same workload across **every**
//! configuration — asserted in `tests/`, measured head-to-head in `benches/`.
//!
//! It is a dev-dependency of the root crate, so both `tests/` and `benches/`
//! see it; the library (`src/`) never does (no cycle, and source stays clean).
//!
//! ## Layout (added phase by phase)
//! - `faults` — `FaultInjectionDiskManager` (I/O errors + torn node writes).
//! - `matrix` — the registries + x-macros (the keystone).
//! - `policy` / `disk` / `engine` — per-axis ctors + conformance harnesses.
//! - `workload` — seeded op streams. `equivalence` — the differential helper.

pub mod disk;
pub mod engine;
pub mod equivalence;
pub mod faults;
pub mod handles;
pub mod isolation;
pub mod matrix;
pub mod policy;
pub mod workload;
