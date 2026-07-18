//! Foundation crate: the shared vocabulary (errors, ids, values, config)
//! and the sync-primitive shim. No InterchangeDB crate sits below this
//! one — everything else depends on it, it depends on nothing internal.
pub mod common;
pub mod storage;
pub mod sync;
pub mod types;
