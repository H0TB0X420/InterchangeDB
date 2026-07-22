//! Synchronization-primitive shim for deterministic concurrency testing.
//!
//! The buffer pool's lock and atomic types are imported through this one
//! module so a single `cfg` switch can swap them for `shuttle`'s
//! instrumented versions during model checking (Q-30 / `docs/stability.md`
//! pillar C), without touching any call site.
//!
//! ## Why a shim instead of using `shuttle` directly
//!
//! `shuttle` is a **test-only** tool: its primitives cooperate with a
//! controlled scheduler that only exists inside a `shuttle::check_*` run, and
//! they panic on real OS threads. So production must keep its real primitives;
//! a prod-vs-test switch is unavoidable.
//!
//! Production stays on `parking_lot` (a deliberate performance choice — the
//! per-page-hit replacer mutex is the documented hot-path bottleneck; see
//! `docs/scalability-investigation.md`). `parking_lot`'s `lock()/read()/
//! write()` return a guard directly, whereas `shuttle` mirrors `std::sync`
//! and returns a `Result`. The thin wrappers below absorb that one API
//! difference so every call site uses the same guard-returning API under both
//! backends. Enable the model backend with `--features shuttle`.

#[cfg(feature = "shuttle")]
pub use model::*;
#[cfg(not(feature = "shuttle"))]
pub use real::*;

/// Trace point for `shuttle` model-check debugging. Under `--features shuttle`
/// it prints with the current task id (replay is deterministic, so output
/// order is the interleaving); in production it compiles to nothing.
#[cfg(feature = "shuttle")]
#[macro_export]
macro_rules! sync_trace {
    ($($arg:tt)*) => {
        // Guard drops run during panic unwinding, and `current::me()`
        // borrows shuttle's ExecutionState — which shuttle itself holds
        // while serializing the failing schedule. Tracing then would
        // panic-in-panic and destroy the replay diagnostics.
        if !::std::thread::panicking() {
            eprintln!("[task {:?}] {}", ::shuttle::current::me(), format_args!($($arg)*))
        }
    };
}

#[cfg(not(feature = "shuttle"))]
#[macro_export]
macro_rules! sync_trace {
    ($($arg:tt)*) => {{}};
}

/// Production backend: real `parking_lot` locks and `std` atomics, re-exported
/// verbatim. Zero overhead — these are the exact types used before the shim.
#[cfg(not(feature = "shuttle"))]
mod real {
    pub use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
    pub use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
}

/// Model backend: `shuttle`'s instrumented primitives, wrapped to present the
/// `parking_lot`-style guard-returning API. `Ordering` is identical to `std`'s
/// (shuttle reuses it), so it passes through untouched.
#[cfg(feature = "shuttle")]
mod model {
    pub use shuttle::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
    pub use shuttle::sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};
    pub use std::sync::atomic::Ordering;

    /// `parking_lot`-API `Mutex` backed by `shuttle`'s. A poisoned lock is a
    /// bug in the model harness, so unwrapping (which `parking_lot` has no
    /// equivalent of) is the right failure mode under test.
    #[derive(Debug, Default)]
    pub struct Mutex<T>(shuttle::sync::Mutex<T>);

    impl<T> Mutex<T> {
        pub fn new(value: T) -> Self {
            Self(shuttle::sync::Mutex::new(value))
        }

        pub fn lock(&self) -> MutexGuard<'_, T> {
            self.0.lock().unwrap()
        }
    }

    /// `parking_lot`-API `RwLock` backed by `shuttle`'s.
    #[derive(Debug, Default)]
    pub struct RwLock<T>(shuttle::sync::RwLock<T>);

    impl<T> RwLock<T> {
        pub fn new(value: T) -> Self {
            Self(shuttle::sync::RwLock::new(value))
        }

        pub fn read(&self) -> RwLockReadGuard<'_, T> {
            self.0.read().unwrap()
        }

        pub fn write(&self) -> RwLockWriteGuard<'_, T> {
            self.0.write().unwrap()
        }
    }
}
