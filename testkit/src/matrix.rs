//! The config registry — the single place each swappable axis's configurations
//! are listed. Every consumer (per-config `#[test]` generators, runtime `Vec`
//! views, criterion bench groups) derives from these x-macros, so adding a
//! configuration is **one line here** and it flows into every test and bench.
//!
//! Each macro takes the *name* of a callback macro and invokes it once per
//! configuration: `$cb!(short_name, ctor_path)`. The callback decides what each
//! entry expands into — a `#[test]`, a `v.push(...)`, a bench function, etc.

/// Invoke `$cb!(name, ctor)` once per eviction policy, where `ctor` is a
/// `fn() -> Box<dyn EvictionPolicy>` in [`crate::policy`].
///
/// Add a 7th policy by adding one line here; the conformance suite, the
/// correctness-neutrality differential, and any policy bench pick it up.
#[macro_export]
macro_rules! for_each_policy {
    ($cb:ident) => {
        $cb!(fifo, $crate::policy::fifo);
        $cb!(clock, $crate::policy::clock);
        $cb!(lru, $crate::policy::lru);
        $cb!(lru_k, $crate::policy::lru_k);
        $cb!(two_q, $crate::policy::two_q);
        $cb!(arc, $crate::policy::arc);
    };
}

/// Invoke `$cb!(name, ctor)` once per disk-manager backend, where `ctor`
/// returns a `Built<ConcreteDiskManager>` from [`crate::disk`]. The `fault`
/// entry is the fault injector with no faults armed (a pass-through).
#[macro_export]
macro_rules! for_each_disk {
    ($cb:ident) => {
        $cb!(memory, $crate::disk::memory);
        $cb!(file, $crate::disk::file);
        $cb!(fault_passthrough, $crate::disk::fault);
    };
}

/// Invoke `$cb!(name, ctor)` once per isolation level, where `ctor` returns an
/// `Arc<dyn IsolationPolicy>` from [`crate::isolation`]. Unlike the other axes
/// this is a *conformance* registry (each level meets its anomaly spectrum),
/// not an equivalence one — levels differ by design.
#[macro_export]
macro_rules! for_each_isolation {
    ($cb:ident) => {
        $cb!(si, $crate::isolation::si);
        $cb!(read_committed, $crate::isolation::read_committed);
    };
}

/// Invoke `$cb!(name, ConcreteType, ctor)` once per storage engine, where
/// `ctor` returns a `Built<ConcreteType>` from [`crate::engine`]. The type
/// token lets compile-time consumers (e.g. the durability sweep, which needs
/// `Database<E>`) instantiate generically; the contract suite ignores it and
/// uses the value.
///
/// Add the fractal-tree engine by adding one line here; the engine contract,
/// the cross-engine equivalence differential, and the durability sweep pick
/// it up.
#[macro_export]
macro_rules! for_each_engine {
    ($cb:ident) => {
        $cb!(
            btree,
            interchangedb::index::btree::BTreeEngine,
            $crate::engine::btree
        );
        $cb!(
            lsm,
            interchangedb::index::lsm::LsmEngine,
            $crate::engine::lsm
        );
    };
}
