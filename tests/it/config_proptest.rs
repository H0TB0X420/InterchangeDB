//! Q-33 / stability.md pillar G: **property-based** config equivalence. proptest
//! generates the op stream; the registry supplies the configs. Every config
//! must agree on every generated stream — the deterministic `config_equivalence`
//! differentials upgraded to explore the workload space.
//!
//! Relationship to `cross_engine_differential` (Q-09): that test is a *deeper*
//! per-op differential for the engine axis specifically (it compares get/scan
//! after *each* op against a `BTreeMap` oracle). This file is the *broader*
//! registry-driven version — final-state equivalence across **every** config of
//! **every** axis, so a new policy or engine is covered by one registry line.
//! The two are complementary; both kept.

use proptest::prelude::*;

use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::MemoryDiskManager;

use testkit::equivalence::assert_all_equal;
use testkit::policy::PolicyMaker;
use testkit::workload::{self, op_strategy, snapshot};

type State = Vec<(Vec<u8>, Vec<u8>)>;

/// A B-tree over a small pool (eviction on the hot path) with the given policy,
/// after applying `ops`.
fn btree_with_policy(make: PolicyMaker, ops: &[workload::Op]) -> State {
    let bpm = BufferPoolManager::new(4, MemoryDiskManager::new());
    bpm.swap_policy(make(), SwapMode::Cold);
    let engine = BTreeEngine::new(bpm).unwrap();
    workload::apply(&engine, ops);
    snapshot(&engine)
}

proptest! {
    // All six eviction policies agree on every generated op stream, under real
    // eviction pressure (pool 4, ~1000-key space). Few cases, long streams —
    // each case must fill the tree past the pool to exercise eviction.
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn all_policies_agree(ops in op_strategy(1500, 1000)) {
        let states: Vec<(&str, State)> = testkit::policy::makers()
            .into_iter()
            .map(|(name, make)| (name, btree_with_policy(make, &ops)))
            .collect();
        assert_all_equal(&states);
    }
}

proptest! {
    // Both engines agree on every generated op stream. No eviction needed — the
    // point is that two *different* engines are observationally equivalent — so
    // smaller streams and more cases.
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn all_engines_agree(ops in op_strategy(300, 200)) {
        let mut states: Vec<(&str, State)> = Vec::new();
        macro_rules! run_engine {
            ($name:ident, $ty:ty, $ctor:path) => {
                let built = $ctor();
                workload::apply(built.get(), &ops);
                states.push((stringify!($name), snapshot(built.get())));
            };
        }
        testkit::for_each_engine!(run_engine);
        assert_all_equal(&states);
    }
}
