//! Q-33 / stability.md pillar G: swapping a configuration must not change
//! observable results — the interchange thesis, asserted. The same seeded
//! workload runs across every configuration of an axis and the final states
//! must be identical. (Performance is the benches' job; correctness is here.)

use interchangedb::buffer::{BufferPoolManager, SwapMode};
use interchangedb::engines::btree::BTreeEngine;
use interchangedb::storage::{FileDiskManager, MemoryDiskManager};

use testkit::equivalence::assert_all_equal;
use testkit::workload::{self, snapshot};

const SEED: u64 = 0x00C0_FFEE;
const LEN: usize = 4000;
const KEYS: u32 = 1000;
/// Small pool relative to the ~1000-key tree, so eviction is real and the
/// policy / disk-manager actually gets exercised.
const POOL: usize = 4;

type State = Vec<(Vec<u8>, Vec<u8>)>;

/// Build a B-tree over the given disk manager + eviction policy, run the
/// workload under eviction pressure, and return its observable state.
fn run_btree<D: interchangedb::storage::DiskManager + 'static>(
    dm: D,
    policy: Box<dyn interchangedb::buffer::replacer::EvictionPolicy>,
    ops: &[workload::Op],
) -> State {
    let bpm = BufferPoolManager::new(POOL, dm);
    bpm.swap_policy(policy, SwapMode::Cold);
    let engine = BTreeEngine::new(bpm).unwrap();
    workload::apply(&engine, ops);
    snapshot(&engine)
}

#[test]
fn eviction_policy_is_correctness_neutral() {
    let ops = workload::seeded(SEED, LEN, KEYS);
    let states: Vec<(&str, State)> = testkit::policy::makers()
        .into_iter()
        .map(|(name, make)| (name, run_btree(MemoryDiskManager::new(), make(), &ops)))
        .collect();
    assert_all_equal(&states);
}

#[test]
fn disk_manager_is_correctness_neutral() {
    // A smaller stream than the memory-only axes: `FileDiskManager` fsyncs
    // every write, and small-pool eviction turns each op into disk I/O. This is
    // still enough eviction (KEYS ⇒ multiple leaves > POOL) to drive the file
    // backend's write+reload path; its correctness contract is covered fast and
    // in full by `disk_manager_conformance`.
    let ops = workload::seeded(SEED, 800, 600);
    let dir = tempfile::tempdir().unwrap();
    let file_dm = FileDiskManager::create(dir.path().join("e.db")).unwrap();

    let states: Vec<(&str, State)> = vec![
        (
            "memory",
            run_btree(MemoryDiskManager::new(), testkit::policy::fifo(), &ops),
        ),
        ("file", run_btree(file_dm, testkit::policy::fifo(), &ops)),
    ];
    assert_all_equal(&states);
}

#[test]
fn engines_are_observationally_equivalent() {
    let ops = workload::seeded(SEED, LEN, KEYS);
    // Heterogeneous engine types — collect a snapshot per engine via the
    // registry, then compare. A new engine is one line in `for_each_engine!`.
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
