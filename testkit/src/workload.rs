//! Seeded op-stream workloads — the single workload definition that the
//! equivalence tests *assert* and the benches *measure* (and a future shuttle
//! harness could *replay*). Pure data (`Vec<Op>`) plus appliers, so none of
//! those modes knows about the others.

use interchangedb::storage::StorageEngine;
use proptest::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

/// A proptest strategy producing op streams up to `max_len` ops over
/// `key_space` distinct keys (~3:1 puts to deletes). Crossed with the config
/// registry, this drives **property-based** differentials: every config must
/// agree on every generated stream — the deterministic [`seeded`] differential
/// upgraded to explore the workload space.
pub fn op_strategy(max_len: usize, key_space: u32) -> impl Strategy<Value = Vec<Op>> {
    let key = (0..key_space).prop_map(|k| format!("k{k:04}").into_bytes());
    let op = prop_oneof![
        3 => (key.clone(), any::<u16>())
            .prop_map(|(k, v)| Op::Put(k, format!("v{v}").into_bytes())),
        1 => key.prop_map(Op::Delete),
    ];
    prop::collection::vec(op, 0..max_len)
}

/// Deterministic SplitMix64-style RNG — a pure function of the seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let mut x = self.0;
        x ^= x >> 33;
        x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        x ^= x >> 33;
        x
    }
}

/// A deterministic op stream: `len` ops over `key_space` distinct keys, ~1 in 5
/// a delete. Same seed ⇒ same stream, on every platform.
pub fn seeded(seed: u64, len: usize, key_space: u32) -> Vec<Op> {
    let mut rng = Rng::new(seed);
    let mut ops = Vec::with_capacity(len);
    for _ in 0..len {
        let key = format!("k{:04}", (rng.next_u64() as u32) % key_space).into_bytes();
        if rng.next_u64() % 5 == 0 {
            ops.push(Op::Delete(key));
        } else {
            let value = format!("v{}", rng.next_u64() % 1000).into_bytes();
            ops.push(Op::Put(key, value));
        }
    }
    ops
}

/// Apply an op stream to an engine.
pub fn apply<E: StorageEngine>(engine: &E, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Put(k, v) => engine.put(k, v).unwrap(),
            Op::Delete(k) => engine.delete(k).unwrap(),
        }
    }
}

/// The observable state of an engine: every live `(key, value)` pair, sorted.
/// The unit of cross-config comparison.
pub fn snapshot<E: StorageEngine>(engine: &E) -> Vec<(Vec<u8>, Vec<u8>)> {
    engine.scan(..).map(|r| r.unwrap()).collect()
}
