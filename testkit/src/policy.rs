//! `EvictionPolicy` axis: the six policy constructors (the registry's leaves)
//! and [`assert_contract`] — the universal contract every policy must satisfy
//! regardless of its eviction *order* (order stays in each replacer's own
//! tests). Driven from [`crate::for_each_policy`].

use interchangedb::buffer::replacer::{
    ArcReplacer, ClockReplacer, EvictionPolicy, FifoReplacer, LruKReplacer, LruReplacer,
    TwoQReplacer,
};
use interchangedb::common::{FrameId, PageId};

/// Capacity for size-parameterized policies — generously above the handful of
/// frames the conformance suite uses, so internal-capacity edges (2Q ghost
/// size, ARC cache size) don't confound the *universal* contract. Those edges
/// are exercised in each replacer's own `mod tests`.
pub const CAP: usize = 64;

/// A named-registry maker: builds a fresh boxed policy.
pub type PolicyMaker = fn() -> Box<dyn EvictionPolicy>;

pub fn fifo() -> Box<dyn EvictionPolicy> {
    Box::new(FifoReplacer::new())
}
pub fn clock() -> Box<dyn EvictionPolicy> {
    Box::new(ClockReplacer::new())
}
pub fn lru() -> Box<dyn EvictionPolicy> {
    Box::new(LruReplacer::new(CAP))
}
pub fn lru_k() -> Box<dyn EvictionPolicy> {
    Box::new(LruKReplacer::new(2))
}
pub fn two_q() -> Box<dyn EvictionPolicy> {
    Box::new(TwoQReplacer::new(CAP))
}
pub fn arc() -> Box<dyn EvictionPolicy> {
    Box::new(ArcReplacer::new(CAP))
}

/// Runtime view of the policy registry — for differentials and benches that
/// must iterate every policy in one function.
pub fn makers() -> Vec<(&'static str, PolicyMaker)> {
    let mut v = Vec::new();
    macro_rules! push {
        ($n:ident, $m:path) => {
            v.push((stringify!($n), $m as fn() -> _));
        };
    }
    crate::for_each_policy!(push);
    v
}

/// Assert the universal `EvictionPolicy` contract. `make` builds a fresh policy
/// for each invariant so they don't interfere. The BPM's usage pattern is
/// mirrored: `record_access` precedes `set_evictable`, evictability is set
/// explicitly (pinned ⇒ `false`, unpinned ⇒ `true`).
pub fn assert_contract(name: &str, make: PolicyMaker) {
    let f = |i: u32| FrameId::new(i as usize);
    let p = PageId::new;

    // 1. A fresh policy has nothing to evict.
    {
        let mut r = make();
        assert_eq!(r.evict(), None, "{name}: evict on empty must be None");
        assert_eq!(r.size(), 0, "{name}: empty size must be 0");
    }

    // 2. The lone evictable frame is returned, then consumed.
    {
        let mut r = make();
        r.record_access(f(0), p(0));
        r.set_evictable(f(0), true);
        assert_eq!(
            r.evict(),
            Some(f(0)),
            "{name}: the lone evictable frame must be returned"
        );
        assert_eq!(r.evict(), None, "{name}: evict must consume the frame");
    }

    // 3. Pin safety — a non-evictable frame is never evicted, even as the only
    //    or the alongside-an-evictable candidate. This is the critical one.
    {
        let mut r = make();
        r.record_access(f(0), p(0));
        r.set_evictable(f(0), false);
        assert_eq!(
            r.evict(),
            None,
            "{name}: a pinned frame must never be evicted"
        );

        r.record_access(f(1), p(1));
        r.set_evictable(f(1), true);
        assert_eq!(
            r.evict(),
            Some(f(1)),
            "{name}: must evict the evictable frame, not the pinned one"
        );
        assert_eq!(
            r.evict(),
            None,
            "{name}: the pinned frame still must not be evicted"
        );
    }

    // 4. `remove` clears a frame from all tracking.
    {
        let mut r = make();
        r.record_access(f(0), p(0));
        r.set_evictable(f(0), true);
        r.remove(f(0));
        assert_eq!(r.size(), 0, "{name}: size 0 after removing the only frame");
        assert_eq!(
            r.evict(),
            None,
            "{name}: a removed frame must not be evicted"
        );
    }

    // 5. `size` reports the number of evictable frames.
    {
        let mut r = make();
        for i in 0..3 {
            r.record_access(f(i), p(i));
            r.set_evictable(f(i), true);
        }
        assert_eq!(r.size(), 3, "{name}: size must count evictable frames");
        r.set_evictable(f(1), false);
        assert_eq!(
            r.size(),
            2,
            "{name}: pinning a frame must drop the evictable count"
        );
    }

    // 6. Re-arm — pin then unpin makes a frame evictable again.
    {
        let mut r = make();
        r.record_access(f(0), p(0));
        r.set_evictable(f(0), false);
        r.set_evictable(f(0), true);
        assert_eq!(
            r.evict(),
            Some(f(0)),
            "{name}: a re-enabled frame must be evictable again"
        );
    }

    // 7. `evict_for_page` obeys pin safety and returns an evictable frame.
    {
        let mut r = make();
        r.record_access(f(0), p(0));
        r.set_evictable(f(0), false);
        r.record_access(f(1), p(1));
        r.set_evictable(f(1), true);
        assert_eq!(
            r.evict_for_page(p(99)),
            Some(f(1)),
            "{name}: evict_for_page must return the evictable frame"
        );
        assert_eq!(
            r.evict_for_page(p(99)),
            None,
            "{name}: evict_for_page must not return the pinned frame"
        );
    }

    // 8. Warm-swap shape — export tags its source and re-imports without panic.
    {
        let mut r = make();
        r.record_access(f(0), p(0));
        r.set_evictable(f(0), true);
        let state = r.export_state();
        assert_eq!(
            state.source_policy,
            r.name(),
            "{name}: export must tag its source policy"
        );
        let mut fresh = make();
        fresh.import_state(&state); // must accept a state without panicking
    }
}
