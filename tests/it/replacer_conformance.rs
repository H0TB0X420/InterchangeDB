//! Q-33 / stability.md pillar G: every `EvictionPolicy` must satisfy the
//! trait's universal contract (pin-safety, evict-consumes, remove, size,
//! warm-swap shape, …). The contract is written **once** in
//! `testkit::policy::assert_contract`; this file generates one `#[test]` per
//! policy from `testkit::for_each_policy!`, so failures are attributed to the
//! policy (`cargo test arc`) and a 7th policy is a single line in the registry.
//!
//! Eviction *order* (FIFO oldest, LRU least-recent, …) is policy-specific and
//! stays in each replacer's own `mod tests` — this suite tests only what *all*
//! policies must agree on.

macro_rules! policy_contract {
    ($name:ident, $make:path) => {
        #[test]
        fn $name() {
            testkit::policy::assert_contract(stringify!($name), $make);
        }
    };
}

testkit::for_each_policy!(policy_contract);
