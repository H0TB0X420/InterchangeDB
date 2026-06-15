//! Q-34 / stability.md pillars E + G: each isolation level meets its required
//! anomaly spectrum. This is **not** an equivalence matrix — isolation levels
//! differ by design; the contract is "block exactly what this level must block,
//! allow what it must allow." The contract is written once in
//! `testkit::isolation`; this file generates one `#[test]` per level from
//! `testkit::for_each_isolation!`, so a new level (e.g. SSI) is one registry
//! line. Teeth: SI must *block* non-repeatable-read and lost-update where Read
//! Committed *allows* them; both must block dirty-write and allow write-skew.

macro_rules! isolation_contract {
    ($name:ident, $ctor:path) => {
        #[test]
        fn $name() {
            testkit::isolation::assert_isolation_contract($ctor);
        }
    };
}

testkit::for_each_isolation!(isolation_contract);
