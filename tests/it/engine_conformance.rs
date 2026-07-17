//! Q-33 / stability.md pillar G: both `StorageEngine` impls must satisfy the
//! key-value contract identically (put/get/overwrite/delete/scan/range).
//! Contract written once in `testkit::engine`; one `#[test]` per engine from
//! `testkit::for_each_engine!`. A new engine (e.g. the fractal tree) inherits
//! this suite by adding one line to the registry.

macro_rules! engine_contract {
    ($name:ident, $ty:ty, $ctor:path) => {
        #[test]
        fn $name() {
            let built = $ctor();
            testkit::engine::assert_contract(stringify!($name), built.get());
        }
    };
}

testkit::for_each_engine!(engine_contract);
