//! Q-33 / stability.md pillar G: every `DiskManager` backend must satisfy the
//! in-session storage contract (allocate / read-after-write / zeros / sizes /
//! overwrite / isolation). Contract written once in `testkit::disk`; one
//! `#[test]` per backend from `testkit::for_each_disk!`, including the fault
//! injector unarmed (proving the wrapper is transparent).

macro_rules! disk_contract {
    ($name:ident, $ctor:path) => {
        #[test]
        fn $name() {
            let mut built = $ctor();
            testkit::disk::assert_contract(stringify!($name), built.get_mut());
        }
    };
}

testkit::for_each_disk!(disk_contract);
