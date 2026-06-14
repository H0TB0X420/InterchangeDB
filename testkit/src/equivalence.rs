//! Config-invariance helper. Correctness must be *invariant* under
//! configuration — swapping a policy, disk manager, or engine changes
//! performance, never observable results. The differential **is** the test.

use std::fmt::Debug;

/// Assert every config's result equals the first, naming the diverging config
/// (and showing both values) on failure.
pub fn assert_all_equal<T: PartialEq + Debug>(named: &[(&str, T)]) {
    assert!(!named.is_empty(), "no configurations to compare");
    let (base_name, base) = &named[0];
    for (name, value) in &named[1..] {
        assert!(
            value == base,
            "config `{name}` diverged from `{base_name}`\n  `{name}`      = {value:?}\n  `{base_name}` = {base:?}"
        );
    }
}
