//! Smoke test for the `tests/it/common/` scaffolding. The harness root
//! (`main.rs`) declares `mod common;`, which compiles it and runs its
//! module-level unit tests; consumers import it via `crate::common::…`.

#[test]
fn scaffolding_compiles_and_exports_pretty_plan() {
    use interchangedb::sql::PhysicalPlan;
    let s = crate::common::golden_plan::pretty_plan(&PhysicalPlan::BeginTxn);
    assert_eq!(s, "BeginTxn\n");
}
