//! Forces `tests/common/` to be compiled by some integration test binary so
//! its module-level unit tests run on `cargo test`. Real consumers (Q-11
//! EXPLAIN goldens, planner determinism, future planner phases) declare
//! `mod common;` in their own files.

mod common;

#[test]
fn scaffolding_compiles_and_exports_pretty_plan() {
    use interchangedb::sql::PhysicalPlan;
    let s = common::golden_plan::pretty_plan(&PhysicalPlan::BeginTxn);
    assert_eq!(s, "BeginTxn\n");
}
