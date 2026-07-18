//! The `it` integration-test harness (build-times P2).
//!
//! Every module below was a standalone tests/*.rs binary; each one
//! statically linked the whole lib, so a one-line src/ edit relinked 88
//! binaries. One harness links once. Slow suites live in the sibling
//! `stress` harness. Measured rationale: docs/plan-build-times.md.

mod common;

#[cfg(feature = "shuttle")]
mod bpm_swap_shuttle;

mod acid_test;
mod analyze_table_test;
mod binder_predicate_coercion_test;
mod binder_proptest;
mod btree_concurrent_test;
mod btree_delete_test;
mod btree_engine_test;
mod btree_insert_test;
mod btree_iter_bounds_test;
mod btree_scan_test;
mod btree_tombstone_test;
mod buffer_pool_integration;
mod buffer_pool_manager_test;
mod catalog_index_engine_test;
mod catalog_table_test;
mod checkpoint_prune_test;
mod common_smoke;
mod config_equivalence;
mod config_proptest;
mod crash_recovery_test;
mod cross_engine_differential;
mod database_test;
mod deadlock_test;
mod decode_fuzz_test;
mod deterministic_test;
mod disk_manager_conformance;
mod dst_recovery_test;
mod engine_conformance;
mod explain_goldens;
mod fault_injection_test;
mod gc_test;
mod goldenscript_bpm;
mod goldenscript_btree;
mod goldenscript_lsm;
mod hash_aggregate_test;
mod hash_join_test;
mod hermitage_si_test;
mod index_engine_reopen_test;
mod index_maintenance_test;
mod index_mvcc_recheck_test;
mod index_nested_loop_join_test;
mod index_persistence_test;
mod index_scan_test;
mod isolation_conformance;
mod join_strategy_equivalence_test;
mod lock_manager_test;
mod lsm_atomic_flush_test;
mod lsm_engine_test;
mod merge_order_consumption_test;
mod mvcc_isolation_test;
mod mvcc_si_conflict_test;
mod near_limit_sizes_test;
mod nested_loop_join_test;
mod operator_tree_test;
mod planner_comparison_test;
mod planner_determinism_test;
mod planner_index_scan_test;
mod planner_three_way_test;
mod predicate_pushdown_test;
mod prepared_statement_test;
mod proptest_test;
mod push_equivalence_test;
mod replacer_conformance;
mod selinger_reorder_test;
mod session_planner_test;
mod slt;
mod sort_operator_test;
mod sql_e2e_test;
mod stats_persistence_test;
mod torn_page_recovery_test;
mod tpcc_transactions_test;
mod txn_database_test;
mod txn_engine_test;
mod txn_isolation_test;
mod wal_group_commit_test;
mod wal_test;
