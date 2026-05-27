//! Shared test scaffolding (Q-02). Consumers: planner tests, EXPLAIN golden
//! tests (Q-11), and every later planner phase (Selinger / Volcano-memo /
//! Cascades) that needs to lock down plan shapes against synthetic schemas
//! and stats.
//!
//! Modules:
//! - `golden_plan` — deterministic pretty-printer + snapshot-style assertion
//!   for `PhysicalPlan` trees.
//! - `mock_catalog` — ergonomic builder around `Catalog<BTreeEngine>` with
//!   pre-populated TPC-C-shaped schemas.
//! - `mock_stats` — synthetic stats provider for cost-model tests. No
//!   consumers yet; Phase 14 (Selinger / `CostModel`) is the first.

pub mod fault_injection;
pub mod golden_plan;
pub mod mock_catalog;
pub mod mock_stats;
