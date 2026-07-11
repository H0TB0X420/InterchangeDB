//! Cost-based planning machinery — everything that ranks alternative
//! plans, layered bottom-up:
//!
//!   - [`stats`]       — `StatsProvider` seam + the owned `QueryStats`
//!     snapshot (raw row counts / NDV / histograms).
//!   - [`selectivity`] — predicate & equi-join selectivity estimation on
//!     top of the stats.
//!   - [`cost`]        — `CostModel`: per-operator `Cost` formulas and
//!     the scalarization planners rank by.
//!   - [`column_map`]  — `ColumnRemap`: textual → physical column-index
//!     rewriting, the one piece of machinery join reordering needs.
//!   - [`join_order`]  — the Selinger DP core (left-deep enumeration).
//!   - [`selinger`]    — `SelingerPlanner`: DP reorder as a
//!     LogicalPlan → LogicalPlan rewrite (Phase 14).
//!   - [`memo`]        — `VolcanoPlanner`: memo + top-down
//!     branch-and-bound over a bushy space with order properties
//!     (Phase 17).

pub mod column_map;
pub mod cost;
pub mod join_order;
pub mod memo;
pub mod selectivity;
pub mod selinger;
pub mod stats;
