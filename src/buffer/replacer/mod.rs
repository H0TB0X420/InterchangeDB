//! Eviction policy implementations (replacers).
//!
//! This module provides:
//! - [`EvictionPolicy`] trait - Interface all policies implement
//! - [`PolicyState`] - State transfer for warm swaps
//! - [`FifoReplacer`] - Simple FIFO baseline
//! - [`ClockReplacer`] - Second-chance approximation of LRU
//! - [`LruReplacer`] - Classic least recently used
//! - [`LruKReplacer`] - LRU-K with scan resistance
//! - [`TwoQReplacer`] - Two-queue with ghost buffer
//! - [`ArcReplacer`] - Adaptive replacement cache (self-tuning)
//! - [`Clock2QPlusReplacer`] - Clock2Q+ / S3-FIFO (paper reproduction — see docs/reproductions/clock2q-plus/)
//!
//! ## Scoring Convention (consistent across all policies)
//! - Higher score = hotter = evict later
//! - Lower score = colder = evict first

mod arc;
mod clock;
mod clock_two_q_plus;
mod fifo;
mod lru;
mod lru_k;
mod lru_k_crp;
mod ordered_list;
mod traits;
mod two_q;

pub use arc::ArcReplacer;
pub use clock::ClockReplacer;
pub use clock_two_q_plus::Clock2QPlusReplacer;
pub use fifo::FifoReplacer;
pub use lru::LruReplacer;
pub use lru_k::LruKReplacer;
pub use lru_k_crp::LruKCrpReplacer;
pub use traits::{EvictionPolicy, MovementStats, PolicyState};
pub use two_q::TwoQReplacer;
