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
//!
//! ## Scoring Convention (consistent across all policies)
//! - Higher score = hotter = evict later
//! - Lower score = colder = evict first

mod arc;
mod clock;
mod fifo;
mod lru;
mod lru_k;
mod traits;
mod two_q;

pub use arc::ArcReplacer;
pub use clock::ClockReplacer;
pub use fifo::FifoReplacer;
pub use lru::LruReplacer;
pub use lru_k::LruKReplacer;
pub use traits::{EvictionPolicy, PolicyState};
pub use two_q::TwoQReplacer;