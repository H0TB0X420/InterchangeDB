//! Eviction policy implementations (replacers).
//!
//! Currently implements:
//! - [`FifoReplacer`] - Simple FIFO for initial testing
//!
//! Future implementations (Week 3-4):
//! - LRU (Least Recently Used)
//! - CLOCK (Second Chance)
//! - LRU-K (K-distance based)
//! - 2Q (Two-Queue)

mod fifo;

pub use fifo::FifoReplacer;