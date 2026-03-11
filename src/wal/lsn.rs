//! Log Sequence Number type.
//!
//! An LSN uniquely identifies a position in the write-ahead log.
//! LSNs are monotonically increasing and never reused.

use std::fmt;

/// Log Sequence Number — monotonically increasing identifier for WAL records.
///
/// LSN 0 is the first valid LSN. `INVALID` (u64::MAX) is the sentinel.
/// LSNs are assigned by the WAL writer and never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Sentinel value representing "no LSN" or uninitialized state.
    pub const INVALID: Lsn = Lsn(u64::MAX);

    /// Create a new LSN.
    #[inline]
    pub fn new(value: u64) -> Self {
        Lsn(value)
    }

    /// Check if this LSN is valid (not the sentinel value).
    #[inline]
    pub fn is_valid(&self) -> bool {
        *self != Self::INVALID
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "Lsn(INVALID)")
        } else {
            write!(f, "Lsn({})", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        let lsn = Lsn::new(42);
        assert_eq!(lsn.0, 42);
        assert!(lsn.is_valid());
    }

    #[test]
    fn zero_is_valid() {
        let lsn = Lsn::new(0);
        assert!(lsn.is_valid());
    }

    #[test]
    fn invalid() {
        assert!(!Lsn::INVALID.is_valid());
        assert_eq!(Lsn::INVALID.0, u64::MAX);
    }

    #[test]
    fn ordering() {
        assert!(Lsn::new(1) < Lsn::new(2));
        assert!(Lsn::new(5) > Lsn::new(3));
        assert_eq!(Lsn::new(7), Lsn::new(7));
    }

    #[test]
    fn display() {
        assert_eq!(format!("{}", Lsn::new(42)), "Lsn(42)");
        assert_eq!(format!("{}", Lsn::INVALID), "Lsn(INVALID)");
    }

    #[test]
    fn hash_and_eq() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Lsn::new(1));
        set.insert(Lsn::new(2));
        set.insert(Lsn::new(1)); // Duplicate.
        assert_eq!(set.len(), 2);
    }
}
