//! Page identifier type.

use std::fmt;

/// Identifies a page on disk.
///
/// Using `u32` allows for 4 billion pages:
/// - 4,294,967,296 pages × 4KB = 16TB maximum database size
///
/// This matches BusTub's `page_id_t` type.
///
/// # Example
/// ```
/// use interchangedb::PageId;
///
/// let page_id = PageId::new(42);
/// assert!(page_id.is_valid());
/// assert_eq!(page_id.0, 42);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PageId(pub u32);

impl PageId {
    /// Invalid/sentinel page ID.
    ///
    /// Used to represent "no page" or uninitialized state.
    pub const INVALID: PageId = PageId(u32::MAX);

    /// Create a new PageId.
    #[inline]
    pub fn new(id: u32) -> Self {
        PageId(id)
    }

    /// Check if this page ID is valid (not the sentinel value).
    #[inline]
    pub fn is_valid(&self) -> bool {
        *self != Self::INVALID
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::INVALID {
            write!(f, "Page(INVALID)")
        } else {
            write!(f, "Page({})", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_id_new() {
        let pid = PageId::new(42);
        assert_eq!(pid.0, 42);
        assert!(pid.is_valid());
    }

    #[test]
    fn test_page_id_invalid() {
        assert!(!PageId::INVALID.is_valid());
        assert_eq!(PageId::INVALID.0, u32::MAX);
    }

    #[test]
    fn test_page_id_ordering() {
        assert!(PageId::new(1) < PageId::new(2));
        assert!(PageId::new(5) > PageId::new(3));
    }

    #[test]
    fn test_page_id_display() {
        assert_eq!(format!("{}", PageId::new(42)), "Page(42)");
        assert_eq!(format!("{}", PageId::INVALID), "Page(INVALID)");
    }
}