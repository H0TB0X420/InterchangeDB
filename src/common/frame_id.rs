//! Frame identifier type.

use std::fmt;

/// Identifies a frame in the buffer pool.
///
/// Using `usize` because:
/// 1. Frames are stored in `Vec<Frame>`
/// 2. Direct indexing without casting: `frames[frame_id.0]`
/// 3. Matches Rust idioms for array/vector indexing
///
/// # Example
/// ```
/// use interchangedb::FrameId;
///
/// let frame_id = FrameId::new(5);
/// // Can use directly as index: frames[frame_id.0]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameId(pub usize);

impl FrameId {
    /// Create a new FrameId.
    #[inline]
    pub fn new(id: usize) -> Self {
        FrameId(id)
    }
}

impl fmt::Display for FrameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Frame({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_id_new() {
        let fid = FrameId::new(10);
        assert_eq!(fid.0, 10);
    }

    #[test]
    fn test_frame_id_equality() {
        assert_eq!(FrameId::new(5), FrameId::new(5));
        assert_ne!(FrameId::new(5), FrameId::new(6));
    }

    #[test]
    fn test_frame_id_display() {
        assert_eq!(format!("{}", FrameId::new(42)), "Frame(42)");
    }
}