//! Error types for InterchangeDB.

use std::fmt;

/// Convenient Result type alias.
///
/// Instead of writing `Result<T, Error>` everywhere, we can write `Result<T>`.
/// This is a common Rust pattern (see `std::io::Result`).
pub type Result<T> = std::result::Result<T, Error>;

/// All possible errors in InterchangeDB.
///
/// This enum represents every error that can occur in the database.
/// By having a single error type, we make error handling consistent
/// across all crates.
#[derive(Debug)]
pub enum Error {
    /// I/O error from disk operations.
    ///
    /// This wraps `std::io::Error` from file read/write operations.
    Io(std::io::Error),

    /// Requested page does not exist on disk.
    PageNotFound(u32),

    /// Buffer pool has no free frames and cannot evict any pages.
    ///
    /// This happens when all frames are pinned.
    NoFreeFrames,

    /// The provided page ID is invalid (e.g., exceeds max pages).
    InvalidPageId(u32),

    /// Buffer pool is at maximum capacity.
    BufferPoolFull,

    /// Attempted to unpin a page that wasn't pinned.
    ///
    /// This indicates a bug - unpinning should match pinning.
    PageNotPinned(u32),

    // =========================================================================
    // Storage Engine Errors
    // =========================================================================

    /// Key not found in storage engine.
    ///
    /// Returned when an operation requires an existing key (e.g., update-only).
    KeyNotFound(Vec<u8>),

    /// Storage data is corrupted.
    ///
    /// Checksum mismatch, invalid format, or structural inconsistency.
    StorageCorrupted(String),

    /// Key exceeds maximum allowed length.
    KeyTooLarge(usize),

    /// Value exceeds maximum allowed length.
    ValueTooLarge(usize),

    // =========================================================================
    // Transaction Errors
    // =========================================================================

    /// Operation on a transaction that is not active (committed, aborted, or unknown).
    TxnNotActive(u64),

    /// Too many concurrent active transactions.
    TxnLimit(usize),

    /// Lock acquisition timed out waiting for a conflicting lock to release.
    LockTimeout,

    ///Deadlock detected, this transaction was chosen as victim
    Deadlock(u64),

    /// Attmepted a write on a read-only txn
    TxnReadOnly(u64),

    /// Txn ops require WAL (use Database::open)
    TxnNotSupported,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::PageNotFound(pid) => write!(f, "Page {} not found", pid),
            Error::NoFreeFrames => write!(f, "No free frames available in buffer pool"),
            Error::InvalidPageId(pid) => write!(f, "Invalid page ID: {}", pid),
            Error::BufferPoolFull => write!(f, "Buffer pool is full"),
            Error::PageNotPinned(pid) => write!(f, "Page {} is not pinned", pid),
            Error::KeyNotFound(key) => write!(f, "Key not found: {:?}", key),
            Error::StorageCorrupted(msg) => write!(f, "Storage corrupted: {}", msg),
            Error::KeyTooLarge(size) => write!(f, "Key too large: {} bytes", size),
            Error::ValueTooLarge(size) => write!(f, "Value too large: {} bytes", size),
            Error::TxnNotActive(txn_id) => write!(f, "Transaction {} is not active", txn_id),
            Error::Deadlock(txn_id) => write!(f, "Deadlock detected: transaction {} aborted", txn_id),
            Error::TxnLimit(max) => write!(f, "Too many active transactions (limit: {})", max),
            Error::LockTimeout => write!(f, "Lock acquisition timed out"),
            Error::TxnReadOnly(txn_id) => write!(f, "Transaction {} is read-only", txn_id),
            Error::TxnNotSupported => write!(f, "Transactions require WAL (use Database::open)"),
        }

    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::PageNotFound(42);
        assert_eq!(format!("{}", err), "Page 42 not found");

        let err = Error::NoFreeFrames;
        assert_eq!(
            format!("{}", err),
            "No free frames available in buffer pool"
        );
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: Error = io_err.into();

        match err {
            Error::Io(_) => {}
            _ => panic!("Expected Io error"),
        }
    }

    #[test]
    fn test_result_type_alias() {
        fn might_fail() -> Result<u32> {
            Ok(42)
        }

        assert_eq!(might_fail().unwrap(), 42);
    }
}
