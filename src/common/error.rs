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

    /// Snapshot-isolation write-write conflict: another transaction committed
    /// a version of the same key after this transaction's begin_ts. The
    /// committing writer's id is reported so callers can log or trace the
    /// conflict source. Callers should abort and retry with a fresh snapshot.
    WriteConflict { writer: u64 },

    // =========================================================================
    // Type System Errors (Phase 9)
    // =========================================================================
    /// Decimal arithmetic failure — overflow, scale mismatch, division by zero.
    /// Message describes which rule was violated.
    DecimalArithmetic(String),
    /// Row not found at the given primary key during an operation that
    /// required it (UPDATE, DELETE-with-strict-semantics). Idempotent
    /// DELETE doesn't raise this. The `table` field is a layout-level
    /// descriptor (e.g., "id=42"); higher layers may rewrap with a
    /// human-readable table name.
    RowNotFound { table: String },

    /// System catalog drift: stored schema for a system table doesn't match
    /// the hardcoded bootstrap schema, or a system table row is missing.
    /// Indicates manual tampering, version skew, or corruption — anything
    /// load is unwilling to silently accept.
    CatalogDrift(String),

    /// A constraint check rejected a value or row (NOT NULL, type mismatch,
    /// length bound, decimal scale, etc.). The `rule` field carries the
    /// specific violation; `column` identifies which column or `<row>` for
    /// row-level violations like arity mismatch.
    ConstraintViolation {
        column: String,
        rule: ConstraintRule,
    },

    /// Insert with a primary key already present. Layered above the layout's
    /// upsert semantics; raised by `Table::insert` only.
    DuplicateKey { table: String },

    /// `create_table` with a name already in the catalog.
    TableAlreadyExists { name: String },

    /// `get_table` / `drop_table` with an unknown name.
    TableNotFound { name: String },

    /// `create_index` with a name already in the catalog.
    IndexAlreadyExists { name: String },

    /// Index name lookup failed.
    IndexNotFound { name: String },

    /// `IndexScan` received a key prefix with more components than the
    /// index has indexed columns. `expected` is the index's column count;
    /// `actual` is what the caller passed.
    IndexLookupArity {
        index: String,
        expected: usize,
        actual: usize,
    },

    // =========================================================================
    // SQL Errors (Phase 11)
    // =========================================================================
    /// SQL parse failure. Message carries sqlparser's diagnostic verbatim.
    SqlParse(String),
}

/// Specific constraint rule violated, used inside `Error::ConstraintViolation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintRule {
    /// Column declared NOT NULL received NULL with no default available.
    NotNull,
    /// Value type doesn't match column type. `actual = None` for `Value::Null`.
    TypeMismatch {
        expected: crate::types::ColumnType,
        actual: Option<crate::types::ColumnType>,
    },
    /// Varchar value byte-length exceeds the column's max.
    VarcharTooLong { max: u16 },
    /// Char value byte-length exceeds the column's max.
    CharTooLong { max: u16 },
    /// Bytes value byte-length exceeds the column's max.
    BytesTooLong { max: u16 },
    /// Decimal value's scale doesn't match the column's declared scale.
    DecimalScaleMismatch { expected: u8, actual: u8 },
    /// Decimal value's mantissa exceeds the column's declared precision.
    DecimalPrecisionExceeded { max: u8 },
    /// PK column received NULL.
    PkNotNull,
    /// Number of values doesn't match number of columns in the schema.
    Arity { expected: usize, actual: usize },
}

impl fmt::Display for ConstraintRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConstraintRule::NotNull => write!(f, "NOT NULL violated"),
            ConstraintRule::TypeMismatch { expected, actual } => write!(
                f,
                "type mismatch: expected {:?}, got {:?}",
                expected, actual
            ),
            ConstraintRule::VarcharTooLong { max } => {
                write!(f, "value exceeds Varchar({}) length", max)
            }
            ConstraintRule::CharTooLong { max } => write!(f, "value exceeds Char({}) length", max),
            ConstraintRule::BytesTooLong { max } => {
                write!(f, "value exceeds Bytes({}) length", max)
            }
            ConstraintRule::DecimalScaleMismatch { expected, actual } => write!(
                f,
                "decimal scale {} doesn't match column scale {}",
                actual, expected
            ),
            ConstraintRule::DecimalPrecisionExceeded { max } => {
                write!(f, "decimal value exceeds precision {}", max)
            }
            ConstraintRule::PkNotNull => write!(f, "PK column cannot be NULL"),
            ConstraintRule::Arity { expected, actual } => write!(
                f,
                "arity mismatch: schema has {} columns, got {}",
                expected, actual
            ),
        }
    }
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
            Error::Deadlock(txn_id) => {
                write!(f, "Deadlock detected: transaction {} aborted", txn_id)
            }
            Error::TxnLimit(max) => write!(f, "Too many active transactions (limit: {})", max),
            Error::LockTimeout => write!(f, "Lock acquisition timed out"),
            Error::TxnReadOnly(txn_id) => write!(f, "Transaction {} is read-only", txn_id),
            Error::TxnNotSupported => write!(f, "Transactions require WAL (use Database::open)"),
            Error::WriteConflict { writer } => {
                write!(f, "write-write conflict: transaction {} committed a newer version", writer)
            }
            Error::DecimalArithmetic(msg) => write!(f, "Decimal arithmetic error: {}", msg),
            Error::RowNotFound { table } => write!(f, "row not found in {}", table),
            Error::CatalogDrift(msg) => write!(f, "catalog drift: {}", msg),
            Error::ConstraintViolation { column, rule } => {
                write!(f, "constraint violation on column '{}': {}", column, rule)
            }
            Error::DuplicateKey { table } => write!(f, "duplicate key in table '{}'", table),
            Error::TableAlreadyExists { name } => write!(f, "table '{}' already exists", name),
            Error::TableNotFound { name } => write!(f, "table '{}' not found", name),
            Error::IndexAlreadyExists { name } => write!(f, "index '{}' already exists", name),
            Error::IndexNotFound { name } => write!(f, "index '{}' not found", name),
            Error::IndexLookupArity { index, expected, actual } => write!(
                f,
                "index '{}' lookup arity mismatch: index has {} indexed columns, got {} prefix components",
                index, expected, actual
            ),
            Error::SqlParse(msg) => write!(f, "SQL parse error: {}", msg),
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
