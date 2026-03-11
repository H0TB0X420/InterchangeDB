//! WAL log record format — binary encoding with CRC32 integrity.
//!
//! ## Record Layout
//!
//! ```text
//! [0..4]     length:u32      — total record size including header and CRC
//! [4..5]     record_type:u8  — Put=1, Delete=2, Begin=3, Commit=4, Abort=5, Checkpoint=6
//! [5..13]    lsn:u64         — log sequence number
//! [13..21]   txn_id:u64      — transaction ID (0 = auto-commit)
//! [21..29]   prev_lsn:u64    — previous LSN for this transaction
//! [29..]     payload          — type-dependent (see LogPayload)
//! [end-4..end] crc32:u32     — CRC32 over bytes [0..end-4]
//! ```
//!
//! ## Payloads
//!
//! - **Put**: `key_len:u16 | key | val_len:u16 | value`
//! - **Delete**: `key_len:u16 | key`
//! - **Begin**: (empty)
//! - **Commit**: `commit_ts:u64`
//! - **Abort**: (empty)
//! - **Checkpoint**: `count:u32 | [txn_id:u64]...`

use crate::common::Error;
use crate::wal::Lsn;

/// Header size: length(4) + type(1) + lsn(8) + txn_id(8) + prev_lsn(8) = 29 bytes.
const HEADER_SIZE: usize = 29;

/// CRC32 trailer size.
const CRC_SIZE: usize = 4;

/// Minimum record size: header + CRC, no payload.
const MIN_RECORD_SIZE: usize = HEADER_SIZE + CRC_SIZE;

/// Maximum record size: 64MB — bounded to prevent unbounded allocation.
const MAX_RECORD_SIZE: u32 = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Record type discriminant
// ---------------------------------------------------------------------------

/// Discriminant byte for log record types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogRecordType {
    Put = 1,
    Delete = 2,
    Begin = 3,
    Commit = 4,
    Abort = 5,
    Checkpoint = 6,
}

impl LogRecordType {
    /// Convert from raw byte, returning None for invalid values.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Put),
            2 => Some(Self::Delete),
            3 => Some(Self::Begin),
            4 => Some(Self::Commit),
            5 => Some(Self::Abort),
            6 => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Payload variants
// ---------------------------------------------------------------------------

/// Type-specific payload for a log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogPayload {
    /// Put: key + value.
    Put { key: Vec<u8>, value: Vec<u8> },

    /// Delete: key only.
    Delete { key: Vec<u8> },

    /// Begin transaction (no payload).
    Begin,

    /// Commit with timestamp.
    Commit { commit_ts: u64 },

    /// Abort transaction (no payload).
    Abort,

    /// Checkpoint with list of active transaction IDs.
    Checkpoint { active_txn_ids: Vec<u64> },
}

// ---------------------------------------------------------------------------
// LogRecord
// ---------------------------------------------------------------------------

/// A single WAL log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub lsn: Lsn,
    pub txn_id: u64,
    pub prev_lsn: Lsn,
    pub payload: LogPayload,
}

impl LogRecord {
    /// Create a Put record for auto-commit (txn_id=0).
    pub fn put(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            lsn: Lsn::INVALID,
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Put { key, value },
        }
    }

    /// Create a Delete record for auto-commit (txn_id=0).
    pub fn delete(key: Vec<u8>) -> Self {
        Self {
            lsn: Lsn::INVALID,
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Delete { key },
        }
    }

    /// Create a Checkpoint record.
    pub fn checkpoint(active_txn_ids: Vec<u64>) -> Self {
        Self {
            lsn: Lsn::INVALID,
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Checkpoint { active_txn_ids },
        }
    }

    /// Get the record type discriminant.
    pub fn record_type(&self) -> LogRecordType {
        match &self.payload {
            LogPayload::Put { .. } => LogRecordType::Put,
            LogPayload::Delete { .. } => LogRecordType::Delete,
            LogPayload::Begin => LogRecordType::Begin,
            LogPayload::Commit { .. } => LogRecordType::Commit,
            LogPayload::Abort => LogRecordType::Abort,
            LogPayload::Checkpoint { .. } => LogRecordType::Checkpoint,
        }
    }

    /// Encode this record into a byte buffer.
    ///
    /// Layout: [length:u32][type:u8][lsn:u64][txn_id:u64][prev_lsn:u64][payload...][crc32:u32]
    pub fn encode(&self) -> Vec<u8> {
        let payload_size = self.payload_size();
        let total_size = HEADER_SIZE + payload_size + CRC_SIZE;
        assert!(total_size <= MAX_RECORD_SIZE as usize);

        let mut buf = Vec::with_capacity(total_size);

        // Header.
        buf.extend_from_slice(&(total_size as u32).to_le_bytes());
        buf.push(self.record_type() as u8);
        buf.extend_from_slice(&self.lsn.0.to_le_bytes());
        buf.extend_from_slice(&self.txn_id.to_le_bytes());
        buf.extend_from_slice(&self.prev_lsn.0.to_le_bytes());

        assert_eq!(buf.len(), HEADER_SIZE);

        // Payload.
        self.encode_payload(&mut buf);
        assert_eq!(buf.len(), total_size - CRC_SIZE);

        // CRC32 over everything before the CRC field.
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        assert_eq!(buf.len(), total_size);
        buf
    }

    /// Decode a record from a byte buffer.
    ///
    /// Returns the decoded record and the number of bytes consumed.
    /// The buffer must contain at least one complete record starting at offset 0.
    pub fn decode(buf: &[u8]) -> crate::common::Result<(Self, usize)> {
        // Need at least enough bytes to read the length field.
        if buf.len() < 4 {
            return Err(Error::StorageCorrupted(
                "WAL record too short for length field".into(),
            ));
        }

        let length = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

        // Validate length bounds.
        if length < MIN_RECORD_SIZE {
            return Err(Error::StorageCorrupted(format!(
                "WAL record length {} below minimum {}",
                length, MIN_RECORD_SIZE
            )));
        }
        if length > MAX_RECORD_SIZE as usize {
            return Err(Error::StorageCorrupted(format!(
                "WAL record length {} exceeds maximum {}",
                length, MAX_RECORD_SIZE
            )));
        }

        // Need the full record in the buffer.
        if buf.len() < length {
            return Err(Error::StorageCorrupted(format!(
                "WAL record truncated: need {} bytes, have {}",
                length,
                buf.len()
            )));
        }

        let record_buf = &buf[..length];

        // Verify CRC32 over bytes [0..length-4].
        let stored_crc =
            u32::from_le_bytes([
                record_buf[length - 4],
                record_buf[length - 3],
                record_buf[length - 2],
                record_buf[length - 1],
            ]);
        let computed_crc = crc32fast::hash(&record_buf[..length - 4]);
        if stored_crc != computed_crc {
            return Err(Error::StorageCorrupted(format!(
                "WAL record CRC mismatch: stored={:#010x}, computed={:#010x}",
                stored_crc, computed_crc
            )));
        }

        // Parse header.
        let record_type_byte = record_buf[4];
        let record_type = LogRecordType::from_u8(record_type_byte).ok_or_else(|| {
            Error::StorageCorrupted(format!("WAL invalid record type: {}", record_type_byte))
        })?;

        let lsn = Lsn::new(u64::from_le_bytes(
            record_buf[5..13].try_into().unwrap(),
        ));
        let txn_id = u64::from_le_bytes(
            record_buf[13..21].try_into().unwrap(),
        );
        let prev_lsn = Lsn::new(u64::from_le_bytes(
            record_buf[21..29].try_into().unwrap(),
        ));

        // Parse payload (bytes between header and CRC).
        let payload_buf = &record_buf[HEADER_SIZE..length - CRC_SIZE];
        let payload = Self::decode_payload(record_type, payload_buf)?;

        Ok((
            Self {
                lsn,
                txn_id,
                prev_lsn,
                payload,
            },
            length,
        ))
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Compute payload byte size (excluding header and CRC).
    fn payload_size(&self) -> usize {
        match &self.payload {
            LogPayload::Put { key, value } => 2 + key.len() + 2 + value.len(),
            LogPayload::Delete { key } => 2 + key.len(),
            LogPayload::Begin => 0,
            LogPayload::Commit { .. } => 8,
            LogPayload::Abort => 0,
            LogPayload::Checkpoint { active_txn_ids } => 4 + active_txn_ids.len() * 8,
        }
    }

    /// Write payload bytes into the buffer.
    fn encode_payload(&self, buf: &mut Vec<u8>) {
        match &self.payload {
            LogPayload::Put { key, value } => {
                buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(value.len() as u16).to_le_bytes());
                buf.extend_from_slice(value);
            }
            LogPayload::Delete { key } => {
                buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
                buf.extend_from_slice(key);
            }
            LogPayload::Begin => {}
            LogPayload::Commit { commit_ts } => {
                buf.extend_from_slice(&commit_ts.to_le_bytes());
            }
            LogPayload::Abort => {}
            LogPayload::Checkpoint { active_txn_ids } => {
                buf.extend_from_slice(&(active_txn_ids.len() as u32).to_le_bytes());
                for txn_id in active_txn_ids {
                    buf.extend_from_slice(&txn_id.to_le_bytes());
                }
            }
        }
    }

    /// Decode a payload from the given buffer according to record type.
    fn decode_payload(
        record_type: LogRecordType,
        buf: &[u8],
    ) -> crate::common::Result<LogPayload> {
        match record_type {
            LogRecordType::Put => {
                if buf.len() < 4 {
                    return Err(Error::StorageCorrupted(
                        "WAL Put payload too short".into(),
                    ));
                }
                let key_len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
                if buf.len() < 2 + key_len + 2 {
                    return Err(Error::StorageCorrupted(
                        "WAL Put payload truncated at key".into(),
                    ));
                }
                let key = buf[2..2 + key_len].to_vec();
                let val_offset = 2 + key_len;
                let val_len =
                    u16::from_le_bytes([buf[val_offset], buf[val_offset + 1]]) as usize;
                if buf.len() < val_offset + 2 + val_len {
                    return Err(Error::StorageCorrupted(
                        "WAL Put payload truncated at value".into(),
                    ));
                }
                let value = buf[val_offset + 2..val_offset + 2 + val_len].to_vec();
                Ok(LogPayload::Put { key, value })
            }
            LogRecordType::Delete => {
                if buf.len() < 2 {
                    return Err(Error::StorageCorrupted(
                        "WAL Delete payload too short".into(),
                    ));
                }
                let key_len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
                if buf.len() < 2 + key_len {
                    return Err(Error::StorageCorrupted(
                        "WAL Delete payload truncated".into(),
                    ));
                }
                let key = buf[2..2 + key_len].to_vec();
                Ok(LogPayload::Delete { key })
            }
            LogRecordType::Begin => Ok(LogPayload::Begin),
            LogRecordType::Commit => {
                if buf.len() < 8 {
                    return Err(Error::StorageCorrupted(
                        "WAL Commit payload too short".into(),
                    ));
                }
                let commit_ts = u64::from_le_bytes(buf[0..8].try_into().unwrap());
                Ok(LogPayload::Commit { commit_ts })
            }
            LogRecordType::Abort => Ok(LogPayload::Abort),
            LogRecordType::Checkpoint => {
                if buf.len() < 4 {
                    return Err(Error::StorageCorrupted(
                        "WAL Checkpoint payload too short".into(),
                    ));
                }
                let count = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                if buf.len() < 4 + count * 8 {
                    return Err(Error::StorageCorrupted(
                        "WAL Checkpoint payload truncated".into(),
                    ));
                }
                let mut active_txn_ids = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = 4 + i * 8;
                    let txn_id =
                        u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
                    active_txn_ids.push(txn_id);
                }
                Ok(LogPayload::Checkpoint { active_txn_ids })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Roundtrip tests — encode then decode, verify equality.
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_put() {
        let record = LogRecord {
            lsn: Lsn::new(1),
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Put {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
            },
        };
        let encoded = record.encode();
        let (decoded, consumed) = LogRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn roundtrip_delete() {
        let record = LogRecord {
            lsn: Lsn::new(2),
            txn_id: 0,
            prev_lsn: Lsn::new(1),
            payload: LogPayload::Delete {
                key: b"goodbye".to_vec(),
            },
        };
        let encoded = record.encode();
        let (decoded, consumed) = LogRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn roundtrip_begin() {
        let record = LogRecord {
            lsn: Lsn::new(10),
            txn_id: 42,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Begin,
        };
        let encoded = record.encode();
        let (decoded, consumed) = LogRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn roundtrip_commit() {
        let record = LogRecord {
            lsn: Lsn::new(11),
            txn_id: 42,
            prev_lsn: Lsn::new(10),
            payload: LogPayload::Commit { commit_ts: 1234567890 },
        };
        let encoded = record.encode();
        let (decoded, consumed) = LogRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn roundtrip_abort() {
        let record = LogRecord {
            lsn: Lsn::new(12),
            txn_id: 99,
            prev_lsn: Lsn::new(11),
            payload: LogPayload::Abort,
        };
        let encoded = record.encode();
        let (decoded, consumed) = LogRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn roundtrip_checkpoint() {
        let record = LogRecord {
            lsn: Lsn::new(100),
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Checkpoint {
                active_txn_ids: vec![1, 2, 3, 42],
            },
        };
        let encoded = record.encode();
        let (decoded, consumed) = LogRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn roundtrip_checkpoint_empty() {
        let record = LogRecord::checkpoint(vec![]);
        let mut record = LogRecord {
            lsn: Lsn::new(50),
            ..record
        };
        record.lsn = Lsn::new(50);
        let encoded = record.encode();
        let (decoded, _) = LogRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, record);
    }

    // -----------------------------------------------------------------------
    // Edge cases: zero-length key/value, large key/value.
    // -----------------------------------------------------------------------

    #[test]
    fn zero_length_key_and_value() {
        let record = LogRecord {
            lsn: Lsn::new(1),
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Put {
                key: vec![],
                value: vec![],
            },
        };
        let encoded = record.encode();
        let (decoded, _) = LogRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn large_key_and_value() {
        // u16::MAX bytes for key and value — max representable with u16 length prefix.
        let key = vec![0xAB; u16::MAX as usize];
        let value = vec![0xCD; u16::MAX as usize];
        let record = LogRecord {
            lsn: Lsn::new(999),
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Put { key, value },
        };
        let encoded = record.encode();
        let (decoded, consumed) = LogRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, record);
    }

    // -----------------------------------------------------------------------
    // Error cases.
    // -----------------------------------------------------------------------

    #[test]
    fn bad_crc() {
        let record = LogRecord::put(b"key".to_vec(), b"val".to_vec());
        let mut record = LogRecord {
            lsn: Lsn::new(1),
            ..record
        };
        record.lsn = Lsn::new(1);
        let mut encoded = record.encode();

        // Corrupt the last byte (part of CRC).
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;

        let result = LogRecord::decode(&encoded);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("CRC mismatch"));
    }

    #[test]
    fn truncated_too_short_for_length() {
        let result = LogRecord::decode(&[0x00, 0x01]);
        assert!(result.is_err());
    }

    #[test]
    fn truncated_at_header() {
        let record = LogRecord {
            lsn: Lsn::new(1),
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Put {
                key: b"key".to_vec(),
                value: b"val".to_vec(),
            },
        };
        let encoded = record.encode();

        // Truncate in the middle of the record.
        let truncated = &encoded[..encoded.len() / 2];
        let result = LogRecord::decode(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_record_type() {
        let record = LogRecord {
            lsn: Lsn::new(1),
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Begin,
        };
        let mut encoded = record.encode();

        // Corrupt the record type byte and recompute CRC.
        encoded[4] = 255;
        let crc = crc32fast::hash(&encoded[..encoded.len() - 4]);
        let len = encoded.len();
        encoded[len - 4..].copy_from_slice(&crc.to_le_bytes());

        let result = LogRecord::decode(&encoded);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("invalid record type"));
    }

    #[test]
    fn length_below_minimum() {
        // Craft a buffer with length=4 (below MIN_RECORD_SIZE of 33).
        let buf = 4u32.to_le_bytes();
        let result = LogRecord::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn from_u8_valid() {
        assert_eq!(LogRecordType::from_u8(1), Some(LogRecordType::Put));
        assert_eq!(LogRecordType::from_u8(2), Some(LogRecordType::Delete));
        assert_eq!(LogRecordType::from_u8(3), Some(LogRecordType::Begin));
        assert_eq!(LogRecordType::from_u8(4), Some(LogRecordType::Commit));
        assert_eq!(LogRecordType::from_u8(5), Some(LogRecordType::Abort));
        assert_eq!(LogRecordType::from_u8(6), Some(LogRecordType::Checkpoint));
    }

    #[test]
    fn from_u8_invalid() {
        assert_eq!(LogRecordType::from_u8(0), None);
        assert_eq!(LogRecordType::from_u8(7), None);
        assert_eq!(LogRecordType::from_u8(255), None);
    }

    #[test]
    fn record_type_discriminant() {
        assert_eq!(LogRecord::put(vec![], vec![]).record_type(), LogRecordType::Put);
        assert_eq!(LogRecord::delete(vec![]).record_type(), LogRecordType::Delete);
        assert_eq!(LogRecord::checkpoint(vec![]).record_type(), LogRecordType::Checkpoint);
    }

    #[test]
    fn minimum_record_size() {
        // Begin has no payload — should be exactly MIN_RECORD_SIZE bytes.
        let record = LogRecord {
            lsn: Lsn::new(0),
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Begin,
        };
        let encoded = record.encode();
        assert_eq!(encoded.len(), MIN_RECORD_SIZE);
    }

    #[test]
    fn multiple_records_in_buffer() {
        // Encode two records back-to-back, decode sequentially.
        let r1 = LogRecord {
            lsn: Lsn::new(1),
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
        };
        let r2 = LogRecord {
            lsn: Lsn::new(2),
            txn_id: 0,
            prev_lsn: Lsn::INVALID,
            payload: LogPayload::Delete {
                key: b"b".to_vec(),
            },
        };

        let mut buf = r1.encode();
        buf.extend_from_slice(&r2.encode());

        let (decoded1, consumed1) = LogRecord::decode(&buf).unwrap();
        assert_eq!(decoded1, r1);

        let (decoded2, consumed2) = LogRecord::decode(&buf[consumed1..]).unwrap();
        assert_eq!(decoded2, r2);
        assert_eq!(consumed1 + consumed2, buf.len());
    }
}
