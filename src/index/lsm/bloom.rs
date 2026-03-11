//! Bloom filter for SSTable key lookups.
//!
//! Provides probabilistic set membership testing to avoid unnecessary disk reads.
//! Uses double-hashing with CRC32 to generate a hash family.
//!
//! Parameters: 10 bits per key, 7 hash functions (~1% false positive rate).
//!
//! Serialization format: `hash_count:u8 | bit_count:u32 | bits:[u8]`.

use crate::common::error::{Error, Result};

/// Bits per key for the bloom filter (10 bits/key ≈ 1% FPR with 7 hashes).
const BITS_PER_KEY: usize = 10;

/// Number of hash functions. Optimal for 10 bits/key: k = ln(2) * m/n ≈ 6.93.
const HASH_COUNT: usize = 7;

/// Minimum bit count to avoid degenerate tiny filters.
const MIN_BIT_COUNT: usize = 64;

/// Bloom filter backed by a bit array.
pub struct BloomFilter {
    bits: Vec<u8>,
    bit_count: usize,
    hash_count: usize,
}

impl BloomFilter {
    /// Build a bloom filter from a set of keys.
    ///
    /// An empty key list produces a minimal filter that rejects everything.
    pub fn build(keys: &[&[u8]]) -> Self {
        let bit_count = (keys.len() * BITS_PER_KEY).max(MIN_BIT_COUNT);
        // Round up to full bytes.
        let byte_count = bit_count.div_ceil(8);
        let bit_count = byte_count * 8;

        let mut bits = vec![0u8; byte_count];

        for key in keys {
            let (h1, h2) = double_hash(key);
            for i in 0..HASH_COUNT {
                let bit_pos = combined_hash(h1, h2, i) % bit_count;
                bits[bit_pos / 8] |= 1 << (bit_pos % 8);
            }
        }

        Self {
            bits,
            bit_count,
            hash_count: HASH_COUNT,
        }
    }

    /// Check if a key might be in the set.
    ///
    /// Returns `true` if the key is probably present (with ~1% FPR),
    /// or `false` if the key is definitely absent.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.bit_count == 0 {
            return false;
        }

        let (h1, h2) = double_hash(key);
        for i in 0..self.hash_count {
            let bit_pos = combined_hash(h1, h2, i) % self.bit_count;
            if self.bits[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize the bloom filter to bytes.
    ///
    /// Format: `hash_count:u8 | bit_count:u32 | bits:[u8]`.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 4 + self.bits.len());
        buf.push(self.hash_count as u8);
        buf.extend_from_slice(&(self.bit_count as u32).to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    /// Deserialize a bloom filter from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 5 {
            return Err(Error::StorageCorrupted(
                "bloom filter data too small".into(),
            ));
        }

        let hash_count = data[0] as usize;
        let bit_count = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
        let byte_count = bit_count.div_ceil(8);

        if data.len() != 5 + byte_count {
            return Err(Error::StorageCorrupted(format!(
                "bloom filter size mismatch: expected {} bytes, got {}",
                5 + byte_count,
                data.len()
            )));
        }

        let bits = data[5..].to_vec();

        Ok(Self {
            bits,
            bit_count,
            hash_count,
        })
    }
}

/// Compute two independent 32-bit hashes for double-hashing.
///
/// h1 = CRC32 of key, h2 = CRC32 of key with salt byte appended.
fn double_hash(key: &[u8]) -> (u32, u32) {
    let h1 = crc32fast::hash(key);
    // For h2, hash key with a fixed salt byte to get an independent hash.
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(key);
    hasher.update(&[0x9E]); // Salt byte.
    let h2 = hasher.finalize();
    (h1, h2)
}

/// Combined hash: h_i = h1 + i * h2 (Kirsch-Mitzenmacher).
fn combined_hash(h1: u32, h2: u32, i: usize) -> usize {
    (h1 as usize).wrapping_add(i.wrapping_mul(h2 as usize))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter() {
        let filter = BloomFilter::build(&[]);
        // Empty filter should reject everything.
        assert!(!filter.may_contain(b"anything"));
    }

    #[test]
    fn inserted_keys_match() {
        let keys: Vec<&[u8]> = vec![b"alice", b"bob", b"charlie"];
        let filter = BloomFilter::build(&keys);

        for key in &keys {
            assert!(
                filter.may_contain(key),
                "bloom filter should contain {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn false_positive_rate() {
        // Build filter with 1000 keys.
        let keys: Vec<Vec<u8>> = (0u32..1000).map(|i| i.to_le_bytes().to_vec()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs);

        // All inserted keys must be found.
        for key in &keys {
            assert!(filter.may_contain(key));
        }

        // Test 10000 non-existent keys for false positives.
        let mut false_positives = 0;
        for i in 1000u32..11000 {
            let key = i.to_le_bytes();
            if filter.may_contain(&key) {
                false_positives += 1;
            }
        }

        let fpr = false_positives as f64 / 10000.0;
        assert!(
            fpr < 0.02,
            "false positive rate too high: {fpr:.4} ({false_positives}/10000)"
        );
    }

    #[test]
    fn encode_decode_roundtrip() {
        let keys: Vec<&[u8]> = vec![b"x", b"y", b"z"];
        let filter = BloomFilter::build(&keys);

        let encoded = filter.encode();
        let decoded = BloomFilter::decode(&encoded).unwrap();

        // Decoded filter should match original behavior.
        for key in &keys {
            assert!(decoded.may_contain(key));
        }
        assert_eq!(decoded.bit_count, filter.bit_count);
        assert_eq!(decoded.hash_count, filter.hash_count);
    }

    #[test]
    fn many_keys() {
        let keys: Vec<Vec<u8>> = (0u32..10_000).map(|i| i.to_le_bytes().to_vec()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let filter = BloomFilter::build(&key_refs);

        // All inserted keys must be found.
        for key in &keys {
            assert!(filter.may_contain(key));
        }

        // Encoded size should be reasonable: ~10K keys * 10 bits = ~12.5 KB.
        let encoded = filter.encode();
        assert!(
            encoded.len() < 15_000,
            "bloom filter too large: {} bytes",
            encoded.len()
        );
    }
}
