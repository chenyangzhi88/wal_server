use bytes::Bytes;

/// On-disk WAL record layout:
/// [crc32:u32][total_len:u32][stream_id:u64][term:u64][entry_id:u64][stream_lsn:u64]
/// [timestamp_ns:u64][key_len:u16][key:bytes][value:bytes]
///
/// total_len = size of everything after total_len field.
/// CRC32 covers bytes from total_len onward (inclusive)

/// Fixed overhead:
/// crc32(4) + total_len(4) + stream_id(8) + term(8) + entry_id(8) + stream_lsn(8)
/// + timestamp_ns(8) + key_len(2) = 50 bytes
pub const RECORD_HEADER_SIZE: usize = 50;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub stream_id: u64,
    pub term: u64,
    pub entry_id: u64,
    pub stream_lsn: u64,
    pub timestamp_ns: u64,
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug)]
pub enum WalRecordError {
    BufferTooSmall,
    CrcMismatch { expected: u32, actual: u32 },
    InvalidKeyLen,
}

impl std::fmt::Display for WalRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BufferTooSmall => write!(f, "buffer too small for WAL record"),
            Self::CrcMismatch { expected, actual } => {
                write!(f, "CRC mismatch: expected 0x{expected:08X}, got 0x{actual:08X}")
            }
            Self::InvalidKeyLen => write!(f, "key length exceeds record"),
        }
    }
}

impl std::error::Error for WalRecordError {}

impl WalRecord {
    /// Total serialized size on disk.
    pub fn encoded_size(&self) -> usize {
        RECORD_HEADER_SIZE + self.key.len() + self.value.len()
    }

    /// Serialize this record into `buf`. Returns bytes written.
    /// Buf must be at least `self.encoded_size()` bytes.
    pub fn encode(&self, buf: &mut [u8]) -> usize {
        let total_size = self.encoded_size();
        assert!(buf.len() >= total_size);

        // total_len = everything after the total_len field
        let body_len = (total_size - 8) as u32; // exclude crc32(4) + total_len(4)

        // Write total_len at offset 4
        buf[4..8].copy_from_slice(&body_len.to_be_bytes());
        buf[8..16].copy_from_slice(&self.stream_id.to_be_bytes());
        buf[16..24].copy_from_slice(&self.term.to_be_bytes());
        buf[24..32].copy_from_slice(&self.entry_id.to_be_bytes());
        buf[32..40].copy_from_slice(&self.stream_lsn.to_be_bytes());
        buf[40..48].copy_from_slice(&self.timestamp_ns.to_be_bytes());
        buf[48..50].copy_from_slice(&(self.key.len() as u16).to_be_bytes());
        // Write key
        buf[50..50 + self.key.len()].copy_from_slice(&self.key);
        // Write value
        let val_start = 50 + self.key.len();
        buf[val_start..val_start + self.value.len()].copy_from_slice(&self.value);

        // CRC32 over bytes [4..total_size] (from total_len onward)
        let crc = crc32fast::hash(&buf[4..total_size]);
        buf[0..4].copy_from_slice(&crc.to_be_bytes());

        total_size
    }

    /// Encode into a new Vec.
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.encoded_size()];
        self.encode(&mut buf);
        buf
    }

    /// Decode a record from buffer. Returns (record, bytes_consumed).
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), WalRecordError> {
        if buf.len() < RECORD_HEADER_SIZE {
            return Err(WalRecordError::BufferTooSmall);
        }

        let stored_crc = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let body_len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        let total_size = 8 + body_len; // crc32(4) + total_len(4) + body

        if buf.len() < total_size {
            return Err(WalRecordError::BufferTooSmall);
        }

        // Verify CRC
        let computed_crc = crc32fast::hash(&buf[4..total_size]);
        if stored_crc != computed_crc {
            return Err(WalRecordError::CrcMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        let stream_id =
            u64::from_be_bytes([buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]]);
        let term =
            u64::from_be_bytes([buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23]]);
        let entry_id = u64::from_be_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]);
        let stream_lsn = u64::from_be_bytes([
            buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
        ]);
        let timestamp_ns = u64::from_be_bytes([
            buf[40], buf[41], buf[42], buf[43], buf[44], buf[45], buf[46], buf[47],
        ]);
        let key_len = u16::from_be_bytes([buf[48], buf[49]]) as usize;

        if 50 + key_len > total_size {
            return Err(WalRecordError::InvalidKeyLen);
        }

        let key = Bytes::copy_from_slice(&buf[50..50 + key_len]);
        let value = Bytes::copy_from_slice(&buf[50 + key_len..total_size]);

        Ok((
            WalRecord {
                stream_id,
                term,
                entry_id,
                stream_lsn,
                timestamp_ns,
                key,
                value,
            },
            total_size,
        ))
    }
}

pub fn current_time_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_roundtrip() {
        let record = WalRecord {
            stream_id: 7,
            term: 2,
            entry_id: 42,
            stream_lsn: 3,
            timestamp_ns: 1234567890,
            key: Bytes::from_static(b"hello"),
            value: Bytes::from_static(b"world"),
        };

        let encoded = record.encode_to_vec();
        assert_eq!(encoded.len(), record.encoded_size());

        let (decoded, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.stream_id, 7);
        assert_eq!(decoded.term, 2);
        assert_eq!(decoded.entry_id, 42);
        assert_eq!(decoded.stream_lsn, 3);
        assert_eq!(decoded.timestamp_ns, 1234567890);
        assert_eq!(decoded.key.as_ref(), b"hello");
        assert_eq!(decoded.value.as_ref(), b"world");
    }

    #[test]
    fn test_record_crc_corruption() {
        let record = WalRecord {
            stream_id: 1,
            term: 1,
            entry_id: 1,
            stream_lsn: 1,
            timestamp_ns: 0,
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
        };

        let mut encoded = record.encode_to_vec();
        // Corrupt a byte in the body
        encoded[10] ^= 0xFF;

        assert!(matches!(
            WalRecord::decode(&encoded),
            Err(WalRecordError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn test_empty_key_value() {
        let record = WalRecord {
            stream_id: 0,
            term: 1,
            entry_id: 0,
            stream_lsn: 0,
            timestamp_ns: 0,
            key: Bytes::new(),
            value: Bytes::new(),
        };

        let encoded = record.encode_to_vec();
        let (decoded, _) = WalRecord::decode(&encoded).unwrap();
        assert!(decoded.key.is_empty());
        assert!(decoded.value.is_empty());
    }
}
