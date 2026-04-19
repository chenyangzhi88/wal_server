use bytes::Bytes;

use super::types::{OpCode, Request, Response, Status};
use super::wire::{MAGIC, RESPONSE_SIZE, REQUEST_HEADER_SIZE, VERSION};

#[derive(Debug)]
pub enum ProtocolError {
    BadMagic(u32),
    BadVersion(u8),
    BadOpCode(u8),
    BufferTooSmall,
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadMagic(m) => write!(f, "bad magic: 0x{m:08X}"),
            Self::BadVersion(v) => write!(f, "bad version: {v}"),
            Self::BadOpCode(op) => write!(f, "bad opcode: {op}"),
            Self::BufferTooSmall => write!(f, "buffer too small"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Try to decode a Request from buffer.
/// Returns Ok(Some((request, bytes_consumed))) if a complete frame is available,
/// Ok(None) if more data is needed, Err on malformed data.
pub fn decode_request(buf: &[u8]) -> Result<Option<(Request, usize)>, ProtocolError> {
    if buf.len() < REQUEST_HEADER_SIZE {
        return Ok(None);
    }

    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != MAGIC {
        return Err(ProtocolError::BadMagic(magic));
    }

    let version = buf[4];
    if version != VERSION {
        return Err(ProtocolError::BadVersion(version));
    }

    let op = OpCode::from_u8(buf[5]).ok_or(ProtocolError::BadOpCode(buf[5]))?;
    let key_len = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]) as usize;
    let payload_len = u32::from_be_bytes([buf[10], buf[11], buf[12], buf[13]]) as usize;

    let total = REQUEST_HEADER_SIZE + key_len + payload_len;
    if buf.len() < total {
        return Ok(None);
    }

    let key = Bytes::copy_from_slice(&buf[REQUEST_HEADER_SIZE..REQUEST_HEADER_SIZE + key_len]);
    let payload = Bytes::copy_from_slice(
        &buf[REQUEST_HEADER_SIZE + key_len..REQUEST_HEADER_SIZE + key_len + payload_len],
    );

    Ok(Some((Request { op, key, payload }, total)))
}

/// Encode a Response into a fixed-size array.
pub fn encode_response(resp: &Response) -> [u8; RESPONSE_SIZE] {
    let mut buf = [0u8; RESPONSE_SIZE];
    buf[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    buf[4] = VERSION;
    buf[5] = resp.status as u8;
    buf[6..14].copy_from_slice(&resp.lsn.to_be_bytes());
    buf
}

/// Encode a Request into bytes (for client use / testing).
pub fn encode_request(req: &Request) -> Vec<u8> {
    let key_len = req.key.len() as u32;
    let payload_len = req.payload.len() as u32;
    let total = REQUEST_HEADER_SIZE + req.key.len() + req.payload.len();
    let mut buf = Vec::with_capacity(total);

    buf.extend_from_slice(&MAGIC.to_be_bytes());
    buf.push(VERSION);
    buf.push(req.op as u8);
    buf.extend_from_slice(&key_len.to_be_bytes());
    buf.extend_from_slice(&payload_len.to_be_bytes());
    buf.extend_from_slice(&req.key);
    buf.extend_from_slice(&req.payload);
    buf
}

/// Decode a Response from a buffer.
pub fn decode_response(buf: &[u8]) -> Result<Response, ProtocolError> {
    if buf.len() < RESPONSE_SIZE {
        return Err(ProtocolError::BufferTooSmall);
    }

    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != MAGIC {
        return Err(ProtocolError::BadMagic(magic));
    }

    let version = buf[4];
    if version != VERSION {
        return Err(ProtocolError::BadVersion(version));
    }

    let status = match buf[5] {
        0 => Status::Ok,
        1 => Status::ErrInvalidRequest,
        2 => Status::ErrShardUnavailable,
        3 => Status::ErrNotFound,
        255 => Status::ErrInternal,
        other => return Err(ProtocolError::BadOpCode(other)),
    };

    let lsn = u64::from_be_bytes([buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13]]);

    Ok(Response { status, lsn })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_roundtrip() {
        let req = Request {
            op: OpCode::Write,
            key: Bytes::from_static(b"test-key"),
            payload: Bytes::from_static(b"test-value"),
        };

        let encoded = encode_request(&req);
        let (decoded, consumed) = decode_request(&encoded).unwrap().unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.op, OpCode::Write);
        assert_eq!(decoded.key.as_ref(), b"test-key");
        assert_eq!(decoded.payload.as_ref(), b"test-value");
    }

    #[test]
    fn test_response_roundtrip() {
        let resp = Response {
            status: Status::Ok,
            lsn: 42,
        };

        let encoded = encode_response(&resp);
        let decoded = decode_response(&encoded).unwrap();

        assert_eq!(decoded.status, Status::Ok);
        assert_eq!(decoded.lsn, 42);
    }

    #[test]
    fn test_incomplete_request() {
        let buf = [0u8; 5]; // too short
        assert!(decode_request(&buf).unwrap().is_none());
    }

    #[test]
    fn test_bad_magic() {
        let mut buf = [0u8; 14];
        buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_be_bytes());
        assert!(matches!(
            decode_request(&buf),
            Err(ProtocolError::BadMagic(0xDEADBEEF))
        ));
    }
}
