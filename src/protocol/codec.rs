use bytes::Bytes;

use super::types::{OpCode, Request, Response, Status, StreamStatusPayload};
use super::wire::{MAGIC, REQUEST_HEADER_SIZE, RESPONSE_HEADER_SIZE, VERSION};

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

const STATUS_PAYLOAD_SIZE: usize = 40;

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
    let stream_id = u64::from_be_bytes([
        buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13],
    ]);
    let epoch = u64::from_be_bytes([
        buf[14], buf[15], buf[16], buf[17], buf[18], buf[19], buf[20], buf[21],
    ]);
    let offset = u64::from_be_bytes([
        buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29],
    ]);
    let payload_len = u32::from_be_bytes([buf[30], buf[31], buf[32], buf[33]]) as usize;

    let total = REQUEST_HEADER_SIZE + payload_len;
    if buf.len() < total {
        return Ok(None);
    }

    let payload =
        Bytes::copy_from_slice(&buf[REQUEST_HEADER_SIZE..REQUEST_HEADER_SIZE + payload_len]);

    Ok(Some((
        Request {
            op,
            stream_id,
            epoch,
            offset,
            payload,
        },
        total,
    )))
}

/// Encode a Response into bytes.
pub fn encode_response(resp: &Response) -> Vec<u8> {
    let mut buf = Vec::with_capacity(RESPONSE_HEADER_SIZE + resp.payload.len());
    buf.extend_from_slice(&MAGIC.to_be_bytes());
    buf.push(VERSION);
    buf.push(resp.status as u8);
    buf.extend_from_slice(&resp.epoch.to_be_bytes());
    buf.extend_from_slice(&resp.offset.to_be_bytes());
    buf.extend_from_slice(&(resp.payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&resp.payload);
    buf
}

/// Encode a Request into bytes (for client use / testing).
pub fn encode_request(req: &Request) -> Vec<u8> {
    let payload_len = req.payload.len() as u32;
    let total = REQUEST_HEADER_SIZE + req.payload.len();
    let mut buf = Vec::with_capacity(total);

    buf.extend_from_slice(&MAGIC.to_be_bytes());
    buf.push(VERSION);
    buf.push(req.op as u8);
    buf.extend_from_slice(&req.stream_id.to_be_bytes());
    buf.extend_from_slice(&req.epoch.to_be_bytes());
    buf.extend_from_slice(&req.offset.to_be_bytes());
    buf.extend_from_slice(&payload_len.to_be_bytes());
    buf.extend_from_slice(&req.payload);
    buf
}

pub fn append_request(stream_id: u64, epoch: u64, payload: Bytes) -> Request {
    Request {
        op: OpCode::Write,
        stream_id,
        epoch,
        offset: 0,
        payload,
    }
}

pub fn read_request(stream_id: u64, epoch: u64, stream_lsn: u64) -> Request {
    Request {
        op: OpCode::Read,
        stream_id,
        epoch,
        offset: stream_lsn,
        payload: Bytes::new(),
    }
}

pub fn ack_request(stream_id: u64, epoch: u64, consumed_stream_lsn: u64) -> Request {
    Request {
        op: OpCode::Ack,
        stream_id,
        epoch,
        offset: consumed_stream_lsn,
        payload: Bytes::new(),
    }
}

pub fn get_status_request(stream_id: u64) -> Request {
    Request {
        op: OpCode::GetStatus,
        stream_id,
        epoch: 0,
        offset: 0,
        payload: Bytes::new(),
    }
}

pub fn encode_stream_status_payload(payload: &StreamStatusPayload) -> Bytes {
    let mut buf = Vec::with_capacity(STATUS_PAYLOAD_SIZE);
    buf.extend_from_slice(&payload.next_stream_lsn.to_be_bytes());
    buf.extend_from_slice(&payload.commit_stream_lsn.to_be_bytes());
    buf.extend_from_slice(&payload.consumed_stream_lsn.to_be_bytes());
    buf.extend_from_slice(&payload.commit_index.to_be_bytes());
    buf.extend_from_slice(&payload.last_applied.to_be_bytes());
    Bytes::from(buf)
}

pub fn decode_stream_status_payload(buf: &[u8]) -> Result<StreamStatusPayload, ProtocolError> {
    if buf.len() < STATUS_PAYLOAD_SIZE {
        return Err(ProtocolError::BufferTooSmall);
    }
    Ok(StreamStatusPayload {
        next_stream_lsn: u64::from_be_bytes(buf[0..8].try_into().expect("slice length")),
        commit_stream_lsn: u64::from_be_bytes(buf[8..16].try_into().expect("slice length")),
        consumed_stream_lsn: u64::from_be_bytes(buf[16..24].try_into().expect("slice length")),
        commit_index: u64::from_be_bytes(buf[24..32].try_into().expect("slice length")),
        last_applied: u64::from_be_bytes(buf[32..40].try_into().expect("slice length")),
    })
}

/// Decode a Response from a buffer.
pub fn decode_response(buf: &[u8]) -> Result<Response, ProtocolError> {
    if buf.len() < RESPONSE_HEADER_SIZE {
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
        4 => Status::ErrEpochFenced,
        5 => Status::ErrNotLeader,
        255 => Status::ErrInternal,
        other => return Err(ProtocolError::BadOpCode(other)),
    };

    let epoch = u64::from_be_bytes([
        buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13],
    ]);
    let offset = u64::from_be_bytes([
        buf[14], buf[15], buf[16], buf[17], buf[18], buf[19], buf[20], buf[21],
    ]);
    let payload_len = u32::from_be_bytes([buf[22], buf[23], buf[24], buf[25]]) as usize;
    if buf.len() < RESPONSE_HEADER_SIZE + payload_len {
        return Err(ProtocolError::BufferTooSmall);
    }
    let payload =
        Bytes::copy_from_slice(&buf[RESPONSE_HEADER_SIZE..RESPONSE_HEADER_SIZE + payload_len]);

    Ok(Response {
        status,
        epoch,
        offset,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_roundtrip() {
        let req = Request {
            op: OpCode::Write,
            stream_id: 99,
            epoch: 7,
            offset: 0,
            payload: Bytes::from_static(b"test-value"),
        };

        let encoded = encode_request(&req);
        let (decoded, consumed) = decode_request(&encoded).unwrap().unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.op, OpCode::Write);
        assert_eq!(decoded.stream_id, 99);
        assert_eq!(decoded.epoch, 7);
        assert_eq!(decoded.offset, 0);
        assert_eq!(decoded.payload.as_ref(), b"test-value");
    }

    #[test]
    fn test_response_roundtrip() {
        let resp = Response {
            status: Status::Ok,
            epoch: 7,
            offset: 42,
            payload: Bytes::from_static(b"abc"),
        };

        let encoded = encode_response(&resp);
        let decoded = decode_response(&encoded).unwrap();

        assert_eq!(decoded.status, Status::Ok);
        assert_eq!(decoded.epoch, 7);
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.payload.as_ref(), b"abc");
    }

    #[test]
    fn test_status_payload_roundtrip() {
        let payload = StreamStatusPayload {
            next_stream_lsn: 11,
            commit_stream_lsn: 9,
            consumed_stream_lsn: 7,
            commit_index: 100,
            last_applied: 99,
        };

        let encoded = encode_stream_status_payload(&payload);
        let decoded = decode_stream_status_payload(&encoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_incomplete_request() {
        let buf = [0u8; 5]; // too short
        assert!(decode_request(&buf).unwrap().is_none());
    }

    #[test]
    fn test_bad_magic() {
        let mut buf = [0u8; REQUEST_HEADER_SIZE];
        buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_be_bytes());
        assert!(matches!(
            decode_request(&buf),
            Err(ProtocolError::BadMagic(0xDEADBEEF))
        ));
    }
}
