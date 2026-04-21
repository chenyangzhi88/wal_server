use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct AppendCommand {
    pub request_id: u64,
    pub stream_id: u64,
    pub epoch: u64,
    pub stream_lsn: u64,
    pub payload: Bytes,
}

#[derive(Debug)]
pub enum CommandError {
    InvalidTag(u8),
    BufferTooSmall,
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTag(tag) => write!(f, "invalid command tag: {tag}"),
            Self::BufferTooSmall => write!(f, "command buffer too small"),
        }
    }
}

impl std::error::Error for CommandError {}

const APPEND_TAG: u8 = 1;

pub fn encode_append_command(cmd: &AppendCommand) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 * 4 + 4 + cmd.payload.len());
    buf.push(APPEND_TAG);
    buf.extend_from_slice(&cmd.request_id.to_be_bytes());
    buf.extend_from_slice(&cmd.stream_id.to_be_bytes());
    buf.extend_from_slice(&cmd.epoch.to_be_bytes());
    buf.extend_from_slice(&cmd.stream_lsn.to_be_bytes());
    buf.extend_from_slice(&(cmd.payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&cmd.payload);
    buf
}

pub fn decode_append_command(buf: &[u8]) -> Result<AppendCommand, CommandError> {
    if buf.len() < 1 + 8 * 4 + 4 {
        return Err(CommandError::BufferTooSmall);
    }
    if buf[0] != APPEND_TAG {
        return Err(CommandError::InvalidTag(buf[0]));
    }
    let request_id = u64::from_be_bytes(buf[1..9].try_into().expect("slice length"));
    let stream_id = u64::from_be_bytes(buf[9..17].try_into().expect("slice length"));
    let epoch = u64::from_be_bytes(buf[17..25].try_into().expect("slice length"));
    let stream_lsn = u64::from_be_bytes(buf[25..33].try_into().expect("slice length"));
    let payload_len = u32::from_be_bytes(buf[33..37].try_into().expect("slice length")) as usize;
    if buf.len() < 37 + payload_len {
        return Err(CommandError::BufferTooSmall);
    }
    Ok(AppendCommand {
        request_id,
        stream_id,
        epoch,
        stream_lsn,
        payload: Bytes::copy_from_slice(&buf[37..37 + payload_len]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_command_roundtrip() {
        let cmd = AppendCommand {
            request_id: 10,
            stream_id: 7,
            epoch: 3,
            stream_lsn: 9,
            payload: Bytes::from_static(b"abc"),
        };
        let encoded = encode_append_command(&cmd);
        let decoded = decode_append_command(&encoded).unwrap();
        assert_eq!(decoded.request_id, 10);
        assert_eq!(decoded.stream_id, 7);
        assert_eq!(decoded.epoch, 3);
        assert_eq!(decoded.stream_lsn, 9);
        assert_eq!(decoded.payload.as_ref(), b"abc");
    }
}
