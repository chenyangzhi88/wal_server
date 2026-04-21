use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpCode {
    Write = 1,
    Read = 2,
    Ack = 3,
    GetStatus = 4,
}

impl OpCode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Write),
            2 => Some(Self::Read),
            3 => Some(Self::Ack),
            4 => Some(Self::GetStatus),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Status {
    Ok = 0,
    ErrInvalidRequest = 1,
    ErrShardUnavailable = 2,
    ErrNotFound = 3,
    ErrEpochFenced = 4,
    ErrNotLeader = 5,
    ErrInternal = 255,
}

#[derive(Debug, Clone)]
pub struct Request {
    pub op: OpCode,
    pub stream_id: u64,
    pub epoch: u64,
    /// For reads/acks this is the target stream LSN.
    /// For writes the server ignores it and allocates the next LSN.
    pub offset: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct Response {
    pub status: Status,
    pub epoch: u64,
    pub offset: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamStatusPayload {
    pub next_stream_lsn: u64,
    pub commit_stream_lsn: u64,
    pub consumed_stream_lsn: u64,
    pub commit_index: u64,
    pub last_applied: u64,
}
