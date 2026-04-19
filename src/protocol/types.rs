use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpCode {
    Write = 1,
    Read = 2,
    Sync = 3,
    GetStatus = 4,
}

impl OpCode {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Write),
            2 => Some(Self::Read),
            3 => Some(Self::Sync),
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
    ErrInternal = 255,
}

#[derive(Debug, Clone)]
pub struct Request {
    pub op: OpCode,
    pub key: Bytes,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy)]
pub struct Response {
    pub status: Status,
    pub lsn: u64,
}
