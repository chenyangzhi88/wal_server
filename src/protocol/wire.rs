/// Protocol magic: "WALS" in ASCII
pub const MAGIC: u32 = 0x57414C53;

/// Protocol version
pub const VERSION: u8 = 1;

/// Request header size:
/// magic(4) + version(1) + op(1) + stream_id(8) + epoch(8) + offset(8) + payload_len(4) = 34
pub const REQUEST_HEADER_SIZE: usize = 34;

/// Response header size:
/// magic(4) + version(1) + status(1) + epoch(8) + offset(8) + payload_len(4) = 26
pub const RESPONSE_HEADER_SIZE: usize = 26;
