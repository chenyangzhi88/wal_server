/// Protocol magic: "WALS" in ASCII
pub const MAGIC: u32 = 0x57414C53;

/// Protocol version
pub const VERSION: u8 = 1;

/// Request header size: magic(4) + version(1) + op(1) + key_len(4) + payload_len(4) = 14
pub const REQUEST_HEADER_SIZE: usize = 14;

/// Response size: magic(4) + version(1) + status(1) + lsn(8) = 14
pub const RESPONSE_SIZE: usize = 14;
