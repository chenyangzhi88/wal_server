use std::path::{Path, PathBuf};

/// Segment header magic: "WSEG"
pub const SEGMENT_MAGIC: u32 = 0x57534547;
/// Segment header size: 64 bytes
/// [magic:u32][version:u8][shard_id:u16][_pad:u8][segment_id:u64][created_ts_ns:u64][reserved:40]
pub const SEGMENT_HEADER_SIZE: u64 = 64;

#[derive(Debug)]
pub enum SegmentError {
    Io(std::io::Error),
    BadMagic(u32),
    BadVersion(u8),
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "segment IO error: {e}"),
            Self::BadMagic(m) => write!(f, "bad segment magic: 0x{m:08X}"),
            Self::BadVersion(v) => write!(f, "bad segment version: {v}"),
        }
    }
}

impl std::error::Error for SegmentError {}

impl From<std::io::Error> for SegmentError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Generate segment file name: {shard_id:04}_{segment_id:020}.wal
pub fn segment_filename(shard_id: u16, segment_id: u64) -> String {
    format!("{:04}_{:020}.wal", shard_id, segment_id)
}

/// Parse shard_id and segment_id from a segment filename.
pub fn parse_segment_filename(name: &str) -> Option<(u16, u64)> {
    let name = name.strip_suffix(".wal")?;
    let (shard_str, lsn_str) = name.split_once('_')?;
    let shard_id = shard_str.parse::<u16>().ok()?;
    let segment_id = lsn_str.parse::<u64>().ok()?;
    Some((shard_id, segment_id))
}

/// Encode the 64-byte segment header.
pub fn encode_segment_header(shard_id: u16, segment_id: u64, created_ts_ns: u64) -> [u8; 64] {
    let mut hdr = [0u8; 64];
    hdr[0..4].copy_from_slice(&SEGMENT_MAGIC.to_be_bytes());
    hdr[4] = 1; // version
    hdr[5..7].copy_from_slice(&shard_id.to_be_bytes());
    // hdr[7] = padding
    hdr[8..16].copy_from_slice(&segment_id.to_be_bytes());
    hdr[16..24].copy_from_slice(&created_ts_ns.to_be_bytes());
    // hdr[24..64] = reserved zeros
    hdr
}

/// Decode and validate a segment header. Returns (shard_id, segment_id, created_ts_ns).
pub fn decode_segment_header(hdr: &[u8; 64]) -> Result<(u16, u64, u64), SegmentError> {
    let magic = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    if magic != SEGMENT_MAGIC {
        return Err(SegmentError::BadMagic(magic));
    }
    let version = hdr[4];
    if version != 1 {
        return Err(SegmentError::BadVersion(version));
    }
    let shard_id = u16::from_be_bytes([hdr[5], hdr[6]]);
    let segment_id = u64::from_be_bytes([hdr[8], hdr[9], hdr[10], hdr[11], hdr[12], hdr[13], hdr[14], hdr[15]]);
    let created_ts_ns = u64::from_be_bytes([
        hdr[16], hdr[17], hdr[18], hdr[19], hdr[20], hdr[21], hdr[22], hdr[23],
    ]);
    Ok((shard_id, segment_id, created_ts_ns))
}

/// Find all segment files for a given shard in a directory, sorted by segment id ascending.
pub fn list_segments(dir: &Path, shard_id: u16) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut segments = Vec::new();
    if !dir.exists() {
        return Ok(segments);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some((sid, segment_id)) = parse_segment_filename(&name_str) {
            if sid == shard_id {
                segments.push((segment_id, entry.path()));
            }
        }
    }
    segments.sort_by_key(|(lsn, _)| *lsn);
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_filename() {
        assert_eq!(segment_filename(3, 1024), "0003_00000000000000001024.wal");
    }

    #[test]
    fn test_parse_segment_filename() {
        let (sid, lsn) = parse_segment_filename("0003_00000000000000001024.wal").unwrap();
        assert_eq!(sid, 3);
        assert_eq!(lsn, 1024);
    }

    #[test]
    fn test_header_roundtrip() {
        let hdr = encode_segment_header(7, 999, 1234567890);
        let (sid, segment_id, ts) = decode_segment_header(&hdr).unwrap();
        assert_eq!(sid, 7);
        assert_eq!(segment_id, 999);
        assert_eq!(ts, 1234567890);
    }
}
