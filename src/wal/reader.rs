use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::index::StreamLogIndex;
use super::record::WalRecord;
use super::segment::list_segments;

#[derive(Debug)]
pub enum WalReaderError {
    Io(std::io::Error),
    NotFound(u64),
    Corrupt(super::record::WalRecordError),
}

impl std::fmt::Display for WalReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "WAL reader IO: {e}"),
            Self::NotFound(lsn) => write!(f, "LSN {lsn} not found"),
            Self::Corrupt(e) => write!(f, "corrupt record: {e}"),
        }
    }
}

impl std::error::Error for WalReaderError {}

/// Async WAL reader using monoio file operations.
/// Single-threaded per shard — no synchronization needed.
pub struct WalReader {
    shard_id: u16,
    data_dir: PathBuf,
    /// Cache of open segment files: segment_start_lsn → File
    segment_cache: HashMap<u64, monoio::fs::File>,
}

impl WalReader {
    pub fn new(shard_id: u16, data_dir: &Path) -> Self {
        Self {
            shard_id,
            data_dir: data_dir.to_path_buf(),
            segment_cache: HashMap::new(),
        }
    }

    /// Read a record by stream-local LSN using the index.
    pub async fn read_record(
        &mut self,
        stream_id: u64,
        stream_lsn: u64,
        index: &StreamLogIndex,
    ) -> Result<WalRecord, WalReaderError> {
        let loc = index
            .lookup(stream_id, stream_lsn)
            .ok_or(WalReaderError::NotFound(stream_lsn))?;
        let loc = *loc;

        let file = self.get_or_open_segment(loc.segment_id).await?;

        // Read the record bytes
        let buf = vec![0u8; loc.size as usize];
        let (res, buf) = file.read_exact_at(buf, loc.offset).await;
        res.map_err(WalReaderError::Io)?;

        let (record, _) = WalRecord::decode(&buf).map_err(WalReaderError::Corrupt)?;
        Ok(record)
    }

    async fn get_or_open_segment(
        &mut self,
        segment_id: u64,
    ) -> Result<&monoio::fs::File, WalReaderError> {
        if !self.segment_cache.contains_key(&segment_id) {
            let segments =
                list_segments(&self.data_dir, self.shard_id).map_err(WalReaderError::Io)?;
            let path = segments
                .iter()
                .find(|(id, _)| *id == segment_id)
                .map(|(_, p)| p.clone())
                .ok_or(WalReaderError::NotFound(segment_id))?;

            let file = monoio::fs::File::open(&path)
                .await
                .map_err(WalReaderError::Io)?;
            self.segment_cache.insert(segment_id, file);
        }

        Ok(self.segment_cache.get(&segment_id).unwrap())
    }

    /// Close cached segment files for GC'd segments.
    pub fn evict_segment(&mut self, segment_id: u64) {
        self.segment_cache.remove(&segment_id);
    }
}
