use std::path::{Path, PathBuf};

use super::index::{LsnIndex, RecordLocation};
use super::record::{current_time_ns, WalRecord};
use super::segment::{
    decode_segment_header, encode_segment_header, list_segments, segment_filename, SEGMENT_HEADER_SIZE,
};
use crate::shard::stream_state::StreamStateTable;

#[derive(Debug)]
pub enum WalWriterError {
    Io(std::io::Error),
    Segment(super::segment::SegmentError),
}

impl std::fmt::Display for WalWriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "WAL writer IO: {e}"),
            Self::Segment(e) => write!(f, "WAL writer segment: {e}"),
        }
    }
}

impl std::error::Error for WalWriterError {}

impl From<std::io::Error> for WalWriterError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Pending write that has been appended but not yet fsync'd.
#[derive(Debug, Clone)]
pub struct PendingWrite {
    pub connection_id: u64,
    pub stream_id: u64,
    pub entry_id: u64,
    pub stream_lsn: u64,
}

/// Async WAL writer using monoio file operations.
/// Single-writer per shard — no locking needed.
pub struct WalWriter {
    shard_id: u16,
    data_dir: PathBuf,
    /// Current segment file (monoio::fs::File)
    current_file: monoio::fs::File,
    /// Current segment identifier.
    current_segment_id: u64,
    /// Current write offset within the segment
    current_offset: u64,
    /// Next entry id to assign.
    next_entry_id: u64,
    /// Next segment id to assign on rotation.
    next_segment_id: u64,
    /// Max segment size
    max_segment_bytes: u64,
}

impl WalWriter {
    /// Open or create the WAL for a shard.
    /// Scans existing segments to find the highest LSN and resumes from there.
    pub async fn open(
        shard_id: u16,
        data_dir: &Path,
        max_segment_bytes: u64,
    ) -> Result<(Self, LsnIndex, StreamStateTable), WalWriterError> {
        std::fs::create_dir_all(data_dir).map_err(WalWriterError::Io)?;

        let mut index = LsnIndex::new();
        let mut streams = StreamStateTable::new();
        let segments = list_segments(data_dir, shard_id).map_err(WalWriterError::Io)?;

        let mut next_entry_id: u64 = 0;
        let mut next_segment_id: u64 = segments.last().map(|(segment_id, _)| *segment_id + 1).unwrap_or(0);

        // Replay existing segments to rebuild index
        for (segment_id, path) in &segments {
            let data = std::fs::read(path).map_err(WalWriterError::Io)?;
            if (data.len() as u64) < SEGMENT_HEADER_SIZE {
                continue;
            }
            let header: [u8; 64] = data[..SEGMENT_HEADER_SIZE as usize]
                .try_into()
                .expect("segment header length checked");
            decode_segment_header(&header).map_err(WalWriterError::Segment)?;
            let mut offset = SEGMENT_HEADER_SIZE as usize;
            while offset < data.len() {
                match WalRecord::decode(&data[offset..]) {
                    Ok((record, consumed)) => {
                        index.insert(
                            record.entry_id,
                            RecordLocation {
                                segment_id: *segment_id,
                                offset: offset as u64,
                                size: consumed as u32,
                            },
                        );
                        streams.observe_recovered(
                            record.stream_id,
                            record.term,
                            record.entry_id,
                            record.stream_lsn,
                        );
                        if record.entry_id >= next_entry_id {
                            next_entry_id = record.entry_id + 1;
                        }
                        offset += consumed;
                    }
                    Err(_) => break, // torn write at end
                }
            }
        }

        // Open the last segment or create a new one
        let (segment_id, file, seg_offset) = if let Some((segment_id, path)) = segments.last() {
            let meta = std::fs::metadata(path).map_err(WalWriterError::Io)?;
            let file = monoio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .await
                .map_err(WalWriterError::Io)?;
            (*segment_id, file, meta.len())
        } else {
            // Create first segment
            let initial_segment_id = next_segment_id;
            let (file, _) = create_segment(data_dir, shard_id, initial_segment_id).await?;
            next_segment_id += 1;
            (initial_segment_id, file, SEGMENT_HEADER_SIZE)
        };

        Ok((
            WalWriter {
                shard_id,
                data_dir: data_dir.to_path_buf(),
                current_file: file,
                current_segment_id: segment_id,
                current_offset: seg_offset,
                next_entry_id,
                next_segment_id,
                max_segment_bytes,
            },
            index,
            streams,
        ))
    }

    /// Append a key-value pair to the WAL. Returns (entry_id, RecordLocation).
    /// Does NOT fsync — caller is responsible for batching syncs.
    pub async fn append(
        &mut self,
        stream_id: u64,
        term: u64,
        stream_lsn: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<(u64, RecordLocation), WalWriterError> {
        let entry_id = self.next_entry_id;
        self.next_entry_id += 1;

        let record = WalRecord {
            stream_id,
            term,
            entry_id,
            stream_lsn,
            timestamp_ns: current_time_ns(),
            key: bytes::Bytes::copy_from_slice(key),
            value: bytes::Bytes::copy_from_slice(value),
        };

        let data = record.encode_to_vec();
        let offset = self.current_offset;

        // write_all_at for positioned write via io_uring
        let (res, _buf) = self.current_file.write_all_at(data, offset).await;
        res.map_err(WalWriterError::Io)?;

        let loc = RecordLocation {
            segment_id: self.current_segment_id,
            offset,
            size: record.encoded_size() as u32,
        };

        self.current_offset += record.encoded_size() as u64;

        // Check if rotation is needed
        if self.current_offset >= self.max_segment_bytes {
            self.rotate().await?;
        }

        Ok((entry_id, loc))
    }

    /// fdatasync the current segment via io_uring.
    pub async fn sync(&self) -> Result<(), WalWriterError> {
        self.current_file.sync_data().await.map_err(WalWriterError::Io)
    }

    pub fn current_segment_id(&self) -> u64 {
        self.current_segment_id
    }

    pub fn next_entry_id(&self) -> u64 {
        self.next_entry_id
    }

    /// Rotate to a new segment.
    async fn rotate(&mut self) -> Result<(), WalWriterError> {
        // Sync the current segment before rotation
        self.sync().await?;

        let new_segment_id = self.next_segment_id;
        let (file, _) = create_segment(&self.data_dir, self.shard_id, new_segment_id).await?;
        self.next_segment_id += 1;

        self.current_file = file;
        self.current_segment_id = new_segment_id;
        self.current_offset = SEGMENT_HEADER_SIZE;

        tracing::info!(
            shard_id = self.shard_id,
            segment_id = new_segment_id,
            "rotated to new segment"
        );

        Ok(())
    }
}

/// Create a new segment file with header. Returns (File, path).
async fn create_segment(
    data_dir: &Path,
    shard_id: u16,
    segment_id: u64,
) -> Result<(monoio::fs::File, PathBuf), WalWriterError> {
    let filename = segment_filename(shard_id, segment_id);
    let path = data_dir.join(&filename);

    let file = monoio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .await
        .map_err(WalWriterError::Io)?;

    let header = encode_segment_header(shard_id, segment_id, current_time_ns());
    let (res, _) = file.write_all_at(header.to_vec(), 0).await;
    res.map_err(WalWriterError::Io)?;

    Ok((file, path))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::wal::reader::WalReader;

    fn make_temp_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("wal_server_writer_test_{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn remove_temp_dir(path: &Path) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn recovers_index_and_stream_state_from_segments() {
        let root = make_temp_dir();
        let shard_dir = root.join("shard_0001");

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let (mut writer, _index, mut streams) = WalWriter::open(1, &shard_dir, 1024).await.unwrap();

            let stream_id = 42;
            let (entry0, loc0) = writer.append(stream_id, 3, 0, b"alpha", b"one").await.unwrap();
            streams.mark_appended(stream_id, entry0, 0, 3);
            let (entry1, _loc1) = writer.append(stream_id, 3, 1, b"alpha", b"two").await.unwrap();
            streams.mark_appended(stream_id, entry1, 1, 3);
            writer.sync().await.unwrap();

            assert_eq!(entry0, 0);
            assert_eq!(loc0.segment_id, 0);
            assert_eq!(writer.current_segment_id(), 0);
        });

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let (_writer, index, streams) = WalWriter::open(1, &shard_dir, 1024).await.unwrap();
            assert_eq!(index.max_lsn(), Some(1));
            assert_eq!(streams.len(), 1);

            let mut reader = WalReader::new(1, &shard_dir);
            let record = reader.read_by_lsn(1, &index).await.unwrap();
            assert_eq!(record.stream_id, 42);
            assert_eq!(record.term, 3);
            assert_eq!(record.entry_id, 1);
            assert_eq!(record.stream_lsn, 1);
            assert_eq!(record.value.as_ref(), b"two");
        });

        remove_temp_dir(&root);
    }
}
