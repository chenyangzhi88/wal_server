use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

pub type StreamId = u64;

#[derive(Debug, Clone, Copy)]
pub struct StreamStatus {
    pub stream_id: StreamId,
    pub max_epoch: u64,
    pub next_stream_lsn: u64,
    pub commit_stream_lsn: u64,
    pub consumed_stream_lsn: u64,
    pub last_entry_id: u64,
}

#[derive(Debug, Clone)]
pub struct TailRecord {
    pub stream_lsn: u64,
    pub entry_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
struct StreamReplicaState {
    stream_id: StreamId,
    max_epoch: u64,
    next_stream_lsn: u64,
    commit_stream_lsn: u64,
    consumed_stream_lsn: u64,
    last_entry_id: u64,
    tail_cache: VecDeque<TailRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochFenceError {
    Stale {
        current_epoch: u64,
        request_epoch: u64,
    },
}

impl StreamReplicaState {
    fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            max_epoch: 0,
            next_stream_lsn: 0,
            commit_stream_lsn: 0,
            consumed_stream_lsn: 0,
            last_entry_id: 0,
            tail_cache: VecDeque::new(),
        }
    }
}

#[derive(Default)]
pub struct StreamStateTable {
    streams: HashMap<StreamId, StreamReplicaState>,
    tail_cache_limit: usize,
}

impl StreamStateTable {
    pub fn new(tail_cache_limit: usize) -> Self {
        Self {
            streams: HashMap::new(),
            tail_cache_limit,
        }
    }

    fn ensure_stream(&mut self, stream_id: StreamId) -> &mut StreamReplicaState {
        self.streams
            .entry(stream_id)
            .or_insert_with(|| StreamReplicaState::new(stream_id))
    }

    pub fn allocate_append(
        &mut self,
        stream_id: StreamId,
        epoch: u64,
    ) -> Result<u64, EpochFenceError> {
        let state = self.ensure_stream(stream_id);
        let stream_lsn = state.next_stream_lsn;
        if epoch < state.max_epoch {
            return Err(EpochFenceError::Stale {
                current_epoch: state.max_epoch,
                request_epoch: epoch,
            });
        }
        state.max_epoch = epoch;
        state.next_stream_lsn += 1;
        Ok(stream_lsn)
    }

    pub fn rollback_uncommitted_append(&mut self, stream_id: StreamId, stream_lsn: u64) {
        if let Some(state) = self.streams.get_mut(&stream_id) {
            let expected_next = stream_lsn.saturating_add(1);
            if state.next_stream_lsn == expected_next && state.commit_stream_lsn <= stream_lsn {
                state.next_stream_lsn = stream_lsn;
            }
        }
    }

    pub fn mark_appended(
        &mut self,
        stream_id: StreamId,
        epoch: u64,
        entry_id: u64,
        stream_lsn: u64,
        payload: Bytes,
    ) {
        let tail_cache_limit = self.tail_cache_limit;
        let state = self.ensure_stream(stream_id);
        state.max_epoch = state.max_epoch.max(epoch);
        state.last_entry_id = state.last_entry_id.max(entry_id);
        state.next_stream_lsn = state.next_stream_lsn.max(stream_lsn + 1);
        state.tail_cache.push_back(TailRecord {
            stream_lsn,
            entry_id,
            payload,
        });
        while state.tail_cache.len() > tail_cache_limit {
            state.tail_cache.pop_front();
        }
    }

    pub fn mark_committed(&mut self, stream_id: StreamId, entry_id: u64, stream_lsn: u64) {
        let state = self.ensure_stream(stream_id);
        state.last_entry_id = state.last_entry_id.max(entry_id);
        state.commit_stream_lsn = state.commit_stream_lsn.max(stream_lsn + 1);
    }

    pub fn advance_consumed(&mut self, stream_id: StreamId, consumed_stream_lsn: u64) -> u64 {
        let state = self.ensure_stream(stream_id);
        state.consumed_stream_lsn = state.consumed_stream_lsn.max(consumed_stream_lsn);
        while state
            .tail_cache
            .front()
            .is_some_and(|record| record.stream_lsn < state.consumed_stream_lsn)
        {
            state.tail_cache.pop_front();
        }
        state.consumed_stream_lsn
    }

    pub fn read_from_tail(&self, stream_id: StreamId, stream_lsn: u64) -> Option<TailRecord> {
        self.streams
            .get(&stream_id)?
            .tail_cache
            .iter()
            .find(|record| record.stream_lsn == stream_lsn)
            .cloned()
    }

    pub fn observe_recovered(
        &mut self,
        stream_id: StreamId,
        epoch: u64,
        entry_id: u64,
        stream_lsn: u64,
        payload: Bytes,
    ) {
        let tail_cache_limit = self.tail_cache_limit;
        let state = self.ensure_stream(stream_id);
        state.max_epoch = state.max_epoch.max(epoch);
        state.last_entry_id = state.last_entry_id.max(entry_id);
        state.next_stream_lsn = state.next_stream_lsn.max(stream_lsn + 1);
        state.commit_stream_lsn = state.commit_stream_lsn.max(stream_lsn + 1);
        state.tail_cache.push_back(TailRecord {
            stream_lsn,
            entry_id,
            payload,
        });
        while state.tail_cache.len() > tail_cache_limit {
            state.tail_cache.pop_front();
        }
    }

    pub fn status(&self, stream_id: StreamId) -> Option<StreamStatus> {
        self.streams.get(&stream_id).map(|state| StreamStatus {
            stream_id: state.stream_id,
            max_epoch: state.max_epoch,
            next_stream_lsn: state.next_stream_lsn,
            commit_stream_lsn: state.commit_stream_lsn,
            consumed_stream_lsn: state.consumed_stream_lsn,
            last_entry_id: state.last_entry_id,
        })
    }

    pub fn current_epoch(&self, stream_id: StreamId) -> u64 {
        self.streams
            .get(&stream_id)
            .map(|state| state.max_epoch)
            .unwrap_or(0)
    }

    pub fn all_statuses(&self) -> Vec<StreamStatus> {
        self.streams
            .values()
            .map(|state| StreamStatus {
                stream_id: state.stream_id,
                max_epoch: state.max_epoch,
                next_stream_lsn: state.next_stream_lsn,
                commit_stream_lsn: state.commit_stream_lsn,
                consumed_stream_lsn: state.consumed_stream_lsn,
                last_entry_id: state.last_entry_id,
            })
            .collect()
    }

    pub fn encode_snapshot(&self) -> Vec<u8> {
        let statuses = self.all_statuses();
        let mut buf = Vec::with_capacity(4 + statuses.len() * 48);
        buf.extend_from_slice(&(statuses.len() as u32).to_be_bytes());
        for status in statuses {
            buf.extend_from_slice(&status.stream_id.to_be_bytes());
            buf.extend_from_slice(&status.max_epoch.to_be_bytes());
            buf.extend_from_slice(&status.next_stream_lsn.to_be_bytes());
            buf.extend_from_slice(&status.commit_stream_lsn.to_be_bytes());
            buf.extend_from_slice(&status.consumed_stream_lsn.to_be_bytes());
            buf.extend_from_slice(&status.last_entry_id.to_be_bytes());
        }
        buf
    }

    pub fn decode_snapshot(
        tail_cache_limit: usize,
        buf: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if buf.is_empty() {
            return Ok(Self::new(tail_cache_limit));
        }
        if buf.len() < 4 {
            return Err("stream snapshot too small".into());
        }
        let count = u32::from_be_bytes(buf[0..4].try_into().expect("slice")) as usize;
        let mut cursor = 4usize;
        let mut streams = HashMap::with_capacity(count);
        for _ in 0..count {
            if buf.len() < cursor + 48 {
                return Err("stream snapshot truncated".into());
            }
            let stream_id = u64::from_be_bytes(buf[cursor..cursor + 8].try_into().expect("slice"));
            cursor += 8;
            let max_epoch = u64::from_be_bytes(buf[cursor..cursor + 8].try_into().expect("slice"));
            cursor += 8;
            let next_stream_lsn =
                u64::from_be_bytes(buf[cursor..cursor + 8].try_into().expect("slice"));
            cursor += 8;
            let commit_stream_lsn =
                u64::from_be_bytes(buf[cursor..cursor + 8].try_into().expect("slice"));
            cursor += 8;
            let consumed_stream_lsn =
                u64::from_be_bytes(buf[cursor..cursor + 8].try_into().expect("slice"));
            cursor += 8;
            let last_entry_id =
                u64::from_be_bytes(buf[cursor..cursor + 8].try_into().expect("slice"));
            cursor += 8;

            streams.insert(
                stream_id,
                StreamReplicaState {
                    stream_id,
                    max_epoch,
                    next_stream_lsn,
                    commit_stream_lsn,
                    consumed_stream_lsn,
                    last_entry_id,
                    tail_cache: VecDeque::new(),
                },
            );
        }
        Ok(Self {
            streams,
            tail_cache_limit,
        })
    }

    pub fn max_entry_id(&self) -> Option<u64> {
        self.streams.values().map(|state| state.last_entry_id).max()
    }

    pub fn len(&self) -> usize {
        self.streams.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_monotonic_stream_lsns() {
        let mut table = StreamStateTable::new(2);
        let stream_id = 9;

        let lsn0 = table.allocate_append(stream_id, 3).unwrap();
        let lsn1 = table.allocate_append(stream_id, 3).unwrap();

        assert_eq!(lsn0, 0);
        assert_eq!(lsn1, 1);
    }

    #[test]
    fn recovery_rebuilds_committed_stream_position() {
        let mut table = StreamStateTable::new(2);
        let stream_id = 7;

        table.observe_recovered(stream_id, 3, 11, 4, Bytes::from_static(b"one"));
        table.observe_recovered(stream_id, 3, 12, 5, Bytes::from_static(b"two"));

        let status = table.status(stream_id).unwrap();
        assert_eq!(status.max_epoch, 3);
        assert_eq!(status.next_stream_lsn, 6);
        assert_eq!(status.commit_stream_lsn, 6);
        assert_eq!(status.last_entry_id, 12);
    }

    #[test]
    fn rejects_stale_epochs() {
        let mut table = StreamStateTable::new(2);
        table.allocate_append(1, 9).unwrap();

        assert!(matches!(
            table.allocate_append(1, 8),
            Err(EpochFenceError::Stale {
                current_epoch: 9,
                request_epoch: 8,
            })
        ));
    }

    #[test]
    fn snapshot_roundtrip() {
        let mut table = StreamStateTable::new(2);
        table.observe_recovered(7, 3, 11, 4, Bytes::from_static(b"one"));
        table.advance_consumed(7, 3);
        let encoded = table.encode_snapshot();
        let decoded = StreamStateTable::decode_snapshot(2, &encoded).unwrap();
        let status = decoded.status(7).unwrap();
        assert_eq!(status.max_epoch, 3);
        assert_eq!(status.next_stream_lsn, 5);
        assert_eq!(status.consumed_stream_lsn, 3);
        assert_eq!(status.last_entry_id, 11);
    }
}
