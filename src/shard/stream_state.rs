use std::collections::HashMap;

use xxhash_rust::xxh3::xxh3_64;

pub type StreamId = u64;

#[derive(Debug, Clone, Copy)]
pub struct StreamReplicaState {
    pub stream_id: StreamId,
    pub term: u64,
    pub next_stream_lsn: u64,
    pub commit_stream_lsn: u64,
    pub last_entry_id: u64,
}

impl StreamReplicaState {
    fn new(stream_id: StreamId) -> Self {
        Self {
            stream_id,
            term: 1,
            next_stream_lsn: 0,
            commit_stream_lsn: 0,
            last_entry_id: 0,
        }
    }
}

#[derive(Default)]
pub struct StreamStateTable {
    streams: HashMap<StreamId, StreamReplicaState>,
}

impl StreamStateTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ensure_stream(&mut self, stream_id: StreamId) -> &mut StreamReplicaState {
        self.streams
            .entry(stream_id)
            .or_insert_with(|| StreamReplicaState::new(stream_id))
    }

    pub fn allocate_append(&mut self, stream_id: StreamId) -> (u64, u64) {
        let state = self.ensure_stream(stream_id);
        let term = state.term;
        let stream_lsn = state.next_stream_lsn;
        state.next_stream_lsn += 1;
        (term, stream_lsn)
    }

    pub fn mark_appended(&mut self, stream_id: StreamId, entry_id: u64, stream_lsn: u64, term: u64) {
        let state = self.ensure_stream(stream_id);
        state.term = state.term.max(term);
        state.last_entry_id = state.last_entry_id.max(entry_id);
        state.next_stream_lsn = state.next_stream_lsn.max(stream_lsn + 1);
    }

    pub fn mark_committed(&mut self, stream_id: StreamId, entry_id: u64, stream_lsn: u64) {
        let state = self.ensure_stream(stream_id);
        state.last_entry_id = state.last_entry_id.max(entry_id);
        state.commit_stream_lsn = state.commit_stream_lsn.max(stream_lsn);
    }

    pub fn observe_recovered(
        &mut self,
        stream_id: StreamId,
        term: u64,
        entry_id: u64,
        stream_lsn: u64,
    ) {
        let state = self.ensure_stream(stream_id);
        state.term = state.term.max(term);
        state.last_entry_id = state.last_entry_id.max(entry_id);
        state.next_stream_lsn = state.next_stream_lsn.max(stream_lsn + 1);
        state.commit_stream_lsn = state.commit_stream_lsn.max(stream_lsn);
    }

    pub fn max_entry_id(&self) -> Option<u64> {
        self.streams.values().map(|state| state.last_entry_id).max()
    }

    pub fn len(&self) -> usize {
        self.streams.len()
    }
}

#[inline]
pub fn derive_stream_id(key: &[u8]) -> StreamId {
    xxh3_64(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_monotonic_stream_lsns() {
        let mut table = StreamStateTable::new();
        let stream_id = derive_stream_id(b"alpha");

        let (_, lsn0) = table.allocate_append(stream_id);
        let (_, lsn1) = table.allocate_append(stream_id);

        assert_eq!(lsn0, 0);
        assert_eq!(lsn1, 1);
    }

    #[test]
    fn recovery_rebuilds_committed_stream_position() {
        let mut table = StreamStateTable::new();
        let stream_id = 7;

        table.observe_recovered(stream_id, 3, 11, 4);
        table.observe_recovered(stream_id, 3, 12, 5);

        let state = table.streams.get(&stream_id).unwrap();
        assert_eq!(state.term, 3);
        assert_eq!(state.next_stream_lsn, 6);
        assert_eq!(state.commit_stream_lsn, 5);
        assert_eq!(state.last_entry_id, 12);
    }
}
