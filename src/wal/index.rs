use std::collections::{BTreeMap, HashMap};

/// Location of a record within segment files.
#[derive(Debug, Clone, Copy)]
pub struct RecordLocation {
    /// The segment id containing this record.
    pub segment_id: u64,
    /// Byte offset within the segment file.
    pub offset: u64,
    /// Total size of the record on disk.
    pub size: u32,
}

/// In-memory index mapping LSN → segment location.
/// Lives on a single shard thread — no synchronization needed.
pub struct StreamLogIndex {
    entries: HashMap<u64, BTreeMap<u64, RecordLocation>>,
}

impl StreamLogIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, stream_id: u64, stream_lsn: u64, loc: RecordLocation) {
        self.entries
            .entry(stream_id)
            .or_default()
            .insert(stream_lsn, loc);
    }

    pub fn lookup(&self, stream_id: u64, stream_lsn: u64) -> Option<&RecordLocation> {
        self.entries.get(&stream_id)?.get(&stream_lsn)
    }

    /// Remove all entries with stream_lsn < the given value (for GC).
    pub fn truncate_stream_before(&mut self, stream_id: u64, stream_lsn: u64) {
        if let Some(entries) = self.entries.get_mut(&stream_id) {
            let keep = entries.split_off(&stream_lsn);
            *entries = keep;
        }
    }

    pub fn len(&self) -> usize {
        self.entries.values().map(BTreeMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.values().all(BTreeMap::is_empty)
    }

    pub fn stream_len(&self, stream_id: u64) -> usize {
        self.entries.get(&stream_id).map(BTreeMap::len).unwrap_or(0)
    }

    pub fn live_segment_ids(&self) -> std::collections::HashSet<u64> {
        self.entries
            .values()
            .flat_map(|entries| entries.values().map(|loc| loc.segment_id))
            .collect()
    }
}
