use std::collections::BTreeMap;

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
pub struct LsnIndex {
    entries: BTreeMap<u64, RecordLocation>,
}

impl LsnIndex {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, lsn: u64, loc: RecordLocation) {
        self.entries.insert(lsn, loc);
    }

    pub fn lookup(&self, lsn: u64) -> Option<&RecordLocation> {
        self.entries.get(&lsn)
    }

    /// Remove all entries with LSN < the given value (for GC).
    pub fn truncate_before(&mut self, lsn: u64) {
        // split_off returns entries >= lsn, we keep those
        let keep = self.entries.split_off(&lsn);
        self.entries = keep;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the highest LSN in the index.
    pub fn max_lsn(&self) -> Option<u64> {
        self.entries.keys().next_back().copied()
    }
}
