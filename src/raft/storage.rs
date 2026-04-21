use std::cmp;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use raft::eraftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use raft::storage::{GetEntriesContext, RaftState, Storage};
use raft::util::limit_size;
use raft::{Error as RaftError, Result as RaftResult, StorageError};

#[derive(Default, Clone)]
struct PersistentState {
    hard_state: HardState,
    conf_state: ConfState,
    entries: Vec<Entry>,
    snapshot: Snapshot,
}

impl PersistentState {
    fn first_index(&self) -> u64 {
        if let Some(entry) = self.entries.first() {
            entry.index
        } else {
            self.snapshot.get_metadata().index + 1
        }
    }

    fn last_index(&self) -> u64 {
        if let Some(entry) = self.entries.last() {
            entry.index
        } else {
            self.snapshot.get_metadata().index
        }
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        if idx == self.snapshot.get_metadata().index {
            return Ok(self.snapshot.get_metadata().term);
        }
        let first = self.first_index();
        if idx < first.saturating_sub(1) {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if idx < first {
            return Ok(self.snapshot.get_metadata().term);
        }
        if idx > self.last_index() {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        Ok(self.entries[(idx - first) as usize].term)
    }
}

#[derive(Clone)]
pub struct PersistentStorage {
    path: Arc<PathBuf>,
    state: Arc<RwLock<PersistentState>>,
}

impl PersistentStorage {
    pub fn open(path: &Path, conf_state: ConfState) -> Result<Self, Box<dyn std::error::Error>> {
        fs::create_dir_all(path)?;
        let storage = Self {
            path: Arc::new(path.join("raft_state.bin")),
            state: Arc::new(RwLock::new(PersistentState::default())),
        };
        if storage.path.exists() {
            let loaded = Self::load_state(&storage.path)?;
            *storage.wl() = loaded;
        } else {
            {
                let mut state = storage.wl();
                state.conf_state = conf_state.clone();
                state.hard_state.term = 1;
                state.hard_state.commit = 1;
                let mut bootstrap = Entry::default();
                bootstrap.index = 1;
                bootstrap.term = 1;
                state.entries.push(bootstrap);
                let mut snap = Snapshot::default();
                snap.mut_metadata().set_conf_state(conf_state);
                snap.mut_metadata().index = 0;
                snap.mut_metadata().term = 0;
                state.snapshot = snap;
            }
            storage.persist()?;
        }
        Ok(storage)
    }

    pub fn set_hardstate(&self, hs: HardState) -> Result<(), Box<dyn std::error::Error>> {
        self.wl().hard_state = hs;
        self.persist()
    }

    pub fn append(&self, ents: &[Entry]) -> Result<(), Box<dyn std::error::Error>> {
        if ents.is_empty() {
            return Ok(());
        }
        let mut state = self.wl();
        let first_index = state.first_index();
        if ents[0].index < first_index {
            return Err("attempted to overwrite compacted raft entries".into());
        }
        if ents[0].index <= state.last_index() {
            let keep = (ents[0].index - first_index) as usize;
            state.entries.truncate(keep);
        }
        state.entries.extend_from_slice(ents);
        drop(state);
        self.persist()
    }

    pub fn apply_snapshot(&self, snapshot: Snapshot) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.wl();
        state.snapshot = snapshot.clone();
        state.conf_state = snapshot.get_metadata().get_conf_state().clone();
        state.hard_state.commit = snapshot.get_metadata().index;
        state.hard_state.term = cmp::max(state.hard_state.term, snapshot.get_metadata().term);
        state.entries.clear();
        drop(state);
        self.persist()
    }

    pub fn create_snapshot(
        &self,
        index: u64,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.wl();
        let mut snapshot = Snapshot::default();
        snapshot.set_data(data.into());
        let mut meta = SnapshotMetadata::default();
        meta.index = index;
        meta.term = state.term(index)?;
        meta.set_conf_state(state.conf_state.clone());
        snapshot.set_metadata(meta);
        state.snapshot = snapshot;
        drop(state);
        self.persist()
    }

    pub fn compact(&self, compact_index: u64) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.wl();
        if compact_index <= state.first_index() {
            return Ok(());
        }
        if compact_index > state.last_index() + 1 {
            return Err("compact index beyond last raft log index".into());
        }
        let first_index = state.first_index();
        state
            .entries
            .drain(..(compact_index - first_index) as usize);
        drop(state);
        self.persist()
    }

    pub fn snapshot_index(&self) -> u64 {
        self.rl().snapshot.get_metadata().index
    }

    pub fn snapshot(&self) -> Snapshot {
        self.rl().snapshot.clone()
    }

    pub fn hard_state(&self) -> HardState {
        self.rl().hard_state.clone()
    }

    fn persist(&self) -> Result<(), Box<dyn std::error::Error>> {
        let state = self.rl().clone();
        let tmp = self.path.with_extension("tmp");
        let mut buf = Vec::new();
        append_message(&mut buf, &state.hard_state)?;
        append_message(&mut buf, &state.conf_state)?;
        append_message(&mut buf, &state.snapshot)?;
        buf.extend_from_slice(&(state.entries.len() as u32).to_be_bytes());
        for entry in &state.entries {
            append_message(&mut buf, entry)?;
        }
        fs::write(&tmp, buf)?;
        fs::rename(tmp, &*self.path)?;
        Ok(())
    }

    fn load_state(path: &Path) -> Result<PersistentState, Box<dyn std::error::Error>> {
        let data = fs::read(path)?;
        let mut cursor = 0usize;
        let hard_state = take_message::<HardState>(&data, &mut cursor)?;
        let conf_state = take_message::<ConfState>(&data, &mut cursor)?;
        let snapshot = take_message::<Snapshot>(&data, &mut cursor)?;
        if data.len() < cursor + 4 {
            return Err("corrupt raft storage state: missing entry count".into());
        }
        let count =
            u32::from_be_bytes(data[cursor..cursor + 4].try_into().expect("slice")) as usize;
        cursor += 4;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(take_message::<Entry>(&data, &mut cursor)?);
        }
        Ok(PersistentState {
            hard_state,
            conf_state,
            entries,
            snapshot,
        })
    }

    fn rl(&self) -> RwLockReadGuard<'_, PersistentState> {
        self.state.read().expect("persistent storage lock poisoned")
    }

    fn wl(&self) -> RwLockWriteGuard<'_, PersistentState> {
        self.state
            .write()
            .expect("persistent storage lock poisoned")
    }
}

impl Storage for PersistentStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let state = self.rl();
        Ok(RaftState::new(
            state.hard_state.clone(),
            state.conf_state.clone(),
        ))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let state = self.rl();
        let first = state.first_index();
        if low < first {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        if high > state.last_index() + 1 {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        if state.entries.is_empty() {
            return Ok(Vec::new());
        }
        let lo = (low - first) as usize;
        let hi = (high - first) as usize;
        let mut ents = state.entries[lo..hi].to_vec();
        limit_size(&mut ents, max_size.into());
        Ok(ents)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        self.rl().term(idx)
    }

    fn first_index(&self) -> RaftResult<u64> {
        Ok(self.rl().first_index())
    }

    fn last_index(&self) -> RaftResult<u64> {
        Ok(self.rl().last_index())
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        let state = self.rl();
        if state.snapshot.get_metadata().index < request_index {
            return Err(RaftError::Store(StorageError::SnapshotOutOfDate));
        }
        Ok(state.snapshot.clone())
    }
}

fn append_message<M: protobuf::Message>(
    buf: &mut Vec<u8>,
    msg: &M,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = msg.write_to_bytes()?;
    buf.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(&bytes);
    Ok(())
}

fn take_message<M: protobuf::Message + Default>(
    data: &[u8],
    cursor: &mut usize,
) -> Result<M, Box<dyn std::error::Error>> {
    if data.len() < *cursor + 4 {
        return Err("corrupt raft storage state: truncated length".into());
    }
    let len = u32::from_be_bytes(data[*cursor..*cursor + 4].try_into().expect("slice")) as usize;
    *cursor += 4;
    if data.len() < *cursor + len {
        return Err("corrupt raft storage state: truncated protobuf payload".into());
    }
    let msg = M::parse_from_bytes(&data[*cursor..*cursor + len])?;
    *cursor += len;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("wal_server_raft_storage_{unique}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn persists_hardstate_entries_and_snapshot() {
        let dir = temp_dir();
        let mut conf = ConfState::default();
        conf.voters = vec![1, 2, 3];
        let store = PersistentStorage::open(&dir, conf.clone()).unwrap();

        let mut hs = HardState::default();
        hs.term = 4;
        hs.commit = 2;
        store.set_hardstate(hs.clone()).unwrap();

        let mut e1 = Entry::default();
        e1.index = 1;
        e1.term = 1;
        let mut e2 = Entry::default();
        e2.index = 2;
        e2.term = 4;
        store.append(&[e1, e2]).unwrap();
        store.create_snapshot(2, b"hello".to_vec()).unwrap();
        store.compact(3).unwrap();

        let reopened = PersistentStorage::open(&dir, conf).unwrap();
        assert_eq!(reopened.hard_state().term, 4);
        assert_eq!(reopened.snapshot_index(), 2);
        assert_eq!(reopened.first_index().unwrap(), 3);
        let _ = fs::remove_dir_all(&dir);
    }
}
