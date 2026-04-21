use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct PeerConfig {
    pub id: u64,
    pub addr: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Logical Raft node id
    pub node_id: u64,
    /// Listen address, e.g. "0.0.0.0:9876"
    pub listen_addr: String,
    /// Raft peer message listen address, e.g. "0.0.0.0:9976"
    pub raft_listen_addr: String,
    /// Data directory for WAL segment files
    pub data_dir: PathBuf,
    /// Number of shards. None = auto-detect (num_cpus - 1)
    pub num_shards: Option<usize>,
    /// Max segment file size in bytes (default 64MB)
    pub segment_max_bytes: u64,
    /// Group commit flush interval in microseconds
    pub group_commit_interval_us: u64,
    /// Max batch size before forced flush
    pub group_commit_max_batch: usize,
    /// Bounded channel capacity per shard
    pub channel_capacity: usize,
    /// Number of committed records to retain per stream for hot tailing reads
    pub tail_cache_entries: usize,
    /// How often to evaluate stream-consumer watermarks for index / segment GC
    pub gc_interval_us: u64,
    /// Raft peers for the single group
    pub peers: Vec<PeerConfig>,
    /// Enable NUMA-aware placement
    pub numa_aware: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            listen_addr: "0.0.0.0:9876".to_string(),
            raft_listen_addr: "0.0.0.0:9976".to_string(),
            data_dir: PathBuf::from("/tmp/wal_server/data"),
            num_shards: None,
            segment_max_bytes: 64 * 1024 * 1024, // 64 MiB
            group_commit_interval_us: 200,
            group_commit_max_batch: 256,
            channel_capacity: 4096,
            tail_cache_entries: 128,
            gc_interval_us: 50_000,
            peers: vec![
                PeerConfig {
                    id: 1,
                    addr: "127.0.0.1:9976".to_string(),
                },
                PeerConfig {
                    id: 2,
                    addr: "127.0.0.1:9977".to_string(),
                },
                PeerConfig {
                    id: 3,
                    addr: "127.0.0.1:9978".to_string(),
                },
            ],
            numa_aware: true,
        }
    }
}

impl ServerConfig {
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: ServerConfig = toml::from_str(&content)?;
        Ok(config)
    }
}
