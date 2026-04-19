use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Listen address, e.g. "0.0.0.0:9876"
    pub listen_addr: String,
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
    /// Enable NUMA-aware placement
    pub numa_aware: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9876".to_string(),
            data_dir: PathBuf::from("/tmp/wal_server/data"),
            num_shards: None,
            segment_max_bytes: 64 * 1024 * 1024, // 64 MiB
            group_commit_interval_us: 200,
            group_commit_max_batch: 256,
            channel_capacity: 4096,
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
