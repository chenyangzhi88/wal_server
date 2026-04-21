use std::path::PathBuf;

use clap::Parser;
use wal_server::config::ServerConfig;
use wal_server::numa::detection::NumaTopology;
use wal_server::numa::placement::ShardPlacement;

#[derive(Parser)]
#[command(name = "wal_server", about = "High-performance WAL server")]
struct Cli {
    /// Path to config file (TOML)
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,

    /// Override listen address
    #[arg(long)]
    listen: Option<String>,

    /// Override raft listen address
    #[arg(long)]
    raft_listen: Option<String>,

    /// Override raft node id
    #[arg(long)]
    node_id: Option<u64>,

    /// Override number of shards
    #[arg(long)]
    shards: Option<usize>,

    /// Override data directory
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // Load config (use defaults if file not found)
    let mut config = if cli.config.exists() {
        ServerConfig::load(&cli.config).unwrap_or_else(|e| {
            tracing::warn!("failed to load config: {e}, using defaults");
            ServerConfig::default()
        })
    } else {
        tracing::info!("no config file found, using defaults");
        ServerConfig::default()
    };

    // Apply CLI overrides
    if let Some(addr) = cli.listen {
        config.listen_addr = addr;
    }
    if let Some(addr) = cli.raft_listen {
        config.raft_listen_addr = addr;
    }
    if let Some(node_id) = cli.node_id {
        config.node_id = node_id;
    }
    if let Some(shards) = cli.shards {
        config.num_shards = Some(shards);
    }
    if let Some(dir) = cli.data_dir {
        config.data_dir = dir;
    }

    // Current milestone: force single Raft group until multi-group routing/placement
    // and inter-group replication semantics are implemented.
    if config.num_shards != Some(1) {
        tracing::info!("forcing single Raft group mode with one shard");
        config.num_shards = Some(1);
    }

    // Detect NUMA topology
    let topology = if config.numa_aware {
        NumaTopology::detect().unwrap_or_else(|e| {
            tracing::warn!("NUMA detection failed: {e}, using fallback");
            NumaTopology::single_node_fallback()
        })
    } else {
        NumaTopology::single_node_fallback()
    };

    tracing::info!(
        nodes = topology.nodes.len(),
        total_cpus = topology.total_cpus(),
        "detected NUMA topology"
    );

    // Compute placement
    let placement = ShardPlacement::compute(&topology, config.num_shards);
    tracing::info!(
        num_shards = placement.shard_assignments.len(),
        acceptor_cpus = ?placement.acceptor_cpus,
        "computed placement"
    );

    for a in &placement.shard_assignments {
        tracing::info!(
            shard_id = a.shard_id,
            cpu_id = a.cpu_id,
            numa_node = a.numa_node,
            "shard assignment"
        );
    }

    // Start the server
    wal_server::server::start(config, placement);
}
