//! `shoald` — the Shoal daemon.
//!
//! Binary entrypoint that ties all Shoal components together into a running
//! node with an S3-compatible HTTP API.
//!
//! # Usage
//!
//! ```text
//! shoald start                   # start the node
//! shoald start -c shoal.toml    # start with a config file
//! shoald status                  # show cluster status
//! shoald repair status           # show repair queue
//! shoald benchmark -n 200 -s 65536  # write/read benchmark
//! ```

mod config;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use shoal_cluster::ClusterState;
use shoal_engine::{ShoalNode, ShoalNodeConfig};
use shoal_meta::MetaStore;
use shoal_s3::{S3Server, S3ServerConfig};
use shoal_store::{FileStore, MemoryStore, ShardStore};
use shoal_types::{Member, MemberState, NodeId, NodeTopology};
use tracing::{error, info};

use config::CliConfig;

// -----------------------------------------------------------------------
// CLI definition
// -----------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "shoald",
    version,
    about = "Shoal distributed object storage daemon"
)]
struct Cli {
    /// Path to TOML config file.
    #[arg(short, long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Shoal node.
    Start,

    /// Show cluster status from the local metadata store.
    Status,

    /// Repair operations.
    Repair {
        #[command(subcommand)]
        action: RepairCommands,
    },

    /// Run a quick read/write benchmark (in-memory).
    Benchmark {
        /// Number of objects to write and read.
        #[arg(short = 'n', long, default_value = "100")]
        count: usize,

        /// Size of each object in bytes.
        #[arg(short, long, default_value = "10240")]
        size: usize,
    },
}

#[derive(Subcommand)]
enum RepairCommands {
    /// Show repair queue status.
    Status,
}

// -----------------------------------------------------------------------
// Entrypoint
// -----------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = CliConfig::load(cli.config.as_deref()).context("failed to load config")?;

    setup_tracing(&config.log.level);

    match cli.command {
        Commands::Start => cmd_start(config).await,
        Commands::Status => cmd_status(&config),
        Commands::Repair { action } => match action {
            RepairCommands::Status => cmd_repair_status(&config),
        },
        Commands::Benchmark { count, size } => cmd_benchmark(&config, count, size).await,
    }
}

/// Initialize the `tracing` subscriber with the given level filter.
///
/// Respects `RUST_LOG` env var if set, otherwise uses the config value.
fn setup_tracing(level: &str) {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

// -----------------------------------------------------------------------
// shoald start
// -----------------------------------------------------------------------

async fn cmd_start(config: CliConfig) -> Result<()> {
    info!("starting shoald");
    info!(
        data_dir = %config.node.data_dir.display(),
        s3_addr = %config.node.s3_listen_addr,
        backend = %config.storage.backend,
        chunk_size = config.chunk_size(),
        erasure_k = config.erasure_k(),
        erasure_m = config.erasure_m(),
        "node configuration"
    );

    // Deterministic node ID from listen address.
    let node_id = NodeId::from_data(config.node.listen_addr.as_bytes());

    // Create data directory.
    std::fs::create_dir_all(&config.node.data_dir).context("failed to create data directory")?;

    // Metadata store.
    let meta_path = config.node.data_dir.join("meta");
    let meta = Arc::new(MetaStore::open(&meta_path).context("failed to open metadata store")?);

    // Shard store.
    let store: Arc<dyn ShardStore> = match config.storage.backend.as_str() {
        "memory" => {
            info!("using in-memory shard store");
            Arc::new(MemoryStore::new(u64::MAX))
        }
        _ => {
            let store_path = config.node.data_dir.join("shards");
            info!(path = %store_path.display(), "using file shard store");
            Arc::new(FileStore::new(&store_path).context("failed to initialize file store")?)
        }
    };

    // Cluster state (single-node bootstrap).
    let cluster = ClusterState::new(node_id, 128);
    cluster
        .add_member(Member {
            node_id,
            capacity: u64::MAX,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        })
        .await;

    // Engine.
    let engine = Arc::new(ShoalNode::new(
        ShoalNodeConfig {
            node_id,
            chunk_size: config.chunk_size(),
            erasure_k: config.erasure_k() as usize,
            erasure_m: config.erasure_m() as usize,
            vnodes_per_node: 128,
        },
        store,
        meta,
        cluster,
    ));

    // S3 HTTP API.
    let server = S3Server::new(S3ServerConfig {
        engine,
        auth_secret: config.s3_auth_secret(),
    });

    info!(addr = %config.node.s3_listen_addr, "S3 API ready");
    server
        .serve(&config.node.s3_listen_addr)
        .await
        .context("S3 server failed")?;

    Ok(())
}

// -----------------------------------------------------------------------
// shoald status
// -----------------------------------------------------------------------

fn cmd_status(config: &CliConfig) -> Result<()> {
    let meta_path = config.node.data_dir.join("meta");

    let meta = MetaStore::open(&meta_path).map_err(|e| {
        error!(path = %meta_path.display(), %e, "failed to open metadata store");
        anyhow::anyhow!(
            "cannot open metadata at {}. Is the node running? ({e})",
            meta_path.display(),
        )
    })?;

    let members = meta.list_members()?;
    println!("Cluster members: {}", members.len());
    for member in &members {
        println!(
            "  {} state={:?} capacity={} gen={}",
            member.node_id, member.state, member.capacity, member.generation,
        );
    }

    let queue_len = meta.repair_queue_len()?;
    println!("Repair queue: {queue_len} shards");

    Ok(())
}

// -----------------------------------------------------------------------
// shoald repair status
// -----------------------------------------------------------------------

fn cmd_repair_status(config: &CliConfig) -> Result<()> {
    let meta_path = config.node.data_dir.join("meta");

    let meta = MetaStore::open(&meta_path).map_err(|e| {
        anyhow::anyhow!(
            "cannot open metadata at {}. Is the node running? ({e})",
            meta_path.display(),
        )
    })?;

    let queue_len = meta.repair_queue_len()?;
    println!("Repair queue length: {queue_len} shards");

    if queue_len == 0 {
        println!("No repairs pending.");
    } else {
        println!("Repairs are pending. The node will process them automatically.");
    }

    Ok(())
}

// -----------------------------------------------------------------------
// shoald benchmark
// -----------------------------------------------------------------------

async fn cmd_benchmark(config: &CliConfig, count: usize, size: usize) -> Result<()> {
    let chunk_size = config.chunk_size();
    let k = config.erasure_k();
    let m = config.erasure_m();

    println!("Shoal Benchmark");
    println!("  objects:    {count}");
    println!("  size:       {size} bytes each");
    println!("  chunk_size: {chunk_size}");
    println!("  erasure:    k={k}, m={m}");
    println!();

    // In-memory setup — measures pure engine throughput.
    let node_id = NodeId::from_data(b"benchmark-node");
    let store = Arc::new(MemoryStore::new(u64::MAX));
    let meta = Arc::new(MetaStore::open_temporary()?);
    let cluster = ClusterState::new(node_id, 128);
    cluster
        .add_member(Member {
            node_id,
            capacity: u64::MAX,
            state: MemberState::Alive,
            generation: 1,
            topology: NodeTopology::default(),
        })
        .await;

    let engine = ShoalNode::new(
        ShoalNodeConfig {
            node_id,
            chunk_size,
            erasure_k: k as usize,
            erasure_m: m as usize,
            vnodes_per_node: 128,
        },
        store,
        meta,
        cluster,
    );

    let data = generate_bench_data(size);
    let total_bytes = count as u64 * size as u64;

    // --- Write ---
    print!("Writing {count} objects... ");
    let start = Instant::now();
    for i in 0..count {
        engine
            .put_object("bench", &format!("obj-{i}"), &data, BTreeMap::new())
            .await?;
    }
    let write_dur = start.elapsed();
    let write_mbs = total_bytes as f64 / write_dur.as_secs_f64() / 1_048_576.0;
    println!("{:.2}s ({write_mbs:.1} MB/s)", write_dur.as_secs_f64());

    // --- Read ---
    print!("Reading {count} objects... ");
    let start = Instant::now();
    for i in 0..count {
        let _ = engine.get_object("bench", &format!("obj-{i}")).await?;
    }
    let read_dur = start.elapsed();
    let read_mbs = total_bytes as f64 / read_dur.as_secs_f64() / 1_048_576.0;
    println!("{:.2}s ({read_mbs:.1} MB/s)", read_dur.as_secs_f64());

    println!();
    println!("Summary:");
    println!("  Write throughput: {write_mbs:.1} MB/s");
    println!("  Read throughput:  {read_mbs:.1} MB/s");
    println!(
        "  Total data:       {:.1} MB",
        total_bytes as f64 / 1_048_576.0
    );

    Ok(())
}

/// Generate deterministic test data for benchmarking.
fn generate_bench_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_start_node_binds_to_port() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let addr = "127.0.0.1:0"; // OS picks a free port.

        // Set up a node exactly like cmd_start would.
        let node_id = NodeId::from_data(addr.as_bytes());
        std::fs::create_dir_all(&data_dir).unwrap();

        let meta = Arc::new(MetaStore::open(data_dir.join("meta")).unwrap());
        let store: Arc<dyn ShardStore> = Arc::new(MemoryStore::new(u64::MAX));

        let cluster = ClusterState::new(node_id, 128);
        cluster
            .add_member(Member {
                node_id,
                capacity: u64::MAX,
                state: MemberState::Alive,
                generation: 1,
                topology: NodeTopology::default(),
            })
            .await;

        let engine = Arc::new(ShoalNode::new(
            ShoalNodeConfig {
                node_id,
                chunk_size: 1024,
                erasure_k: 2,
                erasure_m: 1,
                vnodes_per_node: 128,
            },
            store,
            meta,
            cluster,
        ));

        let server = S3Server::new(S3ServerConfig {
            engine,
            auth_secret: None,
        });

        // Bind the listener ourselves so we can discover the actual port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        // Spawn the server in the background.
        let handle = tokio::spawn(async move {
            axum::serve(listener, server.into_router()).await.ok();
        });

        // Give it a moment, then verify the port is reachable.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let conn = tokio::net::TcpStream::connect(bound_addr).await;
        assert!(conn.is_ok(), "should be able to connect to the S3 port");

        handle.abort();
    }
}
