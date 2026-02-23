//! `shoald` — the Shoal daemon.
//!
//! Binary entrypoint that ties all Shoal components together into a running
//! node with an S3-compatible HTTP API.
//!
//! # Usage
//!
//! ```text
//! shoald start                              # start the node
//! shoald start -c shoal.toml               # start with a config file
//! shoald start -d ./node2 -l 127.0.0.1:4822  # second instance
//! shoald start --peer <endpoint_id>         # join an existing cluster
//! shoald status                             # show cluster status
//! shoald repair status                      # show repair queue
//! shoald benchmark -n 200 -s 65536          # write/read benchmark
//! ```

mod config;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use iroh::{EndpointAddr, SecretKey};
use shoal_cluster::{ClusterIdentity, ClusterState, membership};
use shoal_engine::{ShoalNode, ShoalNodeConfig};
use shoal_meta::MetaStore;
use shoal_net::{ShoalMessage, ShoalTransport};
use shoal_s3::{S3Server, S3ServerConfig};
use shoal_store::{FileStore, MemoryStore, ShardStore};
use shoal_types::{Member, MemberState, NodeId, NodeTopology};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

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
    Start {
        /// Override data directory (useful for running multiple instances).
        #[arg(short, long)]
        data_dir: Option<PathBuf>,

        /// Override S3 listen address (e.g. "127.0.0.1:4822").
        #[arg(short = 'l', long)]
        s3_listen_addr: Option<String>,

        /// Peer node(s) to connect to on startup.
        ///
        /// Format: `<endpoint_id>` or `<endpoint_id>@<host:port>`.
        /// Can be specified multiple times.
        #[arg(short, long)]
        peer: Vec<String>,

        /// Cluster secret for authentication (nodes must share the same secret).
        ///
        /// Can also be set via SHOAL_SECRET env var or `[cluster] secret` in
        /// the config file. If none is provided, a random secret is generated
        /// and displayed.
        #[arg(long, env = "SHOAL_SECRET")]
        secret: Option<String>,

        /// Run fully in-memory (no disk persistence).
        #[arg(short, long)]
        memory: bool,
    },

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
    let mut config = CliConfig::load(cli.config.as_deref()).context("failed to load config")?;

    setup_tracing(&config.log.level);

    match cli.command {
        Commands::Start {
            data_dir,
            s3_listen_addr,
            peer,
            secret,
            memory,
        } => {
            // CLI args override config file values.
            if let Some(dir) = data_dir {
                config.node.data_dir = dir;
            }
            if let Some(addr) = s3_listen_addr {
                config.node.s3_listen_addr = addr;
            }
            // Merge CLI peers with config peers.
            if !peer.is_empty() {
                config.cluster.peers = peer;
            }
            if let Some(s) = secret {
                config.cluster.secret = s;
            }
            if memory {
                config.storage.backend = "memory".to_string();
            }
            cmd_start(config).await
        }
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

async fn cmd_start(mut config: CliConfig) -> Result<()> {
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

    let memory_mode = config.storage.backend == "memory";

    // Create data directory (skip in memory mode).
    if !memory_mode {
        std::fs::create_dir_all(&config.node.data_dir)
            .context("failed to create data directory")?;
    }

    // --- Node identity (iroh SecretKey) ---
    let secret_key = if memory_mode {
        use rand::RngCore;
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let key = SecretKey::from(bytes);
        info!("generated ephemeral node key (memory mode)");
        key
    } else {
        load_or_create_secret_key(&config.node.data_dir)?
    };
    let public_key = secret_key.public();
    let node_id = NodeId::from(*public_key.as_bytes());
    info!(%node_id, endpoint_id = %public_key.fmt_short(), "node identity");

    // --- Cluster secret ---
    // If no secret was provided (CLI flag, env var, or config file), generate
    // a random one and display it so the user can pass it to other nodes.
    let generated_secret = config.cluster.secret.is_empty();
    if generated_secret {
        use rand::RngCore;
        let mut bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut bytes);
        config.cluster.secret = bytes.iter().map(|b| format!("{b:02x}")).collect();
    }

    // --- Network transport (iroh QUIC) ---
    // Derive a cluster-specific ALPN from the shared secret so that nodes
    // with different secrets cannot even establish QUIC connections.
    let cluster_alpn = shoal_net::cluster_alpn(config.cluster.secret.as_bytes());
    info!(
        cluster_id = %blake3::hash(config.cluster.secret.as_bytes()).to_hex()[..16],
        "cluster identity derived from secret"
    );
    let transport = Arc::new(
        ShoalTransport::bind_with_alpn(secret_key, iroh::RelayMode::Default, cluster_alpn)
            .await
            .context("failed to bind iroh endpoint")?,
    );

    let local_addr = transport.addr();
    info!(
        endpoint_id = %transport.endpoint_id().fmt_short(),
        "iroh endpoint ready"
    );
    for addr in local_addr.ip_addrs() {
        info!(%addr, "listening on");
    }

    // --- Metadata store ---
    let meta = if memory_mode {
        info!("using in-memory metadata store");
        Arc::new(MetaStore::in_memory())
    } else {
        let meta_path = config.node.data_dir.join("meta");
        Arc::new(MetaStore::open(&meta_path).context("failed to open metadata store")?)
    };

    // --- Shard store ---
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

    // --- Cluster state ---
    let cluster = ClusterState::new(node_id, 128);

    // --- Address book: NodeId → EndpointAddr for routing ---
    let address_book: Arc<RwLock<HashMap<NodeId, EndpointAddr>>> =
        Arc::new(RwLock::new(HashMap::new()));

    // --- Membership service (foca SWIM) ---
    let identity = ClusterIdentity::new(node_id, 1, u64::MAX, NodeTopology::default());
    let membership_handle = Arc::new(membership::start(
        identity.clone(),
        membership::default_config(100),
        cluster.clone(),
        Some(meta.clone()),
    ));

    // --- Connect to peer nodes ---
    for peer_str in &config.cluster.peers {
        match parse_peer(peer_str) {
            Ok((peer_endpoint_addr, peer_node_id)) => {
                // Store peer address for routing.
                address_book
                    .write()
                    .await
                    .insert(peer_node_id, peer_endpoint_addr);
                let peer_identity =
                    ClusterIdentity::new(peer_node_id, 1, u64::MAX, NodeTopology::default());
                info!(peer = %peer_str, "joining cluster via peer");
                if let Err(e) = membership_handle.join(peer_identity) {
                    warn!(peer = %peer_str, %e, "failed to announce to peer");
                }
            }
            Err(e) => {
                warn!(peer = %peer_str, %e, "invalid peer format, skipping");
            }
        }
    }

    // --- Outgoing SWIM routing loop ---
    // Reads foca's outgoing messages and sends them to target nodes via iroh.
    {
        let transport = transport.clone();
        let handle = membership_handle.clone();
        let book = address_book.clone();
        tokio::spawn(async move {
            loop {
                match handle.next_outgoing().await {
                    Some((target, data)) => {
                        // Resolve target address: check book first, then fall back to relay.
                        let addr = {
                            let book = book.read().await;
                            book.get(&target.node_id).cloned()
                        };
                        let addr = match addr {
                            Some(a) => a,
                            None => {
                                // No known direct address — construct from public key.
                                // iroh will attempt relay-based connection.
                                match iroh::EndpointId::from_bytes(target.node_id.as_bytes()) {
                                    Ok(eid) => EndpointAddr::new(eid),
                                    Err(_) => {
                                        warn!(target = %target.node_id, "invalid endpoint ID");
                                        continue;
                                    }
                                }
                            }
                        };

                        let msg = ShoalMessage::SwimData(data);
                        if let Err(e) = transport.send_to(addr, &msg).await {
                            debug!(target = %target.node_id, %e, "failed to route SWIM message");
                        }
                    }
                    None => {
                        info!("membership service stopped, exiting routing loop");
                        break;
                    }
                }
            }
        });
    }

    // --- Incoming connection handler ---
    // Accepts iroh connections and dispatches messages to the membership service.
    {
        let transport = transport.clone();
        let handle = membership_handle.clone();
        let store = store.clone();
        let meta = meta.clone();
        let address_book = address_book.clone();
        tokio::spawn(async move {
            loop {
                match transport.accept().await {
                    Some(conn) => {
                        // Learn the remote peer's address for future routing.
                        let remote_id = conn.remote_id();
                        let remote_node_id = NodeId::from(*remote_id.as_bytes());
                        let remote_addr = EndpointAddr::new(remote_id);
                        address_book
                            .write()
                            .await
                            .insert(remote_node_id, remote_addr);

                        // Spawn a handler for each connection.
                        let handle = handle.clone();
                        let store = store.clone();
                        let meta = meta.clone();

                        // Handle uni-directional streams (SWIM data, shard push, manifest).
                        let conn_uni = conn.clone();
                        let handle_uni = handle.clone();
                        let store_uni = store.clone();
                        let meta_uni = meta.clone();
                        tokio::spawn(async move {
                            ShoalTransport::handle_connection(conn_uni, move |msg, _conn| {
                                let handle = handle_uni.clone();
                                let store = store_uni.clone();
                                let meta = meta_uni.clone();
                                async move {
                                    match msg {
                                        ShoalMessage::SwimData(data) => {
                                            if let Err(e) = handle.feed_data(data) {
                                                warn!(%e, "failed to feed SWIM data");
                                            }
                                        }
                                        ShoalMessage::ShardPush { shard_id, data } => {
                                            debug!(%shard_id, len = data.len(), "received shard push");
                                            if let Err(e) = store
                                                .put(shard_id, bytes::Bytes::from(data))
                                                .await
                                            {
                                                warn!(%shard_id, %e, "failed to store pushed shard");
                                            }
                                        }
                                        ShoalMessage::ManifestPut {
                                            bucket,
                                            key,
                                            manifest_bytes,
                                        } => {
                                            match postcard::from_bytes::<shoal_types::Manifest>(
                                                &manifest_bytes,
                                            ) {
                                                Ok(manifest) => {
                                                    debug!(
                                                        %bucket, %key,
                                                        object_id = %manifest.object_id,
                                                        "received manifest broadcast"
                                                    );
                                                    if let Err(e) = meta.put_manifest(&manifest) {
                                                        warn!(%e, "failed to store broadcast manifest");
                                                    }
                                                    if let Err(e) =
                                                        meta.put_object_key(&bucket, &key, &manifest.object_id)
                                                    {
                                                        warn!(%e, "failed to store broadcast object key");
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!(%e, "failed to deserialize broadcast manifest");
                                                }
                                            }
                                        }
                                        other => {
                                            debug!("unhandled uni-stream message: {other:?}");
                                        }
                                    }
                                }
                            })
                            .await;
                        });

                        // Handle bi-directional streams (shard pull, manifest pull).
                        let meta_bi = meta.clone();
                        tokio::spawn(async move {
                            ShoalTransport::handle_bi_streams(conn, move |msg| {
                                let store = store.clone();
                                let meta = meta_bi.clone();
                                async move {
                                    match msg {
                                        ShoalMessage::ShardRequest { shard_id } => {
                                            let data = store.get(shard_id).await.ok().flatten();
                                            Some(ShoalMessage::ShardResponse {
                                                shard_id,
                                                data: data.map(|b| b.to_vec()),
                                            })
                                        }
                                        ShoalMessage::ManifestRequest { bucket, key } => {
                                            let manifest_bytes = meta
                                                .get_object_key(&bucket, &key)
                                                .ok()
                                                .flatten()
                                                .and_then(|oid| {
                                                    meta.get_manifest(&oid).ok().flatten()
                                                })
                                                .and_then(|m| postcard::to_allocvec(&m).ok());
                                            Some(ShoalMessage::ManifestResponse {
                                                bucket,
                                                key,
                                                manifest_bytes,
                                            })
                                        }
                                        _ => None,
                                    }
                                }
                            })
                            .await;
                        });
                    }
                    None => {
                        info!("transport shut down, exiting accept loop");
                        break;
                    }
                }
            }
        });
    }

    // Print join command for other nodes.
    if generated_secret {
        info!("cluster secret (generated): {}", config.cluster.secret);
    }
    info!(
        "to join this node: shoald start --secret {} --peer {}",
        config.cluster.secret,
        transport.endpoint_id()
    );

    // --- Engine ---
    let engine = Arc::new(
        ShoalNode::new(
            ShoalNodeConfig {
                node_id,
                chunk_size: config.chunk_size(),
                erasure_k: config.erasure_k() as usize,
                erasure_m: config.erasure_m() as usize,
                vnodes_per_node: 128,
                shard_replication: config.shard_replication() as usize,
            },
            store,
            meta.clone(),
            cluster,
        )
        .with_transport(transport.clone())
        .with_address_book(address_book.clone()),
    );

    // --- S3 HTTP API ---
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
// Networking helpers
// -----------------------------------------------------------------------

/// Parse a peer node string.
///
/// Formats:
/// - `<endpoint_id>` — hex-encoded 32-byte public key (iroh relay used for discovery)
/// - `<endpoint_id>@<host:port>` — with an explicit direct address
fn parse_peer(s: &str) -> Result<(EndpointAddr, NodeId)> {
    let (id_str, addr_str) = match s.split_once('@') {
        Some((id, addr)) => (id, Some(addr)),
        None => (s, None),
    };

    let endpoint_id: iroh::EndpointId = id_str
        .parse()
        .context("invalid endpoint ID (expected hex-encoded public key)")?;

    let mut endpoint_addr = EndpointAddr::new(endpoint_id);
    if let Some(addr) = addr_str {
        let socket_addr: SocketAddr = addr
            .parse()
            .context("invalid socket address in peer (expected host:port)")?;
        endpoint_addr = endpoint_addr.with_ip_addr(socket_addr);
    }

    let node_id = NodeId::from(*endpoint_id.as_bytes());

    Ok((endpoint_addr, node_id))
}

// -----------------------------------------------------------------------
// Key management
// -----------------------------------------------------------------------

/// Load or create a persistent iroh secret key from `data_dir/node.key`.
///
/// On first run, generates a new random ed25519 key and writes it to `node.key`.
/// On subsequent runs, reads the existing key. This gives each node a stable
/// iroh identity across restarts, and different `data_dir`s get different identities.
fn load_or_create_secret_key(data_dir: &Path) -> Result<SecretKey> {
    let key_path = data_dir.join("node.key");
    if key_path.exists() {
        let bytes = std::fs::read(&key_path).context("failed to read node.key")?;
        anyhow::ensure!(bytes.len() == 32, "node.key must be exactly 32 bytes");
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        let key = SecretKey::from_bytes(&arr);
        info!(
            endpoint_id = %key.public().fmt_short(),
            "loaded existing node key"
        );
        Ok(key)
    } else {
        // Generate a new random ed25519 key.
        use rand::RngCore;
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut bytes);
        let key = SecretKey::from(bytes);
        std::fs::write(&key_path, key.to_bytes()).context("failed to write node.key")?;
        info!(
            path = %key_path.display(),
            endpoint_id = %key.public().fmt_short(),
            "generated new node key"
        );
        Ok(key)
    }
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
            shard_replication: 1,
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
                shard_replication: 1,
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

    #[test]
    fn test_parse_peer_endpoint_id_only() {
        // Use a known valid ed25519 public key (all zeros is not valid, use a generated one).
        let key = {
            use rand::RngCore;
            let mut b = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut b);
            SecretKey::from(b)
        };
        let id_str = key.public().to_string();

        let (addr, node_id) = parse_peer(&id_str).unwrap();
        assert_eq!(*addr.id.as_bytes(), *node_id.as_bytes());
        assert!(addr.is_empty()); // no direct addresses, relay-only
    }

    #[test]
    fn test_parse_peer_with_address() {
        let key = {
            use rand::RngCore;
            let mut b = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut b);
            SecretKey::from(b)
        };
        let id_str = key.public().to_string();
        let seed = format!("{id_str}@127.0.0.1:4820");

        let (addr, node_id) = parse_peer(&seed).unwrap();
        assert_eq!(*addr.id.as_bytes(), *node_id.as_bytes());
        assert!(!addr.is_empty()); // has a direct address
    }

    #[test]
    fn test_parse_peer_invalid() {
        assert!(parse_peer("not-a-valid-key").is_err());
        assert!(parse_peer("abc123@not-a-valid-addr").is_err());
    }

    #[test]
    fn test_cli_secret_flag_overrides_config() {
        // The --secret flag should override the config file's cluster secret.
        use clap::Parser;

        // Parse CLI args with --secret flag.
        let cli = Cli::try_parse_from(["shoald", "start", "--secret", "my-unique-secret"])
            .expect("CLI should parse with --secret flag");

        match cli.command {
            Commands::Start { secret, .. } => {
                assert_eq!(
                    secret.as_deref(),
                    Some("my-unique-secret"),
                    "--secret flag should be captured"
                );
            }
            _ => panic!("expected Start command"),
        }
    }

    #[test]
    fn test_default_secret_is_empty() {
        // When no secret is configured, the default is empty.
        // cmd_start will generate a random one at startup.
        let config = CliConfig::auto_detect();
        assert!(
            config.cluster.secret.is_empty(),
            "default cluster secret should be empty (generated at runtime)"
        );
    }

    #[test]
    fn test_cli_peer_flag() {
        use clap::Parser;

        let cli = Cli::try_parse_from(["shoald", "start", "--peer", "abc123", "--peer", "def456"])
            .expect("CLI should parse with --peer flags");

        match cli.command {
            Commands::Start { peer, .. } => {
                assert_eq!(peer, vec!["abc123", "def456"]);
            }
            _ => panic!("expected Start command"),
        }
    }

    #[test]
    fn test_secret_key_persistence() {
        let dir = tempfile::tempdir().unwrap();

        // First call generates a new key.
        let key1 = load_or_create_secret_key(dir.path()).unwrap();

        // Second call loads the same key.
        let key2 = load_or_create_secret_key(dir.path()).unwrap();

        assert_eq!(key1.to_bytes(), key2.to_bytes());
        assert_eq!(key1.public(), key2.public());
    }
}
