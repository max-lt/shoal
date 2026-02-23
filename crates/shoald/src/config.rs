//! TOML configuration for the Shoal daemon.
//!
//! When no config file is provided, [`CliConfig::auto_detect`] probes the
//! system (RAM via `/proc/meminfo`) and picks sensible defaults.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Top-level configuration, parsed from TOML.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct CliConfig {
    /// Node identity and addresses.
    pub node: NodeSection,
    /// Cluster membership.
    pub cluster: ClusterSection,
    /// Shard storage backend.
    pub storage: StorageSection,
    /// Erasure coding parameters.
    pub erasure: ErasureSection,
    /// Repair and rebalancing tuning.
    pub repair: RepairSection,
    /// S3 API settings.
    pub s3: S3Section,
    /// Logging configuration.
    pub log: LogSection,
}

/// `[node]` section.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct NodeSection {
    /// Directory for persistent data (metadata DB, shard files).
    pub data_dir: PathBuf,
    /// Address for inter-node communication.
    pub listen_addr: String,
    /// Address for the S3 HTTP API.
    pub s3_listen_addr: String,
}

impl Default for NodeSection {
    fn default() -> Self {
        let data_dir = dirs::home_dir()
            .map(|h| h.join(".shoal"))
            .unwrap_or_else(|| PathBuf::from(".shoal"));
        Self {
            data_dir,
            listen_addr: "0.0.0.0:4820".to_string(),
            s3_listen_addr: "0.0.0.0:4821".to_string(),
        }
    }
}

/// `[cluster]` section.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ClusterSection {
    /// Shared secret for cluster authentication.
    ///
    /// If not set (empty), a random secret is generated at startup and
    /// displayed so the user can pass it to other nodes.
    pub secret: String,
    /// Peer nodes to contact on startup (`"node-id"` or `"node-id@host:port"`).
    pub peers: Vec<String>,
}

/// `[storage]` section.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct StorageSection {
    /// Backend type: `"file"` (default) or `"memory"`.
    pub backend: String,
    /// Chunk size in bytes. Auto-detected if omitted.
    pub chunk_size: Option<u32>,
    /// Maximum bytes for the read-through shard cache.
    ///
    /// When a node pulls a shard from a remote peer during a read, it is
    /// cached in a bounded LRU instead of the main store. Set to 0 to
    /// disable caching. Defaults to 100 MB.
    pub cache_max_bytes: Option<u64>,
}

impl Default for StorageSection {
    fn default() -> Self {
        Self {
            backend: "file".to_string(),
            chunk_size: None,
            cache_max_bytes: None,
        }
    }
}

/// `[erasure]` section.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ErasureSection {
    /// Number of data shards.
    pub k: Option<u8>,
    /// Number of parity shards.
    pub m: Option<u8>,
    /// Per-shard replication factor.
    ///
    /// How many nodes store each individual shard. Defaults to 1: erasure
    /// coding provides redundancy so each shard only needs one home. Set
    /// higher for belt-and-suspenders replication.
    pub shard_replication: Option<u8>,
}

/// `[repair]` section.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct RepairSection {
    /// Maximum bandwidth for repair traffic (e.g. `"100MB/s"`).
    pub max_bandwidth: Option<String>,
    /// Number of concurrent shard transfers during repair.
    pub concurrent_transfers: Option<u16>,
}

/// `[s3]` section.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct S3Section {
    /// S3 access key (used in Bearer auth).
    pub access_key: Option<String>,
    /// S3 secret key (used as the Bearer token secret).
    pub secret_key: Option<String>,
}

/// `[log]` section.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct LogSection {
    /// Log level filter (e.g. `"info"`, `"debug"`, `"warn"`).
    pub level: String,
}

impl Default for LogSection {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
        }
    }
}

impl CliConfig {
    /// Load config from a TOML file, or auto-detect if no path given.
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        match path {
            Some(p) => {
                let content = std::fs::read_to_string(p)?;
                let config: CliConfig = toml::from_str(&content)?;
                Ok(config)
            }
            None => Ok(Self::auto_detect()),
        }
    }

    /// Parse config from a TOML string (used in tests).
    #[cfg(test)]
    pub fn from_toml(s: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(s)?)
    }

    /// Build a config with hardware-adaptive defaults.
    ///
    /// Probes system RAM and adjusts chunk size and repair concurrency:
    /// - < 1 GB RAM → RPi profile (128 KB chunks, 1 transfer, 1 MB/s)
    /// - 1–16 GB    → default profile (256 KB chunks, 8 transfers, 100 MB/s)
    /// - > 16 GB    → datacenter profile (4 MB chunks, 64 transfers, 1 GB/s)
    pub fn auto_detect() -> Self {
        let mut config = Self::default();

        if let Some(ram) = detect_total_ram() {
            if ram < 1_073_741_824 {
                // < 1 GB: RPi profile.
                config.storage.chunk_size = Some(131_072);
                config.repair.concurrent_transfers = Some(1);
                config.repair.max_bandwidth = Some("1MB/s".to_string());
            } else if ram > 17_179_869_184 {
                // > 16 GB: datacenter profile.
                config.storage.chunk_size = Some(4_194_304);
                config.repair.concurrent_transfers = Some(64);
                config.repair.max_bandwidth = Some("1GB/s".to_string());
            }
            // else: default profile.
        }

        config
    }

    /// Effective chunk size (config value or 256 KB default).
    pub fn chunk_size(&self) -> u32 {
        self.storage.chunk_size.unwrap_or(262_144)
    }

    /// Effective erasure data shard count.
    pub fn erasure_k(&self) -> u8 {
        self.erasure.k.unwrap_or(4)
    }

    /// Effective erasure parity shard count.
    pub fn erasure_m(&self) -> u8 {
        self.erasure.m.unwrap_or(2)
    }

    /// Effective per-shard replication factor.
    pub fn shard_replication(&self) -> u8 {
        self.erasure.shard_replication.unwrap_or(1)
    }

    /// S3 auth secret derived from the configured secret key.
    pub fn s3_auth_secret(&self) -> Option<String> {
        self.s3.secret_key.clone()
    }

    /// Effective maximum repair bandwidth in bytes per second.
    ///
    /// Parses human-readable strings like `"100MB/s"`, `"1GB/s"`, `"1MB/s"`.
    /// Defaults to 100 MB/s.
    pub fn repair_max_bandwidth_bytes(&self) -> u64 {
        self.repair
            .max_bandwidth
            .as_deref()
            .map(parse_bandwidth)
            .unwrap_or(104_857_600) // 100 MB/s
    }

    /// Effective number of concurrent repair transfers.
    ///
    /// Defaults to 8.
    pub fn repair_concurrent_transfers(&self) -> u16 {
        self.repair.concurrent_transfers.unwrap_or(8)
    }

    /// Effective read-through shard cache size in bytes.
    ///
    /// Defaults to 100 MB.
    pub fn cache_max_bytes(&self) -> u64 {
        self.storage.cache_max_bytes.unwrap_or(100 * 1024 * 1024)
    }
}

/// Parse a human-readable bandwidth string into bytes per second.
///
/// Supports: `"100MB/s"`, `"1GB/s"`, `"512KB/s"`, `"1048576"` (raw bytes).
fn parse_bandwidth(s: &str) -> u64 {
    let s = s.trim().trim_end_matches("/s");
    if let Some(num) = s.strip_suffix("GB") {
        num.trim().parse::<u64>().unwrap_or(100) * 1_073_741_824
    } else if let Some(num) = s.strip_suffix("MB") {
        num.trim().parse::<u64>().unwrap_or(100) * 1_048_576
    } else if let Some(num) = s.strip_suffix("KB") {
        num.trim().parse::<u64>().unwrap_or(100) * 1_024
    } else {
        s.parse::<u64>().unwrap_or(104_857_600)
    }
}

/// Detect total system RAM in bytes by reading `/proc/meminfo`.
///
/// Returns `None` on non-Linux platforms or if the file cannot be read.
fn detect_total_ram() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb_str = rest.split_whitespace().next()?;
            let kb: u64 = kb_str.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
[node]
data_dir = "/tmp/shoal-test"
listen_addr = "127.0.0.1:5820"
s3_listen_addr = "127.0.0.1:5821"

[cluster]
secret = "my-cluster-secret"
peers = ["abc123@192.168.1.10:4820"]

[storage]
backend = "file"
chunk_size = 262144

[erasure]
k = 4
m = 2

[repair]
max_bandwidth = "100MB/s"
concurrent_transfers = 8

[s3]
access_key = "shoal"
secret_key = "shoalsecret"

[log]
level = "debug"
"#;

        let config = CliConfig::from_toml(toml).unwrap();
        assert_eq!(config.node.data_dir, PathBuf::from("/tmp/shoal-test"));
        assert_eq!(config.node.listen_addr, "127.0.0.1:5820");
        assert_eq!(config.node.s3_listen_addr, "127.0.0.1:5821");
        assert_eq!(config.cluster.secret, "my-cluster-secret");
        assert_eq!(config.cluster.peers, vec!["abc123@192.168.1.10:4820"]);
        assert_eq!(config.storage.backend, "file");
        assert_eq!(config.storage.chunk_size, Some(262144));
        assert_eq!(config.erasure.k, Some(4));
        assert_eq!(config.erasure.m, Some(2));
        assert_eq!(config.repair.max_bandwidth.as_deref(), Some("100MB/s"));
        assert_eq!(config.repair.concurrent_transfers, Some(8));
        assert_eq!(config.s3.access_key.as_deref(), Some("shoal"));
        assert_eq!(config.s3.secret_key.as_deref(), Some("shoalsecret"));
        assert_eq!(config.log.level, "debug");
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml = "";
        let config = CliConfig::from_toml(toml).unwrap();
        let expected_default = dirs::home_dir()
            .map(|h| h.join(".shoal"))
            .unwrap_or_else(|| PathBuf::from(".shoal"));
        assert_eq!(config.node.data_dir, expected_default);
        assert_eq!(config.node.s3_listen_addr, "0.0.0.0:4821");
        assert_eq!(config.storage.backend, "file");
        assert_eq!(config.chunk_size(), 262_144);
        assert_eq!(config.erasure_k(), 4);
        assert_eq!(config.erasure_m(), 2);
        assert!(config.s3_auth_secret().is_none());
    }

    #[test]
    fn test_parse_partial_config() {
        let toml = r#"
[storage]
backend = "memory"

[erasure]
k = 2
m = 1
"#;
        let config = CliConfig::from_toml(toml).unwrap();
        assert_eq!(config.storage.backend, "memory");
        assert_eq!(config.erasure_k(), 2);
        assert_eq!(config.erasure_m(), 1);
        // Unspecified sections get defaults.
        assert_eq!(config.node.s3_listen_addr, "0.0.0.0:4821");
        assert_eq!(config.chunk_size(), 262_144);
    }

    #[test]
    fn test_auto_detect_returns_valid_config() {
        let config = CliConfig::auto_detect();
        // Should always succeed with sensible values.
        assert!(config.chunk_size() >= 131_072);
        assert!(config.erasure_k() > 0);
        assert!(config.erasure_m() > 0);
    }

    #[test]
    fn test_load_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shoal.toml");
        std::fs::write(
            &path,
            r#"
[node]
data_dir = "/tmp/test-shoal"
s3_listen_addr = "127.0.0.1:9999"
"#,
        )
        .unwrap();

        let config = CliConfig::load(Some(&path)).unwrap();
        assert_eq!(config.node.data_dir, PathBuf::from("/tmp/test-shoal"));
        assert_eq!(config.node.s3_listen_addr, "127.0.0.1:9999");
    }

    #[test]
    fn test_load_without_file_auto_detects() {
        let config = CliConfig::load(None).unwrap();
        assert!(config.chunk_size() >= 131_072);
    }

    #[test]
    fn test_parse_bandwidth() {
        assert_eq!(parse_bandwidth("100MB/s"), 100 * 1_048_576);
        assert_eq!(parse_bandwidth("1GB/s"), 1_073_741_824);
        assert_eq!(parse_bandwidth("512KB/s"), 512 * 1_024);
        assert_eq!(parse_bandwidth("1048576"), 1_048_576);
    }

    #[test]
    fn test_repair_config_defaults() {
        let config = CliConfig::auto_detect();
        assert!(config.repair_max_bandwidth_bytes() > 0);
        assert!(config.repair_concurrent_transfers() > 0);
    }
}
