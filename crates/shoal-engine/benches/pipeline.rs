//! Benchmarks for the complete Shoal pipeline: put_object and get_object.
//!
//! Measures end-to-end throughput including CDC chunking, zstd compression,
//! erasure coding, shard storage, manifest building, and reconstruction.

use std::collections::BTreeMap;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use shoal_cluster::ClusterState;
use shoal_engine::{ShoalNode, ShoalNodeConfig};
use shoal_meta::MetaStore;
use shoal_store::MemoryStore;
use shoal_types::{Member, MemberState, NodeId, NodeTopology};
use tokio::runtime::Runtime;

fn bench_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}

/// Create a single-node in-memory engine for benchmarking.
async fn bench_node(k: usize, m: usize) -> ShoalNode {
    let node_id = NodeId::from_data(b"bench-node");
    let store = Arc::new(MemoryStore::new(u64::MAX));
    let meta = Arc::new(MetaStore::open_temporary().unwrap());
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

    ShoalNode::new(
        ShoalNodeConfig {
            node_id,
            chunk_size: 65536,
            erasure_k: k,
            erasure_m: m,
            vnodes_per_node: 128,
            shard_replication: 1,
            cache_max_bytes: u64::MAX,
        },
        store,
        meta,
        cluster,
    )
}

fn bench_put_object(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let sizes: &[usize] = &[
        1024,            // 1 KB
        64 * 1024,       // 64 KB
        256 * 1024,      // 256 KB
        1024 * 1024,     // 1 MB
        4 * 1024 * 1024, // 4 MB
    ];

    let mut group = c.benchmark_group("put_object");
    for &size in sizes {
        let data = bench_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("k2_m1", size), &data, |b, data| {
            b.to_async(&rt).iter_custom(|iters| {
                let data = data.clone();
                async move {
                    let node = bench_node(2, 1).await;
                    let start = std::time::Instant::now();
                    for i in 0..iters {
                        node.put_object("bench", &format!("obj-{i}"), &data, BTreeMap::new())
                            .await
                            .unwrap();
                    }
                    start.elapsed()
                }
            });
        });
    }
    group.finish();
}

fn bench_get_object(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let sizes: &[usize] = &[
        1024,            // 1 KB
        64 * 1024,       // 64 KB
        256 * 1024,      // 256 KB
        1024 * 1024,     // 1 MB
        4 * 1024 * 1024, // 4 MB
    ];

    let mut group = c.benchmark_group("get_object");
    for &size in sizes {
        let data = bench_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("k2_m1", size), &data, |b, data| {
            b.to_async(&rt).iter_custom(|iters| {
                let data = data.clone();
                async move {
                    let node = bench_node(2, 1).await;
                    // Pre-populate the store.
                    for i in 0..iters {
                        node.put_object("bench", &format!("obj-{i}"), &data, BTreeMap::new())
                            .await
                            .unwrap();
                    }
                    let start = std::time::Instant::now();
                    for i in 0..iters {
                        let _ = node.get_object("bench", &format!("obj-{i}")).await.unwrap();
                    }
                    start.elapsed()
                }
            });
        });
    }
    group.finish();
}

fn bench_put_erasure_configs(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let data = bench_data(1024 * 1024); // 1 MB
    let configs: &[(usize, usize)] = &[(2, 1), (4, 2), (8, 4)];

    let mut group = c.benchmark_group("put_erasure_config");
    group.throughput(Throughput::Bytes(data.len() as u64));
    for &(k, m) in configs {
        group.bench_with_input(
            BenchmarkId::new(format!("k{k}_m{m}"), data.len()),
            &data,
            |b, data| {
                b.to_async(&rt).iter_custom(|iters| {
                    let data = data.clone();
                    async move {
                        let node = bench_node(k, m).await;
                        let start = std::time::Instant::now();
                        for i in 0..iters {
                            node.put_object("bench", &format!("obj-{i}"), &data, BTreeMap::new())
                                .await
                                .unwrap();
                        }
                        start.elapsed()
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_put_object,
    bench_get_object,
    bench_put_erasure_configs
);
criterion_main!(benches);
