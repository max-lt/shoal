//! Benchmarks for erasure encoding and decoding.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use shoal_erasure::{ErasureEncoder, Shard, decode};

fn bench_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}

/// Convert Shards to the (index, data) pairs expected by decode.
fn to_decode_input(shards: &[Shard]) -> Vec<(u8, Vec<u8>)> {
    shards.iter().map(|s| (s.index, s.data.to_vec())).collect()
}

fn bench_encode(c: &mut Criterion) {
    let configs: &[(usize, usize)] = &[(2, 1), (4, 2), (8, 4)];
    let chunk_sizes: &[usize] = &[64 * 1024, 256 * 1024];

    let mut group = c.benchmark_group("erasure_encode");
    for &(k, m) in configs {
        let encoder = ErasureEncoder::new(k, m);
        for &size in chunk_sizes {
            let data = bench_data(size);
            let label = format!("k{k}_m{m}_{size}");
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::new("encode", &label), &data, |b, data| {
                b.iter(|| encoder.encode(data).unwrap());
            });
        }
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let configs: &[(usize, usize)] = &[(2, 1), (4, 2)];
    let chunk_sizes: &[usize] = &[64 * 1024, 256 * 1024];

    let mut group = c.benchmark_group("erasure_decode");
    for &(k, m) in configs {
        let encoder = ErasureEncoder::new(k, m);
        for &size in chunk_sizes {
            let data = bench_data(size);
            let (shards, original_size) = encoder.encode(&data).unwrap();
            let decode_input = to_decode_input(&shards);

            // Decode using all shards (fast path).
            let label = format!("k{k}_m{m}_{size}_all");
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(
                BenchmarkId::new("decode_all", &label),
                &(&decode_input, original_size),
                |b, (input, original_size)| {
                    b.iter(|| decode(k, m, input, *original_size).unwrap());
                },
            );

            // Decode with one shard missing (RS reconstruction).
            let partial: Vec<_> = decode_input.iter().skip(1).cloned().collect();
            let label = format!("k{k}_m{m}_{size}_missing1");
            group.bench_with_input(
                BenchmarkId::new("decode_missing1", &label),
                &(&partial, original_size),
                |b, (input, original_size)| {
                    b.iter(|| decode(k, m, input, *original_size).unwrap());
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
