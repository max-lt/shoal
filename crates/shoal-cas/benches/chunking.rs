//! Benchmarks for CDC chunking.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use shoal_cas::CdcChunker;

fn bench_data(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut state: u32 = 0xDEAD_BEEF;
    for _ in 0..size {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}

fn bench_cdc_chunk(c: &mut Criterion) {
    let chunker = CdcChunker::new();
    let sizes: &[usize] = &[
        64 * 1024,        // 64 KB
        256 * 1024,       // 256 KB
        1024 * 1024,      // 1 MB
        4 * 1024 * 1024,  // 4 MB
        16 * 1024 * 1024, // 16 MB
    ];

    let mut group = c.benchmark_group("cdc_chunk");
    for &size in sizes {
        let data = bench_data(size);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, data| {
            b.iter(|| chunker.chunk(data));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_cdc_chunk);
criterion_main!(benches);
