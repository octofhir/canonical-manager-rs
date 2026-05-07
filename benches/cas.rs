//! File-CAS micro-benchmarks.
//!
//! Measures the per-blob cost of [`FileCas::insert_bytes`] across three
//! payload sizes (1 KB / 10 KB / 100 KB) so we can spot regressions in
//! the tmp-then-rename path. Cold-state run (fresh tempdir per iter)
//! avoids the early-return on existing-blob and keeps the benchmark
//! representative of an actual install workload.
//!
//! Run with `cargo bench --bench cas`.
//!
//! Why these sizes: most FHIR resources land between 2-30 KB. 1 KB
//! probes pure syscall overhead, 100 KB exercises real I/O.

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use octofhir_canonical_manager::store::{FileCas, StorePaths};
use std::hint::black_box;
use tempfile::TempDir;

fn make_payload(size: usize) -> Vec<u8> {
    // Use a counter so each byte differs and SHA-256 sees real entropy.
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn bench_cas_insert(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("cas_insert_bytes");
    for &size in &[1024_usize, 10 * 1024, 100 * 1024] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_function(format!("{}KB", size / 1024), |b| {
            b.iter_batched(
                || {
                    // Fresh tempdir per iter so the blob isn't already
                    // present (would return immediately and skew results).
                    let tmp = TempDir::new().unwrap();
                    let cas = FileCas::new(StorePaths::with_root(tmp.path().to_path_buf()));
                    let payload = make_payload(size);
                    (tmp, cas, payload)
                },
                |(_tmp, cas, payload)| {
                    runtime.block_on(async {
                        let h = cas.insert_bytes(black_box(&payload)).await.unwrap();
                        black_box(h);
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_cas_insert);
criterion_main!(benches);
