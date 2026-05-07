//! Canonical-URL resolve benchmarks.
//!
//! Pre-populates a SQLite-backed storage with synthetic packages, then
//! drives `find_resource` (the exact-match hot path) over a uniform
//! shuffle of known URLs. P50/P99 are reported by criterion's default
//! statistics.
//!
//! Run with `cargo bench --bench resolve`.

mod common;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

fn bench_resolve(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();

    let fixture = runtime.block_on(common::build_fixture(50, 20)); // 1000 resources
    let urls: Vec<String> = fixture.urls.clone();

    let mut group = c.benchmark_group("resolve_find_resource");
    group.throughput(Throughput::Elements(1));
    let mut idx = 0usize;
    group.bench_function("hit", |b| {
        b.iter(|| {
            let url = &urls[idx % urls.len()];
            idx = idx.wrapping_add(1);
            runtime.block_on(async {
                let r = fixture.storage.find_resource(url).await.unwrap();
                black_box(r);
            });
        });
    });
    group.bench_function("miss", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let r = fixture
                    .storage
                    .find_resource("http://example.com/does/not/exist")
                    .await
                    .unwrap();
                black_box(r);
            });
        });
    });
    group.finish();

    // SqliteStorage's deadpool Pool spawns a recycle task on drop; that
    // task needs the Tokio runtime to be alive. Drop fixture inside
    // a block_on so the reactor is still running.
    runtime.block_on(async move {
        drop(fixture);
    });
}

criterion_group!(benches, bench_resolve);
criterion_main!(benches);
