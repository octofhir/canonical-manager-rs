//! Search-engine benchmarks.
//!
//! Drives [`SearchEngine::search`] under three workloads: text-only,
//! type-filter only, and combined text + type filter. Index build cost
//! is measured separately (first call lazily builds it).
//!
//! Run with `cargo bench --bench search`.

mod common;

use criterion::{Criterion, criterion_group, criterion_main};
use octofhir_canonical_manager::search::{SearchEngine, SearchQuery};
use std::hint::black_box;

fn bench_search(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();

    let fixture = runtime.block_on(common::build_fixture(50, 20)); // 1000 resources
    let engine = SearchEngine::new(fixture.storage.clone());

    let mut group = c.benchmark_group("search");

    group.bench_function("text", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let q = SearchQuery {
                    text: Some("Profile".to_string()),
                    ..Default::default()
                };
                let r = engine.search(&q).await.unwrap();
                black_box(r);
            });
        });
    });

    group.bench_function("type_filter", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let q = SearchQuery {
                    resource_types: vec!["StructureDefinition".to_string()],
                    ..Default::default()
                };
                let r = engine.search(&q).await.unwrap();
                black_box(r);
            });
        });
    });

    group.bench_function("text_plus_type", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let q = SearchQuery {
                    text: Some("Profile".to_string()),
                    resource_types: vec!["StructureDefinition".to_string()],
                    ..Default::default()
                };
                let r = engine.search(&q).await.unwrap();
                black_box(r);
            });
        });
    });

    group.finish();

    // Same drop-order constraint as resolve.rs: deadpool needs the
    // reactor at drop time.
    drop(engine);
    runtime.block_on(async move {
        drop(fixture);
    });
}

criterion_group!(benches, bench_search);
criterion_main!(benches);
