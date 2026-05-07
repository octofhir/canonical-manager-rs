//! Install-pipeline benchmarks.
//!
//! End-to-end `install` is dominated by network + tar extraction, both
//! of which are network-bound and not the part we control. This bench
//! focuses on the parts we can iterate on cheaply: the
//! `add_packages_batch` indexing path that turns an [`ExtractedPackage`]
//! into the persisted SQLite + file-CAS state.
//!
//! Three scales (3 / 10 / 30 packages, 50 resources each) line up with
//! AUDIT-D1/D2/D3 dataset shapes. Cold-DB run per iter so prior-state
//! caching doesn't skew batch numbers.
//!
//! Run with `cargo bench --bench install`.

mod common;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use octofhir_canonical_manager::config::StorageConfig;
use octofhir_canonical_manager::sqlite_storage::SqliteStorage;
use std::hint::black_box;
use tempfile::TempDir;

fn bench_batch_index(c: &mut Criterion) {
    // Multi-thread runtime: deadpool-sqlite's recycle task spawns onto
    // the runtime; current_thread loses the reactor between block_on
    // calls and panics on Pool drop.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();

    let mut group = c.benchmark_group("install_add_packages_batch");
    for &(pkgs, per_pkg) in &[(3usize, 50usize), (10, 50), (30, 50)] {
        let total = (pkgs * per_pkg) as u64;
        group.throughput(Throughput::Elements(total));
        group.sample_size(20);
        group.bench_function(format!("{pkgs}pkg_{per_pkg}res"), |b| {
            b.iter_batched(
                || {
                    // Build the ExtractedPackage payloads outside the timer
                    // (they're identical work each iter; the timed code
                    // is just SQLite + CAS writes).
                    runtime.block_on(common::build_fixture(per_pkg, pkgs));
                    let temp = TempDir::new().unwrap();
                    let cfg = StorageConfig {
                        cache_dir: temp.path().join("cache"),
                        packages_dir: temp.path().join("packages"),
                        max_cache_size: "1GB".to_string(),
                        connection_pool_size: 4,
                        fhir_cache_compat: false,
                    };
                    let storage = runtime.block_on(SqliteStorage::new(cfg)).unwrap();
                    let packages = runtime.block_on(common::build_extracted_packages(
                        per_pkg,
                        pkgs,
                        temp.path(),
                    ));
                    (temp, storage, packages)
                },
                |(temp, storage, packages)| {
                    runtime.block_on(async move {
                        storage.add_packages_batch(packages).await.unwrap();
                        // Drop storage inside the runtime so deadpool's
                        // recycle task has a reactor.
                        drop(storage);
                        black_box(temp)
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_batch_index);
criterion_main!(benches);
