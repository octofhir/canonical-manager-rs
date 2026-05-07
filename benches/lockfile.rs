//! Lockfile build/serialise benchmarks.
//!
//! Walks `Lockfile::upsert` + `Lockfile::canonicalise` + TOML
//! serialisation across 1 / 10 / 50 / 200 packages. Catches accidental
//! O(n²) regressions in the determinism pass (sort + dedup) and the
//! TOML emitter.
//!
//! Run with `cargo bench --bench lockfile`.

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use octofhir_canonical_manager::store::{LockedPackage, Lockfile};
use std::hint::black_box;
use tempfile::TempDir;

fn make_pkgs(n: usize) -> Vec<LockedPackage> {
    (0..n)
        .map(|i| LockedPackage {
            name: format!("pkg.{i:04}"),
            version: format!("1.{}.0", i % 50),
            resolved: format!("https://packages.fhir.org/pkg.{i:04}/1.{}.0", i % 50),
            integrity: "sha512-AAAA".into(),
            fhir_version: Some("4.0.1".into()),
            dependencies: (0..3).map(|j| format!("dep.{j}@1.0.0")).collect(),
        })
        .collect()
}

fn bench_canonicalise_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("lockfile_canonicalise");
    for &n in &[1_usize, 10, 50, 200] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("{n}pkgs"), |b| {
            let pkgs = make_pkgs(n);
            b.iter_batched(
                || {
                    let mut lock = Lockfile::empty();
                    for p in &pkgs {
                        lock.upsert(p.clone());
                    }
                    lock
                },
                |mut lock| {
                    lock.canonicalise();
                    black_box(lock);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_save(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("lockfile_save");
    for &n in &[1_usize, 10, 50, 200] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(format!("{n}pkgs"), |b| {
            b.iter_batched(
                || {
                    let mut lock = Lockfile::empty();
                    for p in make_pkgs(n) {
                        lock.upsert(p);
                    }
                    let dir = TempDir::new().unwrap();
                    (dir, lock)
                },
                |(dir, mut lock)| {
                    runtime.block_on(async {
                        lock.save(dir.path()).await.unwrap();
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_canonicalise_only, bench_save);
criterion_main!(benches);
