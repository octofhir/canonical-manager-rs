//! Linker ladder benchmarks: reflink vs hardlink vs copy.
//!
//! Links a 4 KB scratch file from a tempdir into another tempdir under
//! each forced [`LinkerMode`] variant. Capabilities of the host FS
//! decide which modes succeed; missing capabilities surface as bench
//! errors rather than silent fallback so regressions are visible.
//!
//! Run with `cargo bench --bench linker`.

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use octofhir_canonical_manager::store::{LinkCapability, Linker, LinkerMode};
use std::hint::black_box;
use tempfile::TempDir;

fn bench_linker_modes(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("linker_materialise");

    for (label, mode, caps) in [
        (
            "auto",
            LinkerMode::Auto,
            LinkCapability::REFLINK | LinkCapability::HARDLINK | LinkCapability::COPY,
        ),
        (
            "hardlink",
            LinkerMode::Hardlink,
            LinkCapability::HARDLINK | LinkCapability::COPY,
        ),
        ("copy", LinkerMode::Copy, LinkCapability::COPY_ONLY),
    ] {
        group.bench_function(label, |b| {
            b.iter_batched(
                || {
                    let store = TempDir::new().unwrap();
                    let project = TempDir::new().unwrap();
                    let src = store.path().join("source.json");
                    std::fs::write(&src, vec![0u8; 4096]).unwrap();
                    let dst = project.path().join("dst.json");
                    let linker = Linker::with_caps(mode, caps);
                    (store, project, src, dst, linker)
                },
                |(_store, _project, src, dst, linker)| {
                    runtime.block_on(async {
                        let outcome = linker.materialise(&src, &dst).await.unwrap();
                        black_box(outcome);
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_linker_modes);
criterion_main!(benches);
