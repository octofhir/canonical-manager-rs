//! Tarball-extraction throughput across small / medium / large
//! synthetic .tgz files. The measurement that landed picked the
//! blocking `tar` + `flate2` path inside `spawn_blocking` over an
//! `async-tar` + `async-compression` alternative; this bench tracks
//! that path's throughput so future regressions show up.
//!
//! Tarballs are generated in-memory at three sizes representative of
//! real packages.fhir.org payloads:
//!
//! - small  ≈ 50 KB  (a tiny profile-only IG)
//! - medium ≈ 5 MB   (typical national IG, e.g. US-Core, IPS)
//! - large  ≈ 50 MB  (terminology-heavy IG)
//!
//! Run with `cargo bench --bench tar_extract`.

use std::io::Cursor;
use std::path::Path;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use tar::{Builder, Header};
use tempfile::TempDir;

const SMALL_BYTES: usize = 50 * 1024;
const MEDIUM_BYTES: usize = 5 * 1024 * 1024;
const LARGE_BYTES: usize = 50 * 1024 * 1024;

/// Build a deterministic .tgz containing one or more files whose
/// combined uncompressed size approximates `target_bytes`. Each file is
/// 8 KiB of pseudo-random bytes (gzip-resistant, so tarball size is
/// close to file size — closer to reality than a single zero-filled
/// blob which compresses 1000:1).
fn build_tarball(target_bytes: usize) -> Vec<u8> {
    const FILE_BYTES: usize = 8 * 1024;
    let count = target_bytes.div_ceil(FILE_BYTES);

    let mut buf = Vec::with_capacity(target_bytes);
    {
        let encoder = GzEncoder::new(&mut buf, Compression::default());
        let mut tar = Builder::new(encoder);

        let manifest = br#"{"name":"bench.tar","version":"1.0.0","fhirVersions":["4.0.1"]}"#;
        let mut h = Header::new_gnu();
        h.set_path("package/package.json").unwrap();
        h.set_size(manifest.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        tar.append(&h, manifest.as_slice()).unwrap();

        // Pseudo-random body files. Use a simple LCG so different
        // payloads don't collapse under gzip but the bench is still
        // deterministic across runs.
        let mut state: u64 = 0xc0ffee_d00d;
        for i in 0..count {
            let mut body = vec![0u8; FILE_BYTES];
            for byte in body.iter_mut() {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                *byte = (state >> 24) as u8;
            }
            let path = format!("package/r-{i:06}.json");
            let mut h = Header::new_gnu();
            h.set_path(&path).unwrap();
            h.set_size(body.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            tar.append(&h, body.as_slice()).unwrap();
        }

        tar.into_inner().unwrap().finish().unwrap();
    }
    buf
}

fn extract_blocking(tgz: &[u8], out: &Path) -> std::io::Result<()> {
    let cursor = Cursor::new(tgz);
    let tar = GzDecoder::new(cursor);
    let mut archive = tar::Archive::new(tar);
    archive.unpack(out)?;
    Ok(())
}

fn bench_extract(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _enter = runtime.enter();

    for (label, size) in [
        ("small_50KB", SMALL_BYTES),
        ("medium_5MB", MEDIUM_BYTES),
        ("large_50MB", LARGE_BYTES),
    ] {
        let tgz = build_tarball(size);
        let mut group = c.benchmark_group(format!("tar_extract/{label}"));
        group.throughput(Throughput::Bytes(size as u64));
        group.sample_size(10);

        // Inline blocking call: minimum cost, no thread-pool hop.
        // Useful as the "ideal" baseline — production code never uses
        // it because it would block the tokio worker.
        group.bench_function("blocking_inline", |b| {
            b.iter_batched(
                || TempDir::new().unwrap(),
                |temp| {
                    extract_blocking(&tgz, temp.path()).unwrap();
                    temp
                },
                BatchSize::SmallInput,
            );
        });

        // What the install pipeline actually does: hand the unpack to
        // `spawn_blocking` so the tokio worker stays free.
        group.bench_function("blocking_in_spawn_blocking", |b| {
            b.iter_batched(
                || TempDir::new().unwrap(),
                |temp| {
                    let tgz = tgz.clone();
                    let out = temp.path().to_path_buf();
                    runtime.block_on(async move {
                        tokio::task::spawn_blocking(move || extract_blocking(&tgz, &out).unwrap())
                            .await
                            .unwrap();
                    });
                    temp
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}

criterion_group!(benches, bench_extract);
criterion_main!(benches);
