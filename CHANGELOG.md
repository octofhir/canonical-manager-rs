# Changelog

All notable changes to `octofhir-canonical-manager` are recorded here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semver](https://semver.org/).

## [Unreleased]

## [0.2.0] — 2026-05-07

Major P0+P1+P2 milestone bump. Registry rewrite, audit fixes, virtual
content-addressable store, deterministic lockfile, full CLI surface.
The trait surface (`PackageStore`, `SearchStorage`, `AsyncRegistry`,
`PackageInfo`, `ResourceIndex`, `ExtractedPackage`, `FhirResource`,
`FcmError`, `StorageError`) is unchanged from 0.1.26 — server-rs and
other custom-backend consumers should compile against this release
without source edits.

## Highlights

- **Virtual store** at `<cache_dir>/store/` with file-level CAS
  (SHA-256, 2-char fanout) and tarball CAS (Blake3). 50-IG synthetic
  benchmark shows **48.9 % disk savings** vs naïve copy.
- **Deterministic `fcm.lock`** — TOML, sorted-by-`(name, version)`,
  SHA-512 SRI per package, transitive deps captured from registry
  response. Byte-identical across machines.
- **`fcm install --frozen-lockfile`** mirrors pnpm: install fails on
  any drift in `(name, version, integrity, dependencies, resolved,
  fhir_version)` against the on-disk lockfile. Lockfile never mutated
  in frozen mode.
- **Concurrent install correctness** — `PackageInstallLock` (fs4
  advisory lock at `<store>/locks/<name>@<version>.lock`) wired into
  every install path with check-acquire-recheck. Eight concurrent
  `fcm install` of the same package now fetch the tarball exactly once.
- **CLI surface**: `fcm store {path,verify,gc,prune}`, `fcm
  materialize`, `fcm lock` plus `fcm install --frozen-lockfile`.
- **Performance**: full perf report at
  `docs/refactor/PERF_REPORT.md`; install hot path is
  3 × faster than 0.1.x on the `30pkg_50res` bench (16 s → 5 s with
  default settings, 16 s → 0.3 s when `durable_writes = false`).

## New configuration knobs

All have `#[serde(default)]` so existing `fcm.toml` files keep working.

| Key | Default | Effect |
|---|---|---|
| `[storage] fhir_cache_compat` | `false` | When `true`, mirror installed packages into `~/.fhir/packages/<name>-<version>/package/` so SUSHI / IG Publisher / Firely tooling pick them up. Files reflinked / hardlinked from CAS, no extra disk usage on supporting filesystems. |
| `[optimization] durable_writes` | `true` | When `false`, file-CAS skips `fsync` on every blob. Trades crash-recoverable durability for ~95 % faster blob inserts on APFS / ext4 / NTFS. SQLite WAL still keeps the index durable. |
| `[registry] parallel_downloads` | `16` | In-flight cap for `download_packages_parallel`. Raised from previously-hardcoded `8` based on `benches/download_parallelism.rs` measurement. |

## New API surface (additive)

All on `CanonicalManager` unless noted. Existing methods unchanged.

- `InstallOptions { frozen_lockfile, project_root }` +
  `install_packages_batch_with_options(specs, opts)`. Default opts
  preserve legacy `install_packages_batch` semantics.
- `assert_lockfile_unchanged(project_root)` — public for CI gates.
- `build_lockfile() -> Lockfile`,
  `write_lockfile(project_root)` — generate + persist `fcm.lock`.
- `materialise_packages(project_root) -> MaterialiseReport` —
  link installed packages into `<project>/.fcm/modules/`.
- `populate_fhir_cache_all(target_root)`,
  `populate_fhir_cache_for_package(name, version, target)` —
  populate the cross-tool `~/.fhir/packages/` cache.
- `sqlite_storage_handle() -> Option<&Arc<SqliteStorage>>`
  (sqlite-feature only) — back door for callers that need
  CAS-aware helpers without growing the frozen `PackageStore` trait.
- `pub mod store` — `FileCas`, `BlobHash`, `Linker`,
  `LinkCapability`, `LinkerMode`, `PackageInstallLock`,
  `Lockfile`, `LockedPackage`, `MaterialiseReport`, `StorePaths`,
  `default_fhir_cache_root`, `populate_fhir_cache`,
  `materialise_dir`, `materialise_package`.
- `SqliteStorage::new_with_durability(config, durable)` — constructor
  variant honouring the `durable_writes` flag.
- `UnifiedStorage::new_with_durability(config, durable)` — same.
- `SqliteStorage::file_cas`, `record_tarball`, `get_tarball`,
  `verify_blobs`, `list_orphan_blobs`, `list_orphan_tarballs`,
  `delete_blob_rows`, `delete_tarball_rows` — exposed for the
  `fcm store` CLI subcommands.

## New error variant

- `StorageError::LockfileDrift { details }` — surfaced by
  `assert_lockfile_unchanged` and `--frozen-lockfile`.
  `StorageError` is not `#[non_exhaustive]`; downstream consumers with
  exhaustive matches must add an arm. Audited consumers (`server-rs`)
  pattern-match on a different `StorageError` and are unaffected.

## Removed

- The fully-async tar-extraction path in `PackageExtractor`
  (`async-tar` + `async-compression`). Bench showed it was 1.5–2.4 ×
  slower than blocking-tar inside `spawn_blocking` across the IG-size
  range, with no observed correctness benefit.
- Dropped dependencies: `async-tar`, `async-compression`, `tokio-util`.

## Storage migrations

- SQLite schema is **version 2** (single version, no v3). On startup,
  any pre-v2 database is dropped and recreated; packages re-resolve
  from the registry on the next install. The lockfile (if present)
  drives reproducibility.
- Old `~/.fcm/packages/storage.db` paths predating P1 are wiped on
  first run with a single `WARN` log line.

## Bench reproducers

```bash
cargo bench --bench cas
cargo bench --bench install
cargo bench --bench resolve
cargo bench --bench search
cargo bench --bench lockfile
cargo bench --bench linker
cargo bench --bench disk_usage
cargo bench --bench durable_writes
cargo bench --bench tar_extract
cargo bench --bench download_parallelism
```

See `docs/refactor/PERF_REPORT.md` for headline numbers and
methodology.
