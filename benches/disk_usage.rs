//! Disk-deduplication runner (not a Criterion bench).
//!
//! Measures the bytes-on-disk savings the file CAS gives when 50 mock
//! IGs share a common pool of "core" resources and each adds a small
//! number of unique ones. Synthetic fixture is intentional: real
//! packages.fhir.org tarballs hit the network and pull terabyte-class
//! transitive closures; we want a fast deterministic harness that still
//! reflects the dedup pattern (FHIR core resources reused across IGs).
//!
//! Output: single-line summary plus a Markdown row.
//!
//! Run with `cargo bench --bench disk_usage`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use octofhir_canonical_manager::config::StorageConfig;
use octofhir_canonical_manager::package::{ExtractedPackage, FhirResource, PackageManifest};
use octofhir_canonical_manager::sqlite_storage::SqliteStorage;
use serde_json::json;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const PACKAGES: usize = 50;
const SHARED_RESOURCES: usize = 25;
const UNIQUE_PER_PACKAGE: usize = 25;
const RESOURCE_BLOAT_BYTES: usize = 4 * 1024;

fn build_packages(temp: &Path) -> Vec<ExtractedPackage> {
    // Fixed-size payload per resource so the dedup ratio is dominated
    // by sharing pattern rather than payload-size variance.
    let bloat: String = "x".repeat(RESOURCE_BLOAT_BYTES);

    let mut packages = Vec::with_capacity(PACKAGES);
    for pkg_idx in 0..PACKAGES {
        let name = format!("bench.pkg.{pkg_idx:03}");
        let version = "1.0.0".to_string();
        let pkg_dir = temp.join(format!("{name}-{version}"));
        std::fs::create_dir_all(&pkg_dir).unwrap();

        let mut resources = Vec::with_capacity(SHARED_RESOURCES + UNIQUE_PER_PACKAGE);

        // Shared resources: identical bytes across every package, so
        // 50 installs collapse to one CAS blob each.
        for r in 0..SHARED_RESOURCES {
            let url = format!("http://example.com/shared/StructureDefinition/core-{r}");
            let content = json!({
                "resourceType": "StructureDefinition",
                "id": format!("core-{r}"),
                "url": url,
                "name": format!("CoreProfile{r}"),
                "status": "active",
                "kind": "resource",
                "type": "Patient",
                "bloat": bloat,
            });
            let file = pkg_dir.join(format!("core-{r}.json"));
            std::fs::write(&file, content.to_string()).unwrap();
            resources.push(FhirResource {
                resource_type: "StructureDefinition".to_string(),
                id: format!("core-{r}"),
                url: Some(url),
                version: Some("1.0.0".to_string()),
                content,
                file_path: file,
            });
        }

        // Unique-per-package resources: distinct bytes per (package,
        // resource) pair, so each one writes a fresh CAS blob.
        for r in 0..UNIQUE_PER_PACKAGE {
            let url = format!("http://example.com/{name}/StructureDefinition/uniq-{r}");
            let content = json!({
                "resourceType": "StructureDefinition",
                "id": format!("{name}-uniq-{r}"),
                "url": url,
                "name": format!("UniqProfile{r}"),
                "status": "active",
                "kind": "resource",
                "type": "Patient",
                "bloat": bloat,
            });
            let file = pkg_dir.join(format!("uniq-{r}.json"));
            std::fs::write(&file, content.to_string()).unwrap();
            resources.push(FhirResource {
                resource_type: "StructureDefinition".to_string(),
                id: format!("{name}-uniq-{r}"),
                url: Some(url),
                version: Some("1.0.0".to_string()),
                content,
                file_path: file,
            });
        }

        let manifest = PackageManifest {
            name: name.clone(),
            version: version.clone(),
            description: None,
            fhir_versions: Some(vec!["4.0.1".to_string()]),
            dependencies: HashMap::new(),
            canonical: None,
            jurisdiction: None,
            package_type: None,
            title: None,
        };
        packages.push(ExtractedPackage {
            name,
            version,
            manifest,
            resources,
            extraction_path: pkg_dir,
        });
    }
    packages
}

fn dir_total_bytes(root: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        let entries = match std::fs::read_dir(&p) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for e in entries.flatten() {
            let m = match e.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if m.is_dir() {
                stack.push(e.path());
            } else if m.is_file() {
                total += m.len();
            }
        }
    }
    total
}

fn naive_bytes(packages: &[ExtractedPackage]) -> u64 {
    packages
        .iter()
        .flat_map(|p| p.resources.iter())
        .map(|r| serde_json::to_string(&r.content).unwrap().len() as u64)
        .sum()
}

async fn run() {
    // Multi-thread runtime is required because deadpool-sqlite's
    // recycle task spawns onto the runtime; current_thread loses the
    // reactor between block_on calls and panics on Pool drop.
    let temp = TempDir::new().unwrap();
    let cas_root = temp.path().join("cache");
    std::fs::create_dir_all(&cas_root).unwrap();

    let cfg = StorageConfig {
        cache_dir: cas_root.clone(),
        packages_dir: temp.path().join("packages"),
        max_cache_size: "1GB".to_string(),
        connection_pool_size: 4,
        fhir_cache_compat: false,
    };
    let storage = Arc::new(SqliteStorage::new(cfg).await.unwrap());

    let pkg_root = temp.path().join("src-pkgs");
    std::fs::create_dir_all(&pkg_root).unwrap();
    let packages = build_packages(&pkg_root);

    let naive = naive_bytes(&packages);

    let install_start = std::time::Instant::now();
    storage
        .add_packages_batch(packages)
        .await
        .expect("add_packages_batch");
    let install_elapsed = install_start.elapsed();

    let blobs_root = cas_root.join("store").join("blobs");
    let cas_total = dir_total_bytes(&blobs_root);

    let saved = naive.saturating_sub(cas_total);
    let ratio = if naive == 0 {
        0.0
    } else {
        saved as f64 / naive as f64
    };

    println!();
    println!(
        "Disk dedup measurement ({PACKAGES} packages, {SHARED_RESOURCES} shared + {UNIQUE_PER_PACKAGE} unique resources each)"
    );
    println!("  install (add_packages_batch): {install_elapsed:?}");
    println!("  naive bytes (no dedup):       {naive:>12}");
    println!("  CAS blobs total:              {cas_total:>12}");
    println!("  saved:                        {saved:>12}");
    println!("  dedup ratio:                  {:.1}%", ratio * 100.0);
    println!();
    println!("Markdown row:");
    println!(
        "| 50 IGs (25 shared + 25 unique × {bloat} B) | {naive} B | {cas_total} B | **{:.1}%** |",
        ratio * 100.0,
        bloat = RESOURCE_BLOAT_BYTES,
    );
}

fn main() {
    let runtime = Runtime::new().expect("tokio runtime");
    runtime.block_on(run());
}
