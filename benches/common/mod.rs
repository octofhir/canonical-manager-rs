//! Shared bench fixtures.
//!
//! Provides a populated [`SqliteStorage`] with a configurable resource
//! count so resolve/search benches don't each rebuild their own.

#![allow(dead_code)]

use octofhir_canonical_manager::config::StorageConfig;
use octofhir_canonical_manager::package::{ExtractedPackage, FhirResource, PackageManifest};
use octofhir_canonical_manager::sqlite_storage::SqliteStorage;
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

pub struct PopulatedFixture {
    pub _temp: TempDir,
    pub storage: Arc<SqliteStorage>,
    pub urls: Vec<String>,
}

/// Build the same `ExtractedPackage` payloads `build_fixture` would
/// install, but return them un-installed so callers can time the
/// indexing path in isolation.
pub async fn build_extracted_packages(
    resources_per_package: usize,
    packages: usize,
    temp_root: &std::path::Path,
) -> Vec<ExtractedPackage> {
    let mut batch = Vec::with_capacity(packages);
    for pkg_idx in 0..packages {
        let name = format!("bench.pkg.{pkg_idx:03}");
        let version = "1.0.0".to_string();
        let mut resources = Vec::with_capacity(resources_per_package);
        for r in 0..resources_per_package {
            let url = format!("http://example.com/StructureDefinition/{name}-{r}");
            let content = json!({
                "resourceType": "StructureDefinition",
                "id": format!("{name}-{r}"),
                "url": url,
                "name": format!("Profile{r}"),
                "status": "active",
                "kind": "resource",
                "type": "Patient",
            });
            let file = temp_root.join(format!("{pkg_idx}-{r}.json"));
            std::fs::write(&file, content.to_string()).unwrap();
            resources.push(FhirResource {
                resource_type: "StructureDefinition".to_string(),
                id: format!("{name}-{r}"),
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
        batch.push(ExtractedPackage {
            name,
            version,
            manifest,
            resources,
            extraction_path: PathBuf::from(temp_root),
        });
    }
    batch
}

pub async fn build_fixture(resources_per_package: usize, packages: usize) -> PopulatedFixture {
    let temp = TempDir::new().unwrap();
    let cfg = StorageConfig {
        cache_dir: temp.path().join("cache"),
        packages_dir: temp.path().join("packages"),
        max_cache_size: "1GB".to_string(),
        connection_pool_size: 4,
        fhir_cache_compat: false,
    };
    let storage = Arc::new(SqliteStorage::new(cfg).await.unwrap());

    let mut all_urls = Vec::with_capacity(resources_per_package * packages);
    let mut batch = Vec::with_capacity(packages);
    for pkg_idx in 0..packages {
        let name = format!("bench.pkg.{pkg_idx:03}");
        let version = "1.0.0".to_string();
        let mut resources = Vec::with_capacity(resources_per_package);
        for r in 0..resources_per_package {
            let url = format!(
                "http://example.com/StructureDefinition/{name}-{r}",
                name = name
            );
            all_urls.push(url.clone());
            let content = json!({
                "resourceType": "StructureDefinition",
                "id": format!("{name}-{r}"),
                "url": url,
                "name": format!("Profile{r}"),
                "status": "active",
                "kind": "resource",
                "type": "Patient",
            });
            let file = temp.path().join(format!("{name}-{r}.json"));
            std::fs::write(&file, content.to_string()).unwrap();
            resources.push(FhirResource {
                resource_type: "StructureDefinition".to_string(),
                id: format!("{name}-{r}"),
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
        batch.push(ExtractedPackage {
            name,
            version,
            manifest,
            resources,
            extraction_path: PathBuf::from(temp.path()),
        });
    }
    storage.add_packages_batch(batch).await.unwrap();

    PopulatedFixture {
        _temp: temp,
        storage,
        urls: all_urls,
    }
}
