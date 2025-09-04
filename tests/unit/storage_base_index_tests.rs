use octofhir_canonical_manager::binary_storage::BinaryStorage;
use octofhir_canonical_manager::package::{ExtractedPackage, FhirResource, PackageManifest};
use octofhir_canonical_manager::StorageConfig;
use serde_json::json;
use std::collections::HashMap;
use tempfile::TempDir;

#[tokio::test]
async fn base_index_orders_by_semver_and_lists_versions() {
    let tmp = TempDir::new().unwrap();
    let cfg = StorageConfig { 
        cache_dir: tmp.path().join("cache"),
        index_dir: tmp.path().join("index"),
        packages_dir: tmp.path().join("packages"),
        max_cache_size: "10MB".into(),
    };
    let storage = BinaryStorage::new(cfg).await.unwrap();

    // Two packages with URLs carrying version in path
    let base = "http://example.com/StructureDefinition/test";
    let p1 = make_pkg("pkg.a", "1.0.0", &format!("{}/1.0.0", base));
    let p2 = make_pkg("pkg.b", "2.1.0", &format!("{}/2.1.0", base));
    storage.add_package(&p1).await.unwrap();
    storage.add_package(&p2).await.unwrap();

    let versions = storage.list_versions_for_canonical(base).await.unwrap();
    assert_eq!(versions, vec!["2.1.0", "1.0.0"]);

    let latest = storage.find_latest_by_base_url(base).await.unwrap().unwrap();
    assert_eq!(latest.metadata.version.as_deref(), Some("2.1.0"));
}

#[tokio::test]
async fn add_is_atomic_on_duplicate_and_remove_cleans_indexes() {
    let tmp = TempDir::new().unwrap();
    let cfg = StorageConfig { 
        cache_dir: tmp.path().join("cache"),
        index_dir: tmp.path().join("index"),
        packages_dir: tmp.path().join("packages"),
        max_cache_size: "10MB".into(),
    };
    let storage = BinaryStorage::new(cfg).await.unwrap();

    let base = "http://example.com/StructureDefinition/test2";
    let pkg = make_pkg("pkg.x", "1.0.0", base);
    storage.add_package(&pkg).await.unwrap();

    // Duplicate add should error and not change counts
    let before = storage.list_packages().await.unwrap().len();
    let dup = storage.add_package(&pkg).await;
    assert!(dup.is_err());
    let after = storage.list_packages().await.unwrap().len();
    assert_eq!(before, after);

    // Remove and verify indexes updated (no versions left)
    let removed = storage.remove_package("pkg.x", "1.0.0").await.unwrap();
    assert!(removed);
    let versions = storage.list_versions_for_canonical(base).await.unwrap();
    assert!(versions.is_empty());
}

fn make_pkg(name: &str, version: &str, url: &str) -> ExtractedPackage {
    let res = json!({
        "resourceType": "StructureDefinition",
        "id": "t",
        "url": url,
        "version": version,
    });
    let path = std::env::temp_dir().join(format!("{name}-{version}-t.json"));
    std::fs::write(&path, serde_json::to_string_pretty(&res).unwrap()).unwrap();
    let resource = FhirResource {
        resource_type: "StructureDefinition".into(),
        id: "t".into(),
        url: Some(url.into()),
        version: Some(version.into()),
        content: res,
        file_path: path,
    };
    let manifest = PackageManifest {
        name: name.into(),
        version: version.into(),
        fhir_versions: Some(vec!["4.0.1".into()]),
        dependencies: HashMap::new(),
        canonical: None,
        jurisdiction: None,
        package_type: None,
        title: None,
        description: None,
    };
    ExtractedPackage { name: name.into(), version: version.into(), manifest, resources: vec![resource], extraction_path: std::env::temp_dir() }
}

